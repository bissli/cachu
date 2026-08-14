"""Targeted tests for DynamoDB backend internals against DynamoDB Local.

Covers the behaviors the generic suites cannot see: lazy native TTL vs
the client-side expiry check, the hashed partition key, scan narrowing vs
fnmatch authority, the token-guarded lock takeover, stats buffering, and
the configuration knobs.
"""
import asyncio
import concurrent.futures
import logging
import math
import pickle
import threading
import time

import cachu
import pytest
from _fixtures.dynamodb import TABLE_NAME
from botocore.exceptions import ClientError
from cachu.api import NO_VALUE
from cachu.backends.dynamodb import DynamoDBBackend, _pk, create_dynamodb_table
from cachu.exception import ConfigurationError
from cachu.mutex import AsyncDynamoDBMutex, DynamoDBMutex
from cachu.util import make_clear_pattern


@pytest.fixture
def backend(dynamodb_table):
    """Provide a DynamoDBBackend against the DynamoDB Local table.
    """
    return DynamoDBBackend(TABLE_NAME)


@pytest.fixture
def bare_table(backend):
    """Provide a second table with native TTL never enabled.

    Notes
    -----
    - DynamoDB Local's TTL sweep really deletes expired rows about
      every 10 seconds, so a takeover test that sleeps past a lock's
      expiry can lose its teeth mid-run: the sweeper collects the row
      and acquire() exercises the fresh attribute_not_exists branch
      instead of the takeover clause. With TTL never enabled, rows
      survive until deleted.
    - Dropped by `dynamodb_table`'s drop-every-table teardown.
    """
    backend.client.create_table(
        TableName='cachu-bare',
        BillingMode='PAY_PER_REQUEST',
        AttributeDefinitions=[
            {'AttributeName': 'key', 'AttributeType': 'S'},
            ],
        KeySchema=[
            {'AttributeName': 'key', 'KeyType': 'HASH'},
            ])
    backend.client.get_waiter('table_exists').wait(TableName='cachu-bare')
    return 'cachu-bare'


def _put_raw(backend, key, attrs):
    """Write a raw item so tests can craft rows the backend never writes.

    Notes
    -----
    - Rows deliberately omit 'expires_ttl': DynamoDB Local really
      collects expired rows on a roughly 10-second sweep (real AWS
      takes days), and the hand-crafted expired rows must stay put for
      the client-side expiry assertions to mean anything.
    """
    item = {'key': {'S': _pk(key)}, 'key_text': {'S': key}}
    item.update(attrs)
    backend.client.put_item(TableName=TABLE_NAME, Item=item)


def _raw_item(backend, key):
    """Read a raw item back, bypassing the backend's expiry handling.
    """
    response = backend.client.get_item(
        TableName=TABLE_NAME, Key={'key': {'S': _pk(key)}}, ConsistentRead=True)
    return response.get('Item')


class TestClientSideExpiry:
    """Native TTL is lazy, so the backend's own expiry check is authoritative.
    """

    def test_expired_but_uncollected_row_is_miss_and_evicted(self, backend):
        """A row past its expiry reads as a miss and is deleted on the spot.

        Mutation: dropping the client-side expires_at check and trusting
        native TTL, which deletes days late.
        Oracle: a hand-crafted row whose expires_at is one hour in the
        past, present in the raw table before the read and gone after.
        """
        _put_raw(backend, 'stale-key', {
            'value': {'B': pickle.dumps('old')},
            'created_at': {'N': str(time.time() - 7200)},
            'expires_at': {'N': str(time.time() - 3600)},
            })
        assert _raw_item(backend, 'stale-key') is not None

        assert backend.get('stale-key') is NO_VALUE
        assert _raw_item(backend, 'stale-key') is None

    def test_expired_rows_hidden_from_keys_and_count(self, backend):
        """keys() and count() skip rows whose expiry has passed.

        Mutation: dropping the '#e >= :now' scan filter, surfacing rows
        native TTL has not collected yet.
        Oracle: hand-listed key set - one live row, one expired row.
        """
        backend.set('live-key', 'v', 300)
        _put_raw(backend, 'dead-key', {
            'value': {'B': pickle.dumps('old')},
            'created_at': {'N': str(time.time() - 7200)},
            'expires_at': {'N': str(time.time() - 3600)},
            })

        assert list(backend.keys()) == ['live-key']
        assert backend.count() == 1

    def test_undecodable_or_malformed_row_is_miss_and_evicted(self, backend):
        """Garbage pickles and attribute-less rows degrade to an evicting miss.

        Mutation: letting the pickle error or a KeyError on a missing
        'value' or 'created_at' attribute propagate instead of treating
        the row as a miss.
        Oracle: hand-crafted rows - non-pickle bytes, no value attribute,
        no created_at attribute - all three gone after the read.
        """
        _put_raw(backend, 'garbage-key', {
            'value': {'B': b'not a pickle'},
            'created_at': {'N': str(time.time())},
            'expires_at': {'N': str(time.time() + 300)},
            })
        _put_raw(backend, 'valueless-key', {
            'expires_at': {'N': str(time.time() + 300)},
            })
        _put_raw(backend, 'no-created-key', {
            'value': {'B': pickle.dumps('v')},
            'expires_at': {'N': str(time.time() + 300)},
            })

        assert backend.get('garbage-key') is NO_VALUE
        assert backend.get('valueless-key') is NO_VALUE
        assert backend.get_with_metadata('no-created-key') == (NO_VALUE, None)
        assert _raw_item(backend, 'garbage-key') is None
        assert _raw_item(backend, 'valueless-key') is None
        assert _raw_item(backend, 'no-created-key') is None

    def test_evict_expired_spares_a_live_rewrite(self, backend):
        """The expired-row eviction cannot destroy a fresh concurrent write.

        Mutation: an unconditional DeleteItem in the expired-eviction
        path, which would delete a row another process rewrote between
        the read that saw it expired and the delete.
        Oracle: a live row surviving a direct eviction attempt.
        """
        backend.set('k', 'fresh', 300)
        backend._evict_expired('k')
        assert backend.get('k') == 'fresh'


class TestHashedPartitionKey:
    """The partition key is a digest; the readable key lives in key_text.
    """

    def test_key_beyond_partition_cap_roundtrips(self, backend):
        """A cache key longer than DynamoDB's 2048-byte key cap still works.

        Mutation: storing the raw key as the partition key, which
        DynamoDB rejects past 2048 UTF-8 bytes - a permanent,
        fail_open-silenced miss for any long-argument call.
        Oracle: AWS's documented 2048-byte cap versus a 5000-byte key
        exercised through set/get/keys/clear.
        """
        long_key = '5m:test:fn|x=' + 'v' * 5000
        backend.set(long_key, 'big', 300)

        assert backend.get(long_key) == 'big'
        assert list(backend.keys('5m:test:fn|*')) == [long_key]
        assert backend.clear('5m:test:fn|*') == 1
        assert backend.get(long_key) is NO_VALUE

    def test_rows_carry_integer_ttl_attribute(self, backend):
        """Rows carry an integer expires_ttl at the ceiling of the expiry.

        Mutation: pointing the native-TTL collector at the fractional
        expires_at - AWS documents the TTL attribute as integer epoch
        seconds and ignores non-conforming formats - or flooring it,
        which would let the collector fire before the true expiry.
        Oracle: the raw attribute, an integer string equal to
        ceil(expires_at).
        """
        backend.set('k', 'v', 300)
        item = _raw_item(backend, 'k')

        ttl_attr = item['expires_ttl']['N']
        assert '.' not in ttl_attr
        assert int(ttl_attr) == math.ceil(float(item['expires_at']['N']))

    def test_retry_config_counts_total_attempts(self, dynamodb_table):
        """retry_count buys exactly that many retries on top of the first try.

        Mutation: passing 'max_attempts': retry_count + 1, which botocore
        reads as retries EXCLUDING the initial request and normalizes to
        retry_count + 2 total attempts.
        Oracle: botocore's resolved retry config on the live client.
        """
        b = DynamoDBBackend(TABLE_NAME, retry_count=2)
        assert b.client.meta.config.retries == {
            'total_max_attempts': 3,
            'mode': 'standard',
            }


class TestScanNarrowing:
    """begins_with bounds the payload; fnmatch stays the pattern authority.
    """

    def test_escaped_prefix_pattern_clears_exactly_its_own_keys(self, backend):
        """A glob-escaped key_prefix clears its keys and only its keys.

        Mutation: matching on the begins_with prefix alone (over-deletes
        the sibling that shares the literal head), or feeding the raw
        pattern to begins_with (its '[[]' class matches nothing at all).
        Oracle: make_clear_pattern for key_prefix 'app[dev]:' against
        hand-set keys - one inside the prefix, one sharing only its head.
        """
        backend.set('5m:app[dev]:fn|x=1', 'mine', 300)
        backend.set('5m:appdev]:fn|x=1', 'not mine', 300)

        pattern = make_clear_pattern(None, 'app[dev]:', 300)
        assert backend.clear(pattern) == 1
        assert backend.get('5m:app[dev]:fn|x=1') is NO_VALUE
        assert backend.get('5m:appdev]:fn|x=1') == 'not mine'

    def test_pattern_with_no_literal_head_omits_prefix_filter(self, backend):
        """A pattern opening with a metacharacter sends no begins_with at all.

        Mutation: building begins_with with an empty-string operand
        whenever a pattern is given - the engine accepts the empty
        operand and matches every key, so only the emitted request
        parameters can catch it.
        Oracle: the Scan parameters captured off the client's event bus.
        """
        backend.set('5m:a:fn|x=1', 'v1', 300)
        backend.set('5m:b:fn|x=2', 'v2', 300)

        captured = []
        backend.client.meta.events.register(
            'provide-client-params.dynamodb.Scan',
            lambda params, **kwargs: captured.append(params))

        assert backend.clear('*fn|*') == 2
        assert captured
        for params in captured:
            assert 'begins_with' not in params.get('FilterExpression', '')
            assert ':prefix' not in params.get('ExpressionAttributeValues', {})

    def test_consistent_reads_flag_reaches_the_wire(self, dynamodb_table):
        """consistent_reads=False is sent on every GetItem and Scan.

        Mutation: pinning ConsistentRead=True at any call site - the
        single-node DynamoDB Local answers identically either way, but
        real DynamoDB bills strongly consistent reads at double, so the
        README's cost lever must actually reach the wire.
        Oracle: the request parameters captured off the client's event bus.
        """
        b = DynamoDBBackend(TABLE_NAME, consistent_reads=False)
        captured = []
        for operation in ('GetItem', 'Scan'):
            b.client.meta.events.register(
                f'provide-client-params.dynamodb.{operation}',
                lambda params, **kwargs: captured.append(params))

        b.set('k', 'v', 300)
        b.get('k')
        b.count()
        b.count('5m:*')

        assert len(captured) >= 3
        assert all(params['ConsistentRead'] is False for params in captured)

    def test_stats_rows_invisible_to_keys_and_count(self, backend):
        """Stats rows never surface as cache keys.

        Mutation: dropping the freshness filter for the None pattern,
        which would count the expiry-less stats row as an entry.
        Oracle: hand-listed key set - one cache row beside one stats row.
        """
        backend.set('only-key', 'v', 300)
        backend.incr_stat('some_fn', 'hits')
        backend.get_stats('some_fn')

        assert list(backend.keys()) == ['only-key']
        assert backend.count() == 1

    def test_clear_all_wipes_stats_and_lock_rows_too(self, backend):
        """clear(None) empties the whole table - stats and locks included.

        Mutation: reusing the live-rows-only scan for clear, leaving the
        stats row standing after a full wipe; or the backend and the
        mutex hashing keys differently, so the scanned lock row's
        recomputed partition key misses the stored one.
        Oracle: hand-counted 3 deletions, zeroed stats, and a fresh
        single-attempt acquire succeeding on the wiped lock key.
        """
        backend.set('k1', 'v', 300)
        backend.incr_stat('some_fn', 'misses')
        backend.get_stats('some_fn')
        holder = backend.get_mutex('k1')
        assert holder.acquire(timeout=0) is True

        assert backend.clear() == 3
        assert backend.get_stats('some_fn') == (0, 0)
        taker = backend.get_mutex('k1')
        assert taker.acquire(timeout=0) is True


class TestDynamoDBMutexTokens:
    """The lock is exclusive, expirable, floored, and release is token-guarded.
    """

    def test_contended_lock_excludes_second_acquirer(self, backend):
        """A held lock rejects a second acquire until released.

        Mutation: dropping the attribute_not_exists condition, letting
        every put succeed and two callers hold one key.
        Oracle: single-attempt acquire outcomes before and after release.
        """
        m1 = DynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 10.0)
        m2 = DynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 10.0)

        assert m1.acquire(timeout=0) is True
        assert m2.acquire(timeout=0) is False

        m1.release()
        assert m2.acquire(timeout=0) is True

    def test_release_after_takeover_keeps_new_owner_locked(self, backend, bare_table):
        """An expired holder's release cannot free the lock's new owner.

        Mutation: an unconditional DeleteItem in release, or dropping the
        'expires_at < now' takeover clause from acquire.
        Oracle: a third caller's single-attempt acquire, which must still
        fail after the stale holder's release. On `bare_table` so the
        engine's TTL sweep cannot collect the expired row first, which
        would let m2 acquire fresh without exercising the takeover.
        """
        m1 = DynamoDBMutex(backend.client, bare_table, 'lock:k', 0.5)
        m2 = DynamoDBMutex(backend.client, bare_table, 'lock:k', 10.0)
        m3 = DynamoDBMutex(backend.client, bare_table, 'lock:k', 10.0)

        assert m1.acquire(timeout=0) is True
        time.sleep(1.1)
        assert m2.acquire(timeout=0) is True

        m1.release()
        assert m3.acquire(timeout=0) is False

    def test_sub_second_lock_timeout_still_excludes_waiters(self, backend):
        """The lock item outlives a sub-second lock_timeout by the 1s floor.

        Mutation: writing the lock expiry as now + lock_timeout with no
        floor, so a waiter's poll window always reaches the holder's
        expiry, every waiter takes over the live lock, and
        on_lock_timeout='raise' can never fire.
        Oracle: a waiter polling for 0.45s against a 0.3s lock_timeout,
        which must time out because the floored lifetime is 1s.
        """
        m1 = DynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 0.3)
        m2 = DynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 0.3)

        assert m1.acquire(timeout=0) is True
        assert m2.acquire(timeout=0.45) is False

    def test_racing_acquirers_produce_exactly_one_holder(self, backend):
        """Simultaneous acquirers on one key produce exactly one winner.

        Mutation: swapping the conditional put for a read-then-put -
        the sequential exclusion test above still passes, because the
        first write is already visible by the time the second caller
        reads, and only a genuine race exposes the lost condition.
        Oracle: winner count across eight threads released together
        against an engine that serializes conditional writes.
        """
        mutexes = [
            DynamoDBMutex(backend.client, TABLE_NAME, 'lock:race', 30.0)
            for _ in range(8)
            ]
        barrier = threading.Barrier(8)

        def race(mutex):
            barrier.wait()
            return mutex.acquire(timeout=0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(race, mutexes))
        assert results.count(True) == 1

    async def test_async_contended_lock_excludes_second_acquirer(self, backend):
        """The async lock is exclusive and released by its owner.

        Mutation: AsyncDynamoDBMutex.acquire never acquiring (silently
        eating the wait) or release being a no-op - neither is visible to
        the decorator suites, whose fail-open path swallows mutex faults.
        Oracle: single-attempt acquire outcomes before and after release.
        """
        m1 = AsyncDynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 10.0)
        m2 = AsyncDynamoDBMutex(backend.client, TABLE_NAME, 'lock:k', 10.0)

        assert await m1.acquire(timeout=0) is True
        assert await m2.acquire(timeout=0) is False

        await m1.release()
        assert await m2.acquire(timeout=0) is True

    async def test_async_release_after_takeover_keeps_new_owner_locked(self, backend, bare_table):
        """The async release is token-guarded exactly as the sync one.

        Mutation: an unconditional delete in the delegated release path.
        Oracle: a third caller's single-attempt acquire after the stale
        holder's release. On `bare_table` for the same TTL-sweep reason
        as the sync takeover test.
        """
        m1 = AsyncDynamoDBMutex(backend.client, bare_table, 'lock:k', 0.5)
        m2 = AsyncDynamoDBMutex(backend.client, bare_table, 'lock:k', 10.0)
        m3 = AsyncDynamoDBMutex(backend.client, bare_table, 'lock:k', 10.0)

        assert await m1.acquire(timeout=0) is True
        await asyncio.sleep(1.1)
        assert await m2.acquire(timeout=0) is True

        await m1.release()
        assert await m3.acquire(timeout=0) is False

    def test_get_async_mutex_returns_async_type(self, backend):
        """The async mutex accessor returns an awaitable mutex.

        Mutation: get_async_mutex returning the sync DynamoDBMutex, whose
        awaited bool raises TypeError that the decorator's fail-open
        handler swallows - every async call then runs with no dogpile
        lock at all.
        Oracle: the type contract itself.
        """
        assert isinstance(backend.get_async_mutex('k'), AsyncDynamoDBMutex)


class TestStatsBuffering:
    """Stat increments are buffered locally and flushed to shared storage.
    """

    def test_flushed_stats_visible_to_another_instance(self, backend, dynamodb_table):
        """Counts survive the buffering and land in DynamoDB.

        Mutation: get_stats reading only the local buffer, or the flush
        never writing - counts would be process-local, unlike every other
        shared backend's stats.
        Oracle: a second backend instance (fresh buffer) reading the same
        counters.
        """
        backend.incr_stat('fn', 'hits')
        backend.incr_stat('fn', 'hits')
        backend.incr_stat('fn', 'misses')

        assert backend.get_stats('fn') == (2, 1)
        other = DynamoDBBackend(TABLE_NAME)
        assert other.get_stats('fn') == (2, 1)

    def test_stat_deltas_merge_across_instances(self, backend):
        """Two instances' flushes accumulate on the shared counter row.

        Mutation: flushing with SET instead of ADD in the update
        expression - each flush then overwrites the other instance's
        counts, and the single-writer test above cannot tell.
        Oracle: hand-computed totals across two instances flushing in
        turn - 2 hits and 1 miss, where overwriting would report 1 of
        each.
        """
        other = DynamoDBBackend(TABLE_NAME)
        backend.incr_stat('fn', 'hits')
        backend.get_stats('fn')
        other.incr_stat('fn', 'hits')
        other.incr_stat('fn', 'misses')

        assert other.get_stats('fn') == (2, 1)
        assert backend.get_stats('fn') == (2, 1)

    def test_close_flushes_buffered_stats(self, dynamodb_table):
        """close() pushes pending deltas before releasing the client.

        Mutation: dropping the flush from close, losing every count since
        the last read in a short-lived process.
        Oracle: a fresh instance reading counts written only via close.
        """
        b = DynamoDBBackend(TABLE_NAME)
        b.incr_stat('fn2', 'hits')
        b.close()

        assert DynamoDBBackend(TABLE_NAME).get_stats('fn2') == (1, 0)


class TestBatchDelete:
    """clear() retries the documented UnprocessedItems throttling channel.
    """

    def test_unprocessed_items_are_retried(self, backend, monkeypatch):
        """A throttled batch is retried until its deletes land.

        Mutation: returning after the first UnprocessedItems response,
        leaving rows alive after an explicit invalidation and serving
        stale values the caller just cleared.
        Oracle: a stub client that reports the whole first batch
        unprocessed, then delegates; all rows must still be deleted.
        """
        for i in range(3):
            backend.set(f'k{i}', i, 300)

        client = backend.client
        original = client.batch_write_item
        state = {'calls': 0}

        def flaky(**kwargs):
            state['calls'] += 1
            if state['calls'] == 1:
                return {'UnprocessedItems': dict(kwargs['RequestItems'])}
            return original(**kwargs)

        monkeypatch.setattr(client, 'batch_write_item', flaky)

        assert backend.clear('k*') == 3
        assert state['calls'] >= 2
        assert backend.count() == 0


class TestScanPagination:
    """Every scan path walks all pages, not just the engine's first MB.
    """

    def test_scan_paths_see_rows_past_the_first_page(self, backend):
        """keys(), count() and clear() cover rows beyond the 1 MB page cap.

        Mutation: replacing any paginator with a single Scan call -
        rows on the second page silently vanish from keys(), count()
        and clear(), and small-table tests never notice.
        Oracle: 40 hand-written rows of 50 KB (about 2 MB, at least
        two pages against the engine's documented 1 MB Scan limit),
        every one of which must be seen by each path.
        """
        payload = 'x' * 50_000
        for i in range(40):
            backend.set(f'page-key-{i:02d}', payload, 300)

        assert backend.count() == 40
        assert sorted(backend.keys()) == [
            f'page-key-{i:02d}' for i in range(40)]
        assert backend.clear('page-key-*') == 40
        assert backend.count() == 0


class TestItemSizeCap:
    """DynamoDB's 400 KB item cap surfaces out of set() instead of vanishing.
    """

    def test_oversized_value_error_escapes_set(self, backend):
        """A value past the item cap raises; a smaller one on the path lands.

        Mutation: wrapping the put in a swallow that turns the
        documented 'oversized writes raise inside the backend' contract
        into a silent no-write, which the decorator's best-effort write
        handling would then hide forever.
        Oracle: the engine's own 400 KB ValidationException for a
        500 KB value, straddled by a 100 KB value that must succeed.
        """
        backend.set('fits', 'y' * 100_000, 300)
        assert backend.get('fits') == 'y' * 100_000

        with pytest.raises(ClientError):
            backend.set('too-big', 'x' * 500_000, 300)
        assert backend.get('too-big') is NO_VALUE


class TestCreateTable:
    """create_dynamodb_table provisions, verifies, and is idempotent.
    """

    def test_idempotent_and_fully_configured(self, backend, dynamodb_table):
        """A second run leaves a correctly configured table in place.

        Mutation: neutering the update_time_to_live call (nothing else in
        the suite asserts TTL), pointing it at the wrong attribute, or
        losing idempotency so deploy-script reruns crash.
        Oracle: DescribeTable and DescribeTimeToLive after a second
        create_dynamodb_table call - schema, billing mode, TTL attribute.
        """
        create_dynamodb_table(TABLE_NAME)

        table = backend.client.describe_table(TableName=TABLE_NAME)['Table']
        assert table['KeySchema'] == [{'AttributeName': 'key', 'KeyType': 'HASH'}]
        assert table['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'

        ttl_state = backend.client.describe_time_to_live(
            TableName=TABLE_NAME)['TimeToLiveDescription']
        assert ttl_state['TimeToLiveStatus'] == 'ENABLED'
        assert ttl_state['AttributeName'] == 'expires_ttl'

    def test_rejects_existing_table_with_foreign_schema(self, backend):
        """An existing table with a different key schema is refused.

        Mutation: swallowing ResourceInUseException without checking the
        schema, accepting a table where every runtime call then fails
        with a key-mismatch ValidationException that fail_open hides.
        Oracle: a hand-created composite-key table of the same name.
        """
        backend.client.create_table(
            TableName='foreign-table',
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'},
                ],
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
                ])

        with pytest.raises(RuntimeError):
            create_dynamodb_table('foreign-table')

    def test_rejects_ttl_enabled_on_a_different_attribute(self, backend):
        """TTL already enabled on a foreign attribute is an error, not a pass.

        Mutation: blanket-swallowing ValidationException from
        UpdateTimeToLive - AWS raises the same code for 'already enabled'
        and 'enabled on a DIFFERENT attribute', and the latter means
        cachu's rows are never garbage-collected.
        Oracle: a table whose TTL points at 'other_expiry'.
        """
        backend.client.create_table(
            TableName='ttl-table',
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                {'AttributeName': 'key', 'AttributeType': 'S'},
                ],
            KeySchema=[
                {'AttributeName': 'key', 'KeyType': 'HASH'},
                ])
        backend.client.get_waiter('table_exists').wait(TableName='ttl-table')
        backend.client.update_time_to_live(
            TableName='ttl-table',
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'other_expiry',
                })

        with pytest.raises(RuntimeError):
            create_dynamodb_table('ttl-table')


class TestDynamoDBConfiguration:
    """The configuration knobs validate and reach the backend.
    """

    def test_invalid_table_name_rejected(self):
        """configure() rejects an empty or non-string table name.

        Mutation: dropping the dynamodb_table check, deferring the fault
        to the first cache operation where fail_open swallows it.
        Oracle: ConfigurationError at configure() time.
        """
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_table='', package='ddbtest')
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_table=123, package='ddbtest')

    def test_invalid_timeout_and_retry_rejected(self):
        """configure() rejects non-positive timeouts and bool retry counts.

        Mutation: leaving the dynamodb names out of the numeric validation
        buckets, so a True retry count passes as 1.
        Oracle: ConfigurationError for 0, NaN, True and -1.
        """
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_timeout=0, package='ddbtest')
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_timeout=float('nan'), package='ddbtest')
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_retry_count=True, package='ddbtest')
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_retry_count=-1, package='ddbtest')

    def test_non_bool_consistent_reads_rejected(self):
        """configure() rejects a truthy string for dynamodb_consistent_reads.

        Mutation: skipping the bool check - botocore rejects a non-bool
        ConsistentRead on every read, which fail_open converts into a
        silent 100% miss while writes keep succeeding.
        Oracle: ConfigurationError at configure() time for 'yes' and 1.
        """
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_consistent_reads='yes', package='ddbtest')
        with pytest.raises(ConfigurationError):
            cachu.configure(dynamodb_consistent_reads=1, package='ddbtest')

    def test_configured_values_reach_the_backend(self, dynamodb_table):
        """Backend construction consumes the package's dynamodb settings.

        Mutation: the manager's dynamodb branch passing positional config
        values in the wrong order, e.g. region where the endpoint goes.
        Oracle: the configured values, read back off the live instance.
        """
        cachu.configure(
            dynamodb_table=TABLE_NAME,
            dynamodb_region='eu-west-1',
            dynamodb_endpoint_url='http://localhost:8000',
            dynamodb_timeout=2.5,
            dynamodb_retry_count=1,
            dynamodb_consistent_reads=False,
            package='ddbtest',
        )

        backend = cachu.get_backend('dynamodb', package='ddbtest', ttl=300)

        assert backend._table_name == TABLE_NAME
        assert backend._region_name == 'eu-west-1'
        assert backend._endpoint_url == 'http://localhost:8000'
        assert backend._timeout == pytest.approx(2.5)
        assert backend._retry_count == 1
        assert backend._consistent_reads is False

    def test_impossible_deadline_warns_with_the_arithmetic(self, caplog):
        """A deadline the botocore budgets dwarf logs the numbers.

        Mutation: the dynamodb branch skipping the deadline warning, or
        omitting botocore's inter-retry backoff from the estimate - a
        deadline in the gap would silently look honorable.
        Oracle: hand-computed floor - 5.0 * (1 + 3) = 20s of timeouts
        plus 1 + 2 + 4 = 7s of standard-mode backoff - in the message.
        """
        cachu.configure(
            backend_default='dynamodb',
            cache_deadline=1.0,
            dynamodb_timeout=5.0,
            dynamodb_retry_count=3,
            package='ddbtest',
        )

        with caplog.at_level(logging.WARNING, logger='cachu.manager'):
            cachu.get_backend('dynamodb', package='ddbtest', ttl=300)

        messages = [r.message for r in caplog.records]
        assert any(
            'cannot be honored' in m and '20s' in m and 'up to 7s' in m
            for m in messages)


class TestCachedCount:
    """cache_info is answered from the count memo, not a Scan per call.
    """

    def test_cache_info_scans_once_per_memo_window(self, dynamodb_table):
        """Repeated cache_info calls cost one table Scan, not one each.

        Mutation: falling through to count() per call - each cache_info
        would bill a full-table read, saturating the table under a polled
        stats endpoint.
        Oracle: Scan requests captured off the client's event bus across
        two back-to-back cache_info calls.
        """
        @cachu.cache(ttl=300, backend='dynamodb', package='ddbtest')
        def fn(x: int) -> int:
            return x

        fn(1)
        backend = cachu.get_backend('dynamodb', package='ddbtest', ttl=300)
        scans = []
        backend.client.meta.events.register(
            'provide-client-params.dynamodb.Scan',
            lambda params, **kwargs: scans.append(params))

        info_first = cachu.cache_info(fn)
        info_second = cachu.cache_info(fn)

        assert info_first.currsize == 1
        assert info_second.currsize == 1
        assert len(scans) == 1
