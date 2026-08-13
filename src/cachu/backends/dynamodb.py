"""DynamoDB cache backend implementation.

One table holds three kinds of items, distinguished by key prefix exactly
as the Redis backend distinguishes them within one logical DB: cache rows
(cachu's mangled keys), dogpile lock rows ('lock:...'), and stats rows
('cachu:stats:...').

Notes
-----
- The partition key is the SHA-256 hex digest of the cache key, with the
  readable key kept in the 'key_text' attribute. DynamoDB caps a
  partition key value at 2048 UTF-8 bytes, and cachu keys inline rendered
  call arguments, so raw keys cannot be the partition key: a long-argument
  call would fail every operation with ValidationException, which
  `fail_open` would hide as a permanent miss. Pattern operations match
  against 'key_text'.
- DynamoDB's native TTL is garbage collection only: AWS documents deletion
  as lazy ("typically within two days"), and expired items keep appearing
  in reads and scans until collected. Every read therefore re-checks
  'expires_at' itself and treats a stale row as a miss, evicting it. The
  TTL process is documented against integer epoch seconds, so rows carry a
  separate integer 'expires_ttl' attribute (the ceiling of 'expires_at')
  for the collector; correctness never depends on it.
- There is no server-side pattern matching, and a Scan bills every item it
  evaluates: FilterExpression and ProjectionExpression apply AFTER the
  read, so the `begins_with` narrowing bounds the payload returned and the
  client-side matching, never the read cost. fnmatch is the authority on
  the full pattern.
"""
import asyncio
import fnmatch
import hashlib
import logging
import math
import pickle
import random
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Literal

from ..api import NO_VALUE, Backend
from ..mutex import AsyncCacheMutex, AsyncDynamoDBMutex, CacheMutex
from ..mutex import DynamoDBMutex

logger = logging.getLogger(__name__)

_KEY_ATTR = 'key'
_KEY_TEXT_ATTR = 'key_text'
_VALUE_ATTR = 'value'
_CREATED_ATTR = 'created_at'
_EXPIRES_ATTR = 'expires_at'
_EXPIRES_TTL_ATTR = 'expires_ttl'
_STATS_KEY_PREFIX = 'cachu:stats:'
_BATCH_WRITE_SIZE = 25
_BATCH_WRITE_RETRIES = 8
_BATCH_RETRY_BASE_DELAY = 0.05
_BATCH_RETRY_MAX_DELAY = 2.0
_GLOB_METACHARS = '*?['
_MAX_POOL_CONNECTIONS = 32
_COUNT_FRESH_TTL = 60.0
_STATS_FLUSH_THRESHOLD = 100


def _get_boto3_modules() -> tuple[Any, Any]:
    """Import boto3 and botocore.config, raising helpful error if not installed.
    """
    try:
        import boto3
        import botocore.config
        return boto3, botocore.config
    except ImportError as e:
        raise RuntimeError(
            "DynamoDB support requires the 'boto3' package. Install with: pip install cachu[dynamodb]"
        ) from e


def _pk(key: str) -> str:
    """Partition key for a cache key: its SHA-256 hex digest.

    Notes
    -----
    - Hashing every key rather than only over-long ones keeps one uniform
      row shape; `DynamoDBMutex` computes the same digest for lock rows.
    """
    return hashlib.sha256(key.encode()).hexdigest()


def create_dynamodb_table(
    table_name: str,
    region_name: str | None = None,
    endpoint_url: str | None = None,
) -> None:
    """Create the cache table and enable native TTL on it, idempotently.

    Parameters
    ----------
    table_name : str
        Table to create.
    region_name : str or None, default None
        AWS region; boto3's default resolution chain when None.
    endpoint_url : str or None, default None
        Endpoint override, e.g. a DynamoDB Local URL.

    Raises
    ------
    RuntimeError
        If a table of this name exists with a different key schema, or
        has native TTL enabled on an attribute other than 'expires_ttl'.

    Notes
    -----
    - On-demand billing, one string HASH key 'key' (the SHA-256 digest of
      the cache key), no indexes: the digest is the whole primary key and
      nothing else is ever queried by.
    - Native TTL on 'expires_ttl' is garbage collection only; the backend
      never trusts it and re-checks expiry on every read. Lock items carry
      the same attribute, so abandoned locks are collected too.
    - Idempotency is verified rather than assumed. An existing table has
      its key schema checked, and TTL state is read via DescribeTimeToLive
      before any update: AWS answers UpdateTimeToLive with the same
      ValidationException whether TTL is already enabled on this attribute
      or on a DIFFERENT one, so a blanket swallow would silently accept a
      table whose cachu rows are never collected.
    - Needs CreateTable, DescribeTable, DescribeTimeToLive and
      UpdateTimeToLive permissions; the backend itself needs only GetItem,
      PutItem, DeleteItem, UpdateItem, Scan and BatchWriteItem.
    """
    boto3, _ = _get_boto3_modules()
    client = boto3.session.Session().client(
        'dynamodb', region_name=region_name, endpoint_url=endpoint_url)
    expected_schema = [{'AttributeName': _KEY_ATTR, 'KeyType': 'HASH'}]
    try:
        client.create_table(
            TableName=table_name,
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                {'AttributeName': _KEY_ATTR, 'AttributeType': 'S'},
                ],
            KeySchema=expected_schema,
        )
    except client.exceptions.ResourceInUseException:
        schema = client.describe_table(TableName=table_name)['Table']['KeySchema']
        if schema != expected_schema:
            raise RuntimeError(
                f'Table {table_name!r} exists with key schema {schema!r}; '
                f"cachu requires a single HASH key named '{_KEY_ATTR}'")
    client.get_waiter('table_exists').wait(TableName=table_name)

    ttl_state = client.describe_time_to_live(
        TableName=table_name)['TimeToLiveDescription']
    status = ttl_state.get('TimeToLiveStatus', 'DISABLED')
    attribute = ttl_state.get('AttributeName')
    if status in ('ENABLED', 'ENABLING'):
        if attribute != _EXPIRES_TTL_ATTR:
            raise RuntimeError(
                f'Table {table_name!r} has native TTL on {attribute!r}; cachu '
                f'rows carry {_EXPIRES_TTL_ATTR!r} and would never be '
                f'garbage-collected')
        return

    try:
        client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': _EXPIRES_TTL_ATTR,
                },
        )
    except client.exceptions.ClientError:
        # A concurrent deploy may have enabled TTL inside the one-hour
        # window in which AWS rejects further UpdateTimeToLive calls;
        # accept the outcome only if it is the one this helper wanted.
        ttl_state = client.describe_time_to_live(
            TableName=table_name)['TimeToLiveDescription']
        if (ttl_state.get('TimeToLiveStatus') in ('ENABLED', 'ENABLING')
                and ttl_state.get('AttributeName') == _EXPIRES_TTL_ATTR):
            return
        raise


class DynamoDBBackend(Backend):
    """Unified DynamoDB cache backend with both sync and async interfaces.

    Parameters
    ----------
    table_name : str
        Existing table with a single string HASH key named 'key'; see
        `create_dynamodb_table` for the exact schema.
    lock_timeout : float, default 10.0
        Seconds after which an unreleased dogpile lock may be taken over.
    region_name : str or None, default None
        AWS region; boto3's default resolution chain when None.
    endpoint_url : str or None, default None
        Endpoint override, e.g. a DynamoDB Local URL.
    timeout : float, default 5.0
        Seconds passed to botocore as both connect_timeout and
        read_timeout; each retry attempt gets its own budget.
    retry_count : int, default 3
        Retry attempts botocore makes per operation on top of the first.
    consistent_reads : bool, default True
        Use strongly consistent GetItem/Scan. True keeps read-your-writes
        semantics per item at twice the read cost; False halves the cost
        and tolerates seconds of staleness. Either way a paginated Scan
        has no snapshot isolation: consistency is per item, and items
        written mid-scan may be half-seen.

    Notes
    -----
    - The async interface wraps the sync one in `asyncio.to_thread`, which
      runs on the event loop's SHARED default executor: a wedged endpoint
      holds one of its threads for up to `timeout * (1 + retry_count)`
      seconds, so size `dynamodb_timeout` for the latency budget, not just
      the happy path. boto3 has no async client, and botocore clients are
      thread-safe for concurrent method calls.
    - A pickled value plus its attributes must fit DynamoDB's 400 KB item
      cap. An oversized `set` raises inside the backend; the decorator
      treats every cache write as best-effort and logs the fault,
      whichever way `fail_open` is set, so the call returns its result
      uncached.
    - Stats increments are buffered in-process and flushed as atomic ADD
      updates on `get_stats`/`clear_stats`/`close` or every
      `_STATS_FLUSH_THRESHOLD` increments: an UpdateItem per cache hit
      would bill a write per read and serialize the region's whole
      traffic on one stats item (a single item sustains roughly 1000
      writes/s). Counters merge correctly across processes; reads see
      other processes' counts only after their flushes.
    - Reads treat expiry as authoritative client-side, because native TTL
      deletes lazily; see the module docstring.
    """

    def __init__(
        self,
        table_name: str,
        lock_timeout: float = 10.0,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        timeout: float = 5.0,
        retry_count: int = 3,
        consistent_reads: bool = True,
    ) -> None:
        self._table_name = table_name
        self._lock_timeout = lock_timeout
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._timeout = timeout
        self._retry_count = retry_count
        self._consistent_reads = consistent_reads
        self._client: Any = None
        self._init_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._stats_buffer: dict[tuple[str, str], int] = {}
        self._stats_buffered_total = 0
        self._count_lock = threading.Lock()
        self._count_cache: dict[str | None, tuple[int, float]] = {}

    @property
    def client(self) -> Any:
        """Lazy-load the DynamoDB client (built once, under a lock).

        Notes
        -----
        - A dedicated Session per backend rather than boto3's module-level
          default: the default session is not thread-safe during client
          construction, and two backends configured for different regions
          must not share credential-resolution state.
        - `total_max_attempts` rather than `max_attempts`: in client
          config, botocore reads `max_attempts` as retries EXCLUDING the
          initial request and adds one, so `max_attempts=retry_count + 1`
          would buy an extra unwanted retry.
        - The pool is sized to `asyncio.to_thread`'s default executor
          (min(32, cpus + 4) threads); botocore's default of 10 discards
          connections under async fan-out, forcing TCP + TLS handshakes
          onto the hot path.
        """
        if self._client is None:
            with self._init_lock:
                if self._client is None:
                    boto3, botocore_config = _get_boto3_modules()
                    config = botocore_config.Config(
                        connect_timeout=self._timeout,
                        read_timeout=self._timeout,
                        retries={
                            'total_max_attempts': self._retry_count + 1,
                            'mode': 'standard',
                            },
                        max_pool_connections=_MAX_POOL_CONNECTIONS,
                    )
                    self._client = boto3.session.Session().client(
                        'dynamodb',
                        region_name=self._region_name,
                        endpoint_url=self._endpoint_url,
                        config=config)
        return self._client

    def _scan_keys(self, pattern: str | None, live_only: bool) -> list[str]:
        """Collect key texts matching `pattern`, optionally only unexpired rows.

        Parameters
        ----------
        pattern : str or None
            fnmatch-style glob; None matches every key.
        live_only : bool
            Keep only rows whose 'expires_at' has not passed. Rows without
            the attribute (stats rows) never pass this filter, because a
            DynamoDB comparison against a missing attribute is false.

        Returns
        -------
        list of str
            Matching key texts, materialized so callers can iterate or
            delete without holding a live paginator.

        Notes
        -----
        - The pattern's literal prefix (everything before its first glob
          metacharacter) is pushed down as a `begins_with` filter on
          'key_text'. That bounds the payload returned and the client-side
          matching - never the items read or billed, since a Scan filter
          applies after the read. fnmatch remains the authority on the
          full pattern, so escaped-metacharacter classes like '[*]' still
          match literally.
        """
        names = {'#kt': _KEY_TEXT_ATTR}
        values: dict[str, Any] = {}
        filters = []
        if live_only:
            names['#e'] = _EXPIRES_ATTR
            values[':now'] = {'N': str(time.time())}
            filters.append('#e >= :now')
        prefix = ''
        if pattern:
            meta_positions = [
                i for i, ch in enumerate(pattern) if ch in _GLOB_METACHARS]
            prefix = pattern[:meta_positions[0]] if meta_positions else pattern
        if prefix:
            values[':prefix'] = {'S': prefix}
            filters.append('begins_with(#kt, :prefix)')

        kwargs: dict[str, Any] = {
            'TableName': self._table_name,
            'ProjectionExpression': '#kt',
            'ExpressionAttributeNames': names,
            'ConsistentRead': self._consistent_reads,
            }
        if filters:
            kwargs['FilterExpression'] = ' AND '.join(filters)
            kwargs['ExpressionAttributeValues'] = values

        keys = []
        for page in self.client.get_paginator('scan').paginate(**kwargs):
            for item in page.get('Items', []):
                key = item.get(_KEY_TEXT_ATTR, {}).get('S')
                if key is None:
                    continue
                if pattern is None or fnmatch.fnmatchcase(key, pattern):
                    keys.append(key)
        return keys

    def _batch_delete(self, keys: list[str]) -> int:
        """Delete keys via BatchWriteItem. Returns count of acknowledged deletes.

        Notes
        -----
        - UnprocessedItems is the documented throttling channel, not an
          error, and AWS prescribes exponential backoff for it - a bulk
          clear on a fresh on-demand table is the classic trigger. A chunk
          still unprocessed after the retries is logged and excluded from
          the count rather than raised, so a clear that removed most of a
          region reports what it did.
        - A DeleteRequest reports nothing about whether the row existed,
          so the count covers rows the scan saw and DynamoDB acknowledged;
          a row another process removed in between is still counted.
        """
        deleted = 0
        for start in range(0, len(keys), _BATCH_WRITE_SIZE):
            chunk = keys[start:start + _BATCH_WRITE_SIZE]
            requests = [
                {'DeleteRequest': {'Key': {_KEY_ATTR: {'S': _pk(key)}}}}
                for key in chunk
                ]
            for attempt in range(_BATCH_WRITE_RETRIES):
                response = self.client.batch_write_item(
                    RequestItems={self._table_name: requests})
                requests = response.get('UnprocessedItems', {}).get(
                    self._table_name, [])
                if not requests:
                    deleted += len(chunk)
                    break
                delay = min(
                    _BATCH_RETRY_BASE_DELAY * (2 ** attempt),
                    _BATCH_RETRY_MAX_DELAY)
                time.sleep(delay * (0.5 + random.random() / 2))
            else:
                deleted += len(chunk) - len(requests)
                logger.warning(
                    f'{len(requests)} deletes still unprocessed after '
                    f'{_BATCH_WRITE_RETRIES} attempts on {self._table_name}')
        return deleted

    def _evict_expired(self, key: str) -> None:
        """Delete a row observed expired, unless someone rewrote it since.

        Notes
        -----
        - The condition re-checks expiry at delete time: between the read
          that observed the expired row and this delete, another process's
          `set` can land - at exactly the dogpile moment - and an
          unconditional delete would destroy that fresh write.
        """
        try:
            self.client.delete_item(
                TableName=self._table_name,
                Key={_KEY_ATTR: {'S': _pk(key)}},
                ConditionExpression='#e < :now',
                ExpressionAttributeNames={'#e': _EXPIRES_ATTR},
                ExpressionAttributeValues={':now': {'N': str(time.time())}})
        except self.client.exceptions.ConditionalCheckFailedException:
            pass

    def _flush_stats(self) -> None:
        """Push buffered stat deltas to DynamoDB as atomic ADD updates.

        Notes
        -----
        - A delta whose update fails is re-queued before the error
          propagates, so a transient fault delays counts rather than
          losing them.
        """
        with self._stats_lock:
            pending = dict(self._stats_buffer)
            self._stats_buffer.clear()
            self._stats_buffered_total = 0
        for (fn_name, stat), delta in pending.items():
            try:
                self.client.update_item(
                    TableName=self._table_name,
                    Key={_KEY_ATTR: {'S': _pk(f'{_STATS_KEY_PREFIX}{fn_name}')}},
                    UpdateExpression='ADD #s :delta SET #kt = :kt',
                    ExpressionAttributeNames={
                        '#s': stat,
                        '#kt': _KEY_TEXT_ATTR,
                        },
                    ExpressionAttributeValues={
                        ':delta': {'N': str(delta)},
                        ':kt': {'S': f'{_STATS_KEY_PREFIX}{fn_name}'},
                        })
            except Exception:
                with self._stats_lock:
                    buffer_key = (fn_name, stat)
                    self._stats_buffer[buffer_key] = (
                        self._stats_buffer.get(buffer_key, 0) + delta)
                    self._stats_buffered_total += delta
                raise

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found, expired or corrupted.
        """
        return self.get_with_metadata(key)[0]

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.

        Notes
        -----
        - A row that is expired, missing its attributes, or holding an
          undecodable pickle is a miss AND is evicted: native TTL will not
          collect it for days, and the usual cause of an undecodable row
          is a deploy that changed a pickled class while an older release
          still writes the same key. The expired case evicts
          conditionally so a concurrent rewrite survives.
        """
        response = self.client.get_item(
            TableName=self._table_name,
            Key={_KEY_ATTR: {'S': _pk(key)}},
            ProjectionExpression='#v, #c, #e',
            ExpressionAttributeNames={
                '#v': _VALUE_ATTR,
                '#c': _CREATED_ATTR,
                '#e': _EXPIRES_ATTR,
                },
            ConsistentRead=self._consistent_reads)
        item = response.get('Item')
        if item is None:
            return NO_VALUE, None

        value_attr = item.get(_VALUE_ATTR, {}).get('B')
        created_attr = item.get(_CREATED_ATTR, {}).get('N')
        expires_attr = item.get(_EXPIRES_ATTR, {}).get('N')
        if value_attr is None or created_attr is None or expires_attr is None:
            logger.warning(f'Evicting malformed cache row for key {key!r}')
            self.delete(key)
            return NO_VALUE, None

        if time.time() > float(expires_attr):
            self._evict_expired(key)
            return NO_VALUE, None

        try:
            value = pickle.loads(value_attr)
        except Exception:
            logger.warning(
                f'Evicting undecodable cache row for key {key!r}', exc_info=True)
            self.delete(key)
            return NO_VALUE, None
        return value, float(created_attr)

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds. A non-positive TTL is not cached.

        Notes
        -----
        - 'expires_at' carries the exact fractional expiry the reads
          check; 'expires_ttl' carries its ceiling as the integer epoch
          seconds AWS documents for the native TTL collector, so garbage
          collection never fires before the true expiry.
        """
        if ttl <= 0:
            self.delete(key)
            return
        now = time.time()
        expires = now + ttl
        self.client.put_item(
            TableName=self._table_name,
            Item={
                _KEY_ATTR: {'S': _pk(key)},
                _KEY_TEXT_ATTR: {'S': key},
                _VALUE_ATTR: {'B': pickle.dumps(value)},
                _CREATED_ATTR: {'N': str(now)},
                _EXPIRES_ATTR: {'N': str(expires)},
                _EXPIRES_TTL_ATTR: {'N': str(math.ceil(expires))},
                })

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        self.client.delete_item(
            TableName=self._table_name,
            Key={_KEY_ATTR: {'S': _pk(key)}})

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.

        Parameters
        ----------
        pattern : str or None, default None
            fnmatch-style glob to match. None means the ENTIRE table,
            stats and lock rows included, exactly as the Redis backend's
            None means its whole logical DB.

        Returns
        -------
        int
            Number of acknowledged deletes.

        Notes
        -----
        - Expired-but-uncollected rows matching the pattern are deleted
          and counted: a clear must not leave rows native TTL has not got
          to yet.
        - `cache_clear` never passes None: it derives a region-scoped glob
          from cachu's own key shape, so a library-level clear cannot
          reach a lock another caller holds or a key cachu did not write.
        """
        count = self._batch_delete(self._scan_keys(pattern, live_only=False))
        with self._count_lock:
            self._count_cache.clear()
        return count

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over live keys matching pattern.

        Notes
        -----
        - Only rows carrying an unexpired 'expires_at' appear: cache
          entries and live dogpile locks. Stats rows never carry one.
        - Yields the readable key text, not the hashed partition key.
        """
        yield from self._scan_keys(pattern, live_only=True)

    def count(self, pattern: str | None = None) -> int:
        """Count live keys matching pattern.

        Whole-table counts (pattern is None) use a Select=COUNT scan so no
        item data crosses the wire; the read cost is unchanged either way,
        since DynamoDB bills a Scan on the items evaluated, not returned.
        """
        if pattern is None:
            pages = self.client.get_paginator('scan').paginate(
                TableName=self._table_name,
                Select='COUNT',
                FilterExpression='#e >= :now',
                ExpressionAttributeNames={'#e': _EXPIRES_ATTR},
                ExpressionAttributeValues={':now': {'N': str(time.time())}},
                ConsistentRead=self._consistent_reads)
            return sum(page['Count'] for page in pages)
        return len(self._scan_keys(pattern, live_only=True))

    def cached_count(self, pattern: str | None = None) -> int:
        """Count live keys via an in-process memo refreshed every 60 seconds.

        Notes
        -----
        - `cache_info` calls this instead of `count`: a Scan bills the
          whole table, so a stats view polled per request must not pay it
          per call. The memo is per process (unlike the Redis
          currsize cache, which is shared through Redis itself), and is
          dropped by `clear` and `clear_stats`.
        """
        now = time.monotonic()
        with self._count_lock:
            cached = self._count_cache.get(pattern)
            if cached is not None and now - cached[1] < _COUNT_FRESH_TTL:
                return cached[0]
        count = self.count(pattern)
        with self._count_lock:
            self._count_cache[pattern] = (count, time.monotonic())
        return count

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return DynamoDBMutex(
            self.client, self._table_name, f'lock:{key}', self._lock_timeout)

    # ===== Stats interface (sync) =====

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Buffer a stat increment; see the class notes for flush points.
        """
        with self._stats_lock:
            buffer_key = (fn_name, stat)
            self._stats_buffer[buffer_key] = (
                self._stats_buffer.get(buffer_key, 0) + 1)
            self._stats_buffered_total += 1
            flush_now = self._stats_buffered_total >= _STATS_FLUSH_THRESHOLD
        if flush_now:
            self._flush_stats()

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function, flushing buffered deltas first.
        """
        self._flush_stats()
        response = self.client.get_item(
            TableName=self._table_name,
            Key={_KEY_ATTR: {'S': _pk(f'{_STATS_KEY_PREFIX}{fn_name}')}},
            ProjectionExpression='#h, #m',
            ExpressionAttributeNames={'#h': 'hits', '#m': 'misses'},
            ConsistentRead=self._consistent_reads)
        item = response.get('Item') or {}
        return (
            int(item.get('hits', {}).get('N', 0)),
            int(item.get('misses', {}).get('N', 0)),
        )

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.

        Notes
        -----
        - Buffered deltas for the cleared scope are dropped as well, or
          the next flush would resurrect counts the caller just cleared.
        - The cached counts are dropped too, mirroring the Redis backend's
          clear_stats, which drops its currsize cache.
        """
        with self._stats_lock:
            if fn_name:
                for stat in ('hits', 'misses'):
                    self._stats_buffered_total -= self._stats_buffer.pop(
                        (fn_name, stat), 0)
            else:
                self._stats_buffer.clear()
                self._stats_buffered_total = 0
        if fn_name:
            self.delete(f'{_STATS_KEY_PREFIX}{fn_name}')
        else:
            self._batch_delete(
                self._scan_keys(f'{_STATS_KEY_PREFIX}*', live_only=False))
        with self._count_lock:
            self._count_cache.clear()

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or corrupted.
        """
        return await asyncio.to_thread(self.get, key)

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        return await asyncio.to_thread(self.get_with_metadata, key)

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds. A non-positive TTL is not cached.
        """
        await asyncio.to_thread(self.set, key, value, ttl)

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        await asyncio.to_thread(self.delete, key)

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        return await asyncio.to_thread(self.clear, pattern)

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over live keys matching pattern.
        """
        for key in await asyncio.to_thread(self._scan_keys, pattern, True):
            yield key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count live keys matching pattern.
        """
        return await asyncio.to_thread(self.count, pattern)

    async def acached_count(self, pattern: str | None = None) -> int:
        """Async count live keys via the in-process 60-second memo.
        """
        return await asyncio.to_thread(self.cached_count, pattern)

    def get_async_mutex(self, key: str) -> AsyncCacheMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncDynamoDBMutex(
            self.client, self._table_name, f'lock:{key}', self._lock_timeout)

    # ===== Stats interface (async) =====

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async buffer a stat increment.
        """
        await asyncio.to_thread(self.incr_stat, fn_name, stat)

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        return await asyncio.to_thread(self.get_stats, fn_name)

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """
        await asyncio.to_thread(self.clear_stats, fn_name)

    # ===== Lifecycle =====

    def close(self) -> None:
        """Flush buffered stats, then release the client's connection pools.

        Notes
        -----
        - The flush runs whenever deltas are pending, even if it must
          build the client to do so: increments can be buffered before
          any other operation has, and dropping them on close would lose
          a short-lived process's entire count.
        """
        with self._stats_lock:
            has_pending = bool(self._stats_buffer)
        if has_pending:
            try:
                self._flush_stats()
            except Exception:
                logger.warning(
                    'Stats flush on close failed; buffered counts dropped',
                    exc_info=True)
        if self._client is not None:
            client = self._client
            self._client = None
            client.close()

    async def aclose(self) -> None:
        """Async close the backend and release resources.
        """
        await asyncio.to_thread(self.close)
