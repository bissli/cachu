"""Tests that cache_info degrades like the rest of cachu and stays cheap.

Notes
-----
- `cache_info` called `get_cache_info` with no guard, and `get_cache_info`
  performed three unguarded backend operations (`get_backend`, `get_stats`,
  `count`). A Redis fault therefore RAISED out of `cache_info` while every
  other entry point in the library degrades to a miss under `fail_open`.
- The async twin guarded only `currsize`, so `aget_stats` and backend
  construction still raised.
- `currsize` on Redis was a keyspace SCAN on every call, so a stats page
  paid one SCAN per decorated function per view. The async path already had
  a stale-while-revalidate cache in front of it; the sync path did not.
- The `count` glob was `*:<key_prefix><fn_name>|*`, whose leading `*` also
  matches `lock:<region>:<key_prefix><fn_name>|...`, so a live dogpile lock
  was counted as a cached entry.
"""
import threading

import cachu
import pytest
import redis as redis_lib
from cachu.api import CacheInfo
from cachu.backends.memory import MemoryBackend
from cachu.backends.redis import RedisBackend
from cachu.config import _get_caller_package
from cachu.decorator import _CURRSIZE_FRESH_TTL, _currsize_keys
from cachu.manager import manager
from cachu.util import mangle_key


def _boom(*args, **kwargs):
    """Stand in for a backend operation against a faulting store.
    """
    raise RuntimeError('backend exploded')


class TestCacheInfoHonorsFailOpen:
    """A backend fault costs a degraded CacheInfo, not an exception.
    """

    def test_a_stats_fault_yields_a_zeroed_info(self, monkeypatch):
        """fail_open=True turns a stats fault into zeros.

        Mutation: call get_cache_info unguarded (the 0.5.0 behavior), so a
        Redis fault raises out of cache_info while the same fault costs only
        a miss on the read path.
        Oracle: the CacheInfo dataclass, compared whole.
        """
        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        monkeypatch.setattr(MemoryBackend, 'get_stats', _boom)

        assert cachu.get_config().fail_open is True
        assert cachu.cache_info(fetch) == CacheInfo(hits=0, misses=0, currsize=0)

    def test_a_currsize_fault_keeps_the_counters_it_did_read(self, monkeypatch):
        """Partial degradation: hits and misses survive a failed count.

        Mutation: guard the whole body in one try/except, which discards
        counters the backend answered correctly and reports a live cache as
        entirely idle.
        Oracle: hand-counted 1 miss and 1 hit from two calls with one
        argument.
        """
        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        fetch(1)
        monkeypatch.setattr(MemoryBackend, 'count', _boom)

        info = cachu.cache_info(fetch)
        assert (info.hits, info.misses, info.currsize) == (1, 1, 0)

    def test_a_construction_fault_yields_a_zeroed_info(self, monkeypatch):
        """An unbuildable backend degrades rather than raising.

        Mutation: guard only the stats call, leaving `get_backend` - which
        `fail_open` already covers on the read path - able to raise.
        Oracle: the CacheInfo dataclass, compared whole.
        """
        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        monkeypatch.setattr(manager, 'get_backend', _boom)

        assert cachu.cache_info(fetch) == CacheInfo(hits=0, misses=0, currsize=0)

    def test_the_fault_propagates_when_fail_open_is_off(self, monkeypatch):
        """fail_open=False keeps the fault visible.

        Mutation: swallow unconditionally, which hides a broken cache from
        the caller who explicitly asked to see faults.
        Oracle: the sentinel error type raised by the stubbed stats call.
        """
        cachu.configure(fail_open=False, package=_get_caller_package())

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        monkeypatch.setattr(MemoryBackend, 'get_stats', _boom)

        with pytest.raises(RuntimeError, match='backend exploded'):
            cachu.cache_info(fetch)

    def test_an_undecorated_function_still_raises(self):
        """fail_open covers backend faults, not caller mistakes.

        Mutation: fold the ValueError into the fail_open guard, which turns a
        misuse into a silent CacheInfo(0, 0, 0) that reads as a cold cache.
        Oracle: the documented ValueError.
        """
        def plain(x: int) -> int:
            return x

        with pytest.raises(ValueError, match='not decorated'):
            cachu.cache_info(plain)


class TestAsyncCacheInfoHonorsFailOpen:
    """The async twin degrades on every step, not only on currsize.
    """

    async def test_a_stats_fault_yields_a_zeroed_info(self, monkeypatch):
        """aget_stats faults degrade to zeros.

        Mutation: guard currsize only (the 0.5.0 behavior), leaving
        aget_stats able to raise out of async_cache_info.
        Oracle: the CacheInfo dataclass, compared whole.
        """
        @cachu.cache(ttl=300, backend='memory')
        async def fetch(x: int) -> int:
            return x

        await fetch(1)

        async def aboom(*args, **kwargs):
            raise RuntimeError('backend exploded')

        monkeypatch.setattr(MemoryBackend, 'aget_stats', aboom)

        assert await cachu.async_cache_info(fetch) == CacheInfo(
            hits=0, misses=0, currsize=0)

    async def test_a_currsize_fault_keeps_the_counters_it_did_read(self, monkeypatch):
        """Partial degradation on the async path too.

        Mutation: zero the counters alongside currsize.
        Oracle: hand-counted 1 miss and 1 hit.
        """
        @cachu.cache(ttl=300, backend='memory')
        async def fetch(x: int) -> int:
            return x

        await fetch(1)
        await fetch(1)

        async def aboom(*args, **kwargs):
            raise RuntimeError('backend exploded')

        monkeypatch.setattr(MemoryBackend, 'acount', aboom)

        info = await cachu.async_cache_info(fetch)
        assert (info.hits, info.misses, info.currsize) == (1, 1, 0)

    async def test_the_fault_propagates_when_fail_open_is_off(self, monkeypatch):
        """fail_open=False keeps the async fault visible.

        Mutation: swallow unconditionally.
        Oracle: the sentinel error type.
        """
        cachu.configure(fail_open=False, package=_get_caller_package())

        @cachu.cache(ttl=300, backend='memory')
        async def fetch(x: int) -> int:
            return x

        await fetch(1)

        async def aboom(*args, **kwargs):
            raise RuntimeError('backend exploded')

        monkeypatch.setattr(MemoryBackend, 'aget_stats', aboom)

        with pytest.raises(RuntimeError, match='backend exploded'):
            await cachu.async_cache_info(fetch)


class TestCurrsizeCountsOnlyEntries:
    """The currsize glob is scoped to the region it reports on.
    """

    async def test_the_async_glob_is_region_scoped_too(self):
        """The async path counts its own region, not everything named alike.

        Mutation: leave the async glob as `*:<prefix><fn>|*`. Its leading
        '*' matches `lock:<region>:...` and every other TTL region, so an
        async stats view reports one entry as three while the sync one
        reports one - the asymmetry a sync-only fix would leave behind.
        Oracle: hand-counted entries, 1.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory')
        async def fetch(x: int) -> int:
            return x

        await fetch(1)

        backend = manager.get_backend(package, 'memory', 300)
        backend.set(f'lock:{mangle_key("fetch||x=1", "test:", 300)}', b'token', 300)
        backend.set(mangle_key('fetch||x=1', 'test:', 3600), 'stale', 300)

        info = await cachu.async_cache_info(fetch)
        assert info.currsize == 1

    def test_a_dogpile_lock_is_not_counted_as_an_entry(self):
        """currsize counts cached values, not the locks guarding them.

        Mutation: keep the leading-'*' glob `*:<prefix><fn>|*`, which also
        matches `lock:<region>:<prefix><fn>|...` and reports one entry as
        two.
        Oracle: hand-counted entries, 1.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        backend = manager.get_backend(package, 'memory', 300)
        backend.set(f'lock:{mangle_key("fetch||x=1", "test:", 300)}', b'token', 300)

        assert cachu.cache_info(fetch).currsize == 1

    def test_another_ttl_region_is_not_counted(self):
        """currsize reports the region the function actually reads.

        Mutation: keep the region wildcard, so entries left behind by a
        previous TTL of the same function - which this region can never
        serve - inflate the count.
        Oracle: hand-counted entries, 1.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='memory')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        backend = manager.get_backend(package, 'memory', 300)
        backend.set(mangle_key('fetch||x=1', 'test:', 3600), 'stale', 300)

        assert cachu.cache_info(fetch).currsize == 1

    def test_the_swr_cache_key_distinguishes_ttl_regions(self):
        """Two same-named functions in different regions cannot share a count.

        Mutation: key the stale-while-revalidate cache on
        (package, fn_name) alone, as it was before the glob became
        region-scoped. The cached count then answers for whichever region
        asked first, so one function reports the other's size for a whole
        fresh window - and a same-named function in another module of the
        same package collides too.
        Oracle: the three key triples, which must differ by region and by
        tag.
        """
        base = _currsize_keys('pkg', 'fetch', 300)

        assert _currsize_keys('pkg', 'fetch', 3600) != base
        assert _currsize_keys('pkg', 'fetch', 300, 'users') != base
        assert _currsize_keys('pkg', 'fetch', 300) == base

    def test_the_swr_cache_keys_share_one_cluster_slot(self):
        """Adding the region must not split the MGET across slots.

        Mutation: put the region outside the '{...}' hash tag. The three
        keys then hash to different Redis Cluster slots and the multi-key
        MGET in the SWR read becomes a CROSSSLOT error.
        Oracle: the braced substring of each key, which must be identical.
        """
        keys = _currsize_keys('pkg', 'fetch', 300, 'users')
        tags = {key[key.index('{'):key.index('}') + 1] for key in keys}

        assert len(tags) == 1

    def test_the_function_s_own_entries_are_counted(self):
        """Tightening the glob must not stop it matching.

        Mutation: scope to a glob that matches nothing, which would report
        every cache as empty.
        Oracle: hand-counted entries, 3 distinct arguments.
        """
        @cachu.cache(ttl=300, backend='memory', tag='users')
        def fetch(x: int) -> int:
            return x

        for value in (1, 2, 3):
            fetch(value)

        assert cachu.cache_info(fetch).currsize == 3


@pytest.mark.redis
class TestSyncCurrsizeIsNotAScanPerCall:
    """The sync path gets the same stale-while-revalidate cache as async.
    """

    @pytest.fixture
    def redis_url(self, redis_docker):
        """Build the Redis URL for direct access.
        """
        from _fixtures.redis import redis_test_config

        return f'redis://{redis_test_config.host}:{redis_test_config.port}/0'

    @pytest.fixture
    def client(self, redis_url):
        """An independent Redis client for seeding and reading SWR keys.
        """
        conn = redis_lib.Redis.from_url(redis_url)
        yield conn
        conn.close()

    @pytest.fixture
    def scans(self, monkeypatch):
        """Count the keyspace scans RedisBackend.count performs.
        """
        seen = []
        original = RedisBackend.count

        def counting(self, pattern=None):
            seen.append(pattern)
            return original(self, pattern)

        monkeypatch.setattr(RedisBackend, 'count', counting)
        return seen

    def test_repeated_views_scan_once_per_fresh_window(self, scans, client):
        """Three cache_info calls cost one SCAN, not three.

        Mutation: drop the SWR cache from the sync path, restoring a full
        SCAN per decorated function per view - the cost the reported
        consumer defended against with `asyncio.wait_for(..., timeout=2.0)`.
        Oracle: the recorded scan count, 1, with the same currsize returned
        every time.
        """
        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        fetch(2)

        first = cachu.cache_info(fetch).currsize
        second = cachu.cache_info(fetch).currsize
        third = cachu.cache_info(fetch).currsize

        assert (first, second, third) == (2, 2, 2)
        assert len(scans) == 1

    def test_the_last_known_value_is_served_while_a_peer_refreshes(self, scans, client):
        """A held refresh lock means stale-and-instant, never a second scan.

        Mutation: scan anyway when the lock is held, which lets N concurrent
        stats views each pay a full SCAN.
        Oracle: the pre-seeded last-known value, 99, and an empty scan list.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        fresh_key, last_key, lock_key = _currsize_keys(package, 'fetch', 300)
        client.delete(fresh_key)
        client.set(last_key, 99)
        client.set(lock_key, b'1', ex=30)

        assert cachu.cache_info(fetch).currsize == 99
        assert scans == []

    def test_a_fresh_value_is_served_without_scanning(self, scans, client):
        """The fresh key short-circuits the scan entirely.

        Mutation: ignore the fresh key and scan every time.
        Oracle: the pre-seeded fresh value, 42, and an empty scan list.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        fresh_key, _, _ = _currsize_keys(package, 'fetch', 300)
        client.set(fresh_key, 42, ex=_CURRSIZE_FRESH_TTL)

        assert cachu.cache_info(fetch).currsize == 42
        assert scans == []

    def test_concurrent_cold_views_run_one_scan(self, scans, client, monkeypatch):
        """Two callers hitting a cold currsize share one scan.

        Mutation: drop the `nx=True` from the refresh lock, so every
        concurrent stats view pays its own full SCAN - the stampede the lock
        exists to stop, and the one a dashboard reliably produces.
        Oracle: the recorded scan count, 1, with the second caller answered
        while the first still holds the lock. A barrier removes the race, so
        the count is exact rather than probabilistic.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        fresh_key, last_key, lock_key = _currsize_keys(package, 'fetch', 300)
        client.delete(fresh_key, last_key, lock_key)

        scanning = threading.Event()
        release = threading.Event()
        original = RedisBackend.count

        def slow_count(self, pattern=None):
            scanning.set()
            release.wait(timeout=5)
            return original(self, pattern)

        monkeypatch.setattr(RedisBackend, 'count', slow_count)
        scans.clear()

        winner = {}
        thread = threading.Thread(
            target=lambda: winner.setdefault('size', cachu.cache_info(fetch).currsize))
        thread.start()
        try:
            assert scanning.wait(timeout=5)
            assert cachu.cache_info(fetch).currsize == 0
        finally:
            release.set()
            thread.join(timeout=10)

        assert winner['size'] == 1
        assert len(scans) == 1

    def test_a_failed_scan_releases_the_refresh_lock(self, client, monkeypatch):
        """A raising SCAN must not freeze currsize for the lock's whole TTL.

        Mutation: release the lock only on success. currsize then serves the
        last-known value for `_CURRSIZE_LOCK_TTL` after a single transient
        fault, so a recovered cache still reports a stale size.
        Oracle: an independent client reading the lock key back after the
        fault, plus a second call that scans successfully.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)

        fresh_key, last_key, lock_key = _currsize_keys(package, 'fetch', 300)
        client.delete(fresh_key, last_key, lock_key)

        monkeypatch.setattr(RedisBackend, 'count', _boom)
        assert cachu.cache_info(fetch).currsize == 0
        assert client.get(lock_key) is None

        monkeypatch.undo()
        assert cachu.cache_info(fetch).currsize == 1

    def test_a_clear_drops_the_cached_size(self, scans, client):
        """cache_clear must not leave a phantom size behind for a minute.

        Mutation: leave the SWR keys standing on clear. Adding the cache in
        front of currsize would then make `cache_info` report the pre-clear
        size for up to `_CURRSIZE_FRESH_TTL` seconds - a regression against
        the uncached sync path, where a clear was immediately visible.
        Oracle: hand-counted entries, 2 before and 0 after the clear.
        """
        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        fetch(2)
        assert cachu.cache_info(fetch).currsize == 2

        assert cachu.cache_clear(backend='redis', ttl=300) == 2

        assert cachu.cache_info(fetch).currsize == 0

    def test_a_clear_leaves_a_held_refresh_lock_alone(self, client):
        """Invalidating the cached size must not release someone's lock.

        Mutation: sweep every 'cachu:_currsize*' key, lock included. A
        caller mid-scan then loses its exclusion and a second scan starts -
        the same fault as clearing a live dogpile mutex.
        Oracle: an independent client reading the lock key back after the
        clear.
        """
        package = _get_caller_package()

        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        _, _, lock_key = _currsize_keys(package, 'fetch', 300)
        client.set(lock_key, b'1', ex=30)

        cachu.cache_clear(backend='redis', ttl=300)

        assert client.get(lock_key) is not None

    def test_a_faulting_redis_still_reports_the_counters(self, monkeypatch):
        """A Redis fault during currsize does not cost the hit counters.

        Mutation: leave cache_info unguarded, so a stats page 500s the
        moment Redis blips.
        Oracle: hand-counted 1 miss and 1 hit, with currsize degraded to 0.
        """
        @cachu.cache(ttl=300, backend='redis')
        def fetch(x: int) -> int:
            return x

        fetch(1)
        fetch(1)

        monkeypatch.setattr(RedisBackend, 'count', _boom)

        info = cachu.cache_info(fetch)
        assert (info.hits, info.misses, info.currsize) == (1, 1, 0)
