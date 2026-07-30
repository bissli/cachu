"""Tests for the memory backend's LRU bound and expired-entry sweeping.

Notes
-----
- MemoryBackend had no maxsize and swept only inside keys()/count(), which
  nothing on the read path calls.
- An entry that expired and was never read again stayed resident until
  process exit, so a cache keyed on caller-influenced input - a credential
  hash, a tenant id, a search term - grew without bound.
"""
import asyncio
import gc
import time

import cachu
import pytest
from cachu.backends import memory as memory_module
from cachu.backends.memory import MemoryBackend
from cachu.mutex import AsyncioMutex, ThreadingMutex


def _expire(backend: MemoryBackend, key: str) -> None:
    """Rewrite an existing entry so it is already past its expiry.
    """
    value, created_at, _ = backend._cache[key]
    past = time.time() - 1.0
    backend._cache[key] = (value, created_at, past)


class TestUnboundedDefault:
    """The default construction keeps the historical unbounded behaviour.
    """

    def test_no_maxsize_keeps_every_live_entry(self):
        """Without maxsize, 500 live keys stay resident.

        Mutation: apply a default bound (e.g. maxsize=128) when none is asked
        for.
        Oracle: hand-counted insertion count, 500.
        """
        backend = MemoryBackend()
        for i in range(500):
            backend.set(f'k{i}', i, 300)

        assert backend.count() == 500
        assert backend.evictions == 0

    def test_live_entries_are_never_swept(self):
        """A sweep drops expired entries only, never live ones.

        Mutation: drop entries unconditionally in _do_sweep.
        Oracle: hand-counted live count, 3.
        """
        backend = MemoryBackend(sweep_interval=0)
        for i in range(3):
            backend.set(f'k{i}', i, 300)

        assert backend.sweep() == 0
        assert backend.count() == 3

    def test_an_entry_expiring_exactly_now_is_still_live(self):
        """Expiry is strict: expires_at == now has not yet passed.

        Mutation: `now > expires_at` -> `now >= expires_at` in _do_sweep.
        A fresh entry cannot separate the two, so the boundary is pinned
        directly by making expires_at equal the swept clock reading.
        Oracle: hand-derived residency - the entry survives at exactly its
        expiry instant and is dropped one microsecond later.
        """
        backend = MemoryBackend(sweep_interval=0)
        backend.set('k', 'v', 300)
        value, created_at, _ = backend._cache['k']
        boundary = time.time() + 5.0
        backend._cache['k'] = (value, created_at, boundary)

        assert backend._do_sweep(now=boundary) == 0
        assert 'k' in backend._cache

        assert backend._do_sweep(now=boundary + 1e-6) == 1
        assert 'k' not in backend._cache

    def test_a_read_at_the_exact_expiry_instant_still_hits(self, monkeypatch):
        """The same strict boundary governs the read path.

        Mutation: `now > expires_at` -> `now >= expires_at` in _do_get.
        Real time cannot land exactly on expires_at, so the backend's clock
        is pinned to make the boundary reachable at all.
        Oracle: the stored value when now == expires_at, NO_VALUE one
        microsecond later.
        """
        backend = MemoryBackend(sweep_interval=float('inf'))
        backend.set('k', 'v', 300)
        value, created_at, expires_at = backend._cache['k']

        monkeypatch.setattr(memory_module.time, 'time', lambda: expires_at)
        assert backend.get('k') == 'v'

        monkeypatch.setattr(memory_module.time, 'time', lambda: expires_at + 1e-6)
        assert backend.get('k') is cachu.api.NO_VALUE


class TestLruBound:
    """maxsize evicts least-recently-used entries, not arbitrary ones.
    """

    def test_oldest_entry_is_evicted_first(self):
        """Inserting past the bound drops the least-recently-inserted key.

        Mutation: popitem(last=False) -> popitem(last=True), which would evict
        the newest entry instead.
        Oracle: hand-derived survivor set {'b', 'c', 'd'} for insert order
        a, b, c, d at maxsize=3.
        """
        backend = MemoryBackend(maxsize=3)
        for key in ('a', 'b', 'c', 'd'):
            backend.set(key, key, 300)

        assert sorted(backend.keys()) == ['b', 'c', 'd']
        assert backend.evictions == 1

    def test_reading_an_entry_protects_it_from_eviction(self):
        """A read refreshes recency, so the read key outlives an unread older one.

        Mutation: remove the move_to_end(key) call in _do_get, degrading LRU
        to insertion-order FIFO.
        Oracle: hand-derived survivor set {'a', 'c', 'd'} - 'a' is read after
        'b' is written, so 'b' becomes least-recently-used.
        """
        backend = MemoryBackend(maxsize=3)
        backend.set('a', 1, 300)
        backend.set('b', 2, 300)
        backend.set('c', 3, 300)

        assert backend.get('a') == 1

        backend.set('d', 4, 300)

        assert sorted(backend.keys()) == ['a', 'c', 'd']

    def test_overwriting_a_key_does_not_grow_past_the_bound(self):
        """Re-setting an existing key replaces it rather than evicting a peer.

        Mutation: evict before insert, so the count drifts.
        Oracle: hand-counted survivor set {'a', 'b'} and evictions == 0.
        """
        backend = MemoryBackend(maxsize=2)
        backend.set('a', 1, 300)
        backend.set('b', 2, 300)
        backend.set('a', 99, 300)

        assert sorted(backend.keys()) == ['a', 'b']
        assert backend.get('a') == 99
        assert backend.evictions == 0

    def test_overwriting_a_key_refreshes_its_recency(self):
        """A rewrite makes a key most-recently-used, not just newest-valued.

        Mutation: remove move_to_end from _do_set. Plain reassignment does
        NOT reorder an OrderedDict, so 'a' would stay oldest and be evicted
        despite having just been written.
        Oracle: hand-derived survivor set {'a', 'c'} - after writing a, b
        then rewriting a, the least-recently-used key is 'b'.
        """
        backend = MemoryBackend(maxsize=2)
        backend.set('a', 1, 300)
        backend.set('b', 2, 300)
        backend.set('a', 99, 300)
        backend.set('c', 3, 300)

        assert sorted(backend.keys()) == ['a', 'c']

    def test_bound_of_one_keeps_only_the_newest(self):
        """maxsize=1 is honoured exactly, not off by one.

        Mutation: `while len > maxsize` -> `while len > maxsize + 1`.
        Oracle: hand-derived survivor set {'b'}.
        """
        backend = MemoryBackend(maxsize=1)
        backend.set('a', 1, 300)
        backend.set('b', 2, 300)

        assert list(backend.keys()) == ['b']
        assert backend.get('a') is cachu.api.NO_VALUE


class TestExpirySweep:
    """Expired entries are reclaimed without being read again.

    This is the reported leak: 200,000 expired keys survived every later
    lookup because nothing on the read path swept.
    """

    def test_unrelated_read_reclaims_expired_entries(self):
        """A lookup of a different key drops entries that expired unread.

        Mutation: remove the _maybe_sweep() call from _do_get.
        Oracle: hand-derived residency - only the live key 'live' remains, so
        len(backend._cache) == 1 out of 4 written.
        """
        backend = MemoryBackend(sweep_interval=0)
        for key in ('a', 'b', 'c', 'live'):
            backend.set(key, key, 300)
        for key in ('a', 'b', 'c'):
            _expire(backend, key)

        assert len(backend._cache) == 4

        backend.get('live')

        assert list(backend._cache) == ['live']
        assert backend.expired_swept == 3

    def test_write_reclaims_expired_entries(self):
        """A later write also sweeps, so a write-only workload cannot leak.

        Mutation: remove the _maybe_sweep() call from _do_set.
        Oracle: hand-derived residency - only the newly written key remains.
        """
        backend = MemoryBackend(sweep_interval=3600)
        backend.set('stale', 1, 300)
        _expire(backend, 'stale')
        backend._sweep_interval = 0

        backend.set('fresh', 2, 300)

        assert list(backend._cache) == ['fresh']

    def test_sweep_interval_defers_the_scan(self):
        """Inside the interval, an expired entry is not swept wholesale.

        Mutation: ignore sweep_interval and sweep on every operation, making
        every read O(n) on a large cache.
        Oracle: hand-derived residency - the untouched expired key 'other'
        must survive a read of a different key while the interval is open.
        """
        backend = MemoryBackend(sweep_interval=3600)
        backend.set('other', 1, 300)
        _expire(backend, 'other')
        backend.set('live', 2, 300)

        backend.get('live')

        assert 'other' in backend._cache
        assert backend.expired_swept == 0

    def test_expired_entry_is_still_a_miss_before_it_is_swept(self):
        """Deferring the sweep never serves an expired value.

        Mutation: return the cached tuple before the expiry comparison.
        Oracle: NO_VALUE, the documented miss sentinel.
        """
        backend = MemoryBackend(sweep_interval=3600)
        backend.set('k', 'stale-value', 300)
        _expire(backend, 'k')

        assert backend.get('k') is cachu.api.NO_VALUE

    def test_explicit_sweep_reports_what_it_dropped(self):
        """sweep() returns the number of entries it reclaimed.

        Mutation: return len(self._cache) or a bare None instead of the count
        dropped.
        Oracle: hand-counted expired count, 2 of 3 written.
        """
        backend = MemoryBackend(sweep_interval=3600)
        for key in ('a', 'b', 'c'):
            backend.set(key, key, 300)
        _expire(backend, 'a')
        _expire(backend, 'b')

        assert backend.sweep() == 2
        assert list(backend._cache) == ['c']

    async def test_async_sweep_reclaims_expired_entries(self):
        """asweep() reclaims through the async lock.

        Mutation: have asweep() return without calling _do_sweep.
        Oracle: hand-counted expired count, 1.
        """
        backend = MemoryBackend(sweep_interval=3600)
        await backend.aset('a', 1, 300)
        await backend.aset('b', 2, 300)
        _expire(backend, 'a')

        assert await backend.asweep() == 1
        assert list(backend._cache) == ['b']


class TestConfigWiring:
    """configure() bounds reach the backend the manager builds.
    """

    def test_configured_maxsize_bounds_a_decorated_cache(self):
        """memory_maxsize from configure() evicts through the decorator path.

        Mutation: drop maxsize=cfg.memory_maxsize in CacheManager._create_backend.
        Oracle: hand-derived live count, 2, after 5 distinct keys at maxsize=2.
        """
        cachu.configure(memory_maxsize=2)

        @cachu.cache(ttl=300, backend='memory', tag='bounded')
        def fetch(key: int) -> int:
            return key * 10

        for key in range(5):
            fetch(key)

        backend = cachu.get_backend('memory', ttl=300)
        assert backend.count() == 2
        assert backend.evictions == 3

    def test_configured_sweep_interval_reaches_the_backend(self):
        """memory_sweep_interval from configure() reaches the backend.

        Mutation: drop sweep_interval=cfg.memory_sweep_interval in
        CacheManager._create_backend, leaving the 60.0 default.
        Oracle: the configured value, 0.
        """
        cachu.configure(memory_sweep_interval=0)

        @cachu.cache(ttl=300, backend='memory', tag='swept')
        def fetch(key: int) -> int:
            return key

        fetch(1)

        backend = cachu.get_backend('memory', ttl=300)
        assert backend._sweep_interval == 0

    @pytest.mark.parametrize('bad', [0, -1, 1.5, True, 'many'])
    def test_invalid_maxsize_is_rejected(self, bad):
        """memory_maxsize must be a positive integer.

        Mutation: accept any truthy value, letting maxsize=0 evict every
        entry immediately or maxsize=1.5 loop forever on a float compare.
        Oracle: ValueError, the documented rejection for invalid config.
        """
        with pytest.raises(ValueError, match='memory_maxsize'):
            cachu.configure(memory_maxsize=bad)

    def test_sweeping_can_be_switched_off(self):
        """float('inf') disables sweeping and restores the pre-0.4.0 behaviour.

        Mutation: reject non-finite values outright, leaving no documented
        way to opt out of the O(n) sweep this release turned on by default.
        Oracle: hand-derived residency - the expired entry survives a later
        read, exactly as it did before sweeping existed.
        """
        cachu.configure(memory_sweep_interval=float('inf'))

        @cachu.cache(ttl=300, backend='memory', tag='nosweep')
        def fetch(key: int) -> int:
            return key

        fetch(1)
        backend = cachu.get_backend('memory', ttl=300)
        stored = next(iter(backend._cache))
        _expire(backend, stored)
        backend.set('other', 1, 300)

        assert stored in backend._cache
        assert backend.expired_swept == 0

    @pytest.mark.parametrize('bad', [-1, 'often'])
    def test_invalid_sweep_interval_is_rejected(self, bad):
        """memory_sweep_interval must be a non-negative number.

        Mutation: accept negatives, which would sweep on every operation
        while reading as "disabled".
        Oracle: ValueError, the documented rejection for invalid config.
        """
        with pytest.raises(ValueError, match='memory_sweep_interval'):
            cachu.configure(memory_sweep_interval=bad)


class TestPerKeyStateIsBounded:
    """Bounding the cache must bound everything the cache keys, not just entries.

    Notes
    -----
    - The dogpile mutex registries hold one lock per distinct cache key. A
      strong dict never released them, so 200,000 caller-supplied keys cost
      tens of megabytes of locks however small `memory_maxsize` was - the
      same unbounded per-key growth the bound exists to stop.
    """

    def test_mutex_registry_does_not_grow_per_key(self):
        """Distinct keys do not accumulate locks once their mutexes are gone.

        Mutation: hold the per-key locks in a plain dict, restoring the leak
        that survives cache_clear and clear_backends.
        Oracle: hand-derived residency - after 2,000 distinct keys and a
        collection, the registry must hold far fewer than the 2,000 a strong
        dict would keep. The bound is deliberately loose because CPython may
        keep the most recent entries alive.
        """
        ThreadingMutex.clear_locks()

        @cachu.cache(ttl=300, backend='memory', tag='keyed')
        def fetch(token: str) -> str:
            return token

        for index in range(2000):
            fetch(f'token{index}')

        gc.collect()

        assert len(ThreadingMutex._locks) < 100

    def test_contending_callers_still_share_one_lock(self):
        """Weak registration must not hand two live callers different locks.

        Mutation: create a fresh lock per mutex instead of sharing, which
        removes mutual exclusion entirely and reopens the dogpile.
        Oracle: object identity of the underlying lock across two live
        mutexes for one key.
        """
        ThreadingMutex.clear_locks()

        first = ThreadingMutex('shared')
        second = ThreadingMutex('shared')

        assert first._lock is second._lock
        assert first.acquire(timeout=0) is True
        try:
            assert second.acquire(timeout=0) is False
        finally:
            first.release()

    def test_a_held_lock_is_never_collected(self):
        """A lock in use survives collection pressure.

        Mutation: store the lock weakly on the mutex itself, letting a held
        lock vanish and a second caller enter the critical section.
        Oracle: mutual exclusion still observed after a forced collection.
        """
        ThreadingMutex.clear_locks()

        holder = ThreadingMutex('pinned')
        assert holder.acquire(timeout=0) is True
        try:
            gc.collect()
            assert ThreadingMutex('pinned').acquire(timeout=0) is False
        finally:
            holder.release()

    async def test_async_mutex_registry_does_not_grow_per_key(self):
        """The per-loop async registry is bounded the same way.

        Mutation: keep the inner per-loop dict strong, so an event loop
        accumulates one lock per key for its whole lifetime.
        Oracle: hand-derived residency - far fewer than the 2,000 keys used.
        """
        AsyncioMutex.clear_locks()

        @cachu.cache(ttl=300, backend='memory', tag='keyed')
        async def fetch(token: str) -> str:
            return token

        for index in range(2000):
            await fetch(f'token{index}')

        gc.collect()

        loop = asyncio.get_running_loop()
        per_loop = AsyncioMutex._loop_locks.get(loop, {})
        assert len(per_loop) < 100
