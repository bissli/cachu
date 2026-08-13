"""Memory cache backend implementation.
"""
import fnmatch
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterator
from typing import Any, Literal

from ..api import NO_VALUE, Backend
from ..mutex import AsyncioMutex, CacheMutex, ThreadingMutex

logger = logging.getLogger(__name__)


class MemoryBackend(Backend):
    """Thread-safe in-memory cache backend with both sync and async interfaces.

    Parameters
    ----------
    maxsize : int or None, default None
        Maximum number of live entries. Once exceeded, least-recently-used
        entries are evicted until the bound holds again. None is unbounded,
        which is the historical behavior.
    sweep_interval : float, default 60.0
        Minimum seconds between amortized sweeps that drop expired entries.
        0 sweeps on every mutating or reading operation.

    Attributes
    ----------
    evictions : int
        Count of entries dropped by the LRU bound since construction.
    expired_swept : int
        Count of entries dropped by an amortized or on-read expiry sweep.

    Notes
    -----
    - An expired entry that is never read again used to stay resident until
      process exit, because only `keys()`/`count()` swept and nothing on the
      read path calls them. The interval sweep bounds that leak in time; the
      `maxsize` bound caps it in space for key spaces influenced by callers
      (credential hashes, tenant ids, search terms).
    - Recency is tracked on read and on write, so `maxsize` eviction is LRU
      rather than insertion-order FIFO.
    - A sweep is one O(n) pass under the backend lock, so a single caller per
      interval pays it: roughly 1 ms at 10k entries and 20-55 ms at 200k.
      `maxsize` caps n and therefore caps the sweep.
    - One `threading.RLock` guards the data for BOTH the sync and the async
      interface. A second asyncio.Lock would not exclude the sync path, and
      one manager entry keys a backend by (package, backend, ttl) only - so
      a sync and an async decorated function routinely share one instance.
      Every operation here is short, non-blocking CPU work, so holding a
      threading lock from a coroutine costs no more than the work itself.
    """

    def __init__(self, maxsize: int | None = None, sweep_interval: float = 60.0) -> None:
        self._cache: OrderedDict[str, tuple[Any, float, float]] = OrderedDict()
        self._stats: dict[str, tuple[int, int]] = {}
        self._lock = threading.RLock()
        self._maxsize = maxsize
        self._sweep_interval = sweep_interval
        self._last_sweep = time.monotonic()
        self.evictions = 0
        self.expired_swept = 0

    # ===== Core logic (no locking) =====

    def _do_sweep(self, now: float | None = None) -> int:
        """Drop every expired entry without locking. Returns the count dropped.
        """
        now = time.time() if now is None else now
        expired = [
            key for key, (_, _, expires_at) in list(self._cache.items())
            if now > expires_at
        ]
        for key in expired:
            self._cache.pop(key, None)
        self._last_sweep = time.monotonic()
        self.expired_swept += len(expired)
        return len(expired)

    def _maybe_sweep(self, now: float | None = None) -> None:
        """Sweep expired entries when the interval has elapsed (amortized).
        """
        if time.monotonic() - self._last_sweep < self._sweep_interval:
            return
        dropped = self._do_sweep(now)
        if dropped:
            logger.debug(f'Memory backend swept {dropped} expired entries; {len(self._cache)} live')

    def _do_evict(self) -> None:
        """Evict least-recently-used entries until maxsize holds, without locking.
        """
        if self._maxsize is None:
            return
        while len(self._cache) > self._maxsize:
            try:
                self._cache.popitem(last=False)
            except KeyError:
                return
            self.evictions += 1

    def _do_get(self, key: str) -> tuple[Any, float | None]:
        """Get value and metadata without locking, refreshing LRU recency on a hit.
        """
        now = time.time()
        self._maybe_sweep(now)

        entry = self._cache.get(key)
        if entry is None:
            return NO_VALUE, None

        value, created_at, expires_at = entry
        if now > expires_at:
            self._cache.pop(key, None)
            return NO_VALUE, None

        self._cache.move_to_end(key)
        return value, created_at

    def _do_set(self, key: str, value: Any, ttl: int) -> None:
        """Set value without locking. A non-positive TTL is not cached.
        """
        if ttl <= 0:
            self._cache.pop(key, None)
            return
        now = time.time()
        self._maybe_sweep(now)
        self._cache[key] = (value, now, now + ttl)
        self._cache.move_to_end(key)
        self._do_evict()

    def _do_delete(self, key: str) -> None:
        """Delete value without locking.
        """
        self._cache.pop(key, None)

    def _do_clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern without locking.
        """
        if pattern is None:
            count = len(self._cache)
            self._cache.clear()
            return count

        keys_to_delete = [k for k in list(self._cache) if fnmatch.fnmatch(k, pattern)]
        for key in keys_to_delete:
            self._cache.pop(key, None)
        return len(keys_to_delete)

    def _do_keys(self, pattern: str | None = None) -> list[str]:
        """Get keys matching pattern without locking (returns snapshot).
        """
        self._do_sweep()
        if pattern is None:
            return list(self._cache)
        return [key for key in list(self._cache) if fnmatch.fnmatch(key, pattern)]

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._lock:
            value, _ = self._do_get(key)
            return value

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._lock:
            return self._do_get(key)

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        with self._lock:
            self._do_set(key, value, ttl)

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._lock:
            self._do_delete(key)

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._lock:
            return self._do_clear(pattern)

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        with self._lock:
            all_keys = self._do_keys(pattern)
        yield from all_keys

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        with self._lock:
            return len(self._do_keys(pattern))

    def sweep(self) -> int:
        """Drop every expired entry now, regardless of sweep_interval.

        Returns
        -------
        int
            Number of expired entries dropped.
        """
        with self._lock:
            return self._do_sweep()

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return ThreadingMutex(f'memory:{key}')

    # ===== Stats interface (sync) =====

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """
        with self._lock:
            hits, misses = self._stats.get(fn_name, (0, 0))
            if stat == 'hits':
                self._stats[fn_name] = (hits + 1, misses)
            else:
                self._stats[fn_name] = (hits, misses + 1)

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        with self._lock:
            return self._stats.get(fn_name, (0, 0))

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.
        """
        with self._lock:
            if fn_name:
                self._stats.pop(fn_name, None)
            else:
                self._stats.clear()

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._lock:
            value, _ = self._do_get(key)
            return value

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._lock:
            return self._do_get(key)

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """
        with self._lock:
            self._do_set(key, value, ttl)

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        with self._lock:
            self._do_delete(key)

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        with self._lock:
            return self._do_clear(pattern)

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        with self._lock:
            all_keys = self._do_keys(pattern)

        for key in all_keys:
            yield key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """
        with self._lock:
            return len(self._do_keys(pattern))

    async def asweep(self) -> int:
        """Async drop every expired entry now, regardless of sweep_interval.

        Returns
        -------
        int
            Number of expired entries dropped.
        """
        with self._lock:
            return self._do_sweep()

    def get_async_mutex(self, key: str) -> AsyncioMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncioMutex(f'memory:{key}')

    # ===== Stats interface (async) =====

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """
        with self._lock:
            hits, misses = self._stats.get(fn_name, (0, 0))
            if stat == 'hits':
                self._stats[fn_name] = (hits + 1, misses)
            else:
                self._stats[fn_name] = (hits, misses + 1)

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        with self._lock:
            return self._stats.get(fn_name, (0, 0))

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """
        with self._lock:
            if fn_name:
                self._stats.pop(fn_name, None)
            else:
                self._stats.clear()

    # ===== Lifecycle =====

    def close(self) -> None:
        """Close the backend (no-op for memory backend).
        """

    async def aclose(self) -> None:
        """Async close the backend (no-op for memory backend).
        """
