"""Memory cache backend implementation.
"""
import asyncio
import fnmatch
import pickle
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

from ..mutex import AsyncioMutex, CacheMutex, ThreadingMutex
from . import NO_VALUE, Backend


class MemoryBackend(Backend):
    """Thread-safe in-memory cache backend with both sync and async interfaces.
    """

    def __init__(self) -> None:
        self._cache: dict[str, tuple[bytes, float, float]] = {}
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()

    # ===== Core logic (no locking) =====

    def _do_get(self, key: str) -> tuple[Any, float | None]:
        """Get value and metadata without locking.

        Handles corrupted cache data gracefully by treating deserialization
        errors as cache misses (per dogpile.cache behavior).
        """
        entry = self._cache.get(key)
        if entry is None:
            return NO_VALUE, None

        pickled_value, created_at, expires_at = entry
        if time.time() > expires_at:
            del self._cache[key]
            return NO_VALUE, None

        try:
            return pickle.loads(pickled_value), created_at
        except (pickle.UnpicklingError, EOFError, TypeError, AttributeError, ModuleNotFoundError):
            del self._cache[key]
            return NO_VALUE, None

    def _do_set(self, key: str, value: Any, ttl: int) -> None:
        """Set value without locking.
        """
        now = time.time()
        pickled_value = pickle.dumps(value)
        self._cache[key] = (pickled_value, now, now + ttl)

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

        keys_to_delete = [k for k in self._cache if fnmatch.fnmatch(k, pattern)]
        for key in keys_to_delete:
            del self._cache[key]
        return len(keys_to_delete)

    def _do_keys(self, pattern: str | None = None) -> list[str]:
        """Get keys matching pattern without locking (returns snapshot).
        """
        now = time.time()
        result = []
        keys_to_delete = []

        for key, entry in list(self._cache.items()):
            _, _, expires_at = entry
            if now > expires_at:
                keys_to_delete.append(key)
                continue
            if pattern is None or fnmatch.fnmatch(key, pattern):
                result.append(key)

        for key in keys_to_delete:
            self._cache.pop(key, None)

        return result

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._sync_lock:
            value, _ = self._do_get(key)
            return value

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._sync_lock:
            return self._do_get(key)

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        with self._sync_lock:
            self._do_set(key, value, ttl)

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._sync_lock:
            self._do_delete(key)

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._sync_lock:
            return self._do_clear(pattern)

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        with self._sync_lock:
            all_keys = self._do_keys(pattern)
        yield from all_keys

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        with self._sync_lock:
            return len(self._do_keys(pattern))

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return ThreadingMutex(f'memory:{key}')

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or expired.
        """
        async with self._async_lock:
            value, _ = self._do_get(key)
            return value

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        async with self._async_lock:
            return self._do_get(key)

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """
        async with self._async_lock:
            self._do_set(key, value, ttl)

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        async with self._async_lock:
            self._do_delete(key)

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        async with self._async_lock:
            return self._do_clear(pattern)

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        async with self._async_lock:
            all_keys = self._do_keys(pattern)

        for key in all_keys:
            yield key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """
        async with self._async_lock:
            return len(self._do_keys(pattern))

    def get_async_mutex(self, key: str) -> AsyncioMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncioMutex(f'memory:{key}')

    # ===== Lifecycle =====

    def close(self) -> None:
        """Close the backend (no-op for memory backend).
        """

    async def aclose(self) -> None:
        """Async close the backend (no-op for memory backend).
        """
