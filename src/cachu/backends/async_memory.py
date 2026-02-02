"""Async memory cache backend implementation.
"""
import asyncio
import fnmatch
import pickle
import time
from collections.abc import AsyncIterator
from typing import Any

from . import NO_VALUE
from .async_base import AsyncBackend


class AsyncMemoryBackend(AsyncBackend):
    """Async in-memory cache backend using asyncio.Lock.
    """

    def __init__(self) -> None:
        self._cache: dict[str, tuple[bytes, float, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return NO_VALUE

            pickled_value, created_at, expires_at = entry
            if time.time() > expires_at:
                del self._cache[key]
                return NO_VALUE

            return pickle.loads(pickled_value)

    async def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return NO_VALUE, None

            pickled_value, created_at, expires_at = entry
            if time.time() > expires_at:
                del self._cache[key]
                return NO_VALUE, None

            return pickle.loads(pickled_value), created_at

    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        pickled_value = pickle.dumps(value)
        async with self._lock:
            self._cache[key] = (pickled_value, now, now + ttl)

    async def delete(self, key: str) -> None:
        """Delete value by key.
        """
        async with self._lock:
            self._cache.pop(key, None)

    async def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        async with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                return count

            keys_to_delete = [k for k in self._cache if fnmatch.fnmatch(k, pattern)]
            for key in keys_to_delete:
                del self._cache[key]
            return len(keys_to_delete)

    async def keys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()
        async with self._lock:
            all_keys = list(self._cache.keys())

        for key in all_keys:
            async with self._lock:
                entry = self._cache.get(key)
                if entry is None:
                    continue
                _, _, expires_at = entry
                if now > expires_at:
                    del self._cache[key]
                    continue

            if pattern is None or fnmatch.fnmatch(key, pattern):
                yield key

    async def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        count = 0
        async for _ in self.keys(pattern):
            count += 1
        return count

    async def close(self) -> None:
        """Close the backend (no-op for memory backend).
        """
        pass
