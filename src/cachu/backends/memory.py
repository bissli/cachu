"""Memory cache backend implementation.
"""
import fnmatch
import pickle
import threading
import time
from collections.abc import Iterator
from typing import Any

from . import NO_VALUE, Backend


class MemoryBackend(Backend):
    """Thread-safe in-memory cache backend.
    """

    def __init__(self) -> None:
        self._cache: dict[str, tuple[bytes, float, float]] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return NO_VALUE

            pickled_value, created_at, expires_at = entry
            if time.time() > expires_at:
                del self._cache[key]
                return NO_VALUE

            return pickle.loads(pickled_value)

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return NO_VALUE, None

            pickled_value, created_at, expires_at = entry
            if time.time() > expires_at:
                del self._cache[key]
                return NO_VALUE, None

            return pickle.loads(pickled_value), created_at

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        pickled_value = pickle.dumps(value)
        with self._lock:
            self._cache[key] = (pickled_value, now, now + ttl)

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._lock:
            self._cache.pop(key, None)

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                return count

            keys_to_delete = [k for k in self._cache if fnmatch.fnmatch(k, pattern)]
            for key in keys_to_delete:
                del self._cache[key]
            return len(keys_to_delete)

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()
        with self._lock:
            all_keys = list(self._cache.keys())

        for key in all_keys:
            with self._lock:
                entry = self._cache.get(key)
                if entry is None:
                    continue
                _, _, expires_at = entry
                if now > expires_at:
                    del self._cache[key]
                    continue

            if pattern is None or fnmatch.fnmatch(key, pattern):
                yield key

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        return sum(1 for _ in self.keys(pattern))
