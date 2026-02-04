"""Redis cache backend implementation.
"""
import pickle
import struct
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any

from ..mutex import AsyncCacheMutex, AsyncRedisMutex, CacheMutex, RedisMutex
from . import NO_VALUE, Backend

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis


_METADATA_FORMAT = 'd'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)


def _get_redis_module() -> Any:
    """Import redis module, raising helpful error if not installed.
    """
    try:
        import redis
        return redis
    except ImportError as e:
        raise RuntimeError(
            "Redis support requires the 'redis' package. Install with: pip install cachu[redis]"
        ) from e


def _get_async_redis_module() -> Any:
    """Import redis.asyncio module, raising helpful error if not installed.
    """
    try:
        import redis.asyncio as aioredis
        return aioredis
    except ImportError as e:
        raise RuntimeError(
            "Async Redis support requires the 'redis' package (>=4.2.0). "
            "Install with: pip install cachu[redis]"
        ) from e


def get_redis_client(url: str) -> 'redis.Redis':
    """Create a Redis client from URL.

    Args:
        url: Redis URL (e.g., 'redis://localhost:6379/0')
    """
    redis_module = _get_redis_module()
    return redis_module.from_url(url)


def _pack_value(value: Any, created_at: float) -> bytes:
    """Pack value with creation timestamp.
    """
    metadata = struct.pack(_METADATA_FORMAT, created_at)
    pickled = pickle.dumps(value)
    return metadata + pickled


def _unpack_value(data: bytes) -> tuple[Any, float]:
    """Unpack value and creation timestamp.
    """
    created_at = struct.unpack(_METADATA_FORMAT, data[:_METADATA_SIZE])[0]
    value = pickle.loads(data[_METADATA_SIZE:])
    return value, created_at


class RedisBackend(Backend):
    """Unified Redis cache backend with both sync and async interfaces.
    """

    def __init__(self, url: str, lock_timeout: float = 10.0) -> None:
        self._url = url
        self._lock_timeout = lock_timeout
        self._sync_client: redis.Redis | None = None
        self._async_client: aioredis.Redis | None = None

    @property
    def client(self) -> 'redis.Redis':
        """Lazy-load sync Redis client.
        """
        if self._sync_client is None:
            self._sync_client = get_redis_client(self._url)
        return self._sync_client

    def _get_async_client(self) -> 'aioredis.Redis':
        """Lazy-load async Redis client (from_url is NOT async).
        """
        if self._async_client is None:
            aioredis = _get_async_redis_module()
            self._async_client = aioredis.from_url(self._url)
        return self._async_client

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE
        value, _ = _unpack_value(data)
        return value

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE, None
        value, created_at = _unpack_value(data)
        return value, created_at

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        packed = _pack_value(value, now)
        self.client.setex(key, ttl, packed)

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        self.client.delete(key)

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        if pattern is None:
            pattern = '*'

        count = 0
        for key in self.client.scan_iter(match=pattern):
            self.client.delete(key)
            count += 1
        return count

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        redis_pattern = pattern or '*'
        for key in self.client.scan_iter(match=redis_pattern):
            yield key.decode() if isinstance(key, bytes) else key

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        return sum(1 for _ in self.keys(pattern))

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return RedisMutex(self.client, f'lock:{key}', self._lock_timeout)

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE
        value, _ = _unpack_value(data)
        return value

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE, None
        value, created_at = _unpack_value(data)
        return value, created_at

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """
        client = self._get_async_client()
        now = time.time()
        packed = _pack_value(value, now)
        await client.setex(key, ttl, packed)

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        client = self._get_async_client()
        await client.delete(key)

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        client = self._get_async_client()
        if pattern is None:
            pattern = '*'

        count = 0
        async for key in client.scan_iter(match=pattern):
            await client.delete(key)
            count += 1
        return count

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        client = self._get_async_client()
        redis_pattern = pattern or '*'
        async for key in client.scan_iter(match=redis_pattern):
            yield key.decode() if isinstance(key, bytes) else key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """
        count = 0
        async for _ in self.akeys(pattern):
            count += 1
        return count

    def get_async_mutex(self, key: str) -> AsyncCacheMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncRedisMutex(self._get_async_client(), f'lock:{key}', self._lock_timeout)

    # ===== Lifecycle =====

    def _close_sync_client(self) -> None:
        """Close sync client if open.
        """
        if self._sync_client is not None:
            client = self._sync_client
            self._sync_client = None
            client.close()

    def _close_async_client_sync(self) -> None:
        """Forcefully close async client from sync context.
        """
        if self._async_client is not None:
            client = self._async_client
            self._async_client = None
            try:
                client.close()
            except Exception:
                pass

    def close(self) -> None:
        """Close all backend resources from sync context.
        """
        self._close_sync_client()
        self._close_async_client_sync()

    async def aclose(self) -> None:
        """Close all backend resources from async context.
        """
        if self._async_client is not None:
            client = self._async_client
            self._async_client = None
            await client.aclose()
        self._close_sync_client()
