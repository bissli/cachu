"""Async Redis cache backend implementation using redis.asyncio.
"""
import pickle
import struct
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from . import NO_VALUE
from .async_base import AsyncBackend

if TYPE_CHECKING:
    import redis.asyncio as aioredis


_METADATA_FORMAT = 'd'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)


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


async def get_async_redis_client(url: str) -> 'aioredis.Redis':
    """Create an async Redis client from URL.

    Args:
        url: Redis URL (e.g., 'redis://localhost:6379/0')
    """
    aioredis = _get_async_redis_module()
    return aioredis.from_url(url)


class AsyncRedisBackend(AsyncBackend):
    """Async Redis cache backend using redis.asyncio.
    """

    def __init__(self, url: str, distributed_lock: bool = False) -> None:
        self._url = url
        self._distributed_lock = distributed_lock
        self._client: aioredis.Redis | None = None

    async def _get_client(self) -> 'aioredis.Redis':
        """Lazy-load async Redis client.
        """
        if self._client is None:
            self._client = await get_async_redis_client(self._url)
        return self._client

    def _pack_value(self, value: Any, created_at: float) -> bytes:
        """Pack value with creation timestamp.
        """
        metadata = struct.pack(_METADATA_FORMAT, created_at)
        pickled = pickle.dumps(value)
        return metadata + pickled

    def _unpack_value(self, data: bytes) -> tuple[Any, float]:
        """Unpack value and creation timestamp.
        """
        created_at = struct.unpack(_METADATA_FORMAT, data[:_METADATA_SIZE])[0]
        value = pickle.loads(data[_METADATA_SIZE:])
        return value, created_at

    async def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found.
        """
        client = await self._get_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE
        value, _ = self._unpack_value(data)
        return value

    async def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        client = await self._get_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE, None
        value, created_at = self._unpack_value(data)
        return value, created_at

    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        client = await self._get_client()
        now = time.time()
        packed = self._pack_value(value, now)
        await client.setex(key, ttl, packed)

    async def delete(self, key: str) -> None:
        """Delete value by key.
        """
        client = await self._get_client()
        await client.delete(key)

    async def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        client = await self._get_client()
        if pattern is None:
            pattern = '*'

        count = 0
        async for key in client.scan_iter(match=pattern):
            await client.delete(key)
            count += 1
        return count

    async def keys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Iterate over keys matching pattern.
        """
        client = await self._get_client()
        redis_pattern = pattern or '*'
        async for key in client.scan_iter(match=redis_pattern):
            yield key.decode() if isinstance(key, bytes) else key

    async def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        count = 0
        async for _ in self.keys(pattern):
            count += 1
        return count

    async def close(self) -> None:
        """Close the Redis connection.
        """
        if self._client is not None:
            await self._client.close()
            self._client = None
