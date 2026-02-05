"""Redis cache backend implementation.
"""
import pickle
import struct
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal

from ..api import NO_VALUE, Backend
from ..mutex import AsyncCacheMutex, AsyncRedisMutex, CacheMutex, RedisMutex

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis


_METADATA_FORMAT = 'd'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)
_STATS_KEY_PREFIX = 'cachu:stats:'


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


def _unpack_value(data: bytes) -> tuple[Any, float] | None:
    """Unpack value and creation timestamp.

    Returns None if data is corrupted (per dogpile.cache behavior: treat
    deserialization errors as cache misses for graceful degradation).
    """
    try:
        created_at = struct.unpack(_METADATA_FORMAT, data[:_METADATA_SIZE])[0]
        value = pickle.loads(data[_METADATA_SIZE:])
        return value, created_at
    except (pickle.UnpicklingError, EOFError, TypeError, AttributeError, ModuleNotFoundError, struct.error):
        return None


class RedisBackend(Backend):
    """Unified Redis cache backend with both sync and async interfaces.
    """

    def __init__(self, url: str, lock_timeout: float = 10.0) -> None:
        self._url = url
        self._lock_timeout = lock_timeout
        self._sync_client: redis.Redis | None = None
        self._async_client: aioredis.Redis | None = None
        self._init_lock = threading.Lock()

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
        with self._init_lock:
            if self._async_client is None:
                aioredis = _get_async_redis_module()
                self._async_client = aioredis.from_url(self._url)
            return self._async_client

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or corrupted.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE
        result = _unpack_value(data)
        if result is None:
            self.client.delete(key)
            return NO_VALUE
        return result[0]

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found or corrupted.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE, None
        result = _unpack_value(data)
        if result is None:
            self.client.delete(key)
            return NO_VALUE, None
        return result

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

    # ===== Stats interface (sync) =====

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """
        self.client.hincrby(f'{_STATS_KEY_PREFIX}{fn_name}', stat, 1)

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        data = self.client.hgetall(f'{_STATS_KEY_PREFIX}{fn_name}')
        return (int(data.get(b'hits', 0)), int(data.get(b'misses', 0)))

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.
        """
        if fn_name:
            self.client.delete(f'{_STATS_KEY_PREFIX}{fn_name}')
        else:
            for key in self.client.scan_iter(match=f'{_STATS_KEY_PREFIX}*'):
                self.client.delete(key)

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or corrupted.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE
        result = _unpack_value(data)
        if result is None:
            await client.delete(key)
            return NO_VALUE
        return result[0]

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found or corrupted.
        """
        client = self._get_async_client()
        data = await client.get(key)
        if data is None:
            return NO_VALUE, None
        result = _unpack_value(data)
        if result is None:
            await client.delete(key)
            return NO_VALUE, None
        return result

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

    # ===== Stats interface (async) =====

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """
        client = self._get_async_client()
        await client.hincrby(f'{_STATS_KEY_PREFIX}{fn_name}', stat, 1)

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        client = self._get_async_client()
        data = await client.hgetall(f'{_STATS_KEY_PREFIX}{fn_name}')
        return (int(data.get(b'hits', 0)), int(data.get(b'misses', 0)))

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """
        client = self._get_async_client()
        if fn_name:
            await client.delete(f'{_STATS_KEY_PREFIX}{fn_name}')
        else:
            async for key in client.scan_iter(match=f'{_STATS_KEY_PREFIX}*'):
                await client.delete(key)

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
