"""Redis cache backend implementation.
"""
import pickle
import struct
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from . import NO_VALUE, Backend

if TYPE_CHECKING:
    import redis


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
            "Redis support requires the 'redis' package. Install with: pip install cache[redis]"
        ) from e


def get_redis_client(url: str) -> 'redis.Redis':
    """Create a Redis client from URL.

    Args:
        url: Redis URL (e.g., 'redis://localhost:6379/0')
    """
    redis_module = _get_redis_module()
    return redis_module.from_url(url)


class RedisBackend(Backend):
    """Redis cache backend.
    """

    def __init__(self, url: str, distributed_lock: bool = False) -> None:
        self._url = url
        self._distributed_lock = distributed_lock
        self._client: redis.Redis | None = None

    @property
    def client(self) -> 'redis.Redis':
        """Lazy-load Redis client.
        """
        if self._client is None:
            self._client = get_redis_client(self._url)
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

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE
        value, _ = self._unpack_value(data)
        return value

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        data = self.client.get(key)
        if data is None:
            return NO_VALUE, None
        value, created_at = self._unpack_value(data)
        return value, created_at

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        packed = self._pack_value(value, now)
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

    def close(self) -> None:
        """Close the Redis connection.
        """
        if self._client is not None:
            self._client.close()
            self._client = None
