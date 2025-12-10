"""Configuration module for cache backends.
"""
from dataclasses import dataclass


@dataclass
class CacheConfig:
    """Configuration for cache backends.
    """
    debug_key: str = ''
    memory: str = 'dogpile.cache.null'
    redis: str = 'dogpile.cache.null'
    redis_host: str = 'localhost'
    redis_port: int = 6379
    redis_db: int = 0
    redis_distributed: bool = False
    redis_ssl: bool = False
    tmpdir: str = '/tmp'
    default_backend: str = 'memory'


config = CacheConfig()


def configure(**kwargs) -> None:
    """Configure cache settings globally.

    Args:
        debug_key: Prefix for cache keys (default: "")
        memory: Backend for memory cache (default: "dogpile.cache.null", set to "dogpile.cache.memory_pickle" to enable)
        redis: Backend for redis cache (default: "dogpile.cache.null", set to "dogpile.cache.redis" to enable)
        redis_host: Redis server hostname (default: "localhost")
        redis_port: Redis server port (default: 6379)
        redis_db: Redis database number (default: 0)
        redis_distributed: Use distributed locks for Redis (default: False)
        redis_ssl: Use SSL for Redis connection (default: False)
        tmpdir: Directory for file-based caches (default: "/tmp")
        default_backend: Backend for defaultcache() - 'memory', 'redis', or 'file' (default: 'memory')
    """
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
        else:
            raise ValueError(f'Unknown configuration key: {key}')
