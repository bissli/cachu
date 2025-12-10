"""Configuration module for cache backends with namespace isolation.

Each calling library gets its own isolated configuration, preventing
configuration conflicts when multiple libraries use the cache package.
"""
import inspect
import logging
from dataclasses import dataclass, replace
from typing import Any

logger = logging.getLogger(__name__)


def _get_caller_namespace() -> str | None:
    """Get the top-level package name of the caller.
    """
    for frame_info in inspect.stack():
        module = inspect.getmodule(frame_info.frame)
        if module and module.__name__:
            if module.__name__.startswith('cache'):
                continue
            return module.__name__.split('.')[0]
    return None


@dataclass
class CacheConfig:
    """Configuration for cache backends."""
    debug_key: str = ''
    memory: str = 'dogpile.cache.null'
    file: str = 'dogpile.cache.dbm'
    redis: str = 'dogpile.cache.null'
    redis_host: str = 'localhost'
    redis_port: int = 6379
    redis_db: int = 0
    redis_distributed: bool = False
    redis_ssl: bool = False
    tmpdir: str = '/tmp'
    default_backend: str = 'memory'


class ConfigRegistry:
    """Registry that maintains per-namespace cache configurations.

    Each library (identified by top-level package name) gets its own
    isolated configuration. This prevents configuration conflicts when
    multiple libraries use the cache package with different settings.
    """

    def __init__(self) -> None:
        self._configs: dict[str | None, CacheConfig] = {}
        self._default = CacheConfig()

    def configure(self, namespace: str | None = None, **kwargs) -> CacheConfig:
        """Configure cache for a specific namespace.
        """
        if namespace is None:
            namespace = _get_caller_namespace()

        # Safety: if redis is null, disable distributed locking
        if kwargs.get('redis') == 'dogpile.cache.null':
            kwargs.setdefault('redis_distributed', False)

        if namespace not in self._configs:
            # Copy from default so settings like tmpdir are inherited
            self._configs[namespace] = replace(self._default)
            logger.debug(f"Created new cache config for namespace '{namespace}'")

        cfg = self._configs[namespace]
        for key, value in kwargs.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)
            else:
                raise ValueError(f'Unknown configuration key: {key}')

        logger.debug(f"Configured cache for namespace '{namespace}': {kwargs}")
        return cfg

    def get_config(self, namespace: str | None = None) -> CacheConfig:
        """Get config for a namespace, with fallback to default.
        """
        if namespace is None:
            namespace = _get_caller_namespace()

        if namespace in self._configs:
            return self._configs[namespace]

        return self._default

    def get_all_namespaces(self) -> list[str | None]:
        """Return list of configured namespaces.
        """
        return list(self._configs.keys())

    def clear(self) -> None:
        """Clear all namespace configurations. Primarily for testing.
        """
        self._configs.clear()


# Global registry instance
_registry = ConfigRegistry()


class ConfigProxy:
    """Proxy that resolves config attributes based on caller's namespace.

    This maintains backward compatibility with code that does:
        from cache.config import config
        config.redis  # Automatically resolves to caller's namespace
    """

    def __getattr__(self, name: str) -> Any:
        cfg = _registry.get_config()
        return getattr(cfg, name)

    def __repr__(self) -> str:
        namespace = _get_caller_namespace()
        cfg = _registry.get_config(namespace)
        return f'ConfigProxy(namespace={namespace!r}, config={cfg!r})'


# Backward-compatible config object
config = ConfigProxy()


def configure(**kwargs) -> CacheConfig:
    """Configure cache settings for the caller's namespace.

    This is the main entry point for configuration. Each calling package
    gets its own isolated configuration.

    Args:
        debug_key: Prefix for cache keys (default: "")
        memory: Backend for memory cache (default: "dogpile.cache.null")
        file: Backend for file cache (default: "dogpile.cache.dbm")
        redis: Backend for redis cache (default: "dogpile.cache.null")
        redis_host: Redis server hostname (default: "localhost")
        redis_port: Redis server port (default: 6379)
        redis_db: Redis database number (default: 0)
        redis_distributed: Use distributed locks for Redis (default: False)
        redis_ssl: Use SSL for Redis connection (default: False)
        tmpdir: Directory for file-based caches (default: "/tmp")
        default_backend: Backend for defaultcache() - 'memory', 'redis', or 'file'

    Returns
        The CacheConfig for the caller's namespace.
    """
    return _registry.configure(**kwargs)


def get_config(namespace: str | None = None) -> CacheConfig:
    """Get the CacheConfig for a specific namespace or the caller's namespace.
    """
    return _registry.get_config(namespace)


def clear_registry() -> None:
    """Clear all namespace configurations. Primarily for testing.
    """
    _registry.clear()
