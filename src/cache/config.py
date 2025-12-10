"""Configuration module for cache backends with namespace isolation.

Each calling library gets its own isolated configuration, preventing
configuration conflicts when multiple libraries use the cache package.
"""
import inspect
import logging
import os
import pathlib
import sys
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
            ns = module.__name__.split('.')[0]
            if ns == '__main__' and sys.argv and sys.argv[0]:
                return f'__main__.{pathlib.Path(sys.argv[0]).stem}'
            return ns
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

    def configure(
        self,
        namespace: str | None = None,
        debug_key: str | None = None,
        memory: str | None = None,
        file: str | None = None,
        redis: str | None = None,
        redis_host: str | None = None,
        redis_port: int | None = None,
        redis_db: int | None = None,
        redis_distributed: bool | None = None,
        redis_ssl: bool | None = None,
        tmpdir: str | None = None,
        default_backend: str | None = None,
    ) -> CacheConfig:
        """Configure cache for a specific namespace."""
        if namespace is None:
            namespace = _get_caller_namespace()

        updates = {
            'debug_key': debug_key,
            'memory': memory,
            'file': file,
            'redis': redis,
            'redis_host': redis_host,
            'redis_port': redis_port,
            'redis_db': redis_db,
            'redis_distributed': redis_distributed,
            'redis_ssl': redis_ssl,
            'tmpdir': str(tmpdir) if tmpdir else None,
            'default_backend': default_backend,
        }
        updates = {k: v for k, v in updates.items() if v is not None}

        self._validate_config(updates)

        if redis == 'dogpile.cache.null' and redis_distributed is None:
            updates['redis_distributed'] = False

        if namespace not in self._configs:
            self._configs[namespace] = replace(self._default)
            logger.debug(f"Created new cache config for namespace '{namespace}'")

        cfg = self._configs[namespace]
        for key, value in updates.items():
            setattr(cfg, key, value)

        logger.debug(f"Configured cache for namespace '{namespace}': {updates}")
        return cfg

    def _validate_config(self, kwargs: dict[str, Any]) -> None:
        """Validate configuration values.
        """
        if 'redis_port' in kwargs:
            port = kwargs['redis_port']
            if not isinstance(port, int) or port < 1 or port > 65535:
                raise ValueError(f'redis_port must be an integer between 1 and 65535, got {port}')

        if 'redis_db' in kwargs:
            db = kwargs['redis_db']
            if not isinstance(db, int) or db < 0:
                raise ValueError(f'redis_db must be a non-negative integer, got {db}')

        if 'tmpdir' in kwargs:
            tmpdir = kwargs['tmpdir']
            if not pathlib.Path(tmpdir).is_dir():
                raise ValueError(f'tmpdir must be an existing directory, got {tmpdir!r}')
            if not os.access(tmpdir, os.W_OK):
                raise ValueError(f'tmpdir must be writable, got {tmpdir!r}')

        if 'default_backend' in kwargs:
            backend = kwargs['default_backend']
            valid_backends = ('memory', 'redis', 'file')
            if backend not in valid_backends:
                raise ValueError(f'default_backend must be one of {valid_backends}, got {backend!r}')

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


def configure(
    debug_key: str | None = None,
    memory: str | None = None,
    file: str | None = None,
    redis: str | None = None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_db: int | None = None,
    redis_distributed: bool | None = None,
    redis_ssl: bool | None = None,
    tmpdir: str | None = None,
    default_backend: str | None = None,
) -> CacheConfig:
    """Configure cache settings for the caller's namespace.

    This is the main entry point for configuration. Each calling package
    gets its own isolated configuration.
    """
    return _registry.configure(
        debug_key=debug_key,
        memory=memory,
        file=file,
        redis=redis,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_distributed=redis_distributed,
        redis_ssl=redis_ssl,
        tmpdir=str(tmpdir) if tmpdir else None,
        default_backend=default_backend,
    )


def get_config(namespace: str | None = None) -> CacheConfig:
    """Get the CacheConfig for a specific namespace or the caller's namespace.
    """
    return _registry.get_config(namespace)


def clear_registry() -> None:
    """Clear all namespace configurations. Primarily for testing.
    """
    _registry.clear()
