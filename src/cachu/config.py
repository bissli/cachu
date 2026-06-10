"""Configuration module for cache backends with package isolation.

Each calling library gets its own isolated configuration, preventing
configuration conflicts when multiple libraries use the cachu package.
"""
import logging
import os
import pathlib
import sys
import threading
from dataclasses import asdict, dataclass, replace
from typing import Any

logger = logging.getLogger(__name__)

VALID_BACKENDS = ('memory', 'redis', 'file', 'null')

_disabled: bool = False


def disable() -> None:
    """Disable all caching globally.
    """
    global _disabled
    _disabled = True


def enable() -> None:
    """Re-enable caching after disable().
    """
    global _disabled
    _disabled = False


def is_disabled() -> bool:
    """Check if caching is globally disabled.
    """
    return _disabled


def _get_caller_package() -> str | None:
    """Get the top-level package name of the caller.
    """
    frame = sys._getframe(1)
    while frame:
        name = frame.f_globals.get('__name__', '')
        if name and not name.startswith('cachu'):
            pkg = name.split('.')[0]
            if pkg == '__main__' and sys.argv and sys.argv[0]:
                return f'__main__.{pathlib.Path(sys.argv[0]).stem}'
            return pkg
        frame = frame.f_back
    return None


@dataclass
class CacheConfig:
    """Configuration for cache backends.
    """
    backend_default: str = 'memory'
    key_prefix: str = ''
    file_dir: str = '/tmp'
    redis_url: str = 'redis://localhost:6379/0'
    lock_timeout: float = 10.0
    redis_health_check_interval: int = 30
    redis_socket_timeout: float = 5.0
    redis_retry_count: int = 3
    fail_open: bool = True


class ConfigRegistry:
    """Registry that maintains per-package cache configurations.

    Each library (identified by top-level package name) gets its own
    isolated configuration. This prevents configuration conflicts when
    multiple libraries use the cache package with different settings.
    """

    def __init__(self) -> None:
        self._configs: dict[str | None, CacheConfig] = {}
        self._default = CacheConfig()
        self._lock = threading.Lock()

    def configure(
        self,
        package: str | None = None,
        backend_default: str | None = None,
        key_prefix: str | None = None,
        file_dir: str | None = None,
        redis_url: str | None = None,
        lock_timeout: float | None = None,
        redis_health_check_interval: int | None = None,
        redis_socket_timeout: float | None = None,
        redis_retry_count: int | None = None,
        fail_open: bool | None = None,
    ) -> CacheConfig:
        """Configure cache for a specific package.
        """
        if package is None:
            package = _get_caller_package()

        updates = {
            'backend_default': backend_default,
            'key_prefix': key_prefix,
            'file_dir': str(file_dir) if file_dir else None,
            'redis_url': redis_url,
            'lock_timeout': lock_timeout,
            'redis_health_check_interval': redis_health_check_interval,
            'redis_socket_timeout': redis_socket_timeout,
            'redis_retry_count': redis_retry_count,
            'fail_open': fail_open,
        }
        updates = {k: v for k, v in updates.items() if v is not None}

        self._validate_config(updates)

        with self._lock:
            base = self._configs.get(package, self._default)
            new_cfg = replace(base, **updates)
            self._configs[package] = new_cfg

        logger.debug(f"Configured cache for package '{package}': {updates}")
        return new_cfg

    def _validate_config(self, kwargs: dict[str, Any]) -> None:
        """Validate configuration values.
        """
        if 'backend_default' in kwargs:
            backend = kwargs['backend_default']
            if backend not in VALID_BACKENDS:
                raise ValueError(f'backend must be one of {VALID_BACKENDS}, got {backend!r}')

        if 'file_dir' in kwargs:
            file_dir = kwargs['file_dir']
            if not pathlib.Path(file_dir).is_dir():
                raise ValueError(f'file_dir must be an existing directory, got {file_dir!r}')
            if not os.access(file_dir, os.W_OK):
                raise ValueError(f'file_dir must be writable, got {file_dir!r}')

    def get_config(self, package: str | None = None) -> CacheConfig:
        """Get config for a package, with fallback to default.
        """
        if package is None:
            package = _get_caller_package()

        if package in self._configs:
            return self._configs[package]

        return self._default

    def get_all_packages(self) -> list[str | None]:
        """Return list of configured packages.
        """
        return list(self._configs.keys())

    def clear(self) -> None:
        """Clear all package configurations. Primarily for testing.
        """
        self._configs.clear()


_registry = ConfigRegistry()


def configure(
    backend_default: str | None = None,
    key_prefix: str | None = None,
    file_dir: str | None = None,
    redis_url: str | None = None,
    lock_timeout: float | None = None,
    redis_health_check_interval: int | None = None,
    redis_socket_timeout: float | None = None,
    redis_retry_count: int | None = None,
    fail_open: bool | None = None,
) -> CacheConfig:
    """Configure cache settings for the caller's package.

    This is the main entry point for configuration. Each calling package
    gets its own isolated configuration.

    Args:
        backend_default: Default backend type ('memory', 'file', 'redis', 'null')
        key_prefix: Prefix for all cache keys (for versioning/debugging)
        file_dir: Directory for file-based caches
        redis_url: Redis connection URL (e.g., 'redis://localhost:6379/0')
        lock_timeout: Timeout for distributed locks in seconds (default: 10.0)
        redis_health_check_interval: Seconds between connection health checks (default: 30)
        redis_socket_timeout: Socket timeout in seconds (default: 5.0)
        redis_retry_count: Number of retries on connection failure (default: 3)
        fail_open: When True (default), backend read/lock errors degrade to a
            cache miss; when False they propagate to the caller.
    """
    return _registry.configure(
        backend_default=backend_default,
        key_prefix=key_prefix,
        file_dir=str(file_dir) if file_dir else None,
        redis_url=redis_url,
        lock_timeout=lock_timeout,
        redis_health_check_interval=redis_health_check_interval,
        redis_socket_timeout=redis_socket_timeout,
        redis_retry_count=redis_retry_count,
        fail_open=fail_open,
    )


def get_config(package: str | None = None) -> CacheConfig:
    """Get the CacheConfig for a specific package or the caller's package.
    """
    return _registry.get_config(package)


def get_all_configs() -> dict[str | None, dict[str, Any]]:
    """Return all package configurations as a dictionary.
    """
    result: dict[str | None, dict[str, Any]] = {'_default': asdict(_registry._default)}
    for pkg, cfg in _registry._configs.items():
        result[pkg] = asdict(cfg)
    return result
