"""Cache manager for backend lifecycle and instance management.
"""
import asyncio
import logging
import os
import threading
from collections.abc import AsyncIterator, Iterator

from .api import Backend
from .backends.memory import MemoryBackend
from .backends.sqlite import SqliteBackend
from .config import _get_caller_package, get_config

logger = logging.getLogger(__name__)


class CacheManager:
    """Unified manager for cache backends and statistics.
    """

    def __init__(self) -> None:
        self.backends: dict[tuple[str | None, str, int], Backend] = {}
        self._sync_lock = threading.Lock()
        self._async_lock = asyncio.Lock()

    def _create_backend(
        self,
        package: str | None,
        backend_type: str,
        ttl: int,
    ) -> Backend:
        """Create a backend instance (called with lock held).
        """
        cfg = get_config(package)

        if backend_type == 'memory':
            backend: Backend = MemoryBackend()
        elif backend_type == 'file':
            if ttl < 60:
                filename = f'cache{ttl}sec.db'
            elif ttl < 3600:
                filename = f'cache{ttl // 60}min.db'
            else:
                filename = f'cache{ttl // 3600}hour.db'

            if package:
                filename = f'{package}_{filename}'

            filepath = os.path.join(cfg.file_dir, filename)
            backend = SqliteBackend(filepath)
        elif backend_type == 'redis':
            from .backends.redis import RedisBackend
            backend = RedisBackend(
                cfg.redis_url,
                cfg.lock_timeout,
                cfg.redis_health_check_interval,
                cfg.redis_socket_timeout,
                cfg.redis_retry_count,
            )
        elif backend_type == 'null':
            from .backends.null import NullBackend
            backend = NullBackend()
        else:
            raise ValueError(f'Unknown backend type: {backend_type}')

        logger.debug(f"Created {backend_type} backend for package '{package}', {ttl}s TTL")
        return backend

    def get_backend(self, package: str | None, backend_type: str, ttl: int) -> Backend:
        """Get or create a backend instance (sync).
        """
        key = (package, backend_type, ttl)
        with self._sync_lock:
            if key not in self.backends:
                self.backends[key] = self._create_backend(package, backend_type, ttl)
            return self.backends[key]

    async def aget_backend(
        self,
        package: str | None,
        backend_type: str,
        ttl: int,
    ) -> Backend:
        """Get or create a backend instance (async).
        """
        key = (package, backend_type, ttl)
        async with self._async_lock:
            if key not in self.backends:
                self.backends[key] = self._create_backend(package, backend_type, ttl)
            return self.backends[key]

    def clear(self, package: str | None = None) -> None:
        """Clear backend instances (sync).
        """
        with self._sync_lock:
            if package is None:
                for backend in self.backends.values():
                    backend.close()
                self.backends.clear()
            else:
                keys_to_delete = [k for k in self.backends if k[0] == package]
                for key in keys_to_delete:
                    self.backends[key].close()
                    del self.backends[key]

    async def aclear(self, package: str | None = None) -> None:
        """Clear backend instances (async).
        """
        async with self._async_lock:
            if package is None:
                for backend in self.backends.values():
                    await backend.aclose()
                self.backends.clear()
            else:
                keys_to_delete = [k for k in self.backends if k[0] == package]
                for key in keys_to_delete:
                    await self.backends[key].aclose()
                    del self.backends[key]

    def iter_backends(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> Iterator[tuple[tuple[str | None, str, int], Backend]]:
        """Iterate over backend instances matching criteria.
        """
        with self._sync_lock:
            for key, backend in list(self.backends.items()):
                pkg, btype, bttl = key
                if pkg != package:
                    continue
                if backend_types and btype not in backend_types:
                    continue
                if ttl is not None and bttl != ttl:
                    continue
                yield key, backend

    async def aiter_backends(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> AsyncIterator[tuple[tuple[str | None, str, int], Backend]]:
        """Async iterate over backend instances matching criteria.
        """
        async with self._async_lock:
            for key, backend in list(self.backends.items()):
                pkg, btype, bttl = key
                if pkg != package:
                    continue
                if backend_types and btype not in backend_types:
                    continue
                if ttl is not None and bttl != ttl:
                    continue
                yield key, backend


manager = CacheManager()


def get_backend(
    backend_type: str | None = None,
    package: str | None = None,
    *,
    ttl: int,
) -> Backend:
    """Get a backend instance.

    Args:
        backend_type: 'memory', 'file', or 'redis'. Uses config default if None.
        package: Package name. Auto-detected if None.
        ttl: TTL in seconds (used for backend separation).
    """
    if package is None:
        package = _get_caller_package()

    if backend_type is None:
        cfg = get_config(package)
        backend_type = cfg.backend_default

    return manager.get_backend(package, backend_type, ttl)


async def aget_backend(
    backend_type: str | None = None,
    package: str | None = None,
    *,
    ttl: int,
) -> Backend:
    """Get a backend instance (async).

    Args:
        backend_type: 'memory', 'file', or 'redis'. Uses config default if None.
        package: Package name. Auto-detected if None.
        ttl: TTL in seconds (used for backend separation).
    """
    if package is None:
        package = _get_caller_package()

    if backend_type is None:
        cfg = get_config(package)
        backend_type = cfg.backend_default

    return await manager.aget_backend(package, backend_type, ttl)


def clear_backends(package: str | None = None) -> None:
    """Clear all backend instances for a package. Primarily for testing.
    """
    manager.clear(package)


async def clear_async_backends(package: str | None = None) -> None:
    """Clear all async backend instances for a package. Primarily for testing.
    """
    await manager.aclear(package)
