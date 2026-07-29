"""Cache manager for backend lifecycle and instance management.
"""
import logging
import os
import threading
from collections.abc import AsyncIterator, Iterator

from .api import Backend
from .backends.memory import MemoryBackend
from .backends.sqlite import SqliteBackend
from .config import CacheConfig, _get_caller_package, get_config
from .exception import BackendNotFoundError

logger = logging.getLogger(__name__)


def _warn_if_deadline_unenforceable(cfg: CacheConfig) -> None:
    """Warn when the Redis budgets make `cache_deadline` unreachable.

    Parameters
    ----------
    cfg : CacheConfig
        Resolved configuration for the owning package.

    Notes
    -----
    - redis-py retries INSIDE a single operation and applies the timeout to
      the connect and the read, so one uninterruptible call costs up to
      `redis_socket_timeout * (1 + redis_retry_count)`.
    - `cache_deadline` is only checked between steps, so a call already
      blocked in a socket read runs to completion. With the shipped defaults
      that floor is 5.0 * 4 = 20 s, which swamps a 1 s deadline entirely.
    - Deriving the socket timeout from the deadline instead of warning was
      measured to be worse: it silently overrode an explicitly configured
      value and, on a healthy but slow endpoint, timed out every read and
      write - turning the cache into a 100% miss while `fail_open` hid it.
      The caller has to make that trade knowingly.
    """
    if cfg.cache_deadline is None:
        return

    per_operation = cfg.redis_socket_timeout * (1 + cfg.redis_retry_count)
    if per_operation <= cfg.cache_deadline:
        return

    logger.warning(
        f'cache_deadline={cfg.cache_deadline}s cannot be honoured: one Redis '
        f'operation may block for redis_socket_timeout * (1 + '
        f'redis_retry_count) = {cfg.redis_socket_timeout} * '
        f'{1 + cfg.redis_retry_count} = {per_operation:g}s, and an in-flight '
        f'call cannot be interrupted. Lower redis_socket_timeout and '
        f'redis_retry_count to bring the worst case near the deadline.')


class CacheManager:
    """Unified manager for cache backends and statistics.
    """

    def __init__(self) -> None:
        self.backends: dict[tuple[str | None, str, int], Backend] = {}
        self._regions: set[tuple[str | None, str, int]] = set()
        self._lock = threading.RLock()

    # Notes:
    # - One reentrant lock guards `backends` and `_regions` for both the
    #   sync and the async API. A separate asyncio.Lock would not exclude
    #   the sync path, so the same region could be built twice and one
    #   instance orphaned: its writes invisible to everyone else, its
    #   client never closed.
    # - The lock is never held across an `await` or a `close()`. Either
    #   would stall every coroutine on the loop, since acquiring a
    #   threading lock from a coroutine blocks the whole thread.

    def _create_backend(
        self,
        package: str | None,
        backend_type: str,
        ttl: int,
    ) -> Backend:
        """Create a backend instance (called with the manager lock held).

        Notes
        -----
        - Emits no log record. Logging runs arbitrary user handlers and
          filters, and a filter that enriches records from a cachu-decorated
          lookup would re-enter the manager while its lock is held. Callers
          log after releasing it.
        """
        cfg = get_config(package)

        if backend_type == 'memory':
            backend: Backend = MemoryBackend(
                maxsize=cfg.memory_maxsize,
                sweep_interval=cfg.memory_sweep_interval,
            )
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
            _warn_if_deadline_unenforceable(cfg)
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
            raise BackendNotFoundError(f'Unknown backend type: {backend_type}')

        return backend

    def register_region(self, package: str | None, backend_type: str, ttl: int) -> None:
        """Record that a (package, backend, ttl) cache region exists.

        Parameters
        ----------
        package : str or None
            Owning package of the region.
        backend_type : str
            One of VALID_BACKENDS.
        ttl : int
            TTL region identifier (-1 for dynamic TTL).

        Notes
        -----
        - Called by @cache at decoration time, so the set of regions a
          process can hold is known from import time onward - before any
          decorated function has run and therefore before any backend has
          been instantiated.
        - This is what lets `cache_clear` reach a region in a cold process
          instead of silently clearing nothing.
        """
        with self._lock:
            self._regions.add((package, backend_type, ttl))

    def get_regions(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> set[tuple[str | None, str, int]]:
        """Return registered region keys matching the given criteria.

        Parameters
        ----------
        package : str or None
            Owning package to match exactly.
        backend_types : list of str or None, default None
            Backend names to keep; all of them if None or empty.
        ttl : int or None, default None
            TTL region to keep; all of them if None.

        Returns
        -------
        set of tuple
            Matching (package, backend_type, ttl) keys.
        """
        with self._lock:
            return {
                key for key in self._regions
                if key[0] == package
                and (not backend_types or key[1] in backend_types)
                and (ttl is None or key[2] == ttl)
            }

    def materialize(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> int:
        """Instantiate every registered-but-not-yet-built region matching criteria.

        Parameters
        ----------
        package : str or None
            Owning package to match exactly.
        backend_types : list of str or None, default None
            Backend names to build; all of them if None or empty.
        ttl : int or None, default None
            TTL region to build; all of them if None.

        Returns
        -------
        int
            Number of regions successfully instantiated or already live.

        Notes
        -----
        - Backend constructors are lazy (no socket opened, no file created),
          so this is cheap; the cost lands on the first real operation.
        - A construction failure is logged and skipped rather than raised, so
          one broken region cannot abort an operation that would otherwise
          succeed on the others.
        """
        built = 0
        for key in self.get_regions(package, backend_types, ttl):
            try:
                self.get_backend(*key)
            except Exception:
                logger.warning(f'Could not materialize cache region {key}', exc_info=True)
                continue
            built += 1
        return built

    async def amaterialize(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> int:
        """Async variant of materialize(), guarded by the async lock.

        Returns
        -------
        int
            Number of regions successfully instantiated or already live.
        """
        built = 0
        for key in self.get_regions(package, backend_types, ttl):
            try:
                await self.aget_backend(*key)
            except Exception:
                logger.warning(f'Could not materialize cache region {key}', exc_info=True)
                continue
            built += 1
        return built

    def get_backend(self, package: str | None, backend_type: str, ttl: int) -> Backend:
        """Get or create a backend instance (sync).
        """
        key = (package, backend_type, ttl)
        with self._lock:
            backend = self.backends.get(key)
            created = backend is None
            if created:
                backend = self._create_backend(package, backend_type, ttl)
                self.backends[key] = backend

        if created:
            logger.debug(
                f"Created {backend_type} backend for package '{package}', {ttl}s TTL")
        return backend

    async def aget_backend(
        self,
        package: str | None,
        backend_type: str,
        ttl: int,
    ) -> Backend:
        """Get or create a backend instance (async).

        Notes
        -----
        - Delegates to the sync path rather than taking a second lock.
          Backend constructors are lazy and non-blocking, so there is
          nothing to await, and two locks over one dict would let the same
          region be built twice.
        """
        return self.get_backend(package, backend_type, ttl)

    def clear_regions(self, package: str | None = None) -> None:
        """Forget registered regions for a package, or all of them. For testing.
        """
        with self._lock:
            if package is None:
                self._regions.clear()
            else:
                self._regions -= {k for k in self._regions if k[0] == package}

    def clear(self, package: str | None = None) -> None:
        """Clear backend instances (sync).
        """
        for backend in self._detach(package):
            backend.close()

    async def aclear(self, package: str | None = None) -> None:
        """Clear backend instances (async).
        """
        for backend in self._detach(package):
            await backend.aclose()

    def iter_backends(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> Iterator[tuple[tuple[str | None, str, int], Backend]]:
        """Iterate over backend instances matching criteria.
        """
        yield from self._matching(package, backend_types, ttl)

    async def aiter_backends(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> AsyncIterator[tuple[tuple[str | None, str, int], Backend]]:
        """Async iterate over backend instances matching criteria.
        """
        for item in self._matching(package, backend_types, ttl):
            yield item

    def _matching(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
    ) -> list[tuple[tuple[str | None, str, int], Backend]]:
        """Snapshot the live backends matching criteria, taken under the lock.

        Notes
        -----
        - Returning a snapshot rather than yielding under the lock is what
          keeps `iter_backends` safe to consume lazily: holding a
          non-reentrant lock across a yield deadlocks any consumer whose
          loop body touches the manager again, which `cache_clear` does.
        """
        with self._lock:
            return [
                (key, backend) for key, backend in self.backends.items()
                if key[0] == package
                and (not backend_types or key[1] in backend_types)
                and (ttl is None or key[2] == ttl)
            ]

    def _detach(self, package: str | None = None) -> list[Backend]:
        """Remove matching backends from the registry and return them.

        Notes
        -----
        - Closing happens outside the lock. `RedisBackend.close` and
          `SqliteBackend.close` join a helper thread for up to 5 s, and
          holding the lock across that would stall every coroutine and
          thread that touches the cache.
        """
        with self._lock:
            keys = [
                key for key in self.backends
                if package is None or key[0] == package
            ]
            return [self.backends.pop(key) for key in keys]


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
