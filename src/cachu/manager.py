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
from .util import _normalize_tag

logger = logging.getLogger(__name__)


def _warn_if_deadline_unenforceable(
    cfg: CacheConfig,
    label: str,
    timeout_name: str,
    retry_name: str,
    retry_sleep_ceiling: float = 0.0,
) -> None:
    """Warn when a network backend's budgets make `cache_deadline` unreachable.

    Parameters
    ----------
    cfg : CacheConfig
        Resolved configuration for the owning package.
    label : str
        Backend name for the message, e.g. 'Redis'.
    timeout_name : str
        Config field holding the per-attempt timeout in seconds.
    retry_name : str
        Config field holding the retries added on top of the first attempt.
    retry_sleep_ceiling : float, default 0.0
        Worst-case seconds the client sleeps BETWEEN retries, on top of
        the per-attempt timeouts. Zero for redis-py, whose default backoff
        caps at half a second total; botocore's standard mode sleeps up to
        `min(2**attempt, 20)` seconds per retry.

    Notes
    -----
    - Both redis-py and botocore retry INSIDE a single operation and apply
      the timeout per attempt, so one uninterruptible call costs up to
      `timeout * (1 + retry_count)` plus the retry sleeps. For Redis,
      cachu shares one connect attempt across every resolved address, so
      the address count adds a fifth of the budget per extra address
      rather than multiplying the figure by the address count.
    - `cache_deadline` is only checked between steps, so a call already
      blocked in a socket read runs to completion. With the shipped defaults
      that floor is 5.0 * 4 = 20 s, which swamps a 1 s deadline entirely.
    - The mutex release in the `finally` is not gated on the budget either,
      so a call that held the lock can overrun by twice that floor.
    - Deriving the timeout from the deadline instead of warning was
      measured to be worse: it silently overrode an explicitly configured
      value and, on a healthy but slow endpoint, timed out every read and
      write - turning the cache into a 100% miss while `fail_open` hid it.
      The caller has to make that trade knowingly.
    """
    if cfg.cache_deadline is None:
        return

    timeout = getattr(cfg, timeout_name)
    retry_count = getattr(cfg, retry_name)
    per_operation = timeout * (1 + retry_count)
    if per_operation + retry_sleep_ceiling <= cfg.cache_deadline:
        return

    backoff_clause = (
        f' plus up to {retry_sleep_ceiling:g}s of retry backoff'
        if retry_sleep_ceiling else '')
    logger.warning(
        f'cache_deadline={cfg.cache_deadline}s cannot be honored: one '
        f'{label} operation may block for {timeout_name} * (1 + '
        f'{retry_name}) = {timeout} * {1 + retry_count} = '
        f'{per_operation:g}s{backoff_clause}, and an in-flight call cannot '
        f'be interrupted. Lower {timeout_name} and {retry_name} to bring '
        f'the worst case near the deadline.')


class CacheManager:
    """Unified manager for cache backends and statistics.
    """

    def __init__(self) -> None:
        self.backends: dict[tuple[str | None, str, int], Backend] = {}
        self._regions: dict[tuple[str | None, str, int], set[str]] = {}
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
            _warn_if_deadline_unenforceable(
                cfg, 'Redis', 'redis_socket_timeout', 'redis_retry_count')
            backend = RedisBackend(
                cfg.redis_url,
                cfg.lock_timeout,
                cfg.redis_health_check_interval,
                cfg.redis_socket_timeout,
                cfg.redis_retry_count,
            )
        elif backend_type == 'dynamodb':
            from .backends.dynamodb import DynamoDBBackend

            # Notes:
            # - botocore's standard retry mode sleeps up to
            #   random() * min(2**attempt, 20) seconds between retries,
            #   on top of the per-attempt timeouts; the ceiling below is
            #   that sum's upper bound, without which a deadline in the
            #   gap would silently look honorable.
            backoff_ceiling = float(sum(
                min(2 ** attempt, 20)
                for attempt in range(cfg.dynamodb_retry_count)))
            _warn_if_deadline_unenforceable(
                cfg, 'DynamoDB', 'dynamodb_timeout', 'dynamodb_retry_count',
                retry_sleep_ceiling=backoff_ceiling)
            backend = DynamoDBBackend(
                cfg.dynamodb_table,
                cfg.lock_timeout,
                cfg.dynamodb_region,
                cfg.dynamodb_endpoint_url,
                cfg.dynamodb_timeout,
                cfg.dynamodb_retry_count,
                cfg.dynamodb_consistent_reads,
            )
        elif backend_type == 'null':
            from .backends.null import NullBackend
            backend = NullBackend()
        else:
            raise BackendNotFoundError(f'Unknown backend type: {backend_type}')

        return backend

    def register_region(
        self,
        package: str | None,
        backend_type: str,
        ttl: int,
        tag: str = '',
    ) -> None:
        """Record that a (package, backend, ttl) cache region exists.

        Parameters
        ----------
        package : str or None
            Owning package of the region.
        backend_type : str
            One of VALID_BACKENDS.
        ttl : int
            TTL region identifier (-1 for dynamic TTL).
        tag : str, default ''
            Tag the declaring decorator carries. The empty tag records
            nothing.

        Notes
        -----
        - Called by @cache at decoration time, so the set of regions a
          process can hold is known from import time onward - before any
          decorated function has run and therefore before any backend has
          been instantiated.
        - This is what lets `cache_clear` reach a region in a cold process
          instead of silently clearing nothing.
        - Tags ACCUMULATE per region, because one (package, backend, ttl)
          region is shared by every decorator that resolves to it. Recording
          them is what lets a tag-scoped clear skip the regions that cannot
          hold that tag, instead of dialing every backend the package
          declared anywhere.
        - The empty tag is not recorded: it is the decorator default, so
          treating it as a tag would make `tag=''` read as "the untagged
          caches" while matching everything.
        - Tags are recorded NORMALIZED, the same form the cache key carries,
          so the region lookup and the key glob agree. `_normalize_tag` maps
          '|' to '.', so a region declared as 'a|b' is also reachable as
          'a.b' - which is what the glob would have matched anyway.
        """
        with self._lock:
            tags = self._regions.setdefault((package, backend_type, ttl), set())
            if tag:
                tags.add(_normalize_tag(tag))

    def get_regions(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
        tag: str | None = None,
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
        tag : str or None, default None
            Keep only regions a decorator declared with this tag; all of
            them if None or empty.

        Returns
        -------
        set of tuple
            Matching (package, backend_type, ttl) keys.
        """
        with self._lock:
            return {
                key for key, tags in self._regions.items()
                if key[0] == package
                and (not backend_types or key[1] in backend_types)
                and (ttl is None or key[2] == ttl)
                and (not tag or _normalize_tag(tag) in tags)
            }

    def declared_tags(self, package: str | None) -> set[str]:
        """Return every tag the regions of `package` declared.

        Parameters
        ----------
        package : str or None
            Owning package to match exactly.

        Returns
        -------
        set of str
            Non-empty tags recorded by @cache for this package, stripped of
            the pipes `_normalize_tag` wraps them in so they read as the
            caller wrote them.

        Notes
        -----
        - Used to explain a clear that matched nothing: a tag is recorded
          only once its decorator has been imported, so a process that
          never imported it cannot clear it.
        """
        with self._lock:
            return {
                tag.strip('|') for key, tags in self._regions.items()
                if key[0] == package
                for tag in tags
            }

    def materialize(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
        tag: str | None = None,
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
        tag : str or None, default None
            Build only regions declaring this tag; all of them if None.

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
        for key in self.get_regions(package, backend_types, ttl, tag):
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
        tag: str | None = None,
    ) -> int:
        """Async variant of materialize(), guarded by the async lock.

        Returns
        -------
        int
            Number of regions successfully instantiated or already live.
        """
        built = 0
        for key in self.get_regions(package, backend_types, ttl, tag):
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
                return
            for key in [k for k in self._regions if k[0] == package]:
                del self._regions[key]

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
        tag: str | None = None,
    ) -> Iterator[tuple[tuple[str | None, str, int], Backend]]:
        """Iterate over backend instances matching criteria.
        """
        yield from self._matching(package, backend_types, ttl, tag)

    async def aiter_backends(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
        tag: str | None = None,
    ) -> AsyncIterator[tuple[tuple[str | None, str, int], Backend]]:
        """Async iterate over backend instances matching criteria.
        """
        for item in self._matching(package, backend_types, ttl, tag):
            yield item

    def _matching(
        self,
        package: str | None,
        backend_types: list[str] | None = None,
        ttl: int | None = None,
        tag: str | None = None,
    ) -> list[tuple[tuple[str | None, str, int], Backend]]:
        """Snapshot the live backends matching criteria, taken under the lock.

        Notes
        -----
        - Returning a snapshot rather than yielding under the lock is what
          keeps `iter_backends` safe to consume lazily: holding a
          non-reentrant lock across a yield deadlocks any consumer whose
          loop body touches the manager again, which `cache_clear` does.
        - The tag filter is applied to the LIVE backends, not only to the
          regions materialized for this call: a region built earlier in the
          process is live regardless of which tag asked for it, so filtering
          at materialization alone would still hand a tag-scoped clear the
          backends it must not touch.
        """
        with self._lock:
            wanted = None if not tag else _normalize_tag(tag)
            declaring = None if wanted is None else {
                key for key, tags in self._regions.items() if wanted in tags}
            return [
                (key, backend) for key, backend in self.backends.items()
                if key[0] == package
                and (not backend_types or key[1] in backend_types)
                and (ttl is None or key[2] == ttl)
                and (declaring is None or key in declaring)
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
        backend_type: 'memory', 'file', 'redis', 'dynamodb' or 'null'. Uses config default if None.
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
        backend_type: 'memory', 'file', 'redis', 'dynamodb' or 'null'. Uses config default if None.
        package: Package name. Auto-detected if None.
        ttl: TTL in seconds (used for backend separation).
    """
    if package is None:
        package = _get_caller_package()

    if backend_type is None:
        cfg = get_config(package)
        backend_type = cfg.backend_default

    return await manager.aget_backend(package, backend_type, ttl)


def clear_backends(package: str | None = None, regions: bool = False) -> None:
    """Clear all backend instances for a package. Primarily for testing.

    Parameters
    ----------
    package : str or None, default None
        Package to clear for; every package when None.
    regions : bool, default False
        Also forget the (package, backend, ttl) regions that @cache
        registered, so a later `cache_clear` cannot materialize them again.

    Notes
    -----
    - Regions are registered at decoration time and deliberately outlive
      backend instances: that is what lets `cache_clear` reach a region in
      a cold process.
    - A test suite that declares decorators INSIDE test functions needs
      `regions=True` in its teardown, or one test's region is materialized
      and cleared by the next.
    """
    manager.clear(package)
    if regions:
        manager.clear_regions(package)


async def clear_async_backends(package: str | None = None) -> None:
    """Clear all async backend instances for a package. Primarily for testing.
    """
    await manager.aclear(package)
