"""Configuration module for cache backends with package isolation.

Each calling library gets its own isolated configuration, preventing
configuration conflicts when multiple libraries use the cachu package.
"""
import logging
import math
import os
import pathlib
import sys
import threading
from dataclasses import asdict, dataclass, replace
from typing import Any

from .exception import ConfigurationError

logger = logging.getLogger(__name__)

VALID_BACKENDS = ('memory', 'redis', 'file', 'null')
VALID_LOCK_TIMEOUT_ACTIONS = ('run', 'raise')

_disabled: bool = False
_disabled_packages: set[str] = set()
_disabled_tags: set[str] = set()
_disable_lock = threading.Lock()


@dataclass(frozen=True)
class DisabledScopes:
    """Snapshot of which caches are currently switched off.

    Attributes
    ----------
    globally : bool
        True when the process-wide switch set by `disable()` is on.
    packages : frozenset of str
        Packages switched off by `disable(package=...)`.
    tags : frozenset of str
        Tags switched off by `disable(tag=...)`.
    """
    globally: bool
    packages: frozenset[str]
    tags: frozenset[str]


def _is_positive_number(value: Any, allow_zero: bool = False) -> bool:
    """Check that a config value is a real, finite, non-negative number.

    Parameters
    ----------
    value : Any
        Candidate configuration value.
    allow_zero : bool, default False
        Whether 0 is acceptable.

    Returns
    -------
    bool
        True for a finite int or float above the applicable floor.

    Notes
    -----
    - `bool` is rejected even though it subclasses `int`, so a stray
      `True` cannot pass as the number 1.
    - NaN and the infinities are rejected: every ordering comparison
      against NaN is False, which silently disables whichever guard the
      value feeds.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if not math.isfinite(value):
        return False
    return value >= 0 if allow_zero else value > 0


def _is_whole_number(value: Any, minimum: int) -> bool:
    """Check that a config value is an int at or above `minimum`.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return False
    return value >= minimum


def _validate_scope(package: str | None, tag: str | None) -> None:
    """Reject an empty scope name for disable()/enable().

    Raises
    ------
    ValueError
        If `package` or `tag` is the empty string.

    Notes
    -----
    - `tag=''` is the decorator default, so accepting it would read as
      "switch off the untagged caches" while in fact matching nothing.
    - `package=''` can never be produced by `_get_caller_package`, so it can
      only be a caller mistake.
    """
    if package == '':
        raise ConfigurationError("package must be a non-empty name or None, got ''")
    if tag == '':
        raise ConfigurationError(
            "tag must be a non-empty name or None, got '' "
            '(the untagged caches cannot be scoped by tag; '
            'use package= or backend="null")')


def disable(package: str | None = None, tag: str | None = None) -> None:
    """Disable caching globally, or narrowed to a package and/or tag.

    Parameters
    ----------
    package : str or None, default None
        Disable only caches whose resolved package matches. When both
        `package` and `tag` are None the switch is global.
    tag : str or None, default None
        Disable only caches declared with this `tag=` on the decorator.

    Notes
    -----
    - `disable()` with no arguments keeps the historical global behaviour:
      every cache in the process is bypassed.
    - Scoped calls accumulate, so `disable(package='a')` followed by
      `disable(tag='docs')` switches off both scopes.
    - A cache matching either scope is bypassed; the two are OR-ed, not
      AND-ed.
    - Per-cache disabling without touching global state can also be
      expressed as `backend='null'` on that one decorator.

    Raises
    ------
    ValueError
        If `package` or `tag` is given as an empty string.
    """
    global _disabled
    _validate_scope(package, tag)
    with _disable_lock:
        if package is None and tag is None:
            _disabled = True
            return
        if package is not None:
            _disabled_packages.add(package)
        if tag is not None:
            _disabled_tags.add(tag)


def enable(package: str | None = None, tag: str | None = None) -> None:
    """Re-enable caching after disable().

    Parameters
    ----------
    package : str or None, default None
        Re-enable only this package's caches.
    tag : str or None, default None
        Re-enable only caches declared with this tag.

    Notes
    -----
    - `enable()` with no arguments clears the global switch AND every scoped
      disable, restoring a fully enabled process.
    - A scoped `enable()` cannot lift a global `disable()`: the global switch
      wins, so `disable()` then `enable(package='p')` leaves `p` disabled.
      Call `enable()` with no arguments first.

    Raises
    ------
    ValueError
        If `package` or `tag` is given as an empty string.
    """
    global _disabled
    _validate_scope(package, tag)
    with _disable_lock:
        if package is None and tag is None:
            _disabled = False
            _disabled_packages.clear()
            _disabled_tags.clear()
            return
        if package is not None:
            _disabled_packages.discard(package)
        if tag is not None:
            _disabled_tags.discard(tag)


def is_disabled(package: str | None = None, tag: str | None = None) -> bool:
    """Check whether caching is disabled globally or for the given scope.

    Parameters
    ----------
    package : str or None, default None
        Package to test against the scoped disable set.
    tag : str or None, default None
        Tag to test against the scoped disable set.

    Returns
    -------
    bool
        True when the global switch is on, or when either supplied scope has
        been disabled.

    Notes
    -----
    - The empty tag never matches. It is the decorator default, so a scope
      test against it would switch off every untagged cache; `disable()`
      rejects `tag=''` for the same reason.
    """
    if _disabled:
        return True
    if package is not None and package in _disabled_packages:
        return True
    if tag and tag in _disabled_tags:
        return True
    return False


def get_disabled_scopes() -> DisabledScopes:
    """Return a consistent snapshot of the disable state, for introspection.

    Returns
    -------
    DisabledScopes
        The global flag plus the disabled package and tag sets, copied to
        frozensets so a caller cannot mutate library state.
    """
    with _disable_lock:
        return DisabledScopes(
            globally=_disabled,
            packages=frozenset(_disabled_packages),
            tags=frozenset(_disabled_tags),
        )


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

    Attributes
    ----------
    backend_default : str, default 'memory'
        Backend used when a decorator does not name one.
    key_prefix : str, default ''
        Prefix applied to every cache key of this package.
    file_dir : str, default '/tmp'
        Directory holding the SQLite files of the 'file' backend.
    redis_url : str, default 'redis://localhost:6379/0'
        Connection URL for the 'redis' backend.
    lock_timeout : float, default 10.0
        Seconds a caller waits for the per-key dogpile mutex.
    redis_health_check_interval : int, default 30
        Seconds between redis-py connection health checks.
    redis_socket_timeout : float, default 5.0
        Passed to redis-py as BOTH socket_timeout and socket_connect_timeout.
    redis_retry_count : int, default 3
        Retry attempts redis-py makes per operation.
    fail_open : bool, default True
        Degrade backend faults to a cache miss instead of raising.
    cache_deadline : float or None, default None
        Total seconds of cache-attributable work allowed per decorated call,
        measured between backend operations: an operation already in flight
        is never interrupted, and time spent waiting for another caller's
        function is refunded rather than charged. None keeps the historical
        unbounded behaviour.
    on_lock_timeout : {'run', 'raise'}, default 'run'
        What a caller does when it fails to take the dogpile mutex.
    memory_maxsize : int or None, default None
        Entry bound for the 'memory' backend; None is unbounded.
    memory_sweep_interval : float, default 60.0
        Minimum seconds between amortized expired-entry sweeps of the
        'memory' backend. 0 sweeps on every operation; `float('inf')`
        disables sweeping and restores the pre-0.4.0 behaviour.

    Notes
    -----
    - Worst-case Redis latency for one cached call multiplies out rather than
      adding up: `redis_socket_timeout` applies to connect AND read, and is
      retried `redis_retry_count` times INSIDE one logical operation. A miss
      performs six round trips (get, mutex acquire, post-lock re-read, stat
      incr, set, mutex release); against a blackholed endpoint the acquire
      raises rather than polling, so five of them each pay a full budget.
    - That is a hang rather than an exception, so neither `fail_open` nor
      `try`/`except` shortens it - and neither does `cache_deadline`, which
      is checked only between operations. Only `redis_socket_timeout` and
      `redis_retry_count` bound a single in-flight call; `cache_deadline`
      bounds the cumulative work between them. They are complementary, not
      alternatives.
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
    cache_deadline: float | None = None
    on_lock_timeout: str = 'run'
    memory_maxsize: int | None = None
    memory_sweep_interval: float = 60.0


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
        cache_deadline: float | None = None,
        on_lock_timeout: str | None = None,
        memory_maxsize: int | None = None,
        memory_sweep_interval: float | None = None,
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
            'cache_deadline': cache_deadline,
            'on_lock_timeout': on_lock_timeout,
            'memory_maxsize': memory_maxsize,
            'memory_sweep_interval': memory_sweep_interval,
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

        Notes
        -----
        - `bool` is excluded from the numeric checks because it subclasses
          `int`, so `configure(memory_maxsize=True)` would otherwise be
          accepted as a bound of 1.
        - NaN is rejected explicitly: every comparison against it is False,
          so a NaN budget would stop `_budget_spent` ever firing and a NaN
          interval would sweep on every single operation, both while
          looking correctly configured.
        """
        if 'backend_default' in kwargs:
            backend = kwargs['backend_default']
            if backend not in VALID_BACKENDS:
                raise ConfigurationError(f'backend must be one of {VALID_BACKENDS}, got {backend!r}')

        if 'file_dir' in kwargs:
            file_dir = kwargs['file_dir']
            if not pathlib.Path(file_dir).is_dir():
                raise ConfigurationError(f'file_dir must be an existing directory, got {file_dir!r}')
            if not os.access(file_dir, os.W_OK):
                raise ConfigurationError(f'file_dir must be writable, got {file_dir!r}')

        if 'on_lock_timeout' in kwargs:
            action = kwargs['on_lock_timeout']
            if action not in VALID_LOCK_TIMEOUT_ACTIONS:
                raise ConfigurationError(
                    f'on_lock_timeout must be one of {VALID_LOCK_TIMEOUT_ACTIONS}, got {action!r}')

        for name in ('lock_timeout', 'cache_deadline', 'redis_socket_timeout'):
            if name in kwargs and not _is_positive_number(kwargs[name]):
                raise ConfigurationError(
                    f'{name} must be a positive number of seconds, got {kwargs[name]!r}')

        for name in ('redis_retry_count', 'redis_health_check_interval'):
            if name in kwargs and not _is_whole_number(kwargs[name], minimum=0):
                raise ConfigurationError(
                    f'{name} must be a non-negative integer, got {kwargs[name]!r}')

        if 'memory_maxsize' in kwargs and not _is_whole_number(kwargs['memory_maxsize'], minimum=1):
            raise ConfigurationError(
                f"memory_maxsize must be a positive integer, got {kwargs['memory_maxsize']!r}")

        if 'memory_sweep_interval' in kwargs:
            interval = kwargs['memory_sweep_interval']
            if interval != math.inf and not _is_positive_number(interval, allow_zero=True):
                raise ConfigurationError(
                    'memory_sweep_interval must be a non-negative number of '
                    f"seconds, or float('inf') to disable sweeping, got {interval!r}")

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
    cache_deadline: float | None = None,
    on_lock_timeout: str | None = None,
    memory_maxsize: int | None = None,
    memory_sweep_interval: float | None = None,
    package: str | None = None,
) -> CacheConfig:
    """Configure cache settings for a package.

    This is the main entry point for configuration. Each package gets its own
    isolated configuration; only the settings you pass are changed.

    Parameters
    ----------
    backend_default : str or None, default None
        Default backend type: 'memory', 'file', 'redis' or 'null'.
    key_prefix : str or None, default None
        Prefix for all cache keys (for versioning/debugging).
    file_dir : str or None, default None
        Directory for file-based caches.
    redis_url : str or None, default None
        Redis connection URL, e.g. 'redis://localhost:6379/0'.
    lock_timeout : float or None, default None
        Seconds a caller waits for the per-key dogpile mutex (default 10.0).
    redis_health_check_interval : int or None, default None
        Seconds between connection health checks (default 30).
    redis_socket_timeout : float or None, default None
        Socket timeout in seconds (default 5.0); applies to connect and read.
    redis_retry_count : int or None, default None
        Retries on connection failure (default 3).
    fail_open : bool or None, default None
        When True (default), backend construction, read and lock errors
        degrade to a cache miss; when False they propagate to the caller.
    cache_deadline : float or None, default None
        Total seconds of cache-attributable work allowed per decorated call.
        Neither the decorated function's own runtime nor time spent waiting
        on another caller's copy of it counts; that wait answers to
        `lock_timeout`. Once the budget is spent the remaining cache steps
        are skipped, so a wedged backend costs at most this much plus the
        one call already in flight, plus the mutex release if the lock was
        held - two uninterruptible operations, not one. None (default) keeps
        the unbounded behaviour.
    on_lock_timeout : {'run', 'raise'} or None, default None
        Behaviour when the dogpile mutex cannot be taken within
        `lock_timeout`. 'run' (default) executes the function anyway; 'raise'
        raises `cachu.CacheLockTimeout` so load can be shed instead of
        stampeding the backing store.
    memory_maxsize : int or None, default None
        Maximum live entries in the 'memory' backend, evicting
        least-recently-used entries past the bound. None (default) is
        unbounded.
    memory_sweep_interval : float or None, default None
        Minimum seconds between amortized sweeps of expired 'memory' backend
        entries (default 60.0). 0 sweeps on every operation; `float('inf')`
        disables sweeping entirely.
    package : str or None, default None
        Package whose configuration is being set. Auto-detected from the
        caller's top-level package when None. Pass it explicitly to configure
        a sibling package - for instance to give one latency-sensitive cache
        its own timeouts without touching the rest of the process.

    Returns
    -------
    CacheConfig
        The resulting configuration for `package`.

    Notes
    -----
    - Redis timeout budgets compound rather than add; see `CacheConfig` for
      the breakdown and prefer `cache_deadline` when a caller has a deadline.
    - Lowering `lock_timeout` alone does NOT shed load: the default
      `on_lock_timeout='run'` turns each giver-up into its own backend read,
      converting dogpile suppression into a stampede.

    Examples
    --------
    >>> configure(backend_default='redis', redis_url='redis://cache:6379/0')
    >>> configure(package='alpha', redis_socket_timeout=0.25, cache_deadline=1.0)
    >>> configure(package='beta', redis_socket_timeout=9.0)
    """
    return _registry.configure(
        package=package,
        backend_default=backend_default,
        key_prefix=key_prefix,
        file_dir=str(file_dir) if file_dir else None,
        redis_url=redis_url,
        lock_timeout=lock_timeout,
        redis_health_check_interval=redis_health_check_interval,
        redis_socket_timeout=redis_socket_timeout,
        redis_retry_count=redis_retry_count,
        fail_open=fail_open,
        cache_deadline=cache_deadline,
        on_lock_timeout=on_lock_timeout,
        memory_maxsize=memory_maxsize,
        memory_sweep_interval=memory_sweep_interval,
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
