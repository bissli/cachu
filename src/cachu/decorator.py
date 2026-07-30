"""Cache decorator implementation with unified sync and async support.
"""
import asyncio
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from .api import NO_VALUE, CacheInfo, CacheMeta
from .config import VALID_BACKENDS, _get_caller_package, get_config
from .config import is_disabled
from .exception import CacheLockTimeout
from .manager import manager
from .util import _is_connection_like, _predicate_arity, make_key_generator
from .util import make_partial_pattern, mangle_key, validate_entry

logger = logging.getLogger(__name__)

_MISSING = object()


def _budget_spent(started: float | None, deadline: float | None) -> bool:
    """Report whether the per-call cache budget is exhausted.

    Parameters
    ----------
    started : float or None
        `time.monotonic()` origin of the budget, or None when unbounded.
    deadline : float or None
        Seconds of cache work allowed, or None when unbounded.

    Returns
    -------
    bool
        True only when a deadline is configured and it has elapsed.
    """
    if started is None or deadline is None:
        return False
    return (time.monotonic() - started) >= deadline


async def _safe_aget(
    backend_inst: Any,
    cache_key: str,
    fail_open: bool,
    fn_name: str,
) -> tuple[Any, float | None]:
    """Read from the backend, degrading a backend error to a miss when fail_open.
    """
    try:
        return await backend_inst.aget_with_metadata(cache_key)
    except Exception:
        if not fail_open:
            raise
        logger.warning(f'Cache read failed for {fn_name!r}; treating as miss', exc_info=True)
        return NO_VALUE, None


def _safe_get(
    backend_inst: Any,
    cache_key: str,
    fail_open: bool,
    fn_name: str,
) -> tuple[Any, float | None]:
    """Read from the backend, degrading a backend error to a miss when fail_open.
    """
    try:
        return backend_inst.get_with_metadata(cache_key)
    except Exception:
        if not fail_open:
            raise
        logger.warning(f'Cache read failed for {fn_name!r}; treating as miss', exc_info=True)
        return NO_VALUE, None


async def _safe_aincr_stat(backend_inst: Any, fn_name: str, stat: str) -> None:
    """Increment a stat counter; errors are swallowed (stats are best-effort).
    """
    try:
        await backend_inst.aincr_stat(fn_name, stat)
    except Exception:
        logger.warning(f'Cache stat update failed for {fn_name!r}', exc_info=True)


def _safe_incr_stat(backend_inst: Any, fn_name: str, stat: str) -> None:
    """Increment a stat counter; errors are swallowed (stats are best-effort).
    """
    try:
        backend_inst.incr_stat(fn_name, stat)
    except Exception:
        logger.warning(f'Cache stat update failed for {fn_name!r}', exc_info=True)


def _should_cache(
    result: Any,
    args_dict: dict[str, Any],
    cache_if: Callable[..., bool] | None,
    arity: int,
) -> bool:
    """Decide whether a fresh result should be written to the cache.

    Parameters
    ----------
    result : Any
        Value the decorated function returned.
    args_dict : dict
        Filtered args view, the same one used to build the cache key.
    cache_if : Callable or None
        Caller predicate; None means always cache.
    arity : int
        1 for `cache_if(result)`, 2 for `cache_if(result, args)`.

    Returns
    -------
    bool
        True when the write should proceed.
    """
    if cache_if is None:
        return True
    if arity == 2:
        return cache_if(result, args_dict)
    return cache_if(result)


def _resolve_ttl(
    ttl: int | Callable[..., int],
    result: Any,
    args_dict: dict[str, Any],
    is_callable: bool,
    arity: int,
) -> int:
    """Resolve a static or callable ttl for one write.

    Parameters
    ----------
    ttl : int or Callable
        The decorator's ttl argument.
    result : Any
        Value the decorated function returned.
    args_dict : dict
        Filtered args view, the same one used to build the cache key.
    is_callable : bool
        Whether `ttl` is a callable, decided once at decoration time.
    arity : int
        1 for `ttl(result)`, 2 for `ttl(result, args)`.

    Returns
    -------
    int
        TTL in seconds for this write.
    """
    if not is_callable:
        return ttl
    if arity == 2:
        return ttl(result, args_dict)
    return ttl(result)


def cache(
    ttl: int | Callable[..., int] = 300,
    backend: str | None = None,
    tag: str = '',
    exclude: set[str] | None = None,
    cache_if: Callable[..., bool] | None = None,
    validate: Callable[..., bool] | None = None,
    package: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Universal cache decorator for sync and async functions.

    Coroutine functions are detected automatically and take the async code
    path. Dogpiles are suppressed with a per-key mutex.

    Parameters
    ----------
    ttl : int or Callable, default 300
        Time-to-live in seconds, or a callable computing one. Legacy form
        `ttl(result)` receives the function result; two-arg form
        `ttl(result, args)` also receives the filtered args dict (the same
        view used for the cache key, with self/cls/_-prefixed/excluded/
        connection-like values dropped).
    backend : str or None, default None
        'memory', 'file', 'redis' or 'null'; the configured default if None.
        'null' never stores anything, so it switches off one cache without
        the process-wide `disable()`.
    tag : str, default ''
        Tag grouping related entries for selective clearing. Also the unit
        that `cachu.disable(tag=...)` switches off.
    exclude : set of str or None, default None
        Parameter names to leave out of the cache key.
    cache_if : Callable or None, default None
        Predicate deciding whether a fresh result is written. Legacy form
        `cache_if(result)` or two-arg `cache_if(result, args)`. Returning
        False bypasses the write, so concurrent callers each re-fetch.
    validate : Callable or None, default None
        Predicate deciding whether a cached entry is still usable on hit.
        Legacy form `validate(entry)` or two-arg `validate(entry, args)`.
        Return False to recompute.
    package : str or None, default None
        Package name selecting the configuration scope. Auto-detected from
        the calling module's top-level package if None; pass it explicitly
        when the code may be vendored or bundled into another package.

    Returns
    -------
    Callable
        Decorator wrapping a sync or async function with caching.

    Notes
    -----
    - Two reserved kwargs are consumed by the wrapper and never reach the
      function: `_skip_cache=True` bypasses the cache for that call, and
      `_overwrite_cache=True` executes and overwrites the stored value.
    - With `fail_open=True` (default) no cache fault reaches the caller.
      Backend construction and key generation degrade to running the
      function uncached; a read fault, a mutex-creation fault or a failed
      acquire degrade to a miss, so the function runs and its result is
      still written and counted.
    - `fail_open=False` propagates those faults instead.
    - Writes, stat updates and lock release are always best-effort and are
      logged rather than raised, whichever way `fail_open` is set: they run
      after the result already exists, so failing the call would discard a
      correct answer for a cache-only problem.
    - `fail_open` bounds exceptions, not hangs. A wedged Redis endpoint
      blocks inside socket timeouts and never raises, and `cache_deadline`
      cannot shorten it either - the budget is checked only between
      operations. Bound one blocked call with `redis_socket_timeout` and
      `redis_retry_count`; use `cache_deadline` for the cumulative work
      between them.
    - A lock timeout runs the function by default, so N waiters become N
      backend reads. Set `on_lock_timeout='raise'` to shed load instead.
      That raise is intentional and fires even under `fail_open=True`,
      because shedding load is a decision rather than a fault - but only a
      wait that genuinely expired sheds, never a spent budget.

    Examples
    --------
    >>> @cache(ttl=300, tag='users')
    ... def get_user(user_id: int) -> dict:
    ...     return fetch_user(user_id)

    >>> @cache(ttl=300, tag='users')
    ... async def get_user_async(user_id: int) -> dict:
    ...     return await fetch_user(user_id)

    Dynamic TTL taken from the result:

    >>> @cache(ttl=lambda result: result.get('cache_seconds', 300))
    ... def get_settings(key: str) -> dict:
    ...     return fetch_settings(key)

    Args-aware TTL and cache_if, short for today and long for past dates:

    >>> @cache(ttl=lambda r, a: 900 if a['date'] == today() else 86400)
    ... def get_filings(date): ...

    >>> @cache(cache_if=lambda r, a: bool(r) or a['date'] != today())
    ... def get_filings(date): ...

    Per-call control and helper methods:

    >>> user = get_user(123)
    >>> user = get_user(123, _skip_cache=True)
    >>> user = get_user(123, _overwrite_cache=True)
    >>> get_user.clear(user_id=123)
    >>> user = get_user.refresh(user_id=123)
    """
    ttl_is_callable = callable(ttl)
    ttl_for_backend = -1 if ttl_is_callable else ttl
    ttl_arity = _predicate_arity(ttl) if ttl_is_callable else 0
    cache_if_arity = _predicate_arity(cache_if) if cache_if is not None else 0
    validate_arity = _predicate_arity(validate) if validate is not None else 0

    resolved_package = package if package is not None else _get_caller_package()

    if backend is None:
        cfg = get_config(resolved_package)
        resolved_backend = cfg.backend_default
    else:
        if backend not in VALID_BACKENDS:
            raise ValueError(f'backend must be one of {VALID_BACKENDS}, got {backend!r}')
        resolved_backend = backend

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        logger.debug(
            f'@cache {fn.__name__}: package={resolved_package!r}, '
            f'backend={resolved_backend!r}, ttl={ttl_for_backend}')
        manager.register_region(resolved_package, resolved_backend, ttl_for_backend)
        key_generator = make_key_generator(fn, tag, exclude)
        fn_name = getattr(fn, '__wrapped__', fn).__name__
        is_async = asyncio.iscoroutinefunction(fn)

        meta = CacheMeta(
            ttl=ttl_for_backend,
            backend=resolved_backend,
            tag=tag,
            exclude=exclude or set(),
            cache_if=cache_if,
            validate=validate,
            package=resolved_package,
            key_generator=key_generator,
        )

        if is_async:
            @wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                skip_cache = kwargs.pop('_skip_cache', False)
                overwrite_cache = kwargs.pop('_overwrite_cache', False)

                if skip_cache or is_disabled(resolved_package, tag):
                    return await fn(*args, **kwargs)

                cfg = get_config(resolved_package)
                fail_open = cfg.fail_open
                deadline = cfg.cache_deadline
                started = time.monotonic() if deadline is not None else None

                try:
                    backend_inst = await manager.aget_backend(
                        resolved_package,
                        resolved_backend,
                        ttl_for_backend,
                    )
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache backend unavailable for {fn.__name__!r}; '
                        f'running uncached', exc_info=True)
                    return await fn(*args, **kwargs)

                try:
                    base_key, args_dict = key_generator(*args, **kwargs)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache key generation failed for {fn.__name__!r}; '
                        f'running uncached', exc_info=True)
                    return await fn(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)

                if not overwrite_cache and not _budget_spent(started, deadline):
                    value, created_at = await _safe_aget(
                        backend_inst, cache_key, fail_open, fn.__name__)

                    if value is not NO_VALUE and validate_entry(
                            value, created_at, validate, args_dict, validate_arity):
                        if not _budget_spent(started, deadline):
                            await _safe_aincr_stat(backend_inst, fn.__name__, 'hits')
                        return value

                try:
                    mutex = backend_inst.get_async_mutex(cache_key)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache mutex unavailable for {fn.__name__!r}; '
                        f'proceeding without lock', exc_info=True)
                    mutex = None

                acquired = False
                lock_faulted = False
                lock_attempted = False
                if mutex is not None and not _budget_spent(started, deadline):
                    lock_attempted = True
                    lock_started = time.monotonic()
                    try:
                        acquired = await mutex.acquire(timeout=cfg.lock_timeout)
                    except Exception:
                        if not fail_open:
                            raise
                        lock_faulted = True
                        logger.warning(
                            f'Cache lock acquire failed for {fn.__name__!r}; '
                            f'proceeding without lock', exc_info=True)
                    finally:
                        if started is not None:
                            started += time.monotonic() - lock_started
                try:
                    if not overwrite_cache and not _budget_spent(started, deadline):
                        value, created_at = await _safe_aget(
                            backend_inst, cache_key, fail_open, fn.__name__)
                        if value is not NO_VALUE and validate_entry(
                                value, created_at, validate, args_dict, validate_arity):
                            if not _budget_spent(started, deadline):
                                await _safe_aincr_stat(backend_inst, fn.__name__, 'hits')
                            return value

                    if (lock_attempted and not acquired and not lock_faulted
                            and cfg.on_lock_timeout == 'raise'):
                        raise CacheLockTimeout(
                            f'Waited {cfg.lock_timeout}s for the cache lock for '
                            f'{fn.__name__!r} without acquiring it and '
                            f'on_lock_timeout is "raise"; shedding rather than '
                            f'running the function')

                    if not _budget_spent(started, deadline):
                        await _safe_aincr_stat(backend_inst, fn.__name__, 'misses')

                    fn_started = time.monotonic()
                    result = await fn(*args, **kwargs)
                    if started is not None:
                        started += time.monotonic() - fn_started

                    if not _should_cache(result, args_dict, cache_if, cache_if_arity):
                        return result

                    if _budget_spent(started, deadline):
                        logger.warning(
                            f'Cache write skipped for {fn.__name__!r}: '
                            f'cache_deadline of {deadline}s exhausted. A cache '
                            f'whose read alone outlasts the budget can never '
                            f'populate; raise cache_deadline above the '
                            f'backend round trip or the cache stays cold')
                        return result

                    resolved_ttl = _resolve_ttl(
                        ttl, result, args_dict, ttl_is_callable, ttl_arity)
                    try:
                        await backend_inst.aset(cache_key, result, resolved_ttl)
                        logger.debug(f'Cached {fn.__name__} with key {cache_key}')
                    except Exception:
                        logger.warning(
                            f'Cache set failed for {fn.__name__}', exc_info=True)

                    return result
                finally:
                    if acquired:
                        try:
                            await mutex.release()
                        except Exception:
                            logger.warning(
                                f'Cache lock release failed for {fn.__name__!r}',
                                exc_info=True)

            async_wrapper._cache_meta = meta
            async_wrapper._cache_key_generator = key_generator
            _attach_helpers(async_wrapper, key_generator, resolved_package,
                            resolved_backend, ttl_for_backend, is_async=True,
                            original_fn=fn, fn_name=fn_name, tag=tag,
                            exclude=exclude)
            return async_wrapper

        else:
            @wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                skip_cache = kwargs.pop('_skip_cache', False)
                overwrite_cache = kwargs.pop('_overwrite_cache', False)

                if skip_cache or is_disabled(resolved_package, tag):
                    return fn(*args, **kwargs)

                cfg = get_config(resolved_package)
                fail_open = cfg.fail_open
                deadline = cfg.cache_deadline
                started = time.monotonic() if deadline is not None else None

                try:
                    backend_inst = manager.get_backend(
                        resolved_package, resolved_backend, ttl_for_backend)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache backend unavailable for {fn.__name__!r}; '
                        f'running uncached', exc_info=True)
                    return fn(*args, **kwargs)

                try:
                    base_key, args_dict = key_generator(*args, **kwargs)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache key generation failed for {fn.__name__!r}; '
                        f'running uncached', exc_info=True)
                    return fn(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)

                if not overwrite_cache and not _budget_spent(started, deadline):
                    value, created_at = _safe_get(
                        backend_inst, cache_key, fail_open, fn.__name__)

                    if value is not NO_VALUE and validate_entry(
                            value, created_at, validate, args_dict, validate_arity):
                        if not _budget_spent(started, deadline):
                            _safe_incr_stat(backend_inst, fn.__name__, 'hits')
                        return value

                try:
                    mutex = backend_inst.get_mutex(cache_key)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache mutex unavailable for {fn.__name__!r}; '
                        f'proceeding without lock', exc_info=True)
                    mutex = None

                acquired = False
                lock_faulted = False
                lock_attempted = False
                if mutex is not None and not _budget_spent(started, deadline):
                    lock_attempted = True
                    lock_started = time.monotonic()
                    try:
                        acquired = mutex.acquire(timeout=cfg.lock_timeout)
                    except Exception:
                        if not fail_open:
                            raise
                        lock_faulted = True
                        logger.warning(
                            f'Cache lock acquire failed for {fn.__name__!r}; '
                            f'proceeding without lock', exc_info=True)
                    finally:
                        if started is not None:
                            started += time.monotonic() - lock_started
                try:
                    if not overwrite_cache and not _budget_spent(started, deadline):
                        value, created_at = _safe_get(
                            backend_inst, cache_key, fail_open, fn.__name__)
                        if value is not NO_VALUE and validate_entry(
                                value, created_at, validate, args_dict, validate_arity):
                            if not _budget_spent(started, deadline):
                                _safe_incr_stat(backend_inst, fn.__name__, 'hits')
                            return value

                    if (lock_attempted and not acquired and not lock_faulted
                            and cfg.on_lock_timeout == 'raise'):
                        raise CacheLockTimeout(
                            f'Waited {cfg.lock_timeout}s for the cache lock for '
                            f'{fn.__name__!r} without acquiring it and '
                            f'on_lock_timeout is "raise"; shedding rather than '
                            f'running the function')

                    if not _budget_spent(started, deadline):
                        _safe_incr_stat(backend_inst, fn.__name__, 'misses')

                    fn_started = time.monotonic()
                    result = fn(*args, **kwargs)
                    if started is not None:
                        started += time.monotonic() - fn_started

                    if not _should_cache(result, args_dict, cache_if, cache_if_arity):
                        return result

                    if _budget_spent(started, deadline):
                        logger.warning(
                            f'Cache write skipped for {fn.__name__!r}: '
                            f'cache_deadline of {deadline}s exhausted. A cache '
                            f'whose read alone outlasts the budget can never '
                            f'populate; raise cache_deadline above the '
                            f'backend round trip or the cache stays cold')
                        return result

                    resolved_ttl = _resolve_ttl(
                        ttl, result, args_dict, ttl_is_callable, ttl_arity)
                    try:
                        backend_inst.set(cache_key, result, resolved_ttl)
                        logger.debug(f'Cached {fn.__name__} with key {cache_key}')
                    except Exception:
                        logger.warning(
                            f'Cache set failed for {fn.__name__}', exc_info=True)

                    return result
                finally:
                    if acquired:
                        try:
                            mutex.release()
                        except Exception:
                            logger.warning(
                                f'Cache lock release failed for {fn.__name__!r}',
                                exc_info=True)

            sync_wrapper._cache_meta = meta
            sync_wrapper._cache_key_generator = key_generator
            _attach_helpers(sync_wrapper, key_generator, resolved_package,
                            resolved_backend, ttl_for_backend, is_async=False,
                            original_fn=fn, fn_name=fn_name, tag=tag,
                            exclude=exclude)
            return sync_wrapper

    return decorator


def get_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for a decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize
    """
    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=0, misses=0, currsize=0)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
    backend_instance = manager.get_backend(meta.package, meta.backend, meta.ttl)
    hits, misses = backend_instance.get_stats(fn_name)

    cfg = get_config(meta.package)
    pattern = f'*:{cfg.key_prefix}{fn_name}|*'
    currsize = backend_instance.count(pattern)

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


_CURRSIZE_FRESH_TTL = 60
_CURRSIZE_LOCK_TTL = 30
_CURRSIZE_FRESH_PREFIX = 'cachu:_currsize:'
_CURRSIZE_LAST_PREFIX = 'cachu:_currsize_last:'
_CURRSIZE_LOCK_PREFIX = 'cachu:_currsize_lock:'

_background_tasks: set[asyncio.Task] = set()


def _currsize_keys(package: str | None, fn_name: str) -> tuple[str, str, str]:
    """Build the (fresh, last, lock) Redis keys for a (package, fn_name) pair.

    The variable part is wrapped in a '{...}' hash tag so all three keys map to
    the same Redis Cluster slot, keeping the multi-key MGET legal on cluster.
    """
    suffix = '{' + f'{package or "_"}:{fn_name}' + '}'
    return (
        f'{_CURRSIZE_FRESH_PREFIX}{suffix}',
        f'{_CURRSIZE_LAST_PREFIX}{suffix}',
        f'{_CURRSIZE_LOCK_PREFIX}{suffix}',
        )


async def _refresh_currsize_async(
    backend: Any,
    fresh_key: str,
    last_key: str,
    lock_key: str,
    pattern: str,
) -> None:
    """Recompute currsize via the slow scan and refresh both the fresh and last-known keys.
    """
    try:
        count = await backend.acount(pattern)
        client = backend._get_async_client()
        await client.set(fresh_key, count, ex=_CURRSIZE_FRESH_TTL)
        await client.set(last_key, count)
    except Exception:
        logger.exception('currsize refresh failed')
    finally:
        try:
            await backend._get_async_client().delete(lock_key)
        except Exception:
            pass


async def _get_cached_currsize_async(
    backend: Any,
    package: str | None,
    fn_name: str,
    pattern: str,
) -> int:
    """Stale-while-revalidate currsize for a Redis-backed function.

    Returns a fresh value if available; otherwise returns the last-known value
    (or 0 on cold start) and schedules a background refresh.
    """
    fresh_key, last_key, lock_key = _currsize_keys(package, fn_name)
    client = backend._get_async_client()
    fresh, last = await client.mget(fresh_key, last_key)
    if fresh is not None:
        return int(fresh)

    got_lock = await client.set(lock_key, b'1', nx=True, ex=_CURRSIZE_LOCK_TTL)
    if got_lock:
        task = asyncio.create_task(_refresh_currsize_async(
            backend, fresh_key, last_key, lock_key, pattern))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)

    return int(last) if last is not None else 0


async def get_async_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for an async decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize
    """
    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=0, misses=0, currsize=0)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
    backend_instance = await manager.aget_backend(meta.package, meta.backend, meta.ttl)
    hits, misses = await backend_instance.aget_stats(fn_name)

    cfg = get_config(meta.package)
    pattern = f'*:{cfg.key_prefix}{fn_name}|*'

    from .backends.redis import RedisBackend
    try:
        if isinstance(backend_instance, RedisBackend):
            currsize = await _get_cached_currsize_async(
                backend_instance, meta.package, fn_name, pattern)
        else:
            currsize = await backend_instance.acount(pattern)
    except Exception:
        if not get_config(meta.package).fail_open:
            raise
        logger.exception('currsize lookup failed; reporting 0')
        currsize = 0

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


def _attach_helpers(
    wrapper: Callable[..., Any],
    key_generator: Callable[..., str],
    resolved_package: str | None,
    resolved_backend: str,
    ttl: int,
    is_async: bool,
    original_fn: Callable[..., Any],
    fn_name: str = '',
    tag: str = '',
    exclude: set[str] | None = None,
) -> None:
    """Attach helper methods to wrapper (.clear, .refresh, .get, .set, .original).
    """
    exclude = exclude or frozenset()
    if is_async:
        async def clear(_global: bool = False, **kwargs: Any) -> int:
            filtered = {
                k: v for k, v in kwargs.items()
                if k not in {'self', 'cls'}
                and not k.startswith('_')
                and k not in exclude
                and not _is_connection_like(v)
            }
            if kwargs and not filtered:
                logger.warning(
                    f'{fn_name}.clear(): all kwargs were excluded from '
                    f'cache key ({", ".join(kwargs)}); clearing all entries')
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            pattern = make_partial_pattern(
                fn_name, tag, cfg.key_prefix, ttl,
                global_clear=_global, **filtered)
            return await backend.aclear(pattern)

        async def refresh(**kwargs: Any) -> Any:
            await clear(**kwargs)
            return await wrapper(**kwargs)

        async def get(default: Any = _MISSING, **kwargs: Any) -> Any:
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            base_key, _ = key_generator(**kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)
            value = await backend.aget(cache_key)
            if value is NO_VALUE:
                if default is _MISSING:
                    raise KeyError(f'No cached value for key {cache_key}')
                return default
            return value

        async def set(value: Any, **kwargs: Any) -> None:
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            base_key, _ = key_generator(**kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)
            await backend.aset(cache_key, value, ttl)

        async def original(*args: Any, **kwargs: Any) -> Any:
            return await original_fn(*args, **kwargs)

        wrapper.clear = clear
        wrapper.refresh = refresh
        wrapper.get = get
        wrapper.set = set
        wrapper.original = original
    else:
        def clear(_global: bool = False, **kwargs: Any) -> int:
            filtered = {
                k: v for k, v in kwargs.items()
                if k not in {'self', 'cls'}
                and not k.startswith('_')
                and k not in exclude
                and not _is_connection_like(v)
            }
            if kwargs and not filtered:
                logger.warning(
                    f'{fn_name}.clear(): all kwargs were excluded from '
                    f'cache key ({", ".join(kwargs)}); clearing all entries')
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            pattern = make_partial_pattern(
                fn_name, tag, cfg.key_prefix, ttl,
                global_clear=_global, **filtered)
            return backend.clear(pattern)

        def refresh(**kwargs: Any) -> Any:
            clear(**kwargs)
            return wrapper(**kwargs)

        def get(default: Any = _MISSING, **kwargs: Any) -> Any:
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            base_key, _ = key_generator(**kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)
            value = backend.get(cache_key)
            if value is NO_VALUE:
                if default is _MISSING:
                    raise KeyError(f'No cached value for key {cache_key}')
                return default
            return value

        def set(value: Any, **kwargs: Any) -> None:
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            base_key, _ = key_generator(**kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)
            backend.set(cache_key, value, ttl)

        def original(*args: Any, **kwargs: Any) -> Any:
            return original_fn(*args, **kwargs)

        wrapper.clear = clear
        wrapper.refresh = refresh
        wrapper.get = get
        wrapper.set = set
        wrapper.original = original
