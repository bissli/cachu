"""Cache decorator implementation with unified sync and async support.
"""
import asyncio
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from .api import NO_VALUE, CacheInfo, CacheMeta
from .config import VALID_BACKENDS, _get_caller_package, get_config, is_disabled
from .manager import manager
from .util import _is_connection_like, _predicate_arity, make_key_generator
from .util import make_partial_pattern, mangle_key, validate_entry

logger = logging.getLogger(__name__)

_MISSING = object()


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

    Automatically detects async functions and uses appropriate code path.
    Includes dogpile prevention using per-key mutexes.

    Args:
        ttl: Time-to-live in seconds (default: 300). Can also be a callable.
             Legacy form `ttl(result)` receives the function result and returns
             a TTL. Two-arg form `ttl(result, args)` additionally receives the
             filtered args dict (same view used for the cache key, with
             self/cls/_-prefixed/excluded/connection-like values dropped).
        backend: Backend type ('memory', 'file', 'redis'). Uses config default if None.
        tag: Tag for grouping related cache entries
        exclude: Parameter names to exclude from cache key
        cache_if: Predicate that decides whether a fresh result should be
                  written to the cache. Legacy form `cache_if(result)` or
                  two-arg form `cache_if(result, args)`. Returning False
                  bypasses the write; concurrent callers will each re-fetch.
        validate: Predicate that decides whether a cached entry is still
                  usable on hit. Legacy form `validate(entry)` or two-arg
                  form `validate(entry, args)`. Return False to recompute.
        package: Package name for config isolation. Auto-detected from the
                 calling module's top-level package if None. Use explicit
                 values when code may be vendored or bundled into other
                 packages.

    Per-call control via reserved kwargs (not passed to function):
        _skip_cache: If True, bypass cache completely for this call
        _overwrite_cache: If True, execute function and overwrite cached value

    Example:
        @cache(ttl=300, tag='users')
        def get_user(user_id: int) -> dict:
            return fetch_user(user_id)

        @cache(ttl=300, tag='users')
        async def get_user_async(user_id: int) -> dict:
            return await fetch_user(user_id)

        # Dynamic TTL based on result
        @cache(ttl=lambda result: result.get('cache_seconds', 300))
        def get_config(key: str) -> dict:
            return fetch_config(key)

        # Args-aware TTL: short for today, long for past
        import datetime
        @cache(ttl=lambda result, args: 900 if args['date'] == datetime.date.today() else 86400)
        def get_filings(date): ...

        # Args-aware cache_if: skip empty for today, cache empty for past
        @cache(cache_if=lambda result, args: bool(result) or args['date'] != datetime.date.today())
        def get_filings(date): ...

        # Normal call
        user = get_user(123)

        # Skip cache
        user = get_user(123, _skip_cache=True)

        # Force refresh
        user = get_user(123, _overwrite_cache=True)

        # Clear specific entry
        get_user.clear(user_id=123)

        # Refresh specific entry
        user = get_user.refresh(user_id=123)
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

                if is_disabled() or skip_cache:
                    return await fn(*args, **kwargs)

                backend_inst = await manager.aget_backend(
                    resolved_package,
                    resolved_backend,
                    ttl_for_backend,
                )
                cfg = get_config(resolved_package)

                base_key, args_dict = key_generator(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)
                fail_open = cfg.fail_open

                if not overwrite_cache:
                    value, created_at = await _safe_aget(
                        backend_inst, cache_key, fail_open, fn.__name__)

                    if value is not NO_VALUE and validate_entry(
                            value, created_at, validate, args_dict, validate_arity):
                        await _safe_aincr_stat(backend_inst, fn.__name__, 'hits')
                        return value

                mutex = backend_inst.get_async_mutex(cache_key)
                try:
                    acquired = await mutex.acquire(timeout=cfg.lock_timeout)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache lock acquire failed for {fn.__name__!r}; '
                        f'proceeding without lock', exc_info=True)
                    acquired = False
                try:
                    if not overwrite_cache:
                        value, created_at = await _safe_aget(
                            backend_inst, cache_key, fail_open, fn.__name__)
                        if value is not NO_VALUE and validate_entry(
                                value, created_at, validate, args_dict, validate_arity):
                            await _safe_aincr_stat(backend_inst, fn.__name__, 'hits')
                            return value

                    await _safe_aincr_stat(backend_inst, fn.__name__, 'misses')
                    result = await fn(*args, **kwargs)

                    if cache_if is None:
                        should_cache = True
                    elif cache_if_arity == 2:
                        should_cache = cache_if(result, args_dict)
                    else:
                        should_cache = cache_if(result)

                    if should_cache:
                        if not ttl_is_callable:
                            resolved_ttl = ttl
                        elif ttl_arity == 2:
                            resolved_ttl = ttl(result, args_dict)
                        else:
                            resolved_ttl = ttl(result)
                        try:
                            await backend_inst.aset(cache_key, result, resolved_ttl)
                            logger.debug(f'Cached {fn.__name__} with key {cache_key}')
                        except Exception:
                            logger.warning(
                                f'Cache set failed for {fn.__name__}',
                                exc_info=True)

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

                if is_disabled() or skip_cache:
                    return fn(*args, **kwargs)

                backend_inst = manager.get_backend(resolved_package, resolved_backend, ttl_for_backend)
                cfg = get_config(resolved_package)

                base_key, args_dict = key_generator(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)
                fail_open = cfg.fail_open

                if not overwrite_cache:
                    value, created_at = _safe_get(
                        backend_inst, cache_key, fail_open, fn.__name__)

                    if value is not NO_VALUE and validate_entry(
                            value, created_at, validate, args_dict, validate_arity):
                        _safe_incr_stat(backend_inst, fn.__name__, 'hits')
                        return value

                mutex = backend_inst.get_mutex(cache_key)
                try:
                    acquired = mutex.acquire(timeout=cfg.lock_timeout)
                except Exception:
                    if not fail_open:
                        raise
                    logger.warning(
                        f'Cache lock acquire failed for {fn.__name__!r}; '
                        f'proceeding without lock', exc_info=True)
                    acquired = False
                try:
                    if not overwrite_cache:
                        value, created_at = _safe_get(
                            backend_inst, cache_key, fail_open, fn.__name__)
                        if value is not NO_VALUE and validate_entry(
                                value, created_at, validate, args_dict, validate_arity):
                            _safe_incr_stat(backend_inst, fn.__name__, 'hits')
                            return value

                    _safe_incr_stat(backend_inst, fn.__name__, 'misses')
                    result = fn(*args, **kwargs)

                    if cache_if is None:
                        should_cache = True
                    elif cache_if_arity == 2:
                        should_cache = cache_if(result, args_dict)
                    else:
                        should_cache = cache_if(result)

                    if should_cache:
                        if not ttl_is_callable:
                            resolved_ttl = ttl
                        elif ttl_arity == 2:
                            resolved_ttl = ttl(result, args_dict)
                        else:
                            resolved_ttl = ttl(result)
                        try:
                            backend_inst.set(cache_key, result, resolved_ttl)
                            logger.debug(f'Cached {fn.__name__} with key {cache_key}')
                        except Exception:
                            logger.warning(
                                f'Cache set failed for {fn.__name__}',
                                exc_info=True)

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
