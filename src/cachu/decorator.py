"""Cache decorator implementation with unified sync and async support.
"""
import asyncio
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from .api import NO_VALUE, CacheEntry, CacheInfo, CacheMeta
from .config import _get_caller_package, get_config, is_disabled
from .manager import manager
from .util import make_key_generator, mangle_key, validate_entry

logger = logging.getLogger(__name__)

_MISSING = object()


def cache(
    ttl: int | Callable[[Any], int] = 300,
    backend: str | None = None,
    tag: str = '',
    exclude: set[str] | None = None,
    cache_if: Callable[[Any], bool] | None = None,
    validate: Callable[[CacheEntry], bool] | None = None,
    package: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Universal cache decorator for sync and async functions.

    Automatically detects async functions and uses appropriate code path.
    Includes dogpile prevention using per-key mutexes.

    Args:
        ttl: Time-to-live in seconds (default: 300). Can be a callable that
             receives the result and returns the TTL (for dynamic expiration).
        backend: Backend type ('memory', 'file', 'redis'). Uses config default if None.
        tag: Tag for grouping related cache entries
        exclude: Parameter names to exclude from cache key
        cache_if: Function to determine if result should be cached.
                  Called with result value, caches if returns True.
        validate: Function to validate cached entries before returning.
                  Called with CacheEntry, returns False to recompute.
        package: Package name for config isolation. Auto-detected if None.

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

        # Normal call
        user = get_user(123)

        # Skip cache
        user = get_user(123, _skip_cache=True)

        # Force refresh
        user = get_user(123, _overwrite_cache=True)

        # Invalidate specific entry
        get_user.invalidate(user_id=123)

        # Refresh specific entry
        user = get_user.refresh(user_id=123)
    """
    ttl_is_callable = callable(ttl)
    ttl_for_backend = -1 if ttl_is_callable else ttl

    resolved_package = package if package is not None else _get_caller_package()

    if backend is None:
        cfg = get_config(resolved_package)
        resolved_backend = cfg.backend_default
    else:
        resolved_backend = backend

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        key_generator = make_key_generator(fn, tag, exclude)
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

                base_key = key_generator(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)

                if not overwrite_cache:
                    value, created_at = await backend_inst.aget_with_metadata(cache_key)

                    if value is not NO_VALUE and validate_entry(value, created_at, validate):
                        await backend_inst.aincr_stat(fn.__name__, 'hits')
                        return value

                mutex = backend_inst.get_async_mutex(cache_key)
                acquired = await mutex.acquire(timeout=cfg.lock_timeout)
                try:
                    if not overwrite_cache:
                        value, created_at = await backend_inst.aget_with_metadata(cache_key)
                        if value is not NO_VALUE and validate_entry(value, created_at, validate):
                            await backend_inst.aincr_stat(fn.__name__, 'hits')
                            return value

                    await backend_inst.aincr_stat(fn.__name__, 'misses')
                    result = await fn(*args, **kwargs)

                    if cache_if is None or cache_if(result):
                        resolved_ttl = ttl(result) if ttl_is_callable else ttl
                        await backend_inst.aset(cache_key, result, resolved_ttl)
                        logger.debug(f'Cached {fn.__name__} with key {cache_key}')

                    return result
                finally:
                    if acquired:
                        await mutex.release()

            async_wrapper._cache_meta = meta
            async_wrapper._cache_key_generator = key_generator
            _attach_helpers(async_wrapper, key_generator, resolved_package, resolved_backend, ttl_for_backend, is_async=True, original_fn=fn)
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

                base_key = key_generator(*args, **kwargs)
                cache_key = mangle_key(base_key, cfg.key_prefix, ttl_for_backend)

                if not overwrite_cache:
                    value, created_at = backend_inst.get_with_metadata(cache_key)

                    if value is not NO_VALUE and validate_entry(value, created_at, validate):
                        backend_inst.incr_stat(fn.__name__, 'hits')
                        return value

                mutex = backend_inst.get_mutex(cache_key)
                acquired = mutex.acquire(timeout=cfg.lock_timeout)
                try:
                    if not overwrite_cache:
                        value, created_at = backend_inst.get_with_metadata(cache_key)
                        if value is not NO_VALUE and validate_entry(value, created_at, validate):
                            backend_inst.incr_stat(fn.__name__, 'hits')
                            return value

                    backend_inst.incr_stat(fn.__name__, 'misses')
                    result = fn(*args, **kwargs)

                    if cache_if is None or cache_if(result):
                        resolved_ttl = ttl(result) if ttl_is_callable else ttl
                        backend_inst.set(cache_key, result, resolved_ttl)
                        logger.debug(f'Cached {fn.__name__} with key {cache_key}')

                    return result
                finally:
                    if acquired:
                        mutex.release()

            sync_wrapper._cache_meta = meta
            sync_wrapper._cache_key_generator = key_generator
            _attach_helpers(sync_wrapper, key_generator, resolved_package, resolved_backend, ttl_for_backend, is_async=False, original_fn=fn)
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
    currsize = await backend_instance.acount(pattern)

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


def _attach_helpers(
    wrapper: Callable[..., Any],
    key_generator: Callable[..., str],
    resolved_package: str | None,
    resolved_backend: str,
    ttl: int,
    is_async: bool,
    original_fn: Callable[..., Any],
) -> None:
    """Attach helper methods to wrapper (.invalidate, .refresh, .get, .set, .original).
    """
    if is_async:
        async def invalidate(**kwargs: Any) -> None:
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            await backend.adelete(cache_key)

        async def refresh(**kwargs: Any) -> Any:
            await invalidate(**kwargs)
            return await wrapper(**kwargs)

        async def get(default: Any = _MISSING, **kwargs: Any) -> Any:
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            value = await backend.aget(cache_key)
            if value is NO_VALUE:
                if default is _MISSING:
                    raise KeyError(f'No cached value for key {cache_key}')
                return default
            return value

        async def set(value: Any, **kwargs: Any) -> None:
            backend = await manager.aget_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            await backend.aset(cache_key, value, ttl)

        async def original(*args: Any, **kwargs: Any) -> Any:
            return await original_fn(*args, **kwargs)

        wrapper.invalidate = invalidate
        wrapper.refresh = refresh
        wrapper.get = get
        wrapper.set = set
        wrapper.original = original
    else:
        def invalidate(**kwargs: Any) -> None:
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            backend.delete(cache_key)

        def refresh(**kwargs: Any) -> Any:
            invalidate(**kwargs)
            return wrapper(**kwargs)

        def get(default: Any = _MISSING, **kwargs: Any) -> Any:
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            value = backend.get(cache_key)
            if value is NO_VALUE:
                if default is _MISSING:
                    raise KeyError(f'No cached value for key {cache_key}')
                return default
            return value

        def set(value: Any, **kwargs: Any) -> None:
            backend = manager.get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)
            cache_key = mangle_key(key_generator(**kwargs), cfg.key_prefix, ttl)
            backend.set(cache_key, value, ttl)

        def original(*args: Any, **kwargs: Any) -> Any:
            return original_fn(*args, **kwargs)

        wrapper.invalidate = invalidate
        wrapper.refresh = refresh
        wrapper.get = get
        wrapper.set = set
        wrapper.original = original
