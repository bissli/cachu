"""Async cache decorator implementation.
"""
import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from .backends import NO_VALUE
from .backends.async_base import AsyncBackend
from .backends.async_memory import AsyncMemoryBackend
from .config import _get_caller_package, get_config, is_disabled
from .keys import make_key_generator, mangle_key
from .types import CacheEntry, CacheInfo, CacheMeta

logger = logging.getLogger(__name__)

_async_backends: dict[tuple[str | None, str, int], AsyncBackend] = {}
_async_backends_lock = asyncio.Lock()

_async_stats: dict[int, tuple[int, int]] = {}
_async_stats_lock = asyncio.Lock()


async def _get_async_backend(package: str | None, backend_type: str, ttl: int) -> AsyncBackend:
    """Get or create an async backend instance.
    """
    key = (package, backend_type, ttl)

    async with _async_backends_lock:
        if key in _async_backends:
            return _async_backends[key]

        cfg = get_config(package)

        if backend_type == 'memory':
            backend: AsyncBackend = AsyncMemoryBackend()
        elif backend_type == 'file':
            from .backends.async_sqlite import AsyncSqliteBackend

            if ttl < 60:
                filename = f'cache{ttl}sec.db'
            elif ttl < 3600:
                filename = f'cache{ttl // 60}min.db'
            else:
                filename = f'cache{ttl // 3600}hour.db'

            if package:
                filename = f'{package}_{filename}'

            filepath = os.path.join(cfg.file_dir, filename)
            backend = AsyncSqliteBackend(filepath)
        elif backend_type == 'redis':
            from .backends.async_redis import AsyncRedisBackend
            backend = AsyncRedisBackend(cfg.redis_url, cfg.redis_distributed)
        else:
            raise ValueError(f'Unknown backend type: {backend_type}')

        _async_backends[key] = backend
        logger.debug(f"Created async {backend_type} backend for package '{package}', {ttl}s TTL")
        return backend


async def get_async_backend(
    backend_type: str | None = None,
    package: str | None = None,
    *,
    ttl: int,
) -> AsyncBackend:
    """Get an async backend instance.

    Args:
        backend_type: 'memory', 'file', or 'redis'. Uses config default if None.
        package: Package name. Auto-detected if None.
        ttl: TTL in seconds (used for backend separation).
    """
    if package is None:
        package = _get_caller_package()

    if backend_type is None:
        cfg = get_config(package)
        backend_type = cfg.backend

    return await _get_async_backend(package, backend_type, ttl)


def async_cache(
    ttl: int = 300,
    backend: str | None = None,
    tag: str = '',
    exclude: set[str] | None = None,
    cache_if: Callable[[Any], bool] | None = None,
    validate: Callable[[CacheEntry], bool] | None = None,
    package: str | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Async cache decorator with configurable backend and behavior.

    Args:
        ttl: Time-to-live in seconds (default: 300)
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
        @async_cache(ttl=300, tag='users')
        async def get_user(user_id: int) -> dict:
            return await fetch_user(user_id)

        # Normal call
        user = await get_user(123)

        # Skip cache
        user = await get_user(123, _skip_cache=True)

        # Force refresh
        user = await get_user(123, _overwrite_cache=True)
    """
    resolved_package = package if package is not None else _get_caller_package()

    if backend is None:
        cfg = get_config(resolved_package)
        resolved_backend = cfg.backend
    else:
        resolved_backend = backend

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        key_generator = make_key_generator(fn, tag, exclude)

        meta = CacheMeta(
            ttl=ttl,
            backend=resolved_backend,
            tag=tag,
            exclude=exclude or set(),
            cache_if=cache_if,
            validate=validate,
            package=resolved_package,
            key_generator=key_generator,
        )

        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            skip_cache = kwargs.pop('_skip_cache', False)
            overwrite_cache = kwargs.pop('_overwrite_cache', False)

            if is_disabled() or skip_cache:
                return await fn(*args, **kwargs)

            backend_instance = await _get_async_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)

            base_key = key_generator(*args, **kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)

            if not overwrite_cache:
                value, created_at = await backend_instance.get_with_metadata(cache_key)

                if value is not NO_VALUE:
                    if validate is not None and created_at is not None:
                        entry = CacheEntry(
                            value=value,
                            created_at=created_at,
                            age=time.time() - created_at,
                        )
                        if not validate(entry):
                            logger.debug(f'Cache validation failed for {fn.__name__}')
                        else:
                            await _record_async_hit(wrapper)
                            return value
                    else:
                        await _record_async_hit(wrapper)
                        return value

            await _record_async_miss(wrapper)
            result = await fn(*args, **kwargs)

            should_cache = cache_if is None or cache_if(result)

            if should_cache:
                await backend_instance.set(cache_key, result, ttl)
                logger.debug(f'Cached {fn.__name__} with key {cache_key}')

            return result

        wrapper._cache_meta = meta  # type: ignore
        wrapper._cache_key_generator = key_generator  # type: ignore

        return wrapper

    return decorator


async def _record_async_hit(fn: Callable[..., Any]) -> None:
    """Record a cache hit for the async function.
    """
    fn_id = id(fn)
    async with _async_stats_lock:
        hits, misses = _async_stats.get(fn_id, (0, 0))
        _async_stats[fn_id] = (hits + 1, misses)


async def _record_async_miss(fn: Callable[..., Any]) -> None:
    """Record a cache miss for the async function.
    """
    fn_id = id(fn)
    async with _async_stats_lock:
        hits, misses = _async_stats.get(fn_id, (0, 0))
        _async_stats[fn_id] = (hits, misses + 1)


async def get_async_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for an async decorated function.

    Args:
        fn: A function decorated with @async_cache

    Returns
        CacheInfo with hits, misses, and currsize
    """
    fn_id = id(fn)

    async with _async_stats_lock:
        hits, misses = _async_stats.get(fn_id, (0, 0))

    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=hits, misses=misses, currsize=0)

    backend_instance = await _get_async_backend(meta.package, meta.backend, meta.ttl)
    cfg = get_config(meta.package)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
    pattern = f'*:{cfg.key_prefix}{fn_name}|*'

    currsize = await backend_instance.count(pattern)

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


async def clear_async_backends(package: str | None = None) -> None:
    """Clear all async backend instances for a package. Primarily for testing.
    """
    async with _async_backends_lock:
        if package is None:
            for backend in _async_backends.values():
                await backend.close()
            _async_backends.clear()
        else:
            keys_to_delete = [k for k in _async_backends if k[0] == package]
            for key in keys_to_delete:
                await _async_backends[key].close()
                del _async_backends[key]
