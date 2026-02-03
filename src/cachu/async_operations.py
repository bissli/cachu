"""Async cache CRUD operations.
"""
import logging
from collections.abc import Callable
from typing import Any

from .backends import NO_VALUE
from .config import _get_caller_package, get_config
from .async_decorator import _get_async_backend, get_async_cache_info
from .async_decorator import _async_backends, _async_backends_lock, _async_stats, _async_stats_lock
from .keys import mangle_key
from .types import CacheInfo, CacheMeta

logger = logging.getLogger(__name__)

_MISSING = object()


def _get_meta(fn: Callable[..., Any]) -> CacheMeta:
    """Get CacheMeta from a decorated function.
    """
    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        raise ValueError(f'{fn.__name__} is not decorated with @async_cache')
    return meta


async def async_cache_get(fn: Callable[..., Any], default: Any = _MISSING, **kwargs: Any) -> Any:
    """Get a cached value without calling the async function.

    Args:
        fn: A function decorated with @async_cache
        default: Value to return if not found (raises KeyError if not provided)
        **kwargs: Function arguments to build the cache key

    Returns
        The cached value or default

    Raises
        KeyError: If not found and no default provided
        ValueError: If function is not decorated with @async_cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await _get_async_backend(meta.package, meta.backend, meta.ttl)
    value = await backend.get(cache_key)

    if value is NO_VALUE:
        if default is _MISSING:
            raise KeyError(f'No cached value for {fn.__name__} with {kwargs}')
        return default

    return value


async def async_cache_set(fn: Callable[..., Any], value: Any, **kwargs: Any) -> None:
    """Set a cached value directly without calling the async function.

    Args:
        fn: A function decorated with @async_cache
        value: The value to cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @async_cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await _get_async_backend(meta.package, meta.backend, meta.ttl)
    await backend.set(cache_key, value, meta.ttl)

    logger.debug(f'Set cache for {fn.__name__} with key {cache_key}')


async def async_cache_delete(fn: Callable[..., Any], **kwargs: Any) -> None:
    """Delete a specific cached entry.

    Args:
        fn: A function decorated with @async_cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @async_cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await _get_async_backend(meta.package, meta.backend, meta.ttl)
    await backend.delete(cache_key)

    logger.debug(f'Deleted cache for {fn.__name__} with key {cache_key}')


async def async_cache_clear(
    tag: str | None = None,
    backend: str | None = None,
    ttl: int | None = None,
    package: str | None = None,
) -> int:
    """Clear async cache entries matching criteria.

    Args:
        tag: Clear only entries with this tag
        backend: Backend type to clear ('memory', 'file', 'redis'). Clears all if None.
        ttl: Specific TTL region to clear. Clears all TTLs if None.
        package: Package to clear for. Auto-detected if None.

    Returns
        Number of entries cleared (may be approximate)
    """
    if package is None:
        package = _get_caller_package()

    if backend is not None:
        backends_to_clear = [backend]
    else:
        backends_to_clear = ['memory', 'file', 'redis']

    if tag:
        from .keys import _normalize_tag
        pattern = f'*|{_normalize_tag(tag)}|*'
    else:
        pattern = None

    total_cleared = 0

    if backend is not None and ttl is not None:
        backend_instance = await _get_async_backend(package, backend, ttl)
        cleared = await backend_instance.clear(pattern)
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {backend} backend (ttl={ttl})')
    else:
        async with _async_backends_lock:
            for (pkg, btype, bttl), backend_instance in list(_async_backends.items()):
                if pkg != package:
                    continue
                if btype not in backends_to_clear:
                    continue
                if ttl is not None and bttl != ttl:
                    continue

                cleared = await backend_instance.clear(pattern)
                if cleared > 0:
                    total_cleared += cleared
                    logger.debug(f'Cleared {cleared} entries from {btype} backend (ttl={bttl})')

    async with _async_stats_lock:
        _async_stats.clear()

    return total_cleared


async def async_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for an async decorated function.

    Args:
        fn: A function decorated with @async_cache

    Returns
        CacheInfo with hits, misses, and currsize

    Raises
        ValueError: If function is not decorated with @async_cache
    """
    _get_meta(fn)
    return await get_async_cache_info(fn)
