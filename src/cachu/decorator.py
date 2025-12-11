"""Cache decorator implementation.
"""
import logging
import os
import threading
import time
from functools import wraps
from typing import Any
from collections.abc import Callable

from .backends import NO_VALUE, Backend
from .backends.file import FileBackend
from .backends.memory import MemoryBackend
from .config import _get_caller_package, get_config, is_disabled
from .keys import make_key_generator, mangle_key
from .types import CacheEntry, CacheInfo, CacheMeta

logger = logging.getLogger(__name__)

_backends: dict[tuple[str | None, str, int], Backend] = {}
_backends_lock = threading.Lock()

_stats: dict[int, tuple[int, int]] = {}
_stats_lock = threading.Lock()


def _get_backend(package: str | None, backend_type: str, ttl: int) -> Backend:
    """Get or create a backend instance.
    """
    key = (package, backend_type, ttl)

    with _backends_lock:
        if key in _backends:
            return _backends[key]

        cfg = get_config(package)

        if backend_type == 'memory':
            backend = MemoryBackend()
        elif backend_type == 'file':
            if ttl < 60:
                filename = f'cache{ttl}sec'
            elif ttl < 3600:
                filename = f'cache{ttl // 60}min'
            else:
                filename = f'cache{ttl // 3600}hour'

            if package:
                filename = f'{package}_{filename}'

            filepath = os.path.join(cfg.file_dir, filename)
            backend = FileBackend(filepath)
        elif backend_type == 'redis':
            from .backends.redis import RedisBackend
            backend = RedisBackend(cfg.redis_url, cfg.redis_distributed)
        else:
            raise ValueError(f'Unknown backend type: {backend_type}')

        _backends[key] = backend
        logger.debug(f"Created {backend_type} backend for package '{package}', {ttl}s TTL")
        return backend


def get_backend(backend_type: str | None = None, package: str | None = None, ttl: int = 300) -> Backend:
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
        backend_type = cfg.backend

    return _get_backend(package, backend_type, ttl)


def cache(
    ttl: int = 300,
    backend: str | None = None,
    tag: str = '',
    exclude: set[str] | None = None,
    cache_if: Callable[[Any], bool] | None = None,
    validate: Callable[[CacheEntry], bool] | None = None,
    package: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Cache decorator with configurable backend and behavior.

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
        @cache(ttl=300, tag='users')
        def get_user(user_id: int) -> dict:
            return fetch_user(user_id)

        # Normal call
        user = get_user(123)

        # Skip cache
        user = get_user(123, _skip_cache=True)

        # Force refresh
        user = get_user(123, _overwrite_cache=True)
    """
    resolved_package = package if package is not None else _get_caller_package()

    if backend is None:
        cfg = get_config(resolved_package)
        resolved_backend = cfg.backend
    else:
        resolved_backend = backend

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
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
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            skip_cache = kwargs.pop('_skip_cache', False)
            overwrite_cache = kwargs.pop('_overwrite_cache', False)

            if is_disabled() or skip_cache:
                return fn(*args, **kwargs)

            backend_instance = _get_backend(resolved_package, resolved_backend, ttl)
            cfg = get_config(resolved_package)

            base_key = key_generator(*args, **kwargs)
            cache_key = mangle_key(base_key, cfg.key_prefix, ttl)

            if not overwrite_cache:
                value, created_at = backend_instance.get_with_metadata(cache_key)

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
                            _record_hit(wrapper)
                            return value
                    else:
                        _record_hit(wrapper)
                        return value

            _record_miss(wrapper)
            result = fn(*args, **kwargs)

            should_cache = cache_if is None or cache_if(result)

            if should_cache:
                backend_instance.set(cache_key, result, ttl)
                logger.debug(f'Cached {fn.__name__} with key {cache_key}')

            return result

        wrapper._cache_meta = meta  # type: ignore
        wrapper._cache_key_generator = key_generator  # type: ignore

        return wrapper

    return decorator


def _record_hit(fn: Callable[..., Any]) -> None:
    """Record a cache hit for the function.
    """
    fn_id = id(fn)
    with _stats_lock:
        hits, misses = _stats.get(fn_id, (0, 0))
        _stats[fn_id] = (hits + 1, misses)


def _record_miss(fn: Callable[..., Any]) -> None:
    """Record a cache miss for the function.
    """
    fn_id = id(fn)
    with _stats_lock:
        hits, misses = _stats.get(fn_id, (0, 0))
        _stats[fn_id] = (hits, misses + 1)


def get_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for a decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize
    """
    fn_id = id(fn)

    with _stats_lock:
        hits, misses = _stats.get(fn_id, (0, 0))

    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=hits, misses=misses, currsize=0)

    backend_instance = _get_backend(meta.package, meta.backend, meta.ttl)
    cfg = get_config(meta.package)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
    pattern = f'*:{cfg.key_prefix}{fn_name}|*'

    currsize = backend_instance.count(pattern)

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


def clear_backends(package: str | None = None) -> None:
    """Clear all backend instances for a package. Primarily for testing.
    """
    with _backends_lock:
        if package is None:
            _backends.clear()
        else:
            keys_to_delete = [k for k in _backends if k[0] == package]
            for key in keys_to_delete:
                del _backends[key]
