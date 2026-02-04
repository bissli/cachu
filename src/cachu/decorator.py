"""Cache decorator implementation with unified sync and async support.
"""
import asyncio
import logging
import os
import threading
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from .backends import NO_VALUE, Backend
from .backends.memory import MemoryBackend
from .backends.sqlite import SqliteBackend
from .config import _get_caller_package, get_config, is_disabled
from .keys import make_key_generator, mangle_key
from .types import CacheEntry, CacheInfo, CacheMeta

logger = logging.getLogger(__name__)

_MISSING = object()


class CacheManager:
    """Unified manager for cache backends and statistics.
    """

    def __init__(self) -> None:
        self.backends: dict[tuple[str | None, str, int], Backend] = {}
        self.stats: dict[int, tuple[int, int]] = {}
        self._sync_lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        self._stats_lock = threading.Lock()

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
            backend = RedisBackend(cfg.redis_url, cfg.lock_timeout)
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

    def record_hit(self, fn: Callable[..., Any]) -> None:
        """Record a cache hit for the function.
        """
        fn_id = id(fn)
        with self._stats_lock:
            hits, misses = self.stats.get(fn_id, (0, 0))
            self.stats[fn_id] = (hits + 1, misses)

    def record_miss(self, fn: Callable[..., Any]) -> None:
        """Record a cache miss for the function.
        """
        fn_id = id(fn)
        with self._stats_lock:
            hits, misses = self.stats.get(fn_id, (0, 0))
            self.stats[fn_id] = (hits, misses + 1)

    def get_stats(self, fn: Callable[..., Any]) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        fn_id = id(fn)
        with self._stats_lock:
            return self.stats.get(fn_id, (0, 0))

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


def _validate_entry(
    value: Any,
    created_at: float | None,
    validate: Callable[[CacheEntry], bool] | None,
) -> bool:
    """Validate a cached entry using the validate callback.
    """
    if validate is None or created_at is None:
        return True

    entry = CacheEntry(
        value=value,
        created_at=created_at,
        age=time.time() - created_at,
    )
    return validate(entry)


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

                    if value is not NO_VALUE and _validate_entry(value, created_at, validate):
                        manager.record_hit(async_wrapper)
                        return value

                mutex = backend_inst.get_async_mutex(cache_key)
                acquired = await mutex.acquire(timeout=cfg.lock_timeout)
                try:
                    if not overwrite_cache:
                        value, created_at = await backend_inst.aget_with_metadata(cache_key)
                        if value is not NO_VALUE and _validate_entry(value, created_at, validate):
                            manager.record_hit(async_wrapper)
                            return value

                    manager.record_miss(async_wrapper)
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

                    if value is not NO_VALUE and _validate_entry(value, created_at, validate):
                        manager.record_hit(sync_wrapper)
                        return value

                mutex = backend_inst.get_mutex(cache_key)
                acquired = mutex.acquire(timeout=cfg.lock_timeout)
                try:
                    if not overwrite_cache:
                        value, created_at = backend_inst.get_with_metadata(cache_key)
                        if value is not NO_VALUE and _validate_entry(value, created_at, validate):
                            manager.record_hit(sync_wrapper)
                            return value

                    manager.record_miss(sync_wrapper)
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


def async_cache(
    ttl: int | Callable[[Any], int] = 300,
    backend: str | None = None,
    tag: str = '',
    exclude: set[str] | None = None,
    cache_if: Callable[[Any], bool] | None = None,
    validate: Callable[[CacheEntry], bool] | None = None,
    package: str | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Deprecated: Use @cache instead (auto-detects async).
    """
    return cache(
        ttl=ttl,
        backend=backend,
        tag=tag,
        exclude=exclude,
        cache_if=cache_if,
        validate=validate,
        package=package,
    )


def get_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for a decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize
    """
    hits, misses = manager.get_stats(fn)

    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=hits, misses=misses, currsize=0)

    backend_instance = manager.get_backend(meta.package, meta.backend, meta.ttl)
    cfg = get_config(meta.package)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
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
    hits, misses = manager.get_stats(fn)

    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        return CacheInfo(hits=hits, misses=misses, currsize=0)

    backend_instance = await manager.aget_backend(meta.package, meta.backend, meta.ttl)
    cfg = get_config(meta.package)

    fn_name = getattr(fn, '__wrapped__', fn).__name__
    pattern = f'*:{cfg.key_prefix}{fn_name}|*'

    currsize = await backend_instance.acount(pattern)

    return CacheInfo(hits=hits, misses=misses, currsize=currsize)


def clear_backends(package: str | None = None) -> None:
    """Clear all backend instances for a package. Primarily for testing.
    """
    manager.clear(package)


async def clear_async_backends(package: str | None = None) -> None:
    """Clear all async backend instances for a package. Primarily for testing.
    """
    await manager.aclear(package)


get_async_backend = aget_backend
