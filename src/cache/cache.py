import dbm
import inspect
import logging
import os
import pathlib
import threading
from collections.abc import Callable
from functools import partial, wraps
from typing import Any

from dogpile.cache import CacheRegion, make_region
from dogpile.cache.backends.file import AbstractFileLock
from dogpile.cache.region import DefaultInvalidationStrategy
from dogpile.util.readwrite_lock import ReadWriteMutex

from .config import _get_caller_package, config, get_config, is_disabled

logger = logging.getLogger(__name__)


def _is_connection_like(obj: Any) -> bool:
    """Check if object appears to be a database connection.
    """
    if hasattr(obj, 'driver_connection'):
        return True

    if hasattr(obj, 'dialect'):
        return True

    if hasattr(obj, 'engine'):
        return True

    obj_type = str(type(obj))
    connection_indicators = ('Connection', 'Engine', 'psycopg', 'pyodbc', 'sqlite3')

    return any(indicator in obj_type for indicator in connection_indicators)


def _normalize_namespace(namespace: str) -> str:
    """Normalize namespace to always be wrapped in pipes.
    """
    if not namespace:
        return ''
    namespace = namespace.strip('|')
    namespace = namespace.replace('|', '.')
    return f'|{namespace}|'


def _create_namespace_filter(namespace: str) -> Callable[[str], bool]:
    """Create a filter function for namespace-based key matching.
    """
    debug_prefix = config.debug_key
    normalized_ns = _normalize_namespace(namespace)
    namespace_pattern = f'|{normalized_ns}|'

    def matches_namespace(key: str) -> bool:
        if not key.startswith(debug_prefix):
            return False
        key_after_prefix = key[len(debug_prefix):]
        return namespace_pattern in key_after_prefix

    return matches_namespace


def key_generator(namespace: str, fn: Callable[..., Any], exclude_params: set[str] | None = None) -> Callable[..., str]:
    """Generate a cache key for the given namespace and function.
    """
    exclude_params = exclude_params or set()
    unwrapped_fn = getattr(fn, '__wrapped__', fn)
    namespace = f'{unwrapped_fn.__name__}|{_normalize_namespace(namespace)}' if namespace else f'{unwrapped_fn.__name__}'

    argspec = inspect.getfullargspec(unwrapped_fn)
    _args_reversed = list(reversed(argspec.args or []))
    _defaults_reversed = list(reversed(argspec.defaults or []))
    args_with_defaults = { _args_reversed[i]: default for i, default in enumerate(_defaults_reversed)}

    def generate_key(*args, **kwargs) -> str:
        args, vargs = args[:len(argspec.args)], args[len(argspec.args):]
        as_kwargs = dict(**args_with_defaults)
        as_kwargs.update(dict(zip(argspec.args, args)))
        as_kwargs.update({f'vararg{i+1}': varg for i, varg in enumerate(vargs)})
        as_kwargs.update(**kwargs)
        as_kwargs = {k: v for k, v in as_kwargs.items() if not _is_connection_like(v) and k not in {'self', 'cls'}}
        as_kwargs = {k: v for k, v in as_kwargs.items() if not k.startswith('_') and k not in exclude_params}
        as_str = ' '.join(f'{str(k)}={repr(v)}' for k, v in sorted(as_kwargs.items()))
        return f'{namespace}|{as_str}'

    return generate_key


def key_mangler_default(key: str) -> str:
    """Modify the key for debugging purposes by prefixing it with a debug marker.
    """
    return f'{config.debug_key}{key}'


def key_mangler_region(key: str, region: str) -> str:
    """Modify the key for a specific region for debugging purposes.
    """
    return f'{region}:{config.debug_key}{key}'


def _make_key_mangler(debug_key: str) -> Callable[[str], str]:
    """Create a key mangler with a captured debug_key.
    """
    def mangler(key: str) -> str:
        return f'{debug_key}{key}'
    return mangler


def _make_region_key_mangler(debug_key: str, region_name: str) -> Callable[[str], str]:
    """Create a region key mangler with captured debug_key and region name.
    """
    def mangler(key: str) -> str:
        return f'{region_name}:{debug_key}{key}'
    return mangler


def should_cache_fn(value: Any) -> bool:
    """Determine if the given value should be cached.
    """
    return bool(value)


def _seconds_to_region_name(seconds: int) -> str:
    """Convert seconds to a human-readable region name.
    """
    if seconds < 60:
        return f'{seconds}s'
    elif seconds < 3600:
        return f'{seconds // 60}m'
    elif seconds < 86400:
        return f'{seconds // 3600}h'
    else:
        return f'{seconds // 86400}d'


def get_redis_client(namespace: str | None = None) -> Any:
    """Create a Redis client directly from config.
    """
    try:
        import redis
    except ImportError as e:
        raise RuntimeError(
            "Redis support requires the 'redis' package. Install with: pip install redis"
        ) from e
    if namespace is None:
        namespace = _get_caller_package()
    cfg = get_config(namespace)
    connection_kwargs = {}
    if cfg.redis_ssl:
        connection_kwargs['ssl'] = True
    return redis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        db=cfg.redis_db,
        **connection_kwargs
    )


class CacheRegionWrapper:
    """Wrapper for CacheRegion that adds exclude_params support.
    """

    def __init__(self, region: CacheRegion) -> None:
        self._region = region
        self._original_cache_on_arguments = region.cache_on_arguments

    def cache_on_arguments(
        self,
        namespace: str = '',
        should_cache_fn: Callable[[Any], bool] = should_cache_fn,
        exclude_params: set[str] | None = None,
        **kwargs) -> Callable:
        """Cache function results based on arguments with optional parameter exclusion.
        """
        if exclude_params:
            custom_key_gen = partial(key_generator, exclude_params=exclude_params)
            cache_decorator = self._original_cache_on_arguments(
                namespace=namespace,
                should_cache_fn=should_cache_fn,
                function_key_generator=custom_key_gen,
                **kwargs
            )
        else:
            cache_decorator = self._original_cache_on_arguments(
                namespace=namespace,
                should_cache_fn=should_cache_fn,
                **kwargs
            )

        def decorator(fn: Callable) -> Callable:
            cached_fn = cache_decorator(fn)

            @wraps(fn)
            def wrapper(*args, **kw):
                if is_disabled():
                    return fn(*args, **kw)
                return cached_fn(*args, **kw)
            return wrapper
        return decorator

    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes to the wrapped region.
        """
        return getattr(self._region, name)


def _wrap_cache_on_arguments(region: CacheRegion) -> CacheRegionWrapper:
    """Wrap CacheRegion to add exclude_params support with proper IDE typing.
    """
    return CacheRegionWrapper(region)


class CustomFileLock(AbstractFileLock):
    """Implementation of a file lock using a read-write mutex.
    """

    def __init__(self, filename: str) -> None:
        self.mutex = ReadWriteMutex()

    def acquire_read_lock(self, wait: bool) -> bool:
        """Acquire the read lock.
        """
        ret = self.mutex.acquire_read_lock(wait)
        return wait or ret

    def acquire_write_lock(self, wait: bool) -> bool:
        """Acquire the write lock.
        """
        ret = self.mutex.acquire_write_lock(wait)
        return wait or ret

    def release_read_lock(self) -> bool:
        """Release the read lock.
        """
        return self.mutex.release_read_lock()

    def release_write_lock(self) -> bool:
        """Release the write lock.
        """
        return self.mutex.release_write_lock()


class RedisInvalidator(DefaultInvalidationStrategy):
    """Redis invalidation strategy with optional key deletion.
    """

    def __init__(self, region: CacheRegion, delete_keys: bool = False) -> None:
        """Initialize the RedisInvalidator for a given CacheRegion.
        """
        self.region = region
        self.delete_keys = delete_keys
        super().__init__()

    def invalidate(self, hard: bool = True) -> None:
        """Invalidate the cache region using timestamp-based invalidation.
        """
        super().invalidate(hard)
        if self.delete_keys:
            self._delete_backend_keys()

    def _delete_backend_keys(self) -> None:
        """Delete keys from Redis backend for this region.
        """
        try:
            client = self.region.backend.writer_client
            region_prefix = f'{self.region.name}:'
            deleted_count = 0
            for key in client.scan_iter(match=f'{region_prefix}*'):
                client.delete(key)
                deleted_count += 1
            logger.debug(f'Deleted {deleted_count} Redis keys for region "{self.region.name}"')
        except Exception as e:
            logger.warning(f'Failed to delete Redis keys for region "{self.region.name}": {e}')


def _handle_all_regions(regions_dict: dict[tuple[str | None, int], CacheRegionWrapper], log_level: str = 'warning') -> Callable:
    """Decorator to handle clearing all cache regions when seconds=None.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(
            seconds: int | None = None,
            namespace: str | None = None,
            *,
            package: str | None = None,
        ) -> None:
            resolved_ns = package if package is not None else _get_caller_package()
            if seconds is None:
                regions_to_clear = [
                    (ns, secs) for (ns, secs) in regions_dict
                    if ns == resolved_ns
                ]
                if not regions_to_clear:
                    log_func = getattr(logger, log_level)
                    cache_type = func.__name__.replace('clear_', '').replace('cache', ' cache')
                    log_func(f'No{cache_type} regions exist for namespace "{resolved_ns}"')
                    return
                for _, region_seconds in regions_to_clear:
                    func(region_seconds, namespace, _resolved_namespace=resolved_ns)
                return
            return func(seconds, namespace, _resolved_namespace=resolved_ns)
        return wrapper
    return decorator


_region_lock = threading.Lock()
_memory_cache_regions: dict[tuple[str | None, int], CacheRegionWrapper] = {}


def memorycache(seconds: int, *, package: str | None = None) -> CacheRegionWrapper:
    """Create or retrieve a memory cache region with a specified expiration time.
    """
    with _region_lock:
        namespace = package if package is not None else _get_caller_package()
        cfg = get_config(namespace)
        key = (namespace, seconds)

        if key not in _memory_cache_regions:
            region = make_region(
                function_key_generator=key_generator,
                key_mangler=_make_key_mangler(cfg.debug_key),
            ).configure(
                cfg.memory,
                expiration_time=seconds,
            )
            _memory_cache_regions[key] = _wrap_cache_on_arguments(region)
            logger.debug(f"Created memory cache region for namespace '{namespace}', {seconds}s TTL")
        return _memory_cache_regions[key]


_file_cache_regions: dict[tuple[str | None, int], CacheRegionWrapper] = {}


def filecache(seconds: int, *, package: str | None = None) -> CacheRegionWrapper:
    """Create or retrieve a file cache region with a specified expiration time.
    """
    with _region_lock:
        namespace = package if package is not None else _get_caller_package()
        cfg = get_config(namespace)
        key = (namespace, seconds)

        if seconds < 60:
            filename = f'cache{seconds}sec'
        elif seconds < 3600:
            filename = f'cache{seconds // 60}min'
        else:
            filename = f'cache{seconds // 3600}hour'

        if namespace:
            filename = f'{namespace}_{filename}'

        if key not in _file_cache_regions:
            if cfg.file == 'dogpile.cache.null':
                logger.debug(
                    f"filecache() called from '{namespace}' with null backend - "
                    f"caching disabled for this region."
                )
                name = _seconds_to_region_name(seconds)
                region = make_region(name=name, function_key_generator=key_generator,
                                     key_mangler=_make_key_mangler(cfg.debug_key))
                region.configure('dogpile.cache.null')
            else:
                region = make_region(
                    function_key_generator=key_generator,
                    key_mangler=_make_key_mangler(cfg.debug_key),
                ).configure(
                    cfg.file,
                    expiration_time=seconds,
                    arguments={
                        'filename': os.path.join(cfg.tmpdir, filename),
                        'lock_factory': CustomFileLock
                    }
                )
                logger.debug(f"Created file cache region for namespace '{namespace}', {seconds}s TTL")
            _file_cache_regions[key] = _wrap_cache_on_arguments(region)
        return _file_cache_regions[key]


_redis_cache_regions: dict[tuple[str | None, int], CacheRegionWrapper] = {}


def rediscache(seconds: int, *, package: str | None = None) -> CacheRegionWrapper:
    """Create or retrieve a Redis cache region with a specified expiration time.
    """
    with _region_lock:
        namespace = package if package is not None else _get_caller_package()
        cfg = get_config(namespace)
        key = (namespace, seconds)

        if key not in _redis_cache_regions:
            name = _seconds_to_region_name(seconds)
            region = make_region(name=name, function_key_generator=key_generator,
                                 key_mangler=_make_region_key_mangler(cfg.debug_key, name))

            if cfg.redis == 'dogpile.cache.null':
                logger.debug(
                    f"rediscache() called from '{namespace}' with null backend - "
                    f"caching disabled for this region."
                )
                region.configure('dogpile.cache.null')
            else:
                connection_kwargs = {}
                if cfg.redis_ssl:
                    connection_kwargs['ssl'] = True

                region.configure(
                    cfg.redis,
                    arguments={
                        'host': cfg.redis_host,
                        'port': cfg.redis_port,
                        'db': cfg.redis_db,
                        'redis_expiration_time': seconds,
                        'distributed_lock': cfg.redis_distributed,
                        'thread_local_lock': not cfg.redis_distributed,
                        'connection_kwargs': connection_kwargs,
                    },
                    region_invalidator=RedisInvalidator(region)
                )
                logger.debug(f"Created redis cache region for namespace '{namespace}', {seconds}s TTL")
            _redis_cache_regions[key] = _wrap_cache_on_arguments(region)
        return _redis_cache_regions[key]


@_handle_all_regions(_memory_cache_regions)
def clear_memorycache(
    seconds: int | None = None,
    namespace: str | None = None,
    *,
    _resolved_namespace: str | None = None,
) -> None:
    """Clear a memory cache region.
    """
    pkg = _resolved_namespace if _resolved_namespace is not None else _get_caller_package()
    region_key = (pkg, seconds)

    if region_key not in _memory_cache_regions:
        logger.warning(f'No memory cache region exists for namespace "{pkg}", {seconds} seconds')
        return

    cache_dict = _memory_cache_regions[region_key].actual_backend._cache

    if namespace is None:
        cache_dict.clear()
        logger.debug(f'Cleared all memory cache keys for namespace "{pkg}", {seconds} second region')
    else:
        matches_namespace = _create_namespace_filter(namespace)
        keys_to_delete = [key for key in list(cache_dict.keys()) if matches_namespace(key)]
        for key in keys_to_delete:
            del cache_dict[key]
        logger.debug(f'Cleared {len(keys_to_delete)} memory cache keys for namespace "{namespace}"')


@_handle_all_regions(_file_cache_regions)
def clear_filecache(
    seconds: int | None = None,
    namespace: str | None = None,
    *,
    _resolved_namespace: str | None = None,
) -> None:
    """Clear a file cache region.
    """
    pkg = _resolved_namespace if _resolved_namespace is not None else _get_caller_package()
    cfg = get_config(pkg)
    region_key = (pkg, seconds)

    if region_key not in _file_cache_regions:
        logger.warning(f'No file cache region exists for namespace "{pkg}", {seconds} seconds')
        return

    filename = _file_cache_regions[region_key].actual_backend.filename
    basename = pathlib.Path(filename).name
    filepath = os.path.join(cfg.tmpdir, basename)

    if namespace is None:
        with dbm.open(filepath, 'n'):
            pass
        logger.debug(f'Cleared all file cache keys for namespace "{pkg}", {seconds} second region')
    else:
        matches_namespace = _create_namespace_filter(namespace)
        with dbm.open(filepath, 'w') as db:
            keys_to_delete = [
                key for key in list(db.keys())
                if matches_namespace(key.decode())
            ]
            for key in keys_to_delete:
                del db[key]
        logger.debug(f'Cleared {len(keys_to_delete)} file cache keys for namespace "{namespace}"')


@_handle_all_regions(_redis_cache_regions)
def clear_rediscache(
    seconds: int | None = None,
    namespace: str | None = None,
    *,
    _resolved_namespace: str | None = None,
) -> None:
    """Clear a redis cache region.
    """
    pkg = _resolved_namespace if _resolved_namespace is not None else _get_caller_package()
    cfg = get_config(pkg)
    client = get_redis_client(pkg)

    try:
        region_name = _seconds_to_region_name(seconds)
        region_prefix = f'{region_name}:{cfg.debug_key}'
        deleted_count = 0

        if namespace is None:
            for key in client.scan_iter(match=f'{region_prefix}*'):
                client.delete(key)
                deleted_count += 1
            logger.debug(f'Cleared {deleted_count} Redis keys for region "{region_name}"')
        else:
            matches_namespace = _create_namespace_filter(namespace)
            for key in client.scan_iter(match=f'{region_prefix}*'):
                key_str = key.decode()
                key_without_region = key_str[len(region_name) + 1:]
                if matches_namespace(key_without_region):
                    client.delete(key)
                    deleted_count += 1
            logger.debug(f'Cleared {deleted_count} Redis keys for namespace "{namespace}" in region "{region_name}"')
    finally:
        client.close()


def set_memorycache_key(seconds: int, namespace: str, fn: Callable[..., Any], value: Any, **kwargs) -> None:
    """Set a specific cached entry in memory cache.
    """
    region = memorycache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.set(cache_key, value)
    logger.debug(f'Set memory cache key for {fn.__name__} in namespace "{namespace}"')


def delete_memorycache_key(seconds: int, namespace: str, fn: Callable[..., Any], **kwargs) -> None:
    """Delete a specific cached entry from memory cache.
    """
    region = memorycache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.delete(cache_key)
    logger.debug(f'Deleted memory cache key for {fn.__name__} in namespace "{namespace}"')


def set_filecache_key(seconds: int, namespace: str, fn: Callable[..., Any], value: Any, **kwargs) -> None:
    """Set a specific cached entry in file cache.
    """
    region = filecache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.set(cache_key, value)
    logger.debug(f'Set file cache key for {fn.__name__} in namespace "{namespace}"')


def delete_filecache_key(seconds: int, namespace: str, fn: Callable[..., Any], **kwargs) -> None:
    """Delete a specific cached entry from file cache.
    """
    region = filecache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.delete(cache_key)
    logger.debug(f'Deleted file cache key for {fn.__name__} in namespace "{namespace}"')


def set_rediscache_key(seconds: int, namespace: str, fn: Callable[..., Any], value: Any, **kwargs) -> None:
    """Set a specific cached entry in redis cache.
    """
    region = rediscache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.set(cache_key, value)
    logger.debug(f'Set redis cache key for {fn.__name__} in namespace "{namespace}"')


def delete_rediscache_key(seconds: int, namespace: str, fn: Callable[..., Any], **kwargs) -> None:
    """Delete a specific cached entry from redis cache.
    """
    region = rediscache(seconds)
    cache_key = key_generator(namespace, fn)(**kwargs)
    region.delete(cache_key)
    logger.debug(f'Deleted redis cache key for {fn.__name__} in namespace "{namespace}"')


_BACKEND_MAP = {
    'memory': (memorycache, clear_memorycache, set_memorycache_key, delete_memorycache_key),
    'redis': (rediscache, clear_rediscache, set_rediscache_key, delete_rediscache_key),
    'file': (filecache, clear_filecache, set_filecache_key, delete_filecache_key),
}


def defaultcache(seconds: int) -> CacheRegionWrapper:
    """Return cache region based on configured default backend.
    """
    backend = config.default_backend
    if backend not in _BACKEND_MAP:
        raise ValueError(f'Unknown default_backend: {backend}. Must be one of: {list(_BACKEND_MAP.keys())}')
    return _BACKEND_MAP[backend][0](seconds)


def clear_defaultcache(seconds: int | None = None, namespace: str | None = None) -> None:
    """Clear the default cache region.
    """
    return _BACKEND_MAP[config.default_backend][1](seconds, namespace)


def set_defaultcache_key(seconds: int, namespace: str, fn: Callable[..., Any],
                         value: Any, **kwargs) -> None:
    """Set a specific cached entry in default cache.
    """
    return _BACKEND_MAP[config.default_backend][2](seconds, namespace, fn, value, **kwargs)


def delete_defaultcache_key(seconds: int, namespace: str,
                            fn: Callable[..., Any], **kwargs) -> None:
    """Delete a specific cached entry from default cache.
    """
    return _BACKEND_MAP[config.default_backend][3](seconds, namespace, fn, **kwargs)


def clear_cache_for_namespace(
    namespace: str,
    backend: str | None = None,
    seconds: int | None = None,
) -> None:
    """Clear cache regions for a specific namespace (cross-module safe).
    """
    backends = [backend] if backend else ['memory', 'file', 'redis']
    for b in backends:
        if b == 'memory':
            clear_memorycache(seconds=seconds, package=namespace)
        elif b == 'file':
            clear_filecache(seconds=seconds, package=namespace)
        elif b == 'redis':
            clear_rediscache(seconds=seconds, package=namespace)


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
