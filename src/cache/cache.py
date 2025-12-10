import dbm
import inspect
import logging
import os
import pathlib
from collections.abc import Callable
from functools import partial, wraps
from typing import Any

from dogpile.cache import CacheRegion, make_region
from dogpile.cache.backends.file import AbstractFileLock
from dogpile.cache.region import DefaultInvalidationStrategy
from dogpile.util.readwrite_lock import ReadWriteMutex

from .config import _get_caller_namespace, config, get_config

logger = logging.getLogger(__name__)


def _is_connection_like(obj) -> bool:
    """Check if object appears to be a database connection.

    Uses heuristics to detect common database connection objects without
    requiring database library imports.
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

    Parameters
        namespace: The namespace string to normalize.

    Returns
        The namespace wrapped in pipes on both sides (e.g., |foo|).
    """
    if not namespace:
        return ''
    namespace = namespace.strip('|')
    namespace = namespace.replace('|', '.')
    return f'|{namespace}|'


def _create_namespace_filter(namespace: str) -> Callable[[str], bool]:
    """Create a filter function for namespace-based key matching.

    Parameters
        namespace: The namespace to filter by.

    Returns
        A function that returns True if a key matches the namespace.
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

    This function uses the provided function's argument specification to generate
    a unique cache key based on the arguments passed to the function.

    Parameters
        namespace: A string to be prefixed to the key. Can be empty.
        fn: The function for which the key is being generated.
        exclude_params: Optional set of parameter names to exclude from cache key.

    Returns
        A callable that generates a cache key string from the function's arguments.
    """
    exclude_params = exclude_params or set()
    namespace = f'{fn.__name__}|{_normalize_namespace(namespace)}' if namespace else f'{fn.__name__}'

    argspec = inspect.getfullargspec(fn)
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
        as_str = ' '.join(f'{str(k)}={str(v)}' for k, v in sorted(as_kwargs.items()))
        return f'{namespace}|{as_str}'

    return generate_key


def key_mangler_default(key: str) -> str:
    """Modify the key for debugging purposes by prefixing it with a debug marker.

    Parameters
        key: The original cache key.

    Returns
        A modified cache key with the debug marker prepended.
    """
    return f'{config.debug_key}{key}'


def key_mangler_region(key: str, region: str) -> str:
    """Modify the key for a specific region for debugging purposes.

    Parameters
        key: The original cache key.
        region: The cache region name.

    Returns
        A modified cache key that includes the region prefix and debug marker.
    """
    return f'{region}:{config.debug_key}{key}'


def _make_key_mangler(debug_key: str) -> Callable[[str], str]:
    """Create a key mangler with a captured debug_key.

    This ensures the debug_key is bound at region creation time,
    not resolved dynamically at key mangling time.
    """
    def mangler(key: str) -> str:
        return f'{debug_key}{key}'
    return mangler


def _make_region_key_mangler(debug_key: str, region_name: str) -> Callable[[str], str]:
    """Create a region key mangler with captured debug_key and region name."""
    def mangler(key: str) -> str:
        return f'{region_name}:{debug_key}{key}'
    return mangler


def should_cache_fn(value: Any) -> bool:
    """Determine if the given value should be cached.

    Parameters
        value: The value to evaluate for caching.

    Returns
        True if the value should be cached, False otherwise.
    """
    return bool(value)


def _seconds_to_region_name(seconds: int) -> str:
    """Convert seconds to a human-readable region name.

    Parameters
        seconds: The expiration time in seconds.

    Returns
        A short region name (e.g., '30s', '5m', '12h', '2d').
    """
    if seconds < 60:
        return f'{seconds}s'
    elif seconds < 3600:
        return f'{seconds // 60}m'
    elif seconds < 86400:
        return f'{seconds // 3600}h'
    else:
        return f'{seconds // 86400}d'


def get_redis_client(namespace: str | None = None):
    """Create a Redis client directly from config.

    Parameters
        namespace: Package namespace. Auto-detected from caller if not provided.

    Returns
        A redis.Redis client instance.
    """
    import redis
    if namespace is None:
        namespace = _get_caller_namespace()
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
    """Wrapper for CacheRegion that adds exclude_params support to cache_on_arguments.
    """
    def __init__(self, region: CacheRegion) -> None:
        self._region = region
        self._original_cache_on_arguments = region.cache_on_arguments

    def cache_on_arguments(
        self,
        namespace: str = '',
        should_cache_fn: Callable[[Any], bool] = should_cache_fn,
        exclude_params: set[str] | None = None,
        **kwargs
    ) -> Callable:
        """Cache function results based on arguments with optional parameter exclusion.
        """
        if exclude_params:
            custom_key_gen = partial(key_generator, exclude_params=exclude_params)
            return self._original_cache_on_arguments(
                namespace=namespace,
                should_cache_fn=should_cache_fn,
                function_key_generator=custom_key_gen,
                **kwargs
            )
        return self._original_cache_on_arguments(
            namespace=namespace,
            should_cache_fn=should_cache_fn,
            **kwargs
        )

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

    Note:
        This implementation may be replaced with portalocker in the future.
    """
    def __init__(self, filename: str) -> None:
        self.mutex = ReadWriteMutex()

    def acquire_read_lock(self, wait: bool) -> bool:
        """Acquire the read lock.

        Parameters
            wait: Flag indicating whether to wait for the lock.

        Returns
            True if the lock is acquired, False otherwise.
        """
        ret = self.mutex.acquire_read_lock(wait)
        return wait or ret

    def acquire_write_lock(self, wait: bool) -> bool:
        """Acquire the write lock.

        Parameters
            wait: Flag indicating whether to wait for the lock.

        Returns
            True if the lock is acquired, False otherwise.
        """
        ret = self.mutex.acquire_write_lock(wait)
        return wait or ret

    def release_read_lock(self) -> bool:
        """Release the read lock.

        Returns
            True if the lock is released successfully.
        """
        return self.mutex.release_read_lock()

    def release_write_lock(self) -> bool:
        """Release the write lock.

        Returns
            True if the lock is released successfully.
        """
        return self.mutex.release_write_lock()


class RedisInvalidator(DefaultInvalidationStrategy):

    def __init__(self, region: CacheRegion, delete_keys: bool = False) -> None:
        """Initialize the RedisInvalidator for a given CacheRegion.

        Parameters
            region: A dogpile.cache.CacheRegion instance that will be invalidated.
            delete_keys: If True, physically delete keys from Redis on invalidation.
                        If False, rely on timestamp-based invalidation only.
        """
        self.region = region
        self.delete_keys = delete_keys
        super().__init__()

    def invalidate(self, hard: bool = True) -> None:
        """Invalidate the cache region using timestamp-based invalidation.

        Parameters
            hard: If True, perform a hard invalidation.
        """
        super().invalidate(hard)
        if self.delete_keys:
            self._delete_backend_keys()

    def _delete_backend_keys(self) -> None:
        """Delete keys from Redis backend for this region.
        """
        client = self.region.backend.writer_client
        region_prefix = f'{self.region.name}:'
        deleted_count = 0
        for key in client.scan_iter(match=f'{region_prefix}*'):
            client.delete(key)
            deleted_count += 1
        logger.debug(f'Deleted {deleted_count} Redis keys for region "{self.region.name}"')


def _handle_all_regions(regions_dict: dict[tuple[str | None, int], CacheRegionWrapper], log_level: str = 'warning') -> Callable:
    """Decorator to handle clearing all cache regions when seconds=None.

    When seconds=None, iterates through all regions for the caller's namespace
    and calls the decorated function for each one.

    Parameters
        regions_dict: Dictionary of cache regions keyed by (namespace, seconds)
        log_level: Logging level to use when no regions exist (default 'warning')

    Returns
        Decorator function that wraps cache clearing functions
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(seconds: int = None, namespace: str = None) -> None:
            caller_ns = _get_caller_namespace()
            if seconds is None:
                # Find all regions for the caller's namespace
                regions_to_clear = [
                    (ns, secs) for (ns, secs) in regions_dict
                    if ns == caller_ns
                ]
                if not regions_to_clear:
                    log_func = getattr(logger, log_level)
                    cache_type = func.__name__.replace('clear_', '').replace('cache', ' cache')
                    log_func(f'No{cache_type} regions exist for namespace "{caller_ns}"')
                    return
                for ns, region_seconds in regions_to_clear:
                    func(region_seconds, namespace)
                return
            return func(seconds, namespace)
        return wrapper
    return decorator


_memory_cache_regions: dict[tuple[str | None, int], CacheRegionWrapper] = {}


def memorycache(seconds: int) -> CacheRegionWrapper:
    """Create or retrieve a memory cache region with a specified expiration time.

    Each calling namespace (package) gets its own isolated region.

    Parameters
        seconds: The expiration time in seconds for caching.

    Returns
        A configured memory cache region wrapper.
    """
    namespace = _get_caller_namespace()
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


def filecache(seconds: int) -> CacheRegionWrapper:
    """Create or retrieve a file cache region with a specified expiration time.

    Each calling namespace (package) gets its own isolated region.

    Parameters
        seconds: The expiration time in seconds for caching.

    Returns
        A configured file cache region wrapper.
    """
    namespace = _get_caller_namespace()
    cfg = get_config(namespace)
    key = (namespace, seconds)

    if seconds < 60:
        filename = f'cache{seconds}sec'
    elif seconds < 3600:
        filename = f'cache{seconds // 60}min'
    else:
        filename = f'cache{seconds // 3600}hour'

    # Include namespace in filename to isolate per-namespace
    if namespace:
        filename = f'{namespace}_{filename}'

    if key not in _file_cache_regions:
        region = make_region(
            function_key_generator=key_generator,
            key_mangler=_make_key_mangler(cfg.debug_key),
        ).configure(
            'dogpile.cache.dbm',
            expiration_time=seconds,
            arguments={
                'filename': os.path.join(cfg.tmpdir, filename),
                'lock_factory': CustomFileLock
            }
        )
        _file_cache_regions[key] = _wrap_cache_on_arguments(region)
        logger.debug(f"Created file cache region for namespace '{namespace}', {seconds}s TTL")
    return _file_cache_regions[key]


_redis_cache_regions: dict[tuple[str | None, int], CacheRegionWrapper] = {}


def rediscache(seconds: int) -> CacheRegionWrapper:
    """Create or retrieve a Redis cache region with a specified expiration time.

    Each calling namespace (package) gets its own isolated region.

    If the namespace's config has redis='dogpile.cache.null', falls back to
    memorycache to prevent silent failures.

    Parameters
        seconds: The expiration time in seconds for caching.

    Returns
        A configured Redis cache region wrapper.
    """
    namespace = _get_caller_namespace()
    cfg = get_config(namespace)
    key = (namespace, seconds)

    # Safety: if redis backend is null, fall back to memory cache
    if cfg.redis == 'dogpile.cache.null':
        logger.warning(
            f"rediscache() called from '{namespace}' but redis backend is null. "
            f"Falling back to memorycache({seconds})."
        )
        return memorycache(seconds)

    if key not in _redis_cache_regions:
        name = _seconds_to_region_name(seconds)

        region = make_region(name=name, function_key_generator=key_generator,
                             key_mangler=_make_region_key_mangler(cfg.debug_key, name))

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
        _redis_cache_regions[key] = _wrap_cache_on_arguments(region)
        logger.debug(f"Created redis cache region for namespace '{namespace}', {seconds}s TTL")
    return _redis_cache_regions[key]


@_handle_all_regions(_memory_cache_regions)
def clear_memorycache(seconds: int | None = None, namespace: str | None = None) -> None:
    """Clear a memory cache region.

    When decorated, supports seconds=None to clear all regions for the caller's namespace.

    Parameters
        seconds: Expiration time in seconds that identifies the region to clear
        namespace: Optional namespace to filter which keys to clear
    """
    caller_ns = _get_caller_namespace()
    cfg = get_config(caller_ns)
    region_key = (caller_ns, seconds)

    if region_key not in _memory_cache_regions:
        logger.warning(f'No memory cache region exists for namespace "{caller_ns}", {seconds} seconds')
        return

    cache_dict = _memory_cache_regions[region_key].actual_backend._cache

    if namespace is None:
        cache_dict.clear()
        logger.debug(f'Cleared all memory cache keys for namespace "{caller_ns}", {seconds} second region')
    else:
        matches_namespace = _create_namespace_filter(namespace)
        keys_to_delete = [key for key in list(cache_dict.keys()) if matches_namespace(key)]
        for key in keys_to_delete:
            del cache_dict[key]
        logger.debug(f'Cleared {len(keys_to_delete)} memory cache keys for namespace "{namespace}"')


@_handle_all_regions(_file_cache_regions)
def clear_filecache(seconds: int | None = None, namespace: str | None = None) -> None:
    """Clear a file cache region.

    When decorated, supports seconds=None to clear all regions for the caller's namespace.

    Parameters
        seconds: Expiration time in seconds that identifies the region to clear
        namespace: Optional namespace to filter which keys to clear
    """
    caller_ns = _get_caller_namespace()
    cfg = get_config(caller_ns)
    region_key = (caller_ns, seconds)

    if region_key not in _file_cache_regions:
        logger.warning(f'No file cache region exists for namespace "{caller_ns}", {seconds} seconds')
        return

    filename = _file_cache_regions[region_key].actual_backend.filename
    basename = pathlib.Path(filename).name
    filepath = os.path.join(cfg.tmpdir, basename)

    if namespace is None:
        db = dbm.open(filepath, 'n')
        db.close()
        logger.debug(f'Cleared all file cache keys for namespace "{caller_ns}", {seconds} second region')
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


def clear_rediscache(seconds: int | None = None, namespace: str | None = None) -> None:
    """Clear a redis cache region.

    Parameters
        seconds: Expiration time in seconds that identifies the region to clear.
                 If None, namespace must be provided to clear across all regions.
        namespace: Optional namespace to filter which keys to clear.
                   If None, clears all keys in the specified region.

    Raises
        ValueError: If both seconds and namespace are None.
    """
    if seconds is None and namespace is None:
        raise ValueError('Must specify seconds, namespace, or both')

    caller_ns = _get_caller_namespace()
    cfg = get_config(caller_ns)
    client = get_redis_client(caller_ns)
    deleted_count = 0

    if seconds is not None:
        region_name = _seconds_to_region_name(seconds)
        region_prefix = f'{region_name}:{cfg.debug_key}'

        if namespace is None:
            # Clear all keys in region
            for key in client.scan_iter(match=f'{region_prefix}*'):
                client.delete(key)
                deleted_count += 1
            logger.debug(f'Cleared {deleted_count} Redis keys for region "{region_name}"')
        else:
            # Clear namespace in region
            matches_namespace = _create_namespace_filter(namespace)
            for key in client.scan_iter(match=f'{region_prefix}*'):
                key_str = key.decode()
                key_without_region = key_str[len(region_name) + 1:]
                if matches_namespace(key_without_region):
                    client.delete(key)
                    deleted_count += 1
            logger.debug(f'Cleared {deleted_count} Redis keys for namespace "{namespace}" in region "{region_name}"')
    else:
        # namespace only - clear across ALL regions
        matches_namespace = _create_namespace_filter(namespace)
        for key in client.scan_iter(match=f'*:{cfg.debug_key}*'):
            key_str = key.decode()
            if ':' in key_str:
                key_without_region = key_str.split(':', 1)[1]
                if matches_namespace(key_without_region):
                    client.delete(key)
                    deleted_count += 1
        logger.debug(f'Cleared {deleted_count} Redis keys for namespace "{namespace}" across all regions')


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

    The backend is determined by config.default_backend which can be 'memory',
    'redis', or 'file'. This allows applications to switch cache backends
    via configuration (e.g., memory in dev, redis in prod).

    Parameters
        seconds: The expiration time in seconds for caching.

    Returns
        A configured cache region wrapper.
    """
    backend = config.default_backend
    if backend not in _BACKEND_MAP:
        raise ValueError(f'Unknown default_backend: {backend}. Must be one of: {list(_BACKEND_MAP.keys())}')
    return _BACKEND_MAP[backend][0](seconds)


def clear_defaultcache(seconds: int | None = None, namespace: str | None = None) -> None:
    """Clear the default cache region.

    Parameters
        seconds: Expiration time in seconds that identifies the region to clear.
                 If None, clears all regions (or requires namespace for redis).
        namespace: Optional namespace to filter which keys to clear.
    """
    return _BACKEND_MAP[config.default_backend][1](seconds, namespace)


def set_defaultcache_key(seconds: int, namespace: str, fn: Callable[..., Any], value: Any, **kwargs) -> None:
    """Set a specific cached entry in default cache.
    """
    return _BACKEND_MAP[config.default_backend][2](seconds, namespace, fn, value, **kwargs)


def delete_defaultcache_key(seconds: int, namespace: str, fn: Callable[..., Any], **kwargs) -> None:
    """Delete a specific cached entry from default cache.
    """
    return _BACKEND_MAP[config.default_backend][3](seconds, namespace, fn, **kwargs)


def clear_all_regions() -> None:
    """Clear all cache regions across all namespaces. Primarily for testing."""
    _memory_cache_regions.clear()
    _file_cache_regions.clear()
    _redis_cache_regions.clear()
    logger.debug('Cleared all cache region dictionaries')


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
