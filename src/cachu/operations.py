"""Cache CRUD operations for sync and async APIs.
"""
import logging
from collections.abc import Callable
from typing import Any

from .api import NO_VALUE, CacheInfo, CacheMeta
from .config import VALID_BACKENDS, _get_caller_package, get_config
from .decorator import get_async_cache_info, get_cache_info
from .manager import manager
from .util import _tag_to_pattern, mangle_key

logger = logging.getLogger(__name__)

_MISSING = object()


def _get_meta(fn: Callable[..., Any], decorator_name: str = '@cache') -> CacheMeta:
    """Get CacheMeta from a decorated function.
    """
    meta = getattr(fn, '_cache_meta', None)
    if meta is None:
        raise ValueError(f'{fn.__name__} is not decorated with {decorator_name}')
    return meta


def _clear_targets(
    tag: str | None,
    backend: str | None,
    package: str | None,
    global_clear: bool,
) -> tuple[str | None, list[str], str | None]:
    """Resolve what a clear should act on, shared by the sync and async paths.

    Parameters
    ----------
    tag : str or None
        Tag to scope the key glob to.
    backend : str or None
        Single backend name, or None for every shipped backend.
    package : str or None
        Package to clear for; auto-detected from the caller when None.
    global_clear : bool
        Skip key_prefix scoping.

    Returns
    -------
    tuple
        (key glob or None, backend names to visit, resolved package).
    """
    if package is None:
        package = _get_caller_package()

    backend_types = [backend] if backend is not None else list(VALID_BACKENDS)

    pattern = _tag_to_pattern(tag)
    if not global_clear:
        cfg = get_config(package)
        if cfg.key_prefix:
            prefix_glob = f'*:{cfg.key_prefix}*'
            pattern = f'{prefix_glob[:-1]}{pattern}' if pattern else prefix_glob

    return pattern, backend_types, package


def cache_get(fn: Callable[..., Any], default: Any = _MISSING, **kwargs: Any) -> Any:
    """Get a cached value without calling the function.

    Args:
        fn: A function decorated with @cache
        default: Value to return if not found (raises KeyError if not provided)
        **kwargs: Function arguments to build the cache key

    Returns
        The cached value or default

    Raises
        KeyError: If not found and no default provided
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = manager.get_backend(meta.package, meta.backend, meta.ttl)
    value = backend.get(cache_key)

    if value is NO_VALUE:
        if default is _MISSING:
            raise KeyError(f'No cached value for {fn.__name__} with {kwargs}')
        return default

    return value


def cache_set(fn: Callable[..., Any], value: Any, **kwargs: Any) -> None:
    """Set a cached value directly without calling the function.

    Args:
        fn: A function decorated with @cache
        value: The value to cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = manager.get_backend(meta.package, meta.backend, meta.ttl)
    backend.set(cache_key, value, meta.ttl)

    logger.debug(f'Set cache for {fn.__name__} with key {cache_key}')


def cache_delete(fn: Callable[..., Any], **kwargs: Any) -> None:
    """Delete a specific cached entry.

    Args:
        fn: A function decorated with @cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn)
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = manager.get_backend(meta.package, meta.backend, meta.ttl)
    backend.delete(cache_key)

    logger.debug(f'Deleted cache for {fn.__name__} with key {cache_key}')


def cache_clear(
    tag: str | None = None,
    backend: str | None = None,
    ttl: int | None = None,
    package: str | None = None,
    global_clear: bool = False,
) -> int:
    """Clear cache entries matching criteria.

    Parameters
    ----------
    tag : str or None, default None
        Clear only entries carrying this tag.
    backend : str or None, default None
        Backend type to clear: 'memory', 'file', 'redis' or 'null'. All
        backends if None.
    ttl : int or None, default None
        Specific TTL region to clear. All TTL regions if None.
    package : str or None, default None
        Package to clear for. Auto-detected from the caller if None.
    global_clear : bool, default False
        Skip key_prefix scoping and clear all matching keys.

    Returns
    -------
    int
        Number of entries cleared, which may be approximate.

    Notes
    -----
    - @cache registers its (package, backend, ttl) region at decoration
      time, so this call materializes the matching regions and clears them
      even in a cold process where no decorated call has run yet.
    - A setup fixture that clears before any cached call therefore really
      clears a shared backend instead of silently no-opping and letting a
      previous run's value be served.
    - A return of 0 means "no entries matched". When no region matched at
      all - usually a package or backend name that does not exist - a
      warning is logged, since the two cases are otherwise
      indistinguishable.
    - A failure on the backend you NAMED propagates: `backend=` says which
      store you meant, and a silently failed clear of it would be worse
      than a loud one.
    - A failure on a backend you did NOT name is logged and skipped. A
      sweeping `cache_clear(tag=...)` visits every declared region of the
      package, so one unreachable Redis would otherwise abort a clear whose
      target lived entirely in memory - and would do so only after paying
      that backend's full socket budget.
    - Which rule applies depends solely on the arguments, never on whether
      the process happened to be warm.
    """
    pattern, backend_types, package = _clear_targets(
        tag, backend, package, global_clear)
    total_cleared = 0

    if backend is not None and ttl is not None:
        backend_instance = manager.get_backend(package, backend, ttl)
        cleared = backend_instance.clear(pattern)
        backend_instance.clear_stats()
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {backend} backend (ttl={ttl})')
        return total_cleared

    manager.materialize(package, backend_types, ttl)
    targets = list(manager.iter_backends(
        package, backend_types=backend_types, ttl=ttl))

    for (_pkg, btype, bttl), backend_instance in targets:
        try:
            cleared = backend_instance.clear(pattern)
            backend_instance.clear_stats()
        except Exception:
            if backend is not None:
                raise
            logger.warning(
                f'Could not clear the {btype} backend (ttl={bttl}); it was '
                f'visited because no backend= was given', exc_info=True)
            continue
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {btype} backend (ttl={bttl})')

    if not targets:
        logger.warning(
            f'cache_clear found no cache region for package={package!r}, '
            f'backend={backend!r}, ttl={ttl!r}; nothing was cleared')

    return total_cleared


def cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for a decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize

    Raises
        ValueError: If function is not decorated with @cache
    """
    _get_meta(fn)
    return get_cache_info(fn)


async def async_cache_get(
    fn: Callable[..., Any],
    default: Any = _MISSING,
    **kwargs: Any,
) -> Any:
    """Get a cached value without calling the async function.

    Args:
        fn: A function decorated with @cache
        default: Value to return if not found (raises KeyError if not provided)
        **kwargs: Function arguments to build the cache key

    Returns
        The cached value or default

    Raises
        KeyError: If not found and no default provided
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn, '@cache')
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await manager.aget_backend(meta.package, meta.backend, meta.ttl)
    value = await backend.aget(cache_key)

    if value is NO_VALUE:
        if default is _MISSING:
            raise KeyError(f'No cached value for {fn.__name__} with {kwargs}')
        return default

    return value


async def async_cache_set(fn: Callable[..., Any], value: Any, **kwargs: Any) -> None:
    """Set a cached value directly without calling the async function.

    Args:
        fn: A function decorated with @cache
        value: The value to cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn, '@cache')
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await manager.aget_backend(meta.package, meta.backend, meta.ttl)
    await backend.aset(cache_key, value, meta.ttl)

    logger.debug(f'Set cache for {fn.__name__} with key {cache_key}')


async def async_cache_delete(fn: Callable[..., Any], **kwargs: Any) -> None:
    """Delete a specific cached entry.

    Args:
        fn: A function decorated with @cache
        **kwargs: Function arguments to build the cache key

    Raises
        ValueError: If function is not decorated with @cache
    """
    meta = _get_meta(fn, '@cache')
    cfg = get_config(meta.package)

    key_generator = fn._cache_key_generator
    base_key, _ = key_generator(**kwargs)
    cache_key = mangle_key(base_key, cfg.key_prefix, meta.ttl)

    backend = await manager.aget_backend(meta.package, meta.backend, meta.ttl)
    await backend.adelete(cache_key)

    logger.debug(f'Deleted cache for {fn.__name__} with key {cache_key}')


async def async_cache_clear(
    tag: str | None = None,
    backend: str | None = None,
    ttl: int | None = None,
    package: str | None = None,
    global_clear: bool = False,
) -> int:
    """Clear async cache entries matching criteria.

    Parameters
    ----------
    tag : str or None, default None
        Clear only entries carrying this tag.
    backend : str or None, default None
        Backend type to clear: 'memory', 'file', 'redis' or 'null'. All
        backends if None.
    ttl : int or None, default None
        Specific TTL region to clear. All TTL regions if None.
    package : str or None, default None
        Package to clear for. Auto-detected from the caller if None.
    global_clear : bool, default False
        Skip key_prefix scoping and clear all matching keys.

    Returns
    -------
    int
        Number of entries cleared, which may be approximate.

    Notes
    -----
    - Like `cache_clear`, this materializes the @cache-declared regions
      matching its arguments, so it works in a cold process.
    - A return of 0 means "no entries matched"; a warning is logged when no
      region matched at all.
    - A failure on the backend you named propagates; a failure on one merely
      swept because no `backend=` was given is logged and skipped.
    """
    pattern, backend_types, package = _clear_targets(
        tag, backend, package, global_clear)
    total_cleared = 0

    if backend is not None and ttl is not None:
        backend_instance = await manager.aget_backend(package, backend, ttl)
        cleared = await backend_instance.aclear(pattern)
        await backend_instance.aclear_stats()
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {backend} backend (ttl={ttl})')
        return total_cleared

    await manager.amaterialize(package, backend_types, ttl)
    targets = [
        item async for item in manager.aiter_backends(
            package,
            backend_types=backend_types,
            ttl=ttl,
        )
    ]

    for (_pkg, btype, bttl), backend_instance in targets:
        try:
            cleared = await backend_instance.aclear(pattern)
            await backend_instance.aclear_stats()
        except Exception:
            if backend is not None:
                raise
            logger.warning(
                f'Could not clear the {btype} backend (ttl={bttl}); it was '
                f'visited because no backend= was given', exc_info=True)
            continue
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {btype} backend (ttl={bttl})')

    if not targets:
        logger.warning(
            f'async_cache_clear found no cache region for package={package!r}, '
            f'backend={backend!r}, ttl={ttl!r}; nothing was cleared')

    return total_cleared


async def async_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for an async decorated function.

    Args:
        fn: A function decorated with @cache

    Returns
        CacheInfo with hits, misses, and currsize

    Raises
        ValueError: If function is not decorated with @cache
    """
    _get_meta(fn, '@cache')
    return await get_async_cache_info(fn)
