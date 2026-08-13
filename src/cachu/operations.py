"""Cache CRUD operations for sync and async APIs.
"""
import logging
from collections.abc import Callable
from typing import Any

from .api import NO_VALUE, CacheInfo, CacheMeta
from .config import VALID_BACKENDS, _get_caller_package, get_config
from .decorator import get_async_cache_info, get_cache_info
from .manager import manager
from .util import _normalize_tag, make_clear_pattern, mangle_key

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
) -> tuple[str | None, list[str], str | None, str]:
    """Resolve what a clear should act on, shared by the sync and async paths.

    Parameters
    ----------
    tag : str or None
        Tag to narrow the clear to.
    backend : str or None
        Single backend name, or None for every shipped backend.
    package : str or None
        Package to clear for; auto-detected from the caller when None.

    Returns
    -------
    tuple
        (normalized tag, backend names to visit, resolved package, key
        prefix to scope the globs to).

    Notes
    -----
    - Returns the INPUTS to a glob rather than a glob: the glob carries the
      region segment of the key, which is known per (backend, ttl) region
      and not once per call. Building it once from a `*:` wildcard was what
      let a clear of one TTL region reach another's entries - and, with no
      `key_prefix` configured, let it reach every key in the store.
    - An empty tag is normalized to None. `tag=''` is the decorator default,
      so treating it as a tag would narrow to regions that declared nothing.
    """
    if package is None:
        package = _get_caller_package()

    backend_types = [backend] if backend is not None else list(VALID_BACKENDS)

    return tag or None, backend_types, package, get_config(package).key_prefix


def _no_region_message(
    caller: str,
    package: str | None,
    backend: str | None,
    ttl: int | None,
    tag: str | None,
) -> str:
    """Explain a clear that matched no cache region at all.

    Parameters
    ----------
    caller : str
        Name of the entry point, for the log line.
    package, backend, ttl, tag
        The arguments that failed to match, echoed back.

    Returns
    -------
    str
        Warning text naming the package's declared tags when a tag was
        asked for and some other tag exists.

    Notes
    -----
    - A tag-scoped clear now visits only the regions declaring that tag, so
      "no region declares it" became reachable: an admin process that never
      imported the decorator would otherwise clear nothing in silence.
    - The import hint is only added when the tag is genuinely UNKNOWN.
      When the tag IS declared, the `backend=`/`ttl=` filters are what
      excluded it, and blaming a missing import would send the reader after
      the wrong cause.
    """
    message = (
        f'{caller} found no cache region for package={package!r}, '
        f'backend={backend!r}, ttl={ttl!r}, tag={tag!r}; nothing was cleared')
    if tag is None:
        return message

    declared = sorted(manager.declared_tags(package))
    if not declared:
        return message

    if _normalize_tag(tag).strip('|') in declared:
        return (
            f'{message}. Tag {tag!r} IS declared by this package, so it was '
            f'the backend={backend!r} or ttl={ttl!r} filter that excluded '
            f'every region carrying it')

    return (
        f'{message}. Regions of this package declare {declared}; a tag is '
        f'recorded when its @cache decorator is imported, so a clear from a '
        f'process that never imported it matches nothing')


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
        Backend type to clear: 'memory', 'file', 'redis', 'dynamodb' or 'null'. All
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
      all - usually a package, backend or tag name that does not exist - a
      warning is logged, since the two cases are otherwise
      indistinguishable.
    - A failure on the backend you NAMED propagates: `backend=` says which
      store you meant, and a silently failed clear of it would be worse
      than a loud one.
    - A failure on a backend you did NOT name is logged and skipped. An
      untagged `cache_clear()` visits every declared region of the package,
      so one unreachable Redis would otherwise abort a clear whose target
      lived entirely in memory - and would do so only after paying that
      backend's full socket budget.
    - Which rule applies depends solely on the arguments, never on whether
      the process happened to be warm.
    - Every glob is scoped to cachu's own key shape, region by region, so a
      clear can never delete a key cachu did not write. `global_clear`
      widens the key PREFIX, not the namespace.
    - A `tag=` clear visits only the regions whose @cache declared that tag,
      so a memory-pinned tag performs no network I/O even in a package that
      configures Redis for other caches. That narrowing needs the decorator
      to have been imported, which is what the "no cache region" warning
      names the declared tags for.
    - Naming both `backend=` and `ttl=` addresses that one region directly
      and is deliberately NOT tag-narrowed: those arguments already say
      which store you meant, exactly as they already make its failures
      propagate. The tag still scopes the key glob.
    """
    tag, backend_types, package, key_prefix = _clear_targets(tag, backend, package)
    total_cleared = 0

    if backend is not None and ttl is not None:
        pattern = make_clear_pattern(tag, key_prefix, ttl, global_clear)
        backend_instance = manager.get_backend(package, backend, ttl)
        cleared = backend_instance.clear(pattern)
        backend_instance.clear_stats()
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {backend} backend (ttl={ttl})')
        return total_cleared

    manager.materialize(package, backend_types, ttl, tag)
    targets = list(manager.iter_backends(
        package, backend_types=backend_types, ttl=ttl, tag=tag))

    for (_pkg, btype, bttl), backend_instance in targets:
        pattern = make_clear_pattern(tag, key_prefix, bttl, global_clear)
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
        logger.warning(_no_region_message('cache_clear', package, backend, ttl, tag))

    return total_cleared


def cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for a decorated function.

    Parameters
    ----------
    fn : Callable
        Function decorated with @cache.

    Returns
    -------
    CacheInfo
        hits, misses and currsize for `fn`.

    Raises
    ------
    ValueError
        If `fn` is not decorated with @cache.

    Notes
    -----
    - Unlike the other CRUD helpers, this one obeys `fail_open`: a backend
      fault costs a degraded `CacheInfo` rather than an exception, since a
      view asking how the cache is doing is better answered with zeros than
      with the fault it is asking about. A failed stats read reports zeros; a
      failed `currsize` reports 0 for that field alone and keeps the counters
      the backend did answer. `fail_open=False` propagates instead.
    - Not being decorated is a caller mistake rather than a backend fault, so
      the ValueError is raised whatever `fail_open` says.
    - `currsize` counts only this region's entries, and on Redis is served
      from a 60-second stale-while-revalidate cache rather than scanning the
      keyspace per call.
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
        Backend type to clear: 'memory', 'file', 'redis', 'dynamodb' or 'null'. All
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
    - Globs are region-scoped and tag clears are region-narrowed exactly as
      in `cache_clear`.
    """
    tag, backend_types, package, key_prefix = _clear_targets(tag, backend, package)
    total_cleared = 0

    if backend is not None and ttl is not None:
        pattern = make_clear_pattern(tag, key_prefix, ttl, global_clear)
        backend_instance = await manager.aget_backend(package, backend, ttl)
        cleared = await backend_instance.aclear(pattern)
        await backend_instance.aclear_stats()
        if cleared > 0:
            total_cleared += cleared
            logger.debug(f'Cleared {cleared} entries from {backend} backend (ttl={ttl})')
        return total_cleared

    await manager.amaterialize(package, backend_types, ttl, tag)
    targets = [
        item async for item in manager.aiter_backends(
            package,
            backend_types=backend_types,
            ttl=ttl,
            tag=tag,
        )
    ]

    for (_pkg, btype, bttl), backend_instance in targets:
        pattern = make_clear_pattern(tag, key_prefix, bttl, global_clear)
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
        logger.warning(_no_region_message('async_cache_clear', package, backend, ttl, tag))

    return total_cleared


async def async_cache_info(fn: Callable[..., Any]) -> CacheInfo:
    """Get cache statistics for an async decorated function.

    Parameters
    ----------
    fn : Callable
        Function decorated with @cache.

    Returns
    -------
    CacheInfo
        hits, misses and currsize for `fn`.

    Raises
    ------
    ValueError
        If `fn` is not decorated with @cache.

    Notes
    -----
    - Obeys `fail_open` and degrades partially, exactly as `cache_info` does.
    - `currsize` on a cold Redis start is 0 rather than a count: the async
      refresh runs as a background task instead of inline.
    """
    _get_meta(fn, '@cache')
    return await get_async_cache_info(fn)
