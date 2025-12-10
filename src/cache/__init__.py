"""Flexible caching library built on dogpile.cache.
"""
from .cache import clear_all_regions, clear_cache_for_namespace
from .cache import clear_defaultcache, clear_filecache, clear_memorycache
from .cache import clear_rediscache, defaultcache, delete_defaultcache_key
from .cache import delete_filecache_key, delete_memorycache_key
from .cache import delete_rediscache_key, filecache, get_redis_client
from .cache import memorycache, rediscache, set_defaultcache_key
from .cache import set_filecache_key, set_memorycache_key, set_rediscache_key
from .config import clear_registry, config, configure, get_config

__all__ = [
    'configure',
    'config',
    'get_config',
    'clear_registry',
    'get_redis_client',
    'memorycache',
    'filecache',
    'rediscache',
    'defaultcache',
    'clear_memorycache',
    'clear_filecache',
    'clear_rediscache',
    'clear_defaultcache',
    'clear_all_regions',
    'clear_cache_for_namespace',
    'set_memorycache_key',
    'set_filecache_key',
    'set_rediscache_key',
    'set_defaultcache_key',
    'delete_memorycache_key',
    'delete_filecache_key',
    'delete_rediscache_key',
    'delete_defaultcache_key',
    ]
