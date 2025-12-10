"""Flexible caching library built on dogpile.cache.
"""
from .cache import clear_all_regions, clear_defaultcache, clear_filecache
from .cache import clear_memorycache, clear_rediscache, defaultcache
from .cache import delete_defaultcache_key, delete_filecache_key
from .cache import delete_memorycache_key, delete_rediscache_key, filecache
from .cache import get_redis_client, memorycache, rediscache
from .cache import set_defaultcache_key, set_filecache_key
from .cache import set_memorycache_key, set_rediscache_key
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
    'set_memorycache_key',
    'set_filecache_key',
    'set_rediscache_key',
    'set_defaultcache_key',
    'delete_memorycache_key',
    'delete_filecache_key',
    'delete_rediscache_key',
    'delete_defaultcache_key',
    ]
