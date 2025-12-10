"""Flexible caching library built on dogpile.cache.
"""
from .cache import clear_defaultcache, clear_filecache
from .cache import clear_memorycache, clear_rediscache
from .cache import defaultcache, delete_defaultcache_key
from .cache import delete_filecache_key, delete_memorycache_key
from .cache import delete_rediscache_key, filecache, get_redis_client
from .cache import memorycache, rediscache, set_defaultcache_key
from .cache import set_filecache_key, set_memorycache_key, set_rediscache_key
from .config import config, configure

__all__ = [
    'configure',
    'config',
    'get_redis_client',
    'memorycache',
    'filecache',
    'rediscache',
    'defaultcache',
    'clear_memorycache',
    'clear_filecache',
    'clear_rediscache',
    'clear_defaultcache',
    'set_memorycache_key',
    'set_filecache_key',
    'set_rediscache_key',
    'set_defaultcache_key',
    'delete_memorycache_key',
    'delete_filecache_key',
    'delete_rediscache_key',
    'delete_defaultcache_key',
]
