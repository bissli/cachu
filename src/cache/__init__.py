"""Flexible caching library built on dogpile.cache.
"""
from .cache import clear_cache_for_namespace, clear_defaultcache
from .cache import clear_filecache, clear_memorycache, clear_rediscache
from .cache import defaultcache, delete_defaultcache_key, delete_filecache_key
from .cache import delete_memorycache_key, delete_rediscache_key, filecache
from .cache import get_redis_client, memorycache, rediscache
from .cache import set_defaultcache_key, set_filecache_key
from .cache import set_memorycache_key, set_rediscache_key
from .config import config, configure, disable, enable, get_all_configs
from .config import get_config, is_disabled

__all__ = [
    'configure',
    'config',
    'get_config',
    'get_all_configs',
    'disable',
    'enable',
    'is_disabled',
    'get_redis_client',
    'memorycache',
    'filecache',
    'rediscache',
    'defaultcache',
    'clear_memorycache',
    'clear_filecache',
    'clear_rediscache',
    'clear_defaultcache',
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
