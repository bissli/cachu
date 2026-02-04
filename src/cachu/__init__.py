"""Flexible caching library with support for memory, file, and Redis backends.
"""
__version__ = '0.2.10'

from .backends import Backend
from .backends.redis import get_redis_client
from .config import configure, disable, enable, get_all_configs, get_config
from .config import is_disabled
from .decorator import aget_backend, cache, clear_async_backends
from .decorator import get_async_backend, get_async_cache_info, get_backend
from .operations import async_cache_clear, async_cache_delete, async_cache_get
from .operations import async_cache_info, async_cache_set, cache_clear
from .operations import cache_delete, cache_get, cache_info, cache_set

__all__ = [
    'configure',
    'get_config',
    'get_all_configs',
    'disable',
    'enable',
    'is_disabled',
    'cache',
    'cache_get',
    'cache_set',
    'cache_delete',
    'cache_clear',
    'cache_info',
    'get_backend',
    'aget_backend',
    'get_redis_client',
    'Backend',
    'async_cache_get',
    'async_cache_set',
    'async_cache_delete',
    'async_cache_clear',
    'async_cache_info',
    'get_async_backend',
    'get_async_cache_info',
    'clear_async_backends',
]
