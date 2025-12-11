"""Flexible caching library with support for memory, file, and Redis backends.
"""
__version__ = '0.1.2'

from .backends.redis import get_redis_client
from .config import configure, disable, enable, get_all_configs, get_config
from .config import is_disabled
from .decorator import cache, get_backend
from .operations import cache_clear, cache_delete, cache_get, cache_info
from .operations import cache_set

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
    'get_redis_client',
]
