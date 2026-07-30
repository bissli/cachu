"""Flexible caching library with support for memory, file, and Redis backends.
"""
__version__ = '0.4.1'

from . import backends, presets
from .api import Backend, CacheEntry, CacheInfo, CacheMeta
from .backends.redis import get_redis_client
from .config import VALID_BACKENDS as BACKENDS
from .config import CacheConfig, DisabledScopes, configure, disable, enable
from .config import get_all_configs, get_config, get_disabled_scopes
from .config import is_disabled
from .decorator import cache, get_async_cache_info, get_cache_info
from .exception import BackendNotFoundError, CacheError, CacheLockTimeout
from .exception import ConfigurationError
from .manager import aget_backend, clear_async_backends, clear_backends
from .manager import get_backend
from .operations import async_cache_clear, async_cache_delete, async_cache_get
from .operations import async_cache_info, async_cache_set, cache_clear
from .operations import cache_delete, cache_get, cache_info, cache_set

__all__ = [
    'BACKENDS',
    'Backend',
    'BackendNotFoundError',
    'CacheConfig',
    'CacheEntry',
    'CacheError',
    'CacheInfo',
    'CacheLockTimeout',
    'CacheMeta',
    'ConfigurationError',
    'DisabledScopes',
    'aget_backend',
    'async_cache_clear',
    'async_cache_delete',
    'async_cache_get',
    'async_cache_info',
    'async_cache_set',
    'backends',
    'cache',
    'cache_clear',
    'cache_delete',
    'cache_get',
    'cache_info',
    'cache_set',
    'clear_async_backends',
    'clear_backends',
    'configure',
    'disable',
    'enable',
    'get_all_configs',
    'get_async_cache_info',
    'get_backend',
    'get_cache_info',
    'get_config',
    'get_disabled_scopes',
    'get_redis_client',
    'is_disabled',
    'presets',
]
