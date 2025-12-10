"""Shared fixtures for cache tests.
"""
import logging
import pathlib
import shutil
import site
import tempfile

import cache
import pytest

try:
    import redis
except ImportError:
    raise Exception('redis package not installed, cannot test Redis functionality')


logger = logging.getLogger(__name__)

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
site.addsitedir(HERE)


@pytest.fixture(autouse=True)
def reset_cache_config(request):
    """Reset cache configuration and clear cache regions before each test.

    Since namespace isolation is now enabled, we configure the registry's
    default config directly so all test namespaces use the test settings.
    """
    from cache.config import CacheConfig, _registry

    cache.clear_all_regions()
    cache.clear_registry()

    is_redis_test = 'redis' in [marker.name for marker in request.node.iter_markers()]

    if not is_redis_test:
        is_redis_test = 'redis_docker' in request.fixturenames

    if not is_redis_test and hasattr(request.node, 'callspec'):
        params = request.node.callspec.params
        is_redis_test = (
            params.get('cache_type') == 'redis' or
            params.get('fixture') == 'redis_docker' or
            params.get('fixture_needed') == 'redis_docker'
        )

    # For redis tests, ensure the fixture runs first to get dynamic port
    redis_host = 'localhost'
    redis_port = 6379
    if is_redis_test:
        request.getfixturevalue('redis_docker')
        # Import the config from the fixture module
        from fixtures.redis import redis_test_config
        redis_host = redis_test_config.host
        redis_port = redis_test_config.port

    # Configure the registry's default config so all test namespaces use these settings
    _registry._default = CacheConfig(
        debug_key='test:',
        memory='dogpile.cache.memory_pickle',
        redis='dogpile.cache.redis' if is_redis_test else 'dogpile.cache.null',
        tmpdir=tempfile.gettempdir(),
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=0,
        redis_ssl=False,
        redis_distributed=False,
        default_backend='memory',
    )

    if is_redis_test:
        try:
            r = redis.Redis(host=redis_host, port=redis_port, db=0)
            r.flushdb()
            r.close()
        except Exception:
            pass
    yield
    cache.clear_all_regions()
    cache.clear_registry()
    _registry._default = CacheConfig()


@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for file cache tests.
    """
    from cache.config import _registry
    temp_dir = tempfile.mkdtemp(prefix='cache_test_')
    _registry._default.tmpdir = temp_dir
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def get_memory_region(seconds: int):
    """Get memory cache region by seconds (finds first matching region)."""
    for (ns, secs), region in cache.cache._memory_cache_regions.items():
        if secs == seconds:
            return region
    return None


def get_file_region(seconds: int):
    """Get file cache region by seconds (finds first matching region)."""
    for (ns, secs), region in cache.cache._file_cache_regions.items():
        if secs == seconds:
            return region
    return None


def get_redis_region(seconds: int):
    """Get redis cache region by seconds (finds first matching region)."""
    for (ns, secs), region in cache.cache._redis_cache_regions.items():
        if secs == seconds:
            return region
    return None


def has_memory_region(seconds: int) -> bool:
    """Check if a memory cache region exists for given seconds."""
    return any(secs == seconds for (ns, secs) in cache.cache._memory_cache_regions)


def has_file_region(seconds: int) -> bool:
    """Check if a file cache region exists for given seconds."""
    return any(secs == seconds for (ns, secs) in cache.cache._file_cache_regions)


def has_redis_region(seconds: int) -> bool:
    """Check if a redis cache region exists for given seconds."""
    return any(secs == seconds for (ns, secs) in cache.cache._redis_cache_regions)


@pytest.fixture
def sample_function():
    """Provide a simple function for cache testing.
    """
    def compute(x: int, y: int) -> int:
        return x + y
    return compute


pytest_plugins = [
    'fixtures.redis',
]
