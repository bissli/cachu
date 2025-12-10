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
    """
    cache.cache._memory_cache_regions.clear()
    cache.cache._file_cache_regions.clear()
    cache.cache._redis_cache_regions.clear()

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

    cache.configure(
        debug_key='test:',
        memory='dogpile.cache.memory_pickle',
        redis='dogpile.cache.redis' if is_redis_test else 'dogpile.cache.null',
        tmpdir=tempfile.gettempdir(),
        redis_host='localhost',
        redis_port=6379,
        redis_db=0,
        redis_ssl=False,
        redis_distributed=False,
        default_backend='memory',
    )

    if is_redis_test:
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.flushdb()
            r.close()
        except Exception:
            pass
    yield
    cache.cache._memory_cache_regions.clear()
    cache.cache._file_cache_regions.clear()
    cache.cache._redis_cache_regions.clear()
    cache.configure(
        debug_key='',
        memory='dogpile.cache.null',
        redis='dogpile.cache.null',
        tmpdir='/tmp',
        redis_host='localhost',
        redis_port=6379,
        redis_db=0,
        redis_ssl=False,
        redis_distributed=False
    )


@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for file cache tests.
    """
    temp_dir = tempfile.mkdtemp(prefix='cache_test_')
    cache.configure(tmpdir=temp_dir)
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


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
