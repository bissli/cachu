"""Shared fixtures for cache tests.
"""
import logging
import pathlib
import shutil
import site
import tempfile

import pytest

try:
    import redis
except ImportError:
    raise Exception('redis package not installed, cannot test Redis functionality')


logger = logging.getLogger(__name__)

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
site.addsitedir(HERE)


def _clear_all_backends() -> None:
    """Clear all backend instances (internal test helper).
    """
    from cachu.decorator import _backends, _backends_lock, _stats, _stats_lock

    with _backends_lock:
        for backend in _backends.values():
            try:
                backend.clear()
            except Exception:
                pass
        _backends.clear()

    with _stats_lock:
        _stats.clear()


@pytest.fixture(autouse=True)
def reset_cache_config(request):
    """Reset cache configuration and clear backends before each test.
    """
    from cachu.config import CacheConfig, _registry, enable

    enable()
    _clear_all_backends()
    _registry._configs.clear()

    is_redis_test = 'redis' in [marker.name for marker in request.node.iter_markers()]

    if not is_redis_test:
        is_redis_test = 'redis_docker' in request.fixturenames

    if not is_redis_test and hasattr(request.node, 'callspec'):
        params = request.node.callspec.params
        is_redis_test = (
            params.get('cache_type') == 'redis' or
            params.get('fixture') == 'redis_docker' or
            params.get('fixture_needed') == 'redis_docker' or
            params.get('backend_type') == 'redis'
        )

    redis_host = 'localhost'
    redis_port = 6379
    if is_redis_test:
        request.getfixturevalue('redis_docker')
        from fixtures.redis import redis_test_config
        redis_host = redis_test_config.host
        redis_port = redis_test_config.port

    _registry._default = CacheConfig(
        backend='memory',
        key_prefix='test:',
        file_dir=tempfile.gettempdir(),
        redis_url=f'redis://{redis_host}:{redis_port}/0',
        redis_distributed=False,
    )

    if is_redis_test:
        try:
            r = redis.Redis(host=redis_host, port=redis_port, db=0)
            r.flushdb()
            r.close()
        except Exception:
            pass

    yield

    _clear_all_backends()
    _registry._configs.clear()
    _registry._default = CacheConfig()


@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for file cache tests.
    """
    from cachu.config import _registry
    temp_dir = tempfile.mkdtemp(prefix='cache_test_')
    _registry._default.file_dir = temp_dir
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
