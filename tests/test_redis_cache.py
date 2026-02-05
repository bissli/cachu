"""Test Redis cache backend operations via inheritance-based test suite.
"""
import pytest
from cachu.backends.redis import RedisBackend
from fixtures.backend_suite import _GenericAsyncBackendTestSuite
from fixtures.backend_suite import _GenericAsyncDirectBackendTestSuite
from fixtures.backend_suite import _GenericBackendTestSuiteWithTTL
from fixtures.backend_suite import _GenericDirectBackendTestSuite

pytestmark = pytest.mark.redis


class TestRedisBackend(_GenericBackendTestSuiteWithTTL):
    """Sync tests for Redis backend via decorator.
    """

    backend = 'redis'
    supports_zero_ttl = False

    @pytest.fixture(autouse=True)
    def setup_redis(self, redis_docker):
        """Ensure Redis is available.
        """


class TestAsyncRedisBackend(_GenericAsyncBackendTestSuite):
    """Async tests for Redis backend via decorator.
    """

    backend = 'redis'

    @pytest.fixture(autouse=True)
    def setup_redis(self, redis_docker):
        """Ensure Redis is available.
        """


class TestRedisBackendDirect(_GenericDirectBackendTestSuite):
    """Direct API tests for RedisBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_redis(self, redis_docker):
        """Ensure Redis is available.
        """
        from fixtures.redis import redis_test_config

        self._redis_url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'

    def create_backend(self):
        """Create RedisBackend instance.
        """
        return RedisBackend(self._redis_url)


@pytest.mark.asyncio
class TestAsyncRedisBackendDirect(_GenericAsyncDirectBackendTestSuite):
    """Async direct API tests for RedisBackend.
    """

    @pytest.fixture(autouse=True)
    def setup_redis(self, redis_docker):
        """Ensure Redis is available.
        """
        from fixtures.redis import redis_test_config

        self._redis_url = f'redis://{redis_test_config.host}:{redis_test_config.port}/0'

    def create_backend(self):
        """Create RedisBackend instance.
        """
        return RedisBackend(self._redis_url)
