import logging

import pytest

logger = logging.getLogger(__name__)


class RedisTestConfig:
    """Singleton to store Redis test configuration.
    """
    host = 'localhost'
    port = 6379


redis_test_config = RedisTestConfig()


@pytest.fixture(scope='session')
def redis_docker():
    """Start Redis container for testing using testcontainers.
    """
    from testcontainers.redis import RedisContainer

    try:
        import redis as redis_lib
    except ImportError:
        raise Exception('redis package not installed, cannot test Redis functionality')

    with RedisContainer('redis:7-alpine') as redis_container:
        host = redis_container.get_container_host_ip()
        port = int(redis_container.get_exposed_port(6379))

        # Update the singleton
        redis_test_config.host = host
        redis_test_config.port = port

        import cache
        cache.configure(redis_url=f'redis://{host}:{port}/0')

        r = redis_lib.Redis(host=host, port=port, db=0)
        r.ping()
        r.close()
        logger.debug(f'Redis container ready at {host}:{port}')

        yield redis_container
