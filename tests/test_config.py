"""Test cache configuration.
"""
import cache
import pytest


def test_default_configuration():
    """Verify default configuration values are set correctly.
    """
    from cache.config import CacheConfig
    default_config = CacheConfig()
    assert default_config.memory == 'dogpile.cache.null'
    assert default_config.redis == 'dogpile.cache.null'
    assert default_config.tmpdir == '/tmp'


def test_configure_updates_settings():
    """Verify configure() updates global configuration.
    """
    cache.configure(debug_key='v2:', tmpdir='/var/cache')
    cfg = cache.config
    assert cfg.debug_key == 'v2:'
    assert cfg.tmpdir == '/var/cache'


def test_configure_rejects_invalid_keys():
    """Verify configure() raises error for unknown configuration keys.
    """
    with pytest.raises(ValueError, match='Unknown configuration key'):
        cache.configure(invalid_key='value')


def test_configure_redis_settings():
    """Verify Redis-specific configuration can be updated.
    """
    cache.configure(
        redis_host='redis.example.com',
        redis_port=6380,
        redis_db=1,
        redis_ssl=True,
        redis_distributed=True
    )
    cfg = cache.config
    assert cfg.redis_host == 'redis.example.com'
    assert cfg.redis_port == 6380
    assert cfg.redis_db == 1
    assert cfg.redis_ssl is True
    assert cfg.redis_distributed is True
