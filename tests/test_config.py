"""Test cache configuration.
"""
import cache
import pytest


def test_default_configuration():
    """Verify default configuration values are set correctly.
    """
    from cache.config import CacheConfig
    default_config = CacheConfig()
    assert default_config.backend == 'memory'
    assert default_config.key_prefix == ''
    assert default_config.file_dir == '/tmp'
    assert default_config.redis_url == 'redis://localhost:6379/0'


def test_configure_updates_settings(tmp_path):
    """Verify configure() updates global configuration.
    """
    cache.configure(key_prefix='v2:', file_dir=str(tmp_path))
    cfg = cache.get_config()
    assert cfg.key_prefix == 'v2:'
    assert cfg.file_dir == str(tmp_path)


def test_configure_redis_settings():
    """Verify Redis-specific configuration can be updated.
    """
    cache.configure(
        redis_url='redis://redis.example.com:6380/1',
        redis_distributed=True,
    )
    cfg = cache.get_config()
    assert cfg.redis_url == 'redis://redis.example.com:6380/1'
    assert cfg.redis_distributed is True


def test_configure_backend_setting():
    """Verify default backend can be changed.
    """
    cache.configure(backend='file')
    cfg = cache.get_config()
    assert cfg.backend == 'file'


def test_configure_invalid_backend_raises():
    """Verify invalid backend raises ValueError.
    """
    with pytest.raises(ValueError, match='backend must be one of'):
        cache.configure(backend='invalid')


def test_configure_invalid_file_dir_raises(tmp_path):
    """Verify invalid file_dir raises ValueError.
    """
    with pytest.raises(ValueError, match='file_dir must be an existing directory'):
        cache.configure(file_dir='/nonexistent/path')
