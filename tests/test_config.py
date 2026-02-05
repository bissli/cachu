"""Test cache configuration.
"""
import cachu
import pytest


def test_configure_updates_settings(tmp_path):
    """Verify configure() updates global configuration.
    """
    cachu.configure(key_prefix='v2:', file_dir=str(tmp_path))
    cfg = cachu.get_config()
    assert cfg.key_prefix == 'v2:'
    assert cfg.file_dir == str(tmp_path)


def test_configure_redis_settings():
    """Verify Redis-specific configuration can be updated.
    """
    cachu.configure(redis_url='redis://redis.example.com:6380/1')
    cfg = cachu.get_config()
    assert cfg.redis_url == 'redis://redis.example.com:6380/1'


def test_configure_backend_default_setting():
    """Verify default backend can be changed via backend_default.
    """
    cachu.configure(backend_default='file')
    cfg = cachu.get_config()
    assert cfg.backend_default == 'file'


def test_configure_invalid_backend_raises():
    """Verify invalid backend raises ValueError.
    """
    with pytest.raises(ValueError, match='backend must be one of'):
        cachu.configure(backend_default='invalid')


def test_configure_invalid_file_dir_raises(tmp_path):
    """Verify invalid file_dir raises ValueError.
    """
    with pytest.raises(ValueError, match='file_dir must be an existing directory'):
        cachu.configure(file_dir='/nonexistent/path')
