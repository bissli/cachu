"""Tests for cache disable/enable and get_all_configs functions.
"""
import cachu


def test_get_all_configs_returns_default():
    """Verify get_all_configs returns at least the default config.
    """
    configs = cachu.get_all_configs()
    assert '_default' in configs
    assert 'backend' in configs['_default']
    assert 'key_prefix' in configs['_default']
    assert 'redis_url' in configs['_default']


def test_get_all_configs_includes_package_configs():
    """Verify get_all_configs includes package-specific configurations.
    """
    cachu.configure(key_prefix='ns1:', backend='memory')
    configs = cachu.get_all_configs()
    assert '_default' in configs
    pkg_configs = [k for k in configs if k != '_default']
    assert len(pkg_configs) >= 1


def test_disable_prevents_caching():
    """Verify disable() prevents values from being cached.
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def expensive_fn(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    expensive_fn(5)
    assert call_count == 1
    expensive_fn(5)
    assert call_count == 1

    cachu.disable()
    expensive_fn(5)
    assert call_count == 2
    expensive_fn(5)
    assert call_count == 3


def test_enable_restores_caching():
    """Verify enable() restores caching after disable().
    """
    call_count = 0

    @cachu.cache(ttl=300, backend='memory')
    def expensive_fn(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    cachu.disable()
    expensive_fn(10)
    expensive_fn(10)
    assert call_count == 2

    cachu.enable()
    expensive_fn(10)
    assert call_count == 3
    expensive_fn(10)
    assert call_count == 3


def test_is_disabled_reflects_state():
    """Verify is_disabled() returns correct state.
    """
    assert cachu.is_disabled() is False
    cachu.disable()
    assert cachu.is_disabled() is True
    cachu.enable()
    assert cachu.is_disabled() is False
