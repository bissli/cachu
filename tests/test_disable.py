"""Tests for cache disable/enable and get_all_configs functions.
"""
import cache


def test_get_all_configs_returns_default():
    """Verify get_all_configs returns at least the default config.
    """
    configs = cache.get_all_configs()
    assert '_default' in configs
    assert 'debug_key' in configs['_default']
    assert 'memory' in configs['_default']
    assert 'redis' in configs['_default']


def test_get_all_configs_includes_namespace_configs():
    """Verify get_all_configs includes namespace-specific configurations.
    """
    cache.configure(debug_key='ns1:', memory='dogpile.cache.memory_pickle')
    configs = cache.get_all_configs()
    assert '_default' in configs
    ns_configs = [k for k in configs if k != '_default']
    assert len(ns_configs) >= 1


def test_disable_prevents_caching():
    """Verify disable() prevents values from being cached.
    """
    call_count = 0

    @cache.memorycache(300).cache_on_arguments()
    def expensive_fn(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    expensive_fn(5)
    assert call_count == 1
    expensive_fn(5)
    assert call_count == 1

    cache.disable()
    expensive_fn(5)
    assert call_count == 2
    expensive_fn(5)
    assert call_count == 3


def test_enable_restores_caching():
    """Verify enable() restores caching after disable().
    """
    call_count = 0

    @cache.memorycache(300).cache_on_arguments()
    def expensive_fn(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    cache.disable()
    expensive_fn(10)
    expensive_fn(10)
    assert call_count == 2

    cache.enable()
    expensive_fn(10)
    assert call_count == 3
    expensive_fn(10)
    assert call_count == 3


def test_is_disabled_reflects_state():
    """Verify is_disabled() returns correct state.
    """
    assert cache.is_disabled() is False
    cache.disable()
    assert cache.is_disabled() is True
    cache.enable()
    assert cache.is_disabled() is False
