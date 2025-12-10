"""Test namespace isolation for cache configurations and regions.
"""
import cache
import pytest
from cache.config import ConfigRegistry, _get_caller_namespace


class TestConfigRegistry:
    """Tests for ConfigRegistry class."""

    def test_registry_creates_new_config_for_namespace(self):
        """Verify registry creates separate configs per namespace."""
        registry = ConfigRegistry()

        cfg1 = registry.configure(namespace='pkg1', debug_key='v1:')
        cfg2 = registry.configure(namespace='pkg2', debug_key='v2:')

        assert cfg1.debug_key == 'v1:'
        assert cfg2.debug_key == 'v2:'
        assert cfg1 is not cfg2

    def test_registry_returns_same_config_for_same_namespace(self, tmp_path):
        """Verify registry returns existing config for same namespace."""
        registry = ConfigRegistry()

        cfg1 = registry.configure(namespace='pkg1', debug_key='v1:')
        cfg2 = registry.configure(namespace='pkg1', tmpdir=str(tmp_path))

        assert cfg1 is cfg2
        assert cfg1.debug_key == 'v1:'
        assert cfg1.tmpdir == str(tmp_path)

    def test_registry_get_config_returns_default_for_unknown_namespace(self):
        """Verify get_config returns default config for unconfigured namespace."""
        registry = ConfigRegistry()
        registry.configure(namespace='pkg1', debug_key='v1:')

        cfg = registry.get_config(namespace='unknown')

        assert cfg.debug_key == ''
        assert cfg.memory == 'dogpile.cache.null'

    def test_registry_get_config_returns_configured_for_known_namespace(self):
        """Verify get_config returns correct config for configured namespace."""
        registry = ConfigRegistry()
        registry.configure(namespace='pkg1', debug_key='v1:', memory='dogpile.cache.memory_pickle')

        cfg = registry.get_config(namespace='pkg1')

        assert cfg.debug_key == 'v1:'
        assert cfg.memory == 'dogpile.cache.memory_pickle'

    def test_registry_get_all_namespaces(self):
        """Verify get_all_namespaces returns all configured namespaces."""
        registry = ConfigRegistry()
        registry.configure(namespace='pkg1')
        registry.configure(namespace='pkg2')

        namespaces = registry.get_all_namespaces()

        assert 'pkg1' in namespaces
        assert 'pkg2' in namespaces

    def test_registry_clear_removes_all_configs(self):
        """Verify clear removes all namespace configurations."""
        registry = ConfigRegistry()
        registry.configure(namespace='pkg1', debug_key='v1:')
        registry.configure(namespace='pkg2', debug_key='v2:')

        registry.clear()

        assert len(registry.get_all_namespaces()) == 0

    def test_registry_rejects_invalid_config_keys(self):
        """Verify configure raises error for unknown keys."""
        registry = ConfigRegistry()

        with pytest.raises(ValueError, match='Unknown configuration key'):
            registry.configure(namespace='pkg1', invalid_key='value')

    def test_registry_sets_default_distributed_lock_when_redis_null(self):
        """Verify redis_distributed defaults to False when redis backend is null."""
        registry = ConfigRegistry()

        # When not explicitly set, defaults to False when redis is null
        cfg = registry.configure(
            namespace='pkg1',
            redis='dogpile.cache.null',
        )

        assert cfg.redis_distributed is False


class TestGetConfig:
    """Tests for get_config module function."""

    def test_get_config_returns_configured_values(self):
        """Verify get_config returns config for caller's namespace."""
        cache.configure(debug_key='test:', memory='dogpile.cache.memory_pickle')

        cfg = cache.get_config()

        assert cfg.debug_key == 'test:'
        assert cfg.memory == 'dogpile.cache.memory_pickle'

    def test_get_config_with_explicit_namespace(self):
        """Verify get_config can retrieve config for explicit namespace."""
        from cache.config import _registry

        _registry.configure(namespace='explicit_ns', debug_key='explicit:')

        cfg = cache.get_config(namespace='explicit_ns')

        assert cfg.debug_key == 'explicit:'


class TestClearRegistry:
    """Tests for clear_registry function."""

    def test_clear_registry_removes_namespace_configs(self):
        """Verify clear_registry clears all namespace-specific configurations."""
        from cache.config import _registry

        # Create a namespace-specific config
        cache.configure(debug_key='v1:')
        namespaces_before = _registry.get_all_namespaces()
        assert len(namespaces_before) > 0

        cache.clear_registry()

        # Namespace-specific configs should be cleared
        namespaces_after = _registry.get_all_namespaces()
        assert len(namespaces_after) == 0


class TestClearAllRegions:
    """Tests for clear_all_regions function."""

    def test_clear_all_regions_clears_memory_regions(self):
        """Verify clear_all_regions clears memory cache region dictionaries."""
        @cache.memorycache(seconds=300).cache_on_arguments()
        def func(x: int) -> int:
            return x

        func(1)
        assert len(cache.cache._memory_cache_regions) > 0

        cache.clear_all_regions()

        assert len(cache.cache._memory_cache_regions) == 0

    def test_clear_all_regions_clears_file_regions(self, temp_cache_dir):
        """Verify clear_all_regions clears file cache region dictionaries."""
        @cache.filecache(seconds=300).cache_on_arguments()
        def func(x: int) -> int:
            return x

        func(1)
        assert len(cache.cache._file_cache_regions) > 0

        cache.clear_all_regions()

        assert len(cache.cache._file_cache_regions) == 0

    @pytest.mark.redis
    def test_clear_all_regions_clears_redis_regions(self, redis_docker):
        """Verify clear_all_regions clears redis cache region dictionaries."""
        @cache.rediscache(seconds=300).cache_on_arguments()
        def func(x: int) -> int:
            return x

        func(1)
        assert len(cache.cache._redis_cache_regions) > 0

        cache.clear_all_regions()

        assert len(cache.cache._redis_cache_regions) == 0


class TestKeyManglerCapture:
    """Tests for key mangler debug_key capture at region creation time."""

    def test_key_mangler_captures_debug_key_at_creation(self):
        """Verify key mangler uses debug_key from region creation time."""
        cache.configure(debug_key='v1:', memory='dogpile.cache.memory_pickle')

        @cache.memorycache(seconds=300).cache_on_arguments()
        def func(x: int) -> int:
            return x

        func(5)

        # Check that keys use v1: prefix
        from conftest import get_memory_region
        region = get_memory_region(300)
        keys = list(region.actual_backend._cache.keys())
        assert len(keys) > 0
        assert keys[0].startswith('v1:')

    def test_key_mangler_not_affected_by_later_config_changes(self):
        """Verify key mangler is not affected by config changes after region creation."""
        cache.configure(debug_key='v1:', memory='dogpile.cache.memory_pickle')

        @cache.memorycache(seconds=300).cache_on_arguments()
        def func(x: int) -> int:
            return x

        func(5)

        # Change debug_key after region creation
        cache.configure(debug_key='v2:')

        func(10)

        # Both keys should still use v1: (from creation time)
        from conftest import get_memory_region
        region = get_memory_region(300)
        keys = list(region.actual_backend._cache.keys())
        assert all(k.startswith('v1:') for k in keys)


class TestGetCallerNamespace:
    """Tests for _get_caller_namespace function."""

    def test_get_caller_namespace_returns_string(self):
        """Verify _get_caller_namespace returns a string or None."""
        ns = _get_caller_namespace()
        assert ns is None or isinstance(ns, str)

    def test_get_caller_namespace_excludes_cache_package(self):
        """Verify _get_caller_namespace skips cache package frames."""
        # When called from test code, should return test module name
        ns = _get_caller_namespace()
        assert ns is None or not ns.startswith('cache')
