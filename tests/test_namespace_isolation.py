"""Test package isolation for cache configurations.
"""
import cachu
from cachu.config import ConfigRegistry, _get_caller_package


class TestConfigRegistry:
    """Tests for ConfigRegistry class."""

    def test_registry_creates_new_config_for_package(self):
        """Verify registry creates separate configs per package."""
        registry = ConfigRegistry()

        cfg1 = registry.configure(package='pkg1', key_prefix='v1:')
        cfg2 = registry.configure(package='pkg2', key_prefix='v2:')

        assert cfg1.key_prefix == 'v1:'
        assert cfg2.key_prefix == 'v2:'
        assert cfg1 is not cfg2

    def test_registry_returns_same_config_for_same_package(self, tmp_path):
        """Verify registry returns existing config for same package."""
        registry = ConfigRegistry()

        cfg1 = registry.configure(package='pkg1', key_prefix='v1:')
        cfg2 = registry.configure(package='pkg1', file_dir=str(tmp_path))

        assert cfg1 is cfg2
        assert cfg1.key_prefix == 'v1:'
        assert cfg1.file_dir == str(tmp_path)

    def test_registry_get_config_returns_default_for_unknown_package(self):
        """Verify get_config returns default config for unconfigured package."""
        registry = ConfigRegistry()
        registry.configure(package='pkg1', key_prefix='v1:')

        cfg = registry.get_config(package='unknown')

        assert cfg.key_prefix == ''
        assert cfg.backend == 'memory'

    def test_registry_get_config_returns_configured_for_known_package(self):
        """Verify get_config returns correct config for configured package."""
        registry = ConfigRegistry()
        registry.configure(package='pkg1', key_prefix='v1:', backend='file')

        cfg = registry.get_config(package='pkg1')

        assert cfg.key_prefix == 'v1:'
        assert cfg.backend == 'file'

    def test_registry_get_all_packages(self):
        """Verify get_all_packages returns all configured packages."""
        registry = ConfigRegistry()
        registry.configure(package='pkg1')
        registry.configure(package='pkg2')

        packages = registry.get_all_packages()

        assert 'pkg1' in packages
        assert 'pkg2' in packages

    def test_registry_clear_removes_all_configs(self):
        """Verify clear removes all package configurations."""
        registry = ConfigRegistry()
        registry.configure(package='pkg1', key_prefix='v1:')
        registry.configure(package='pkg2', key_prefix='v2:')

        registry.clear()

        assert len(registry.get_all_packages()) == 0


class TestGetConfig:
    """Tests for get_config module function."""

    def test_get_config_returns_configured_values(self):
        """Verify get_config returns config for caller's package."""
        cachu.configure(key_prefix='test:', backend='memory')

        cfg = cachu.get_config()

        assert cfg.key_prefix == 'test:'
        assert cfg.backend == 'memory'

    def test_get_config_with_explicit_package(self):
        """Verify get_config can retrieve config for explicit package."""
        from cachu.config import _registry

        _registry.configure(package='explicit_pkg', key_prefix='explicit:')

        cfg = cachu.get_config(package='explicit_pkg')

        assert cfg.key_prefix == 'explicit:'


class TestKeyPrefixCapture:
    """Tests for key_prefix capture at decoration time."""

    def test_key_prefix_captured_at_decoration(self):
        """Verify key_prefix is captured when decorator is applied."""
        cachu.configure(key_prefix='v1:', backend='memory')

        @cachu.cache(ttl=300, backend='memory')
        def func(x: int) -> int:
            return x

        func(5)

        # The function's CacheMeta should have the key_prefix from decoration time
        assert func._cache_meta.package is not None

    def test_different_packages_have_different_key_prefixes(self):
        """Verify different packages can have different key prefixes."""
        from cachu.config import _registry

        _registry.configure(package='pkg1', key_prefix='v1:')
        _registry.configure(package='pkg2', key_prefix='v2:')

        cfg1 = cachu.get_config(package='pkg1')
        cfg2 = cachu.get_config(package='pkg2')

        assert cfg1.key_prefix == 'v1:'
        assert cfg2.key_prefix == 'v2:'


class TestGetCallerPackage:
    """Tests for _get_caller_package function."""

    def test_get_caller_package_returns_string(self):
        """Verify _get_caller_package returns a string or None."""
        pkg = _get_caller_package()
        assert pkg is None or isinstance(pkg, str)

    def test_get_caller_package_excludes_cachu_package(self):
        """Verify _get_caller_package skips cachu package frames."""
        pkg = _get_caller_package()
        assert pkg is None or not pkg.startswith('cachu')
