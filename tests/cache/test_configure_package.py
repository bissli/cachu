"""Tests for configuring a package other than the caller's.

Notes
-----
- ConfigRegistry.configure has always taken `package`, but the public
  cachu.configure did not forward it and always configured
  _get_caller_package().
- A package holding one request-path cache that wants a 0.25s socket
  timeout alongside thirty batch caches that want the default could not
  express that through the documented API - only through the private
  _registry singleton.
"""
import cachu
import pytest
from cachu.config import _registry


class TestPackageForwarding:
    """configure(package=...) reaches the registry entry it names.
    """

    def test_sibling_packages_get_independent_timeouts(self):
        """Two named packages hold different values at the same time.

        Mutation: drop `package=package` from the forward to
        _registry.configure, collapsing both onto the caller's package.
        Oracle: the two values as written, 0.25 and 9.0.
        """
        cachu.configure(package='alpha', redis_socket_timeout=0.25)
        cachu.configure(package='beta', redis_socket_timeout=9.0)

        assert cachu.get_config('alpha').redis_socket_timeout == 0.25
        assert cachu.get_config('beta').redis_socket_timeout == 9.0

    def test_naming_a_package_leaves_the_caller_untouched(self):
        """Configuring a sibling does not configure the calling package.

        Mutation: forward the caller's package regardless of the argument.
        Oracle: the default redis_socket_timeout, 5.0, still seen by the
        caller's own package.
        """
        caller_default = cachu.get_config().redis_socket_timeout

        cachu.configure(package='alpha', redis_socket_timeout=0.25)

        assert cachu.get_config().redis_socket_timeout == caller_default
        assert 'alpha' in _registry.get_all_packages()

    def test_returned_config_is_the_named_packages_config(self):
        """The return value reflects the package that was configured.

        Mutation: return the caller's config instead of the named one.
        Oracle: the value just written, 0.25.
        """
        result = cachu.configure(package='alpha', redis_socket_timeout=0.25)

        assert result.redis_socket_timeout == 0.25
        assert result == cachu.get_config('alpha')

    def test_repeated_configure_merges_into_the_same_package(self):
        """A second call adds settings rather than resetting the package.

        Mutation: replace the stored config with a fresh CacheConfig instead
        of merging onto the existing one.
        Oracle: both written values, 0.25 and 'a:', present together.
        """
        cachu.configure(package='alpha', redis_socket_timeout=0.25)
        cachu.configure(package='alpha', key_prefix='a:')

        config = cachu.get_config('alpha')
        assert config.redis_socket_timeout == 0.25
        assert config.key_prefix == 'a:'


class TestDecoratorUsesNamedPackageConfig:
    """A cache pinned to a package picks up that package's settings.
    """

    def test_key_prefix_of_the_named_package_is_applied(self):
        """@cache(package='alpha') keys are prefixed by alpha's key_prefix.

        Mutation: resolve the decorator's config from the caller's package.
        Oracle: the configured prefix, 'alpha:', in the stored key.
        """
        cachu.configure(package='alpha', key_prefix='alpha:')

        @cachu.cache(ttl=300, backend='memory', package='alpha', tag='t')
        def fetch(key: int) -> int:
            return key

        fetch(1)

        backend = cachu.get_backend('memory', package='alpha', ttl=300)
        assert all('alpha:' in key for key in backend.keys())

    def test_per_package_deadline_does_not_leak_to_siblings(self):
        """One package's cache_deadline leaves its sibling unbounded.

        Mutation: store new settings on the default config shared by every
        package.
        Oracle: the written value 1.0 for alpha and the unbounded default
        None for beta.
        """
        cachu.configure(package='alpha', cache_deadline=1.0)

        assert cachu.get_config('alpha').cache_deadline == 1.0
        assert cachu.get_config('beta').cache_deadline is None


class TestBackwardCompatibleSignature:
    """`package` was appended, so existing positional calls still work.
    """

    def test_positional_arguments_keep_their_meaning(self):
        """configure('file', 'v1:') still sets backend_default and key_prefix.

        Mutation: insert `package` as the first parameter, silently turning
        every existing positional configure('redis') call into a package name.
        Oracle: the two positional values as written.
        """
        cachu.configure('file', 'v1:')

        config = cachu.get_config()
        assert config.backend_default == 'file'
        assert config.key_prefix == 'v1:'

    def test_unset_options_keep_their_defaults(self):
        """New options are absent from a config the caller never set.

        Mutation: give the new options non-default values when unspecified.
        Oracle: the documented defaults - None, 'run', None, 60.0.
        """
        cachu.configure(key_prefix='v1:')

        config = cachu.get_config()
        assert config.cache_deadline is None
        assert config.on_lock_timeout == 'run'
        assert config.memory_maxsize is None
        assert config.memory_sweep_interval == 60.0

    def test_all_new_options_are_reported_by_get_all_configs(self):
        """Introspection exposes every new setting.

        Mutation: omit a field from the dataclass so it silently falls back
        to a hardcoded value.
        Oracle: the four documented new field names.
        """
        cachu.configure(key_prefix='v1:')

        default = cachu.get_all_configs()['_default']
        for field in ('cache_deadline', 'on_lock_timeout',
                      'memory_maxsize', 'memory_sweep_interval'):
            assert field in default


class TestValidationAppliesToNamedPackages:
    """Validation runs before the named package is written.
    """

    def test_invalid_value_does_not_create_the_package(self):
        """A rejected configure leaves no partial entry behind.

        Mutation: validate after writing, leaving a half-configured package.
        Oracle: absence of 'gamma' from the registry's package list.
        """
        with pytest.raises(ValueError):
            cachu.configure(package='gamma', backend_default='mongo')

        assert 'gamma' not in _registry.get_all_packages()
