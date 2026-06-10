"""Tests for config validation, immutability, and the fail_open knob.
"""
import cachu
import pytest


def test_decorator_invalid_backend_raises_at_decoration():
    """An unknown backend= on the decorator fails fast at decoration time.
    """
    with pytest.raises(ValueError):
        @cachu.cache(ttl=300, backend='reids')
        def func(x: int) -> int:
            return x


def test_decorator_valid_backend_accepted():
    """Valid backend names still decorate without error.
    """
    @cachu.cache(ttl=300, backend='memory')
    def func(x: int) -> int:
        return x

    assert func(5) == 5


def test_configure_returns_new_config_each_time():
    """Reconfiguring builds a new CacheConfig rather than mutating in place.
    """
    cfg1 = cachu.configure(key_prefix='a:')
    cfg2 = cachu.configure(key_prefix='b:')

    assert cfg1.key_prefix == 'a:'
    assert cfg2.key_prefix == 'b:'
    assert cfg1 is not cfg2


def test_fail_open_defaults_true_and_is_configurable():
    """fail_open defaults to True and can be turned off.
    """
    assert cachu.get_config().fail_open is True

    cachu.configure(fail_open=False)
    assert cachu.get_config().fail_open is False
