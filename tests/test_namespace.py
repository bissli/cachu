"""Test key generation, tag construction, and filtering logic.
"""
import pytest
from cachu.keys import _normalize_tag, make_key_generator


@pytest.mark.parametrize(('input_tag', 'expected'), [
    ('users', '|users|'),
    ('api_data', '|api_data|'),
    ('reports', '|reports|'),
    ('|users|', '|users|'),
    ('|api_data|', '|api_data|'),
    ('user|admin', '|user.admin|'),
    ('api|weather|data', '|api.weather.data|'),
    ('|user|admin|', '|user.admin|'),
    ('|api|data|', '|api.data|'),
    ('', ''),
])
def test_normalize_tag(input_tag, expected):
    """Verify tag normalization handles various input formats correctly.
    """
    assert _normalize_tag(input_tag or '') == expected


def test_key_generator_basic():
    """Verify key generator creates keys from function arguments.
    """
    def sample_func(x: int, y: int) -> int:
        return x + y

    keygen = make_key_generator(sample_func, tag='users')
    key = keygen(5, 10)

    assert 'sample_func' in key
    assert '|users|' in key
    assert 'x=5' in key
    assert 'y=10' in key


def test_key_generator_with_defaults():
    """Verify key generator handles default argument values.
    """
    def func_with_defaults(x: int, y: int = 10) -> int:
        return x + y

    keygen = make_key_generator(func_with_defaults)
    key1 = keygen(5)
    key2 = keygen(5, 10)

    assert key1 == key2


def test_key_generator_filters_self():
    """Verify key generator excludes 'self' parameter from keys.
    """
    class Sample:
        def method(self, x: int) -> int:
            return x

    keygen = make_key_generator(Sample.method)
    key = keygen(None, 5)

    assert 'self' not in key
    assert 'x=5' in key


def test_key_generator_filters_underscore_params():
    """Verify key generator excludes underscore-prefixed parameters.
    """
    def func(x: int, _internal: str = 'test') -> int:
        return x

    keygen = make_key_generator(func)
    key = keygen(5, _internal='hidden')

    assert 'x=5' in key
    assert '_internal' not in key


def test_key_generator_filters_connection_objects():
    """Verify key generator excludes connection-like objects.
    """
    class MockConnection:
        def __init__(self):
            self.driver_connection = True

    def func_with_conn(conn, x: int) -> int:
        return x

    keygen = make_key_generator(func_with_conn)
    mock_conn = MockConnection()
    key = keygen(mock_conn, 5)

    assert 'x=5' in key
    assert 'conn=' not in key
    assert 'MockConnection' not in key


def test_key_generator_with_exclude():
    """Verify key generator respects exclude parameter.
    """
    def func(logger, context, x: int) -> int:
        return x

    keygen = make_key_generator(func, exclude={'logger', 'context'})
    key = keygen('log1', 'ctx1', 5)

    assert 'x=5' in key
    assert 'logger=' not in key
    assert 'context=' not in key


def test_key_generator_with_tag():
    """Verify key generator includes tag in key.
    """
    def func(x: int) -> int:
        return x

    keygen = make_key_generator(func, tag='users')
    key = keygen(5)

    assert 'func|' in key
    assert '|users|' in key
    assert 'x=5' in key
