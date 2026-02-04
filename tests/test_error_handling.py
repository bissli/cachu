"""Tests for error handling edge cases.
"""
import cachu
import pytest


class TestUnpickleableObjects:
    """Tests for handling objects that cannot be pickled.
    """

    def test_unpickleable_return_value_raises(self):
        """Verify unpickleable return values raise PicklingError.

        Objects like lambdas cannot be pickled. The cache decorator will
        raise an exception when trying to cache such values.
        """
        @cachu.cache(ttl=300, backend='memory')
        def returns_lambda() -> object:
            return lambda x: x * 2

        with pytest.raises(Exception):
            returns_lambda()

    def test_unpickleable_with_cache_if_false_succeeds(self):
        """Verify unpickleable values work when cache_if returns False.
        """
        @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: False)
        def returns_lambda() -> object:
            return lambda x: x * 2

        result = returns_lambda()
        assert callable(result)
        assert result(5) == 10


class TestCacheIfExceptions:
    """Tests for exception handling in cache_if callbacks.
    """

    def test_cache_if_exception_propagates(self):
        """Verify exception in cache_if propagates to caller.

        If the cache_if callback raises an exception, it should propagate
        rather than being silently swallowed.
        """
        def bad_cache_if(result: int) -> bool:
            raise ValueError('cache_if error')

        @cachu.cache(ttl=300, backend='memory', cache_if=bad_cache_if)
        def func(x: int) -> int:
            return x * 2

        with pytest.raises(ValueError, match='cache_if error'):
            func(5)


class TestValidateExceptions:
    """Tests for exception handling in validate callbacks.
    """

    def test_validate_exception_propagates(self):
        """Verify exception in validate propagates to caller.

        If the validate callback raises an exception, it should propagate
        rather than being silently swallowed.
        """
        def bad_validate(entry: cachu.cache) -> bool:
            raise ValueError('validate error')

        @cachu.cache(ttl=300, backend='memory', validate=bad_validate)
        def func(x: int) -> int:
            return x * 2

        func(5)

        with pytest.raises(ValueError, match='validate error'):
            func(5)


class TestCacheIfBehavior:
    """Tests for cache_if callback behavior.
    """

    def test_cache_if_prevents_caching_on_false(self):
        """Verify cache_if=False prevents caching the result.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: r > 0)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x

        func(-1)
        func(-1)
        assert call_count == 2

        func(1)
        func(1)
        assert call_count == 3

    def test_cache_if_allows_caching_on_true(self):
        """Verify cache_if=True allows caching the result.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend='memory', cache_if=lambda r: r is not None)
        def func(x: int) -> int | None:
            nonlocal call_count
            call_count += 1
            return x if x > 0 else None

        func(1)
        func(1)
        assert call_count == 1


class TestValidateBehavior:
    """Tests for validate callback behavior.
    """

    def test_validate_false_triggers_recompute(self):
        """Verify validate=False causes recomputation.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend='memory', validate=lambda e: False)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        func(5)
        assert call_count == 2

    def test_validate_true_uses_cache(self):
        """Verify validate=True returns cached value.
        """
        call_count = 0

        @cachu.cache(ttl=300, backend='memory', validate=lambda e: True)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        func(5)
        func(5)
        assert call_count == 1

    def test_validate_receives_cache_entry(self):
        """Verify validate receives CacheEntry with correct fields.
        """
        entries_seen = []

        def capture_entry(entry):
            entries_seen.append(entry)
            return True

        @cachu.cache(ttl=300, backend='memory', validate=capture_entry)
        def func(x: int) -> int:
            return x * 2

        func(5)
        func(5)

        assert len(entries_seen) == 1
        entry = entries_seen[0]
        assert entry.value == 10
        assert entry.created_at is not None
        assert entry.age >= 0
