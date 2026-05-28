"""Tests for args-aware (2-arg) ttl, cache_if, and validate predicates.
"""
import datetime

import pytest

import cachu


class TestArgsAwareTTL:
    """Tests for 2-arg callable TTL in sync functions.
    """

    def test_2arg_ttl_receives_filtered_args(self):
        """Verify a 2-arg ttl callable receives the filtered args dict.
        """
        received = []

        def capture(result: int, args: dict) -> int:
            received.append((result, args))
            return 60

        @cachu.cache(ttl=capture, backend='memory')
        def compute(x: int, y: int) -> int:
            return x + y

        compute(3, y=4)

        assert len(received) == 1
        result_val, args_dict = received[0]
        assert result_val == 7
        assert args_dict == {'x': 3, 'y': 4}

    def test_2arg_ttl_varies_by_args(self):
        """Verify TTL can resolve differently based on call args.
        """
        seen_ttls = []

        def ttl_by_date(result, args: dict) -> int:
            ttl = 60 if args['date'] == datetime.date(2026, 5, 28) else 3600
            seen_ttls.append(ttl)
            return ttl

        @cachu.cache(ttl=ttl_by_date, backend='memory')
        def fetch(date: datetime.date) -> str:
            return f'data-{date.isoformat()}'

        fetch(datetime.date(2026, 5, 28))
        fetch(datetime.date(2024, 1, 1))

        assert seen_ttls == [60, 3600]

    def test_1arg_ttl_still_works(self):
        """Verify legacy 1-arg TTL callables remain unchanged.
        """
        received = []

        def legacy(result: int) -> int:
            received.append(result)
            return 60

        @cachu.cache(ttl=legacy, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        compute(5)
        assert received == [10]

    def test_2arg_ttl_with_default_param_is_2arg(self):
        """Verify a callable with `args=None` default is treated as 2-arg.

        Common style: def f(result, args=None) opts into the 2-arg form
        without forcing the caller to always pass args.
        """
        received = []

        def predicate(result, args=None):
            received.append(args)
            return 60

        @cachu.cache(ttl=predicate, backend='memory')
        def compute(x: int) -> int:
            return x

        compute(5)
        assert received == [{'x': 5}]

    def test_starargs_callable_is_2arg(self):
        """Verify *args callables get the args dict.
        """
        received = []

        def predicate(*everything):
            received.append(everything)
            return 60

        @cachu.cache(ttl=predicate, backend='memory')
        def compute(x: int) -> int:
            return x

        compute(5)
        assert len(received) == 1
        assert received[0] == (5, {'x': 5})


class TestArgsAwareCacheIf:
    """Tests for 2-arg cache_if in sync functions.
    """

    def test_2arg_cache_if_receives_args(self):
        """Verify a 2-arg cache_if callable receives result and args.
        """
        received = []

        def gate(result, args: dict) -> bool:
            received.append((result, args))
            return True

        @cachu.cache(cache_if=gate, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        compute(7)

        assert received == [(14, {'x': 7})]

    def test_skip_empty_for_today_only(self):
        """Verify the today-only-skip-empty pattern: cached for past, refetched for today.
        """
        today = datetime.date(2026, 5, 28)
        call_count = 0

        def gate(result, args: dict) -> bool:
            if result:
                return True
            return args['date'] != today

        @cachu.cache(cache_if=gate, backend='memory')
        def fetch(date: datetime.date) -> list:
            nonlocal call_count
            call_count += 1
            return []

        fetch(today)
        fetch(today)
        assert call_count == 2

        past = datetime.date(2024, 1, 1)
        fetch(past)
        fetch(past)
        assert call_count == 3

    def test_1arg_cache_if_still_works(self):
        """Verify legacy 1-arg cache_if remains unchanged.
        """
        @cachu.cache(cache_if=lambda r: r > 0, backend='memory')
        def compute(x: int) -> int:
            return x

        assert compute(5) == 5
        assert compute(-1) == -1


class TestArgsAwareValidate:
    """Tests for 2-arg validate predicate.
    """

    def test_2arg_validate_receives_args(self):
        """Verify a 2-arg validate callable receives entry and args on hit.
        """
        seen_args = []

        def gate(entry, args: dict) -> bool:
            seen_args.append(args)
            return True

        @cachu.cache(validate=gate, backend='memory')
        def compute(x: int) -> int:
            return x * 2

        compute(5)
        compute(5)

        assert seen_args == [{'x': 5}]

    def test_2arg_validate_can_invalidate_by_args(self):
        """Verify validate can use args to force recomputation per call shape.
        """
        call_count = 0

        @cachu.cache(
            validate=lambda entry, args: args.get('fresh') is not True,
            backend='memory',
        )
        def compute(x: int, fresh: bool = False) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        compute(5)
        compute(5)
        assert call_count == 1

        compute(5, fresh=True)
        assert call_count == 2

    def test_1arg_validate_still_works(self):
        """Verify legacy 1-arg validate remains unchanged.
        """
        @cachu.cache(
            validate=lambda entry: entry.age < 10,
            backend='memory',
        )
        def compute(x: int) -> int:
            return x

        compute(5)
        assert compute(5) == 5


class TestArityErrors:
    """Tests for decoration-time arity validation.
    """

    def test_zero_arg_predicate_raises(self):
        """Verify a predicate with no positional params raises at decoration.
        """
        with pytest.raises(TypeError, match='0 positional args'):
            @cachu.cache(ttl=lambda: 60, backend='memory')
            def f(x: int) -> int:
                return x

    def test_three_arg_predicate_raises(self):
        """Verify a predicate with >2 required positionals raises at decoration.
        """
        def too_many(a, b, c):
            return 60

        with pytest.raises(TypeError, match='3 required args'):
            @cachu.cache(ttl=too_many, backend='memory')
            def f(x: int) -> int:
                return x

    def test_builtin_predicate_falls_back_to_1arg(self):
        """Verify a callable where inspect.signature fails falls back to legacy 1-arg.
        """
        @cachu.cache(cache_if=bool, backend='memory')
        def f(x: int) -> int:
            return x

        assert f(5) == 5
        assert f(0) == 0


class TestExcludeStripsArgsDict:
    """Tests for filtering coupling between key and predicate args.
    """

    def test_exclude_strips_from_predicate_args(self):
        """Verify a param in `exclude=` is dropped from the predicate's args dict.
        """
        captured = []

        def predicate(result, args: dict) -> int:
            captured.append(args)
            return 60

        @cachu.cache(ttl=predicate, exclude={'session'}, backend='memory')
        def compute(x: int, session: str) -> int:
            return x

        compute(5, session='abc')

        assert captured == [{'x': 5}]

    def test_self_and_underscore_stripped_from_predicate_args(self):
        """Verify self/cls/_-prefixed params are dropped, matching key derivation.
        """
        captured = []

        def predicate(result, args: dict) -> int:
            captured.append(args)
            return 60

        class Holder:
            @cachu.cache(ttl=predicate, backend='memory')
            def compute(self, x: int) -> int:
                return x

        Holder().compute(5)
        assert captured == [{'x': 5}]


class TestHelpersWithArgsAware:
    """Tests that .clear / .refresh / .get / .set still work with 2-arg predicates.
    """

    def test_refresh_with_2arg_predicates(self):
        """Verify .refresh() forces re-computation with 2-arg predicates active.
        """
        call_count = 0

        @cachu.cache(
            ttl=lambda r, a: 60,
            cache_if=lambda r, a: True,
            backend='memory',
        )
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x

        compute(5)
        assert call_count == 1

        compute(5)
        assert call_count == 1

        compute.refresh(x=5)
        assert call_count == 2

    def test_clear_with_2arg_predicates(self):
        """Verify .clear() invalidates a specific entry with 2-arg predicates active.
        """
        call_count = 0

        @cachu.cache(ttl=lambda r, a: 60, backend='memory')
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x

        compute(5)
        assert call_count == 1

        compute.clear(x=5)
        compute(5)
        assert call_count == 2


class TestTodayAwarePreset:
    """Tests for cachu.presets.today_aware.
    """

    def test_today_uses_short_ttl(self):
        """Verify the preset selects today_ttl when date matches today.
        """
        fixed_today = datetime.date(2026, 5, 28)
        seen = []

        def fake_today():
            return fixed_today

        preset = cachu.presets.today_aware(
            date_param='date',
            today_ttl=60,
            past_ttl=3600,
            today_fn=fake_today,
        )

        seen_ttl = preset['ttl']
        assert seen_ttl([1, 2, 3], {'date': fixed_today}) == 60
        assert seen_ttl([1, 2, 3], {'date': datetime.date(2024, 1, 1)}) == 3600

    def test_skip_empty_today_caches_past_empty(self):
        """Verify cache_if skips empty for today but keeps empty for past.
        """
        fixed_today = datetime.date(2026, 5, 28)
        preset = cachu.presets.today_aware(
            date_param='date',
            today_ttl=60,
            past_ttl=3600,
            today_fn=lambda: fixed_today,
        )

        gate = preset['cache_if']

        assert gate([], {'date': fixed_today}) is False
        assert gate([], {'date': datetime.date(2024, 1, 1)}) is True
        assert gate([1], {'date': fixed_today}) is True
        assert gate([1], {'date': datetime.date(2024, 1, 1)}) is True

    def test_today_aware_integrates_with_decorator(self):
        """Verify today_aware works end-to-end through @cache.
        """
        fixed_today = datetime.date(2026, 5, 28)
        fetch_count = 0

        @cachu.cache(
            backend='memory',
            **cachu.presets.today_aware(
                date_param='date',
                today_ttl=60,
                past_ttl=3600,
                today_fn=lambda: fixed_today,
            ),
        )
        def fetch(date: datetime.date) -> list:
            nonlocal fetch_count
            fetch_count += 1
            return []

        fetch(fixed_today)
        fetch(fixed_today)
        assert fetch_count == 2

        past = datetime.date(2024, 1, 1)
        fetch(past)
        fetch(past)
        assert fetch_count == 3

    def test_today_aware_missing_param_raises_keyerror(self):
        """Verify a clear error when date_param is not in the args dict.
        """
        preset = cachu.presets.today_aware(
            date_param='wrong_name',
            today_ttl=60,
            past_ttl=3600,
        )

        with pytest.raises(KeyError, match='wrong_name'):
            preset['ttl']([], {'date': datetime.date.today()})


class TestAsyncArgsAware:
    """Tests for args-aware predicates in async functions.
    """

    async def test_2arg_ttl_async(self):
        """Verify a 2-arg ttl works on async functions.
        """
        received = []

        def capture(result, args: dict) -> int:
            received.append((result, args))
            return 60

        @cachu.cache(ttl=capture, backend='memory')
        async def compute(x: int) -> int:
            return x * 2

        await compute(5)

        assert received == [(10, {'x': 5})]

    async def test_2arg_cache_if_async(self):
        """Verify a 2-arg cache_if works on async functions.
        """
        seen = []

        def gate(result, args: dict) -> bool:
            seen.append(args)
            return True

        @cachu.cache(cache_if=gate, backend='memory')
        async def compute(x: int) -> int:
            return x

        await compute(7)
        assert seen == [{'x': 7}]

    async def test_today_aware_async(self):
        """Verify today_aware preset on an async function.
        """
        fixed_today = datetime.date(2026, 5, 28)
        fetch_count = 0

        @cachu.cache(
            backend='memory',
            **cachu.presets.today_aware(
                date_param='date',
                today_ttl=60,
                past_ttl=3600,
                today_fn=lambda: fixed_today,
            ),
        )
        async def fetch(date: datetime.date) -> list:
            nonlocal fetch_count
            fetch_count += 1
            return []

        await fetch(fixed_today)
        await fetch(fixed_today)
        assert fetch_count == 2

        past = datetime.date(2024, 1, 1)
        await fetch(past)
        await fetch(past)
        assert fetch_count == 3


@pytest.mark.redis
class TestArgsAwareRedis:
    """Smoke tests for args-aware predicates on the redis backend.
    """

    def test_2arg_ttl_redis(self):
        """Verify a 2-arg ttl callable works against redis.
        """
        received = []

        def predicate(result, args: dict) -> int:
            received.append(args)
            return 60

        @cachu.cache(ttl=predicate, backend='redis')
        def compute(x: int) -> int:
            return x * 2

        compute(5)
        assert received == [{'x': 5}]

    def test_today_aware_redis(self):
        """Verify the today_aware preset on the redis backend.
        """
        fixed_today = datetime.date(2026, 5, 28)
        fetch_count = 0

        @cachu.cache(
            backend='redis',
            **cachu.presets.today_aware(
                date_param='date',
                today_ttl=60,
                past_ttl=3600,
                today_fn=lambda: fixed_today,
            ),
        )
        def fetch(date: datetime.date) -> list:
            nonlocal fetch_count
            fetch_count += 1
            return []

        fetch(fixed_today)
        fetch(fixed_today)
        assert fetch_count == 2
