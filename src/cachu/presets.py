"""Composable predicate presets for common args-aware caching patterns.

Presets build on the 2-arg form of cachu's ttl and cache_if predicates.
They return a dict of decorator kwargs intended to be splatted into
@cache(**preset(...)), keeping the call site declarative.
"""
import datetime
from collections.abc import Callable
from typing import Any


def today_aware(
    *,
    date_param: str,
    today_ttl: int,
    past_ttl: int,
    skip_empty_today: bool = True,
    skip_empty_past: bool = False,
    today_fn: Callable[[], datetime.date] = datetime.date.today,
) -> dict[str, Any]:
    """Build {ttl, cache_if} for date-keyed fetches where 'today' is volatile.

    The decorated function must take a date parameter named `date_param`.
    Calls where the date equals today() use the short `today_ttl` and
    (by default) skip caching empty results so a transient empty does
    not pin the cache. Calls for historical dates use the long `past_ttl`
    and cache empty results (legitimate empties for Sundays, delisted
    instruments, etc. should not re-hit the upstream).

    Args:
        date_param: Name of the function parameter that holds the date.
                    Must not be filtered out by `exclude=` on the decorator.
        today_ttl: TTL in seconds for calls matching today.
        past_ttl: TTL in seconds for calls in the past.
        skip_empty_today: If True (default), do not cache empty results
                          for today. Concurrent callers will each re-fetch
                          until upstream returns non-empty.
        skip_empty_past: If True, also skip empty results for past dates.
                         Default False; historical empties usually stay
                         empty and benefit from caching.
        today_fn: Callable returning today's date. Injected for testability.

    Returns
        A dict with `ttl` and `cache_if` keys suitable for splatting into
        @cache(...).

    Example:
        @cache(tag='fmp', **today_aware(
            date_param='date', today_ttl=900, past_ttl=86400))
        def get_filings_for_date(date):
            ...
    """
    def _ttl(result: Any, args: dict[str, Any]) -> int:
        if date_param not in args:
            raise KeyError(
                f'today_aware(date_param={date_param!r}) requires '
                f'{date_param!r} in the filtered args dict; available keys: '
                f'{sorted(args)}. Likely cause: parameter is named differently '
                f'or was removed by `exclude=`.'
            )
        return today_ttl if args[date_param] == today_fn() else past_ttl

    def _cache_if(result: Any, args: dict[str, Any]) -> bool:
        if date_param not in args:
            raise KeyError(
                f'today_aware(date_param={date_param!r}) requires '
                f'{date_param!r} in the filtered args dict; available keys: '
                f'{sorted(args)}. Likely cause: parameter is named differently '
                f'or was removed by `exclude=`.'
            )
        is_today = args[date_param] == today_fn()
        if result:
            return True
        return not (skip_empty_today if is_today else skip_empty_past)

    return {'ttl': _ttl, 'cache_if': _cache_if}
