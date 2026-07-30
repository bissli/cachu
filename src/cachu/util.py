"""Utility functions for cache key generation and validation.
"""
import inspect
import time
from collections.abc import Callable
from typing import Any

from .api import CacheEntry

_KEY_ESCAPE = {'%': '%25', ' ': '%20', '=': '%3D', '|': '%7C', '*': '%2A', '?': '%3F', '[': '%5B', ']': '%5D'}
_KEY_ESCAPE_TABLE = str.maketrans({ord(k): v for k, v in _KEY_ESCAPE.items()})

_GLOB_ESCAPE = {'*': '[*]', '?': '[?]', '[': '[[]'}
_GLOB_ESCAPE_TABLE = str.maketrans({ord(k): v for k, v in _GLOB_ESCAPE.items()})


def _escape_glob(text: str) -> str:
    """Escape glob metacharacters so `text` can only match itself.

    Parameters
    ----------
    text : str
        Literal fragment being interpolated into a clear or count pattern.

    Returns
    -------
    str
        The fragment with '*', '?' and '[' neutralised.

    Notes
    -----
    - Applied to the configured `key_prefix` and to a tag on the PATTERN
      side only. A key holds both literally, and `_render_value` already
      percent-escapes the same characters inside parameter values, so the
      two sides agree.
    - Single-character classes are the one escape form shared by all three
      matchers cachu targets: fnmatch (memory), SQLite GLOB (file) and
      Redis's own glob. A backslash would work on Redis and fail on the
      other two.
    - ']' needs no escaping: outside a class it is already literal in all
      three.
    - '\' is deliberately NOT escaped, because no single form works
      everywhere: matching a literal backslash needs '\\' on Redis and a
      bare '\' on fnmatch and SQLite GLOB, and each form fails on the
      other engine (measured). A prefix, tag or argument holding a
      backslash - a Windows path is the realistic case - therefore does not
      clear on Redis. Fixing it needs a per-backend pattern dialect rather
      than one escape table.
    - Without this, `key_prefix='app[dev]:'` turns into a character class
      and every clear silently matches NOTHING, while `key_prefix='p*x:'`
      matches 'prod-x:' as well - one prefix's clear deleting another
      prefix's entries, which is the same "reaches keys it does not own"
      fault that region scoping exists to prevent.
    """
    return text.translate(_GLOB_ESCAPE_TABLE)


def _stable_repr(value: Any) -> str:
    """Render a value to a deterministic, restart-stable string.

    Sets and dicts are emitted in a canonical (sorted) order so the cache key
    does not depend on PYTHONHASHSEED-randomised iteration order.
    """
    if isinstance(value, (set, frozenset)):
        return '{' + ', '.join(_stable_repr(v) for v in sorted(value, key=repr)) + '}'
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda kv: repr(kv[0]))
        return '{' + ', '.join(f'{_stable_repr(k)}: {_stable_repr(v)}' for k, v in items) + '}'
    if isinstance(value, list):
        return '[' + ', '.join(_stable_repr(v) for v in value) + ']'
    if isinstance(value, tuple):
        return '(' + ', '.join(_stable_repr(v) for v in value) + ')'
    return repr(value)


def _render_value(value: Any) -> str:
    """Render a value for a cache key: stable across restarts and free of the
    space/'='/'|' delimiters and glob metacharacters used to build keys and
    clear patterns.
    """
    return _stable_repr(value).translate(_KEY_ESCAPE_TABLE)


def _is_connection_like(obj: Any) -> bool:
    """Check if object appears to be a database connection.

    Detects SQLAlchemy connections, psycopg2, pyodbc, sqlite3, and similar.
    """
    if hasattr(obj, 'driver_connection'):
        return True

    if hasattr(obj, 'dialect'):
        return True

    if hasattr(obj, 'engine'):
        return True

    obj_type = str(type(obj))
    connection_indicators = ('Connection', 'Engine', 'psycopg', 'pyodbc', 'sqlite3')

    return any(indicator in obj_type for indicator in connection_indicators)


def _normalize_tag(tag: str) -> str:
    """Normalize tag to always be wrapped in pipes.
    """
    if not tag:
        return ''
    tag = tag.strip('|')
    tag = tag.replace('|', '.')
    return f'|{tag}|'


def make_key_generator(
    fn: Callable[..., Any],
    tag: str = '',
    exclude: set[str] | None = None,
) -> Callable[..., tuple[str, dict[str, Any]]]:
    """Create a key generator function for the given function.

    The generated keys include:
    - Function name
    - Tag (if provided)
    - All parameters except: self, cls, connections, underscore-prefixed, and excluded

    Args:
        fn: The function to generate keys for
        tag: Optional tag for key grouping
        exclude: Parameter names to exclude from the key

    Returns
        A function that generates (cache_key, filtered_args_dict) tuples from
        arguments. The filtered dict applies the same filtering rules used to
        build the key, so predicates that consume it see exactly the args that
        contribute to the key.
    """
    exclude = exclude or set()
    unwrapped_fn = getattr(fn, '__wrapped__', fn)
    fn_name = unwrapped_fn.__name__

    if tag:
        key_prefix = f'{fn_name}|{_normalize_tag(tag)}'
    else:
        key_prefix = fn_name

    argspec = inspect.getfullargspec(unwrapped_fn)
    args_reversed = list(reversed(argspec.args or []))
    defaults_reversed = list(reversed(argspec.defaults or []))
    args_with_defaults = {args_reversed[i]: default for i, default in enumerate(defaults_reversed)}

    def generate_key(*args: Any, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        """Generate a (cache_key, filtered_args) tuple from function arguments.
        """
        positional_args = args[:len(argspec.args)]
        varargs = args[len(argspec.args):]

        as_kwargs = dict(**args_with_defaults)
        as_kwargs.update(dict(zip(argspec.args, positional_args)))
        as_kwargs.update({f'vararg{i + 1}': varg for i, varg in enumerate(varargs)})
        as_kwargs.update(**kwargs)

        filtered = {
            k: v for k, v in as_kwargs.items()
            if k not in {'self', 'cls'}
            and not k.startswith('_')
            and k not in exclude
            and not _is_connection_like(v)
        }

        params_str = ' '.join(f'{k}={_render_value(v)}' for k, v in sorted(filtered.items()))
        return f'{key_prefix}|{params_str}', filtered

    return generate_key


def _predicate_arity(fn: Callable[..., Any]) -> int:
    """Return 1 or 2: how many positional args the predicate expects.

    Predicates may be passed in either legacy 1-arg form (receives the result
    or CacheEntry only) or new 2-arg form (also receives the filtered args
    dict). Detection is done once at decoration time by inspecting the
    callable's signature.

    Rules:
    - Count positional-acceptable params (POSITIONAL_ONLY,
      POSITIONAL_OR_KEYWORD) regardless of default. Defaults are common when
      users write `def f(result, args=None)` to opt into the 2-arg form
      gracefully.
    - A *args parameter accepts any number of positionals, treat as 2-arg.
    - 0 positionals raises TypeError at decoration.
    - >2 required positionals and no *args raises TypeError at decoration.
    - inspect.signature() failure (builtins, C extensions) falls back to
      legacy 1-arg.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return 1

    positional_kinds = (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )
    positionals = [p for p in sig.parameters.values() if p.kind in positional_kinds]
    has_var_positional = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL
        for p in sig.parameters.values()
    )

    if has_var_positional:
        return 2

    required = [p for p in positionals if p.default is p.empty]
    name = getattr(fn, '__name__', repr(fn))

    if not positionals:
        raise TypeError(
            f'cachu predicate {name!r} takes 0 positional args; '
            f'expected (result) or (result, args)'
        )
    if len(required) > 2:
        raise TypeError(
            f'cachu predicate {name!r} takes {len(required)} required args; '
            f'expected (result) or (result, args)'
        )
    if len(positionals) >= 2:
        return 2
    return 1


def mangle_key(key: str, key_prefix: str, ttl: int) -> str:
    """Apply key mangling with prefix and TTL region.

    Args:
        key: The base cache key
        key_prefix: Global key prefix from config
        ttl: TTL in seconds (used as region identifier)

    Returns
        The mangled key
    """
    region = _seconds_to_region_name(ttl)
    return f'{region}:{key_prefix}{key}'


def _seconds_to_region_name(seconds: int) -> str:
    """Convert seconds to a human-readable region name.

    Parameters
    ----------
    seconds : int
        TTL of the region. A float is truncated to an int first.

    Returns
    -------
    str
        Region segment of a mangled key, e.g. '30s', '5m', '1h', '1d', or
        'dynamic' for the callable-ttl sentinel -1.

    Notes
    -----
    - The truncation is what keeps the name a function of the REGION rather
      than of the literal a decorator was written with. `manager` keys its
      regions by a `(package, backend, ttl)` tuple, and 300 == 300.0 hashes
      equal, so `@cache(ttl=300)` and `@cache(ttl=300.0)` - the second is
      what `timedelta.total_seconds()` or a JSON config yields - share ONE
      region. Without truncation they wrote keys under '5m' and '5.0m' while
      the region recorded whichever imported first, so every clear built one
      name and could never match the other's entries.
    """
    seconds = int(seconds)
    if seconds == -1:
        return 'dynamic'
    if seconds < 60:
        return f'{seconds}s'
    elif seconds < 3600:
        return f'{seconds // 60}m'
    elif seconds < 86400:
        return f'{seconds // 3600}h'
    else:
        return f'{seconds // 86400}d'


def make_clear_pattern(
    tag: str | None,
    key_prefix: str,
    ttl: int,
    global_clear: bool = False,
) -> str:
    """Build the glob a clear applies to one (backend, ttl) cache region.

    Parameters
    ----------
    tag : str or None
        Tag to narrow to, or None for every entry of the region.
    key_prefix : str
        Configured key prefix; skipped when `global_clear` is set.
    ttl : int
        TTL of the region being cleared, which names its key segment.
    global_clear : bool, default False
        Match every key prefix rather than only the configured one.

    Returns
    -------
    str
        Glob matching only keys of cachu's own shape,
        `<region>:<key_prefix><fn_name>|<tag>|<params>`.

    Notes
    -----
    - Never returns None and never returns '*'. A pattern that widened to
      '*' made `RedisBackend.clear` SCAN and UNLINK every key in the logical
      DB, including keys cachu never wrote - reachable with nothing more
      exotic than the default `key_prefix=''`.
    - `global_clear` widens the PREFIX, not the namespace: it exists to
      reach entries written under another `key_prefix`, not another
      library's keys.
    - The `|` is what pins the shape. `mangle_key` always emits the
      `fn_name|params` separator, so a foreign key that merely opens with a
      region-like segment cannot match, and neither can the `lock:` key of
      a dogpile mutex a live caller is holding.
    - One glob per region rather than one shared `*:` glob: the region
      segment comes from the TTL of the region being cleared, so a 5m clear
      cannot reach the 1h entries of the same function.
    - `key_prefix` and `tag` are glob-escaped, so a prefix or tag containing
      '*', '?' or '[' matches itself rather than its neighbours or nothing
      at all.
    """
    region = _seconds_to_region_name(ttl)
    prefix = '' if global_clear else _escape_glob(key_prefix)
    if tag:
        return f'{region}:{prefix}*{_escape_glob(_normalize_tag(tag))}*'
    return f'{region}:{prefix}*|*'


def make_partial_pattern(
    fn_name: str,
    tag: str,
    key_prefix: str,
    ttl: int,
    global_clear: bool = False,
    **kwargs: Any,
) -> str:
    """Build a glob pattern for one decorated function's entries.

    Constructs patterns matching the key format produced by
    make_key_generator + mangle_key. Supports exact (all params),
    partial (some params), and blanket (no params) matching.

    Notes
    -----
    - `key_prefix` and `tag` are glob-escaped for the same reason
      `make_clear_pattern` escapes them: unescaped, a prefix or tag holding
      '*', '?' or '[' would either match its neighbours or match nothing.
      `fn_name` is a Python identifier and needs no escaping, and parameter
      values are already escaped by `_render_value` on both sides.
    - `global_clear` drops the `key_prefix` and keeps the region segment,
      which is the whole of its documented contract. Dropping the region as
      well left the glob unanchored at the front: `*fn_name|*` matched a
      foreign `worker:fn_name|job-7`, the same function's entries in other
      TTL regions, and the `lock:` key of a mutex a live caller was holding
      - so `.clear(_global=True)` could release someone else's lock. A
      decorated function only ever writes into its own region, so anchoring
      costs nothing it was meant to reach.
    """
    region = _seconds_to_region_name(ttl)
    norm_tag = _escape_glob(_normalize_tag(tag))

    if tag:
        base = f'{fn_name}|{norm_tag}'
    else:
        base = fn_name

    if global_clear:
        prefix = f'{region}:*{base}|'
    else:
        prefix = f'{region}:{_escape_glob(key_prefix)}{base}|'

    if kwargs:
        fragments = [f'{k}={_render_value(v)}' for k, v in sorted(kwargs.items())]
        params = f'*{"*".join(fragments)}*'
    else:
        params = '*'

    return f'{prefix}{params}'


def validate_entry(
    value: Any,
    created_at: float | None,
    validate: Callable[..., bool] | None,
    args_dict: dict[str, Any] | None = None,
    validate_arity: int = 1,
) -> bool:
    """Validate a cached entry using the validate callback.

    The validate callable is called with (entry) when validate_arity is 1
    (legacy) or with (entry, args_dict) when validate_arity is 2. args_dict
    holds the same filtered args that generate_key produced for this call.
    """
    if validate is None or created_at is None:
        return True

    entry = CacheEntry(
        value=value,
        created_at=created_at,
        age=time.time() - created_at,
    )
    if validate_arity == 2:
        return validate(entry, args_dict or {})
    return validate(entry)
