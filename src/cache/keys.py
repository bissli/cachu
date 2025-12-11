"""Cache key generation and parameter filtering.
"""
import inspect
from collections.abc import Callable
from typing import Any


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
) -> Callable[..., str]:
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
        A function that generates cache keys from arguments
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

    def generate_key(*args: Any, **kwargs: Any) -> str:
        """Generate a cache key from function arguments.
        """
        positional_args = args[:len(argspec.args)]
        varargs = args[len(argspec.args):]

        as_kwargs = dict(**args_with_defaults)
        as_kwargs.update(dict(zip(argspec.args, positional_args)))
        as_kwargs.update({f'vararg{i+1}': varg for i, varg in enumerate(varargs)})
        as_kwargs.update(**kwargs)

        filtered = {
            k: v for k, v in as_kwargs.items()
            if k not in {'self', 'cls'}
            and not k.startswith('_')
            and k not in exclude
            and not _is_connection_like(v)
        }

        params_str = ' '.join(f'{k}={repr(v)}' for k, v in sorted(filtered.items()))
        return f'{key_prefix}|{params_str}'

    return generate_key


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
    """
    if seconds < 60:
        return f'{seconds}s'
    elif seconds < 3600:
        return f'{seconds // 60}m'
    elif seconds < 86400:
        return f'{seconds // 3600}h'
    else:
        return f'{seconds // 86400}d'
