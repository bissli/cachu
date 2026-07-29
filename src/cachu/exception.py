"""Custom exceptions for the cache library.
"""


class CacheError(Exception):
    """Base exception for all cache-related errors.
    """


class BackendNotFoundError(CacheError, ValueError):
    """Raised when a requested backend type is not available.

    Notes
    -----
    - Also a ValueError, which is what this condition raised before the
      class was wired up, so existing `except ValueError` handlers keep
      working.
    """


class ConfigurationError(CacheError, ValueError):
    """Raised when a configuration value is invalid.

    Notes
    -----
    - Also a ValueError, so `except ValueError` around `configure()` keeps
      working while `except cachu.CacheError` now catches it too.
    """


class CacheLockTimeout(CacheError):
    """Raised when a dogpile mutex could not be acquired within lock_timeout.

    Only raised when the resolved config sets ``on_lock_timeout='raise'``. The
    default (``'run'``) executes the decorated function instead.
    """
