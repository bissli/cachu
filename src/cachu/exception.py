"""Custom exceptions for the cache library.
"""


class CacheError(Exception):
    """Base exception for all cache-related errors.
    """


class BackendNotFoundError(CacheError):
    """Raised when a requested backend type is not available.
    """


class ConfigurationError(CacheError):
    """Raised when there is an invalid configuration.
    """
