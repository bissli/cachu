"""File-based cache backend using SQLite.

Note: This module previously used DBM. Migration to SQLite happened in v0.2.0.
Existing DBM cache files will be ignored - clear your cache directory on upgrade.
"""
from .sqlite import SqliteBackend

FileBackend = SqliteBackend

__all__ = ['FileBackend']
