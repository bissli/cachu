"""Cache backend implementations.

Four backends ship with cachu, all reachable by name from `@cache(backend=...)`
and `configure(backend_default=...)`:

- 'memory' : `memory.MemoryBackend`, process-local, optionally LRU-bounded.
- 'file'   : `sqlite.SqliteBackend`, a SQLite file under `file_dir`.
- 'redis'  : `redis.RedisBackend`, shared across processes.
- 'null'   : `null.NullBackend`, never stores anything, so the decorated
             function re-executes on every call. This is how a single cache
             is switched off without the process-wide `cachu.disable()`.
"""
from . import memory, null, redis, sqlite
from .memory import MemoryBackend
from .null import NullBackend
from .redis import RedisBackend
from .sqlite import SqliteBackend

__all__ = [
    'MemoryBackend',
    'NullBackend',
    'RedisBackend',
    'SqliteBackend',
    'memory',
    'null',
    'redis',
    'sqlite',
]
