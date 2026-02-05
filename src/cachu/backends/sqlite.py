"""SQLite-based cache backend.
"""
import asyncio
import logging
import pickle
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal

from ..api import NO_VALUE, Backend
from ..mutex import AsyncCacheMutex, AsyncioMutex, CacheMutex, ThreadingMutex

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import aiosqlite


def _get_aiosqlite_module() -> Any:
    """Import aiosqlite module, raising helpful error if not installed.
    """
    try:
        import aiosqlite
        return aiosqlite
    except ImportError as e:
        raise RuntimeError(
            "Async SQLite support requires the 'aiosqlite' package. "
            "Install with: pip install cachu[async]"
        ) from e


class SqliteBackend(Backend):
    """Unified SQLite file-based cache backend with both sync and async interfaces.
    """

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath
        self._sync_lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        self._async_write_lock = asyncio.Lock()
        self._async_connection: aiosqlite.Connection | None = None
        self._async_initialized = False
        self._sync_initialized = False

    def _ensure_sync_initialized(self) -> None:
        """Ensure sync database schema is initialized (lazy, once).
        """
        if self._sync_initialized:
            return
        with self._sync_lock:
            if self._sync_initialized:
                return
            conn = sqlite3.connect(self._filepath)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires
                    ON cache(expires_at)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_stats (
                        fn_name TEXT PRIMARY KEY,
                        hits INTEGER DEFAULT 0,
                        misses INTEGER DEFAULT 0
                    )
                """)
                conn.commit()
            finally:
                conn.close()
            self._sync_initialized = True

    async def _ensure_async_initialized(self) -> 'aiosqlite.Connection':
        """Ensure async database is initialized and return connection.
        """
        async with self._async_lock:
            if self._async_connection is None:
                aiosqlite = _get_aiosqlite_module()
                self._async_connection = await aiosqlite.connect(self._filepath)
                await self._async_connection.execute('PRAGMA journal_mode=WAL')
                await self._async_connection.execute('PRAGMA busy_timeout=5000')

            if not self._async_initialized:
                await self._async_connection.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                await self._async_connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires
                    ON cache(expires_at)
                """)
                await self._async_connection.execute("""
                    CREATE TABLE IF NOT EXISTS cache_stats (
                        fn_name TEXT PRIMARY KEY,
                        hits INTEGER DEFAULT 0,
                        misses INTEGER DEFAULT 0
                    )
                """)
                await self._async_connection.commit()
                self._async_initialized = True

        return self._async_connection

    def _get_sync_connection(self) -> sqlite3.Connection:
        """Get a sync database connection.
        """
        self._ensure_sync_initialized()
        return sqlite3.connect(self._filepath)

    def _fnmatch_to_glob(self, pattern: str) -> str:
        """Convert fnmatch pattern to SQLite GLOB pattern.
        """
        return pattern

    def _schedule_async_delete(self, key: str) -> None:
        """Schedule a background deletion task (fire-and-forget).
        """
        async def _delete() -> None:
            try:
                async with self._async_write_lock:
                    conn = await self._ensure_async_initialized()
                    await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    await conn.commit()
            except Exception:
                pass

        asyncio.create_task(_delete())

    # ===== Sync interface =====

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT value, expires_at FROM cache WHERE key = ?',
                    (key,),
                )
                row = cursor.fetchone()

                if row is None:
                    return NO_VALUE

                value_blob, expires_at = row
                if time.time() > expires_at:
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                    return NO_VALUE

                return pickle.loads(value_blob)
            except Exception:
                return NO_VALUE
            finally:
                conn.close()

    def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT value, created_at, expires_at FROM cache WHERE key = ?',
                    (key,),
                )
                row = cursor.fetchone()

                if row is None:
                    return NO_VALUE, None

                value_blob, created_at, expires_at = row
                if time.time() > expires_at:
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                    return NO_VALUE, None

                return pickle.loads(value_blob), created_at
            except Exception:
                return NO_VALUE, None
            finally:
                conn.close()

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        value_blob = pickle.dumps(value)

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
                       VALUES (?, ?, ?, ?)""",
                    (key, value_blob, now, now + ttl),
                )
                conn.commit()
            finally:
                conn.close()

    def delete(self, key: str) -> None:
        """Delete value by key.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                conn.commit()
            except Exception:
                pass
            finally:
                conn.close()

    def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute('SELECT COUNT(*) FROM cache')
                    count = cursor.fetchone()[0]
                    conn.execute('DELETE FROM cache')
                    conn.commit()
                    return count

                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ?',
                    (glob_pattern,),
                )
                count = cursor.fetchone()[0]
                conn.execute('DELETE FROM cache WHERE key GLOB ?', (glob_pattern,))
                conn.commit()
                return count
            except Exception:
                return 0
            finally:
                conn.close()

    def keys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute(
                        'SELECT key FROM cache WHERE expires_at > ?',
                        (now,),
                    )
                else:
                    glob_pattern = self._fnmatch_to_glob(pattern)
                    cursor = conn.execute(
                        'SELECT key FROM cache WHERE key GLOB ? AND expires_at > ?',
                        (glob_pattern, now),
                    )

                all_keys = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()

        yield from all_keys

    def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if pattern is None:
                    cursor = conn.execute(
                        'SELECT COUNT(*) FROM cache WHERE expires_at > ?',
                        (now,),
                    )
                else:
                    glob_pattern = self._fnmatch_to_glob(pattern)
                    cursor = conn.execute(
                        'SELECT COUNT(*) FROM cache WHERE key GLOB ? AND expires_at > ?',
                        (glob_pattern, now),
                    )

                return cursor.fetchone()[0]
            except Exception:
                return 0
            finally:
                conn.close()

    def get_mutex(self, key: str) -> CacheMutex:
        """Get a mutex for dogpile prevention on the given key.
        """
        return ThreadingMutex(f'sqlite:{self._filepath}:{key}')

    def cleanup_expired(self) -> int:
        """Remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE expires_at <= ?',
                    (now,),
                )
                count = cursor.fetchone()[0]
                conn.execute('DELETE FROM cache WHERE expires_at <= ?', (now,))
                conn.commit()
                return count
            finally:
                conn.close()

    # ===== Stats interface (sync) =====

    def incr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Increment a stat counter for a function.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if stat == 'hits':
                    conn.execute(
                        """INSERT INTO cache_stats (fn_name, hits, misses)
                           VALUES (?, 1, 0)
                           ON CONFLICT(fn_name) DO UPDATE SET hits = hits + 1""",
                        (fn_name,),
                    )
                else:
                    conn.execute(
                        """INSERT INTO cache_stats (fn_name, hits, misses)
                           VALUES (?, 0, 1)
                           ON CONFLICT(fn_name) DO UPDATE SET misses = misses + 1""",
                        (fn_name,),
                    )
                conn.commit()
            finally:
                conn.close()

    def get_stats(self, fn_name: str) -> tuple[int, int]:
        """Get (hits, misses) for a function.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                cursor = conn.execute(
                    'SELECT hits, misses FROM cache_stats WHERE fn_name = ?',
                    (fn_name,),
                )
                row = cursor.fetchone()
                return (row[0], row[1]) if row else (0, 0)
            finally:
                conn.close()

    def clear_stats(self, fn_name: str | None = None) -> None:
        """Clear stats for a function, or all stats if fn_name is None.
        """
        with self._sync_lock:
            conn = self._get_sync_connection()
            try:
                if fn_name:
                    conn.execute('DELETE FROM cache_stats WHERE fn_name = ?', (fn_name,))
                else:
                    conn.execute('DELETE FROM cache_stats')
                conn.commit()
            finally:
                conn.close()

    # ===== Async interface =====

    async def aget(self, key: str) -> Any:
        """Async get value by key. Returns NO_VALUE if not found or expired.
        """
        try:
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT value, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE

            value_blob, expires_at = row
            if time.time() > expires_at:
                self._schedule_async_delete(key)
                return NO_VALUE

            return pickle.loads(value_blob)
        except Exception:
            return NO_VALUE

    async def aget_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Async get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        try:
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT value, created_at, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE, None

            value_blob, created_at, expires_at = row
            if time.time() > expires_at:
                self._schedule_async_delete(key)
                return NO_VALUE, None

            return pickle.loads(value_blob), created_at
        except Exception:
            return NO_VALUE, None

    async def aset(self, key: str, value: Any, ttl: int) -> None:
        """Async set value with TTL in seconds.
        """
        now = time.time()
        value_blob = pickle.dumps(value)

        async with self._async_write_lock:
            conn = await self._ensure_async_initialized()
            await conn.execute(
                """INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
                   VALUES (?, ?, ?, ?)""",
                (key, value_blob, now, now + ttl),
            )
            await conn.commit()

    async def adelete(self, key: str) -> None:
        """Async delete value by key.
        """
        async with self._async_write_lock:
            try:
                conn = await self._ensure_async_initialized()
                await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                await conn.commit()
            except Exception:
                pass

    async def aclear(self, pattern: str | None = None) -> int:
        """Async clear entries matching pattern. Returns count of cleared entries.
        """
        async with self._async_write_lock:
            try:
                conn = await self._ensure_async_initialized()
                if pattern is None:
                    cursor = await conn.execute('SELECT COUNT(*) FROM cache')
                    row = await cursor.fetchone()
                    count = row[0]
                    await conn.execute('DELETE FROM cache')
                    await conn.commit()
                    return count

                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ?',
                    (glob_pattern,),
                )
                row = await cursor.fetchone()
                count = row[0]
                await conn.execute('DELETE FROM cache WHERE key GLOB ?', (glob_pattern,))
                await conn.commit()
                return count
            except Exception:
                return 0

    async def akeys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Async iterate over keys matching pattern.
        """
        now = time.time()
        conn = await self._ensure_async_initialized()

        if pattern is None:
            cursor = await conn.execute(
                'SELECT key FROM cache WHERE expires_at > ?',
                (now,),
            )
        else:
            glob_pattern = self._fnmatch_to_glob(pattern)
            cursor = await conn.execute(
                'SELECT key FROM cache WHERE key GLOB ? AND expires_at > ?',
                (glob_pattern, now),
            )

        all_keys = [row[0] for row in await cursor.fetchall()]

        for key in all_keys:
            yield key

    async def acount(self, pattern: str | None = None) -> int:
        """Async count keys matching pattern.
        """
        now = time.time()

        try:
            conn = await self._ensure_async_initialized()
            if pattern is None:
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE expires_at > ?',
                    (now,),
                )
            else:
                glob_pattern = self._fnmatch_to_glob(pattern)
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE key GLOB ? AND expires_at > ?',
                    (glob_pattern, now),
                )

            row = await cursor.fetchone()
            return row[0]
        except Exception:
            return 0

    def get_async_mutex(self, key: str) -> AsyncCacheMutex:
        """Get an async mutex for dogpile prevention on the given key.
        """
        return AsyncioMutex(f'sqlite:{self._filepath}:{key}')

    async def acleanup_expired(self) -> int:
        """Async remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        async with self._async_write_lock:
            conn = await self._ensure_async_initialized()
            cursor = await conn.execute(
                'SELECT COUNT(*) FROM cache WHERE expires_at <= ?',
                (now,),
            )
            row = await cursor.fetchone()
            count = row[0]
            await conn.execute('DELETE FROM cache WHERE expires_at <= ?', (now,))
            await conn.commit()
            return count

    # ===== Stats interface (async) =====

    async def aincr_stat(self, fn_name: str, stat: Literal['hits', 'misses']) -> None:
        """Async increment a stat counter for a function.
        """
        async with self._async_write_lock:
            conn = await self._ensure_async_initialized()
            if stat == 'hits':
                await conn.execute(
                    """INSERT INTO cache_stats (fn_name, hits, misses)
                       VALUES (?, 1, 0)
                       ON CONFLICT(fn_name) DO UPDATE SET hits = hits + 1""",
                    (fn_name,),
                )
            else:
                await conn.execute(
                    """INSERT INTO cache_stats (fn_name, hits, misses)
                       VALUES (?, 0, 1)
                       ON CONFLICT(fn_name) DO UPDATE SET misses = misses + 1""",
                    (fn_name,),
                )
            await conn.commit()

    async def aget_stats(self, fn_name: str) -> tuple[int, int]:
        """Async get (hits, misses) for a function.
        """
        conn = await self._ensure_async_initialized()
        cursor = await conn.execute(
            'SELECT hits, misses FROM cache_stats WHERE fn_name = ?',
            (fn_name,),
        )
        row = await cursor.fetchone()
        return (row[0], row[1]) if row else (0, 0)

    async def aclear_stats(self, fn_name: str | None = None) -> None:
        """Async clear stats for a function, or all stats if fn_name is None.
        """
        async with self._async_write_lock:
            conn = await self._ensure_async_initialized()
            if fn_name:
                await conn.execute('DELETE FROM cache_stats WHERE fn_name = ?', (fn_name,))
            else:
                await conn.execute('DELETE FROM cache_stats')
            await conn.commit()

    # ===== Lifecycle =====

    def close(self) -> None:
        """Close sync resources. Use aclose() from async context for full cleanup.
        """
        self._sync_initialized = False
        if self._async_connection is not None:
            logger.warning(
                'SqliteBackend.close() called with active async connection. '
                'Use aclose() from async context for clean shutdown.'
            )
            self._async_connection = None
            self._async_initialized = False

    async def aclose(self) -> None:
        """Close all backend resources from async context.
        """
        self._sync_initialized = False
        if self._async_connection is not None:
            conn = self._async_connection
            self._async_connection = None
            self._async_initialized = False
            await conn.close()
