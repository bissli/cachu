"""SQLite-based cache backend.
"""
import asyncio
import pickle
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any

from . import NO_VALUE, AsyncBackend, Backend

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
    """SQLite file-based cache backend.
    """

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema.
        """
        with self._lock:
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
                conn.commit()
            finally:
                conn.close()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection.
        """
        return sqlite3.connect(self._filepath)

    def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        with self._lock:
            conn = self._get_connection()
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
        with self._lock:
            conn = self._get_connection()
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

        with self._lock:
            conn = self._get_connection()
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
        with self._lock:
            conn = self._get_connection()
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
        with self._lock:
            conn = self._get_connection()
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

        with self._lock:
            conn = self._get_connection()
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

        with self._lock:
            conn = self._get_connection()
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

    def _fnmatch_to_glob(self, pattern: str) -> str:
        """Convert fnmatch pattern to SQLite GLOB pattern.

        fnmatch uses * and ? which are the same as SQLite GLOB.
        The main difference is character classes [...] which we don't use.
        """
        return pattern

    def cleanup_expired(self) -> int:
        """Remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        with self._lock:
            conn = self._get_connection()
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


class AsyncSqliteBackend(AsyncBackend):
    """Async SQLite file-based cache backend using aiosqlite.
    """

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath
        self._connection: aiosqlite.Connection | None = None
        self._init_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._initialized = False

    async def _ensure_initialized(self) -> 'aiosqlite.Connection':
        """Ensure database is initialized and return connection.
        """
        async with self._init_lock:
            if self._connection is None:
                aiosqlite = _get_aiosqlite_module()
                self._connection = await aiosqlite.connect(self._filepath)
                await self._connection.execute('PRAGMA journal_mode=WAL')
                await self._connection.execute('PRAGMA busy_timeout=5000')

            if not self._initialized:
                await self._connection.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                """)
                await self._connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expires
                    ON cache(expires_at)
                """)
                await self._connection.commit()
                self._initialized = True

        return self._connection

    def _schedule_delete(self, key: str) -> None:
        """Schedule a background deletion task (fire-and-forget).
        """
        async def _delete() -> None:
            try:
                async with self._write_lock:
                    conn = await self._ensure_initialized()
                    await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    await conn.commit()
            except Exception:
                pass

        asyncio.create_task(_delete())

    async def get(self, key: str) -> Any:
        """Get value by key. Returns NO_VALUE if not found or expired.
        """
        try:
            conn = await self._ensure_initialized()
            cursor = await conn.execute(
                'SELECT value, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE

            value_blob, expires_at = row
            if time.time() > expires_at:
                self._schedule_delete(key)
                return NO_VALUE

            return pickle.loads(value_blob)
        except Exception:
            return NO_VALUE

    async def get_with_metadata(self, key: str) -> tuple[Any, float | None]:
        """Get value and creation timestamp. Returns (NO_VALUE, None) if not found.
        """
        try:
            conn = await self._ensure_initialized()
            cursor = await conn.execute(
                'SELECT value, created_at, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = await cursor.fetchone()

            if row is None:
                return NO_VALUE, None

            value_blob, created_at, expires_at = row
            if time.time() > expires_at:
                self._schedule_delete(key)
                return NO_VALUE, None

            return pickle.loads(value_blob), created_at
        except Exception:
            return NO_VALUE, None

    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds.
        """
        now = time.time()
        value_blob = pickle.dumps(value)

        async with self._write_lock:
            conn = await self._ensure_initialized()
            await conn.execute(
                """INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
                   VALUES (?, ?, ?, ?)""",
                (key, value_blob, now, now + ttl),
            )
            await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete value by key.
        """
        async with self._write_lock:
            try:
                conn = await self._ensure_initialized()
                await conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                await conn.commit()
            except Exception:
                pass

    async def clear(self, pattern: str | None = None) -> int:
        """Clear entries matching pattern. Returns count of cleared entries.
        """
        async with self._write_lock:
            try:
                conn = await self._ensure_initialized()
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

    async def keys(self, pattern: str | None = None) -> AsyncIterator[str]:
        """Iterate over keys matching pattern.
        """
        now = time.time()
        conn = await self._ensure_initialized()

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

    async def count(self, pattern: str | None = None) -> int:
        """Count keys matching pattern.
        """
        now = time.time()

        try:
            conn = await self._ensure_initialized()
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

    def _fnmatch_to_glob(self, pattern: str) -> str:
        """Convert fnmatch pattern to SQLite GLOB pattern.
        """
        return pattern

    async def cleanup_expired(self) -> int:
        """Remove expired entries. Returns count of removed entries.
        """
        now = time.time()

        async with self._write_lock:
            conn = await self._ensure_initialized()
            cursor = await conn.execute(
                'SELECT COUNT(*) FROM cache WHERE expires_at <= ?',
                (now,),
            )
            row = await cursor.fetchone()
            count = row[0]
            await conn.execute('DELETE FROM cache WHERE expires_at <= ?', (now,))
            await conn.commit()
            return count

    async def close(self) -> None:
        """Close the database connection.
        """
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._initialized = False
