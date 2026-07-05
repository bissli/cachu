"""Tests for SQLite backend robustness: poison eviction, expiry boundary,
and safe background deletion.
"""
import asyncio
import pickle
import sqlite3

from cachu.api import NO_VALUE
from cachu.backends.sqlite import SqliteBackend


def test_get_evicts_corrupt_row(tmp_path):
    """A corrupt blob is evicted on read so the next call recomputes.
    """
    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    backend.set('k', 'v', 300)

    conn = sqlite3.connect(backend._filepath)
    conn.execute("UPDATE cache SET value = ? WHERE key = 'k'", (b'\x00not-a-pickle',))
    conn.commit()
    conn.close()

    assert backend.get('k') is NO_VALUE

    conn = sqlite3.connect(backend._filepath)
    remaining = conn.execute("SELECT COUNT(*) FROM cache WHERE key = 'k'").fetchone()[0]
    conn.close()
    assert remaining == 0


def test_sync_path_is_wal_and_tolerates_concurrent_writers(tmp_path):
    """The sync backend opens the DB in WAL mode with a busy_timeout, so many
    concurrent sync writers neither raise 'database is locked' nor drop a value.
    """
    import threading

    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    backend.set('warm', 'v', 300)

    conn = sqlite3.connect(backend._filepath)
    mode = conn.execute('PRAGMA journal_mode').fetchone()[0]
    conn.close()
    assert mode.lower() == 'wal'

    errors = []

    def writer(worker):
        try:
            for j in range(20):
                backend.set(f'k{worker}-{j}', j, 300)
        except sqlite3.OperationalError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert backend.get('k7-19') == 19


async def test_aget_evicts_corrupt_row(tmp_path):
    """Async read also evicts a corrupt row.
    """
    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    await backend.aset('k', 'v', 300)

    conn = sqlite3.connect(backend._filepath)
    conn.execute("UPDATE cache SET value = ? WHERE key = 'k'", (b'\x00bad',))
    conn.commit()
    conn.close()

    assert await backend.aget('k') is NO_VALUE

    remaining = 1
    for _ in range(50):
        await asyncio.sleep(0.02)
        conn = sqlite3.connect(backend._filepath)
        remaining = conn.execute(
            "SELECT COUNT(*) FROM cache WHERE key = 'k'").fetchone()[0]
        conn.close()
        if remaining == 0:
            break
    assert remaining == 0


def test_get_and_count_agree_at_expiry_boundary(tmp_path, monkeypatch):
    """get() and count() must agree on whether an entry is live at the exact
    expiry instant.
    """
    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    backend._ensure_sync_initialized()

    boundary = 10_000.0
    conn = sqlite3.connect(backend._filepath)
    conn.execute(
        'INSERT INTO cache (key, value, created_at, expires_at) VALUES (?, ?, ?, ?)',
        ('k', pickle.dumps('v'), boundary - 100, boundary))
    conn.commit()
    conn.close()

    monkeypatch.setattr('cachu.backends.sqlite.time.time', lambda: boundary)

    assert backend.get('k') == 'v'
    assert backend.count() == 1


def test_schedule_async_delete_without_running_loop_is_safe(tmp_path):
    """Scheduling a background delete with no running event loop must not raise.
    """
    backend = SqliteBackend(str(tmp_path / 'cache.db'))
    backend._schedule_async_delete('k')
