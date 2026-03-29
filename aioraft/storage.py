import abc
import sqlite3
from typing import List, Optional

from aioraft.protos import raft_pb2

__all__ = ("Storage", "MemoryStorage", "SQLiteStorage")


class Storage(abc.ABC):
    """Abstract persistent storage for Raft state."""

    @abc.abstractmethod
    async def save_term(self, term: int) -> None: ...

    @abc.abstractmethod
    async def load_term(self) -> int: ...

    @abc.abstractmethod
    async def save_vote(self, voted_for: Optional[str]) -> None: ...

    @abc.abstractmethod
    async def load_vote(self) -> Optional[str]: ...

    @abc.abstractmethod
    async def append_logs(self, entries: List[raft_pb2.Log]) -> None: ...

    @abc.abstractmethod
    async def truncate_logs_from(self, index: int) -> None: ...

    @abc.abstractmethod
    async def load_logs(self) -> List[raft_pb2.Log]: ...

    @abc.abstractmethod
    async def save_log_entry(self, entry: raft_pb2.Log) -> None: ...


class MemoryStorage(Storage):
    """In-memory storage for testing."""

    def __init__(self):
        self._term = 0
        self._voted_for: Optional[str] = None
        self._logs: List[raft_pb2.Log] = []

    async def save_term(self, term: int) -> None:
        self._term = term

    async def load_term(self) -> int:
        return self._term

    async def save_vote(self, voted_for: Optional[str]) -> None:
        self._voted_for = voted_for

    async def load_vote(self) -> Optional[str]:
        return self._voted_for

    async def append_logs(self, entries: List[raft_pb2.Log]) -> None:
        self._logs.extend(entries)

    async def save_log_entry(self, entry: raft_pb2.Log) -> None:
        self._logs.append(entry)

    async def truncate_logs_from(self, index: int) -> None:
        self._logs = [e for e in self._logs if e.index < index]

    async def load_logs(self) -> List[raft_pb2.Log]:
        return list(self._logs)


class SQLiteStorage(Storage):
    """SQLite-based persistent storage."""

    def __init__(self, db_path: str = "raft.db"):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    async def initialize(self) -> None:
        """Create tables if they don't exist."""
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS raft_state (key TEXT PRIMARY KEY, value TEXT)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS raft_log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, command TEXT NOT NULL)"
        )
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()

    async def save_term(self, term: int) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('current_term', ?)",
            (str(term),),
        )
        self._conn.commit()

    async def load_term(self) -> int:
        cursor = self._conn.execute(
            "SELECT value FROM raft_state WHERE key = 'current_term'"
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    async def save_vote(self, voted_for: Optional[str]) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('voted_for', ?)",
            (voted_for,),
        )
        self._conn.commit()

    async def load_vote(self) -> Optional[str]:
        cursor = self._conn.execute(
            "SELECT value FROM raft_state WHERE key = 'voted_for'"
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None

    async def append_logs(self, entries: List[raft_pb2.Log]) -> None:
        self._conn.executemany(
            "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
            [(e.index, e.term, e.command) for e in entries],
        )
        self._conn.commit()

    async def save_log_entry(self, entry: raft_pb2.Log) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
            (entry.index, entry.term, entry.command),
        )
        self._conn.commit()

    async def truncate_logs_from(self, index: int) -> None:
        self._conn.execute("DELETE FROM raft_log WHERE idx >= ?", (index,))
        self._conn.commit()

    async def load_logs(self) -> List[raft_pb2.Log]:
        cursor = self._conn.execute(
            "SELECT idx, term, command FROM raft_log ORDER BY idx"
        )
        return [
            raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            for row in cursor.fetchall()
        ]
