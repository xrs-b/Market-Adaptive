from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


MARKET_STATUS_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_status (
    timestamp TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('trend', 'sideways')),
    adx_value REAL NOT NULL,
    volatility REAL NOT NULL
);
"""

MARKET_STATUS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_market_status_symbol ON market_status(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_market_status_status ON market_status(status);",
]


class DatabaseInitializer:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(MARKET_STATUS_SCHEMA)
            for statement in MARKET_STATUS_INDEXES:
                conn.execute(statement)
            conn.commit()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()
