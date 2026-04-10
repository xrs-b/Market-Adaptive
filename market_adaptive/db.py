from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
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


@dataclass
class MarketStatusRecord:
    timestamp: str
    symbol: str
    status: str
    adx_value: float
    volatility: float


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

    def insert_market_status(self, record: MarketStatusRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_status (timestamp, symbol, status, adx_value, volatility)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    record.timestamp,
                    record.symbol,
                    record.status,
                    record.adx_value,
                    record.volatility,
                ),
            )
            conn.commit()

    def fetch_latest_market_status(self, symbol: str) -> MarketStatusRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT timestamp, symbol, status, adx_value, volatility
                FROM market_status
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()

        if row is None:
            return None

        return MarketStatusRecord(
            timestamp=str(row["timestamp"]),
            symbol=str(row["symbol"]),
            status=str(row["status"]),
            adx_value=float(row["adx_value"]),
            volatility=float(row["volatility"]),
        )
