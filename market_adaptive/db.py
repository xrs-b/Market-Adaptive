from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import json


MARKET_STATUS_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_status (
    timestamp TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('trend', 'sideways', 'trend_impulse')),
    adx_value REAL NOT NULL,
    volatility REAL NOT NULL
);
"""

STRATEGY_RUNTIME_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS strategy_runtime_state (
    strategy_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last_status TEXT NOT NULL CHECK(last_status IN ('trend', 'sideways', 'trend_impulse')),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (strategy_name, symbol)
);
"""

SYSTEM_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS system_state (
    state_key TEXT PRIMARY KEY,
    state_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

ACCOUNT_DAILY_SNAPSHOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS account_daily_snapshot (
    snapshot_date TEXT PRIMARY KEY,
    settled_at TEXT NOT NULL,
    equity REAL NOT NULL,
    daily_start_equity REAL NOT NULL,
    daily_pnl REAL NOT NULL,
    initial_equity REAL NOT NULL,
    total_pnl REAL NOT NULL
);
"""

TRADE_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS trade_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    side TEXT,
    action TEXT,
    trigger_family TEXT,
    trigger_reason TEXT,
    pathway TEXT,
    price REAL,
    size REAL,
    pnl REAL,
    metadata_json TEXT
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


@dataclass
class StrategyRuntimeState:
    strategy_name: str
    symbol: str
    last_status: str
    updated_at: str


@dataclass
class SystemStateRecord:
    state_key: str
    state_value: str
    updated_at: str


@dataclass
class AccountDailySnapshotRecord:
    snapshot_date: str
    settled_at: str
    equity: float
    daily_start_equity: float
    daily_pnl: float
    initial_equity: float
    total_pnl: float


@dataclass
class TradeJournalRecord:
    timestamp: str
    strategy_name: str
    symbol: str
    event_type: str
    side: str | None = None
    action: str | None = None
    trigger_family: str | None = None
    trigger_reason: str | None = None
    pathway: str | None = None
    price: float | None = None
    size: float | None = None
    pnl: float | None = None
    metadata: dict | None = None


@dataclass
class TriggerFamilyPerformance:
    trigger_family: str
    side: str | None
    sample_count: int
    close_count: int
    win_count: int
    loss_count: int
    total_pnl: float
    avg_pnl: float
    win_rate: float


@dataclass
class TradeJournalRow:
    timestamp: str
    strategy_name: str
    symbol: str
    event_type: str
    side: str | None
    action: str | None
    trigger_family: str | None
    trigger_reason: str | None
    pathway: str | None
    price: float | None
    size: float | None
    pnl: float | None
    metadata: dict | None = None


class DatabaseInitializer:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            self._migrate_legacy_status_tables(conn)
            conn.execute(MARKET_STATUS_SCHEMA)
            conn.execute(STRATEGY_RUNTIME_STATE_SCHEMA)
            conn.execute(SYSTEM_STATE_SCHEMA)
            conn.execute(ACCOUNT_DAILY_SNAPSHOT_SCHEMA)
            conn.execute(TRADE_JOURNAL_SCHEMA)
            for statement in MARKET_STATUS_INDEXES:
                conn.execute(statement)
            conn.commit()

    def _table_sql(self, conn: sqlite3.Connection, table_name: str) -> str:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return str(row[0]) if row and row[0] else ""

    def _migrate_legacy_status_tables(self, conn: sqlite3.Connection) -> None:
        self._migrate_market_status_table(conn)
        self._migrate_strategy_runtime_state_table(conn)

    def _migrate_market_status_table(self, conn: sqlite3.Connection) -> None:
        sql = self._table_sql(conn, "market_status")
        if not sql or "trend_impulse" in sql:
            return
        conn.execute("ALTER TABLE market_status RENAME TO market_status_legacy")
        conn.execute(MARKET_STATUS_SCHEMA)
        for statement in MARKET_STATUS_INDEXES:
            conn.execute(statement)
        conn.execute(
            """
            INSERT INTO market_status (timestamp, symbol, status, adx_value, volatility)
            SELECT timestamp, symbol, status, adx_value, volatility
            FROM market_status_legacy
            """
        )
        conn.execute("DROP TABLE market_status_legacy")

    def _migrate_strategy_runtime_state_table(self, conn: sqlite3.Connection) -> None:
        sql = self._table_sql(conn, "strategy_runtime_state")
        if not sql or "trend_impulse" in sql:
            return
        conn.execute("ALTER TABLE strategy_runtime_state RENAME TO strategy_runtime_state_legacy")
        conn.execute(STRATEGY_RUNTIME_STATE_SCHEMA)
        conn.execute(
            """
            INSERT INTO strategy_runtime_state (strategy_name, symbol, last_status, updated_at)
            SELECT strategy_name, symbol, last_status, updated_at
            FROM strategy_runtime_state_legacy
            """
        )
        conn.execute("DROP TABLE strategy_runtime_state_legacy")

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

    def get_strategy_runtime_state(self, strategy_name: str, symbol: str) -> StrategyRuntimeState | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT strategy_name, symbol, last_status, updated_at
                FROM strategy_runtime_state
                WHERE strategy_name = ? AND symbol = ?
                LIMIT 1
                """,
                (strategy_name, symbol),
            ).fetchone()

        if row is None:
            return None

        return StrategyRuntimeState(
            strategy_name=str(row["strategy_name"]),
            symbol=str(row["symbol"]),
            last_status=str(row["last_status"]),
            updated_at=str(row["updated_at"]),
        )

    def upsert_strategy_runtime_state(self, state: StrategyRuntimeState) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO strategy_runtime_state (strategy_name, symbol, last_status, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(strategy_name, symbol)
                DO UPDATE SET
                    last_status=excluded.last_status,
                    updated_at=excluded.updated_at
                """,
                (state.strategy_name, state.symbol, state.last_status, state.updated_at),
            )
            conn.commit()

    def upsert_system_state(self, state: SystemStateRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO system_state (state_key, state_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(state_key)
                DO UPDATE SET
                    state_value=excluded.state_value,
                    updated_at=excluded.updated_at
                """,
                (state.state_key, state.state_value, state.updated_at),
            )
            conn.commit()

    def get_system_state(self, state_key: str) -> SystemStateRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT state_key, state_value, updated_at
                FROM system_state
                WHERE state_key = ?
                LIMIT 1
                """,
                (state_key,),
            ).fetchone()

        if row is None:
            return None

        return SystemStateRecord(
            state_key=str(row["state_key"]),
            state_value=str(row["state_value"]),
            updated_at=str(row["updated_at"]),
        )

    def upsert_account_daily_snapshot(self, record: AccountDailySnapshotRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO account_daily_snapshot (
                    snapshot_date,
                    settled_at,
                    equity,
                    daily_start_equity,
                    daily_pnl,
                    initial_equity,
                    total_pnl
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_date)
                DO UPDATE SET
                    settled_at=excluded.settled_at,
                    equity=excluded.equity,
                    daily_start_equity=excluded.daily_start_equity,
                    daily_pnl=excluded.daily_pnl,
                    initial_equity=excluded.initial_equity,
                    total_pnl=excluded.total_pnl
                """,
                (
                    record.snapshot_date,
                    record.settled_at,
                    record.equity,
                    record.daily_start_equity,
                    record.daily_pnl,
                    record.initial_equity,
                    record.total_pnl,
                ),
            )
            conn.commit()

    def fetch_account_daily_snapshots(self, month_prefix: str) -> list[AccountDailySnapshotRecord]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT snapshot_date, settled_at, equity, daily_start_equity, daily_pnl, initial_equity, total_pnl
                FROM account_daily_snapshot
                WHERE snapshot_date LIKE ?
                ORDER BY snapshot_date ASC
                """,
                (f"{month_prefix}%",),
            ).fetchall()

        return [
            AccountDailySnapshotRecord(
                snapshot_date=str(row["snapshot_date"]),
                settled_at=str(row["settled_at"]),
                equity=float(row["equity"]),
                daily_start_equity=float(row["daily_start_equity"]),
                daily_pnl=float(row["daily_pnl"]),
                initial_equity=float(row["initial_equity"]),
                total_pnl=float(row["total_pnl"]),
            )
            for row in rows
        ]

    def insert_trade_journal(self, record: TradeJournalRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_journal (
                    timestamp,
                    strategy_name,
                    symbol,
                    event_type,
                    side,
                    action,
                    trigger_family,
                    trigger_reason,
                    pathway,
                    price,
                    size,
                    pnl,
                    metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.timestamp,
                    record.strategy_name,
                    record.symbol,
                    record.event_type,
                    record.side,
                    record.action,
                    record.trigger_family,
                    record.trigger_reason,
                    record.pathway,
                    record.price,
                    record.size,
                    record.pnl,
                    json.dumps(record.metadata, ensure_ascii=False, sort_keys=True) if record.metadata is not None else None,
                ),
            )
            conn.commit()

    def fetch_trigger_family_performance(
        self,
        strategy_name: str,
        symbol: str,
        side: str | None = None,
        min_samples: int = 3,
    ) -> list[TriggerFamilyPerformance]:
        with self.connect() as conn:
            base_query = """
                SELECT
                    trigger_family,
                    side,
                    COUNT(*) as sample_count,
                    SUM(CASE WHEN event_type = 'trade_close' THEN 1 ELSE 0 END) as close_count,
                    SUM(CASE WHEN pnl > 0 AND event_type = 'trade_close' THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN pnl < 0 AND event_type = 'trade_close' THEN 1 ELSE 0 END) as loss_count,
                    SUM(CASE WHEN event_type = 'trade_close' THEN COALESCE(pnl, 0.0) ELSE 0.0 END) as total_pnl
                FROM trade_journal
                WHERE strategy_name = ?
                  AND symbol = ?
                  AND trigger_family IS NOT NULL
                  AND trigger_family != ''
                  AND trigger_family != 'waiting'
                  AND event_type IN ('trade_open', 'trade_close', 'blocked_signal')
            """
            params: list = [strategy_name, symbol]
            if side is not None:
                base_query += " AND side = ?"
                params.append(side)
            base_query += " GROUP BY trigger_family, side HAVING close_count >= ? ORDER BY total_pnl DESC"
            params.append(min_samples)
            rows = conn.execute(base_query, params).fetchall()
        results: list[TriggerFamilyPerformance] = []
        for row in rows:
            close_count = int(row["close_count"] or 0)
            win_count = int(row["win_count"] or 0)
            loss_count = int(row["loss_count"] or 0)
            total_pnl = float(row["total_pnl"] or 0.0)
            sample_count = int(row["sample_count"] or 0)
            results.append(
                TriggerFamilyPerformance(
                    trigger_family=str(row["trigger_family"] or "unknown"),
                    side=str(row["side"]) if row["side"] else None,
                    sample_count=sample_count,
                    close_count=close_count,
                    win_count=win_count,
                    loss_count=loss_count,
                    total_pnl=total_pnl,
                    avg_pnl=total_pnl / close_count if close_count > 0 else 0.0,
                    win_rate=win_count / close_count if close_count > 0 else 0.0,
                )
            )
        return results

    def fetch_trade_journal_rows(
        self,
        strategy_name: str,
        symbol: str,
        *,
        limit: int = 500,
        event_types: tuple[str, ...] | None = None,
    ) -> list[TradeJournalRow]:
        with self.connect() as conn:
            query = """
                SELECT timestamp, strategy_name, symbol, event_type, side, action, trigger_family,
                       trigger_reason, pathway, price, size, pnl, metadata_json
                FROM trade_journal
                WHERE strategy_name = ? AND symbol = ?
            """
            params: list[object] = [strategy_name, symbol]
            if event_types:
                placeholders = ",".join("?" for _ in event_types)
                query += f" AND event_type IN ({placeholders})"
                params.extend(event_types)
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(max(1, int(limit)))
            rows = conn.execute(query, params).fetchall()
        result: list[TradeJournalRow] = []
        for row in rows:
            metadata = None
            raw_metadata = row["metadata_json"]
            if raw_metadata not in (None, ""):
                try:
                    metadata = json.loads(str(raw_metadata))
                except Exception:
                    metadata = None
            result.append(
                TradeJournalRow(
                    timestamp=str(row["timestamp"]),
                    strategy_name=str(row["strategy_name"]),
                    symbol=str(row["symbol"]),
                    event_type=str(row["event_type"]),
                    side=str(row["side"]) if row["side"] is not None else None,
                    action=str(row["action"]) if row["action"] is not None else None,
                    trigger_family=str(row["trigger_family"]) if row["trigger_family"] is not None else None,
                    trigger_reason=str(row["trigger_reason"]) if row["trigger_reason"] is not None else None,
                    pathway=str(row["pathway"]) if row["pathway"] is not None else None,
                    price=float(row["price"]) if row["price"] is not None else None,
                    size=float(row["size"]) if row["size"] is not None else None,
                    pnl=float(row["pnl"]) if row["pnl"] is not None else None,
                    metadata=metadata,
                )
            )
        return result
