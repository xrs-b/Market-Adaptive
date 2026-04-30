#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"


@dataclass
class OpenLot:
    row_id: int
    timestamp: str
    side: str
    size: float
    family: str
    pathway: str


@dataclass
class FamilyStats:
    blocked: int = 0
    passed: int = 0
    opens: int = 0
    closes: int = 0
    wins: int = 0
    realized_pnl: float = 0.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CTA family-level journal report from configured sqlite database")
    parser.add_argument("--days", type=float, default=7.0, help="Lookback window in days (default: 7)")
    parser.add_argument("--hours", type=float, help="Override lookback window in hours")
    parser.add_argument("--symbol", help="Optional symbol filter, e.g. BTC/USDT")
    parser.add_argument("--limit-blockers", type=int, default=3, help="Top blocker reasons per family")
    return parser


def load_db_path() -> Path:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    db_path = ((config.get("database") or {}).get("path"))
    if not db_path:
        raise SystemExit(f"database.path not found in {CONFIG_PATH}")
    path = Path(db_path)
    return path if path.is_absolute() else ROOT / path


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def safe_json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def bucket(value: Any, fallback: str = "UNKNOWN") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def fetch_rows(conn: sqlite3.Connection, since_iso: str, symbol: str | None) -> list[sqlite3.Row]:
    sql = """
        SELECT *
        FROM trade_journal
        WHERE strategy_name = 'cta' AND timestamp >= ?
    """
    params: list[Any] = [since_iso]
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol)
    sql += " ORDER BY timestamp ASC, id ASC"
    return conn.execute(sql, params).fetchall()


def find_matching_open(open_lots: list[OpenLot], close_side: str, close_size: float) -> tuple[OpenLot | None, int | None]:
    candidates: list[tuple[float, float, int, OpenLot]] = []
    for idx, lot in enumerate(open_lots):
        if lot.side != close_side:
            continue
        size_gap = abs(lot.size - close_size)
        candidates.append((0.0 if size_gap < 1e-9 else 1.0, size_gap, -idx, lot))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    chosen = candidates[0][3]
    chosen_idx = next(i for i, lot in enumerate(open_lots) if lot.row_id == chosen.row_id)
    return chosen, chosen_idx


def fmt_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "-"
    return f"{(100.0 * numerator / denominator):.1f}%"


def is_passed_blocker(value: str) -> bool:
    return value.strip().upper() == "PASSED"


def main() -> int:
    args = build_parser().parse_args()
    hours = args.hours if args.hours is not None else args.days * 24.0
    db_path = load_db_path()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = fetch_rows(conn, since.isoformat(), args.symbol)

    families: dict[str, FamilyStats] = defaultdict(FamilyStats)
    open_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    close_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    blocked_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    blocker_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    passed_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    action_counts: dict[str, Counter[str]] = defaultdict(Counter)
    unmatched_closes: list[sqlite3.Row] = []
    open_lots: list[OpenLot] = []

    event_counts: Counter[str] = Counter()
    first_ts: datetime | None = None
    last_ts: datetime | None = None

    for row in rows:
        ts = parse_ts(row["timestamp"])
        if ts is not None:
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)

        event_type = bucket(row["event_type"])
        event_counts[event_type] += 1
        family = bucket(row["trigger_family"])
        pathway = bucket(row["pathway"])
        meta = safe_json_loads(row["metadata_json"])

        if event_type == "blocked_signal":
            action_counts[family][bucket(row["action"])] += 1
            blocker = bucket(
                meta.get("blocker_reason")
                or meta.get("reason")
                or meta.get("block_reason")
                or row["trigger_reason"]
            )
            if is_passed_blocker(blocker):
                families[family].passed += 1
                passed_pathways[family][pathway] += 1
            else:
                families[family].blocked += 1
                blocked_pathways[family][pathway] += 1
                blocker_reasons[family][blocker] += 1
        elif event_type == "trade_open":
            families[family].opens += 1
            open_pathways[family][pathway] += 1
            open_lots.append(
                OpenLot(
                    row_id=int(row["id"]),
                    timestamp=row["timestamp"],
                    side=bucket(row["side"]),
                    size=float(row["size"] or 0.0),
                    family=family,
                    pathway=pathway,
                )
            )
        elif event_type == "trade_close":
            close_side = bucket(row["side"])
            close_size = float(row["size"] or 0.0)
            matched, idx = find_matching_open(open_lots, close_side, close_size)
            if matched is None or idx is None:
                unmatched_closes.append(row)
                continue
            open_lots.pop(idx)
            families[matched.family].closes += 1
            pnl = float(row["pnl"] or 0.0)
            families[matched.family].realized_pnl += pnl
            if pnl > 0:
                families[matched.family].wins += 1
            close_pathways[matched.family][matched.pathway] += 1

    sort_keys = sorted(
        families.keys(),
        key=lambda fam: (
            -(families[fam].opens + families[fam].blocked + families[fam].closes),
            fam,
        ),
    )

    print("CTA Family Journal Report")
    print(f"DB: {db_path}")
    print(f"Window: last {hours:g}h | symbol={args.symbol or 'ALL'}")
    print(f"Query range (UTC): {since.isoformat()} -> {now.isoformat()}")
    print(f"Matched rows: {len(rows)} | Actual row range: {(first_ts.isoformat() if first_ts else '-')} -> {(last_ts.isoformat() if last_ts else '-')}")
    print(f"Event counts: {dict(event_counts)}")
    print("")
    print("family | blocked | passed | opens | closes | open_rate | win_rate | realized_pnl | open pathways | blocked pathways | passed pathways | top blockers")

    for family in sort_keys:
        stats = families[family]
        attempts = stats.blocked + stats.passed + stats.opens
        open_rate = fmt_pct(stats.opens, attempts)
        win_rate = fmt_pct(stats.wins, stats.closes)
        open_pw = ", ".join(f"{k}:{v}" for k, v in open_pathways[family].most_common()) or "-"
        blocked_pw = ", ".join(f"{k}:{v}" for k, v in blocked_pathways[family].most_common()) or "-"
        passed_pw = ", ".join(f"{k}:{v}" for k, v in passed_pathways[family].most_common()) or "-"
        blockers = ", ".join(f"{k}:{v}" for k, v in blocker_reasons[family].most_common(args.limit_blockers)) or "-"
        print(
            f"{family} | {stats.blocked} | {stats.passed} | {stats.opens} | {stats.closes} | {open_rate} | {win_rate} | {stats.realized_pnl:.2f} | {open_pw} | {blocked_pw} | {passed_pw} | {blockers}"
        )

    print("")
    print("Notes:")
    print("- closes are attributed back to trigger_family using a same-side nearest-size open-match heuristic.")
    print(f"- unmatched closes in window: {len(unmatched_closes)}")
    if open_lots:
        remaining = Counter(lot.family for lot in open_lots)
        print(f"- still-open lots seen in window: {dict(remaining)}")
    else:
        print("- still-open lots seen in window: {}")

    if unmatched_closes:
        print("- unmatched close ids: " + ", ".join(str(row["id"]) for row in unmatched_closes[:10]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
