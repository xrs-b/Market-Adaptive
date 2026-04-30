#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"PyYAML is required: {exc}")

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review recent trade_journal activity from the configured live sqlite database."
    )
    parser.add_argument("--hours", type=float, default=24.0, help="Lookback window in hours (default: 24)")
    parser.add_argument("--symbol", help="Optional symbol filter, e.g. BTC/USDT")
    return parser


def load_db_path() -> Path:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    db_path = (((config or {}).get("database") or {}).get("path"))
    if not db_path:
        raise SystemExit(f"database.path not found in {CONFIG_PATH}")
    path = Path(db_path)
    if not path.is_absolute():
        path = ROOT / path
    return path


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


def short_bucket(value: Any, fallback: str = "UNKNOWN") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def top_lines(counter: Counter, limit: int = 5, indent: str = "  ") -> list[str]:
    if not counter:
        return [f"{indent}(none)"]
    lines = []
    for key, count in counter.most_common(limit):
        lines.append(f"{indent}- {key}: {count}")
    return lines


def fetch_rows(conn: sqlite3.Connection, since_iso: str, symbol: str | None) -> list[sqlite3.Row]:
    sql = """
        SELECT *
        FROM trade_journal
        WHERE timestamp >= ?
    """
    params: list[Any] = [since_iso]
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol)
    sql += " ORDER BY timestamp ASC, id ASC"
    return conn.execute(sql, params).fetchall()


def main() -> int:
    args = build_parser().parse_args()
    db_path = load_db_path()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=args.hours)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = fetch_rows(conn, since.isoformat(), args.symbol)

    event_counts: Counter[str] = Counter()
    blocked_actions: Counter[str] = Counter()
    blocked_reasons: Counter[str] = Counter()
    blocked_families: Counter[str] = Counter()
    blocked_pathways: Counter[str] = Counter()
    open_by_pathway: Counter[str] = Counter()
    open_by_family: Counter[str] = Counter()
    close_by_pathway: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0, "pnl": 0.0, "wins": 0})

    first_ts = None
    last_ts = None

    for row in rows:
        ts = parse_ts(row["timestamp"])
        if ts is not None:
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)

        event_type = short_bucket(row["event_type"])
        event_counts[event_type] += 1
        meta = safe_json_loads(row["metadata_json"])

        if event_type == "blocked_signal":
            blocked_actions[short_bucket(row["action"])] += 1
            blocked_families[short_bucket(row["trigger_family"])] += 1
            blocked_pathways[short_bucket(row["pathway"])] += 1
            blocker_reason = (
                meta.get("blocker_reason")
                or meta.get("reason")
                or meta.get("block_reason")
                or row["trigger_reason"]
            )
            blocked_reasons[short_bucket(blocker_reason)] += 1

        elif event_type == "trade_open":
            open_by_pathway[short_bucket(row["pathway"])] += 1
            open_by_family[short_bucket(row["trigger_family"])] += 1

        elif event_type == "trade_close":
            pathway = short_bucket(row["pathway"] or meta.get("pathway"), fallback="UNKNOWN")
            pnl = row["pnl"]
            pnl_value = float(pnl) if pnl is not None else 0.0
            bucket = close_by_pathway[pathway]
            bucket["count"] += 1
            bucket["pnl"] += pnl_value
            if pnl_value > 0:
                bucket["wins"] += 1

    total_closes = sum(bucket["count"] for bucket in close_by_pathway.values())
    total_close_pnl = sum(bucket["pnl"] for bucket in close_by_pathway.values())
    total_close_wins = sum(bucket["wins"] for bucket in close_by_pathway.values())
    total_close_avg = (total_close_pnl / total_closes) if total_closes else 0.0
    total_close_win_rate = (100.0 * total_close_wins / total_closes) if total_closes else 0.0

    symbol_label = args.symbol or "ALL"
    range_label = f"{since.isoformat()} -> {now.isoformat()}"
    actual_range = f"{first_ts.isoformat() if first_ts else '-'} -> {last_ts.isoformat() if last_ts else '-'}"

    lines: list[str] = []
    lines.append("Trade Journal Review")
    lines.append(f"DB: {db_path}")
    lines.append(f"Filter: last {args.hours:g}h | symbol={symbol_label}")
    lines.append(f"Query range (UTC): {range_label}")
    lines.append(f"Matched rows: {len(rows)} | Actual row range: {actual_range}")
    lines.append("")

    lines.append("1) Event counts")
    lines.extend(top_lines(event_counts, limit=20))
    lines.append("")

    lines.append("2) blocked_signal top buckets")
    blocked_total = event_counts.get("blocked_signal", 0)
    lines.append(f"  total: {blocked_total}")
    lines.append("  action:")
    lines.extend(top_lines(blocked_actions, limit=5, indent="    "))
    lines.append("  blocker_reason:")
    lines.extend(top_lines(blocked_reasons, limit=5, indent="    "))
    lines.append("  trigger_family:")
    lines.extend(top_lines(blocked_families, limit=5, indent="    "))
    lines.append("  pathway:")
    lines.extend(top_lines(blocked_pathways, limit=5, indent="    "))
    lines.append("")

    lines.append("3) trade_open breakdown")
    lines.append(f"  total: {event_counts.get('trade_open', 0)}")
    lines.append("  pathway:")
    lines.extend(top_lines(open_by_pathway, limit=10, indent="    "))
    lines.append("  trigger_family:")
    lines.extend(top_lines(open_by_family, limit=10, indent="    "))
    lines.append("")

    lines.append("4) trade_close summary")
    lines.append(
        f"  total: {total_closes} | total_pnl: {total_close_pnl:.2f} | win_rate: {total_close_win_rate:.1f}% | avg_pnl: {total_close_avg:.2f}"
    )
    if close_by_pathway:
        lines.append("  by pathway:")
        for pathway, bucket in sorted(close_by_pathway.items(), key=lambda item: (-item[1]["count"], item[0])):
            count = int(bucket["count"])
            pnl = bucket["pnl"]
            wins = int(bucket["wins"])
            win_rate = (100.0 * wins / count) if count else 0.0
            avg_pnl = (pnl / count) if count else 0.0
            lines.append(
                f"    - {pathway}: count={count}, total_pnl={pnl:.2f}, win_rate={win_rate:.1f}%, avg_pnl={avg_pnl:.2f}"
            )
    else:
        lines.append("  by pathway:")
        lines.append("    (none)")

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
