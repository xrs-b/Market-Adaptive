#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"
LOG_DIR = ROOT / "logs"
ARCHIVE_DIR = LOG_DIR / "archive"
GRID_CLOSE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2},\d+ .*?\[GRID_TRADE_CLOSE\] .*?entry_side=(?P<entry_side>\w+) exit_side=(?P<exit_side>\w+) entry=(?P<entry>[-+]?\d+(?:\.\d+)?) exit=(?P<exit>[-+]?\d+(?:\.\d+)?) size=(?P<size>[-+]?\d+(?:\.\d+)?) pnl=(?P<pnl>-?\d+(?:\.\d+)?)"
)
GRID_HEDGE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2},\d+ .*?Grid websocket hedge order \| fill_side=(?P<fill_side>\w+) fill_price=(?P<fill_price>[-+]?\d+(?:\.\d+)?) counter_side=(?P<counter_side>\w+) counter_price=(?P<counter_price>[-+]?\d+(?:\.\d+)?) amount=(?P<amount>[-+]?\d+(?:\.\d+)?)"
)


@dataclass
class OpenLot:
    row_id: int
    timestamp: str
    side: str
    size: float
    family: str
    pathway: str


@dataclass
class DayBucket:
    cta_pnl: float = 0.0
    cta_close_count: int = 0
    cta_open_count: int = 0
    cta_blocked_count: int = 0
    cta_by_family: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    cta_by_pathway: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    cta_close_rows: list[dict[str, Any]] = field(default_factory=list)
    grid_pnl: float = 0.0
    grid_close_count: int = 0
    grid_target_missing_pnl: float = 0.0
    grid_target_missing_count: int = 0
    grid_files: set[str] = field(default_factory=set)

    def to_json(self) -> dict[str, Any]:
        total = self.cta_pnl + self.grid_pnl
        drag_pct = 0.0
        if self.grid_pnl > 0:
            drag_pct = abs(min(0.0, self.cta_pnl)) / self.grid_pnl * 100.0
        return {
            "cta_pnl": round(self.cta_pnl, 6),
            "grid_pnl": round(self.grid_pnl, 6),
            "combined_pnl": round(total, 6),
            "cta_close_count": self.cta_close_count,
            "cta_open_count": self.cta_open_count,
            "cta_blocked_count": self.cta_blocked_count,
            "grid_close_count": self.grid_close_count,
            "grid_target_missing_pnl": round(self.grid_target_missing_pnl, 6),
            "grid_target_missing_count": self.grid_target_missing_count,
            "cta_drag_vs_grid_pct": round(drag_pct, 2),
            "cta_by_family": dict(sorted(self.cta_by_family.items(), key=lambda kv: kv[1])),
            "cta_by_pathway": dict(sorted(self.cta_by_pathway.items(), key=lambda kv: kv[1])),
            "grid_files": sorted(self.grid_files),
            "cta_close_rows": self.cta_close_rows,
        }


def load_db_path() -> Path:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    db_path = ((config.get("database") or {}).get("path"))
    if not db_path:
        raise SystemExit(f"database.path not found in {CONFIG_PATH}")
    path = Path(db_path)
    return path if path.is_absolute() else ROOT / path


def safe_json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


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


def collect_cta(days: dict[str, DayBucket], db_path: Path, since_iso: str, symbol: str | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
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

    open_lots: list[OpenLot] = []

    for row in conn.execute(sql, params):
        ts = str(row["timestamp"])
        day = ts[:10]
        bucket = days[day]
        event_type = str(row["event_type"] or "")
        if event_type == "trade_close":
            pnl = float(row["pnl"] or 0.0)
            bucket.cta_pnl += pnl
            bucket.cta_close_count += 1
            meta = safe_json_loads(row["metadata_json"])
            family = str(meta.get("origin_trigger_family") or row["trigger_family"] or "").strip()
            pathway = str(meta.get("origin_pathway") or row["pathway"] or "").strip()
            if not family or not pathway:
                matched, idx = find_matching_open(open_lots, str(row["side"] or ""), float(row["size"] or 0.0))
                if matched is not None and idx is not None:
                    open_lots.pop(idx)
                    family = family or matched.family
                    pathway = pathway or matched.pathway
            family = family or "UNKNOWN"
            pathway = pathway or "UNKNOWN"
            bucket.cta_by_family[family] += pnl
            bucket.cta_by_pathway[pathway] += pnl
            bucket.cta_close_rows.append(
                {
                    "timestamp": ts,
                    "side": row["side"],
                    "size": float(row["size"] or 0.0),
                    "pnl": round(pnl, 6),
                    "family": family,
                    "pathway": pathway,
                    "entry_price": meta.get("entry_price"),
                    "exit_price": meta.get("exit_price"),
                    "roi": meta.get("roi"),
                }
            )
        elif event_type == "trade_open":
            bucket.cta_open_count += 1
            meta = safe_json_loads(row["metadata_json"])
            open_lots.append(
                OpenLot(
                    row_id=int(row["id"]),
                    timestamp=ts,
                    side=str(row["side"] or ""),
                    size=float(row["size"] or 0.0),
                    family=str(row["trigger_family"] or meta.get("origin_trigger_family") or "UNKNOWN"),
                    pathway=str(row["pathway"] or meta.get("origin_pathway") or "UNKNOWN"),
                )
            )
        elif event_type == "blocked_signal":
            bucket.cta_blocked_count += 1


def iter_log_files() -> list[Path]:
    files = sorted(LOG_DIR.glob("main_controller*.log")) + sorted(ARCHIVE_DIR.glob("main_controller-*.log"))
    return [path for path in files if path.is_file()]


def _round_key(value: float) -> float:
    return round(float(value), 8)


def _grid_tuple_key(day: str, entry_side: str, exit_side: str, entry_price: float, exit_price: float, amount: float) -> tuple[Any, ...]:
    return (
        day,
        str(entry_side).lower(),
        str(exit_side).lower(),
        _round_key(entry_price),
        _round_key(exit_price),
        _round_key(amount),
    )


def collect_grid(days: dict[str, DayBucket], since_date: str) -> None:
    confirmed_close_keys: set[tuple[Any, ...]] = set()
    raw_hedges: list[tuple[str, Path, tuple[Any, ...], float]] = []

    for path in iter_log_files():
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    close_match = GRID_CLOSE_RE.search(line)
                    if close_match:
                        day = close_match.group("ts")
                        if day < since_date:
                            continue
                        entry_side = close_match.group("entry_side")
                        exit_side = close_match.group("exit_side")
                        entry_price = float(close_match.group("entry"))
                        exit_price = float(close_match.group("exit"))
                        amount = float(close_match.group("size"))
                        key = _grid_tuple_key(day, entry_side, exit_side, entry_price, exit_price, amount)
                        if key not in confirmed_close_keys:
                            confirmed_close_keys.add(key)
                            pnl = float(close_match.group("pnl"))
                            bucket = days[day]
                            bucket.grid_pnl += pnl
                            bucket.grid_close_count += 1
                            bucket.grid_files.add(path.name)
                        continue

                    hedge_match = GRID_HEDGE_RE.search(line)
                    if hedge_match:
                        day = hedge_match.group("ts")
                        if day < since_date:
                            continue
                        fill_side = hedge_match.group("fill_side")
                        counter_side = hedge_match.group("counter_side")
                        fill_price = float(hedge_match.group("fill_price"))
                        counter_price = float(hedge_match.group("counter_price"))
                        amount = float(hedge_match.group("amount"))
                        estimated_pnl = (counter_price - fill_price) * amount if fill_side.lower() == "buy" else (fill_price - counter_price) * amount
                        key = _grid_tuple_key(day, fill_side, counter_side, fill_price, counter_price, amount)
                        raw_hedges.append((day, path, key, estimated_pnl))
        except FileNotFoundError:
            continue

    seen_hedge_keys: set[tuple[Any, ...]] = set()
    for day, path, key, estimated_pnl in raw_hedges:
        if key in confirmed_close_keys or key in seen_hedge_keys:
            continue
        seen_hedge_keys.add(key)
        bucket = days[day]
        bucket.grid_target_missing_pnl += estimated_pnl
        bucket.grid_target_missing_count += 1
        bucket.grid_files.add(path.name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily CTA-vs-Grid drag report")
    parser.add_argument("--days", type=float, default=14.0, help="Lookback window in days (default: 14)")
    parser.add_argument("--hours", type=float, help="Override lookback window in hours")
    parser.add_argument("--symbol", default="BTC/USDT", help="Optional symbol filter for CTA journal rows")
    parser.add_argument("--json", help="Optional JSON output path")
    return parser


def print_report(days: dict[str, DayBucket]) -> None:
    print("CTA vs Grid Daily Drag Report")
    print("day | grid_pnl | grid_target_missing | cta_pnl | combined | cta_drag_vs_grid | cta_closes | grid_closes | blocked | worst_cta_family | worst_cta_pathway")
    for day in sorted(days):
        bucket = days[day]
        worst_family = min(bucket.cta_by_family.items(), key=lambda kv: kv[1])[0] if bucket.cta_by_family else "-"
        worst_pathway = min(bucket.cta_by_pathway.items(), key=lambda kv: kv[1])[0] if bucket.cta_by_pathway else "-"
        total = bucket.cta_pnl + bucket.grid_pnl
        drag_pct = "-"
        if bucket.grid_pnl > 0:
            drag_pct = f"{abs(min(0.0, bucket.cta_pnl)) / bucket.grid_pnl * 100.0:.1f}%"
        print(
            f"{day} | {bucket.grid_pnl:.4f} | {bucket.grid_target_missing_pnl:.4f} | {bucket.cta_pnl:.4f} | {total:.4f} | {drag_pct} | "
            f"{bucket.cta_close_count} | {bucket.grid_close_count} | {bucket.cta_blocked_count} | {worst_family} | {worst_pathway}"
        )

    overlap_days = [day for day, bucket in days.items() if bucket.grid_close_count > 0 and bucket.cta_close_count > 0]
    print("")
    print(f"Days with CTA/Grid overlap: {len(overlap_days)}")
    if overlap_days:
        print("Overlap days: " + ", ".join(sorted(overlap_days)))
    else:
        print("Overlap days: none in current retained data")


def main() -> int:
    args = build_parser().parse_args()
    hours = args.hours if args.hours is not None else args.days * 24.0
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    since_date = since.date().isoformat()

    days: dict[str, DayBucket] = defaultdict(DayBucket)
    db_path = load_db_path()
    collect_cta(days, db_path, since.isoformat(), args.symbol)
    collect_grid(days, since_date)

    print_report(days)

    payload = {
        "window": {
            "hours": hours,
            "since_utc": since.isoformat(),
            "until_utc": now.isoformat(),
            "symbol": args.symbol,
        },
        "days": {day: days[day].to_json() for day in sorted(days)},
    }
    if args.json:
        Path(args.json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
