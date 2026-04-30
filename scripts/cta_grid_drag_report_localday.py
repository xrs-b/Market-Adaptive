#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"
DISCORD_CHANNEL_ID = "1468582455660515350"
DISCORD_GUILD_ID = "1468582454796353713"
LOCAL_TZ_OFFSET_HOURS = 8


@dataclass
class DayBucket:
    cta_pnl: float = 0.0
    grid_pnl: float = 0.0
    cta_close_count: int = 0
    grid_close_count: int = 0
    cta_by_family: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    cta_by_pathway: dict[str, float] = field(default_factory=lambda: defaultdict(float))


@dataclass
class OpenLot:
    row_id: int
    side: str
    size: float
    family: str
    pathway: str


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


def to_local_day(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    local_dt = dt.astimezone(timezone(timedelta(hours=LOCAL_TZ_OFFSET_HOURS)))
    return local_dt.date().isoformat()


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


def collect_cta(days: dict[str, DayBucket], db_path: Path, since_iso: str, symbol: str) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM trade_journal
        WHERE strategy_name='cta' AND symbol=? AND timestamp >= ?
        ORDER BY timestamp ASC, id ASC
        """,
        (symbol, since_iso),
    ).fetchall()
    open_lots: list[OpenLot] = []
    for row in rows:
        event_type = str(row["event_type"] or "")
        ts = str(row["timestamp"])
        day = to_local_day(ts)
        bucket = days[day]
        if event_type == "trade_open":
            meta = safe_json_loads(row["metadata_json"])
            open_lots.append(
                OpenLot(
                    row_id=int(row["id"]),
                    side=str(row["side"] or ""),
                    size=float(row["size"] or 0.0),
                    family=str(row["trigger_family"] or meta.get("origin_trigger_family") or "UNKNOWN"),
                    pathway=str(row["pathway"] or meta.get("origin_pathway") or "UNKNOWN"),
                )
            )
        elif event_type == "trade_close":
            bucket.cta_close_count += 1
            pnl = float(row["pnl"] or 0.0)
            bucket.cta_pnl += pnl
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


def grid_rows_from_discord_search_json(raw: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    messages = (((raw or {}).get("results") or {}).get("messages") or [])
    for bundle in messages:
        if not bundle:
            continue
        msg = bundle[0]
        embeds = msg.get("embeds") or []
        if not embeds:
            continue
        embed = embeds[0]
        title = str(embed.get("title") or "")
        if "网格已实现" not in title:
            continue
        pnl = None
        for field in embed.get("fields") or []:
            if str(field.get("name") or "") == "累计已实现盈亏":
                text = str(field.get("value") or "").replace(" USDT", "").replace("+", "")
                try:
                    pnl = float(text)
                except ValueError:
                    pnl = None
        ts = msg.get("timestampUtc") or msg.get("timestamp")
        if pnl is None or not ts:
            continue
        out.append({"timestamp": str(ts), "pnl": float(pnl)})
    return out


def print_report(days: dict[str, DayBucket]) -> None:
    print("CTA/Grid Drag Report by Local Day (Asia/Shanghai)")
    print("day | grid_pnl | cta_pnl | combined | cta_drag_vs_grid | cta_closes | grid_closes | worst_cta_family | worst_cta_pathway")
    for day in sorted(days):
        bucket = days[day]
        total = bucket.grid_pnl + bucket.cta_pnl
        drag_pct = "-"
        if bucket.grid_pnl > 0 and bucket.cta_pnl < 0:
            drag_pct = f"{abs(bucket.cta_pnl) / bucket.grid_pnl * 100.0:.1f}%"
        worst_family = min(bucket.cta_by_family.items(), key=lambda kv: kv[1])[0] if bucket.cta_by_family else "-"
        worst_pathway = min(bucket.cta_by_pathway.items(), key=lambda kv: kv[1])[0] if bucket.cta_by_pathway else "-"
        print(f"{day} | {bucket.grid_pnl:.4f} | {bucket.cta_pnl:.4f} | {total:.4f} | {drag_pct} | {bucket.cta_close_count} | {bucket.grid_close_count} | {worst_family} | {worst_pathway}")

    print("\nCTA drag days (grid>0 and cta<0):")
    drag_days = []
    for day, bucket in days.items():
        if bucket.grid_pnl > 0 and bucket.cta_pnl < 0:
            drag_days.append((day, abs(bucket.cta_pnl) / bucket.grid_pnl if bucket.grid_pnl else 0.0, bucket))
    drag_days.sort(key=lambda item: item[1], reverse=True)
    if not drag_days:
        print("- none")
    else:
        for day, ratio, bucket in drag_days:
            print(f"- {day}: cta={bucket.cta_pnl:.4f}, grid={bucket.grid_pnl:.4f}, combined={bucket.cta_pnl + bucket.grid_pnl:.4f}, drag_ratio={ratio*100:.1f}%")


def main() -> int:
    parser = argparse.ArgumentParser(description="CTA vs Grid drag report using local-day bucketing")
    parser.add_argument("--days", type=float, default=14.0)
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--grid-json", help="Path to Discord grid search export JSON")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=args.days)
    days: dict[str, DayBucket] = defaultdict(DayBucket)

    collect_cta(days, load_db_path(), since.isoformat(), args.symbol)

    if args.grid_json:
        raw = json.loads(Path(args.grid_json).read_text(encoding="utf-8"))
        for row in grid_rows_from_discord_search_json(raw):
            day = to_local_day(row["timestamp"])
            if day < (since + timedelta(hours=LOCAL_TZ_OFFSET_HOURS)).date().isoformat():
                continue
            bucket = days[day]
            bucket.grid_pnl += float(row["pnl"])
            bucket.grid_close_count += 1

    print_report(days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
