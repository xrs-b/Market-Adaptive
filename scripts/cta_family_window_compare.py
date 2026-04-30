#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cta_family_report import (
    FamilyStats,
    OpenLot,
    bucket,
    find_matching_open,
    is_passed_blocker,
    load_db_path,
    parse_ts,
    safe_json_loads,
)


@dataclass
class WindowReport:
    hours: float
    rows: int
    first_ts: datetime | None
    last_ts: datetime | None
    event_counts: Counter[str]
    families: dict[str, FamilyStats]
    blocker_reasons: dict[str, Counter[str]]
    blocked_pathways: dict[str, Counter[str]]
    passed_pathways: dict[str, Counter[str]]
    open_pathways: dict[str, Counter[str]]
    close_pathways: dict[str, Counter[str]]
    unmatched_closes: int
    still_open_lots: Counter[str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare CTA family × blocker_reason × pathway across recent windows"
    )
    parser.add_argument(
        "--windows",
        default="24,168",
        help="Comma-separated windows in hours (default: 24,168 for 24h and 7d)",
    )
    parser.add_argument("--symbol", help="Optional symbol filter, e.g. BTC/USDT")
    parser.add_argument("--top-families", type=int, default=6, help="Families per window (default: 6)")
    parser.add_argument("--top-blockers", type=int, default=3, help="Blocker reasons per family (default: 3)")
    parser.add_argument("--top-pathways", type=int, default=3, help="Pathways per family bucket (default: 3)")
    return parser


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


def summarize_window(rows: list[sqlite3.Row], hours: float) -> WindowReport:
    families: dict[str, FamilyStats] = defaultdict(FamilyStats)
    blocker_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    blocked_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    passed_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    open_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    close_pathways: dict[str, Counter[str]] = defaultdict(Counter)
    open_lots: list[OpenLot] = []
    event_counts: Counter[str] = Counter()
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    unmatched_closes = 0

    for row in rows:
        ts = parse_ts(row["timestamp"])
        if ts is not None:
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)

        event_type = bucket(row["event_type"])
        family = bucket(row["trigger_family"])
        pathway = bucket(row["pathway"])
        meta = safe_json_loads(row["metadata_json"])
        event_counts[event_type] += 1

        if event_type == "blocked_signal":
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
            continue

        if event_type == "trade_open":
            families[family].opens += 1
            open_pathways[family][pathway] += 1
            open_lots.append(
                OpenLot(
                    row_id=int(row["id"]),
                    timestamp=str(row["timestamp"]),
                    side=bucket(row["side"]),
                    size=float(row["size"] or 0.0),
                    family=family,
                    pathway=pathway,
                )
            )
            continue

        if event_type == "trade_close":
            matched, idx = find_matching_open(
                open_lots,
                close_side=bucket(row["side"]),
                close_size=float(row["size"] or 0.0),
            )
            if matched is None or idx is None:
                unmatched_closes += 1
                continue
            open_lots.pop(idx)
            families[matched.family].closes += 1
            pnl = float(row["pnl"] or 0.0)
            families[matched.family].realized_pnl += pnl
            if pnl > 0:
                families[matched.family].wins += 1
            close_pathways[matched.family][matched.pathway] += 1

    return WindowReport(
        hours=hours,
        rows=len(rows),
        first_ts=first_ts,
        last_ts=last_ts,
        event_counts=event_counts,
        families=families,
        blocker_reasons=blocker_reasons,
        blocked_pathways=blocked_pathways,
        passed_pathways=passed_pathways,
        open_pathways=open_pathways,
        close_pathways=close_pathways,
        unmatched_closes=unmatched_closes,
        still_open_lots=Counter(lot.family for lot in open_lots),
    )


def fmt_counter(counter: Counter[str], limit: int) -> str:
    return ", ".join(f"{k}:{v}" for k, v in counter.most_common(limit)) or "-"


def fmt_pct(num: int, den: int) -> str:
    if den <= 0:
        return "-"
    return f"{100.0 * num / den:.1f}%"


def format_family_line(
    family: str,
    report: WindowReport,
    *,
    top_blockers: int,
    top_pathways: int,
) -> str:
    stats = report.families[family]
    attempts = stats.blocked + stats.passed + stats.opens
    return (
        f"- {family}: attempts={attempts}, blocked={stats.blocked}, passed={stats.passed}, opens={stats.opens}, closes={stats.closes}, "
        f"open_rate={fmt_pct(stats.opens, attempts)}, win_rate={fmt_pct(stats.wins, stats.closes)}, pnl={stats.realized_pnl:.2f}\n"
        f"  blockers: {fmt_counter(report.blocker_reasons[family], top_blockers)}\n"
        f"  blocked_pathways: {fmt_counter(report.blocked_pathways[family], top_pathways)}\n"
        f"  passed_pathways: {fmt_counter(report.passed_pathways[family], top_pathways)}\n"
        f"  open_pathways: {fmt_counter(report.open_pathways[family], top_pathways)}\n"
        f"  close_pathways: {fmt_counter(report.close_pathways[family], top_pathways)}"
    )


def main() -> int:
    args = build_parser().parse_args()
    windows = [float(part.strip()) for part in args.windows.split(",") if part.strip()]
    if not windows:
        raise SystemExit("No windows provided")

    db_path = load_db_path()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    max_hours = max(windows)
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=max_hours)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    base_rows = fetch_rows(conn, since.isoformat(), args.symbol)

    reports: list[WindowReport] = []
    for hours in windows:
        cutoff = now - timedelta(hours=hours)
        rows = [row for row in base_rows if (parse_ts(row["timestamp"]) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]
        reports.append(summarize_window(rows, hours))

    print("CTA Family × Blocker × Pathway Window Compare")
    print(f"DB: {db_path}")
    print(f"Symbol: {args.symbol or 'ALL'}")
    print(f"Windows (hours): {', '.join(f'{h:g}' for h in windows)}")
    print(f"Fetched base rows: {len(base_rows)} since {since.isoformat()} UTC")

    for report in reports:
        print("")
        print(f"=== Window: last {report.hours:g}h ===")
        print(
            f"rows={report.rows} | range={(report.first_ts.isoformat() if report.first_ts else '-')} -> {(report.last_ts.isoformat() if report.last_ts else '-')}"
        )
        print(f"events={dict(report.event_counts)}")

        ranked = sorted(
            report.families.keys(),
            key=lambda fam: (
                -(report.families[fam].blocked + report.families[fam].opens + report.families[fam].closes),
                -report.families[fam].realized_pnl,
                fam,
            ),
        )
        if not ranked:
            print("(no CTA rows in window)")
            continue

        print("key families:")
        for family in ranked[: args.top_families]:
            print(format_family_line(family, report, top_blockers=args.top_blockers, top_pathways=args.top_pathways))

        print(
            f"summary: unmatched_closes={report.unmatched_closes} | still_open_lots={dict(report.still_open_lots)}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
