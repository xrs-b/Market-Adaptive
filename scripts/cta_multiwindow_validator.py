#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_adaptive.config import CTAConfig, ExecutionConfig
from scripts.cta_backtest_sandbox import CTABacktester, load_csv


def _norm(value: Any, fallback: str = "unknown") -> str:
    text = str(value or "").strip()
    return text or fallback


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run multi-window CTA validation and summarize stable/unstable buckets.")
    parser.add_argument("--csv", required=True, help="OHLCV csv path with timestamp/open/high/low/close/volume columns")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--window-size", type=int, default=1200, help="Rows per validation window")
    parser.add_argument("--step-size", type=int, default=600, help="Row step between windows")
    parser.add_argument("--max-windows", type=int, default=0, help="Optional cap on number of windows; 0 means all")
    parser.add_argument("--tail", type=int, default=0, help="Optional tail rows before slicing windows; 0 means full file")
    parser.add_argument("--warmup-bars", type=int, default=200)
    parser.add_argument("--starting-balance", type=float, default=10000.0)
    parser.add_argument("--taker-fee-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--fill-mode", default="next_open", choices=["next_open", "current_close"])
    parser.add_argument("--bucket-min-trades", type=int, default=2, help="Minimum trades for a bucket to count in stability summary")
    parser.add_argument("--output", help="Optional JSON output path")
    return parser


def iter_windows(total_rows: int, window_size: int, step_size: int, max_windows: int = 0) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    if total_rows < window_size:
        return windows
    start = 0
    while start + window_size <= total_rows:
        windows.append((start, start + window_size))
        if max_windows and len(windows) >= max_windows:
            break
        start += step_size
    return windows


def summarize_window(report, start: int, end: int) -> dict[str, Any]:
    trade_quality = report.diagnostics.get("trade_quality_report", {})
    return {
        "window_start": int(start),
        "window_end_exclusive": int(end),
        "summary": {
            "total_return_pct": report.total_return_pct,
            "max_drawdown_abs": report.max_drawdown_abs,
            "max_drawdown_pct": report.max_drawdown_pct,
            "win_rate": report.win_rate,
            "profit_factor": report.profit_factor,
            "total_trades": report.total_trades,
            "fees_paid": report.fees_paid,
            "realized_pnl": report.realized_pnl,
        },
        "trade_quality_summary": trade_quality.get("summary", {}),
        "by_trigger_family": trade_quality.get("by_trigger_family", []),
        "by_side": trade_quality.get("by_side", []),
        "by_pathway_quality": trade_quality.get("by_pathway_quality", []),
        "by_trigger_pathway": trade_quality.get("by_trigger_pathway", []),
    }


def collect_bucket_stability(window_results: list[dict[str, Any]], *, min_trades: int) -> dict[str, list[dict[str, Any]]]:
    bucket_sources = {
        "trigger_family": "by_trigger_family",
        "side": "by_side",
        "pathway_quality": "by_pathway_quality",
        "trigger_pathway": "by_trigger_pathway",
    }
    stability: dict[str, list[dict[str, Any]]] = {}

    for bucket_name, field_name in bucket_sources.items():
        accumulator: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "bucket": None,
            "windows": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "total_realized_pnl": 0.0,
            "positive_windows": 0,
            "negative_windows": 0,
            "flat_windows": 0,
            "window_pnls": [],
        })

        for window in window_results:
            for row in window.get(field_name, []) or []:
                trade_count = int(row.get("trade_count") or 0)
                if trade_count < min_trades:
                    continue
                if bucket_name == "trigger_family":
                    bucket_id = _norm(row.get("trigger_family"))
                elif bucket_name == "side":
                    bucket_id = _norm(row.get("side"))
                elif bucket_name == "pathway_quality":
                    bucket_id = f"{_norm(row.get('entry_pathway'))}::{_norm(row.get('quality_tier'))}"
                else:
                    bucket_id = f"{_norm(row.get('trigger_family'))}::{_norm(row.get('entry_pathway'))}"

                acc = accumulator[bucket_id]
                acc["bucket"] = bucket_id
                acc["windows"] += 1
                acc["trades"] += trade_count
                acc["wins"] += int(row.get("wins") or 0)
                acc["losses"] += int(row.get("losses") or 0)
                pnl = _safe_float(row.get("total_realized_pnl"))
                acc["total_realized_pnl"] += pnl
                acc["window_pnls"].append(round(pnl, 6))
                if pnl > 0:
                    acc["positive_windows"] += 1
                elif pnl < 0:
                    acc["negative_windows"] += 1
                else:
                    acc["flat_windows"] += 1

        rows: list[dict[str, Any]] = []
        for acc in accumulator.values():
            windows = max(1, int(acc["windows"]))
            trades = max(1, int(acc["trades"]))
            wins = int(acc["wins"])
            losses = int(acc["losses"])
            total_realized_pnl = round(float(acc["total_realized_pnl"]), 6)
            positive_windows = int(acc["positive_windows"])
            negative_windows = int(acc["negative_windows"])
            consistency = round((positive_windows - negative_windows) / windows, 4)
            row = {
                "bucket": acc["bucket"],
                "windows": windows,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": round(wins / trades * 100.0, 2),
                "total_realized_pnl": total_realized_pnl,
                "avg_window_pnl": round(total_realized_pnl / windows, 6),
                "positive_windows": positive_windows,
                "negative_windows": negative_windows,
                "flat_windows": int(acc["flat_windows"]),
                "consistency_score": consistency,
                "window_pnls": acc["window_pnls"],
                "stable_positive": positive_windows > 0 and negative_windows == 0,
                "stable_negative": negative_windows > 0 and positive_windows == 0,
                "mixed": positive_windows > 0 and negative_windows > 0,
            }
            rows.append(row)

        rows.sort(key=lambda row: (-row["consistency_score"], -row["total_realized_pnl"], -row["windows"], row["bucket"]))
        stability[bucket_name] = rows
    return stability


def summarize_overall(window_results: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(window_results)
    if count == 0:
        return {
            "windows": 0,
            "total_realized_pnl": 0.0,
            "avg_window_realized_pnl": 0.0,
            "positive_windows": 0,
            "negative_windows": 0,
            "flat_windows": 0,
        }

    pnls = [_safe_float(window["summary"].get("realized_pnl")) for window in window_results]
    return {
        "windows": count,
        "total_realized_pnl": round(sum(pnls), 6),
        "avg_window_realized_pnl": round(sum(pnls) / count, 6),
        "positive_windows": sum(1 for pnl in pnls if pnl > 0),
        "negative_windows": sum(1 for pnl in pnls if pnl < 0),
        "flat_windows": sum(1 for pnl in pnls if pnl == 0),
        "best_window_realized_pnl": round(max(pnls), 6),
        "worst_window_realized_pnl": round(min(pnls), 6),
    }


def main() -> int:
    args = build_parser().parse_args()
    df = load_csv(args.csv)
    if args.tail and args.tail > 0:
        df = df.tail(args.tail).reset_index(drop=True)

    windows = iter_windows(len(df), args.window_size, args.step_size, args.max_windows)
    cta_config = CTAConfig(symbol=args.symbol)
    execution_config = ExecutionConfig()

    window_results: list[dict[str, Any]] = []
    for start, end in windows:
        sub = df.iloc[start:end].reset_index(drop=True)
        backtester = CTABacktester(
            sub,
            cta_config=cta_config,
            execution_config=execution_config,
            symbol=args.symbol,
            starting_balance=args.starting_balance,
            taker_fee_rate=args.taker_fee_rate,
            slippage_rate=args.slippage_rate,
            fill_mode=args.fill_mode,
            warmup_bars=args.warmup_bars,
        )
        report = backtester.run()
        window_results.append(summarize_window(report, start, end))

    stability = collect_bucket_stability(window_results, min_trades=args.bucket_min_trades)
    payload = {
        "config": {
            "csv": args.csv,
            "symbol": args.symbol,
            "window_size": args.window_size,
            "step_size": args.step_size,
            "max_windows": args.max_windows,
            "tail": args.tail,
            "warmup_bars": args.warmup_bars,
            "bucket_min_trades": args.bucket_min_trades,
        },
        "overall": summarize_overall(window_results),
        "windows": window_results,
        "stability": stability,
        "stable_positive_candidates": {
            name: [row for row in rows if row.get("stable_positive")]
            for name, rows in stability.items()
        },
        "stable_negative_candidates": {
            name: [row for row in rows if row.get("stable_negative")]
            for name, rows in stability.items()
        },
        "mixed_buckets": {
            name: [row for row in rows if row.get("mixed")]
            for name, rows in stability.items()
        },
    }

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
