#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import load_config


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_meta(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


@dataclass
class Outcome:
    horizon_min: int
    future_close: float | None
    future_best: float | None
    future_worst: float | None
    move_pct: float | None
    mfe_pct: float | None
    mae_pct: float | None
    would_help_block: bool | None


def find_future(candles: list[list[Any]], ts: datetime, horizon_min: int) -> list[list[Any]]:
    start_ms = int(ts.timestamp() * 1000)
    end_ms = int((ts + timedelta(minutes=horizon_min)).timestamp() * 1000)
    return [c for c in candles if int(c[0]) > start_ms and int(c[0]) <= end_ms]


def calc_outcome(row: sqlite3.Row, candles: list[list[Any]], horizon_min: int) -> Outcome:
    ts = parse_ts(row["timestamp"])
    side = str(row["side"] or "")
    entry = float(row["price"] or 0.0)
    future = find_future(candles, ts, horizon_min)
    if not future or entry <= 0:
        return Outcome(horizon_min, None, None, None, None, None, None, None)
    close = float(future[-1][4])
    highs = [float(c[2]) for c in future]
    lows = [float(c[3]) for c in future]
    best = max(highs) if side == "long" else min(lows)
    worst = min(lows) if side == "long" else max(highs)
    if side == "long":
        move = (close - entry) / entry * 100.0
        mfe = (best - entry) / entry * 100.0
        mae = (entry - worst) / entry * 100.0
    else:
        move = (entry - close) / entry * 100.0
        mfe = (entry - best) / entry * 100.0
        mae = (worst - entry) / entry * 100.0
    return Outcome(horizon_min, close, best, worst, move, mfe, mae, move <= 0.0)


def load_rows(db_path: Path, since_iso: str, actions_prefix: str = "cta:pro_signal_") -> list[sqlite3.Row]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        return list(
            con.execute(
                """
                SELECT * FROM trade_journal
                WHERE strategy_name='cta'
                  AND event_type='blocked_signal'
                  AND timestamp >= ?
                  AND action LIKE ?
                ORDER BY timestamp
                """,
                (since_iso, f"{actions_prefix}%"),
            )
        )
    finally:
        con.close()


def summarize(rows: list[dict[str, Any]], horizon: int) -> dict[str, Any]:
    scoped = [r for r in rows if r.get(f"h{horizon}_move_pct") is not None]
    if not scoped:
        return {}
    helped = [r for r in scoped if r.get(f"h{horizon}_would_help_block")]
    missed = [r for r in scoped if r.get(f"h{horizon}_would_help_block") is False]
    moves = [float(r[f"h{horizon}_move_pct"]) for r in scoped]
    mfe = [float(r[f"h{horizon}_mfe_pct"]) for r in scoped]
    mae = [float(r[f"h{horizon}_mae_pct"]) for r in scoped]
    return {
        "n": len(scoped),
        "helped_blocks": len(helped),
        "missed_good_moves": len(missed),
        "help_rate": round(len(helped) / len(scoped), 4),
        "avg_directional_move_pct": round(mean(moves), 4),
        "avg_mfe_pct": round(mean(mfe), 4),
        "avg_mae_pct": round(mean(mae), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Postmortem pro-signal blocked CTA signals against future OHLCV movement.")
    parser.add_argument("--config", default=str(ROOT / "config" / "config.yaml"))
    parser.add_argument("--db", default=str(ROOT / "data" / "market_adaptive.sqlite3"))
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--horizons", default="30,60,120")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=args.hours)
    rows = load_rows(Path(args.db), iso_utc(since))[-args.limit :]
    horizons = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]

    cfg = load_config(args.config)
    client = OKXClient(cfg.okx, cfg.execution)
    max_h = max(horizons) if horizons else 120
    since_ms = int((since - timedelta(minutes=5)).timestamp() * 1000)
    # OKX/ccxt limit can be capped; use generous limit for 1-2 days on 1m where possible.
    candles: list[list[Any]] = []
    cursor = since_ms
    end_ms = int((now + timedelta(minutes=max_h + 5)).timestamp() * 1000)
    for _ in range(20):
        batch = client.fetch_ohlcv(args.symbol, args.timeframe, since=cursor, limit=300)
        if not batch:
            break
        for candle in batch:
            if not candles or int(candle[0]) > int(candles[-1][0]):
                candles.append(candle)
        last_ms = int(batch[-1][0])
        if last_ms >= end_ms or last_ms <= cursor:
            break
        cursor = last_ms + 1

    out_rows: list[dict[str, Any]] = []
    for r in rows:
        meta = parse_meta(r["metadata_json"])
        item: dict[str, Any] = {
            "timestamp": r["timestamp"],
            "side": r["side"],
            "action": r["action"],
            "trigger_family": r["trigger_family"],
            "price": float(r["price"] or 0.0),
            "pathway": r["pathway"],
            "pro_signal_reason": meta.get("pro_signal_reason"),
            "pro_signal_rr": meta.get("pro_signal_rr"),
            "pro_signal_htf_bias": meta.get("pro_signal_htf_bias"),
            "pro_signal_setup_family": meta.get("pro_signal_setup_family"),
            "entry_decider_score": meta.get("entry_decider_score"),
            "signal_confidence": meta.get("signal_confidence"),
            "market_regime": meta.get("market_regime"),
        }
        for h in horizons:
            o = calc_outcome(r, candles, h)
            item[f"h{h}_close"] = o.future_close
            item[f"h{h}_move_pct"] = None if o.move_pct is None else round(o.move_pct, 4)
            item[f"h{h}_mfe_pct"] = None if o.mfe_pct is None else round(o.mfe_pct, 4)
            item[f"h{h}_mae_pct"] = None if o.mae_pct is None else round(o.mae_pct, 4)
            item[f"h{h}_would_help_block"] = o.would_help_block
        out_rows.append(item)

    print(json.dumps({
        "since": iso_utc(since),
        "rows": len(out_rows),
        "summaries": {str(h): summarize(out_rows, h) for h in horizons},
        "items": out_rows,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
