from __future__ import annotations

from collections import defaultdict
from typing import Any


def _norm(value: Any, fallback: str = "unknown") -> str:
    text = str(value or "").strip()
    return text or fallback


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def build_bucket_rows(trades: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades or []:
        bucket_key = tuple(_norm(trade.get(key)) for key in keys)
        buckets[bucket_key].append(trade)

    rows: list[dict[str, Any]] = []
    for bucket_key, bucket_trades in sorted(buckets.items()):
        pnl_values = [_safe_float(trade.get("realized_pnl")) for trade in bucket_trades]
        fee_values = [_safe_float(trade.get("fees")) for trade in bucket_trades]
        hold_minutes = [_safe_float(trade.get("holding_minutes")) for trade in bucket_trades if trade.get("holding_minutes") is not None]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
        row = {
            "trade_count": len(bucket_trades),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round((wins / len(bucket_trades) * 100.0), 2) if bucket_trades else 0.0,
            "total_realized_pnl": round(sum(pnl_values), 6),
            "avg_realized_pnl": round(sum(pnl_values) / len(bucket_trades), 6) if bucket_trades else 0.0,
            "total_fees": round(sum(fee_values), 6),
            "avg_fees": round(sum(fee_values) / len(bucket_trades), 6) if bucket_trades else 0.0,
            "avg_holding_minutes": round(sum(hold_minutes) / len(hold_minutes), 4) if hold_minutes else None,
            "profit_factor": round(float(profit_factor), 4) if profit_factor != float("inf") else "inf",
            "long_count": sum(1 for trade in bucket_trades if _norm(trade.get("side")) == "long"),
            "short_count": sum(1 for trade in bucket_trades if _norm(trade.get("side")) == "short"),
            "quick_trade_count": sum(1 for trade in bucket_trades if bool(trade.get("quick_trade_mode"))),
            "relaxed_entry_count": sum(1 for trade in bucket_trades if bool(trade.get("relaxed_entry"))),
        }
        for key, value in zip(keys, bucket_key):
            row[key] = value
        rows.append(row)

    rows.sort(
        key=lambda row: (
            -_safe_int(row.get("trade_count")),
            -_safe_float(row.get("total_realized_pnl")),
            *[_norm(row.get(key)) for key in keys],
        )
    )
    return rows


def summarize_cta_trade_quality(trades: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_trades = [dict(trade) for trade in (trades or [])]
    pnl_values = [_safe_float(trade.get("realized_pnl")) for trade in normalized_trades]
    wins = sum(1 for value in pnl_values if value > 0)
    losses = sum(1 for value in pnl_values if value < 0)
    gross_profit = sum(value for value in pnl_values if value > 0)
    gross_loss = abs(sum(value for value in pnl_values if value < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    return {
        "summary": {
            "trade_count": len(normalized_trades),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round((wins / len(normalized_trades) * 100.0), 2) if normalized_trades else 0.0,
            "total_realized_pnl": round(sum(pnl_values), 6),
            "avg_realized_pnl": round(sum(pnl_values) / len(normalized_trades), 6) if normalized_trades else 0.0,
            "total_fees": round(sum(_safe_float(trade.get("fees")) for trade in normalized_trades), 6),
            "profit_factor": round(float(profit_factor), 4) if profit_factor != float("inf") else "inf",
            "long_count": sum(1 for trade in normalized_trades if _norm(trade.get("side")) == "long"),
            "short_count": sum(1 for trade in normalized_trades if _norm(trade.get("side")) == "short"),
            "quick_trade_count": sum(1 for trade in normalized_trades if bool(trade.get("quick_trade_mode"))),
            "relaxed_entry_count": sum(1 for trade in normalized_trades if bool(trade.get("relaxed_entry"))),
        },
        "by_trigger_family": build_bucket_rows(normalized_trades, "trigger_family"),
        "by_entry_pathway": build_bucket_rows(normalized_trades, "entry_pathway"),
        "by_quality_tier": build_bucket_rows(normalized_trades, "quality_tier"),
        "by_side": build_bucket_rows(normalized_trades, "side"),
        "by_pathway_quality": build_bucket_rows(normalized_trades, "entry_pathway", "quality_tier"),
        "by_trigger_pathway": build_bucket_rows(normalized_trades, "trigger_family", "entry_pathway"),
        "by_trigger_quality": build_bucket_rows(normalized_trades, "trigger_family", "quality_tier"),
    }
