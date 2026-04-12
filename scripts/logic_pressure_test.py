#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import AppConfig, CTAConfig, load_config
from market_adaptive.strategies.cta_robot import CTARobot

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


class ReplayClient:
    def __init__(self, ohlcv_by_timeframe: dict[str, list[list[Any]]], *, cursor_ms: int) -> None:
        self.ohlcv_by_timeframe = ohlcv_by_timeframe
        self.cursor_ms = int(cursor_ms)

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        del symbol, since
        payload = [row for row in self.ohlcv_by_timeframe.get(timeframe, []) if int(row[0]) <= self.cursor_ms]
        return payload[-limit:]


class DummyDatabase:
    pass


class PressureTestRobot(CTARobot):
    def should_notify_action(self, action: str) -> bool:
        del action
        return False


def _timeframe_to_minutes(timeframe: str) -> int:
    normalized = str(timeframe).strip().lower()
    if normalized.endswith("m"):
        return int(normalized[:-1])
    if normalized.endswith("h"):
        return int(normalized[:-1]) * 60
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _fetch_history(client: OKXClient, config: CTAConfig, hours: float) -> dict[str, list[list[Any]]]:
    execution_minutes = _timeframe_to_minutes(config.execution_timeframe)
    replay_bars = max(1, int((hours * 60) / execution_minutes))
    execution_limit = max(int(config.lookback_limit), replay_bars + int(config.lookback_limit))
    swing_limit = max(int(config.lookback_limit), replay_bars + 32)
    major_limit = max(int(config.lookback_limit), max(48, replay_bars // 4 + 24))
    return {
        config.execution_timeframe: client.fetch_ohlcv(config.symbol, config.execution_timeframe, limit=execution_limit),
        config.swing_timeframe: client.fetch_ohlcv(config.symbol, config.swing_timeframe, limit=swing_limit),
        config.major_timeframe: client.fetch_ohlcv(config.symbol, config.major_timeframe, limit=major_limit),
    }


def _build_robot(ohlcv_by_timeframe: dict[str, list[list[Any]]], cta_config: CTAConfig, *, cursor_ms: int) -> PressureTestRobot:
    replay_client = ReplayClient(ohlcv_by_timeframe, cursor_ms=cursor_ms)
    robot = PressureTestRobot(
        client=replay_client,
        database=DummyDatabase(),
        config=cta_config,
        execution_config=type("ExecutionConfigStub", (), {"cta_order_size": 0.01})(),
        notifier=None,
        risk_manager=None,
        sentiment_analyst=None,
    )
    return robot


def _evaluate_at_bar(ohlcv_by_timeframe: dict[str, list[list[Any]]], cta_config: CTAConfig, *, cursor_ms: int) -> dict[str, Any] | None:
    robot = _build_robot(ohlcv_by_timeframe, cta_config, cursor_ms=cursor_ms)
    signal = robot._build_trend_signal()
    if signal is None:
        return None
    trigger_ready = bool(signal.direction > 0)
    obv = signal.obv_confirmation
    blocker = "ready"
    if signal.raw_direction <= 0:
        blocker = str(signal.execution_trigger_reason)
    elif signal.long_setup_reason:
        blocker = str(signal.long_setup_reason)
    return {
        "timestamp": datetime.fromtimestamp(cursor_ms / 1000, tz=timezone.utc).isoformat(),
        "price": float(signal.price),
        "bullish_ready": bool(signal.bullish_ready),
        "direction": int(signal.direction),
        "raw_direction": int(signal.raw_direction),
        "mtf_aligned": bool(signal.mtf_aligned),
        "execution_golden_cross": bool(signal.execution_golden_cross),
        "execution_breakout": bool(signal.execution_breakout),
        "execution_trigger_reason": str(signal.execution_trigger_reason),
        "swing_rsi": float(signal.swing_rsi),
        "risk_percent": float(signal.risk_percent),
        "obv_bias": int(signal.obv_bias),
        "obv_current": float(obv.current_obv),
        "obv_sma": float(obv.sma_value),
        "obv_increment": float(obv.increment_value),
        "obv_zscore": float(obv.zscore),
        "obv_zscore_threshold": float(cta_config.obv_zscore_threshold),
        "obv_confirmation_passed": bool(signal.obv_confirmation_passed),
        "volume_filter_passed": bool(signal.volume_filter_passed),
        "long_setup_blocked": bool(signal.long_setup_blocked),
        "long_setup_reason": str(signal.long_setup_reason),
        "blocker": blocker,
        "trigger_ready": trigger_ready,
    }


def _zscore_summary(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    blocked = [float(row["obv_zscore"]) for row in rows if not row["trigger_ready"]]
    if not blocked:
        return {"count": 0, "min": None, "max": None, "avg": None}
    return {
        "count": len(blocked),
        "min": min(blocked),
        "max": max(blocked),
        "avg": mean(blocked),
    }


def _blocked_reason_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(row.get("blocker") or "other" for row in rows if not row["trigger_ready"])
    return dict(counter.most_common())


def _stage_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = Counter()
    for row in rows:
        if row["trigger_ready"]:
            summary["ready"] += 1
        elif row["raw_direction"] <= 0:
            summary["execution_not_aligned"] += 1
        elif row["long_setup_blocked"]:
            summary["post_trigger_filter_blocked"] += 1
        else:
            summary["other_blocked"] += 1
    return dict(summary)


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ready_count = sum(1 for row in rows if row["trigger_ready"])
    return {
        "trigger_count": ready_count,
        "blocked_count": len(rows) - ready_count,
        "stage_counts": _stage_summary(rows),
        "blocker_counts": _blocked_reason_summary(rows),
        "blocked_obv_zscore": _zscore_summary(rows),
    }


def run_pressure_test(
    config: AppConfig,
    *,
    hours: float,
    baseline_threshold: float | None = None,
    compare_threshold: float | None = None,
    obv_scale: float | None = None,
) -> dict[str, Any]:
    client = OKXClient(config.okx, config.execution)
    history = _fetch_history(client, config.cta, hours)
    execution_rows = history[config.cta.execution_timeframe]
    if not execution_rows:
        raise RuntimeError("No execution timeframe OHLCV fetched")

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=float(hours))
    replay_points = [
        row for row in execution_rows
        if start_dt.timestamp() * 1000 <= int(row[0]) <= end_dt.timestamp() * 1000
    ]
    baseline_config = replace(
        config.cta,
        obv_zscore_threshold=float(config.cta.obv_zscore_threshold if baseline_threshold is None else baseline_threshold),
    )
    if compare_threshold is None:
        scale = 1.0 if obv_scale is None else float(obv_scale)
        compare_threshold = float(baseline_config.obv_zscore_threshold) * scale
    compare_config = replace(config.cta, obv_zscore_threshold=float(compare_threshold))

    baseline_rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    unlocked: list[dict[str, Any]] = []

    for row in replay_points:
        cursor_ms = int(row[0])
        baseline = _evaluate_at_bar(history, baseline_config, cursor_ms=cursor_ms)
        compare = _evaluate_at_bar(history, compare_config, cursor_ms=cursor_ms)
        if baseline is None or compare is None:
            continue
        baseline_rows.append(baseline)
        compare_rows.append(compare)
        if (not baseline["trigger_ready"]) and compare["trigger_ready"]:
            unlocked.append({"baseline": baseline, "compare": compare})

    baseline_summary = _build_summary(baseline_rows)
    compare_summary = _build_summary(compare_rows)
    return {
        "symbol": config.cta.symbol,
        "hours": float(hours),
        "baseline_threshold": float(baseline_config.obv_zscore_threshold),
        "compare_threshold": float(compare_config.obv_zscore_threshold),
        "baseline_summary": baseline_summary,
        "compare_summary": compare_summary,
        "delta_trigger_count": int(compare_summary["trigger_count"]) - int(baseline_summary["trigger_count"]),
        "replay_points": len(baseline_rows),
        "baseline_rows": baseline_rows,
        "compare_rows": compare_rows,
        "unlocked_windows": unlocked,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay last N hours of CTA logic and compare OBV z-score thresholds with blocker ranking."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to config.yaml")
    parser.add_argument("--hours", type=float, default=24.0, help="Replay last N hours on execution timeframe")
    parser.add_argument("--baseline-threshold", type=float, default=None, help="Baseline OBV z-score threshold")
    parser.add_argument("--compare-threshold", type=float, default=None, help="Compare OBV z-score threshold")
    parser.add_argument("--obv-scale", type=float, default=None, help="Legacy alias: scale baseline threshold by this factor")
    parser.add_argument("--json", action="store_true", help="Print JSON payload")
    args = parser.parse_args()

    config = load_config(args.config)
    report = run_pressure_test(
        config,
        hours=args.hours,
        baseline_threshold=args.baseline_threshold,
        compare_threshold=args.compare_threshold,
        obv_scale=args.obv_scale,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(
            f"symbol={report['symbol']} hours={report['hours']} baseline_threshold={report['baseline_threshold']:.2f} "
            f"compare_threshold={report['compare_threshold']:.2f} baseline={report['baseline_summary']['trigger_count']} "
            f"compare={report['compare_summary']['trigger_count']} delta={report['delta_trigger_count']} replay_points={report['replay_points']}"
        )
        print(f"baseline blockers: {report['baseline_summary']['blocker_counts']}")
        print(f"compare blockers: {report['compare_summary']['blocker_counts']}")
        for unlocked in report["unlocked_windows"]:
            compare = unlocked["compare"]
            print(
                "unlocked {timestamp} price={price:.4f} z={obv_zscore:.2f}>{obv_zscore_threshold:.2f} blocker={blocker} reason={execution_trigger_reason}".format(
                    **compare,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
