#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
        order_flow_monitor=None,
    )
    return robot


def _score_components(snapshot) -> dict[str, float]:
    return {component.name: float(component.score) for component in snapshot.components}


def _evaluate_at_bar(ohlcv_by_timeframe: dict[str, list[list[Any]]], cta_config: CTAConfig, *, cursor_ms: int) -> dict[str, Any] | None:
    robot = _build_robot(ohlcv_by_timeframe, cta_config, cursor_ms=cursor_ms)
    signal = robot._build_trend_signal()
    if signal is None:
        return None
    score_snapshot = robot._build_score_snapshot(signal, None)
    trigger_ready = bool(signal.direction > 0 and score_snapshot.trade_allowed)
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
        "execution_rsi": float(signal.execution_rsi),
        "execution_adx": float(signal.execution_adx),
        "obv_bias": int(signal.obv_bias),
        "obv_signal_value": float(signal.obv_signal_value),
        "obv_slope_angle": float(signal.obv_slope_angle),
        "obv_slope_threshold": float(cta_config.obv_slope_threshold_degrees),
        "obv_slope_gap": float(signal.obv_slope_angle - cta_config.obv_slope_threshold_degrees),
        "obv_slope_passed": bool(signal.obv_slope_passed),
        "volume_filter_passed": bool(signal.volume_filter_passed),
        "volume_breakout_passed": bool(signal.volume_breakout_passed),
        "long_setup_blocked": bool(signal.long_setup_blocked),
        "long_setup_reason": str(signal.long_setup_reason),
        "score_total": float(score_snapshot.total_score),
        "score_min_trade": float(score_snapshot.min_trade_score),
        "score_tier": str(score_snapshot.tier),
        "trade_allowed": bool(score_snapshot.trade_allowed),
        "trigger_ready": trigger_ready,
        "score_components": _score_components(score_snapshot),
    }


def run_pressure_test(config: AppConfig, *, hours: float, obv_scale: float) -> dict[str, Any]:
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
    baseline_config = config.cta
    relaxed_config = replace(config.cta, obv_slope_threshold_degrees=float(config.cta.obv_slope_threshold_degrees) * float(obv_scale))

    baseline_rows: list[dict[str, Any]] = []
    relaxed_rows: list[dict[str, Any]] = []
    unlocked: list[dict[str, Any]] = []

    for row in replay_points:
        cursor_ms = int(row[0])
        baseline = _evaluate_at_bar(history, baseline_config, cursor_ms=cursor_ms)
        relaxed = _evaluate_at_bar(history, relaxed_config, cursor_ms=cursor_ms)
        if baseline is None or relaxed is None:
            continue
        baseline_rows.append(baseline)
        relaxed_rows.append(relaxed)
        if (not baseline["trigger_ready"]) and relaxed["trigger_ready"]:
            unlocked.append({"baseline": baseline, "relaxed": relaxed})

    return {
        "symbol": config.cta.symbol,
        "hours": float(hours),
        "obv_scale": float(obv_scale),
        "baseline_trigger_count": sum(1 for item in baseline_rows if item["trigger_ready"]),
        "relaxed_trigger_count": sum(1 for item in relaxed_rows if item["trigger_ready"]),
        "delta_trigger_count": sum(1 for item in relaxed_rows if item["trigger_ready"]) - sum(1 for item in baseline_rows if item["trigger_ready"]),
        "replay_points": len(baseline_rows),
        "baseline_rows": baseline_rows,
        "relaxed_rows": relaxed_rows,
        "unlocked_windows": unlocked,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay last N hours of CTA logic and compare relaxed OBV slope thresholds.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to config.yaml")
    parser.add_argument("--hours", type=float, default=10.0, help="Replay last N hours on execution timeframe")
    parser.add_argument("--obv-scale", type=float, default=0.8, help="Scale OBV slope threshold by this factor")
    parser.add_argument("--json", action="store_true", help="Print JSON payload")
    args = parser.parse_args()

    config = load_config(args.config)
    report = run_pressure_test(config, hours=args.hours, obv_scale=args.obv_scale)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(
            f"symbol={report['symbol']} hours={report['hours']} baseline={report['baseline_trigger_count']} "
            f"relaxed={report['relaxed_trigger_count']} delta={report['delta_trigger_count']} replay_points={report['replay_points']}"
        )
        for unlocked in report["unlocked_windows"]:
            relaxed = unlocked["relaxed"]
            print(
                "unlocked {timestamp} price={price:.4f} obv={obv_slope_angle:.2f}/{obv_slope_threshold:.2f} score={score_total:.2f} reason={execution_trigger_reason}".format(
                    **relaxed,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
