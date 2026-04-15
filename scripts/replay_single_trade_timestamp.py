from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import load_config
from market_adaptive.indicators import compute_indicator_snapshot, ohlcv_to_dataframe
from market_adaptive.oracles.market_oracle import (
    MultiTimeframeMarketSnapshot,
    indicator_confirms_trend,
    snapshot_supports_short_regime_thaw,
)
from market_adaptive.strategies.cta_robot import CTARobot
from market_adaptive.strategies.mtf_engine import MultiTimeframeSignalEngine
from market_adaptive.timeframe_utils import maybe_use_closed_candles


def timeframe_to_minutes(raw: str) -> int:
    raw = str(raw).strip().lower()
    units = {"m": 1, "h": 60, "d": 1440}
    return int(raw[:-1]) * units[raw[-1]]


def timeframe_to_pandas_freq(raw: str) -> str:
    raw = str(raw).strip().lower()
    amount = int(raw[:-1])
    unit = raw[-1]
    mapping = {"m": "min", "h": "h", "d": "d"}
    return f"{amount}{mapping[unit]}"


def historical_fetch_df(client: OKXClient, symbol: str, timeframe: str, *, start_ms: int, end_ms: int, prefer_closed: bool, limit_per_call: int = 200) -> pd.DataFrame:
    rows: list[list[float]] = []
    cursor = int(start_ms)
    step_ms = timeframe_to_minutes(timeframe) * 60_000
    while cursor <= end_ms + step_ms:
        batch = client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=cursor, limit=limit_per_call)
        batch = maybe_use_closed_candles(batch, enabled=prefer_closed)
        if not batch:
            break
        fresh = [row for row in batch if int(row[0]) >= cursor]
        if not fresh:
            break
        rows.extend(fresh)
        last_ts = int(fresh[-1][0])
        next_cursor = last_ts + step_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if last_ts > end_ms + step_ms:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame = ohlcv_to_dataframe(rows).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return frame[(frame["timestamp"] >= pd.to_datetime(start_ms, unit="ms", utc=True)) & (frame["timestamp"] <= pd.to_datetime(end_ms, unit="ms", utc=True))].reset_index(drop=True)


def build_partial_execution_frame(one_min: pd.DataFrame, execution_timeframe: str, target_ts: pd.Timestamp, lookback_limit: int) -> pd.DataFrame:
    if one_min.empty:
        return one_min.copy()
    bucket_minutes = timeframe_to_minutes(execution_timeframe)
    freq = timeframe_to_pandas_freq(execution_timeframe)
    bucket_start = target_ts.floor(freq)
    completed_minute_cutoff = target_ts.floor("1min")
    completed = one_min[one_min["timestamp"] < completed_minute_cutoff].copy()
    if completed.empty:
        return pd.DataFrame(columns=one_min.columns)
    completed["bucket"] = completed["timestamp"].dt.floor(freq)
    grouped = completed.groupby("bucket", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index().rename(columns={"bucket": "timestamp"})

    partial_minutes = completed[completed["timestamp"] >= bucket_start].copy()
    if not partial_minutes.empty:
        partial = pd.DataFrame([
            {
                "timestamp": bucket_start,
                "open": float(partial_minutes["open"].iloc[0]),
                "high": float(partial_minutes["high"].max()),
                "low": float(partial_minutes["low"].min()),
                "close": float(partial_minutes["close"].iloc[-1]),
                "volume": float(partial_minutes["volume"].sum()),
            }
        ])
        grouped = grouped[grouped["timestamp"] < bucket_start]
        grouped = pd.concat([grouped, partial], ignore_index=True)
    return grouped.tail(lookback_limit).reset_index(drop=True)


class StaticReplayClient:
    def __init__(self, frames: dict[str, pd.DataFrame], base_client: OKXClient) -> None:
        self.frames = frames
        self.base_client = base_client

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h", limit: int = 200, since: int | None = None):
        del symbol, since
        frame = self.frames[timeframe].tail(limit)
        return [
            [
                int(pd.Timestamp(row.timestamp).value // 1_000_000),
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                float(row.volume),
            ]
            for row in frame.itertuples(index=False)
        ]

    def fetch_server_time(self):
        return None


def evaluate_timestamp(config_path: Path, target: str) -> dict:
    cfg = load_config(config_path)
    target_ts = pd.Timestamp(target, tz="UTC")
    client = OKXClient(cfg.okx, cfg.execution)
    cta = cfg.cta
    oracle = cfg.market_oracle
    symbol = cta.symbol

    lookback = max(cta.lookback_limit, oracle.lookback_limit)
    major_start_ms = int((target_ts - pd.Timedelta(hours=4 * (lookback + 5))).value // 1_000_000)
    swing_start_ms = int((target_ts - pd.Timedelta(hours=1 * (lookback + 5))).value // 1_000_000)
    execution_bucket_start = target_ts.floor(timeframe_to_pandas_freq(cta.execution_timeframe))
    one_min_start_ms = int((execution_bucket_start - pd.Timedelta(minutes=timeframe_to_minutes(cta.execution_timeframe) * (lookback + 5))).value // 1_000_000)
    end_ms = int(target_ts.floor("1min").value // 1_000_000)

    major_frame = historical_fetch_df(client, symbol, cta.major_timeframe, start_ms=major_start_ms, end_ms=end_ms, prefer_closed=cta.prefer_closed_major_timeframe_candles)
    swing_frame = historical_fetch_df(client, symbol, cta.swing_timeframe, start_ms=swing_start_ms, end_ms=end_ms, prefer_closed=cta.prefer_closed_swing_timeframe_candles)
    oracle_high_frame = historical_fetch_df(client, symbol, oracle.higher_timeframe, start_ms=swing_start_ms, end_ms=end_ms, prefer_closed=oracle.prefer_closed_higher_timeframe_candles)
    oracle_low_frame = historical_fetch_df(client, symbol, oracle.lower_timeframe, start_ms=one_min_start_ms, end_ms=end_ms, prefer_closed=oracle.prefer_closed_lower_timeframe_candles)
    one_min_frame = historical_fetch_df(client, symbol, "1m", start_ms=one_min_start_ms, end_ms=end_ms, prefer_closed=True)
    execution_frame = build_partial_execution_frame(one_min_frame, cta.execution_timeframe, target_ts, cta.lookback_limit)

    frames = {
        cta.major_timeframe: major_frame.tail(cta.lookback_limit).reset_index(drop=True),
        cta.swing_timeframe: swing_frame.tail(cta.lookback_limit).reset_index(drop=True),
        cta.execution_timeframe: execution_frame,
    }
    replay_client = StaticReplayClient(frames, client)
    signal = MultiTimeframeSignalEngine(replay_client, cta).build_signal()
    if signal is None:
        raise RuntimeError("unable to build signal from replayed frames")

    high_snapshot = compute_indicator_snapshot(oracle_high_frame.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
    low_snapshot = compute_indicator_snapshot(oracle_low_frame.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
    market_regime_passed = any(indicator_confirms_trend(item, oracle) for item in (high_snapshot, low_snapshot)) or snapshot_supports_short_regime_thaw(
        MultiTimeframeMarketSnapshot(
            symbol=str(symbol),
            higher_timeframe=str(oracle.higher_timeframe),
            lower_timeframe=str(oracle.lower_timeframe),
            higher=high_snapshot,
            lower=low_snapshot,
        ),
        oracle,
    )

    robot = CTARobot(client=replay_client, database=None, config=cta, execution_config=cfg.execution, notifier=None, risk_manager=None, sentiment_analyst=None)
    trend_signal = robot._build_trend_signal()
    if trend_signal is None:
        raise RuntimeError("unable to build CTA robot trend signal from replayed frames")

    return {
        "target_timestamp": target_ts.isoformat(),
        "execution_partial_bucket_start": execution_bucket_start.isoformat(),
        "included_1m_bars_in_partial": int(len(one_min_frame[(one_min_frame["timestamp"] >= execution_bucket_start) & (one_min_frame["timestamp"] < target_ts.floor("1min"))])),
        "note": "Partial 15m candle is built from completed 1m bars strictly before the target minute; no sub-minute/tick data is used.",
        "market_regime_passed": bool(market_regime_passed),
        "oracle_high_snapshot": asdict(high_snapshot),
        "oracle_low_snapshot": asdict(low_snapshot),
        "mtf_signal": {
            "major_direction": signal.major_direction,
            "weak_bull_bias": signal.weak_bull_bias,
            "early_bullish": signal.early_bullish,
            "bullish_score": signal.bullish_score,
            "bullish_threshold": signal.bullish_threshold,
            "bullish_ready": signal.bullish_ready,
            "fully_aligned": signal.fully_aligned,
            "execution_entry_mode": signal.execution_entry_mode,
            "execution_trigger_reason": signal.execution_trigger.reason,
            "execution_memory_active": signal.execution_trigger.bullish_memory_active,
            "execution_latch_active": signal.execution_trigger.bullish_latch_active,
            "execution_latch_price": signal.execution_trigger.latch_low_price,
            "execution_memory_bars_ago": signal.execution_trigger.bullish_cross_bars_ago,
            "execution_breakout": signal.execution_trigger.prior_high_break,
            "execution_frontrun_near_breakout": signal.execution_trigger.frontrun_near_breakout,
            "execution_frontrun_gap_ratio": signal.execution_trigger.frontrun_gap_ratio,
            "execution_obv_zscore": signal.execution_obv_zscore,
            "data_alignment_valid": signal.data_alignment_valid,
            "blocker_reason": signal.blocker_reason,
            "current_price": signal.current_price,
        },
        "cta_robot": {
            "would_open_long": bool(trend_signal.direction > 0),
            "raw_direction": trend_signal.raw_direction,
            "direction": trend_signal.direction,
            "long_setup_blocked": trend_signal.long_setup_blocked,
            "long_setup_reason": trend_signal.long_setup_reason,
            "blocker_reason": trend_signal.blocker_reason,
            "obv_confirmation_passed": trend_signal.obv_confirmation_passed,
            "volume_filter_passed": trend_signal.volume_filter_passed,
            "obv_above_sma": trend_signal.obv_confirmation.above_sma,
            "obv_zscore": trend_signal.obv_confirmation.zscore,
            "obv_threshold": trend_signal.obv_threshold,
            "execution_trigger_reason": trend_signal.execution_trigger_reason,
            "bullish_ready": trend_signal.bullish_ready,
            "bullish_score": trend_signal.bullish_score,
            "price": trend_signal.price,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    result = evaluate_timestamp(Path(args.config), args.target)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
