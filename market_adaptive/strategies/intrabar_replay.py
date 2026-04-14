from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.indicators import ohlcv_to_dataframe
from market_adaptive.strategies.mtf_engine import MTFSignal, MultiTimeframeSignalEngine


@dataclass
class IntrabarReplayFrames:
    major: pd.DataFrame
    swing: pd.DataFrame
    execution: pd.DataFrame
    intrabar: pd.DataFrame


class _IntrabarReplayClient:
    def __init__(self, *, symbol: str, config: CTAConfig, frames: IntrabarReplayFrames, evaluation_ts: pd.Timestamp) -> None:
        self.symbol = symbol
        self.config = config
        self.frames = frames
        self.evaluation_ts = pd.Timestamp(evaluation_ts)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200):
        del symbol
        frame = self._frame_for_timeframe(timeframe).tail(limit)
        if frame.empty:
            return []
        return frame[["timestamp", "open", "high", "low", "close", "volume"]].values.tolist()

    def fetch_server_time(self) -> int:
        return int(self.evaluation_ts.value // 1_000_000)

    def _frame_for_timeframe(self, timeframe: str) -> pd.DataFrame:
        if timeframe == self.config.major_timeframe:
            return self.frames.major[self.frames.major["timestamp"] <= self.evaluation_ts].copy()
        if timeframe == self.config.swing_timeframe:
            return self.frames.swing[self.frames.swing["timestamp"] <= self.evaluation_ts].copy()
        if timeframe == self.config.execution_timeframe:
            return build_execution_replay_frame(
                execution_frame=self.frames.execution,
                intrabar_frame=self.frames.intrabar,
                evaluation_ts=self.evaluation_ts,
                execution_timeframe=self.config.execution_timeframe,
            )
        raise ValueError(f"Unsupported timeframe: {timeframe}")


def timeframe_to_timedelta(timeframe: str) -> pd.Timedelta:
    raw = str(timeframe).strip().lower()
    unit = raw[-1]
    value = int(raw[:-1])
    if unit == "m":
        return pd.Timedelta(minutes=value)
    if unit == "h":
        return pd.Timedelta(hours=value)
    if unit == "d":
        return pd.Timedelta(days=value)
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def floor_timestamp_to_timeframe(timestamp: pd.Timestamp, timeframe: str) -> pd.Timestamp:
    raw = str(timeframe).strip().lower()
    unit = raw[-1]
    value = int(raw[:-1])
    if unit == "m":
        return timestamp.floor(f"{value}min")
    if unit == "h":
        return timestamp.floor(f"{value}h")
    if unit == "d":
        return timestamp.floor(f"{value}d")
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def build_partial_candle(intrabar_frame: pd.DataFrame) -> pd.DataFrame:
    if intrabar_frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    first = intrabar_frame.iloc[0]
    last = intrabar_frame.iloc[-1]
    return pd.DataFrame(
        [
            {
                "timestamp": last["timestamp"],
                "open": float(first["open"]),
                "high": float(intrabar_frame["high"].max()),
                "low": float(intrabar_frame["low"].min()),
                "close": float(last["close"]),
                "volume": float(intrabar_frame["volume"].sum()),
            }
        ]
    )


def build_execution_replay_frame(
    *,
    execution_frame: pd.DataFrame,
    intrabar_frame: pd.DataFrame,
    evaluation_ts: pd.Timestamp,
    execution_timeframe: str,
) -> pd.DataFrame:
    evaluation_ts = pd.Timestamp(evaluation_ts)
    bucket_start = floor_timestamp_to_timeframe(evaluation_ts, execution_timeframe)
    closed = execution_frame[execution_frame["timestamp"] < bucket_start].copy()
    current_intrabar = intrabar_frame[
        (intrabar_frame["timestamp"] >= bucket_start) & (intrabar_frame["timestamp"] <= evaluation_ts)
    ].copy()
    if current_intrabar.empty:
        return closed
    partial = build_partial_candle(current_intrabar)
    return pd.concat([closed, partial], ignore_index=True)


def replay_signal_at_timestamp(*, config: CTAConfig, frames: IntrabarReplayFrames, evaluation_ts: pd.Timestamp) -> MTFSignal | None:
    client = _IntrabarReplayClient(symbol=config.symbol, config=config, frames=frames, evaluation_ts=evaluation_ts)
    engine = MultiTimeframeSignalEngine(client, config)
    return engine.build_signal()


def replay_signal_with_intrabar_scan(*, config: CTAConfig, frames: IntrabarReplayFrames, target_bar_ts: pd.Timestamp) -> list[MTFSignal]:
    bucket_start = pd.Timestamp(target_bar_ts)
    bucket_end = bucket_start + timeframe_to_timedelta(config.execution_timeframe)
    intrabar_rows = frames.intrabar[
        (frames.intrabar["timestamp"] >= bucket_start) & (frames.intrabar["timestamp"] < bucket_end)
    ].copy()
    signals: list[MTFSignal] = []
    for _, row in intrabar_rows.iterrows():
        signal = replay_signal_at_timestamp(config=config, frames=frames, evaluation_ts=row["timestamp"])
        if signal is not None:
            signals.append(signal)
    return signals
