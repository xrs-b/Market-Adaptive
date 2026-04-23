from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.strategies.cta_robot import CTARobot, TrendSignal
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


def build_partial_candle(*, intrabar_frame: pd.DataFrame, candle_timestamp: pd.Timestamp) -> pd.DataFrame:
    if intrabar_frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    first = intrabar_frame.iloc[0]
    last = intrabar_frame.iloc[-1]
    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp(candle_timestamp),
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
    completed_minute_cutoff = evaluation_ts.floor("1min")
    closed = execution_frame[execution_frame["timestamp"] < bucket_start].copy()
    current_intrabar = intrabar_frame[
        (intrabar_frame["timestamp"] >= bucket_start) & (intrabar_frame["timestamp"] < completed_minute_cutoff)
    ].copy()
    if current_intrabar.empty:
        return closed.reset_index(drop=True)
    partial = build_partial_candle(intrabar_frame=current_intrabar, candle_timestamp=bucket_start)
    return pd.concat([closed, partial], ignore_index=True)


def replay_signal_at_timestamp(*, config: CTAConfig, frames: IntrabarReplayFrames, evaluation_ts: pd.Timestamp) -> MTFSignal | None:
    client = _IntrabarReplayClient(symbol=config.symbol, config=config, frames=frames, evaluation_ts=evaluation_ts)
    engine = MultiTimeframeSignalEngine(client, config)
    return engine.build_signal()


def _build_replay_robot(*, client: _IntrabarReplayClient, config: CTAConfig, execution_config: Any | None) -> CTARobot:
    if execution_config is None:
        execution_config = SimpleNamespace(cta_order_size=0.01)
    return CTARobot(
        client=client,
        database=None,
        config=config,
        execution_config=execution_config,
        notifier=None,
        risk_manager=None,
        sentiment_analyst=None,
    )


def replay_trend_signal_at_timestamp(*, config: CTAConfig, frames: IntrabarReplayFrames, evaluation_ts: pd.Timestamp, execution_config: Any | None = None) -> TrendSignal | None:
    client = _IntrabarReplayClient(symbol=config.symbol, config=config, frames=frames, evaluation_ts=evaluation_ts)
    robot = _build_replay_robot(client=client, config=config, execution_config=execution_config)
    return robot._build_trend_signal()


class _IntrabarReplayExecutionClient(_IntrabarReplayClient):
    def __init__(self, *, symbol: str, config: CTAConfig, frames: IntrabarReplayFrames, evaluation_ts: pd.Timestamp, last_price: float | None = None) -> None:
        super().__init__(symbol=symbol, config=config, frames=frames, evaluation_ts=evaluation_ts)
        current_frame = self._frame_for_timeframe(config.execution_timeframe)
        inferred_price = float(current_frame["close"].iloc[-1]) if not current_frame.empty else 0.0
        self.last_price = float(last_price if last_price is not None else inferred_price)
        self.market_orders: list[dict[str, Any]] = []
        self.limit_orders: list[dict[str, Any]] = []

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return self.last_price

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        del quote_currency
        return 10_000.0

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return 0.0

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(float(amount)) * abs(float(price))

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 8)

    def fetch_order_book(self, symbol: str, limit: int | None = None):
        del symbol, limit
        bid = max(self.last_price - 0.1, 0.0)
        ask = self.last_price + 0.1
        return {
            "bids": [[bid - (index * 0.1), 5.0] for index in range(20)],
            "asks": [[ask + (index * 0.1), 5.0] for index in range(20)],
        }

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {
            "id": f"replay-market-{len(self.market_orders) + 1}",
            "symbol": symbol,
            "side": side,
            "amount": float(amount),
            "filled": float(amount),
            "average": float(self.last_price),
            "price": float(self.last_price),
            "remaining": 0.0,
            "status": "closed",
            **kwargs,
        }
        self.market_orders.append(payload)
        return payload

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        payload = {
            "id": f"replay-limit-{len(self.limit_orders) + 1}",
            "symbol": symbol,
            "side": side,
            "amount": float(amount),
            "price": float(price),
            "filled": float(amount),
            "average": float(price),
            "remaining": 0.0,
            "status": "closed",
            **kwargs,
        }
        self.limit_orders.append(payload)
        return payload

    def fetch_order(self, order_id: str, symbol: str):
        del symbol
        for order in self.limit_orders:
            if order.get("id") == order_id:
                return dict(order)
        return None


def replay_open_position_at_timestamp(
    *,
    config: CTAConfig,
    frames: IntrabarReplayFrames,
    evaluation_ts: pd.Timestamp,
    execution_config: Any | None = None,
) -> tuple[TrendSignal | None, str, CTARobot]:
    client = _IntrabarReplayExecutionClient(symbol=config.symbol, config=config, frames=frames, evaluation_ts=evaluation_ts)
    robot = _build_replay_robot(client=client, config=config, execution_config=execution_config)
    signal = robot._build_trend_signal()
    if signal is None or signal.direction == 0:
        return signal, "cta:no_signal", robot
    return signal, robot._open_position(signal), robot


def replay_signal_with_intrabar_scan(*, config: CTAConfig, frames: IntrabarReplayFrames, target_bar_ts: pd.Timestamp) -> list[MTFSignal]:
    bucket_start = pd.Timestamp(target_bar_ts)
    bucket_end = bucket_start + timeframe_to_timedelta(config.execution_timeframe)
    intrabar_rows = frames.intrabar[
        (frames.intrabar["timestamp"] >= bucket_start) & (frames.intrabar["timestamp"] < bucket_end)
    ].copy()
    signals: list[MTFSignal] = []
    for _, row in intrabar_rows.iterrows():
        signal = replay_signal_at_timestamp(
            config=config,
            frames=frames,
            evaluation_ts=pd.Timestamp(row["timestamp"]) + pd.Timedelta(minutes=1),
        )
        if signal is not None:
            signals.append(signal)
    return signals


def replay_trend_signal_with_intrabar_scan(*, config: CTAConfig, frames: IntrabarReplayFrames, target_bar_ts: pd.Timestamp, execution_config: Any | None = None) -> list[TrendSignal]:
    bucket_start = pd.Timestamp(target_bar_ts)
    bucket_end = bucket_start + timeframe_to_timedelta(config.execution_timeframe)
    intrabar_rows = frames.intrabar[
        (frames.intrabar["timestamp"] >= bucket_start) & (frames.intrabar["timestamp"] < bucket_end)
    ].copy()
    signals: list[TrendSignal] = []
    for _, row in intrabar_rows.iterrows():
        signal = replay_trend_signal_at_timestamp(
            config=config,
            frames=frames,
            evaluation_ts=pd.Timestamp(row["timestamp"]) + pd.Timedelta(minutes=1),
            execution_config=execution_config,
        )
        if signal is not None:
            signals.append(signal)
    return signals
