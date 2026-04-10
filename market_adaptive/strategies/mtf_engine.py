from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.indicators import compute_kdj, compute_rsi, compute_supertrend, ohlcv_to_dataframe


@dataclass
class ExecutionTriggerSnapshot:
    kdj_golden_cross: bool
    prior_high_break: bool
    prior_high: float | None
    reason: str


@dataclass
class MTFSignal:
    major_timeframe: str
    swing_timeframe: str
    execution_timeframe: str
    major_direction: int
    swing_rsi: float
    bullish_ready: bool
    execution_trigger: ExecutionTriggerSnapshot
    fully_aligned: bool
    current_price: float
    major_frame: pd.DataFrame
    swing_frame: pd.DataFrame
    execution_frame: pd.DataFrame


class MultiTimeframeSignalEngine:
    def __init__(self, client, config: CTAConfig) -> None:
        self.client = client
        self.config = config

    @property
    def minimum_bars(self) -> int:
        return max(
            self.config.supertrend_period * 3,
            self.config.swing_rsi_period * 3,
            self.config.kdj_length + max(self.config.kdj_k_smoothing, self.config.kdj_d_smoothing) + 2,
            self.config.execution_breakout_lookback + 2,
            self.config.atr_period * 3,
            self.config.obv_signal_period + 2,
            self.config.obv_slope_window + 2,
        )

    def build_signal(self) -> MTFSignal | None:
        execution_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.execution_timeframe,
            limit=self.config.lookback_limit,
        )
        swing_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.swing_timeframe,
            limit=self.config.lookback_limit,
        )
        major_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.major_timeframe,
            limit=self.config.lookback_limit,
        )

        if (
            len(execution_ohlcv) < self.minimum_bars
            or len(swing_ohlcv) < self.minimum_bars
            or len(major_ohlcv) < self.minimum_bars
        ):
            return None

        execution_frame = ohlcv_to_dataframe(execution_ohlcv)
        swing_frame = ohlcv_to_dataframe(swing_ohlcv)
        major_frame = ohlcv_to_dataframe(major_ohlcv)

        major_supertrend = compute_supertrend(
            major_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        swing_rsi = compute_rsi(swing_frame, length=self.config.swing_rsi_period)
        execution_kdj = compute_kdj(
            execution_frame,
            length=self.config.kdj_length,
            k_smoothing=self.config.kdj_k_smoothing,
            d_smoothing=self.config.kdj_d_smoothing,
        )

        major_direction = int(major_supertrend["direction"].iloc[-1])
        current_swing_rsi = float(swing_rsi.iloc[-1])
        bullish_ready = major_direction > 0 and current_swing_rsi > float(self.config.swing_rsi_ready_threshold)

        current_k = float(execution_kdj["k"].iloc[-1])
        current_d = float(execution_kdj["d"].iloc[-1])
        previous_k = float(execution_kdj["k"].iloc[-2])
        previous_d = float(execution_kdj["d"].iloc[-2])
        kdj_golden_cross = previous_k <= previous_d and current_k > current_d

        prior_high_series = execution_frame["high"].shift(1).rolling(
            max(1, int(self.config.execution_breakout_lookback)),
            min_periods=max(1, int(self.config.execution_breakout_lookback)),
        ).max()
        prior_high_value = prior_high_series.iloc[-1]
        prior_high = None if pd.isna(prior_high_value) else float(prior_high_value)
        current_price = float(execution_frame["close"].iloc[-1])
        prior_high_break = prior_high is not None and current_price > prior_high + 1e-12

        reasons: list[str] = []
        if kdj_golden_cross:
            reasons.append("kdj_golden_cross")
        if prior_high_break:
            reasons.append("prior_high_break")
        reason = "+".join(reasons) if reasons else "waiting_execution_trigger"

        execution_trigger = ExecutionTriggerSnapshot(
            kdj_golden_cross=kdj_golden_cross,
            prior_high_break=prior_high_break,
            prior_high=prior_high,
            reason=reason,
        )
        fully_aligned = bullish_ready and (kdj_golden_cross or prior_high_break)

        return MTFSignal(
            major_timeframe=self.config.major_timeframe,
            swing_timeframe=self.config.swing_timeframe,
            execution_timeframe=self.config.execution_timeframe,
            major_direction=major_direction,
            swing_rsi=current_swing_rsi,
            bullish_ready=bullish_ready,
            execution_trigger=execution_trigger,
            fully_aligned=fully_aligned,
            current_price=current_price,
            major_frame=major_frame,
            swing_frame=swing_frame,
            execution_frame=execution_frame,
        )
