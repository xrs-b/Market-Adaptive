from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.indicators import (
    compute_kdj,
    compute_obv,
    compute_obv_confirmation_snapshot,
    compute_rsi,
    compute_supertrend,
    ohlcv_to_dataframe,
)

logger = logging.getLogger(__name__)


@dataclass
class ExecutionTriggerSnapshot:
    kdj_golden_cross: bool
    kdj_dead_cross: bool
    bullish_memory_active: bool
    bearish_memory_active: bool
    bullish_cross_bars_ago: int | None
    bearish_cross_bars_ago: int | None
    prior_high_break: bool
    prior_low_break: bool
    prior_high: float | None
    prior_low: float | None
    reason: str


@dataclass
class MTFSignal:
    major_timeframe: str
    swing_timeframe: str
    execution_timeframe: str
    major_direction: int
    major_bias_score: float
    weak_bull_bias: bool
    swing_rsi: float
    swing_rsi_slope: float
    bullish_score: float
    bullish_threshold: float
    bullish_ready: bool
    execution_entry_mode: str
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

    @staticmethod
    def _bars_since_last_true(mask: pd.Series, max_bars: int) -> int | None:
        recent = mask.fillna(False).astype(bool).tail(max(1, int(max_bars)))
        reversed_values = list(reversed(recent.tolist()))
        for bars_ago, value in enumerate(reversed_values):
            if value:
                return bars_ago
        return None

    @property
    def minimum_bars(self) -> int:
        return max(
            self.config.supertrend_period * 3,
            self.config.swing_rsi_period * 4,
            self.config.kdj_length + max(self.config.kdj_k_smoothing, self.config.kdj_d_smoothing) + 2,
            self.config.execution_breakout_lookback + 2,
            self.config.atr_period * 3,
            self.config.obv_signal_period + 2,
            self.config.obv_signal_window + 2,
            self.config.weak_bias_slow_ema + 3,
            self.config.rsi_rebound_lookback + 3,
        )

    def _resolve_major_bias(self, major_direction: int, swing_frame: pd.DataFrame) -> tuple[float, bool]:
        if major_direction > 0:
            return float(self.config.strong_bull_bias_score), False

        swing_supertrend = compute_supertrend(
            swing_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        price_above_swing_supertrend = float(swing_frame["close"].iloc[-1]) > float(swing_supertrend["supertrend"].iloc[-1])
        fast_ema = swing_frame["close"].ewm(span=self.config.weak_bias_fast_ema, adjust=False).mean()
        slow_ema = swing_frame["close"].ewm(span=self.config.weak_bias_slow_ema, adjust=False).mean()
        ema_converged_bullish = float(fast_ema.iloc[-1]) > float(slow_ema.iloc[-1])
        weak_bull_bias = price_above_swing_supertrend and ema_converged_bullish
        return (float(self.config.weak_bull_bias_score) if weak_bull_bias else 0.0), weak_bull_bias

    def _resolve_swing_readiness(self, swing_rsi: pd.Series) -> tuple[float, float]:
        current_rsi = float(swing_rsi.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        if current_rsi >= float(self.config.swing_rsi_ready_threshold):
            return float(self.config.dynamic_rsi_trend_score), rsi_slope
        if current_rsi >= float(self.config.dynamic_rsi_floor) and rsi_slope > 0:
            return float(self.config.dynamic_rsi_trend_score), rsi_slope

        rebound_window = max(2, int(self.config.rsi_rebound_lookback))
        recent_min_rsi = float(swing_rsi.tail(rebound_window).min())
        oversold_rebound = (
            recent_min_rsi < float(self.config.rsi_oversold_threshold)
            and current_rsi >= float(self.config.rsi_rebound_confirmation_level)
            and rsi_slope > 0
        )
        if oversold_rebound:
            return float(self.config.dynamic_rsi_rebound_score), rsi_slope
        return 0.0, rsi_slope

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
        execution_obv = compute_obv(execution_frame)
        execution_obv_confirmation = compute_obv_confirmation_snapshot(
            execution_frame,
            obv=execution_obv,
            sma_period=self.config.obv_sma_period,
            zscore_window=self.config.obv_zscore_window,
        )

        major_direction = int(major_supertrend["direction"].iloc[-1])
        current_swing_rsi = float(swing_rsi.iloc[-1])
        major_bias_score, weak_bull_bias = self._resolve_major_bias(major_direction, swing_frame)
        swing_score, swing_rsi_slope = self._resolve_swing_readiness(swing_rsi)
        bullish_score = major_bias_score + swing_score
        bullish_ready = bullish_score >= float(self.config.bullish_ready_score_threshold)

        current_k = float(execution_kdj["k"].iloc[-1])
        current_d = float(execution_kdj["d"].iloc[-1])
        previous_k = float(execution_kdj["k"].iloc[-2])
        previous_d = float(execution_kdj["d"].iloc[-2])
        kdj_golden_cross = previous_k <= previous_d and current_k > current_d
        kdj_dead_cross = previous_k >= previous_d and current_k < current_d

        bullish_cross_mask = (execution_kdj["k"].shift(1) <= execution_kdj["d"].shift(1)) & (execution_kdj["k"] > execution_kdj["d"])
        bearish_cross_mask = (execution_kdj["k"].shift(1) >= execution_kdj["d"].shift(1)) & (execution_kdj["k"] < execution_kdj["d"])
        memory_bars = max(1, int(self.config.kdj_signal_memory_bars))

        bullish_cross_bars_ago = self._bars_since_last_true(bullish_cross_mask, memory_bars)
        bearish_cross_bars_ago = self._bars_since_last_true(bearish_cross_mask, memory_bars)
        bullish_memory_active = bullish_cross_bars_ago is not None
        bearish_memory_active = bearish_cross_bars_ago is not None

        prior_high_series = execution_frame["high"].shift(1).rolling(
            max(1, int(self.config.execution_breakout_lookback)),
            min_periods=max(1, int(self.config.execution_breakout_lookback)),
        ).max()
        prior_low_series = execution_frame["low"].shift(1).rolling(
            max(1, int(self.config.execution_breakout_lookback)),
            min_periods=max(1, int(self.config.execution_breakout_lookback)),
        ).min()
        prior_high_value = prior_high_series.iloc[-1]
        prior_low_value = prior_low_series.iloc[-1]
        prior_high = None if pd.isna(prior_high_value) else float(prior_high_value)
        prior_low = None if pd.isna(prior_low_value) else float(prior_low_value)
        current_price = float(execution_frame["close"].iloc[-1])
        prior_high_break = prior_high is not None and current_price > prior_high + 1e-12
        prior_low_break = prior_low is not None and current_price < prior_low - 1e-12

        current_major_atr = float(major_supertrend["atr"].iloc[-1])
        relevant_rail = float(major_supertrend["upper_band"].iloc[-1] if major_direction <= 0 else major_supertrend["lower_band"].iloc[-1])
        rail_distance = abs(current_price - relevant_rail)
        magnetism_limit = float(self.config.magnetism_rail_atr_multiplier) * current_major_atr
        magnetism_distance_pct = (rail_distance / relevant_rail * 100.0) if abs(relevant_rail) > 1e-12 else 0.0
        magnetism_obv_ready = execution_obv_confirmation.zscore > float(self.config.magnetism_obv_zscore_threshold)
        bullish_magnetism_ready = (
            major_direction <= 0
            and not bullish_ready
            and current_major_atr > 0.0
            and rail_distance < magnetism_limit
            and magnetism_obv_ready
        )
        if bullish_magnetism_ready:
            bullish_ready = True
            bullish_score = max(bullish_score, float(self.config.bullish_ready_score_threshold))
            logger.info(
                "磁吸力预判：距离轨道 %.3f%%，OBV 已确认 | symbol=%s timeframe=%s rail=%.4f price=%.4f atr=%.4f obv_z=%.2f",
                magnetism_distance_pct,
                self.config.symbol,
                self.config.major_timeframe,
                relevant_rail,
                current_price,
                current_major_atr,
                float(execution_obv_confirmation.zscore),
            )

        execution_entry_mode = "breakout_confirmed"
        if weak_bull_bias:
            execution_entry_mode = "weak_bull_scale_in_limit"

        if bullish_memory_active and prior_high_break:
            reason = f"Triggered via Memory Window: KDJ crossed {bullish_cross_bars_ago} bars ago + Price Breakout NOW"
        elif weak_bull_bias and bullish_memory_active:
            reason = f"Weak bull bias active: KDJ crossed {bullish_cross_bars_ago} bars ago + scale-in allowed before breakout"
        elif bullish_magnetism_ready:
            reason = f"磁吸力预判：距离轨道 {magnetism_distance_pct:.3f}%，OBV 已确认"
        elif kdj_golden_cross:
            reason = "kdj_golden_cross_waiting_breakout"
        elif prior_high_break:
            reason = "prior_high_break_waiting_kdj_memory"
        else:
            reason = "waiting_execution_trigger"

        execution_trigger = ExecutionTriggerSnapshot(
            kdj_golden_cross=kdj_golden_cross,
            kdj_dead_cross=kdj_dead_cross,
            bullish_memory_active=bullish_memory_active,
            bearish_memory_active=bearish_memory_active,
            bullish_cross_bars_ago=bullish_cross_bars_ago,
            bearish_cross_bars_ago=bearish_cross_bars_ago,
            prior_high_break=prior_high_break,
            prior_low_break=prior_low_break,
            prior_high=prior_high,
            prior_low=prior_low,
            reason=reason,
        )
        fully_aligned = bullish_ready and (
            (weak_bull_bias and bullish_memory_active)
            or (not weak_bull_bias and prior_high_break and (bullish_memory_active or kdj_golden_cross))
        )

        return MTFSignal(
            major_timeframe=self.config.major_timeframe,
            swing_timeframe=self.config.swing_timeframe,
            execution_timeframe=self.config.execution_timeframe,
            major_direction=major_direction,
            major_bias_score=major_bias_score,
            weak_bull_bias=weak_bull_bias,
            swing_rsi=current_swing_rsi,
            swing_rsi_slope=swing_rsi_slope,
            bullish_score=bullish_score,
            bullish_threshold=float(self.config.bullish_ready_score_threshold),
            bullish_ready=bullish_ready,
            execution_entry_mode=execution_entry_mode,
            execution_trigger=execution_trigger,
            fully_aligned=fully_aligned,
            current_price=current_price,
            major_frame=major_frame,
            swing_frame=swing_frame,
            execution_frame=execution_frame,
        )
