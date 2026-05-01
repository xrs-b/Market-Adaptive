from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.indicators import (
    compute_kdj,
    compute_obv,
    compute_obv_confirmation_snapshot,
    compute_rsi,
    compute_supertrend,
    ohlcv_to_dataframe,
    compute_atr,
    compute_volume_profile,
)
from market_adaptive.timeframe_utils import maybe_use_closed_candles

logger = logging.getLogger(__name__)


class SignalQualityTier(Enum):
    """Signal quality tier derived from score strength + readiness."""

    TIER_HIGH = 1
    TIER_MEDIUM = 2
    TIER_LOW = 3


@dataclass(frozen=True)
class EnhancedSignalScore:
    base_score: float
    strength_bonus: float
    total_score: float
    quality_tier: SignalQualityTier
    confidence: float
    rejection_reason: str = ""


def classify_trigger_group(family: str) -> str:
    family_value = str(family or "waiting")
    if family_value.startswith("waiting_") or family_value == "waiting":
        return "waiting"
    if family_value.startswith("early_"):
        return "early"
    if family_value in {"bullish_memory_breakout", "bearish_memory_breakdown"}:
        return "confirmed"
    if family_value in {"bearish_retest", "major_bull_retest"}:
        return "retest"
    if family_value in {"trend_continuation_near_breakout", "major_bull_impulse_reclaim"}:
        return "continuation"
    if family_value in {"price_led_override", "soft_latch_breakout"}:
        return "override"
    if family_value in {"weak_bull_scale_in", "weak_bear_scale_in"}:
        return "scale_in"
    if family_value in {"starter_frontrun", "starter_short_frontrun", "rail_momentum", "magnetism"}:
        return "momentum"
    return "other"


def resolve_execution_trigger_proximity_budget_ratio(*, starter_frontrun_breakout_buffer_ratio: float, bullish_memory_retest_breakout_buffer_ratio: float) -> float:
    return max(
        float(starter_frontrun_breakout_buffer_ratio),
        float(bullish_memory_retest_breakout_buffer_ratio),
    )


def classify_waiting_execution_trigger(
    *,
    bullish_ready: bool,
    state_label: str,
    bullish_memory_active: bool,
    bullish_latch_active: bool,
    bullish_urgency_active: bool,
    prior_high_break: bool,
    frontrun_near_breakout: bool,
    frontrun_gap_ratio: float,
    execution_trigger_proximity_budget_ratio: float,
) -> str:
    if not bullish_ready:
        return "waiting_execution_trigger"
    if frontrun_near_breakout or state_label == "ARMED_READY" or prior_high_break:
        return "waiting_execution_trigger_near_breakout"
    stale_execution_memory = bool(
        frontrun_gap_ratio > max(0.0, float(execution_trigger_proximity_budget_ratio))
    )
    if (bullish_memory_active or bullish_latch_active or bullish_urgency_active) and not stale_execution_memory:
        return "waiting_execution_trigger_memory_desync"
    return "waiting_execution_trigger_drift"


@dataclass
class TimeframeAlignmentCheck:
    major_timestamp_ms: int
    swing_timestamp_ms: int
    execution_timestamp_ms: int
    max_gap_ms: int
    valid: bool


@dataclass(frozen=True)
class LiquiditySweepSnapshot:
    detected: bool = False
    bullish: bool = False
    bearish: bool = False
    reference_price: float | None = None
    sweep_distance_ratio: float = 0.0
    close_reclaim_ratio: float = 0.0
    volume_ratio: float = 0.0
    trigger_label: str = ""


@dataclass
class ExecutionTriggerSnapshot:
    kdj_golden_cross: bool
    kdj_dead_cross: bool
    bullish_memory_active: bool
    bearish_memory_active: bool
    bullish_cross_bars_ago: int | None
    bearish_cross_bars_ago: int | None
    bullish_latch_active: bool = False
    bearish_latch_active: bool = False
    latch_low_price: float | None = None
    latch_high_price: float | None = None
    prior_high_break: bool = False
    prior_low_break: bool = False
    prior_high: float | None = None
    prior_low: float | None = None
    frontrun_gap_ratio: float = 0.0
    bullish_urgency_active: bool = False
    bullish_urgency_decay_step: int | None = None
    frontrun_near_breakout: bool = False
    frontrun_impulse_confirmed: bool = False
    frontrun_obv_confirmed: bool = False
    frontrun_ready: bool = False
    pullback_near_support: bool = False
    pullback_near_resistance: bool = False
    pullback_depth_ratio: float = 0.0
    pullback_resistance_depth_ratio: float = 0.0
    stretch_value: float = 0.0
    stretch_blocked: bool = False
    pending_retest: bool = False
    exhaustion_penalty_applied: bool = False
    state_label: str = "WAITING_SETUP"
    family: str = "waiting"
    group: str = "waiting"
    reason: str = ""
    liquidity_sweep: bool = False
    liquidity_sweep_side: str = ""


@dataclass
class OIFundingSnapshot:
    oi_change_pct: float = 0.0
    funding_rate: float = 0.0
    is_short_squeeze: bool = False
    is_long_liquidation: bool = False


@dataclass
class MTFSignal:
    major_timeframe: str
    swing_timeframe: str
    execution_timeframe: str
    major_direction: int
    major_bias_score: float
    weak_bull_bias: bool
    early_bullish: bool
    entry_size_multiplier: float
    swing_rsi: float
    swing_rsi_slope: float
    bullish_score: float
    bullish_threshold: float
    bullish_ready: bool
    execution_entry_mode: str
    execution_trigger: ExecutionTriggerSnapshot
    fully_aligned: bool
    current_price: float
    execution_obv_zscore: float
    execution_obv_threshold: float
    execution_atr: float
    atr_price_ratio_pct: float
    server_time_iso: str
    local_time_iso: str
    server_local_skew_ms: int | None
    major_timestamp_ms: int
    swing_timestamp_ms: int
    execution_timestamp_ms: int
    data_alignment_valid: bool
    data_mismatch_ms: int
    blocker_reason: str
    major_frame: pd.DataFrame
    swing_frame: pd.DataFrame
    execution_frame: pd.DataFrame
    rsi_blocking_overridden: bool = False
    weak_bear_bias: bool = False
    early_bearish: bool = False
    bearish_score: float = 0.0
    bearish_threshold: float = 0.0
    bearish_ready: bool = False
    signal_quality_tier: SignalQualityTier = SignalQualityTier.TIER_LOW
    signal_confidence: float = 0.0
    signal_strength_bonus: float = 0.0
    stretch_value: float = 0.0
    stretch_blocked: bool = False
    pending_retest: bool = False
    exhaustion_penalty_applied: bool = False
    oi_change_pct: float = 0.0
    funding_rate: float = 0.0
    is_short_squeeze: bool = False
    is_long_liquidation: bool = False


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


    @staticmethod
    def _count_bars_since_direction_change(direction_series: pd.Series) -> int:
        current_dir = int(direction_series.iloc[-1])
        for i in range(len(direction_series) - 2, -1, -1):
            if int(direction_series.iloc[i]) != current_dir:
                return len(direction_series) - 1 - i
        return len(direction_series)

    @staticmethod
    def _timeframe_to_milliseconds(timeframe: str) -> int:
        units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
        raw = str(timeframe).strip().lower()
        if len(raw) < 2 or raw[-1] not in units:
            return 900_000
        try:
            return max(1, int(raw[:-1])) * units[raw[-1]]
        except ValueError:
            return 900_000

    def _major_bull_retest_near_breakout_ready(
        self,
        *,
        major_direction: int,
        bullish_ready: bool,
        bullish_memory_active: bool,
        frontrun_gap_ratio: float,
    ) -> bool:
        if major_direction <= 0 or not bullish_ready or not bullish_memory_active:
            return False
        tolerance_ratio = resolve_execution_trigger_proximity_budget_ratio(
            starter_frontrun_breakout_buffer_ratio=float(getattr(self.config, "starter_frontrun_breakout_buffer_ratio", 0.002)),
            bullish_memory_retest_breakout_buffer_ratio=float(getattr(self.config, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
        )
        return frontrun_gap_ratio <= tolerance_ratio

    def _trend_continuation_near_breakout_ready(
        self,
        *,
        major_direction: int,
        bullish_score: float,
        frontrun_near_breakout: bool,
        prior_high_break: bool,
        kdj_dead_cross: bool,
        execution_obv_ready: bool,
        execution_obv_zscore: float,
    ) -> bool:
        if major_direction <= 0 or float(bullish_score) < 75.0:
            return False
        if not frontrun_near_breakout or prior_high_break or kdj_dead_cross:
            return False
        return bool(execution_obv_ready or float(execution_obv_zscore) > 0.0)

    @staticmethod
    def _compute_price_stretch(*, current_price: float, anchor_price: float, atr_value: float, bullish: bool) -> float:
        if atr_value <= 1e-12 or anchor_price <= 1e-12:
            return 0.0
        if bullish:
            return float((current_price - anchor_price) / atr_value)
        return float((anchor_price - current_price) / atr_value)

    def _compute_exhaustion_penalty(
        self,
        *,
        execution_frame: pd.DataFrame,
        swing_rsi: pd.Series,
        bullish: bool,
        volatility_squeeze_breakout: bool,
    ) -> tuple[float, str, bool]:
        penalty = float(getattr(self.config, "exhaustion_penalty_score", 0.0))
        if volatility_squeeze_breakout or penalty <= 0.0:
            return 0.0, "", False
        lookback = max(3, int(getattr(self.config, "exhaustion_lookback_bars", 5)))
        min_trend_bars = max(2, int(getattr(self.config, "exhaustion_min_trend_bars", 4)))
        rsi_bars = max(2, int(getattr(self.config, "exhaustion_rsi_bars", 3)))
        if len(execution_frame) < lookback or len(swing_rsi) < rsi_bars:
            return 0.0, "", False

        recent = execution_frame.tail(lookback)
        close_diff = recent["close"].diff()
        trend_bars = int((close_diff > 0).sum()) if bullish else int((close_diff < 0).sum())
        if trend_bars < min_trend_bars:
            return 0.0, "", False

        recent_rsi = swing_rsi.tail(rsi_bars)
        rsi_extreme = bool(
            (recent_rsi > float(getattr(self.config, "exhaustion_rsi_long_threshold", 70.0))).all()
            if bullish
            else (recent_rsi < float(getattr(self.config, "exhaustion_rsi_short_threshold", 30.0))).all()
        )
        if not rsi_extreme:
            return 0.0, "", False

        reason = (
            f"Exhaustion_Risk: trend_bars={trend_bars}/{lookback} rsi_extreme={rsi_bars}"
        )
        return penalty, reason, True

    @staticmethod
    def _resolve_directional_latch(
        *,
        cross_mask: pd.Series,
        reverse_cross_mask: pd.Series,
        execution_frame: pd.DataFrame,
        defended_column: str,
        violation_column: str,
        bullish: bool,
    ) -> tuple[bool, float | None, int | None]:
        recent_crosses = cross_mask.fillna(False).astype(bool)
        if recent_crosses.empty or not recent_crosses.any():
            return False, None, None
        cross_positions = [idx for idx, value in enumerate(recent_crosses.tolist()) if value]
        cross_position = cross_positions[-1]
        reverse_crosses = reverse_cross_mask.fillna(False).astype(bool)
        reverse_positions = [idx for idx, value in enumerate(reverse_crosses.tolist()) if value]
        if reverse_positions and reverse_positions[-1] > cross_position:
            return False, None, None
        defended_price = float(execution_frame[defended_column].iloc[cross_position])
        trailing_slice = execution_frame[violation_column].iloc[cross_position + 1 :]
        epsilon = 1e-12
        if bullish:
            violated = bool((trailing_slice < defended_price - epsilon).any())
        else:
            violated = bool((trailing_slice > defended_price + epsilon).any())
        if violated:
            return False, defended_price, len(execution_frame) - 1 - cross_position
        return True, defended_price, len(execution_frame) - 1 - cross_position

    @staticmethod
    def _detect_liquidity_sweep(
        *,
        execution_frame: pd.DataFrame,
        reference_price: float | None,
        atr_value: float,
        bullish: bool,
        breakout_lookback: int,
        min_wick_ratio: float,
        min_reclaim_ratio: float,
        min_volume_ratio: float,
        max_atr_excursion: float,
    ) -> LiquiditySweepSnapshot:
        if reference_price is None or reference_price <= 0.0 or atr_value <= 1e-12 or len(execution_frame) < max(5, breakout_lookback + 2):
            return LiquiditySweepSnapshot()

        candle = execution_frame.iloc[-1]
        volume_window = execution_frame["volume"].iloc[-max(2, breakout_lookback + 1) : -1]
        avg_volume = float(volume_window.mean()) if not volume_window.empty else 0.0
        current_volume = float(candle["volume"])
        volume_ratio = current_volume / max(avg_volume, 1e-12) if avg_volume > 0.0 else 0.0
        high = float(candle["high"])
        low = float(candle["low"])
        open_price = float(candle["open"])
        close = float(candle["close"])
        candle_range = max(high - low, 1e-12)

        if bullish:
            sweep_distance = max(0.0, reference_price - low)
            reclaim_distance = max(0.0, close - reference_price)
            lower_wick = max(0.0, min(open_price, close) - low)
            wick_ratio = lower_wick / candle_range
            reclaim_ratio = reclaim_distance / candle_range
            body_confirmation = close > open_price
            detected = bool(
                sweep_distance > 0.0
                and close > reference_price
                and body_confirmation
                and wick_ratio >= min_wick_ratio
                and reclaim_ratio >= min_reclaim_ratio
                and volume_ratio >= min_volume_ratio
                and (sweep_distance / atr_value) <= max_atr_excursion
            )
            return LiquiditySweepSnapshot(
                detected=detected,
                bullish=detected,
                reference_price=float(reference_price),
                sweep_distance_ratio=sweep_distance / max(reference_price, 1e-12),
                close_reclaim_ratio=reclaim_ratio,
                volume_ratio=volume_ratio,
                trigger_label="spring_reclaim" if detected else "",
            )

        sweep_distance = max(0.0, high - reference_price)
        reclaim_distance = max(0.0, reference_price - close)
        upper_wick = max(0.0, high - max(open_price, close))
        wick_ratio = upper_wick / candle_range
        reclaim_ratio = reclaim_distance / candle_range
        body_confirmation = close < open_price
        detected = bool(
            sweep_distance > 0.0
            and close < reference_price
            and body_confirmation
            and wick_ratio >= min_wick_ratio
            and reclaim_ratio >= min_reclaim_ratio
            and volume_ratio >= min_volume_ratio
            and (sweep_distance / atr_value) <= max_atr_excursion
        )
        return LiquiditySweepSnapshot(
            detected=detected,
            bearish=detected,
            reference_price=float(reference_price),
            sweep_distance_ratio=sweep_distance / max(reference_price, 1e-12),
            close_reclaim_ratio=reclaim_ratio,
            volume_ratio=volume_ratio,
            trigger_label="upthrust_reclaim" if detected else "",
        )

    def _resolve_bullish_urgency_window(
        self,
        *,
        bullish_ready: bool,
        kdj_dead_cross: bool,
        bullish_cross_bars_ago: int | None,
        prior_high_break: bool,
        frontrun_near_breakout: bool,
        frontrun_gap_ratio: float,
        frontrun_impulse_confirmed: bool,
        execution_obv_ready: bool,
        major_direction: int,
    ) -> tuple[bool, int | None, bool]:
        if not bullish_ready or kdj_dead_cross or bullish_cross_bars_ago is None:
            return False, None, False
        memory_bars = max(1, int(self.config.kdj_signal_memory_bars))
        decay_bars = max(0, int(getattr(self.config, "kdj_urgency_decay_bars", 0)))
        if decay_bars <= 0 or bullish_cross_bars_ago < memory_bars:
            return False, None, False
        decay_step = bullish_cross_bars_ago - memory_bars + 1
        if decay_step < 1 or decay_step > decay_bars:
            return False, None, False
        retest_tolerance_ratio = resolve_execution_trigger_proximity_budget_ratio(
            starter_frontrun_breakout_buffer_ratio=float(getattr(self.config, "starter_frontrun_breakout_buffer_ratio", 0.002)),
            bullish_memory_retest_breakout_buffer_ratio=float(getattr(self.config, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
        )
        price_location_guard = bool(prior_high_break or frontrun_near_breakout or frontrun_gap_ratio <= retest_tolerance_ratio)
        if not price_location_guard:
            return False, None, False
        breakout_reclaim_guard = bool(prior_high_break or (major_direction > 0 and frontrun_near_breakout and (frontrun_impulse_confirmed or execution_obv_ready)))
        urgency_trigger_ready = bool(
            decay_step == 1 and price_location_guard
            or decay_step > 1 and breakout_reclaim_guard
        )
        return True, decay_step, urgency_trigger_ready

    def _check_timeframe_alignment(self, major_frame: pd.DataFrame, swing_frame: pd.DataFrame, execution_frame: pd.DataFrame) -> TimeframeAlignmentCheck:
        major_ts = int(pd.Timestamp(major_frame["timestamp"].iloc[-1]).value // 1_000_000)
        swing_ts = int(pd.Timestamp(swing_frame["timestamp"].iloc[-1]).value // 1_000_000)
        execution_ts = int(pd.Timestamp(execution_frame["timestamp"].iloc[-1]).value // 1_000_000)
        swing_tf_ms = self._timeframe_to_milliseconds(self.config.swing_timeframe)
        execution_tf_ms = self._timeframe_to_milliseconds(self.config.execution_timeframe)
        tolerance_ms = execution_tf_ms

        def boundary_residual(diff_ms: int, boundary_ms: int) -> int:
            remainder = abs(diff_ms) % boundary_ms
            return min(remainder, boundary_ms - remainder if remainder else 0)

        major_vs_swing = boundary_residual(major_ts - swing_ts, swing_tf_ms)
        swing_vs_execution = boundary_residual(swing_ts - execution_ts, execution_tf_ms)
        major_vs_execution = boundary_residual(major_ts - execution_ts, execution_tf_ms)
        max_gap_ms = max(major_vs_swing, swing_vs_execution, major_vs_execution)
        valid = max_gap_ms <= tolerance_ms
        if not valid:
            logger.warning(
                "DATA_MISMATCH_WARNING | major_ts=%s swing_ts=%s execution_ts=%s residual_major_swing=%s residual_swing_execution=%s residual_major_execution=%s max_gap_ms=%s tolerance_ms=%s",
                major_ts,
                swing_ts,
                execution_ts,
                major_vs_swing,
                swing_vs_execution,
                major_vs_execution,
                max_gap_ms,
                tolerance_ms,
            )
        return TimeframeAlignmentCheck(
            major_timestamp_ms=major_ts,
            swing_timestamp_ms=swing_ts,
            execution_timestamp_ms=execution_ts,
            max_gap_ms=max_gap_ms,
            valid=valid,
        )

    def _compute_signal_quality(
        self,
        *,
        base_score: float,
        strength_bonus: float,
        total_score: float,
        ready: bool,
        fully_aligned: bool,
    ) -> EnhancedSignalScore:
        high_floor = float(getattr(self.config, "tier_high_minimum_score", 85.0))
        medium_floor = float(getattr(self.config, "tier_medium_minimum_score", 70.0))
        if total_score >= high_floor and ready and fully_aligned:
            confidence = min(1.0, max(float(getattr(self.config, "tier_high_confidence_threshold", 0.8)), 0.75 + min(0.25, (total_score - high_floor) / 20.0)))
            return EnhancedSignalScore(
                base_score=float(base_score),
                strength_bonus=float(strength_bonus),
                total_score=float(total_score),
                quality_tier=SignalQualityTier.TIER_HIGH,
                confidence=float(confidence),
            )
        if total_score >= medium_floor and ready:
            confidence = min(0.79, 0.50 + max(0.0, (total_score - medium_floor) / max(1.0, (high_floor - medium_floor))) * 0.25)
            return EnhancedSignalScore(
                base_score=float(base_score),
                strength_bonus=float(strength_bonus),
                total_score=float(total_score),
                quality_tier=SignalQualityTier.TIER_MEDIUM,
                confidence=float(confidence),
            )
        rejection_reason = ""
        if total_score < medium_floor:
            rejection_reason = f"Score too low: {total_score:.1f} < {medium_floor:.1f}"
        elif not ready:
            rejection_reason = "Signal not ready"
        else:
            rejection_reason = "Not fully aligned"
        return EnhancedSignalScore(
            base_score=float(base_score),
            strength_bonus=float(strength_bonus),
            total_score=float(total_score),
            quality_tier=SignalQualityTier.TIER_LOW,
            confidence=0.0,
            rejection_reason=rejection_reason,
        )

    def _resolve_blocker_reason(self, *, data_alignment_valid: bool, major_direction: int, weak_bull_bias: bool, early_bullish: bool, swing_score: float, bullish_ready: bool, fully_aligned: bool, execution_reason: str, bullish_score: float, execution_frontrun_near_breakout: bool, drive_first_tradeable: bool, rsi_rollover_blocked: bool, bearish_score: float = 0.0, bearish_ready: bool = False, weak_bear_bias: bool = False, early_bearish: bool = False, short_rsi_blocking_overridden: bool = False) -> str:
        if not data_alignment_valid:
            return "DATA_MISMATCH_WARNING"
        directional_context_ready = bool(
            major_direction > 0 or weak_bull_bias or early_bullish or major_direction < 0 or weak_bear_bias or early_bearish
        )
        if not directional_context_ready:
            return "Blocked_By_SuperTrend_Regime"
        if rsi_rollover_blocked:
            return "Blocked_By_RSI_ROLLOVER"
        high_momentum_breakout_clearance = bool(float(bullish_score) >= 75.0 and execution_frontrun_near_breakout)
        high_momentum_breakdown_clearance = bool(float(bearish_score) >= 75.0 and execution_frontrun_near_breakout)
        if swing_score <= 0.0 and not high_momentum_breakout_clearance and not high_momentum_breakdown_clearance and not drive_first_tradeable and not short_rsi_blocking_overridden:
            return "Blocked_By_RSI_Threshold"
        if not bullish_ready and not bearish_ready:
            return "Blocked_By_Bullish_Score"
        if not fully_aligned:
            return f"Blocked_By_Trigger:{execution_reason}"
        return "PASSED"

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

    def _resolve_major_bias(self, major_direction: int, swing_frame: pd.DataFrame, swing_supertrend: pd.DataFrame, current_major_atr: float) -> tuple[float, bool]:
        if major_direction > 0:
            return float(self.config.strong_bull_bias_score), False

        recovery_ema = swing_frame["close"].ewm(span=self.config.recovery_ema_period, adjust=False).mean()
        current_price = float(swing_frame["close"].iloc[-1])
        current_ema = float(recovery_ema.iloc[-1])
        slope_lookback = max(1, min(int(self.config.recovery_ema_slope_lookback), len(recovery_ema) - 1))
        ema_slope = current_ema - float(recovery_ema.iloc[-1 - slope_lookback])
        flat_tolerance = float(self.config.recovery_ema_flat_tolerance_atr_ratio) * max(current_major_atr, 1e-12)
        ema_flat_or_up = ema_slope >= -flat_tolerance
        weak_bull_bias = current_price > current_ema and ema_flat_or_up
        return (float(self.config.weak_bull_bias_score) if weak_bull_bias else 0.0), weak_bull_bias

    def _resolve_swing_readiness(self, swing_rsi: pd.Series) -> tuple[float, float]:
        current_rsi = float(swing_rsi.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        rsi_sma = swing_rsi.rolling(max(2, int(self.config.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi_sma = float(rsi_sma.iloc[-1])
        previous_rsi_sma = float(rsi_sma.iloc[-2])
        momentum_recovery = (
            current_rsi > float(self.config.recovery_rsi_floor)
            and current_rsi > current_rsi_sma
            and (previous_rsi <= previous_rsi_sma or rsi_slope > 0.0)
        )
        if momentum_recovery:
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

    def _resolve_early_bullish_recovery_bonus(self, *, early_bullish: bool, swing_rsi: pd.Series) -> float:
        if not early_bullish:
            return 0.0
        bonus = float(getattr(self.config, "early_bullish_score_bonus", 0.0))
        if bonus <= 0.0 or len(swing_rsi) < 2:
            return 0.0
        rsi_sma = swing_rsi.rolling(max(2, int(self.config.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi = float(swing_rsi.iloc[-1])
        current_rsi_sma = float(rsi_sma.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        rsi_buffer = max(0.0, 0.5 * abs(rsi_slope))
        recovery_still_supported = current_rsi >= (current_rsi_sma - rsi_buffer)
        if recovery_still_supported and current_rsi >= float(self.config.swing_rsi_ready_threshold):
            return bonus
        return 0.0

    def _has_direction_confirmation(self, direction_series: pd.Series, expected_direction: int) -> bool:
        confirmation_bars = max(1, int(getattr(self.config, "early_entry_direction_confirmation_bars", 2)))
        if len(direction_series) < confirmation_bars:
            return False
        recent_directions = direction_series.tail(confirmation_bars)
        return bool((recent_directions == expected_direction).all())

    def _resolve_early_bearish(self, *, major_direction: int, swing_direction: int, swing_frame: pd.DataFrame, major_supertrend: pd.DataFrame, swing_rsi: pd.Series, weak_bear_bias: bool, bearish_memory_active: bool, kdj_dead_cross: bool, swing_supertrend: pd.DataFrame) -> bool:
        if major_direction < 0 or len(swing_frame) < 1 or len(major_supertrend) < 2 or len(swing_rsi) < 2:
            return False
        current_price = float(swing_frame["close"].iloc[-1])
        current_upper_band = float(major_supertrend["upper_band"].iloc[-1])
        previous_upper_band = float(major_supertrend["upper_band"].iloc[-2])
        current_atr = float(major_supertrend["atr"].iloc[-1])
        upper_band_slope = current_upper_band - previous_upper_band
        maximum_slope = float(self.config.early_bullish_lower_band_slope_atr_threshold) * max(current_atr, 1e-12)
        rsi_sma = swing_rsi.rolling(max(2, int(self.config.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi = float(swing_rsi.iloc[-1])
        current_rsi_sma = float(rsi_sma.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        rsi_buffer = max(0.0, 0.5 * abs(rsi_slope))
        bearish_rsi_structure = current_rsi <= (current_rsi_sma + rsi_buffer) and rsi_slope < 0.0
        swing_direction_confirmed = self._has_direction_confirmation(swing_supertrend["direction"], -1)
        swing_rollover_ready = swing_direction < 0 or bearish_rsi_structure
        bearish_trigger_support = bearish_memory_active or kdj_dead_cross or weak_bear_bias
        return (
            current_price < current_upper_band
            and upper_band_slope <= maximum_slope
            and swing_rollover_ready
            and swing_direction_confirmed
            and bearish_trigger_support
        )

    def _resolve_early_bearish_score_bonus(self, *, early_bearish: bool, swing_rsi: pd.Series) -> float:
        if not early_bearish:
            return 0.0
        bonus = float(getattr(self.config, "early_bullish_score_bonus", 0.0))
        if bonus <= 0.0 or len(swing_rsi) < 2:
            return 0.0
        rsi_sma = swing_rsi.rolling(max(2, int(self.config.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi = float(swing_rsi.iloc[-1])
        current_rsi_sma = float(rsi_sma.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        rsi_buffer = max(0.0, 0.5 * abs(rsi_slope))
        rollover_still_supported = current_rsi <= (current_rsi_sma + rsi_buffer)
        if rollover_still_supported and current_rsi <= float(self.config.swing_rsi_ready_threshold):
            return bonus
        return 0.0

    def _has_starter_frontrun_impulse(self, execution_frame: pd.DataFrame) -> bool:
        impulse_bars = max(2, int(getattr(self.config, "starter_frontrun_impulse_bars", 3)))
        volume_window = max(impulse_bars + 1, int(getattr(self.config, "starter_frontrun_volume_window", 12)))
        if len(execution_frame) < volume_window:
            return False
        recent = execution_frame.tail(impulse_bars)
        baseline = execution_frame.iloc[:-impulse_bars].tail(volume_window)
        volume_mean = float(baseline["volume"].mean()) if not baseline.empty else 0.0
        if volume_mean <= 0:
            return False
        bullish_bars = bool((recent["close"] > recent["open"]).all())
        volume_multiplier = float(getattr(self.config, "starter_frontrun_volume_multiplier", 1.15))
        supported_volume = bool((recent["volume"] >= volume_mean * volume_multiplier).all())
        return bullish_bars and supported_volume

    def _has_starter_short_frontrun_impulse(self, execution_frame: pd.DataFrame) -> bool:
        impulse_bars = max(2, int(getattr(self.config, "starter_frontrun_impulse_bars", 3)))
        volume_window = max(impulse_bars + 1, int(getattr(self.config, "starter_frontrun_volume_window", 12)))
        if len(execution_frame) < volume_window:
            return False
        recent = execution_frame.tail(impulse_bars)
        baseline = execution_frame.iloc[:-impulse_bars].tail(volume_window)
        volume_mean = float(baseline["volume"].mean()) if not baseline.empty else 0.0
        if volume_mean <= 0:
            return False
        bearish_bars = bool((recent["close"] < recent["open"]).all())
        volume_multiplier = float(getattr(self.config, "starter_frontrun_volume_multiplier", 1.15))
        supported_volume = bool((recent["volume"] >= volume_mean * volume_multiplier).all())
        return bearish_bars and supported_volume

    def _resolve_early_bullish(self, major_frame: pd.DataFrame, swing_frame: pd.DataFrame, major_supertrend: pd.DataFrame) -> bool:
        if len(major_frame) < 2 or len(swing_frame) < 1 or len(major_supertrend) < 2:
            return False

        swing_supertrend = compute_supertrend(
            swing_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        swing_direction = int(swing_supertrend["direction"].iloc[-1])
        if swing_direction <= 0 or not self._has_direction_confirmation(swing_supertrend["direction"], 1):
            return False

        current_price = float(swing_frame["close"].iloc[-1])
        current_lower_band = float(major_supertrend["lower_band"].iloc[-1])
        previous_lower_band = float(major_supertrend["lower_band"].iloc[-2])
        current_atr = float(major_supertrend["atr"].iloc[-1])
        lower_band_slope = current_lower_band - previous_lower_band
        minimum_slope = -float(self.config.early_bullish_lower_band_slope_atr_threshold) * max(current_atr, 1e-12)
        return current_price > current_lower_band and lower_band_slope >= minimum_slope

    @staticmethod
    def _extract_oi_funding_snapshot(execution_frame: pd.DataFrame) -> OIFundingSnapshot:
        if execution_frame.empty:
            return OIFundingSnapshot()
        columns = set(execution_frame.columns)
        close = execution_frame["close"].astype(float)
        price_lookback = min(3, len(close) - 1)
        price_change_pct = 0.0
        if price_lookback >= 1:
            reference_price = float(close.iloc[-1 - price_lookback])
            if abs(reference_price) > 1e-12:
                price_change_pct = ((float(close.iloc[-1]) - reference_price) / reference_price) * 100.0

        oi_change_pct = 0.0
        if "open_interest" in columns and len(execution_frame["open_interest"]) >= 2:
            oi_series = pd.to_numeric(execution_frame["open_interest"], errors="coerce").astype(float)
            oi_lookback = min(3, len(oi_series) - 1)
            reference_oi = float(oi_series.iloc[-1 - oi_lookback]) if oi_lookback >= 1 else float("nan")
            current_oi = float(oi_series.iloc[-1])
            if oi_lookback >= 1 and pd.notna(reference_oi) and pd.notna(current_oi) and abs(reference_oi) > 1e-12:
                oi_change_pct = ((current_oi - reference_oi) / reference_oi) * 100.0

        funding_rate = 0.0
        for funding_col in ("funding_rate", "funding", "fundingRate"):
            if funding_col in columns:
                funding_series = pd.to_numeric(execution_frame[funding_col], errors="coerce").astype(float)
                if not funding_series.empty and pd.notna(funding_series.iloc[-1]):
                    funding_rate = float(funding_series.iloc[-1])
                    break

        oi_drop_fast_threshold_pct = 1.5
        price_move_threshold_pct = 0.35
        is_short_squeeze = bool(price_change_pct >= price_move_threshold_pct and oi_change_pct <= -oi_drop_fast_threshold_pct)
        is_long_liquidation = bool(price_change_pct <= -price_move_threshold_pct and oi_change_pct <= -oi_drop_fast_threshold_pct)
        return OIFundingSnapshot(
            oi_change_pct=float(oi_change_pct),
            funding_rate=float(funding_rate),
            is_short_squeeze=is_short_squeeze,
            is_long_liquidation=is_long_liquidation,
        )

    @staticmethod
    def _compute_oi_funding_score_adjustment(snapshot: OIFundingSnapshot) -> tuple[float, float]:
        bullish_adjustment = 0.0
        bearish_adjustment = 0.0
        if snapshot.is_short_squeeze:
            bullish_adjustment -= 5.0
        if snapshot.is_long_liquidation:
            bearish_adjustment -= 5.0
        if snapshot.funding_rate >= 0.0008:
            bullish_adjustment -= 2.0
        elif snapshot.funding_rate <= -0.0008:
            bearish_adjustment -= 2.0
        return bullish_adjustment, bearish_adjustment

    def build_signal(self) -> MTFSignal | None:
        execution_ohlcv = maybe_use_closed_candles(
            self.client.fetch_ohlcv(
                symbol=self.config.symbol,
                timeframe=self.config.execution_timeframe,
                limit=self.config.lookback_limit,
            ),
            enabled=self.config.prefer_closed_execution_timeframe_candles,
        )
        swing_ohlcv = maybe_use_closed_candles(
            self.client.fetch_ohlcv(
                symbol=self.config.symbol,
                timeframe=self.config.swing_timeframe,
                limit=self.config.lookback_limit,
            ),
            enabled=self.config.prefer_closed_swing_timeframe_candles,
        )
        major_ohlcv = maybe_use_closed_candles(
            self.client.fetch_ohlcv(
                symbol=self.config.symbol,
                timeframe=self.config.major_timeframe,
                limit=self.config.lookback_limit,
            ),
            enabled=self.config.prefer_closed_major_timeframe_candles,
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
        oi_funding_snapshot = self._extract_oi_funding_snapshot(execution_frame)

        major_supertrend = compute_supertrend(
            major_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        swing_supertrend = compute_supertrend(
            swing_frame,
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
        swing_direction = int(swing_supertrend["direction"].iloc[-1])
        alignment = self._check_timeframe_alignment(major_frame, swing_frame, execution_frame)
        current_swing_rsi = float(swing_rsi.iloc[-1])
        swing_rsi_sma = swing_rsi.rolling(max(2, int(self.config.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi_sma = float(swing_rsi_sma.iloc[-1])
        current_major_atr = float(major_supertrend["atr"].iloc[-1])
        early_bullish = major_direction <= 0 and self._resolve_early_bullish(major_frame, swing_frame, major_supertrend)
        major_bias_score, weak_bull_bias = self._resolve_major_bias(major_direction, swing_frame, swing_supertrend, current_major_atr)
        swing_score, swing_rsi_slope = self._resolve_swing_readiness(swing_rsi)
        recovery_ema = swing_frame["close"].ewm(span=max(2, int(self.config.recovery_ema_period)), adjust=False).mean()
        current_recovery_ema = float(recovery_ema.iloc[-1])
        recovery_ema_lookback_index = max(0, len(recovery_ema) - 1 - max(1, int(self.config.recovery_ema_slope_lookback)))
        recovery_ema_slope = current_recovery_ema - float(recovery_ema.iloc[recovery_ema_lookback_index])
        weak_bear_bias = bool(
            major_direction >= 0
            and float(swing_frame["close"].iloc[-1]) < current_recovery_ema
            and recovery_ema_slope <= float(self.config.recovery_ema_flat_tolerance_atr_ratio) * max(current_major_atr, 1e-12)
        )

        current_k = float(execution_kdj["k"].iloc[-1])
        current_d = float(execution_kdj["d"].iloc[-1])
        previous_k = float(execution_kdj["k"].iloc[-2])
        previous_d = float(execution_kdj["d"].iloc[-2])
        kdj_golden_cross = previous_k <= previous_d and current_k > current_d
        kdj_dead_cross = previous_k >= previous_d and current_k < current_d

        bullish_cross_mask = (execution_kdj["k"].shift(1) <= execution_kdj["d"].shift(1)) & (execution_kdj["k"] > execution_kdj["d"])
        bearish_cross_mask = (execution_kdj["k"].shift(1) >= execution_kdj["d"].shift(1)) & (execution_kdj["k"] < execution_kdj["d"])
        memory_bars = max(1, int(self.config.kdj_signal_memory_bars))

        urgency_scan_bars = memory_bars + max(0, int(getattr(self.config, "kdj_urgency_decay_bars", 0)))
        bullish_cross_bars_ago = self._bars_since_last_true(bullish_cross_mask, urgency_scan_bars)
        bearish_cross_bars_ago = self._bars_since_last_true(bearish_cross_mask, memory_bars)
        bullish_memory_active = bullish_cross_bars_ago is not None and bullish_cross_bars_ago < memory_bars
        bearish_memory_active = bearish_cross_bars_ago is not None
        bearish_bridge_rollover = bool(
            major_direction >= 0
            and bearish_memory_active
            and float(swing_frame["close"].iloc[-1]) < current_recovery_ema
            and current_swing_rsi <= current_rsi_sma
            and swing_rsi_slope < 0.0
        )
        weak_bear_bias = bool(weak_bear_bias or bearish_bridge_rollover)
        early_bearish = self._resolve_early_bearish(
            major_direction=major_direction,
            swing_direction=swing_direction,
            swing_frame=swing_frame,
            major_supertrend=major_supertrend,
            swing_rsi=swing_rsi,
            weak_bear_bias=weak_bear_bias,
            bearish_memory_active=bearish_memory_active,
            kdj_dead_cross=kdj_dead_cross,
            swing_supertrend=swing_supertrend,
        )
        bullish_latch_active, latch_low_price, bullish_latch_bars_ago = self._resolve_directional_latch(
            cross_mask=bullish_cross_mask,
            reverse_cross_mask=bearish_cross_mask,
            execution_frame=execution_frame,
            defended_column="low",
            violation_column="low",
            bullish=True,
        )
        bearish_latch_active, latch_high_price, bearish_latch_bars_ago = self._resolve_directional_latch(
            cross_mask=bearish_cross_mask,
            reverse_cross_mask=bullish_cross_mask,
            execution_frame=execution_frame,
            defended_column="high",
            violation_column="high",
            bullish=False,
        )
        current_price = float(execution_frame["close"].iloc[-1])
        execution_atr = float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])

        score_4h = float(self.config.strong_bull_bias_score) if major_direction > 0 else 0.0
        score_1h = 0.0
        if score_4h <= 0.0:
            if swing_direction > 0:
                score_1h = float(getattr(self.config, "swing_supertrend_bullish_score", 0.0))
            elif weak_bull_bias:
                score_1h = float(self.config.weak_bull_bias_score)
        score_rsi = swing_score
        score_early_recovery = self._resolve_early_bullish_recovery_bonus(early_bullish=early_bullish, swing_rsi=swing_rsi)
        score_kdj = float(getattr(self.config, "kdj_memory_score_bonus", 0.0)) if bullish_memory_active else 0.0
        score_magnet = 0.0
        volatility_squeeze_breakout = bool(
            len(execution_frame) >= 20
            and execution_atr > 0.0
            and (float(execution_frame["high"].iloc[-1]) - float(execution_frame["low"].iloc[-1])) >= execution_atr * 1.2
            and execution_frame["close"].tail(20).pct_change().std(ddof=0) <= max(1e-9, execution_atr / max(current_price, 1e-12)) * 0.75
        )
        base_bullish_score = score_4h + score_1h + score_rsi + score_early_recovery + score_kdj
        bullish_score = base_bullish_score

        bearish_score_4h = float(self.config.strong_bull_bias_score) if major_direction < 0 else 0.0
        bearish_score_1h = 0.0
        if bearish_score_4h <= 0.0:
            if swing_direction < 0 or early_bearish:
                bearish_score_1h = float(getattr(self.config, "swing_supertrend_bullish_score", 0.0))
            elif weak_bear_bias:
                bearish_score_1h = float(self.config.weak_bull_bias_score)
        bearish_score_rsi = float(self.config.dynamic_rsi_trend_score) if current_swing_rsi <= float(self.config.dynamic_rsi_floor) and swing_rsi_slope < 0.0 else 0.0
        bearish_score_early = self._resolve_early_bearish_score_bonus(early_bearish=early_bearish, swing_rsi=swing_rsi)
        bearish_score_kdj = float(getattr(self.config, "kdj_memory_score_bonus", 0.0)) if bearish_memory_active else 0.0
        base_bearish_score = bearish_score_4h + bearish_score_1h + bearish_score_rsi + bearish_score_early + bearish_score_kdj
        bearish_score = base_bearish_score
        drive_first_tradeable = bullish_score >= float(getattr(self.config, "drive_first_tradeable_score", 60.0))

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
        stretch_ema = execution_frame["close"].ewm(span=max(2, int(getattr(self.config, "stretch_ema_period", 20))), adjust=False).mean()
        current_stretch_ema = float(stretch_ema.iloc[-1])
        volume_profile = compute_volume_profile(
            execution_frame,
            lookback_hours=self.config.volume_profile_lookback_hours,
            value_area_pct=self.config.volume_profile_value_area_pct,
            bin_count=self.config.volume_profile_bin_count,
        )
        prior_high_break = prior_high is not None and current_price > prior_high + 1e-12
        prior_low_break = prior_low is not None and current_price < prior_low - 1e-12
        frontrun_gap_ratio = 0.0
        frontrun_near_breakout = False
        if prior_high is not None and prior_high > 0 and current_price < prior_high:
            frontrun_gap_ratio = max(0.0, (prior_high - current_price) / prior_high)
            frontrun_near_breakout = frontrun_gap_ratio <= float(getattr(self.config, "starter_frontrun_breakout_buffer_ratio", 0.002))
        short_frontrun_gap_ratio = 0.0
        short_frontrun_near_breakout = False
        if prior_low is not None and prior_low > 0 and current_price > prior_low:
            short_frontrun_gap_ratio = max(0.0, (current_price - prior_low) / prior_low)
            short_frontrun_near_breakout = short_frontrun_gap_ratio <= float(getattr(self.config, "starter_frontrun_breakout_buffer_ratio", 0.002))

        liquidity_sweep_wick_ratio = float(getattr(self.config, "liquidity_sweep_min_wick_ratio", 0.35))
        liquidity_sweep_reclaim_ratio = float(getattr(self.config, "liquidity_sweep_min_reclaim_ratio", 0.15))
        liquidity_sweep_volume_ratio = float(getattr(self.config, "liquidity_sweep_min_volume_ratio", 1.0))
        liquidity_sweep_max_atr_excursion = float(getattr(self.config, "liquidity_sweep_max_atr_excursion", 1.25))
        bullish_liquidity_sweep = self._detect_liquidity_sweep(
            execution_frame=execution_frame,
            reference_price=prior_low,
            atr_value=execution_atr,
            bullish=True,
            breakout_lookback=max(1, int(self.config.execution_breakout_lookback)),
            min_wick_ratio=liquidity_sweep_wick_ratio,
            min_reclaim_ratio=liquidity_sweep_reclaim_ratio,
            min_volume_ratio=liquidity_sweep_volume_ratio,
            max_atr_excursion=liquidity_sweep_max_atr_excursion,
        )
        bearish_liquidity_sweep = self._detect_liquidity_sweep(
            execution_frame=execution_frame,
            reference_price=prior_high,
            atr_value=execution_atr,
            bullish=False,
            breakout_lookback=max(1, int(self.config.execution_breakout_lookback)),
            min_wick_ratio=liquidity_sweep_wick_ratio,
            min_reclaim_ratio=liquidity_sweep_reclaim_ratio,
            min_volume_ratio=liquidity_sweep_volume_ratio,
            max_atr_excursion=liquidity_sweep_max_atr_excursion,
        )
        liquidity_sweep_score_bonus = max(0.0, float(getattr(self.config, "liquidity_sweep_score_bonus", 8.0)))
        bullish_liquidity_sweep_active = bool(major_direction > 0 and bullish_liquidity_sweep.detected)
        bearish_liquidity_sweep_active = bool(major_direction < 0 and bearish_liquidity_sweep.detected)
        if bullish_liquidity_sweep_active:
            bullish_score += liquidity_sweep_score_bonus
        if bearish_liquidity_sweep_active:
            bearish_score += liquidity_sweep_score_bonus

        short_bias_dominance = bool(
            major_direction < 0
            and bearish_score >= float(getattr(self.config, "short_bias_dominance_min_bearish_score", 68.0))
            and bearish_score >= bullish_score + float(getattr(self.config, "short_bias_dominance_score_gap", 8.0))
            and (
                bearish_memory_active
                or bearish_latch_active
                or short_frontrun_near_breakout
                or prior_low_break
                or bearish_liquidity_sweep_active
            )
        )
        if short_bias_dominance and weak_bull_bias:
            weak_bull_bias = False
            major_bias_score = 0.0

        # 回踩支撑检测（多头）：价格回撤至VAL附近时视为入场机会
        pullback_near_support = False
        pullback_depth_ratio = 0.0
        if prior_high is not None and prior_high > 0 and major_direction > 0:
            pullback_depth_ratio = max(0.0, (prior_high - current_price) / prior_high)
            pullback_threshold = float(getattr(self.config, "pullback_entry_depth_ratio", 0.015))
            if pullback_depth_ratio >= pullback_threshold:
                if volume_profile is not None:
                    val = float(volume_profile.value_area_low)
                    atr_val = float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])
                    support_tolerance = max(atr_val, current_price * 0.002)
                    if current_price <= val + support_tolerance:
                        pullback_near_support = True

        # 回踩阻力检测（空头）：价格回涨至VAH附近时视为做空机会
        pullback_near_resistance = False
        pullback_resistance_depth_ratio = 0.0
        if prior_low is not None and prior_low > 0 and major_direction < 0:
            pullback_resistance_depth_ratio = max(0.0, (current_price - prior_low) / prior_low)
            pullback_threshold = float(getattr(self.config, "pullback_entry_depth_ratio", 0.015))
            if pullback_resistance_depth_ratio >= pullback_threshold:
                if volume_profile is not None:
                    vah = float(volume_profile.value_area_high)
                    atr_val = float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])
                    resistance_tolerance = max(atr_val, current_price * 0.002)
                    if current_price >= vah - resistance_tolerance:
                        pullback_near_resistance = True

        relevant_rail = float(major_supertrend["upper_band"].iloc[-1] if major_direction <= 0 else major_supertrend["lower_band"].iloc[-1])
        rail_distance = abs(current_price - relevant_rail)
        magnetism_limit = float(self.config.magnetism_rail_atr_multiplier) * current_major_atr
        magnetism_distance_pct = (rail_distance / relevant_rail * 100.0) if abs(relevant_rail) > 1e-12 else 0.0
        magnetism_obv_ready = execution_obv_confirmation.zscore > float(self.config.magnetism_obv_zscore_threshold)
        rail_momentum_ready = bool(
            major_direction <= 0
            and current_major_atr > 0.0
            and rail_distance <= magnetism_limit
            and (swing_direction > 0 or current_swing_rsi >= float(self.config.dynamic_rsi_floor))
            and (bullish_memory_active or kdj_golden_cross)
        )
        bullish_magnetism_ready = (
            major_direction <= 0
            and current_major_atr > 0.0
            and rail_distance <= magnetism_limit
            and magnetism_obv_ready
        )
        if bullish_magnetism_ready:
            score_magnet = float(getattr(self.config, "magnetism_score_bonus", 0.0))
            bullish_score += score_magnet
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

        bullish_exhaustion_penalty, bullish_exhaustion_reason, bullish_exhaustion_applied = self._compute_exhaustion_penalty(
            execution_frame=execution_frame,
            swing_rsi=swing_rsi,
            bullish=True,
            volatility_squeeze_breakout=volatility_squeeze_breakout,
        )
        bearish_exhaustion_penalty, bearish_exhaustion_reason, bearish_exhaustion_applied = self._compute_exhaustion_penalty(
            execution_frame=execution_frame,
            swing_rsi=swing_rsi,
            bullish=False,
            volatility_squeeze_breakout=volatility_squeeze_breakout,
        )
        if bullish_exhaustion_applied:
            bullish_score -= bullish_exhaustion_penalty
        if bearish_exhaustion_applied:
            bearish_score -= bearish_exhaustion_penalty

        oi_bullish_adjustment, oi_bearish_adjustment = self._compute_oi_funding_score_adjustment(oi_funding_snapshot)
        bullish_score += oi_bullish_adjustment
        bearish_score += oi_bearish_adjustment

        trend_strength_bars = min(20, self._count_bars_since_direction_change(major_supertrend["direction"]))
        trend_strength_bonus = min(
            float(getattr(self.config, "signal_strength_trend_bonus_cap", 5.0)),
            (trend_strength_bars / 20.0) * float(getattr(self.config, "signal_strength_trend_bonus_cap", 5.0)),
        )
        upper_band = float(major_supertrend["upper_band"].iloc[-1])
        lower_band = float(major_supertrend["lower_band"].iloc[-1])
        band_width = max(upper_band - lower_band, 1e-12)
        price_position_in_band = (current_price - lower_band) / band_width
        direction_clarity_bonus = min(
            float(getattr(self.config, "signal_strength_direction_bonus_cap", 10.0)),
            abs(price_position_in_band - 0.5) * 20.0,
        )
        if len(execution_frame) >= 2:
            recent_range = float(execution_frame["high"].iloc[-1]) - float(execution_frame["low"].iloc[-1])
            prev_range = float(execution_frame["high"].iloc[-2]) - float(execution_frame["low"].iloc[-2])
            volatility_expansion_bonus = min(
                float(getattr(self.config, "signal_strength_volatility_bonus_cap", 5.0)),
                max(0.0, ((recent_range / max(prev_range, 1e-12)) - 1.0) * 10.0),
            )
        else:
            volatility_expansion_bonus = 0.0
        dominant_direction = 1 if bullish_score >= bearish_score else -1
        dominant_obv_strength = max(0.0, float(execution_obv_confirmation.zscore) * dominant_direction)
        obv_strength_bonus = min(
            float(getattr(self.config, "signal_strength_obv_bonus_cap", 10.0)),
            dominant_obv_strength * 5.0,
        )
        strength_bonus = trend_strength_bonus + direction_clarity_bonus + volatility_expansion_bonus + obv_strength_bonus
        bullish_score += strength_bonus if dominant_direction > 0 else 0.0
        bearish_score += strength_bonus if dominant_direction < 0 else 0.0

        bullish_threshold = float(self.config.bullish_ready_score_threshold)
        bearish_threshold = float(self.config.bullish_ready_score_threshold)
        if major_direction >= 0 and early_bearish:
            bearish_threshold = min(
                bearish_threshold,
                max(
                    float(getattr(self.config, "swing_supertrend_bullish_score", 0.0)) + float(getattr(self.config, "kdj_memory_score_bonus", 0.0)),
                    bearish_threshold - float(getattr(self.config, "kdj_memory_score_bonus", 0.0)) - 5.0,
                ),
            )
        rsi_relax_score = float(getattr(self.config, "aggressive_rsi_relax_score", 70.0))
        rsi_extreme_threshold = float(getattr(self.config, "aggressive_rsi_extreme_threshold", 85.0))
        rsi_rollover_blocked = bool(
            major_direction > 0
            and bullish_score >= rsi_relax_score
            and current_swing_rsi > rsi_extreme_threshold
            and swing_rsi_slope < 0.0
            and current_swing_rsi < current_rsi_sma
        )
        bullish_stretch = self._compute_price_stretch(
            current_price=current_price,
            anchor_price=current_stretch_ema,
            atr_value=execution_atr,
            bullish=True,
        )
        bearish_stretch = self._compute_price_stretch(
            current_price=current_price,
            anchor_price=current_stretch_ema,
            atr_value=execution_atr,
            bullish=False,
        )
        stretch_blocked_bullish = bool(
            major_direction > 0
            and not early_bullish
            and bullish_score >= float(getattr(self.config, "stretch_block_min_score", 75.0))
            and bullish_stretch > float(getattr(self.config, "stretch_block_atr_threshold", 1.5))
        )
        stretch_blocked_bearish = bool(
            major_direction < 0
            and not early_bearish
            and bearish_score >= float(getattr(self.config, "stretch_block_min_score", 75.0))
            and bearish_stretch > float(getattr(self.config, "stretch_block_atr_threshold", 1.5))
        )
        pending_retest_bullish = bool(stretch_blocked_bullish)
        pending_retest_bearish = bool(stretch_blocked_bearish)
        bullish_retest_release_ready = bool(
            major_direction > 0
            and bullish_score >= float(getattr(self.config, "stretch_retest_release_min_score", 60.0))
            and bullish_stretch <= float(getattr(self.config, "stretch_retest_release_threshold", 0.5))
            and pullback_near_support
        )
        bearish_retest_release_ready = bool(
            major_direction < 0
            and bearish_score >= float(getattr(self.config, "stretch_retest_release_min_score", 60.0))
            and bearish_stretch <= float(getattr(self.config, "stretch_retest_release_threshold", 0.5))
            and pullback_near_resistance
        )
        logger.info(
            "Bullish Score: %.0f/%.0f [base=%.0f strength=%.1f 4H: %.0f, 1H: %.0f, Magnet: %.0f, RSI: %.0f, Early: %.0f, KDJ: %.0f] | symbol=%s major_dir=%s swing_dir=%s weak_bull=%s early_bullish=%s stretch=%.2f exhaustion=%s",
            bullish_score,
            bullish_threshold,
            base_bullish_score,
            strength_bonus if dominant_direction > 0 else 0.0,
            score_4h,
            score_1h,
            score_magnet,
            score_rsi,
            score_early_recovery,
            score_kdj,
            self.config.symbol,
            major_direction,
            swing_direction,
            weak_bull_bias,
            early_bullish,
            bullish_stretch,
            bullish_exhaustion_reason or "none",
        )
        bullish_ready = bullish_score >= bullish_threshold
        bearish_ready = bearish_score >= bearish_threshold
        early_entry_minimum_score = float(getattr(self.config, "early_entry_minimum_score", 70.0))
        starter_frontrun_minimum_score = float(getattr(self.config, "starter_frontrun_minimum_score", early_entry_minimum_score))
        early_bullish = bool(early_bullish and bullish_score >= early_entry_minimum_score)
        early_bearish = bool(early_bearish and bearish_score >= early_entry_minimum_score)

        execution_obv_ready = execution_obv_confirmation.buy_confirmed(
            zscore_threshold=float(self.config.obv_zscore_threshold),
        )
        execution_obv_sell_ready = execution_obv_confirmation.sell_confirmed(
            zscore_threshold=float(self.config.obv_zscore_threshold),
        )
        frontrun_impulse_confirmed = self._has_starter_frontrun_impulse(execution_frame)
        short_frontrun_impulse_confirmed = self._has_starter_short_frontrun_impulse(execution_frame)
        starter_frontrun_ready = bool(
            getattr(self.config, "starter_frontrun_enabled", True)
            and bullish_ready
            and not stretch_blocked_bullish
            and base_bullish_score >= starter_frontrun_minimum_score
            and execution_obv_ready
            and frontrun_near_breakout
            and frontrun_impulse_confirmed
            and self._has_direction_confirmation(swing_supertrend["direction"], 1)
            and (bullish_memory_active or kdj_golden_cross)
        )
        starter_short_frontrun_ready = bool(
            getattr(self.config, "starter_frontrun_enabled", True)
            and bearish_ready
            and not stretch_blocked_bearish
            and base_bearish_score >= starter_frontrun_minimum_score
            and execution_obv_sell_ready
            and short_frontrun_near_breakout
            and short_frontrun_impulse_confirmed
            and self._has_direction_confirmation(swing_supertrend["direction"], -1)
            and (bearish_memory_active or kdj_dead_cross)
        )
        near_breakout_release_ready = bool(
            getattr(self.config, "near_breakout_release_enabled", True)
            and bullish_ready
            and not stretch_blocked_bullish
            and bullish_score >= float(getattr(self.config, "near_breakout_release_minimum_score", 70.0))
            and frontrun_near_breakout
            and not prior_high_break
            and not kdj_dead_cross
            and self._has_direction_confirmation(swing_supertrend["direction"], 1)
            and (bullish_memory_active or bullish_latch_active)
        )
        high_confidence_price_override = bool(bullish_score >= 75.0 and frontrun_near_breakout and not kdj_dead_cross and not stretch_blocked_bullish)
        trend_continuation_near_breakout_ready = (
            not stretch_blocked_bullish
            and self._trend_continuation_near_breakout_ready(
                major_direction=major_direction,
                bullish_score=bullish_score,
                frontrun_near_breakout=frontrun_near_breakout,
                prior_high_break=prior_high_break,
                kdj_dead_cross=kdj_dead_cross,
                execution_obv_ready=execution_obv_ready,
                execution_obv_zscore=float(execution_obv_confirmation.zscore),
            )
        )
        medium_confidence_latch_breakout_ready = bool(
            major_direction > 0
            and bullish_ready
            and bullish_score < 70.0
            and bullish_latch_active
            and prior_high_break
        )
        bullish_urgency_active, bullish_urgency_decay_step, bullish_urgency_trigger_ready = self._resolve_bullish_urgency_window(
            bullish_ready=bullish_ready,
            kdj_dead_cross=kdj_dead_cross,
            bullish_cross_bars_ago=bullish_cross_bars_ago,
            prior_high_break=prior_high_break,
            frontrun_near_breakout=frontrun_near_breakout or short_frontrun_near_breakout,
            frontrun_gap_ratio=min(frontrun_gap_ratio, short_frontrun_gap_ratio) if short_frontrun_near_breakout else frontrun_gap_ratio,
            frontrun_impulse_confirmed=frontrun_impulse_confirmed,
            execution_obv_ready=execution_obv_ready,
            major_direction=major_direction,
        )
        if bullish_urgency_active and not bullish_latch_active:
            bullish_urgency_active = False
            bullish_urgency_decay_step = None
            bullish_urgency_trigger_ready = False
        major_bull_retest_ready = self._major_bull_retest_near_breakout_ready(
            major_direction=major_direction,
            bullish_ready=bullish_ready,
            bullish_memory_active=bullish_memory_active,
            frontrun_gap_ratio=frontrun_gap_ratio,
        ) or bullish_urgency_trigger_ready
        major_bull_impulse_reclaim_ready = bool(
            major_direction > 0
            and bullish_ready
            and frontrun_impulse_confirmed
            and (
                prior_high_break
                or (execution_obv_ready and frontrun_near_breakout)
            )
            and not (bullish_memory_active or kdj_golden_cross)
        )

        execution_trigger_proximity_budget_ratio = resolve_execution_trigger_proximity_budget_ratio(
            starter_frontrun_breakout_buffer_ratio=float(getattr(self.config, "starter_frontrun_breakout_buffer_ratio", 0.002)),
            bullish_memory_retest_breakout_buffer_ratio=float(getattr(self.config, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
        )

        bullish_spring_reclaim_ready = bool(
            bullish_liquidity_sweep_active
            and bullish_ready
            and not stretch_blocked_bullish
            and (bullish_memory_active or bullish_latch_active or execution_obv_ready or pullback_near_support)
        )
        bearish_upthrust_reclaim_ready = bool(
            bearish_liquidity_sweep_active
            and bearish_ready
            and not stretch_blocked_bearish
            and (bearish_memory_active or bearish_latch_active or execution_obv_sell_ready or pullback_near_resistance)
        )

        bearish_breakout_ready = bool(major_direction < 0 and bearish_ready and prior_low_break and (bearish_memory_active or kdj_dead_cross or bearish_latch_active))
        bearish_retest_ready = bool(major_direction < 0 and bearish_ready and bearish_memory_active and short_frontrun_near_breakout)

        execution_entry_mode = "breakout_confirmed"
        entry_size_multiplier = 1.0
        if weak_bull_bias and not short_bias_dominance:
            execution_entry_mode = "weak_bull_scale_in_limit"
        if starter_frontrun_ready:
            execution_entry_mode = "starter_frontrun_limit"
            entry_size_multiplier = max(0.0, min(1.0, float(getattr(self.config, "starter_frontrun_fraction", 0.20))))
        if starter_short_frontrun_ready:
            execution_entry_mode = "starter_short_frontrun_limit"
            entry_size_multiplier = max(0.0, min(1.0, float(getattr(self.config, "starter_frontrun_fraction", 0.20))))
        if early_bullish:
            execution_entry_mode = "early_bullish_starter_limit"
            entry_size_multiplier = max(0.0, min(1.0, float(self.config.early_bullish_starter_fraction)))
        if weak_bear_bias:
            execution_entry_mode = "weak_bear_scale_in_limit"
        if early_bearish:
            execution_entry_mode = "early_bearish_starter_limit"
            entry_size_multiplier = max(0.0, min(1.0, float(self.config.early_bullish_starter_fraction)))
        if not (early_bullish or early_bearish):
            if bullish_retest_release_ready:
                execution_entry_mode = "bullish_retest_limit"
            elif bearish_retest_release_ready:
                execution_entry_mode = "bearish_retest_limit"

        state_label = "WAITING_SETUP"
        trigger_family = "waiting"
        if early_bearish:
            trigger_family = "early_bearish"
            reason = "early_bearish: 1h supertrend bearish + price below 4h upper band + 4h upper band flattening"
        elif pending_retest_bearish and not bearish_retest_release_ready:
            trigger_family = "pending_retest_bearish"
            reason = f"Pending_Retest_Bearish: stretch={bearish_stretch:.2f} > release={float(getattr(self.config, 'stretch_retest_release_threshold', 0.5)):.2f}"
        elif bearish_retest_release_ready:
            trigger_family = "bearish_retest_entry"
            reason = f"bearish_retest_entry: stretch={bearish_stretch:.2f} + score={bearish_score:.0f}"
        elif bearish_upthrust_reclaim_ready:
            trigger_family = "upthrust_reclaim"
            reason = f"upthrust_reclaim: sweep={bearish_liquidity_sweep.sweep_distance_ratio * 100:.3f}% above prior_high + reclaim={bearish_liquidity_sweep.close_reclaim_ratio:.2f} + vol={bearish_liquidity_sweep.volume_ratio:.2f}x"
        elif bearish_memory_active and prior_low_break and major_direction < 0 and bearish_ready:
            trigger_family = "bearish_memory_breakdown"
            reason = f"Triggered via Bearish Memory Window: KDJ crossed {bearish_cross_bars_ago} bars ago + Price Breakdown NOW"
        elif bearish_retest_ready:
            trigger_family = "upthrust_reclaim"
            reason = f"upthrust_reclaim: sweep={bearish_liquidity_sweep.sweep_distance_ratio * 100:.3f}% above prior_high + reclaim={bearish_liquidity_sweep.close_reclaim_ratio:.2f} + vol={bearish_liquidity_sweep.volume_ratio:.2f}x"
        elif bearish_memory_active and prior_low_break and major_direction < 0 and bearish_ready:
            trigger_family = "bearish_memory_breakdown"
            reason = f"Triggered via Bearish Memory Window: KDJ crossed {bearish_cross_bars_ago} bars ago + Price Breakdown NOW"
        elif bearish_retest_ready:
            trigger_family = "bearish_retest"
            reason = f"major_bear_retest_ready: gap={short_frontrun_gap_ratio * 100:.3f}% + KDJ memory {bearish_cross_bars_ago} bars ago"
        elif early_bullish:
            trigger_family = "early_bullish"
            reason = "early_bullish: 1h supertrend bullish + price above 4h lower band + 4h lower band slope flattening"
        elif pending_retest_bullish and not bullish_retest_release_ready:
            trigger_family = "pending_retest_bullish"
            reason = f"Pending_Retest_Bullish: stretch={bullish_stretch:.2f} > release={float(getattr(self.config, 'stretch_retest_release_threshold', 0.5)):.2f}"
        elif bullish_retest_release_ready:
            trigger_family = "bullish_retest_entry"
            reason = f"bullish_retest_entry: stretch={bullish_stretch:.2f} + score={bullish_score:.0f}"
        elif bullish_spring_reclaim_ready:
            trigger_family = "spring_reclaim"
            reason = f"spring_reclaim: sweep={bullish_liquidity_sweep.sweep_distance_ratio * 100:.3f}% below prior_low + reclaim={bullish_liquidity_sweep.close_reclaim_ratio:.2f} + vol={bullish_liquidity_sweep.volume_ratio:.2f}x"
        elif bullish_memory_active and prior_high_break:
            trigger_family = "bullish_memory_breakout"
            reason = f"Triggered via Memory Window: KDJ crossed {bullish_cross_bars_ago} bars ago + Price Breakout NOW"
        elif starter_short_frontrun_ready:
            trigger_family = "starter_short_frontrun"
            reason = f"starter_short_frontrun: gap={short_frontrun_gap_ratio * 100:.3f}% + 1m {int(getattr(self.config, 'starter_frontrun_impulse_bars', 3))} bearish bars + OBV confirmed"
        elif pullback_near_resistance and major_direction < 0 and bearish_ready:
            trigger_family = "pullback_resistance_entry"
            reason = f"pullback_resistance_entry: depth={pullback_resistance_depth_ratio*100:.2f}% near VAH resistance"
        elif starter_frontrun_ready:
            trigger_family = "starter_frontrun"
            reason = f"starter_frontrun: gap={frontrun_gap_ratio * 100:.3f}% + 1m {int(getattr(self.config, 'starter_frontrun_impulse_bars', 3))} bullish bars + OBV confirmed"
        elif pullback_near_support and major_direction > 0 and bullish_ready:
            trigger_family = "pullback_support_entry"
            reason = f"pullback_support_entry: depth={pullback_depth_ratio*100:.2f}% near VAL support"
        elif near_breakout_release_ready:
            trigger_family = "near_breakout_release"
            reason = f"near_breakout_release: bullish_score={bullish_score:.0f} + gap={frontrun_gap_ratio * 100:.3f}% + latch_or_memory_active"
        elif trend_continuation_near_breakout_ready:
            trigger_family = "trend_continuation_near_breakout"
            reason = f"trend_continuation_near_breakout_ready: bullish_score={bullish_score:.0f} + gap={frontrun_gap_ratio * 100:.3f}% + obv_support={'confirmed' if execution_obv_ready else 'positive_zscore'}"
        elif high_confidence_price_override:
            trigger_family = "price_led_override"
            reason = f"price_led_override: bullish_score={bullish_score:.0f} + near_breakout_gap={frontrun_gap_ratio * 100:.3f}%"
        elif major_bull_retest_ready:
            trigger_family = "major_bull_retest"
            if bullish_urgency_active and not bullish_memory_active:
                if prior_high_break:
                    reason = f"major_bull_retest_ready: decaying urgency window step={bullish_urgency_decay_step}/{max(1, int(getattr(self.config, 'kdj_urgency_decay_bars', 0)))} + breakout reclaim after KDJ memory expiry ({bullish_cross_bars_ago} bars ago)"
                else:
                    reason = f"major_bull_retest_ready: decaying urgency window step={bullish_urgency_decay_step}/{max(1, int(getattr(self.config, 'kdj_urgency_decay_bars', 0)))} + near-breakout hold after KDJ memory expiry ({bullish_cross_bars_ago} bars ago)"
            else:
                reason = f"major_bull_retest_ready: gap={frontrun_gap_ratio * 100:.3f}% + KDJ memory {bullish_cross_bars_ago} bars ago"
        elif medium_confidence_latch_breakout_ready:
            trigger_family = "soft_latch_breakout"
            reason = f"soft_latch_breakout: bullish_score={bullish_score:.0f} + latch_low={latch_low_price:.4f} + breakout confirmed"
        elif major_bull_impulse_reclaim_ready:
            trigger_family = "major_bull_impulse_reclaim"
            if prior_high_break:
                reason = "major_bull_impulse_reclaim_ready: breakout reclaimed with 15m impulse despite expired KDJ memory"
            else:
                reason = f"major_bull_impulse_reclaim_ready: gap={frontrun_gap_ratio * 100:.3f}% + 15m impulse + OBV confirmed"
        elif rail_momentum_ready:
            trigger_family = "rail_momentum"
            reason = "rail_momentum_ready: near major rail + 15m momentum confirmation"
        elif weak_bull_bias and not short_bias_dominance and bullish_memory_active:
            trigger_family = "weak_bull_scale_in"
            reason = f"Weak bull bias active: KDJ crossed {bullish_cross_bars_ago} bars ago + scale-in allowed before breakout"
        elif bullish_magnetism_ready:
            trigger_family = "magnetism"
            reason = f"磁吸力预判：距离轨道 {magnetism_distance_pct:.3f}%，OBV 已确认"
        else:
            if pending_retest_bullish or pending_retest_bearish:
                state_label = "PENDING_RETEST"
            elif bullish_ready and (high_confidence_price_override or bullish_latch_active or kdj_golden_cross or frontrun_near_breakout):
                state_label = "ARMED_READY"
            elif bearish_ready and (bearish_latch_active or kdj_dead_cross or short_frontrun_near_breakout or prior_low_break):
                state_label = "ARMED_READY"
            reason = classify_waiting_execution_trigger(
                bullish_ready=bullish_ready,
                state_label=state_label,
                bullish_memory_active=bullish_memory_active,
                bullish_latch_active=bullish_latch_active,
                bullish_urgency_active=bullish_urgency_active,
                prior_high_break=prior_high_break,
                frontrun_near_breakout=frontrun_near_breakout,
                frontrun_gap_ratio=frontrun_gap_ratio,
                execution_trigger_proximity_budget_ratio=execution_trigger_proximity_budget_ratio,
            )
            trigger_family = reason

        execution_trigger = ExecutionTriggerSnapshot(
            kdj_golden_cross=kdj_golden_cross,
            kdj_dead_cross=kdj_dead_cross,
            bullish_memory_active=bullish_memory_active,
            bearish_memory_active=bearish_memory_active,
            bullish_cross_bars_ago=bullish_cross_bars_ago,
            bearish_cross_bars_ago=bearish_cross_bars_ago,
            bullish_latch_active=bullish_latch_active,
            bearish_latch_active=bearish_latch_active,
            latch_low_price=latch_low_price,
            latch_high_price=latch_high_price,
            bullish_urgency_active=bullish_urgency_active,
            bullish_urgency_decay_step=bullish_urgency_decay_step,
            prior_high_break=prior_high_break,
            prior_low_break=prior_low_break,
            prior_high=prior_high,
            prior_low=prior_low,
            frontrun_near_breakout=frontrun_near_breakout or short_frontrun_near_breakout,
            frontrun_gap_ratio=min(frontrun_gap_ratio, short_frontrun_gap_ratio) if short_frontrun_near_breakout else frontrun_gap_ratio,
            frontrun_impulse_confirmed=frontrun_impulse_confirmed,
            frontrun_obv_confirmed=execution_obv_ready,
            frontrun_ready=bool(starter_frontrun_ready or starter_short_frontrun_ready),
            pullback_near_support=pullback_near_support,
            pullback_near_resistance=pullback_near_resistance,
            pullback_depth_ratio=pullback_depth_ratio,
            pullback_resistance_depth_ratio=pullback_resistance_depth_ratio,
            stretch_value=bullish_stretch if bullish_score >= bearish_score else bearish_stretch,
            stretch_blocked=bool(stretch_blocked_bullish or stretch_blocked_bearish),
            pending_retest=bool(pending_retest_bullish or pending_retest_bearish),
            exhaustion_penalty_applied=bool(bullish_exhaustion_applied or bearish_exhaustion_applied),
            state_label=state_label,
            family=trigger_family,
            group=classify_trigger_group(trigger_family),
            reason=reason,
            liquidity_sweep=bool(bullish_liquidity_sweep.detected or bearish_liquidity_sweep.detected),
            liquidity_sweep_side=("long" if bullish_liquidity_sweep.detected else "short" if bearish_liquidity_sweep.detected else ""),
        )
        bullish_fully_aligned = early_bullish or bullish_retest_release_ready or bullish_spring_reclaim_ready or starter_frontrun_ready or high_confidence_price_override or medium_confidence_latch_breakout_ready or trend_continuation_near_breakout_ready or major_bull_retest_ready or major_bull_impulse_reclaim_ready or (
            major_direction > 0
            and bullish_ready
            and (
                ((swing_direction > 0) and prior_high_break and (bullish_memory_active or bullish_urgency_active or kdj_golden_cross))
                or (not weak_bull_bias and prior_high_break and (bullish_memory_active or bullish_urgency_active or kdj_golden_cross))
                or pullback_near_support
            )
        )
        bearish_fully_aligned = early_bearish or bearish_retest_release_ready or bearish_upthrust_reclaim_ready or starter_short_frontrun_ready or bearish_breakout_ready or bearish_retest_ready or (
            major_direction < 0 and pullback_near_resistance and bearish_ready
        )
        fully_aligned = bullish_fully_aligned or bearish_fully_aligned
        if not alignment.valid:
            bullish_ready = False
            bearish_ready = False
            fully_aligned = False

        execution_atr = float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])
        atr_price_ratio_pct = (execution_atr / current_price * 100.0) if abs(current_price) > 1e-12 else 0.0
        local_dt = datetime.now(timezone.utc)
        server_ts = self.client.fetch_server_time() if hasattr(self.client, "fetch_server_time") else None
        server_dt = datetime.fromtimestamp(server_ts / 1000.0, tz=timezone.utc) if server_ts else None
        rsi_blocking_overridden = bool(
            swing_score <= 0.0
            and major_direction > 0
            and bullish_score >= rsi_relax_score
            and not rsi_rollover_blocked
        )
        short_rsi_blocking_overridden = bool(
            swing_score <= 0.0
            and major_direction < 0
            and bearish_score >= float(getattr(self.config, "short_rsi_relax_score", 70.0))
            and current_swing_rsi <= float(getattr(self.config, "short_rsi_relax_max_threshold", 68.0))
            and swing_rsi_slope <= float(getattr(self.config, "short_rsi_relax_max_slope", 0.5))
            and (
                early_bearish
                or bearish_breakout_ready
                or bearish_retest_ready
                or bearish_upthrust_reclaim_ready
                or starter_short_frontrun_ready
                or bearish_retest_release_ready
            )
        )
        if rsi_rollover_blocked:
            fully_aligned = False
        blocker_reason = self._resolve_blocker_reason(
            data_alignment_valid=alignment.valid,
            major_direction=major_direction,
            weak_bull_bias=weak_bull_bias,
            early_bullish=early_bullish,
            swing_score=swing_score,
            bullish_ready=bullish_ready,
            fully_aligned=fully_aligned,
            execution_reason=reason,
            bullish_score=bullish_score,
            execution_frontrun_near_breakout=frontrun_near_breakout,
            drive_first_tradeable=drive_first_tradeable,
            rsi_rollover_blocked=rsi_rollover_blocked,
            bearish_score=bearish_score,
            bearish_ready=bearish_ready,
            weak_bear_bias=weak_bear_bias,
            early_bearish=early_bearish,
            short_rsi_blocking_overridden=short_rsi_blocking_overridden,
        )

        dominant_is_bullish = bool(bullish_score >= bearish_score)
        enhanced_signal_score = self._compute_signal_quality(
            base_score=(base_bullish_score + score_magnet) if dominant_is_bullish else base_bearish_score,
            strength_bonus=strength_bonus,
            total_score=bullish_score if dominant_is_bullish else bearish_score,
            ready=bullish_ready if dominant_is_bullish else bearish_ready,
            fully_aligned=fully_aligned,
        )
        logger.info(
            "Signal Quality | symbol=%s tier=%s confidence=%.2f total=%.1f base=%.1f strength=%.1f reject=%s",
            self.config.symbol,
            enhanced_signal_score.quality_tier.name,
            enhanced_signal_score.confidence,
            enhanced_signal_score.total_score,
            enhanced_signal_score.base_score,
            enhanced_signal_score.strength_bonus,
            enhanced_signal_score.rejection_reason or "n/a",
        )

        return MTFSignal(
            major_timeframe=self.config.major_timeframe,
            swing_timeframe=self.config.swing_timeframe,
            execution_timeframe=self.config.execution_timeframe,
            major_direction=major_direction,
            major_bias_score=major_bias_score,
            weak_bull_bias=weak_bull_bias,
            early_bullish=early_bullish,
            entry_size_multiplier=entry_size_multiplier,
            swing_rsi=current_swing_rsi,
            swing_rsi_slope=swing_rsi_slope,
            bullish_score=bullish_score,
            bullish_threshold=float(self.config.bullish_ready_score_threshold),
            bullish_ready=bullish_ready,
            weak_bear_bias=weak_bear_bias,
            early_bearish=early_bearish,
            bearish_score=bearish_score,
            bearish_threshold=bearish_threshold,
            bearish_ready=bearish_ready,
            execution_entry_mode=execution_entry_mode,
            execution_trigger=execution_trigger,
            fully_aligned=fully_aligned,
            current_price=current_price,
            execution_obv_zscore=float(execution_obv_confirmation.zscore),
            execution_obv_threshold=float(self.config.obv_zscore_threshold),
            execution_atr=execution_atr,
            atr_price_ratio_pct=atr_price_ratio_pct,
            server_time_iso=server_dt.isoformat() if server_dt is not None else "",
            local_time_iso=local_dt.isoformat(),
            server_local_skew_ms=(int((local_dt - server_dt).total_seconds() * 1000) if server_dt is not None else None),
            major_timestamp_ms=alignment.major_timestamp_ms,
            swing_timestamp_ms=alignment.swing_timestamp_ms,
            execution_timestamp_ms=alignment.execution_timestamp_ms,
            data_alignment_valid=alignment.valid,
            data_mismatch_ms=alignment.max_gap_ms,
            blocker_reason=blocker_reason,
            major_frame=major_frame,
            swing_frame=swing_frame,
            execution_frame=execution_frame,
            rsi_blocking_overridden=rsi_blocking_overridden,
            signal_quality_tier=enhanced_signal_score.quality_tier,
            signal_confidence=enhanced_signal_score.confidence,
            signal_strength_bonus=enhanced_signal_score.strength_bonus,
            stretch_value=bullish_stretch if bullish_score >= bearish_score else bearish_stretch,
            stretch_blocked=bool(stretch_blocked_bullish or stretch_blocked_bearish),
            pending_retest=bool(pending_retest_bullish or pending_retest_bearish),
            exhaustion_penalty_applied=bool(bullish_exhaustion_applied or bearish_exhaustion_applied),
            oi_change_pct=oi_funding_snapshot.oi_change_pct,
            funding_rate=oi_funding_snapshot.funding_rate,
            is_short_squeeze=oi_funding_snapshot.is_short_squeeze,
            is_long_liquidation=oi_funding_snapshot.is_long_liquidation,
        )
