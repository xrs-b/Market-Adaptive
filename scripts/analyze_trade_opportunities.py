from __future__ import annotations

import argparse
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ValueAreaDecision:
    inside_value_area: bool
    blocked: bool
    reason: str | None = None


def evaluate_value_area_decision(*, volume_profile, current_price: float, atr_value: float, major_direction: int, bullish_score: float, execution_frontrun_near_breakout: bool, drive_first_tradeable_score: float = 60.0, value_area_edge_atr_multiplier: float = 1.0) -> ValueAreaDecision:
    if volume_profile is None:
        return ValueAreaDecision(inside_value_area=False, blocked=False)
    inside_value_area = bool(volume_profile.contains_price(current_price))
    if not inside_value_area:
        return ValueAreaDecision(inside_value_area=False, blocked=False)
    edge_threshold = float(value_area_edge_atr_multiplier) * max(0.0, float(atr_value))
    value_area_high = float(volume_profile.value_area_high)
    value_area_low = float(volume_profile.value_area_low)
    if float(bullish_score) >= 75.0 and bool(execution_frontrun_near_breakout):
        return ValueAreaDecision(inside_value_area=True, blocked=False, reason="High Momentum")
    if int(major_direction) > 0 and float(bullish_score) >= float(drive_first_tradeable_score) and float(current_price) >= value_area_high - edge_threshold:
        return ValueAreaDecision(inside_value_area=True, blocked=False, reason="Edge Proximity")
    if int(major_direction) < 0 and float(current_price) < value_area_low + edge_threshold:
        return ValueAreaDecision(inside_value_area=True, blocked=False, reason="Edge Proximity")
    return ValueAreaDecision(inside_value_area=True, blocked=True)
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import pandas as pd

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import load_config
from market_adaptive.indicators import (
    compute_atr,
    compute_indicator_snapshot,
    compute_kdj,
    compute_obv,
    compute_obv_confirmation_snapshot,
    compute_rsi,
    compute_supertrend,
    compute_volume_profile,
    ohlcv_to_dataframe,
)
from market_adaptive.oracles.market_oracle import indicator_confirms_trend
from market_adaptive.strategies.mtf_engine import (
    classify_waiting_execution_trigger,
    resolve_execution_trigger_proximity_budget_ratio,
)
from market_adaptive.strategies.obv_gate import resolve_dynamic_obv_gate
from market_adaptive.strategies.order_flow_sentinel import OrderFlowSentinel
from market_adaptive.timeframe_utils import maybe_use_closed_candles


@dataclass
class CTAAuditRow:
    ts: pd.Timestamp
    market_regime: str
    blocker: str
    passed_market_regime: bool
    passed_mtf_regime: bool
    passed_bullish_ready: bool
    passed_trigger: bool
    passed_obv: bool
    passed_volume_profile: bool
    sentiment_blocked: bool
    risk_blocked: bool
    trigger_reason: str
    swing_rsi: float
    bullish_score: float
    obv_zscore: float
    current_price: float


def _timeframe_to_ms(raw: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    raw = raw.lower()
    return int(raw[:-1]) * units[raw[-1]]


def _boundary_residual(diff_ms: int, boundary_ms: int) -> int:
    remainder = abs(diff_ms) % boundary_ms
    return min(remainder, boundary_ms - remainder if remainder else 0)


def _slice_until(frame: pd.DataFrame, ts: pd.Timestamp) -> pd.DataFrame:
    subset = frame[frame["timestamp"] <= ts]
    return subset.copy()


def _major_bull_retest_near_breakout_ready(
    *,
    cta,
    major_direction: int,
    bullish_ready: bool,
    bullish_memory_active: bool,
    frontrun_gap_ratio: float,
) -> bool:
    if major_direction <= 0 or not bullish_ready or not bullish_memory_active:
        return False
    tolerance_ratio = max(
        float(getattr(cta, "starter_frontrun_breakout_buffer_ratio", 0.002)),
        float(getattr(cta, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
    )
    return frontrun_gap_ratio <= tolerance_ratio


def _trend_continuation_near_breakout_ready(
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


def _resolve_bullish_urgency_window(
    *,
    cta,
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
    memory_bars = max(1, int(cta.kdj_signal_memory_bars))
    decay_bars = max(0, int(getattr(cta, "kdj_urgency_decay_bars", 0)))
    if decay_bars <= 0 or bullish_cross_bars_ago < memory_bars:
        return False, None, False
    decay_step = bullish_cross_bars_ago - memory_bars + 1
    if decay_step < 1 or decay_step > decay_bars:
        return False, None, False
    retest_tolerance_ratio = max(
        float(getattr(cta, "starter_frontrun_breakout_buffer_ratio", 0.002)),
        float(getattr(cta, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
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


def fetch_ohlcv_df(client: OKXClient, symbol: str, timeframe: str, *, limit_per_call: int = 200, prefer_closed: bool = True) -> pd.DataFrame:
    rows = client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit_per_call)
    rows = maybe_use_closed_candles(rows, enabled=prefer_closed)
    return ohlcv_to_dataframe(rows)


def replay_cta(config_path: Path, hours: int) -> dict:
    cfg = load_config(config_path)
    client = OKXClient(cfg.okx, cfg.execution)
    cta = cfg.cta
    oracle = cfg.market_oracle
    symbol = cta.symbol

    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(hours=hours)
    major = fetch_ohlcv_df(client, symbol, cta.major_timeframe, prefer_closed=cta.prefer_closed_major_timeframe_candles)
    swing = fetch_ohlcv_df(client, symbol, cta.swing_timeframe, prefer_closed=cta.prefer_closed_swing_timeframe_candles)
    execution = fetch_ohlcv_df(
        client,
        symbol,
        cta.execution_timeframe,
        prefer_closed=cta.prefer_closed_execution_timeframe_candles,
    )
    oracle_high = fetch_ohlcv_df(client, symbol, oracle.higher_timeframe, prefer_closed=oracle.prefer_closed_higher_timeframe_candles)
    oracle_low = fetch_ohlcv_df(client, symbol, oracle.lower_timeframe, prefer_closed=oracle.prefer_closed_lower_timeframe_candles)

    execution_window = execution.copy()
    sentinel = OrderFlowSentinel(client, cta)

    try:
        ratio_history = client.fetch_long_short_account_ratio_history(symbol, timeframe=cfg.sentiment.timeframe, limit=min(hours * 12 + 50, 1000))
    except Exception:
        ratio_history = []
    ratio_frame = pd.DataFrame(ratio_history)
    if not ratio_frame.empty:
        ratio_frame["timestamp"] = pd.to_datetime(ratio_frame["timestamp"].astype("int64"), unit="ms", utc=True)
        ratio_col = "longShortRatio"
        if ratio_col not in ratio_frame.columns:
            ratio_col = "longShortAccountRatio" if "longShortAccountRatio" in ratio_frame.columns else ratio_frame.columns[-1]
        ratio_frame["ratio"] = pd.to_numeric(ratio_frame[ratio_col], errors="coerce")
        ratio_frame = ratio_frame[["timestamp", "ratio"]].dropna().sort_values("timestamp")

    rows: list[CTAAuditRow] = []
    order_flow_samples: list[dict] = []
    for idx, row in execution_window.iterrows():
        ts = row["timestamp"]
        major_slice = _slice_until(major, ts)
        swing_slice = _slice_until(swing, ts)
        exec_slice = _slice_until(execution, ts)
        oracle_high_slice = _slice_until(oracle_high, ts)
        oracle_low_slice = _slice_until(oracle_low, ts)
        if min(len(major_slice), len(swing_slice), len(exec_slice), len(oracle_high_slice), len(oracle_low_slice)) < max(cta.lookback_limit // 2, 120):
            continue

        high_snapshot = compute_indicator_snapshot(oracle_high_slice.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
        low_snapshot = compute_indicator_snapshot(oracle_low_slice.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
        trend_detected = any(indicator_confirms_trend(i, oracle) for i in (high_snapshot, low_snapshot))
        market_regime = "trend" if trend_detected else "sideways"
        passed_market_regime = market_regime in {"trend", "trend_impulse"}

        major_frame = major_slice.tail(cta.lookback_limit).copy()
        swing_frame = swing_slice.tail(cta.lookback_limit).copy()
        exec_frame = exec_slice.tail(cta.lookback_limit).copy()

        major_supertrend = compute_supertrend(major_frame, length=cta.supertrend_period, multiplier=cta.supertrend_multiplier)
        major_direction = int(major_supertrend["direction"].iloc[-1])
        swing_rsi = compute_rsi(swing_frame, length=cta.swing_rsi_period)
        current_rsi = float(swing_rsi.iloc[-1])
        previous_rsi = float(swing_rsi.iloc[-2])
        rsi_slope = current_rsi - previous_rsi
        rsi_sma = swing_rsi.rolling(max(2, int(cta.recovery_rsi_sma_period)), min_periods=1).mean()
        current_rsi_sma = float(rsi_sma.iloc[-1])
        previous_rsi_sma = float(rsi_sma.iloc[-2])
        momentum_recovery = current_rsi > float(cta.recovery_rsi_floor) and current_rsi > current_rsi_sma and (previous_rsi <= previous_rsi_sma or rsi_slope > 0)
        if momentum_recovery:
            swing_score = float(cta.dynamic_rsi_trend_score)
        else:
            rebound_window = max(2, int(cta.rsi_rebound_lookback))
            recent_min_rsi = float(swing_rsi.tail(rebound_window).min())
            oversold_rebound = recent_min_rsi < float(cta.rsi_oversold_threshold) and current_rsi >= float(cta.rsi_rebound_confirmation_level) and rsi_slope > 0
            swing_score = float(cta.dynamic_rsi_rebound_score) if oversold_rebound else 0.0

        swing_supertrend = compute_supertrend(swing_frame, length=cta.supertrend_period, multiplier=cta.supertrend_multiplier)
        recovery_ema = swing_frame["close"].ewm(span=cta.recovery_ema_period, adjust=False).mean()
        slope_lookback = max(1, min(int(cta.recovery_ema_slope_lookback), len(recovery_ema) - 1))
        ema_slope = float(recovery_ema.iloc[-1]) - float(recovery_ema.iloc[-1 - slope_lookback])
        flat_tolerance = float(cta.recovery_ema_flat_tolerance_atr_ratio) * max(float(major_supertrend["atr"].iloc[-1]), 1e-12)
        weak_bull_bias = major_direction <= 0 and float(swing_frame["close"].iloc[-1]) > float(recovery_ema.iloc[-1]) and ema_slope >= -flat_tolerance
        if major_direction > 0:
            major_bias_score = float(cta.strong_bull_bias_score)
        else:
            major_bias_score = float(cta.weak_bull_bias_score) if weak_bull_bias else 0.0
        swing_direction = int(swing_supertrend["direction"].iloc[-1])
        early_bullish = False
        if major_direction <= 0 and len(major_frame) >= 2:
            current_price = float(swing_frame["close"].iloc[-1])
            current_lower_band = float(major_supertrend["lower_band"].iloc[-1])
            previous_lower_band = float(major_supertrend["lower_band"].iloc[-2])
            current_atr = float(major_supertrend["atr"].iloc[-1])
            lower_band_slope = current_lower_band - previous_lower_band
            minimum_slope = -float(cta.early_bullish_lower_band_slope_atr_threshold) * max(current_atr, 1e-12)
            early_bullish = swing_direction > 0 and current_price > current_lower_band and lower_band_slope >= minimum_slope
        early_recovery_score = 0.0
        if early_bullish and float(getattr(cta, "early_bullish_score_bonus", 0.0)) > 0.0:
            rsi_buffer = max(0.0, 0.5 * abs(rsi_slope))
            recovery_still_supported = current_rsi >= (current_rsi_sma - rsi_buffer)
            if recovery_still_supported and current_rsi >= float(cta.swing_rsi_ready_threshold):
                early_recovery_score = float(cta.early_bullish_score_bonus)
        score_4h = float(cta.strong_bull_bias_score) if major_direction > 0 else 0.0
        score_1h = 0.0
        if score_4h <= 0.0:
            if swing_direction > 0:
                score_1h = float(getattr(cta, "swing_supertrend_bullish_score", 0.0))
            elif weak_bull_bias:
                score_1h = float(cta.weak_bull_bias_score)
        bullish_score = score_4h + score_1h + swing_score + early_recovery_score
        execution_obv = compute_obv(exec_frame)
        execution_obv_confirmation = compute_obv_confirmation_snapshot(exec_frame, obv=execution_obv, sma_period=cta.obv_sma_period, zscore_window=cta.obv_zscore_window)
        execution_atr = float(compute_atr(exec_frame, length=cta.atr_period).iloc[-1])
        current_price = float(exec_frame["close"].iloc[-1])
        if major_direction <= 0 and not (weak_bull_bias or early_bullish):
            relevant_rail = float(major_supertrend["upper_band"].iloc[-1])
            current_major_atr = float(major_supertrend["atr"].iloc[-1])
            rail_distance = abs(current_price - relevant_rail)
            magnetism_limit = float(cta.magnetism_rail_atr_multiplier) * current_major_atr
            if current_major_atr > 0 and rail_distance < magnetism_limit and execution_obv_confirmation.zscore > float(cta.magnetism_obv_zscore_threshold):
                bullish_score = max(bullish_score, float(cta.bullish_ready_score_threshold))
        bullish_ready = bullish_score >= float(cta.bullish_ready_score_threshold)
        drive_first_tradeable = bullish_score >= float(getattr(cta, "drive_first_tradeable_score", 60.0))

        execution_kdj = compute_kdj(exec_frame, length=cta.kdj_length, k_smoothing=cta.kdj_k_smoothing, d_smoothing=cta.kdj_d_smoothing)
        current_k = float(execution_kdj["k"].iloc[-1]); current_d = float(execution_kdj["d"].iloc[-1])
        previous_k = float(execution_kdj["k"].iloc[-2]); previous_d = float(execution_kdj["d"].iloc[-2])
        kdj_golden_cross = previous_k <= previous_d and current_k > current_d
        kdj_dead_cross = previous_k >= previous_d and current_k < current_d
        bullish_cross_mask = (execution_kdj["k"].shift(1) <= execution_kdj["d"].shift(1)) & (execution_kdj["k"] > execution_kdj["d"])
        recent = bullish_cross_mask.fillna(False).astype(bool).tail(max(1, int(cta.kdj_signal_memory_bars)) + max(0, int(getattr(cta, "kdj_urgency_decay_bars", 0))))
        bullish_cross_bars_ago = None
        for bars_ago, value in enumerate(reversed(recent.tolist())):
            if value:
                bullish_cross_bars_ago = bars_ago
                break
        bullish_memory_active = bullish_cross_bars_ago is not None and bullish_cross_bars_ago < max(1, int(cta.kdj_signal_memory_bars))
        prior_high = exec_frame["high"].shift(1).rolling(max(1, int(cta.execution_breakout_lookback)), min_periods=max(1, int(cta.execution_breakout_lookback))).max().iloc[-1]
        prior_high_break = not pd.isna(prior_high) and current_price > float(prior_high)
        frontrun_gap_ratio = 999.0
        if not pd.isna(prior_high) and abs(float(prior_high)) > 1e-12:
            frontrun_gap_ratio = max(0.0, (float(prior_high) - current_price) / float(prior_high))
        frontrun_near_breakout = frontrun_gap_ratio <= float(getattr(cta, "starter_frontrun_breakout_buffer_ratio", 0.002))
        impulse_bars = max(2, int(getattr(cta, "starter_frontrun_impulse_bars", 3)))
        volume_window = max(impulse_bars + 1, int(getattr(cta, "starter_frontrun_volume_window", 12)))
        frontrun_impulse_confirmed = False
        if len(exec_frame) >= volume_window:
            recent = exec_frame.tail(impulse_bars)
            baseline = exec_frame.iloc[:-impulse_bars].tail(volume_window)
            volume_mean = float(baseline["volume"].mean()) if not baseline.empty else 0.0
            if volume_mean > 0.0:
                bullish_bars = bool((recent["close"] > recent["open"]).all())
                volume_multiplier = float(getattr(cta, "starter_frontrun_volume_multiplier", 1.15))
                supported_volume = bool((recent["volume"] >= volume_mean * volume_multiplier).all())
                frontrun_impulse_confirmed = bullish_bars and supported_volume
        execution_obv_ready = execution_obv_confirmation.buy_confirmed(zscore_threshold=float(cta.obv_zscore_threshold))
        high_confidence_price_override = bool(bullish_score >= 75.0 and frontrun_near_breakout and not kdj_dead_cross)
        bullish_urgency_active, bullish_urgency_decay_step, bullish_urgency_trigger_ready = _resolve_bullish_urgency_window(
            cta=cta,
            bullish_ready=bullish_ready,
            kdj_dead_cross=kdj_dead_cross,
            bullish_cross_bars_ago=bullish_cross_bars_ago,
            prior_high_break=prior_high_break,
            frontrun_near_breakout=frontrun_near_breakout,
            frontrun_gap_ratio=frontrun_gap_ratio,
            frontrun_impulse_confirmed=frontrun_impulse_confirmed,
            execution_obv_ready=execution_obv_ready,
            major_direction=major_direction,
        )
        major_bull_retest_ready = _major_bull_retest_near_breakout_ready(
            cta=cta,
            major_direction=major_direction,
            bullish_ready=bullish_ready,
            bullish_memory_active=bullish_memory_active,
            frontrun_gap_ratio=frontrun_gap_ratio,
        ) or bullish_urgency_trigger_ready
        trend_continuation_near_breakout_ready = _trend_continuation_near_breakout_ready(
            major_direction=major_direction,
            bullish_score=bullish_score,
            frontrun_near_breakout=frontrun_near_breakout,
            prior_high_break=prior_high_break,
            kdj_dead_cross=kdj_dead_cross,
            execution_obv_ready=execution_obv_ready,
            execution_obv_zscore=float(execution_obv_confirmation.zscore),
        )
        major_bull_impulse_reclaim_ready = bool(
            major_direction > 0
            and bullish_ready
            and frontrun_impulse_confirmed
            and (prior_high_break or (execution_obv_ready and frontrun_near_breakout))
            and not (bullish_memory_active or kdj_golden_cross)
        )
        execution_trigger_proximity_budget_ratio = resolve_execution_trigger_proximity_budget_ratio(
            starter_frontrun_breakout_buffer_ratio=float(getattr(cta, "starter_frontrun_breakout_buffer_ratio", 0.002)),
            bullish_memory_retest_breakout_buffer_ratio=float(getattr(cta, "bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
        )
        trigger_reason = classify_waiting_execution_trigger(
            bullish_ready=bullish_ready,
            state_label="ARMED_READY" if bullish_ready and (kdj_golden_cross or frontrun_near_breakout) else "WAITING_SETUP",
            bullish_memory_active=bullish_memory_active,
            bullish_latch_active=False,
            bullish_urgency_active=bullish_urgency_active,
            prior_high_break=prior_high_break,
            frontrun_near_breakout=frontrun_near_breakout,
            frontrun_gap_ratio=frontrun_gap_ratio,
            execution_trigger_proximity_budget_ratio=execution_trigger_proximity_budget_ratio,
        )
        if early_bullish:
            trigger_reason = "early_bullish"
        elif bullish_memory_active and prior_high_break:
            trigger_reason = "memory+breakout"
        elif trend_continuation_near_breakout_ready:
            trigger_reason = "trend_continuation_near_breakout_ready"
        elif high_confidence_price_override:
            trigger_reason = "price_led_override"
        elif major_bull_retest_ready:
            trigger_reason = "major_bull_retest_ready_urgency" if bullish_urgency_active and not bullish_memory_active else "major_bull_retest_ready"
        elif major_bull_impulse_reclaim_ready:
            trigger_reason = "major_bull_impulse_reclaim_ready"
        elif weak_bull_bias and bullish_memory_active:
            trigger_reason = "weak_bias_scale_in"
        elif kdj_golden_cross:
            trigger_reason = "kdj_cross_wait_breakout"
        elif prior_high_break:
            trigger_reason = "breakout_wait_memory"
        fully_aligned = early_bullish or trend_continuation_near_breakout_ready or high_confidence_price_override or major_bull_retest_ready or major_bull_impulse_reclaim_ready or (bullish_ready and ((weak_bull_bias and bullish_memory_active) or ((not weak_bull_bias) and prior_high_break and (bullish_memory_active or bullish_urgency_active or kdj_golden_cross))))

        major_ts = int(pd.Timestamp(major_frame["timestamp"].iloc[-1]).value // 1_000_000)
        swing_ts = int(pd.Timestamp(swing_frame["timestamp"].iloc[-1]).value // 1_000_000)
        execution_ts = int(pd.Timestamp(exec_frame["timestamp"].iloc[-1]).value // 1_000_000)
        max_gap_ms = max(
            _boundary_residual(major_ts - swing_ts, _timeframe_to_ms(cta.swing_timeframe)),
            _boundary_residual(swing_ts - execution_ts, _timeframe_to_ms(cta.execution_timeframe)),
            _boundary_residual(major_ts - execution_ts, _timeframe_to_ms(cta.execution_timeframe)),
        )
        data_alignment_valid = max_gap_ms <= _timeframe_to_ms(cta.execution_timeframe)
        if not data_alignment_valid:
            bullish_ready = False
            fully_aligned = False

        passed_mtf_regime = bool(major_direction > 0 or weak_bull_bias or early_bullish)
        passed_bullish_ready = bool(bullish_ready)
        passed_trigger = bool(fully_aligned)
        obv_gate = resolve_dynamic_obv_gate(
            bullish_score=bullish_score,
            configured_threshold=cta.obv_zscore_threshold,
            major_direction=major_direction,
            early_bullish=early_bullish,
            weak_bull_bias=weak_bull_bias,
            execution_frontrun_near_breakout=frontrun_near_breakout,
            trigger_reason=trigger_reason,
            execution_entry_mode=(
                "early_bullish_starter_limit"
                if early_bullish
                else "weak_bull_scale_in_limit"
                if weak_bull_bias
                else "breakout_confirmed"
            ),
        )
        relaxed_obv_allowed = bool(
            major_direction > 0
            and drive_first_tradeable
            and float(execution_obv_confirmation.zscore) > float(obv_gate.threshold)
        )
        volume_filter_passed = fully_aligned and (obv_gate.passed(execution_obv_confirmation) or relaxed_obv_allowed)
        passed_obv = bool(volume_filter_passed)
        volume_profile = compute_volume_profile(exec_frame, lookback_hours=cta.volume_profile_lookback_hours, value_area_pct=cta.volume_profile_value_area_pct, bin_count=cta.volume_profile_bin_count)
        value_area_decision = evaluate_value_area_decision(
            volume_profile=volume_profile,
            current_price=current_price,
            atr_value=execution_atr,
            major_direction=major_direction,
            bullish_score=bullish_score,
            execution_frontrun_near_breakout=frontrun_near_breakout,
            drive_first_tradeable_score=float(getattr(cta, "drive_first_tradeable_score", 60.0)),
            value_area_edge_atr_multiplier=float(getattr(cta, "value_area_edge_atr_multiplier", 1.0)),
        )
        passed_volume_profile = bool(volume_profile and volume_profile.above_poc(current_price) and (not value_area_decision.blocked) and (value_area_decision.reason is not None or volume_profile.above_value_area(current_price)))

        blocker = "PASSED"
        if not passed_market_regime:
            blocker = "MARKET_REGIME_SIDEWAYS"
        elif not data_alignment_valid:
            blocker = "DATA_MISMATCH_WARNING"
        elif not passed_mtf_regime:
            blocker = "Blocked_By_SuperTrend_Regime"
        elif not passed_bullish_ready:
            blocker = "Blocked_By_Bullish_Score" if bullish_score > 0 else "Blocked_By_RSI_Threshold"
        elif not passed_trigger:
            blocker = f"Blocked_By_Trigger:{trigger_reason}"
        elif not obv_gate.exempt and not execution_obv_confirmation.above_sma:
            blocker = "Blocked_By_OBV_BELOW_SMA"
        elif not volume_filter_passed:
            blocker = "Blocked_By_OBV_STRENGTH_NOT_CONFIRMED"
        elif volume_profile is None:
            blocker = "Blocked_By_MISSING_VOLUME_PROFILE"
        elif not volume_profile.above_poc(current_price):
            blocker = "Blocked_By_BELOW_POC"
        elif value_area_decision.blocked:
            blocker = "Blocked_By_INSIDE_VALUE_AREA"
        elif not value_area_decision.reason and not volume_profile.above_value_area(current_price):
            blocker = "Blocked_By_BELOW_VALUE_AREA_HIGH"

        sentiment_blocked = False
        if blocker == "PASSED" and not ratio_frame.empty:
            current_ratio_rows = ratio_frame[ratio_frame["timestamp"] <= ts]
            if not current_ratio_rows.empty:
                latest_ratio = float(current_ratio_rows.iloc[-1]["ratio"])
                sentiment_blocked = latest_ratio >= float(cfg.sentiment.extreme_bullish_ratio) and cfg.sentiment.normalized_cta_buy_action == "block"
                if sentiment_blocked:
                    blocker = "cta:sentiment_blocked"

        risk_blocked = False
        if blocker == "PASSED":
            target_notional = client.fetch_total_equity("USDT") * float(cta.margin_fraction_per_trade) * float(cta.nominal_leverage)
            symbol_limit = float(cfg.risk_control.resolve_symbol_notional_limit(symbol))
            risk_blocked = symbol_limit > 0 and target_notional > symbol_limit + 1e-9
            if risk_blocked:
                blocker = f"cta:risk_blocked:symbol_limit={symbol_limit:.0f}<target_notional={target_notional:.0f}"
                if len(order_flow_samples) < 20:
                    try:
                        assessment = sentinel.assess_entry(symbol, "buy", 0.01)
                        order_flow_samples.append({
                            "ts": ts.isoformat(),
                            "imbalance_ratio": assessment.imbalance_ratio,
                            "passed": assessment.entry_allowed,
                            "reason": assessment.reason,
                        })
                    except Exception as exc:
                        order_flow_samples.append({"ts": ts.isoformat(), "error": type(exc).__name__})

        rows.append(CTAAuditRow(ts=ts, market_regime=market_regime, blocker=blocker, passed_market_regime=passed_market_regime, passed_mtf_regime=passed_mtf_regime, passed_bullish_ready=passed_bullish_ready, passed_trigger=passed_trigger, passed_obv=passed_obv, passed_volume_profile=passed_volume_profile, sentiment_blocked=sentiment_blocked, risk_blocked=risk_blocked, trigger_reason=trigger_reason, swing_rsi=current_rsi, bullish_score=bullish_score, obv_zscore=float(execution_obv_confirmation.zscore), current_price=current_price))

    df = pd.DataFrame([r.__dict__ for r in rows])
    blocker_counts = df["blocker"].value_counts().to_dict() if not df.empty else {}
    result = {
        "window_hours": hours,
        "cycles": int(len(df)),
        "market_regime_counts": df["market_regime"].value_counts().to_dict() if not df.empty else {},
        "funnel": {
            "passed_market_regime": int(df["passed_market_regime"].sum()) if not df.empty else 0,
            "passed_mtf_regime": int(df["passed_mtf_regime"].sum()) if not df.empty else 0,
            "passed_bullish_ready": int(df["passed_bullish_ready"].sum()) if not df.empty else 0,
            "passed_trigger": int(df["passed_trigger"].sum()) if not df.empty else 0,
            "passed_obv": int(df["passed_obv"].sum()) if not df.empty else 0,
            "passed_volume_profile": int(df["passed_volume_profile"].sum()) if not df.empty else 0,
            "sentiment_blocked": int(df["sentiment_blocked"].sum()) if not df.empty else 0,
            "risk_blocked": int(df["risk_blocked"].sum()) if not df.empty else 0,
        },
        "blockers": blocker_counts,
        "samples": df.sort_values("ts").tail(8).to_dict(orient="records") if not df.empty else [],
        "order_flow_live_probe": {
            "samples": order_flow_samples,
            "pass_count": sum(1 for x in order_flow_samples if x.get("passed")),
            "sample_count": len(order_flow_samples),
        },
    }
    return result


def replay_grid(config_path: Path, hours: int) -> dict:
    cfg = load_config(config_path)
    client = OKXClient(cfg.okx, cfg.execution)
    grid = cfg.grid
    oracle = cfg.market_oracle
    symbol = grid.symbol
    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(hours=hours)
    one_min = fetch_ohlcv_df(client, symbol, "1m", prefer_closed=True)
    one_hour = fetch_ohlcv_df(client, symbol, grid.atr_timeframe, prefer_closed=True)
    low = fetch_ohlcv_df(client, symbol, oracle.lower_timeframe, prefer_closed=True)
    high = fetch_ohlcv_df(client, symbol, oracle.higher_timeframe, prefer_closed=True)

    grid_rows = []
    poll_index = one_min.iloc[:: max(1, int(grid.polling_interval_seconds // 60)) or 1].copy()
    if poll_index.empty:
        poll_index = one_min.copy()
    for _, minute_row in poll_index.iterrows():
        ts = minute_row["timestamp"]
        current_price = float(minute_row["close"])
        hour_slice = _slice_until(one_hour, ts)
        low_slice = _slice_until(low, ts)
        high_slice = _slice_until(high, ts)
        if min(len(hour_slice), len(low_slice), len(high_slice)) < 50:
            continue
        frame_hour = hour_slice.tail(max(grid.atr_period * 4, 80)).copy()
        atr_value = float(compute_atr(frame_hour, length=grid.atr_period).iloc[-1])
        high_snapshot = compute_indicator_snapshot(high_slice.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
        low_snapshot = compute_indicator_snapshot(low_slice.tail(oracle.lookback_limit).values.tolist(), adx_length=oracle.adx_length, bb_length=oracle.bb_length, bb_std=oracle.bb_std)
        adx_trend_allows = high_snapshot.adx_trend_label in {"flat", "falling"}
        weighted_gap = (high_snapshot.plus_di_value - high_snapshot.minus_di_value) * 0.6 + (low_snapshot.plus_di_value - low_snapshot.minus_di_value) * 0.4
        strongest_adx = max(high_snapshot.adx_value, low_snapshot.adx_value, 1.0)
        bias_value = float(weighted_gap / strongest_adx)
        threshold = float(grid.directional_bias_threshold)
        if bias_value >= threshold:
            buy_levels = int(grid.bullish_buy_levels); sell_levels = int(grid.bullish_sell_levels)
            center_shift = max(0.0, atr_value * float(grid.bullish_center_shift_atr_ratio))
            buy_spacing_ratio = float(grid.bullish_buy_spacing_ratio); sell_spacing_ratio = float(grid.bullish_sell_spacing_ratio)
            bias_label = "bullish"
        elif bias_value <= -threshold:
            buy_levels = int(grid.bearish_buy_levels); sell_levels = int(grid.bearish_sell_levels)
            center_shift = -max(0.0, atr_value * float(grid.bearish_center_shift_atr_ratio))
            buy_spacing_ratio = float(grid.bearish_buy_spacing_ratio); sell_spacing_ratio = float(grid.bearish_sell_spacing_ratio)
            bias_label = "bearish"
        else:
            buy_levels = max(1, grid.levels // 2); sell_levels = max(1, grid.levels - buy_levels)
            center_shift = 0.0
            buy_spacing_ratio = sell_spacing_ratio = 0.0
            bias_label = "neutral"
        center = current_price + center_shift
        lower_bound = center - float(grid.atr_multiplier) * atr_value
        upper_bound = center + float(grid.atr_multiplier) * atr_value
        buy_step = max((center - lower_bound) / buy_levels, center * max(grid.min_spacing_ratio, buy_spacing_ratio), 1e-12)
        sell_step = max((upper_bound - center) / sell_levels, center * max(grid.min_spacing_ratio, sell_spacing_ratio), 1e-12)
        buy_prices = [center - buy_step * (i + 1) for i in range(buy_levels)]
        sell_prices = [center + sell_step * (i + 1) for i in range(sell_levels)]
        nearest_buy_gap_pct = ((current_price - max(buy_prices)) / current_price * 100.0) if buy_prices else None
        nearest_sell_gap_pct = ((min(sell_prices) - current_price) / current_price * 100.0) if sell_prices else None

        future = one_min[(one_min["timestamp"] > ts) & (one_min["timestamp"] <= ts + pd.Timedelta(minutes=60))]
        if future.empty:
            continue
        min_low = float(future["low"].min())
        max_high = float(future["high"].max())
        buy_hits = sum(1 for p in buy_prices if min_low <= p)
        sell_hits = sum(1 for p in sell_prices if max_high >= p)
        total_hits = buy_hits + sell_hits
        range_1h_pct = (max_high - min_low) / current_price * 100.0 if current_price else 0.0
        grid_rows.append({
            "ts": ts.isoformat(),
            "price": current_price,
            "atr": atr_value,
            "adx_trend_allows": adx_trend_allows,
            "bias": bias_label,
            "bias_value": bias_value,
            "buy_levels": buy_levels,
            "sell_levels": sell_levels,
            "nearest_buy_gap_pct": nearest_buy_gap_pct,
            "nearest_sell_gap_pct": nearest_sell_gap_pct,
            "range_1h_pct": range_1h_pct,
            "buy_hits_1h": buy_hits,
            "sell_hits_1h": sell_hits,
            "total_hits_1h": total_hits,
        })

    df = pd.DataFrame(grid_rows)
    if df.empty:
        return {"window_hours": hours, "cycles": 0}
    def _median(col):
        series = [float(x) for x in df[col].dropna().tolist()]
        return median(series) if series else None
    return {
        "window_hours": hours,
        "cycles": int(len(df)),
        "adx_trend_allows_rate_pct": round(float(df["adx_trend_allows"].mean() * 100.0), 2),
        "bias_counts": df["bias"].value_counts().to_dict(),
        "median_nearest_buy_gap_pct": _median("nearest_buy_gap_pct"),
        "median_nearest_sell_gap_pct": _median("nearest_sell_gap_pct"),
        "median_range_1h_pct": _median("range_1h_pct"),
        "hours_with_zero_hits_pct": round(float((df["total_hits_1h"] == 0).mean() * 100.0), 2),
        "hours_with_buy_hits_pct": round(float((df["buy_hits_1h"] > 0).mean() * 100.0), 2),
        "hours_with_sell_hits_pct": round(float((df["sell_hits_1h"] > 0).mean() * 100.0), 2),
        "samples": df.tail(8).to_dict(orient="records"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    config_path = Path(args.config)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cta": replay_cta(config_path, args.hours),
        "grid": replay_grid(config_path, args.hours),
    }
    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
