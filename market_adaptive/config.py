from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class OKXConfig:
    api_key: str
    api_secret: str
    passphrase: str
    sandbox: bool = True
    simulated_id: str = "1"
    simulated_trading: bool = True
    default_type: str = "swap"
    timeout_ms: int = 10_000

    @property
    def headers(self) -> dict[str, str]:
        headers = {"x-simulated-id": str(self.simulated_id)}
        if self.simulated_trading:
            headers["x-simulated-trading"] = "1"
        return headers


@dataclass
class DatabaseConfig:
    path: Path


@dataclass
class DiscordNotificationConfig:
    enabled: bool = False
    channel_id: str = ""
    webhook_url: str = ""
    bot_token: str = ""
    username: str = "Market-Adaptive"


@dataclass
class NotificationConfig:
    discord: DiscordNotificationConfig


@dataclass
class RuntimeConfig:
    timezone: str = "Asia/Shanghai"
    default_timeframe: str = "1h"
    default_ohlcv_limit: int = 200
    account_check_interval_seconds: int = 60
    risk_check_interval_seconds: int = 60
    fast_risk_check_interval_seconds: int = 1
    shutdown_cancel_open_orders: bool = True
    start_grid_websocket_on_boot: bool = True
    shutdown_join_timeout_seconds: float = 5.0
    account_initial_equity: float = 0.0


@dataclass
class RiskControlConfig:
    daily_loss_warning_pct: float = 0.03
    daily_loss_warning_scale: float = 0.50
    daily_loss_stop_openings_pct: float = 0.05
    daily_loss_reduce_exposure_pct: float = 0.07
    daily_loss_cutoff_pct: float = 0.05
    max_margin_ratio: float = 0.60
    recovery_check_interval_seconds: int = 60
    position_sync_tolerance: float = 1e-6
    default_symbol_max_notional: float = 0.0
    symbol_notional_limits: dict[str, float] = field(default_factory=dict)
    cta_single_trade_equity_multiple: float = 0.0
    max_directional_leverage: float = 8.0
    grid_margin_ratio_warning: float = 0.45
    grid_deviation_reduce_ratio: float = 0.25
    grid_liquidation_warning_ratio: float = 0.10
    grid_reduction_step_pct: float = 0.25
    grid_reduction_cooldown_seconds: int = 300

    def resolve_symbol_notional_limit(self, symbol: str) -> float:
        if symbol in self.symbol_notional_limits:
            return float(self.symbol_notional_limits[symbol])
        return float(self.default_symbol_max_notional)


@dataclass
class SentimentConfig:
    enabled: bool = True
    symbol: str = ""
    timeframe: str = "5m"
    lookback_limit: int = 1
    extreme_bullish_ratio: float = 2.5
    cta_buy_action: str = "block"
    gradient_reduce_ratio_threshold: float = 3.0
    gradient_reduce_ratio_multiplier: float = 0.7
    gradient_heavy_reduce_ratio_threshold: float = 3.5
    gradient_heavy_reduce_ratio_multiplier: float = 0.4

    def resolve_symbol(self, fallback_symbol: str) -> str:
        return self.symbol or fallback_symbol

    @property
    def normalized_cta_buy_action(self) -> str:
        action = self.cta_buy_action.strip().lower()
        return action if action in {"block", "halve", "gradient"} else "block"


@dataclass
class MarketOracleConfig:
    symbol: str = "BTC/USDT"
    polling_interval_seconds: int = 300
    higher_timeframe: str = "1h"
    lower_timeframe: str = "15m"
    lookback_limit: int = 200
    adx_length: int = 14
    bb_length: int = 20
    bb_std: float = 2.0
    trend_adx_threshold: float = 18.0
    sideways_adx_threshold: float = 15.0
    trend_di_gap_threshold: float = 8.0
    relaxed_trend_adx_buffer: float = 3.0
    relaxed_trend_di_gap_bonus: float = 1.0
    bb_width_contraction_tolerance_ratio: float = 0.03
    range_breakout_bb_width_threshold: float = 0.10
    range_breakout_volatility_threshold: float = 0.015
    prefer_closed_higher_timeframe_candles: bool = True
    prefer_closed_lower_timeframe_candles: bool = True
    impulse_timeframe: str = "1m"
    prefer_closed_impulse_candles: bool = False
    impulse_consecutive_bars: int = 3
    impulse_volume_window: int = 12
    impulse_volume_multiplier: float = 1.2
    short_regime_thaw_enabled: bool = True
    short_regime_thaw_adx_floor: float = 15.0
    short_regime_thaw_di_gap_floor: float = 6.0
    short_regime_thaw_volatility_floor: float = 0.008


@dataclass
class ExecutionConfig:
    td_mode: str = "isolated"
    cta_order_size: float = 0.01
    grid_order_size: float = 0.01


@dataclass
class WebsocketRuntimeConfig:
    enabled: bool = True
    ticker_enabled: bool = True
    orders_enabled: bool = True
    positions_enabled: bool = True
    reconnect_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        self.reconnect_delay_seconds = max(0.01, float(self.reconnect_delay_seconds))
        self.reconnect_max_delay_seconds = max(self.reconnect_delay_seconds, float(self.reconnect_max_delay_seconds))


@dataclass
class CTAConfig:
    symbol: str = "BTC/USDT"
    margin_fraction_per_trade: float = 0.05
    nominal_leverage: float = 3.0
    timeframe: str = "15m"  # legacy alias for execution_timeframe
    lower_timeframe: str = "15m"  # legacy alias for execution_timeframe
    higher_timeframe: str = "1h"  # legacy alias for swing_timeframe
    major_timeframe: str = "4h"
    swing_timeframe: str = "1h"
    execution_timeframe: str = "15m"
    prefer_closed_major_timeframe_candles: bool = True
    prefer_closed_swing_timeframe_candles: bool = True
    prefer_closed_execution_timeframe_candles: bool = False
    lookback_limit: int = 200
    supertrend_period: int = 10
    supertrend_multiplier: float = 3.0
    swing_rsi_period: int = 14
    swing_rsi_ready_threshold: float = 50.0
    dynamic_rsi_floor: float = 45.0
    rsi_rebound_lookback: int = 6
    rsi_oversold_threshold: float = 30.0
    rsi_rebound_confirmation_level: float = 35.0
    strong_bull_bias_score: float = 60.0
    weak_bull_bias_score: float = 22.0
    weak_bull_memory_score_bonus: float = 0.0
    dynamic_rsi_trend_score: float = 15.0
    recovery_ema_period: int = 21
    recovery_ema_slope_lookback: int = 3
    recovery_ema_flat_tolerance_atr_ratio: float = 0.02
    recovery_rsi_floor: float = 40.0
    recovery_rsi_sma_period: int = 14
    dynamic_rsi_rebound_score: float = 15.0
    early_bullish_score_bonus: float = 10.0
    swing_supertrend_bullish_score: float = 30.0
    magnetism_score_bonus: float = 20.0
    kdj_memory_score_bonus: float = 10.0
    rail_momentum_score_bonus: float = 0.0
    bullish_ready_score_threshold: float = 55.0
    drive_first_tradeable_score: float = 60.0
    aggressive_rsi_relax_score: float = 70.0
    aggressive_rsi_extreme_threshold: float = 85.0
    aggressive_obv_zscore_floor: float = -0.1
    value_area_edge_atr_multiplier: float = 1.0
    weak_bias_fast_ema: int = 21
    weak_bias_slow_ema: int = 55
    kdj_length: int = 9
    kdj_k_smoothing: int = 3
    kdj_d_smoothing: int = 3
    kdj_signal_memory_bars: int = 5
    kdj_urgency_decay_bars: int = 2
    execution_breakout_lookback: int = 3
    obv_signal_period: int = 8
    obv_signal_window: int = 8
    obv_signal_threshold_degrees: float = 30.0
    obv_sma_period: int = 50
    obv_zscore_window: int = 100
    obv_zscore_threshold: float = 1.0
    magnetism_obv_zscore_threshold: float = 1.2
    magnetism_rail_atr_multiplier: float = 1.0
    atr_period: int = 14
    atr_trailing_multiplier: float = 2.5
    stop_loss_atr: float = 2.0
    dynamic_stop_loss_enabled: bool = True
    dynamic_stop_loss_min_scale: float = 0.85
    dynamic_stop_loss_max_scale: float = 1.05
    risk_percent_per_trade: float = 0.02
    boosted_risk_percent_per_trade: float = 0.03
    first_take_profit_pct: float = 0.02
    first_take_profit_size: float = 0.25
    second_take_profit_pct: float = 0.05
    second_take_profit_size: float = 0.25
    minimum_expected_rr: float = 0.0
    relaxed_entry_minimum_expected_rr: float = 0.0
    starter_entry_minimum_expected_rr: float = 0.0
    breakout_rr_target_atr_multiplier: float = 3.0
    early_entry_minimum_score: float = 70.0
    starter_frontrun_minimum_score: float = 80.0
    relaxed_entry_require_near_breakout: bool = True
    starter_entry_require_near_breakout: bool = True
    early_entry_direction_confirmation_bars: int = 2
    volume_profile_lookback_hours: int = 24
    volume_profile_bin_count: int = 24
    volume_profile_value_area_pct: float = 0.70
    order_flow_enabled: bool = True
    order_flow_depth_levels: int = 20
    order_flow_confirmation_ratio: float = 1.5
    order_flow_high_conviction_ratio: float = 2.0
    order_flow_history_window: int = 20
    order_flow_decay_lookback: int = 3
    order_flow_health_sigma_multiplier: float = 1.0
    order_flow_limit_buffer_bps: float = 3.0
    order_flow_max_slippage_bps: float = 12.0
    heartbeat_interval_seconds: float = 300.0
    near_miss_report_interval_seconds: float = 7200.0
    near_miss_report_max_samples: int = 5
    near_miss_report_min_samples: int = 2
    signal_profiler_summary_interval: int = 20
    signal_profiler_min_blocking_count: int = 2
    early_bullish_starter_fraction: float = 0.30
    early_bullish_lower_band_slope_atr_threshold: float = 0.05
    starter_frontrun_enabled: bool = True
    starter_frontrun_fraction: float = 0.20
    starter_frontrun_breakout_buffer_ratio: float = 0.002
    bullish_memory_retest_breakout_buffer_ratio: float = 0.0026
    near_breakout_release_enabled: bool = True
    near_breakout_release_fraction: float = 0.12
    near_breakout_release_minimum_score: float = 70.0
    near_breakout_release_obv_zscore_floor: float = -0.25
    starter_frontrun_impulse_bars: int = 3
    starter_frontrun_volume_window: int = 12
    starter_frontrun_volume_multiplier: float = 1.15
    signal_flip_reduce_ratio: float = 0.50
    fast_ema: int = 7  # legacy compatibility
    slow_ema: int = 21  # legacy compatibility
    polling_interval_seconds: int = 60
    cta_assist_trim_ratio: float = 0.25
    same_direction_stop_cooldown_seconds: int = 300
    same_direction_stop_cooldown_window_seconds: int = 1200
    same_direction_stop_cooldown_escalation_count: int = 2
    same_direction_stop_cooldown_escalation_seconds: int = 900
    obv_scalp_entry_fraction: float = 0.35
    obv_scalp_first_take_profit_pct: float = 0.006
    obv_scalp_second_take_profit_pct: float = 0.012
    obv_scalp_stop_multiplier_scale: float = 0.60
    obv_scalp_break_even_profit_pct: float = 0.003
    obv_scalp_max_hold_seconds: int = 1800
    relaxed_short_minimum_score: float = 48.0
    relaxed_short_minimum_expected_rr: float = 1.15
    relaxed_short_max_countertrend_score_gap: float = 12.0
    relaxed_short_require_early_or_breakdown: bool = True
    obv_scalp_min_bearish_score: float = 52.0
    obv_scalp_max_bullish_score: float = 62.0
    obv_scalp_require_early_bearish: bool = True
    obv_scalp_max_positive_obv_zscore: float = 0.15
    quick_trade_minimum_expected_rr: float = 1.35
    standard_entry_minimum_expected_rr: float = 0.0
    starter_quality_minimum_score: float = 72.0
    scale_in_quality_minimum_score: float = 68.0
    starter_countertrend_max_score_gap: float = 10.0
    tier_high_minimum_score: float = 85.0
    tier_medium_minimum_score: float = 70.0
    tier_high_confidence_threshold: float = 0.8
    signal_strength_trend_bonus_cap: float = 5.0
    signal_strength_direction_bonus_cap: float = 10.0
    signal_strength_volatility_bonus_cap: float = 5.0
    signal_strength_obv_bonus_cap: float = 10.0

    def __post_init__(self) -> None:
        default_execution = "15m"
        default_swing = "1h"
        default_timeframe = "15m"

        if self.execution_timeframe == default_execution and self.lower_timeframe != default_execution:
            self.execution_timeframe = self.lower_timeframe
        if self.execution_timeframe == default_execution and self.timeframe != default_timeframe:
            self.execution_timeframe = self.timeframe
        if self.lower_timeframe == default_execution and self.execution_timeframe != default_execution:
            self.lower_timeframe = self.execution_timeframe
        if self.timeframe == default_timeframe and self.execution_timeframe != default_execution:
            self.timeframe = self.execution_timeframe

        if self.swing_timeframe == default_swing and self.higher_timeframe != default_swing:
            self.swing_timeframe = self.higher_timeframe
        if self.higher_timeframe == default_swing and self.swing_timeframe != default_swing:
            self.higher_timeframe = self.swing_timeframe

        self.major_timeframe = str(self.major_timeframe or "4h")
        self.swing_timeframe = str(self.swing_timeframe or self.higher_timeframe or default_swing)
        self.execution_timeframe = str(self.execution_timeframe or self.lower_timeframe or self.timeframe or default_execution)
        self.lower_timeframe = self.execution_timeframe
        self.higher_timeframe = self.swing_timeframe
        self.timeframe = self.execution_timeframe
        if self.boosted_risk_percent_per_trade <= 0:
            self.boosted_risk_percent_per_trade = self.risk_percent_per_trade
        self.kdj_signal_memory_bars = max(1, int(self.kdj_signal_memory_bars))
        self.kdj_urgency_decay_bars = max(0, int(self.kdj_urgency_decay_bars))
        self.bullish_memory_retest_breakout_buffer_ratio = max(
            float(self.starter_frontrun_breakout_buffer_ratio),
            float(self.bullish_memory_retest_breakout_buffer_ratio),
        )
        self.dynamic_rsi_floor = float(self.dynamic_rsi_floor)
        self.recovery_ema_period = max(2, int(self.recovery_ema_period))
        self.recovery_ema_slope_lookback = max(1, int(self.recovery_ema_slope_lookback))
        self.recovery_ema_flat_tolerance_atr_ratio = max(0.0, float(self.recovery_ema_flat_tolerance_atr_ratio))
        self.recovery_rsi_floor = float(self.recovery_rsi_floor)
        self.recovery_rsi_sma_period = max(2, int(self.recovery_rsi_sma_period))
        self.rsi_rebound_lookback = max(2, int(self.rsi_rebound_lookback))
        self.rsi_oversold_threshold = float(self.rsi_oversold_threshold)
        self.rsi_rebound_confirmation_level = float(self.rsi_rebound_confirmation_level)
        self.strong_bull_bias_score = max(0.0, float(self.strong_bull_bias_score))
        self.weak_bull_bias_score = max(0.0, float(self.weak_bull_bias_score))
        self.weak_bull_memory_score_bonus = max(0.0, float(self.weak_bull_memory_score_bonus))
        self.dynamic_rsi_trend_score = max(0.0, float(self.dynamic_rsi_trend_score))
        self.dynamic_rsi_rebound_score = max(0.0, float(self.dynamic_rsi_rebound_score))
        self.early_bullish_score_bonus = max(0.0, float(self.early_bullish_score_bonus))
        self.swing_supertrend_bullish_score = max(0.0, float(self.swing_supertrend_bullish_score))
        self.magnetism_score_bonus = max(0.0, float(self.magnetism_score_bonus))
        self.kdj_memory_score_bonus = max(0.0, float(self.kdj_memory_score_bonus))
        self.rail_momentum_score_bonus = max(0.0, float(self.rail_momentum_score_bonus))
        self.bullish_ready_score_threshold = max(1.0, float(self.bullish_ready_score_threshold))
        self.drive_first_tradeable_score = max(self.bullish_ready_score_threshold, float(self.drive_first_tradeable_score))
        self.aggressive_rsi_relax_score = max(self.drive_first_tradeable_score, float(self.aggressive_rsi_relax_score))
        self.aggressive_rsi_extreme_threshold = max(self.aggressive_rsi_relax_score, float(self.aggressive_rsi_extreme_threshold))
        self.aggressive_obv_zscore_floor = float(self.aggressive_obv_zscore_floor)
        self.value_area_edge_atr_multiplier = max(0.0, float(self.value_area_edge_atr_multiplier))
        self.dynamic_stop_loss_min_scale = max(0.1, float(self.dynamic_stop_loss_min_scale))
        self.dynamic_stop_loss_max_scale = max(self.dynamic_stop_loss_min_scale, float(self.dynamic_stop_loss_max_scale))
        self.weak_bias_fast_ema = max(2, int(self.weak_bias_fast_ema))
        self.weak_bias_slow_ema = max(self.weak_bias_fast_ema + 1, int(self.weak_bias_slow_ema))
        self.obv_signal_window = max(1, int(self.obv_signal_window))
        self.obv_signal_threshold_degrees = float(self.obv_signal_threshold_degrees)
        self.breakout_rr_target_atr_multiplier = max(0.0, float(self.breakout_rr_target_atr_multiplier))
        self.order_flow_depth_levels = max(1, int(self.order_flow_depth_levels))
        self.order_flow_confirmation_ratio = max(0.0, float(self.order_flow_confirmation_ratio))
        self.order_flow_high_conviction_ratio = max(
            self.order_flow_confirmation_ratio,
            float(self.order_flow_high_conviction_ratio),
        )
        self.order_flow_history_window = max(1, int(self.order_flow_history_window))
        self.order_flow_decay_lookback = max(1, int(self.order_flow_decay_lookback))
        self.order_flow_health_sigma_multiplier = max(0.0, float(self.order_flow_health_sigma_multiplier))
        self.order_flow_limit_buffer_bps = max(0.0, float(self.order_flow_limit_buffer_bps))
        self.order_flow_max_slippage_bps = max(0.0, float(self.order_flow_max_slippage_bps))
        self.near_miss_report_interval_seconds = max(0.0, float(self.near_miss_report_interval_seconds))
        self.near_miss_report_max_samples = max(1, int(self.near_miss_report_max_samples))
        self.near_miss_report_min_samples = max(1, int(self.near_miss_report_min_samples))
        self.signal_profiler_summary_interval = max(1, int(self.signal_profiler_summary_interval))
        self.signal_profiler_min_blocking_count = max(1, int(self.signal_profiler_min_blocking_count))
        self.minimum_expected_rr = max(0.0, float(self.minimum_expected_rr))
        self.relaxed_entry_minimum_expected_rr = max(self.minimum_expected_rr, float(self.relaxed_entry_minimum_expected_rr))
        self.starter_entry_minimum_expected_rr = max(self.relaxed_entry_minimum_expected_rr, float(self.starter_entry_minimum_expected_rr))
        self.relaxed_short_minimum_score = max(0.0, float(self.relaxed_short_minimum_score))
        self.relaxed_short_minimum_expected_rr = max(self.relaxed_entry_minimum_expected_rr, float(self.relaxed_short_minimum_expected_rr))
        self.relaxed_short_max_countertrend_score_gap = max(0.0, float(self.relaxed_short_max_countertrend_score_gap))
        self.obv_scalp_min_bearish_score = max(0.0, float(self.obv_scalp_min_bearish_score))
        self.obv_scalp_max_bullish_score = max(0.0, float(self.obv_scalp_max_bullish_score))
        self.obv_scalp_max_positive_obv_zscore = float(self.obv_scalp_max_positive_obv_zscore)
        self.quick_trade_minimum_expected_rr = max(self.relaxed_short_minimum_expected_rr, float(self.quick_trade_minimum_expected_rr))
        self.standard_entry_minimum_expected_rr = max(self.minimum_expected_rr, float(self.standard_entry_minimum_expected_rr))
        self.starter_quality_minimum_score = max(0.0, float(self.starter_quality_minimum_score))
        self.scale_in_quality_minimum_score = max(0.0, float(self.scale_in_quality_minimum_score))
        self.starter_countertrend_max_score_gap = max(0.0, float(self.starter_countertrend_max_score_gap))
        self.tier_medium_minimum_score = max(0.0, float(self.tier_medium_minimum_score))
        self.tier_high_minimum_score = max(self.tier_medium_minimum_score, float(self.tier_high_minimum_score))
        self.tier_high_confidence_threshold = min(1.0, max(0.0, float(self.tier_high_confidence_threshold)))
        self.signal_strength_trend_bonus_cap = max(0.0, float(self.signal_strength_trend_bonus_cap))
        self.signal_strength_direction_bonus_cap = max(0.0, float(self.signal_strength_direction_bonus_cap))
        self.signal_strength_volatility_bonus_cap = max(0.0, float(self.signal_strength_volatility_bonus_cap))
        self.signal_strength_obv_bonus_cap = max(0.0, float(self.signal_strength_obv_bonus_cap))
        self.early_entry_minimum_score = max(0.0, float(self.early_entry_minimum_score))
        self.starter_frontrun_minimum_score = max(self.early_entry_minimum_score, float(self.starter_frontrun_minimum_score))
        self.early_entry_direction_confirmation_bars = max(1, int(self.early_entry_direction_confirmation_bars))
        self.signal_flip_reduce_ratio = min(1.0, max(0.0, float(self.signal_flip_reduce_ratio)))
        self.cta_assist_trim_ratio = min(1.0, max(0.0, float(self.cta_assist_trim_ratio)))

    @property
    def obv_slope_window(self) -> int:
        return int(self.obv_signal_window)

    @obv_slope_window.setter
    def obv_slope_window(self, value: int) -> None:
        self.obv_signal_window = int(value)

    @property
    def obv_slope_threshold_degrees(self) -> float:
        return float(self.obv_signal_threshold_degrees)

    @obv_slope_threshold_degrees.setter
    def obv_slope_threshold_degrees(self, value: float) -> None:
        self.obv_signal_threshold_degrees = float(value)




@dataclass
class SignalScoringConfig:
    enabled: bool = True
    min_trade_score: float = 3.0
    high_quality_score: float = 5.0
    trend_weight: float = 1.0
    volume_weight: float = 2.0
    timeframe_weight: float = 2.0
    order_flow_weight: float = 1.0
    obv_signal_weight: float = 1.0
    execution_trigger_weight: float = 1.0

    @property
    def obv_slope_weight(self) -> float:
        return float(self.obv_signal_weight)

    @obv_slope_weight.setter
    def obv_slope_weight(self, value: float) -> None:
        self.obv_signal_weight = float(value)

@dataclass
class GridConfig:
    symbol: str = "BTC/USDT"
    equity_allocation_ratio: float = 0.40
    timeframe: str = "1h"  # legacy alias for Bollinger timeframe
    lookback_limit: int = 120
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    levels: int = 8
    leverage: int = 3
    martingale_factor: float = 1.25
    trigger_window_seconds: int = 300
    trigger_limit_per_layer: int = 3
    layer_cooldown_seconds: int = 300
    rebalance_exposure_threshold: float = 2.0
    max_rebalance_orders: int = 2
    rebalance_threshold_ratio: float = 0.65  # legacy compatibility
    polling_interval_seconds: int = 60
    range_percent: float = 0.03  # fixed fallback range: current price ±3%
    liquidation_protection_ratio: float = 0.05
    use_dynamic_range: bool = True
    atr_timeframe: str = "1h"
    atr_period: int = 14
    atr_multiplier: float = 2.5
    min_spacing_ratio: float = 0.007
    atr_spacing_floor_multiplier: float = 0.5
    fee_rate: float = 0.001
    higher_timeframe_trend_guard_enabled: bool = True
    higher_timeframe_trend_timeframe: str = "4h"
    higher_timeframe_trend_supertrend_period: int = 10
    higher_timeframe_trend_supertrend_multiplier: float = 3.0
    higher_timeframe_trend_distance_atr_threshold: float = 0.8
    trend_defense_enabled: bool = True
    trend_defense_breakout_atr_ratio: float = 1.0
    trend_defense_reduction_ratio: float = 0.50
    directional_skew_enabled: bool = True
    directional_bias_threshold: float = 0.20
    sideways_neutral_bias_threshold: float = 0.12
    bearish_directional_bias_threshold: float = 0.30
    bullish_buy_levels: int = 6
    bullish_sell_levels: int = 2
    bullish_buy_spacing_ratio: float = 0.0045
    bullish_sell_spacing_ratio: float = 0.011
    bullish_center_shift_atr_ratio: float = 0.003
    bearish_buy_levels: int = 2
    bearish_sell_levels: int = 6
    bearish_buy_spacing_ratio: float = 0.011
    bearish_sell_spacing_ratio: float = 0.0045
    bearish_center_shift_atr_ratio: float = 0.003
    atr_regrid_change_ratio: float = 0.10
    regrid_trigger_atr_ratio: float = 0.30
    hard_reanchor_atr_ratio: float = 1.20
    min_grid_lifetime_seconds: int = 300
    flash_crash_enabled: bool = True
    flash_crash_timeframe: str = "1m"
    flash_crash_atr_multiplier: float = 1.5
    flash_crash_cooldown_seconds: int = 300
    hard_stop_buffer_ratio: float = 0.01
    max_directional_exposure_ratio: float = 0.50
    websocket_order_sync_enabled: bool = True
    heavy_inventory_threshold: float = 0.60
    active_hedge_mode_enabled: bool = False
    active_hedge_min_inventory_ratio: float = 0.45
    active_hedge_requires_cta_position: bool = True

    def __post_init__(self) -> None:
        self.levels = max(2, int(self.levels))
        self.bullish_buy_levels = max(1, int(self.bullish_buy_levels))
        self.bullish_sell_levels = max(1, int(self.bullish_sell_levels))
        total_bullish_levels = self.bullish_buy_levels + self.bullish_sell_levels
        if total_bullish_levels != self.levels:
            scale = self.levels / max(total_bullish_levels, 1)
            self.bullish_buy_levels = max(1, int(round(self.bullish_buy_levels * scale)))
            self.bullish_sell_levels = max(1, self.levels - self.bullish_buy_levels)
        self.bearish_buy_levels = max(1, int(self.bearish_buy_levels))
        self.bearish_sell_levels = max(1, int(self.bearish_sell_levels))
        total_bearish_levels = self.bearish_buy_levels + self.bearish_sell_levels
        if total_bearish_levels != self.levels:
            scale = self.levels / max(total_bearish_levels, 1)
            self.bearish_sell_levels = max(1, int(round(self.bearish_sell_levels * scale)))
            self.bearish_buy_levels = max(1, self.levels - self.bearish_sell_levels)
        self.directional_bias_threshold = max(0.0, float(self.directional_bias_threshold))
        self.sideways_neutral_bias_threshold = max(0.0, float(self.sideways_neutral_bias_threshold))
        self.bearish_directional_bias_threshold = max(
            self.directional_bias_threshold,
            float(self.bearish_directional_bias_threshold),
        )
        self.bullish_buy_spacing_ratio = max(0.0, float(self.bullish_buy_spacing_ratio))
        self.bullish_sell_spacing_ratio = max(0.0, float(self.bullish_sell_spacing_ratio))
        self.bullish_center_shift_atr_ratio = max(0.0, float(self.bullish_center_shift_atr_ratio))
        self.bearish_buy_spacing_ratio = max(0.0, float(self.bearish_buy_spacing_ratio))
        self.bearish_sell_spacing_ratio = max(0.0, float(self.bearish_sell_spacing_ratio))
        self.bearish_center_shift_atr_ratio = max(0.0, float(self.bearish_center_shift_atr_ratio))
        self.higher_timeframe_trend_timeframe = str(self.higher_timeframe_trend_timeframe or "4h")
        self.higher_timeframe_trend_supertrend_period = max(2, int(self.higher_timeframe_trend_supertrend_period))
        self.higher_timeframe_trend_supertrend_multiplier = max(0.1, float(self.higher_timeframe_trend_supertrend_multiplier))
        self.higher_timeframe_trend_distance_atr_threshold = max(0.0, float(self.higher_timeframe_trend_distance_atr_threshold))
        self.trend_defense_breakout_atr_ratio = max(0.0, float(self.trend_defense_breakout_atr_ratio))
        self.trend_defense_reduction_ratio = min(1.0, max(0.0, float(self.trend_defense_reduction_ratio)))
        self.heavy_inventory_threshold = min(1.0, max(0.0, float(self.heavy_inventory_threshold)))
        self.active_hedge_min_inventory_ratio = min(1.0, max(0.0, float(self.active_hedge_min_inventory_ratio)))


@dataclass
class AppConfig:
    okx: OKXConfig
    database: DatabaseConfig
    notification: NotificationConfig
    runtime: RuntimeConfig
    risk_control: RiskControlConfig
    sentiment: SentimentConfig
    market_oracle: MarketOracleConfig
    execution: ExecutionConfig
    cta: CTAConfig
    grid: GridConfig
    config_path: Path


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode, deep: bool = False):
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"Duplicate config key detected: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.load(handle, Loader=_UniqueKeySafeLoader) or {}

    if not isinstance(payload, dict):
        raise ValueError("Root config payload must be a mapping")

    return payload


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    payload = _read_yaml(path)

    okx_payload = payload.get("okx") or {}
    db_payload = payload.get("database") or {}
    notification_payload = payload.get("notification") or {}
    discord_payload = notification_payload.get("discord") or {}
    runtime_payload = payload.get("runtime") or {}
    risk_payload = payload.get("risk_control") or {}
    sentiment_payload = payload.get("sentiment") or {}
    market_oracle_payload = payload.get("market_oracle") or {}
    execution_payload = payload.get("execution") or {}
    cta_payload = payload.get("cta") or {}
    grid_payload = payload.get("grid") or {}

    okx = OKXConfig(
        api_key=str(okx_payload.get("api_key", "")),
        api_secret=str(okx_payload.get("api_secret", "")),
        passphrase=str(okx_payload.get("passphrase", "")),
        sandbox=bool(okx_payload.get("sandbox", True)),
        simulated_id=str(okx_payload.get("simulated_id", "1")),
        simulated_trading=bool(okx_payload.get("simulated_trading", True)),
        default_type=str(okx_payload.get("default_type", "swap")),
        timeout_ms=int(okx_payload.get("timeout_ms", 10_000)),
    )
    database = DatabaseConfig(
        path=(path.parent.parent / str(db_payload.get("path", "data/market_adaptive.sqlite3"))).resolve()
    )
    notification = NotificationConfig(
        discord=DiscordNotificationConfig(
            enabled=bool(discord_payload.get("enabled", False)),
            channel_id=str(discord_payload.get("channel_id", "")),
            webhook_url=str(discord_payload.get("webhook_url", "")),
            bot_token=str(discord_payload.get("bot_token", "")),
            username=str(discord_payload.get("username", "Market-Adaptive")),
        )
    )
    runtime = RuntimeConfig(
        timezone=str(runtime_payload.get("timezone", "Asia/Shanghai")),
        default_timeframe=str(runtime_payload.get("default_timeframe", "1h")),
        default_ohlcv_limit=int(runtime_payload.get("default_ohlcv_limit", 200)),
        account_check_interval_seconds=int(runtime_payload.get("account_check_interval_seconds", 60)),
        risk_check_interval_seconds=int(runtime_payload.get("risk_check_interval_seconds", 60)),
        fast_risk_check_interval_seconds=int(runtime_payload.get("fast_risk_check_interval_seconds", 1)),
        shutdown_cancel_open_orders=bool(runtime_payload.get("shutdown_cancel_open_orders", True)),
        start_grid_websocket_on_boot=bool(runtime_payload.get("start_grid_websocket_on_boot", True)),
        shutdown_join_timeout_seconds=float(runtime_payload.get("shutdown_join_timeout_seconds", 5.0)),
        account_initial_equity=float(runtime_payload.get("account_initial_equity", 0.0)),
    )
    risk_control = RiskControlConfig(
        daily_loss_warning_pct=float(risk_payload.get("daily_loss_warning_pct", 0.03)),
        daily_loss_warning_scale=float(risk_payload.get("daily_loss_warning_scale", 0.50)),
        daily_loss_stop_openings_pct=float(risk_payload.get("daily_loss_stop_openings_pct", 0.05)),
        daily_loss_reduce_exposure_pct=float(risk_payload.get("daily_loss_reduce_exposure_pct", 0.07)),
        daily_loss_cutoff_pct=float(risk_payload.get("daily_loss_cutoff_pct", 0.05)),
        max_margin_ratio=float(risk_payload.get("max_margin_ratio", 0.60)),
        recovery_check_interval_seconds=int(risk_payload.get("recovery_check_interval_seconds", 60)),
        position_sync_tolerance=float(risk_payload.get("position_sync_tolerance", 1e-6)),
        default_symbol_max_notional=float(risk_payload.get("default_symbol_max_notional", 0.0)),
        symbol_notional_limits={
            str(symbol): float(limit)
            for symbol, limit in (risk_payload.get("symbol_notional_limits") or {}).items()
        },
        cta_single_trade_equity_multiple=float(risk_payload.get("cta_single_trade_equity_multiple", 0.0)),
        max_directional_leverage=float(risk_payload.get("max_directional_leverage", 8.0)),
        grid_margin_ratio_warning=float(risk_payload.get("grid_margin_ratio_warning", 0.45)),
        grid_deviation_reduce_ratio=float(risk_payload.get("grid_deviation_reduce_ratio", 0.25)),
        grid_liquidation_warning_ratio=float(risk_payload.get("grid_liquidation_warning_ratio", 0.10)),
        grid_reduction_step_pct=float(risk_payload.get("grid_reduction_step_pct", 0.25)),
        grid_reduction_cooldown_seconds=int(risk_payload.get("grid_reduction_cooldown_seconds", 300)),
    )
    sentiment = SentimentConfig(
        enabled=bool(sentiment_payload.get("enabled", True)),
        symbol=str(sentiment_payload.get("symbol", "")),
        timeframe=str(sentiment_payload.get("timeframe", "5m")),
        lookback_limit=max(1, int(sentiment_payload.get("lookback_limit", 1))),
        extreme_bullish_ratio=float(sentiment_payload.get("extreme_bullish_ratio", 2.5)),
        cta_buy_action=str(sentiment_payload.get("cta_buy_action", "block")),
        gradient_reduce_ratio_threshold=float(sentiment_payload.get("gradient_reduce_ratio_threshold", 3.0)),
        gradient_reduce_ratio_multiplier=float(sentiment_payload.get("gradient_reduce_ratio_multiplier", 0.7)),
        gradient_heavy_reduce_ratio_threshold=float(sentiment_payload.get("gradient_heavy_reduce_ratio_threshold", 3.5)),
        gradient_heavy_reduce_ratio_multiplier=float(sentiment_payload.get("gradient_heavy_reduce_ratio_multiplier", 0.4)),
    )
    market_oracle = MarketOracleConfig(
        symbol=str(market_oracle_payload.get("symbol", "BTC/USDT")),
        polling_interval_seconds=int(market_oracle_payload.get("polling_interval_seconds", 300)),
        higher_timeframe=str(market_oracle_payload.get("higher_timeframe", "1h")),
        lower_timeframe=str(market_oracle_payload.get("lower_timeframe", "15m")),
        lookback_limit=int(market_oracle_payload.get("lookback_limit", 200)),
        adx_length=int(market_oracle_payload.get("adx_length", 14)),
        bb_length=int(market_oracle_payload.get("bb_length", 20)),
        bb_std=float(market_oracle_payload.get("bb_std", 2.0)),
        trend_adx_threshold=float(market_oracle_payload.get("trend_adx_threshold", 25)),
        sideways_adx_threshold=float(market_oracle_payload.get("sideways_adx_threshold", 20)),
        trend_di_gap_threshold=float(market_oracle_payload.get("trend_di_gap_threshold", 8)),
        relaxed_trend_adx_buffer=float(market_oracle_payload.get("relaxed_trend_adx_buffer", 3.0)),
        relaxed_trend_di_gap_bonus=float(market_oracle_payload.get("relaxed_trend_di_gap_bonus", 1.0)),
        bb_width_contraction_tolerance_ratio=float(market_oracle_payload.get("bb_width_contraction_tolerance_ratio", 0.03)),
        range_breakout_bb_width_threshold=float(market_oracle_payload.get("range_breakout_bb_width_threshold", 0.10)),
        range_breakout_volatility_threshold=float(market_oracle_payload.get("range_breakout_volatility_threshold", 0.015)),
        prefer_closed_higher_timeframe_candles=bool(market_oracle_payload.get("prefer_closed_higher_timeframe_candles", True)),
        prefer_closed_lower_timeframe_candles=bool(market_oracle_payload.get("prefer_closed_lower_timeframe_candles", True)),
        impulse_timeframe=str(market_oracle_payload.get("impulse_timeframe", "1m")),
        prefer_closed_impulse_candles=bool(market_oracle_payload.get("prefer_closed_impulse_candles", False)),
        impulse_consecutive_bars=int(market_oracle_payload.get("impulse_consecutive_bars", 3)),
        impulse_volume_window=int(market_oracle_payload.get("impulse_volume_window", 12)),
        impulse_volume_multiplier=float(market_oracle_payload.get("impulse_volume_multiplier", 1.2)),
    )
    execution = ExecutionConfig(
        td_mode=str(execution_payload.get("td_mode", "isolated")),
        cta_order_size=float(execution_payload.get("cta_order_size", 0.01)),
        grid_order_size=float(execution_payload.get("grid_order_size", 0.01)),
    )
    cta_timeframe = str(cta_payload.get("timeframe", "15m"))
    cta_execution_timeframe = str(
        cta_payload.get("execution_timeframe", cta_payload.get("lower_timeframe", cta_timeframe))
    )
    cta_swing_timeframe = str(
        cta_payload.get("swing_timeframe", cta_payload.get("higher_timeframe", "1h"))
    )
    cta_base_risk_percent = float(cta_payload.get("risk_percent_per_trade", 0.02))
    cta_boosted_risk_percent = float(
        cta_payload.get("boosted_risk_percent_per_trade", max(cta_base_risk_percent, 0.03))
    )
    cta = CTAConfig(
        symbol=str(cta_payload.get("symbol", "BTC/USDT")),
        margin_fraction_per_trade=float(cta_payload.get("margin_fraction_per_trade", 0.05)),
        nominal_leverage=float(cta_payload.get("nominal_leverage", 3.0)),
        timeframe=cta_timeframe,
        lower_timeframe=cta_execution_timeframe,
        higher_timeframe=cta_swing_timeframe,
        major_timeframe=str(cta_payload.get("major_timeframe", "4h")),
        swing_timeframe=cta_swing_timeframe,
        execution_timeframe=cta_execution_timeframe,
        prefer_closed_major_timeframe_candles=bool(cta_payload.get("prefer_closed_major_timeframe_candles", True)),
        prefer_closed_swing_timeframe_candles=bool(cta_payload.get("prefer_closed_swing_timeframe_candles", True)),
        prefer_closed_execution_timeframe_candles=bool(cta_payload.get("prefer_closed_execution_timeframe_candles", False)),
        lookback_limit=int(cta_payload.get("lookback_limit", 200)),
        supertrend_period=int(cta_payload.get("supertrend_period", 10)),
        supertrend_multiplier=float(cta_payload.get("supertrend_multiplier", 3.0)),
        swing_rsi_period=int(cta_payload.get("swing_rsi_period", 14)),
        swing_rsi_ready_threshold=float(cta_payload.get("swing_rsi_ready_threshold", 50.0)),
        dynamic_rsi_floor=float(cta_payload.get("dynamic_rsi_floor", 45.0)),
        recovery_ema_period=int(cta_payload.get("recovery_ema_period", 21)),
        recovery_ema_slope_lookback=int(cta_payload.get("recovery_ema_slope_lookback", 3)),
        recovery_ema_flat_tolerance_atr_ratio=float(cta_payload.get("recovery_ema_flat_tolerance_atr_ratio", 0.02)),
        recovery_rsi_floor=float(cta_payload.get("recovery_rsi_floor", 40.0)),
        recovery_rsi_sma_period=int(cta_payload.get("recovery_rsi_sma_period", 14)),
        rsi_rebound_lookback=int(cta_payload.get("rsi_rebound_lookback", 6)),
        rsi_oversold_threshold=float(cta_payload.get("rsi_oversold_threshold", 30.0)),
        rsi_rebound_confirmation_level=float(cta_payload.get("rsi_rebound_confirmation_level", 35.0)),
        strong_bull_bias_score=float(cta_payload.get("strong_bull_bias_score", 60.0)),
        weak_bull_bias_score=float(cta_payload.get("weak_bull_bias_score", 20.0)),
        weak_bull_memory_score_bonus=float(cta_payload.get("weak_bull_memory_score_bonus", 0.0)),
        dynamic_rsi_trend_score=float(cta_payload.get("dynamic_rsi_trend_score", 15.0)),
        dynamic_rsi_rebound_score=float(cta_payload.get("dynamic_rsi_rebound_score", 15.0)),
        early_bullish_score_bonus=float(cta_payload.get("early_bullish_score_bonus", 10.0)),
        swing_supertrend_bullish_score=float(cta_payload.get("swing_supertrend_bullish_score", 30.0)),
        magnetism_score_bonus=float(cta_payload.get("magnetism_score_bonus", 20.0)),
        kdj_memory_score_bonus=float(cta_payload.get("kdj_memory_score_bonus", 10.0)),
        rail_momentum_score_bonus=float(cta_payload.get("rail_momentum_score_bonus", 0.0)),
        bullish_ready_score_threshold=float(cta_payload.get("bullish_ready_score_threshold", 55.0)),
        drive_first_tradeable_score=float(cta_payload.get("drive_first_tradeable_score", 60.0)),
        aggressive_rsi_relax_score=float(cta_payload.get("aggressive_rsi_relax_score", 70.0)),
        aggressive_rsi_extreme_threshold=float(cta_payload.get("aggressive_rsi_extreme_threshold", 85.0)),
        aggressive_obv_zscore_floor=float(cta_payload.get("aggressive_obv_zscore_floor", -0.1)),
        value_area_edge_atr_multiplier=float(cta_payload.get("value_area_edge_atr_multiplier", 1.0)),
        weak_bias_fast_ema=int(cta_payload.get("weak_bias_fast_ema", cta_payload.get("fast_ema", 21))),
        weak_bias_slow_ema=int(cta_payload.get("weak_bias_slow_ema", cta_payload.get("slow_ema", 55))),
        kdj_length=int(cta_payload.get("kdj_length", 9)),
        kdj_k_smoothing=int(cta_payload.get("kdj_k_smoothing", 3)),
        kdj_d_smoothing=int(cta_payload.get("kdj_d_smoothing", 3)),
        kdj_urgency_decay_bars=int(cta_payload.get("kdj_urgency_decay_bars", 2)),
        execution_breakout_lookback=int(cta_payload.get("execution_breakout_lookback", 3)),
        obv_signal_period=int(cta_payload.get("obv_signal_period", 8)),
        obv_signal_window=int(cta_payload.get("obv_signal_window", cta_payload.get("obv_slope_window", 8))),
        obv_signal_threshold_degrees=float(cta_payload.get("obv_signal_threshold_degrees", cta_payload.get("obv_slope_threshold_degrees", 30.0))),
        obv_sma_period=int(cta_payload.get("obv_sma_period", 50)),
        obv_zscore_window=int(cta_payload.get("obv_zscore_window", 100)),
        obv_zscore_threshold=float(cta_payload.get("obv_zscore_threshold", 1.0)),
        magnetism_obv_zscore_threshold=float(cta_payload.get("magnetism_obv_zscore_threshold", 1.2)),
        magnetism_rail_atr_multiplier=float(cta_payload.get("magnetism_rail_atr_multiplier", 1.0)),
        atr_period=int(cta_payload.get("atr_period", 14)),
        atr_trailing_multiplier=float(cta_payload.get("atr_trailing_multiplier", 2.5)),
        stop_loss_atr=float(cta_payload.get("stop_loss_atr", 2.0)),
        dynamic_stop_loss_enabled=bool(cta_payload.get("dynamic_stop_loss_enabled", True)),
        dynamic_stop_loss_min_scale=float(cta_payload.get("dynamic_stop_loss_min_scale", 0.85)),
        dynamic_stop_loss_max_scale=float(cta_payload.get("dynamic_stop_loss_max_scale", 1.05)),
        risk_percent_per_trade=cta_base_risk_percent,
        boosted_risk_percent_per_trade=cta_boosted_risk_percent,
        first_take_profit_pct=float(cta_payload.get("first_take_profit_pct", 0.02)),
        first_take_profit_size=float(cta_payload.get("first_take_profit_size", 0.25)),
        second_take_profit_pct=float(cta_payload.get("second_take_profit_pct", 0.05)),
        second_take_profit_size=float(cta_payload.get("second_take_profit_size", 0.25)),
        minimum_expected_rr=float(cta_payload.get("minimum_expected_rr", 0.0)),
        relaxed_entry_minimum_expected_rr=float(cta_payload.get("relaxed_entry_minimum_expected_rr", 0.0)),
        starter_entry_minimum_expected_rr=float(cta_payload.get("starter_entry_minimum_expected_rr", 0.0)),
        standard_entry_minimum_expected_rr=float(cta_payload.get("standard_entry_minimum_expected_rr", 0.0)),
        breakout_rr_target_atr_multiplier=float(cta_payload.get("breakout_rr_target_atr_multiplier", 3.0)),
        early_entry_minimum_score=float(cta_payload.get("early_entry_minimum_score", 70.0)),
        starter_frontrun_minimum_score=float(cta_payload.get("starter_frontrun_minimum_score", 80.0)),
        relaxed_entry_require_near_breakout=bool(cta_payload.get("relaxed_entry_require_near_breakout", True)),
        starter_entry_require_near_breakout=bool(cta_payload.get("starter_entry_require_near_breakout", True)),
        early_entry_direction_confirmation_bars=int(cta_payload.get("early_entry_direction_confirmation_bars", 2)),
        volume_profile_lookback_hours=int(cta_payload.get("volume_profile_lookback_hours", 24)),
        volume_profile_bin_count=int(cta_payload.get("volume_profile_bin_count", 24)),
        volume_profile_value_area_pct=float(cta_payload.get("volume_profile_value_area_pct", 0.70)),
        order_flow_enabled=bool(cta_payload.get("order_flow_enabled", True)),
        order_flow_depth_levels=int(cta_payload.get("order_flow_depth_levels", 20)),
        order_flow_confirmation_ratio=float(cta_payload.get("order_flow_confirmation_ratio", 1.5)),
        order_flow_high_conviction_ratio=float(cta_payload.get("order_flow_high_conviction_ratio", 2.0)),
        order_flow_history_window=int(cta_payload.get("order_flow_history_window", 20)),
        order_flow_decay_lookback=int(cta_payload.get("order_flow_decay_lookback", 3)),
        order_flow_health_sigma_multiplier=float(cta_payload.get("order_flow_health_sigma_multiplier", 1.0)),
        order_flow_limit_buffer_bps=float(cta_payload.get("order_flow_limit_buffer_bps", 3.0)),
        order_flow_max_slippage_bps=float(cta_payload.get("order_flow_max_slippage_bps", 12.0)),
        heartbeat_interval_seconds=float(cta_payload.get("heartbeat_interval_seconds", 300.0)),
        near_miss_report_interval_seconds=float(cta_payload.get("near_miss_report_interval_seconds", 7200.0)),
        near_miss_report_max_samples=int(cta_payload.get("near_miss_report_max_samples", 5)),
        near_miss_report_min_samples=int(cta_payload.get("near_miss_report_min_samples", 2)),
        signal_profiler_summary_interval=int(cta_payload.get("signal_profiler_summary_interval", 20)),
        signal_profiler_min_blocking_count=int(cta_payload.get("signal_profiler_min_blocking_count", 2)),
        early_bullish_starter_fraction=float(cta_payload.get("early_bullish_starter_fraction", 0.30)),
        early_bullish_lower_band_slope_atr_threshold=float(
            cta_payload.get("early_bullish_lower_band_slope_atr_threshold", 0.05)
        ),
        starter_frontrun_enabled=bool(cta_payload.get("starter_frontrun_enabled", True)),
        starter_frontrun_fraction=float(cta_payload.get("starter_frontrun_fraction", 0.20)),
        starter_frontrun_breakout_buffer_ratio=float(cta_payload.get("starter_frontrun_breakout_buffer_ratio", 0.002)),
        bullish_memory_retest_breakout_buffer_ratio=float(cta_payload.get("bullish_memory_retest_breakout_buffer_ratio", 0.0026)),
        near_breakout_release_enabled=bool(cta_payload.get("near_breakout_release_enabled", True)),
        near_breakout_release_fraction=float(cta_payload.get("near_breakout_release_fraction", 0.12)),
        near_breakout_release_minimum_score=float(cta_payload.get("near_breakout_release_minimum_score", 70.0)),
        near_breakout_release_obv_zscore_floor=float(cta_payload.get("near_breakout_release_obv_zscore_floor", -0.25)),
        starter_frontrun_impulse_bars=int(cta_payload.get("starter_frontrun_impulse_bars", 3)),
        starter_frontrun_volume_window=int(cta_payload.get("starter_frontrun_volume_window", 12)),
        starter_frontrun_volume_multiplier=float(cta_payload.get("starter_frontrun_volume_multiplier", 1.15)),
        signal_flip_reduce_ratio=float(cta_payload.get("signal_flip_reduce_ratio", 0.50)),
        fast_ema=int(cta_payload.get("fast_ema", 7)),
        slow_ema=int(cta_payload.get("slow_ema", 21)),
        polling_interval_seconds=int(cta_payload.get("polling_interval_seconds", 60)),
        cta_assist_trim_ratio=float(cta_payload.get("cta_assist_trim_ratio", 0.25)),
    )
    grid = GridConfig(
        symbol=str(grid_payload.get("symbol", "BTC/USDT")),
        equity_allocation_ratio=float(grid_payload.get("equity_allocation_ratio", 0.40)),
        timeframe=str(grid_payload.get("bollinger_timeframe", grid_payload.get("timeframe", "1h"))),
        lookback_limit=int(grid_payload.get("lookback_limit", 120)),
        bollinger_period=int(grid_payload.get("bollinger_length", grid_payload.get("bollinger_period", 20))),
        bollinger_std=float(grid_payload.get("bollinger_std", 2.0)),
        levels=int(grid_payload.get("levels", 8)),
        leverage=int(grid_payload.get("leverage", 3)),
        martingale_factor=float(grid_payload.get("martingale_factor", 1.25)),
        trigger_window_seconds=int(grid_payload.get("layer_trigger_window_seconds", grid_payload.get("trigger_window_seconds", 300))),
        trigger_limit_per_layer=int(grid_payload.get("layer_trigger_limit", grid_payload.get("trigger_limit_per_layer", 3))),
        layer_cooldown_seconds=int(grid_payload.get("layer_cooldown_seconds", 300)),
        rebalance_exposure_threshold=float(grid_payload.get("rebalance_exposure_threshold", 2.0)),
        max_rebalance_orders=int(grid_payload.get("max_rebalance_orders", 2)),
        rebalance_threshold_ratio=float(grid_payload.get("rebalance_threshold_ratio", 0.65)),
        polling_interval_seconds=int(grid_payload.get("polling_interval_seconds", 60)),
        range_percent=float(grid_payload.get("price_band_ratio", grid_payload.get("range_percent", 0.03))),
        liquidation_protection_ratio=float(grid_payload.get("liquidation_protection_ratio", 0.05)),
        use_dynamic_range=bool(grid_payload.get("use_dynamic_range", True)),
        atr_timeframe=str(grid_payload.get("atr_timeframe", "1h")),
        atr_period=int(grid_payload.get("atr_period", 14)),
        atr_multiplier=float(grid_payload.get("atr_multiplier", 2.5)),
        min_spacing_ratio=float(grid_payload.get("min_spacing_ratio", 0.007)),
        fee_rate=float(grid_payload.get("fee_rate", 0.001)),
        higher_timeframe_trend_guard_enabled=bool(grid_payload.get("higher_timeframe_trend_guard_enabled", True)),
        higher_timeframe_trend_timeframe=str(grid_payload.get("higher_timeframe_trend_timeframe", "4h")),
        higher_timeframe_trend_supertrend_period=int(grid_payload.get("higher_timeframe_trend_supertrend_period", 10)),
        higher_timeframe_trend_supertrend_multiplier=float(grid_payload.get("higher_timeframe_trend_supertrend_multiplier", 3.0)),
        higher_timeframe_trend_distance_atr_threshold=float(grid_payload.get("higher_timeframe_trend_distance_atr_threshold", 0.8)),
        trend_defense_enabled=bool(grid_payload.get("trend_defense_enabled", True)),
        trend_defense_breakout_atr_ratio=float(grid_payload.get("trend_defense_breakout_atr_ratio", 1.0)),
        trend_defense_reduction_ratio=float(grid_payload.get("trend_defense_reduction_ratio", 0.50)),
        directional_skew_enabled=bool(grid_payload.get("directional_skew_enabled", True)),
        directional_bias_threshold=float(grid_payload.get("directional_bias_threshold", 0.20)),
        sideways_neutral_bias_threshold=float(grid_payload.get("sideways_neutral_bias_threshold", 0.12)),
        bearish_directional_bias_threshold=float(grid_payload.get("bearish_directional_bias_threshold", 0.30)),
        bullish_buy_levels=int(grid_payload.get("bullish_buy_levels", 6)),
        bullish_sell_levels=int(grid_payload.get("bullish_sell_levels", 2)),
        bullish_buy_spacing_ratio=float(grid_payload.get("bullish_buy_spacing_ratio", 0.0045)),
        bullish_sell_spacing_ratio=float(grid_payload.get("bullish_sell_spacing_ratio", 0.011)),
        bullish_center_shift_atr_ratio=float(grid_payload.get("bullish_center_shift_atr_ratio", 0.003)),
        bearish_buy_levels=int(grid_payload.get("bearish_buy_levels", 2)),
        bearish_sell_levels=int(grid_payload.get("bearish_sell_levels", 6)),
        bearish_buy_spacing_ratio=float(grid_payload.get("bearish_buy_spacing_ratio", 0.011)),
        bearish_sell_spacing_ratio=float(grid_payload.get("bearish_sell_spacing_ratio", 0.0045)),
        bearish_center_shift_atr_ratio=float(grid_payload.get("bearish_center_shift_atr_ratio", 0.003)),
        atr_regrid_change_ratio=float(grid_payload.get("atr_regrid_change_ratio", 0.10)),
        regrid_trigger_atr_ratio=float(grid_payload.get("regrid_trigger_atr_ratio", 0.30)),
        hard_reanchor_atr_ratio=float(grid_payload.get("hard_reanchor_atr_ratio", 1.20)),
        min_grid_lifetime_seconds=int(grid_payload.get("min_grid_lifetime_seconds", 300)),
        flash_crash_enabled=bool(grid_payload.get("flash_crash_enabled", True)),
        flash_crash_timeframe=str(grid_payload.get("flash_crash_timeframe", "1m")),
        flash_crash_atr_multiplier=float(grid_payload.get("flash_crash_atr_multiplier", 1.5)),
        flash_crash_cooldown_seconds=int(grid_payload.get("flash_crash_cooldown_seconds", 300)),
        hard_stop_buffer_ratio=float(grid_payload.get("hard_stop_buffer_ratio", 0.01)),
        max_directional_exposure_ratio=float(grid_payload.get("max_directional_exposure_ratio", 0.50)),
        websocket_order_sync_enabled=bool(grid_payload.get("websocket_order_sync_enabled", True)),
        heavy_inventory_threshold=float(grid_payload.get("heavy_inventory_threshold", 0.60)),
        active_hedge_mode_enabled=bool(grid_payload.get("active_hedge_mode_enabled", False)),
        active_hedge_min_inventory_ratio=float(grid_payload.get("active_hedge_min_inventory_ratio", 0.45)),
        active_hedge_requires_cta_position=bool(grid_payload.get("active_hedge_requires_cta_position", True)),
    )
    return AppConfig(
        okx=okx,
        database=database,
        notification=notification,
        runtime=runtime,
        risk_control=risk_control,
        sentiment=sentiment,
        market_oracle=market_oracle,
        execution=execution,
        cta=cta,
        grid=grid,
        config_path=path,
    )
