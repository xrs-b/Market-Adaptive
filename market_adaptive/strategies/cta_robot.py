from __future__ import annotations
import math

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from types import SimpleNamespace

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.coordination import StrategyRuntimeContext
from market_adaptive.indicators import (
    OBVConfirmationSnapshot,
    VolumeProfileSnapshot,
    compute_atr,
    compute_obv,
    compute_obv_confirmation_snapshot,
    compute_volume_profile,
)
from market_adaptive.risk import CTARiskProfile, LogicalPositionSnapshot
from market_adaptive.sentiment import SentimentAnalyst
from market_adaptive.strategies.base import BaseStrategyRobot
from market_adaptive.db import TradeJournalRecord
from market_adaptive.ml_signal_engine import MLSignalDecision, MarketAdaptiveMLEngine
from market_adaptive.strategies.mtf_engine import MTFSignal, MultiTimeframeSignalEngine
from market_adaptive.strategies.obv_gate import resolve_dynamic_obv_gate_for_signal
from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment, OrderFlowSentinel
from market_adaptive.strategies.signal_profiler import SignalProfiler

logger = logging.getLogger(__name__)


class EntryPathway(Enum):
    FAST_TRACK = "fast_track"
    STANDARD = "standard"
    STRICT = "strict"


@dataclass
class TrendSignal:
    direction: int
    raw_direction: int
    major_direction: int
    major_bias_score: float = 0.0
    weak_bull_bias: bool = False
    weak_bear_bias: bool = False
    early_bullish: bool = False
    early_bearish: bool = False
    entry_size_multiplier: float = 1.0
    swing_rsi: float = 0.0
    swing_rsi_slope: float = 0.0
    bullish_score: float = 0.0
    bearish_score: float = 0.0
    bullish_threshold: float = 0.0
    bearish_threshold: float = 0.0
    bullish_ready: bool = False
    bearish_ready: bool = False
    execution_entry_mode: str = "breakout_confirmed"
    execution_golden_cross: bool = False
    execution_breakout: bool = False
    execution_breakdown: bool = False
    execution_memory_active: bool = False
    execution_latch_active: bool = False
    execution_latch_price: float | None = None
    execution_frontrun_near_breakout: bool = False
    execution_memory_bars_ago: int | None = None
    execution_trigger_family: str = "waiting"
    execution_trigger_group: str = "waiting"
    execution_trigger_reason: str = ""
    pullback_near_support: bool = False
    volatility_squeeze_breakout: bool = False
    stretch_value: float = 0.0
    stretch_blocked: bool = False
    pending_retest: bool = False
    exhaustion_penalty_applied: bool = False
    mtf_aligned: bool = False
    obv_bias: int = 0
    obv_confirmation: OBVConfirmationSnapshot = field(default_factory=lambda: OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    obv_threshold: float | None = None
    obv_confirmation_passed: bool = False
    volume_filter_passed: bool = False
    volume_profile: VolumeProfileSnapshot | None = None
    long_setup_blocked: bool = False
    long_setup_reason: str = ""
    price: float = 0.0
    atr: float = 0.0
    risk_percent: float = 0.0
    blocker_reason: str = ""
    data_alignment_valid: bool = True
    data_mismatch_ms: int = 0
    relaxed_entry: bool = False
    relaxed_reasons: tuple[str, ...] = ()
    quick_trade_mode: bool = False
    entry_pathway: EntryPathway = EntryPathway.STRICT
    signal_quality_tier: str = "TIER_LOW"
    signal_confidence: float = 0.0
    signal_strength_bonus: float = 0.0
    entry_decider_decision: str = "unevaluated"
    entry_decider_score: float = 0.0
    entry_decider_reasons: tuple[str, ...] = ()
    candidate_state: str = "idle"
    candidate_reason: str = ""
    watch_sample_promoted: bool = False
    watch_sample_age_seconds: float | None = None
    watch_sample_origin_reason: str = ""
    ml_used_model: bool = False
    ml_model_used: bool = False
    ml_prediction: int = 0
    ml_probability_up: float = 0.5
    ml_aligned_confidence: float = 0.5
    ml_gate_passed: bool = True
    ml_reason: str = "ml_unavailable"
    ml_gate_reason: str = "ml_unavailable"
    liquidity_sweep: bool = False
    liquidity_sweep_side: str = ""
    oi_change_pct: float = 0.0
    funding_rate: float = 0.0
    is_short_squeeze: bool = False
    is_long_liquidation: bool = False
    resonance_allowed: bool = False
    resonance_reason: str = ""
    reverse_intercepted: bool = False
    reverse_intercept_reason: str = ""
    sweep_extreme_price: float | None = None
    entry_location_score: float = 0.0
    entry_location_reasons: tuple[str, ...] = ()


@dataclass
class StatisticalPricing:
    """Volume-Node / VWAP-based statistical execution pricing.
    Replaces simple breakout-pursuit market orders with limit orders placed
    at statistically significant pullback levels (high-volume nodes, VWAP ± 1σ).
    """

    symbol: str

    def resolve_best_limit_price(
        self,
        *,
        side: str,
        execution_frame,
        volume_profile,
        vwap_std_multiplier: float = 1.0,
        atr_value: float | None = None,
    ) -> float | None:
        """Return the best limit order price, or None if market order preferred."""
        import numpy as np

        if len(execution_frame) < 20:
            return None

        close_prices = execution_frame['close'].values.astype(float)
        volumes = execution_frame['volume'].values.astype(float)

        # VWAP + 1σ band
        typical_price = (
            execution_frame['high'].values.astype(float) +
            execution_frame['low'].values.astype(float) +
            close_prices
        ) / 3.0
        vwap = float((typical_price * volumes).sum() / volumes.sum())
        vol_std = float(np.sqrt(((typical_price - vwap) ** 2 * volumes).sum() / volumes.sum()))
        vwap_lower = vwap - vwap_std_multiplier * vol_std
        vwap_upper = vwap + vwap_std_multiplier * vol_std

        current_price = close_prices[-1]
        is_long = side == 'buy'

        node_prices, node_volumes = self._find_volume_nodes(close_prices, volumes, n_nodes=5)

        if is_long:
            eligible = [(p, v) for p, v in zip(node_prices, node_volumes) if p < current_price]
            if not eligible:
                return None
            eligible.sort(key=lambda x: (current_price - x[0], -x[1]))
            best_node_price, _ = eligible[0]
            vwap_support = vwap_lower if vwap_lower < current_price else None
            if vwap_support is not None and (current_price - vwap_support) < (current_price - best_node_price) * 0.6:
                return self._round_to_tick(vwap_support, tick_size=self._atr_based_tick(atr_value, current_price))
            return self._round_to_tick(best_node_price, tick_size=self._atr_based_tick(atr_value, current_price))
        else:
            eligible = [(p, v) for p, v in zip(node_prices, node_volumes) if p > current_price]
            if not eligible:
                return None
            eligible.sort(key=lambda x: (x[0] - current_price, -x[1]))
            best_node_price, _ = eligible[0]
            vwap_resistance = vwap_upper if vwap_upper > current_price else None
            if vwap_resistance is not None and (vwap_resistance - current_price) < (best_node_price - current_price) * 0.6:
                return self._round_to_tick(vwap_resistance, tick_size=self._atr_based_tick(atr_value, current_price))
            return self._round_to_tick(best_node_price, tick_size=self._atr_based_tick(atr_value, current_price))

    @staticmethod
    def _find_volume_nodes(
        prices, volumes, n_nodes: int = 5
    ) -> tuple:
        import numpy as np
        if len(prices) < 20:
            return [], []
        price_min, price_max = float(prices.min()), float(prices.max())
        if price_max == price_min:
            return [], []
        n_bins = max(20, len(prices) // 5)
        bin_edges = np.linspace(price_min - 1e-8, price_max + 1e-8, n_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
        bin_idx = np.digitize(prices, bin_edges) - 1
        bin_volumes = np.zeros(n_bins)
        np.add.at(bin_volumes, bin_idx, volumes.astype(float))
        bin_volumes = np.maximum(bin_volumes, 0.0)
        if bin_volumes.sum() <= 0:
            return [], []
        vol_pct = bin_volumes / bin_volumes.sum()
        mean_pct = vol_pct.mean()
        std_pct = vol_pct.std() if len(vol_pct) > 1 else 0.0
        threshold = mean_pct + 0.5 * std_pct
        node_indices = np.where(vol_pct > threshold)[0]
        if len(node_indices) == 0:
            return [], []
        sorted_idx = node_indices[np.argsort(-vol_pct[node_indices])[:n_nodes]]
        return bin_centers[sorted_idx].tolist(), bin_volumes[sorted_idx].tolist()

    @staticmethod
    def _round_to_tick(price: float, tick_size: float) -> float:
        if tick_size <= 0:
            return price
        return round(price / tick_size) * tick_size

    @staticmethod
    def _atr_based_tick(atr_value: float | None, price: float) -> float:
        if atr_value is not None and atr_value > 0:
            return max(atr_value * 0.1, 0.01)
        return max(price * 0.0001, 0.01)



    atr: float = 0.0
    risk_percent: float = 0.0
    blocker_reason: str = ""
    data_alignment_valid: bool = True
    data_mismatch_ms: int = 0
    relaxed_entry: bool = False
    relaxed_reasons: tuple[str, ...] = ()
    quick_trade_mode: bool = False
    entry_pathway: EntryPathway = EntryPathway.STRICT
    signal_quality_tier: str = "TIER_LOW"
    signal_confidence: float = 0.0
    signal_strength_bonus: float = 0.0

    @property
    def obv_signal_strength(self) -> float:
        return self.obv_confirmation.zscore

    @property
    def obv_signal_confirmed(self) -> bool:
        return self.obv_confirmation_passed

    @property
    def obv_slope_angle(self) -> float:
        return self.obv_signal_strength

    @property
    def obv_slope_passed(self) -> bool:
        return self.obv_signal_confirmed


@dataclass
class CTANearMissSample:
    symbol: str
    captured_at: float
    candidate_state: str = "idle"
    candidate_reason: str = ""
    execution_trigger_family: str = "waiting"
    execution_trigger_group: str = "waiting"
    execution_memory_active: bool = False
    execution_trigger_reason: str = ""
    execution_memory_bars_ago: int | None = None
    execution_breakout: bool = False
    execution_golden_cross: bool = False
    obv_zscore: float = 0.0
    obv_threshold: float = 0.0
    obv_gap: float = 0.0
    price: float = 0.0


@dataclass(frozen=True)
class ValueAreaDecision:
    inside_value_area: bool
    blocked: bool
    reason: str | None = None


@dataclass(frozen=True)
class HighMomentumClearanceDecision:
    eligible: bool
    used_rsi_override: bool = False
    used_value_area_override: bool = False

    @property
    def activated(self) -> bool:
        return bool(self.eligible and self.used_rsi_override and self.used_value_area_override)


@dataclass
class EntryOrderResult:
    order: dict
    used_limit_order: bool
    filled_amount: float
    average_price: float | None


@dataclass(frozen=True)
class FinalEntryPermit:
    allowed: bool
    status: str
    action: str | None = None
    stage: str = "final"
    reason: str = ""
    position_side: str = ""
    notional_price: float = 0.0
    order_flow_assessment: OrderFlowAssessment | None = None


@dataclass
class ManagedPosition:
    side: str
    entry_price: float
    initial_size: float
    remaining_size: float
    stop_price: float
    best_price: float
    atr_value: float
    stop_distance: float
    risk_percent: float = 0.0
    first_target_hit: bool = False
    second_target_hit: bool = False
    max_unrealized_profit_pct: float = 0.0
    quick_trade_mode: bool = False
    opened_at_ms: int = 0
    origin_trigger_family: str | None = None
    origin_trigger_reason: str | None = None
    origin_pathway: str | None = None

    @property
    def direction(self) -> int:
        return 1 if self.side == "long" else -1

    @property
    def exit_side(self) -> str:
        return "sell" if self.side == "long" else "buy"

    def profit_ratio(self, price: float) -> float:
        if self.side == "long":
            return (price - self.entry_price) / self.entry_price
        return (self.entry_price - price) / self.entry_price

    def update_dynamic_stop(self, price: float, atr: float, stop_multiplier: float) -> None:
        raw_profit_ratio = float(self.profit_ratio(price))
        self.max_unrealized_profit_pct = max(float(self.max_unrealized_profit_pct), raw_profit_ratio * 100.0)

        current_stop_distance = max(float(atr) * float(stop_multiplier), float(price) * 0.001)
        if self.max_unrealized_profit_pct > 4.0:
            current_stop_distance = min(current_stop_distance, max(float(atr) * 0.5, float(price) * 0.001))

        self.atr_value = float(atr)
        self.stop_distance = current_stop_distance

        if self.side == "long":
            self.best_price = max(self.best_price, price)
            candidate = self.best_price - current_stop_distance
            if self.max_unrealized_profit_pct > 2.0:
                candidate = max(candidate, float(self.entry_price))
            self.stop_price = max(self.stop_price, candidate)
            return

        self.best_price = min(self.best_price, price)
        candidate = self.best_price + current_stop_distance
        if self.max_unrealized_profit_pct > 2.0:
            candidate = min(candidate, float(self.entry_price))
        self.stop_price = min(self.stop_price, candidate)

    def stop_hit(self, price: float) -> bool:
        if self.side == "long":
            return price <= self.stop_price
        return price >= self.stop_price


class CTARobot(BaseStrategyRobot):
    strategy_name = "cta"
    activation_status = "trend"
    activation_statuses = ("trend", "trend_impulse", "range_breakout_ready")

    def __init__(
        self,
        client,
        database,
        config: CTAConfig,
        execution_config: ExecutionConfig,
        notifier=None,
        risk_manager=None,
        sentiment_analyst: SentimentAnalyst | None = None,
        runtime_context: StrategyRuntimeContext | None = None,
        signal_profiler: SignalProfiler | None = None,
        grid_center_provider=None,
    ) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.sentiment_analyst = sentiment_analyst
        self.runtime_context = runtime_context
        self.signal_profiler = signal_profiler
        self.grid_center_provider = grid_center_provider
        self.position: ManagedPosition | None = None
        self.mtf_engine = MultiTimeframeSignalEngine(client, config)
        self.order_flow_sentinel = OrderFlowSentinel(client, config)
        self.statistical_pricing = StatisticalPricing(config.symbol)
        self.ml_engine = MarketAdaptiveMLEngine(
            enabled=bool(getattr(config, "ml_enabled", False)),
            model_path=str(getattr(config, "ml_model_path", "data/ml_models")),
        )
        self._last_signal_heartbeat_at = 0.0
        self._last_major_direction: int | None = None
        self._last_bullish_ready: bool | None = None
        self._near_miss_samples: list[CTANearMissSample] = []
        self._near_miss_window_started_at: float | None = None
        self._last_near_miss_report_at = 0.0
        self._time_provider = time.time
        self._signal_flip_pending = False
        self._same_direction_cooldown_until: dict[str, float] = {"long": 0.0, "short": 0.0}
        self._same_direction_stop_events: dict[str, deque[float]] = {"long": deque(), "short": deque()}
        self._fast_track_reuse_until: dict[str, float] = {"long": 0.0, "short": 0.0}
        self._fast_track_reuse_signature: dict[str, tuple | None] = {"long": None, "short": None}
        self._entry_zone_cooldown_until: dict[str, float] = {"long": 0.0, "short": 0.0}
        self._entry_zone_anchor_price: dict[str, float | None] = {"long": None, "short": None}
        self._entry_zone_trigger_family: dict[str, str | None] = {"long": None, "short": None}
        self._recent_watch_samples: dict[str, dict[str, float | str]] = {}

    def _resolve_obv_gate(self, signal: MTFSignal):
        return resolve_dynamic_obv_gate_for_signal(
            signal,
            configured_threshold=float(self.config.obv_zscore_threshold),
            obv_sma_period=int(self.config.obv_sma_period),
            obv_zscore_window=int(self.config.obv_zscore_window),
        )

    def _evaluate_high_momentum_clearance(
        self,
        *,
        mtf_signal: MTFSignal,
        inside_value_area: bool,
        raw_direction: int,
    ) -> HighMomentumClearanceDecision:
        long_eligible = bool(
            raw_direction > 0
            and float(mtf_signal.bullish_score) >= 75.0
            and mtf_signal.execution_trigger.frontrun_near_breakout
        )
        short_eligible = bool(
            raw_direction < 0
            and float(getattr(mtf_signal, "bearish_score", 0.0)) >= 75.0
            and mtf_signal.execution_trigger.frontrun_near_breakout
        )
        eligible = bool(long_eligible or short_eligible)
        used_rsi_override = bool(eligible and getattr(mtf_signal, "rsi_blocking_overridden", False))
        used_value_area_override = bool(eligible and inside_value_area)
        return HighMomentumClearanceDecision(
            eligible=eligible,
            used_rsi_override=used_rsi_override,
            used_value_area_override=used_value_area_override,
        )

    def _resolve_entry_pathway(self, mtf_signal: MTFSignal) -> EntryPathway:
        quality_tier = str(getattr(getattr(mtf_signal, "signal_quality_tier", None), "name", "TIER_LOW"))
        confidence = float(getattr(mtf_signal, "signal_confidence", 0.0) or 0.0)
        fully_aligned = bool(getattr(mtf_signal, "fully_aligned", False))
        high_confidence_floor = float(getattr(self.config, "tier_high_confidence_threshold", 0.8))
        trigger_family = str(getattr(getattr(mtf_signal, "execution_trigger", None), "family", "") or "")

        if (
            quality_tier == "TIER_HIGH"
            and fully_aligned
            and confidence >= high_confidence_floor
            and trigger_family != "major_bull_retest"
        ):
            return EntryPathway.FAST_TRACK
        if quality_tier in {"TIER_HIGH", "TIER_MEDIUM"}:
            return EntryPathway.STANDARD
        return EntryPathway.STRICT

    def _resolve_dynamic_stop_loss_multiplier(self, signal: TrendSignal) -> float:
        base_multiplier = float(self.config.stop_loss_atr)
        if not bool(getattr(self.config, "dynamic_stop_loss_enabled", True)):
            return base_multiplier
        score = float(signal.bullish_score if signal.direction >= 0 else signal.bearish_score)
        score_ratio = min(1.0, max(0.0, score / 100.0))
        min_scale = float(getattr(self.config, "dynamic_stop_loss_min_scale", 0.85))
        max_scale = float(getattr(self.config, "dynamic_stop_loss_max_scale", 1.05))
        scale = max_scale - ((max_scale - min_scale) * score_ratio)
        return base_multiplier * scale

    def _evaluate_value_area_decision(
        self,
        *,
        volume_profile: VolumeProfileSnapshot | None,
        current_price: float,
        atr_value: float,
        major_direction: int,
        bullish_score: float,
        bearish_score: float,
        execution_frontrun_near_breakout: bool,
        raw_direction: int,
    ) -> ValueAreaDecision:
        if volume_profile is None:
            return ValueAreaDecision(inside_value_area=False, blocked=False)

        inside_value_area = bool(volume_profile.contains_price(current_price))
        if not inside_value_area:
            return ValueAreaDecision(inside_value_area=False, blocked=False)

        edge_threshold = float(getattr(self.config, "value_area_edge_atr_multiplier", 1.0)) * max(0.0, float(atr_value))
        value_area_high = float(volume_profile.value_area_high)
        value_area_low = float(volume_profile.value_area_low)

        drive_first_score = float(getattr(self.config, "drive_first_tradeable_score", 60.0))
        if raw_direction > 0 and float(bullish_score) >= 75.0 and bool(execution_frontrun_near_breakout):
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='High Momentum')
        if raw_direction < 0 and float(bearish_score) >= 75.0 and bool(execution_frontrun_near_breakout):
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='High Momentum')

        if int(major_direction) > 0 and raw_direction > 0 and float(bullish_score) >= drive_first_score and float(current_price) >= value_area_high - edge_threshold:
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='Edge Proximity')

        if int(major_direction) < 0 and raw_direction < 0 and float(bearish_score) >= drive_first_score and float(current_price) <= value_area_low + edge_threshold:
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='Edge Proximity')

        if raw_direction < 0 and float(current_price) >= value_area_high - edge_threshold:
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='VAH Proximity')

        return ValueAreaDecision(inside_value_area=True, blocked=True)

    def _relaxed_short_passes_quality_gate(self, signal: TrendSignal) -> bool:
        if signal.direction >= 0 or not signal.relaxed_entry:
            return True
        bearish_score = float(getattr(signal, "bearish_score", 0.0))
        bullish_score = float(getattr(signal, "bullish_score", 0.0))
        if bearish_score < float(getattr(self.config, "relaxed_short_minimum_score", 48.0)):
            return False
        if (
            int(signal.major_direction) > 0
            and bullish_score > bearish_score
            and (bullish_score - bearish_score) > float(getattr(self.config, "relaxed_short_max_countertrend_score_gap", 12.0))
        ):
            return False
        if bool(getattr(self.config, "relaxed_short_require_early_or_breakdown", True)):
            if not (bool(signal.early_bearish) or bool(signal.execution_breakdown) or bool(signal.weak_bear_bias)):
                return False
        return True

    def _quality_filter_short_signal(self, signal: TrendSignal) -> TrendSignal:
        if signal.direction >= 0:
            return signal
        if not self._relaxed_short_passes_quality_gate(signal):
            blocker_reason = "Blocked_By_RELAXED_SHORT_LOW_QUALITY"
            return TrendSignal(
                **{
                    **signal.__dict__,
                    "direction": 0,
                    "long_setup_blocked": True,
                    "long_setup_reason": "relaxed_short_low_quality",
                    "blocker_reason": blocker_reason,
                    "relaxed_entry": False,
                    "relaxed_reasons": tuple(r for r in signal.relaxed_reasons if not str(r).startswith("SHORT_")),
                }
            )
        return signal

    def _log_value_area_event(
        self,
        *,
        volume_profile: VolumeProfileSnapshot | None,
        current_price: float,
        decision: ValueAreaDecision,
    ) -> None:
        if volume_profile is None or not decision.inside_value_area:
            return
        context = (
            f'POC: {float(volume_profile.poc_price):.4f}, '
            f'VAH: {float(volume_profile.value_area_high):.4f}, '
            f'VAL: {float(volume_profile.value_area_low):.4f}, '
            f'Price: {float(current_price):.4f}'
        )
        if decision.blocked:
            logger.info('Blocked: Inside VA [%s]', context)
            return
        if decision.reason:
            logger.info('Passed: VA Override [Reason: %s] [%s]', decision.reason, context)

    def _evaluate_ml_signal(self, *, execution_frame, direction: int) -> MLSignalDecision:
        engine = getattr(self, "ml_engine", None)
        if engine is None:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_unavailable")
        try:
            return engine.evaluate(
                symbol=self.symbol,
                execution_frame=execution_frame,
                direction=direction,
                min_confidence=float(getattr(self.config, "ml_min_confidence", 0.60)),
            )
        except Exception:
            logger.exception("CTA ML evaluation failed | symbol=%s direction=%s", self.symbol, direction)
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_exception")

    def _apply_ml_entry_gate(
        self,
        *,
        execution_frame,
        final_direction: int,
        long_setup_blocked: bool,
        long_setup_reason: str,
    ) -> tuple[int, bool, str, MLSignalDecision]:
        ml_decision = self._evaluate_ml_signal(execution_frame=execution_frame, direction=final_direction)
        if final_direction != 0 and ml_decision.used_model and not ml_decision.gate_passed:
            logger.info(
                "CTA ML gate blocked | symbol=%s direction=%s aligned_confidence=%.3f min_confidence=%.3f reason=%s",
                self.symbol,
                final_direction,
                float(ml_decision.aligned_confidence),
                float(getattr(self.config, "ml_min_confidence", 0.60)),
                ml_decision.reason,
            )
            return 0, True, f"ml_gate_blocked:{ml_decision.reason}", ml_decision
        return final_direction, long_setup_blocked, long_setup_reason, ml_decision

    def should_notify_action(self, action: str) -> bool:
        if action in {
            "cta:hold",
            "cta:no_signal",
            "cta:insufficient_data",
            "cta:risk_blocked",
            "cta:range_filter_blocked",
            "cta:bullish_ready",
            "cta:order_flow_blocked",
            "cta:slippage_blocked",
            "skip:inactive",
        }:
            return False
        if action.startswith("cta:open_"):
            return False
        return super().should_notify_action(action)

    def flatten_and_cancel_all(self, reason: str) -> None:
        super().flatten_and_cancel_all(reason)
        self.position = None
        self._publish_risk_profile(None)

    def force_risk_exit(self, reason: str) -> str:
        if self.position is None:
            self.client.cancel_all_orders(self.symbol)
            self._publish_risk_profile(None)
            return "cta:risk_exit_no_position"

        self.client.cancel_all_orders(self.symbol)
        self._close_remaining_position(reason=reason)
        self._publish_risk_profile(None)
        return "cta:risk_exit_all_out"

    def get_logical_position(self) -> LogicalPositionSnapshot | None:
        if self.position is None:
            return None
        return LogicalPositionSnapshot(
            symbol=self.symbol,
            side=self.position.side,
            size=self._round_size(self.position.remaining_size),
            strategy_name=self.strategy_name,
        )

    def reset_local_position(self, reason: str) -> None:
        previous_side = self.position.side if self.position is not None else None
        self.position = None
        if previous_side is not None and reason in {"position_mismatch", "exchange_flat"}:
            self._activate_same_direction_cooldown(previous_side, f"recovery_reset:{reason}")
        self._publish_risk_profile(None)

    def _cooldown_remaining_seconds(self, side: str) -> int:
        until_ts = float(self._same_direction_cooldown_until.get(side, 0.0))
        remaining = max(0.0, until_ts - float(self._time_provider()))
        return int(remaining)

    def _build_fast_track_reuse_signature(self, signal: TrendSignal) -> tuple | None:
        if signal.entry_pathway is not EntryPathway.FAST_TRACK or signal.direction <= 0:
            return None
        return (
            int(signal.direction),
            str(signal.execution_trigger_family or "waiting"),
            str(signal.execution_trigger_reason or ""),
            signal.execution_memory_bars_ago,
        )

    def _arm_fast_track_reuse_cooldown(self, signal: TrendSignal) -> None:
        signature = self._build_fast_track_reuse_signature(signal)
        if signature is None:
            return
        cooldown_seconds = max(0, int(getattr(self.config, "fast_track_reuse_cooldown_seconds", 300)))
        if cooldown_seconds <= 0:
            return
        side = "long" if signal.direction > 0 else "short"
        self._fast_track_reuse_signature[side] = signature
        self._fast_track_reuse_until[side] = float(self._time_provider()) + cooldown_seconds
        logger.info(
            "CTA fast-track reuse cooldown armed | symbol=%s side=%s cooldown=%ss signature=%s",
            self.symbol,
            side,
            cooldown_seconds,
            signature,
        )

    def _fast_track_reuse_remaining_seconds(self, signal: TrendSignal) -> int:
        signature = self._build_fast_track_reuse_signature(signal)
        if signature is None:
            return 0
        side = "long" if signal.direction > 0 else "short"
        if self._fast_track_reuse_signature.get(side) != signature:
            return 0
        until_ts = float(self._fast_track_reuse_until.get(side, 0.0))
        remaining = max(0.0, until_ts - float(self._time_provider()))
        return int(remaining)

    def _activate_same_direction_cooldown(self, side: str, reason: str) -> None:
        now_ts = float(self._time_provider())
        cooldown_seconds = max(0, int(getattr(self.config, "same_direction_stop_cooldown_seconds", 300)))
        escalation_window_seconds = max(1, int(getattr(self.config, "same_direction_stop_cooldown_window_seconds", 1200)))
        escalation_count = max(2, int(getattr(self.config, "same_direction_stop_cooldown_escalation_count", 2)))
        escalation_seconds = max(cooldown_seconds, int(getattr(self.config, "same_direction_stop_cooldown_escalation_seconds", 900)))
        if cooldown_seconds <= 0:
            return

        events = self._same_direction_stop_events.setdefault(side, deque())
        while events and (now_ts - events[0]) > escalation_window_seconds:
            events.popleft()
        events.append(now_ts)

        if len(events) >= escalation_count:
            cooldown_seconds = escalation_seconds
            reason = f"{reason}|escalated:{len(events)}in{escalation_window_seconds}s"

        until_ts = now_ts + cooldown_seconds
        self._same_direction_cooldown_until[side] = until_ts
        logger.info(
            "CTA same-direction cooldown armed | symbol=%s side=%s reason=%s cooldown=%ss recent_stopouts=%s window=%ss",
            self.symbol,
            side,
            reason,
            cooldown_seconds,
            len(events),
            escalation_window_seconds,
        )

    def _repeated_entry_zone_remaining_seconds(self, signal: TrendSignal) -> int:
        side = "long" if signal.direction > 0 else "short"
        trigger_family = str(getattr(signal, "execution_trigger_family", "") or "")
        if not trigger_family or trigger_family == "waiting":
            return 0
        tracked_family = getattr(self, "_entry_zone_trigger_family", {"long": None, "short": None}).get(side)
        if tracked_family != trigger_family:
            return 0
        anchor_price = self._entry_zone_anchor_price.get(side)
        if anchor_price in {None, 0.0}:
            return 0
        atr_value = self._normalized_atr(float(signal.price), float(signal.atr))
        tolerance_atr = max(0.0, float(getattr(self.config, "repeated_entry_price_atr_tolerance", 0.6)))
        price_tolerance = atr_value * tolerance_atr
        if price_tolerance <= 0 or abs(float(signal.price) - float(anchor_price)) > price_tolerance:
            return 0
        until_ts = float(getattr(self, "_entry_zone_cooldown_until", {"long": 0.0, "short": 0.0}).get(side, 0.0))
        remaining = max(0.0, until_ts - float(self._time_provider()))
        return int(remaining)

    def _arm_repeated_entry_zone_cooldown(self, signal: TrendSignal, entry_price: float) -> None:
        cooldown_seconds = max(0, int(getattr(self.config, "repeated_entry_family_cooldown_seconds", 900)))
        if cooldown_seconds <= 0:
            return
        side = "long" if signal.direction > 0 else "short"
        trigger_family = str(getattr(signal, "execution_trigger_family", "") or "")
        if not trigger_family or trigger_family == "waiting":
            return
        self._entry_zone_trigger_family[side] = trigger_family
        self._entry_zone_anchor_price[side] = float(entry_price)
        self._entry_zone_cooldown_until[side] = float(self._time_provider()) + cooldown_seconds
        logger.info(
            "CTA repeated-entry cooldown armed | symbol=%s side=%s trigger_family=%s entry_price=%.4f cooldown=%ss",
            self.symbol,
            side,
            trigger_family,
            float(entry_price),
            cooldown_seconds,
        )

    def execute_active_cycle(self) -> str:
        signal = self._build_trend_signal()
        if signal is None:
            self._publish_risk_profile(None)
            return "cta:insufficient_data"

        self._maybe_log_signal_heartbeat(signal)
        self._request_urgent_wakeup_on_signal_transition(signal)
        self._collect_near_miss_sample(signal)
        self._maybe_flush_near_miss_report()

        coordination_action = self._apply_runtime_coordination(signal)
        if coordination_action is not None:
            return coordination_action

        actions: list[str] = []
        closed_position = False

        if self.position is not None:
            actions, closed_position = self._manage_position(signal)
            if actions:
                return "+".join(actions)
            if closed_position:
                self._publish_risk_profile(None)
                return "cta:hold"

        action: str
        if self.position is None and signal.direction != 0:
            action = self._open_position(signal)
        else:
            self._publish_risk_profile(signal)
            if self.position is None and signal.long_setup_blocked:
                action = "cta:range_filter_blocked"
            elif self.position is None and signal.bullish_ready and signal.raw_direction == 0:
                action = "cta:bullish_ready"
            else:
                action = "cta:no_signal" if self.position is None else "cta:hold"

        if self.position is None and action in {
            "cta:order_flow_blocked",
            "cta:entry_quality_blocked",
            "cta:entry_location_blocked",
            "cta:reward_risk_blocked",
            "cta:repeated_entry_zone_cooldown",
            "cta:fast_track_reuse_cooldown",
            "cta:same_direction_cooldown",
            "cta:range_filter_blocked",
        }:
            self._journal_event(
                event_type="blocked_signal",
                side="long" if signal.raw_direction > 0 else "short" if signal.raw_direction < 0 else None,
                action=action,
                trigger_family=str(signal.execution_trigger_family or "waiting"),
                trigger_reason=str(signal.execution_trigger_reason or ""),
                pathway=signal.entry_pathway.name,
                price=float(signal.price),
                metadata={
                    "raw_direction": int(signal.raw_direction),
                    "signal_confidence": float(signal.signal_confidence),
                    "signal_quality_tier": str(signal.signal_quality_tier),
                    "blocker_reason": str(signal.blocker_reason or ""),
                    "long_setup_reason": str(signal.long_setup_reason or ""),
                    "relaxed_entry": bool(signal.relaxed_entry),
                    "relaxed_reasons": list(signal.relaxed_reasons),
                    "ml_used_model": bool(signal.ml_used_model),
                    "ml_gate_passed": bool(signal.ml_gate_passed),
                    "ml_aligned_confidence": float(signal.ml_aligned_confidence),
                    "ml_reason": str(signal.ml_reason),
                    "entry_location_score": float(signal.entry_location_score),
                    "entry_location_reasons": list(signal.entry_location_reasons),
                },
            )

        return action

    def _build_trend_signal(self) -> TrendSignal | None:
        mtf_signal = self.mtf_engine.build_signal()
        if mtf_signal is None:
            return None

        execution_frame = mtf_signal.execution_frame
        execution_obv = compute_obv(execution_frame)
        atr_series = compute_atr(execution_frame, length=self.config.atr_period)

        obv_confirmation = compute_obv_confirmation_snapshot(
            execution_frame,
            obv=execution_obv,
            sma_period=self.config.obv_sma_period,
            zscore_window=self.config.obv_zscore_window,
        )
        obv_bias = 1 if obv_confirmation.above_sma else -1 if obv_confirmation.below_sma else 0
        bullish_raw_direction = 1 if (mtf_signal.fully_aligned and int(mtf_signal.major_direction) >= 0 and not bool(getattr(mtf_signal, "bearish_ready", False))) else 0
        bearish_raw_direction = -1 if (
            mtf_signal.fully_aligned
            and bool(getattr(mtf_signal, "bearish_ready", False))
            and (
                int(mtf_signal.major_direction) < 0
                or bool(getattr(mtf_signal, "weak_bear_bias", False))
                or bool(getattr(mtf_signal, "early_bearish", False))
            )
        ) else 0
        raw_direction = bullish_raw_direction if bullish_raw_direction != 0 else bearish_raw_direction
        entry_pathway = self._resolve_entry_pathway(mtf_signal)
        signal_quality_tier = str(getattr(getattr(mtf_signal, "signal_quality_tier", None), "name", "TIER_LOW"))
        signal_confidence = float(getattr(mtf_signal, "signal_confidence", 0.0) or 0.0)
        signal_strength_bonus = float(getattr(mtf_signal, "signal_strength_bonus", 0.0) or 0.0)
        obv_gate = self._resolve_obv_gate(mtf_signal)
        obv_threshold = float(obv_gate.threshold)
        obv_exempt = bool(obv_gate.exempt)
        drive_first_tradeable = bool(float(mtf_signal.bullish_score) >= float(getattr(self.config, "drive_first_tradeable_score", 60.0)))
        relaxed_obv_allowed = bool(
            raw_direction > 0
            and int(mtf_signal.major_direction) > 0
            and drive_first_tradeable
            and float(obv_confirmation.zscore) > float(obv_threshold)
        )
        volume_filter_passed = False
        if raw_direction > 0:
            volume_filter_passed = bool(obv_gate.passed(obv_confirmation) or relaxed_obv_allowed)
        elif raw_direction < 0:
            volume_filter_passed = bool(obv_gate.passed(obv_confirmation))
        current_price = float(execution_frame["close"].iloc[-1])
        volume_profile = compute_volume_profile(
            execution_frame,
            lookback_hours=self.config.volume_profile_lookback_hours,
            value_area_pct=self.config.volume_profile_value_area_pct,
            bin_count=self.config.volume_profile_bin_count,
        )
        inside_value_area = bool(volume_profile.contains_price(current_price)) if volume_profile is not None else False
        high_momentum_clearance = self._evaluate_high_momentum_clearance(
            mtf_signal=mtf_signal,
            inside_value_area=inside_value_area,
            raw_direction=raw_direction,
        )

        final_direction = raw_direction
        long_setup_blocked = False
        long_setup_reason = ""
        obv_confirmation_passed = True
        relaxed_reasons: list[str] = []
        quick_trade_mode = False
        standard_path = entry_pathway is EntryPathway.STANDARD
        liquidity_sweep = bool(getattr(mtf_signal.execution_trigger, "liquidity_sweep", False))
        liquidity_sweep_side = str(getattr(mtf_signal.execution_trigger, "liquidity_sweep_side", "") or "")
        oi_change_pct = float(getattr(mtf_signal, "oi_change_pct", 0.0) or 0.0)
        funding_rate = float(getattr(mtf_signal, "funding_rate", 0.0) or 0.0)
        is_short_squeeze = bool(getattr(mtf_signal, "is_short_squeeze", False))
        is_long_liquidation = bool(getattr(mtf_signal, "is_long_liquidation", False))
        resonance_allowed, resonance_reason = self._supports_sweep_resonance(
            direction=raw_direction,
            sweep_side=liquidity_sweep_side,
            oi_change_pct=oi_change_pct,
            is_short_squeeze=is_short_squeeze,
            is_long_liquidation=is_long_liquidation,
        )
        sweep_extreme_price = None
        if liquidity_sweep_side == "long":
            sweep_candidates = [getattr(mtf_signal.execution_trigger, "prior_low", None), getattr(mtf_signal.execution_trigger, "latch_low_price", None)]
            sweep_candidates = [float(v) for v in sweep_candidates if v not in (None, 0, "0")]
            if sweep_candidates:
                sweep_extreme_price = min(sweep_candidates)
        elif liquidity_sweep_side == "short":
            sweep_candidates = [getattr(mtf_signal.execution_trigger, "prior_high", None), getattr(mtf_signal.execution_trigger, "latch_high_price", None)]
            sweep_candidates = [float(v) for v in sweep_candidates if v not in (None, 0, "0")]
            if sweep_candidates:
                sweep_extreme_price = max(sweep_candidates)
        standard_obv_floor = max(
            float(getattr(self.config, "near_breakout_release_obv_zscore_floor", -0.25)),
            float(obv_threshold) - 1.0,
        )

        if raw_direction > 0:
            obv_confirmation_passed = volume_filter_passed
            value_area_decision = self._evaluate_value_area_decision(
                volume_profile=volume_profile,
                current_price=current_price,
                atr_value=float(atr_series.iloc[-1]),
                major_direction=int(mtf_signal.major_direction),
                bullish_score=float(mtf_signal.bullish_score),
                bearish_score=float(getattr(mtf_signal, "bearish_score", 0.0)),
                execution_frontrun_near_breakout=bool(mtf_signal.execution_trigger.frontrun_near_breakout),
                raw_direction=raw_direction,
            )
            if not obv_exempt and not obv_confirmation_passed:
                if resonance_allowed:
                    relaxed_reasons.append(resonance_reason or "SWEEP_RESONANCE_OBV_BYPASS")
                elif standard_path and float(obv_confirmation.zscore) >= standard_obv_floor:
                    relaxed_reasons.append(f"STANDARD_OBV({float(obv_confirmation.zscore):.2f}) >= Floor({float(standard_obv_floor):.2f})")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "obv_strength_not_confirmed"
            elif not obv_exempt and not obv_confirmation.above_sma:
                if resonance_allowed:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + "_BELOW_SMA_BYPASS")
                elif relaxed_obv_allowed:
                    relaxed_reasons.append(f"OBV({float(obv_confirmation.zscore):.2f}) > Floor({float(obv_threshold):.2f})")
                elif standard_path and float(obv_confirmation.zscore) >= standard_obv_floor:
                    relaxed_reasons.append(f"STANDARD_OBV_BELOW_SMA({float(obv_confirmation.zscore):.2f})")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "obv_below_sma"
            elif volume_profile is None:
                if resonance_allowed:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + "_NO_VOLUME_PROFILE")
                elif standard_path:
                    relaxed_reasons.append("STANDARD_NO_VOLUME_PROFILE")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "missing_volume_profile"
            elif not volume_profile.above_poc(current_price):
                poc_reclaim_tolerance = max(
                    float(getattr(self.config, "value_area_edge_atr_multiplier", 1.0)) * max(float(atr_series.iloc[-1]), 0.0),
                    float(current_price) * 0.001,
                )
                poc_gap = max(0.0, float(volume_profile.poc_price) - float(current_price))
                if resonance_allowed and poc_gap <= max(poc_reclaim_tolerance, float(atr_series.iloc[-1]) * 0.5):
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + f"_POC_RECLAIM({poc_gap:.4f})")
                elif standard_path and bool(mtf_signal.execution_trigger.frontrun_near_breakout or mtf_signal.execution_trigger.prior_high_break) and poc_gap <= poc_reclaim_tolerance:
                    relaxed_reasons.append(f"STANDARD_POC_RECLAIM_OK({poc_gap:.4f})")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "below_poc"
            elif value_area_decision.blocked:
                # 如果是 pullback 支撑入场，允许绕过 inside_value_area 限制
                if bool(getattr(mtf_signal.execution_trigger, 'pullback_near_support', False)) and raw_direction > 0:
                    pass  # 不 block，允许入场
                elif resonance_allowed:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + "_VA_BYPASS")
                elif standard_path:
                    relaxed_reasons.append("STANDARD_VA_BYPASS")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "inside_value_area"
            elif value_area_decision.inside_value_area and value_area_decision.reason:
                if value_area_decision.reason in {"High Momentum", "Edge Proximity"}:
                    relaxed_reasons.append(f"VA:{value_area_decision.reason}")
                self._log_value_area_event(
                    volume_profile=volume_profile,
                    current_price=current_price,
                    decision=value_area_decision,
                )
            elif not volume_profile.above_value_area(current_price):
                if resonance_allowed:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + "_BELOW_VAH_OK")
                elif standard_path:
                    relaxed_reasons.append("STANDARD_BELOW_VAH_OK")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "below_value_area_high"

            if long_setup_reason == "inside_value_area":
                self._log_value_area_event(
                    volume_profile=volume_profile,
                    current_price=current_price,
                    decision=value_area_decision,
                )

            if long_setup_blocked:
                final_direction = 0

            if final_direction > 0 and high_momentum_clearance.activated:
                logger.info("[FINAL_TRIGGER_OVERRIDE] Full Clearance - All Guards Relaxed for High Momentum Breakout")
            if final_direction > 0 and bool(getattr(mtf_signal, "rsi_blocking_overridden", False)):
                relaxed_reasons.append(
                    f"RSI({float(mtf_signal.swing_rsi):.2f}) tolerated with Score({float(mtf_signal.bullish_score):.0f})"
                )
        elif raw_direction < 0:
            obv_confirmation_passed = volume_filter_passed
            if not volume_filter_passed:
                if resonance_allowed:
                    relaxed_reasons.append(resonance_reason or "SWEEP_RESONANCE_SHORT_OBV_BYPASS")
                else:
                    final_direction = 0
                    long_setup_blocked = True
                    long_setup_reason = "obv_above_sma" if bool(obv_confirmation.above_sma) else "obv_strength_not_confirmed"
            elif volume_profile is None:
                if resonance_allowed:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + "_NO_VOLUME_PROFILE")
                else:
                    final_direction = 0
                    long_setup_blocked = True
                    long_setup_reason = "missing_volume_profile"
            elif float(current_price) >= float(volume_profile.poc_price):
                poc_gap = max(0.0, float(current_price) - float(volume_profile.poc_price))
                poc_tolerance = max(float(atr_series.iloc[-1]) * 0.5, float(current_price) * 0.001)
                if resonance_allowed and poc_gap <= poc_tolerance:
                    relaxed_reasons.append((resonance_reason or "SWEEP_RESONANCE") + f"_POC_REJECT_OK({poc_gap:.4f})")
                else:
                    final_direction = 0
                    long_setup_blocked = True
                    long_setup_reason = "above_poc"
            if long_setup_blocked and long_setup_reason == "obv_strength_not_confirmed":
                bearish_score = float(getattr(mtf_signal, "bearish_score", 0.0))
                bullish_score = float(getattr(mtf_signal, "bullish_score", 0.0))
                obv_scalp_candidate = bool(
                    bearish_score >= float(getattr(self.config, "obv_scalp_min_bearish_score", 52.0))
                    and bullish_score <= float(getattr(self.config, "obv_scalp_max_bullish_score", 62.0))
                    and bool(getattr(mtf_signal, "weak_bear_bias", False))
                    and (
                        not bool(getattr(self.config, "obv_scalp_require_early_bearish", True))
                        or bool(getattr(mtf_signal, "early_bearish", False))
                    )
                    and (
                        "major_bull_retest_ready" in str(mtf_signal.execution_trigger.reason)
                        or "Triggered via Memory Window" in str(mtf_signal.execution_trigger.reason)
                    )
                    and bool(mtf_signal.execution_trigger.bullish_memory_active)
                    and not bool(obv_confirmation.above_sma)
                    and float(obv_confirmation.zscore) <= float(getattr(self.config, "obv_scalp_max_positive_obv_zscore", 0.15))
                )
                if obv_scalp_candidate:
                    quick_trade_mode = True
                    long_setup_blocked = False
                    final_direction = raw_direction
                    relaxed_reasons.append("OBV_SCALP_OVERRIDE")
                    logger.info(
                        "CTA quick short scalp override | symbol=%s reason=%s bearish_score=%.1f weak_bear=%s obv_above_sma=%s obv_z=%.2f",
                        self.symbol,
                        mtf_signal.execution_trigger.reason,
                        float(getattr(mtf_signal, 'bearish_score', 0.0)),
                        bool(getattr(mtf_signal, 'weak_bear_bias', False)),
                        bool(obv_confirmation.above_sma),
                        float(obv_confirmation.zscore),
                    )

        final_direction, long_setup_blocked, long_setup_reason, ml_decision = self._apply_ml_entry_gate(
            execution_frame=execution_frame,
            final_direction=final_direction,
            long_setup_blocked=long_setup_blocked,
            long_setup_reason=long_setup_reason,
        )

        blocker_reason = mtf_signal.blocker_reason
        if long_setup_blocked:
            blocker_reason = f"Blocked_By_{str(long_setup_reason).upper()}"
        if self.signal_profiler is not None:
            grid_center = self.grid_center_provider() if callable(self.grid_center_provider) else None
            self.signal_profiler.record(
                mtf_signal,
                grid_center_price=grid_center,
                blocker_reason=blocker_reason,
                execution_obv_threshold=float(obv_threshold),
            )

        candidate_state, candidate_reason = self._derive_candidate_state(type("_PreSignal", (), {
            "bullish_ready": bool(getattr(mtf_signal, "bullish_ready", False)),
            "raw_direction": int(raw_direction),
            "execution_memory_active": bool(getattr(mtf_signal, "execution_trigger", None) and getattr(mtf_signal.execution_trigger, "bullish_memory_active", False)),
            "execution_latch_active": bool(getattr(mtf_signal, "execution_trigger", None) and getattr(mtf_signal.execution_trigger, "bullish_latch_active", False)),
            "execution_frontrun_near_breakout": bool(getattr(mtf_signal, "execution_trigger", None) and getattr(mtf_signal.execution_trigger, "frontrun_near_breakout", False)),
            "execution_breakout": bool(getattr(mtf_signal, "execution_trigger", None) and getattr(mtf_signal.execution_trigger, "prior_high_break", False)),
            "execution_trigger_reason": str(getattr(getattr(mtf_signal, "execution_trigger", None), "reason", "")),
            "long_setup_reason": str(long_setup_reason),
            "blocker_reason": str(blocker_reason),
        })())
        signal = TrendSignal(
            direction=final_direction,
            raw_direction=raw_direction,
            major_direction=mtf_signal.major_direction,
            major_bias_score=mtf_signal.major_bias_score,
            weak_bull_bias=mtf_signal.weak_bull_bias,
            weak_bear_bias=bool(getattr(mtf_signal, "weak_bear_bias", False)),
            early_bullish=mtf_signal.early_bullish,
            early_bearish=bool(getattr(mtf_signal, "early_bearish", False)),
            entry_size_multiplier=mtf_signal.entry_size_multiplier,
            swing_rsi=mtf_signal.swing_rsi,
            swing_rsi_slope=mtf_signal.swing_rsi_slope,
            bullish_score=mtf_signal.bullish_score,
            bearish_score=float(getattr(mtf_signal, "bearish_score", 0.0)),
            bullish_threshold=mtf_signal.bullish_threshold,
            bearish_threshold=float(getattr(mtf_signal, "bearish_threshold", 0.0)),
            bullish_ready=mtf_signal.bullish_ready,
            bearish_ready=bool(getattr(mtf_signal, "bearish_ready", False)),
            execution_entry_mode=mtf_signal.execution_entry_mode,
            execution_golden_cross=mtf_signal.execution_trigger.kdj_golden_cross,
            execution_breakout=mtf_signal.execution_trigger.prior_high_break,
            execution_breakdown=mtf_signal.execution_trigger.prior_low_break,
            execution_memory_active=mtf_signal.execution_trigger.bullish_memory_active,
            execution_latch_active=mtf_signal.execution_trigger.bullish_latch_active,
            execution_latch_price=mtf_signal.execution_trigger.latch_low_price,
            execution_frontrun_near_breakout=mtf_signal.execution_trigger.frontrun_near_breakout,
            execution_memory_bars_ago=mtf_signal.execution_trigger.bullish_cross_bars_ago,
            execution_trigger_family=("obv_scalp" if quick_trade_mode else str(getattr(mtf_signal.execution_trigger, "family", "waiting"))),
            execution_trigger_group=str(getattr(mtf_signal.execution_trigger, "group", "waiting")),
            execution_trigger_reason=(f"OBV_SCALP|{mtf_signal.execution_trigger.reason}" if quick_trade_mode else mtf_signal.execution_trigger.reason),
            pullback_near_support=bool(getattr(mtf_signal.execution_trigger, 'pullback_near_support', False)),
            volatility_squeeze_breakout=bool(getattr(mtf_signal, "execution_trigger", None) and getattr(mtf_signal.execution_trigger, "family", "") in {"starter_frontrun", "bullish_memory_breakout"} and not bool(getattr(mtf_signal.execution_trigger, "pending_retest", False))),
            stretch_value=float(getattr(mtf_signal, "stretch_value", 0.0) or 0.0),
            stretch_blocked=bool(getattr(mtf_signal, "stretch_blocked", False)),
            pending_retest=bool(getattr(mtf_signal, "pending_retest", False)),
            exhaustion_penalty_applied=bool(getattr(mtf_signal, "exhaustion_penalty_applied", False)),
            mtf_aligned=mtf_signal.fully_aligned,
            obv_bias=obv_bias,
            obv_confirmation=obv_confirmation,
            obv_threshold=obv_threshold,
            obv_confirmation_passed=obv_confirmation_passed,
            volume_filter_passed=volume_filter_passed,
            volume_profile=volume_profile,
            long_setup_blocked=long_setup_blocked,
            long_setup_reason=long_setup_reason,
            price=current_price,
            atr=float(atr_series.iloc[-1]),
            risk_percent=self._resolve_risk_percent(mtf_signal),
            blocker_reason=blocker_reason,
            data_alignment_valid=mtf_signal.data_alignment_valid,
            data_mismatch_ms=mtf_signal.data_mismatch_ms,
            relaxed_entry=bool(relaxed_reasons),
            relaxed_reasons=tuple(dict.fromkeys(relaxed_reasons)),
            quick_trade_mode=quick_trade_mode,
            entry_pathway=entry_pathway,
            signal_quality_tier=signal_quality_tier,
            signal_confidence=signal_confidence,
            signal_strength_bonus=signal_strength_bonus,
            candidate_state=candidate_state,
            candidate_reason=candidate_reason,
            ml_used_model=bool(ml_decision.used_model),
            ml_model_used=bool(ml_decision.used_model),
            ml_prediction=int(ml_decision.prediction),
            ml_probability_up=float(ml_decision.probability_up),
            ml_aligned_confidence=float(ml_decision.aligned_confidence),
            ml_gate_passed=bool(ml_decision.gate_passed),
            ml_reason=str(ml_decision.reason),
            ml_gate_reason=str(ml_decision.reason),
            liquidity_sweep=liquidity_sweep,
            liquidity_sweep_side=liquidity_sweep_side,
            oi_change_pct=oi_change_pct,
            funding_rate=funding_rate,
            is_short_squeeze=is_short_squeeze,
            is_long_liquidation=is_long_liquidation,
            resonance_allowed=resonance_allowed,
            resonance_reason=resonance_reason,
            sweep_extreme_price=sweep_extreme_price,
        )
        entry_location_score, entry_location_reasons = self._score_entry_location(signal)
        signal.entry_location_score = float(entry_location_score)
        signal.entry_location_reasons = tuple(entry_location_reasons)
        signal = self._quality_filter_short_signal(signal)
        candidate_state, candidate_reason = self._derive_candidate_state(signal)
        signal.candidate_state = candidate_state
        signal.candidate_reason = candidate_reason
        signal = self._annotate_watch_sample_persistence(signal)
        logger.info(
            "CTA Signal | symbol=%s quality=%s pathway=%s direction=%s raw_direction=%s score=%.1f confidence=%.2f strength_bonus=%.1f aligned=%s trigger=%s",
            self.symbol,
            signal.signal_quality_tier,
            signal.entry_pathway.name,
            signal.direction,
            signal.raw_direction,
            float(signal.bullish_score if signal.raw_direction >= 0 else signal.bearish_score),
            float(signal.signal_confidence),
            float(signal.signal_strength_bonus),
            bool(signal.mtf_aligned),
            signal.execution_trigger_reason,
        )
        return signal

    def _is_breakout_style_signal(self, signal: TrendSignal) -> bool:
        mode = str(getattr(signal, "execution_entry_mode", "") or "").lower()
        family = str(getattr(signal, "execution_trigger_family", "") or "").lower()
        return bool(
            getattr(signal, "execution_breakout", False)
            or getattr(signal, "execution_breakdown", False)
            or getattr(signal, "execution_frontrun_near_breakout", False)
            or any(marker in mode for marker in ("breakout", "breakdown", "frontrun", "starter"))
            or any(marker in family for marker in ("breakout", "reclaim", "frontrun", "continuation"))
        )

    def _resolve_reverse_intercept_reason(self, signal: TrendSignal) -> str | None:
        if not self._is_breakout_style_signal(signal):
            return None
        if signal.direction > 0 and bool(getattr(signal, "is_short_squeeze", False)):
            return "breakout_long_into_short_squeeze"
        if signal.direction < 0 and bool(getattr(signal, "is_long_liquidation", False)):
            return "breakout_short_into_long_liquidation"
        return None

    def _supports_sweep_resonance(self, *, direction: int, sweep_side: str, oi_change_pct: float, is_short_squeeze: bool, is_long_liquidation: bool) -> tuple[bool, str]:
        min_oi_turn_pct = float(getattr(self.config, "sweep_resonance_min_oi_turn_pct", 0.15))
        if direction > 0 and sweep_side == "long":
            if oi_change_pct >= min_oi_turn_pct:
                return True, f"SWEEP_RESONANCE_LONG_OI_TURN({oi_change_pct:.2f}%)"
            if is_long_liquidation:
                return True, "SWEEP_RESONANCE_LONG_LIQUIDATION_FLUSH"
        if direction < 0 and sweep_side == "short":
            if oi_change_pct >= min_oi_turn_pct:
                return True, f"SWEEP_RESONANCE_SHORT_OI_TURN({oi_change_pct:.2f}%)"
            if is_short_squeeze:
                return True, "SWEEP_RESONANCE_SHORT_SQUEEZE_FLUSH"
        return False, ""

    def _resolve_sweep_stop_anchor(self, signal: TrendSignal, entry_price: float, fallback_stop_distance: float) -> tuple[float | None, str | None]:
        sweep_extreme_price = getattr(signal, "sweep_extreme_price", None)
        if not bool(getattr(signal, "liquidity_sweep", False)) or sweep_extreme_price in (None, 0, "0"):
            return None, None
        buffer_ratio = float(getattr(self.config, "sweep_stop_buffer_atr_ratio", 0.15))
        min_price_buffer_ratio = float(getattr(self.config, "sweep_stop_min_price_ratio", 0.0006))
        atr_value = self._normalized_atr(entry_price, signal.atr)
        buffer_distance = max(atr_value * buffer_ratio, float(entry_price) * min_price_buffer_ratio)
        anchor = float(sweep_extreme_price)
        if signal.direction > 0:
            candidate = anchor + buffer_distance
            if candidate >= entry_price:
                return None, None
            return candidate, f"sweep_anchor_long({anchor:.4f})"
        if signal.direction < 0:
            candidate = anchor - buffer_distance
            if candidate <= entry_price:
                return None, None
            return candidate, f"sweep_anchor_short({anchor:.4f})"
        return None, None

    def _resonance_execution_allowance(self, signal: TrendSignal) -> tuple[bool, str]:
        if not bool(getattr(signal, "resonance_allowed", False)):
            return False, ""
        if not bool(getattr(signal, "liquidity_sweep", False)):
            return False, ""
        trigger_family = str(getattr(signal, "execution_trigger_family", "") or "")
        if trigger_family not in {"spring_reclaim", "upthrust_reclaim"}:
            return False, ""
        direction = int(getattr(signal, "direction", 0) or 0)
        sweep_side = str(getattr(signal, "liquidity_sweep_side", "") or "")
        if (direction > 0 and sweep_side != "long") or (direction < 0 and sweep_side != "short"):
            return False, ""
        if int(getattr(signal, "major_direction", 0) or 0) != direction:
            return False, ""
        quality_tier = str(getattr(signal, "signal_quality_tier", "TIER_LOW") or "TIER_LOW")
        if quality_tier not in {"TIER_HIGH", "TIER_MEDIUM"}:
            return False, ""
        confidence = float(getattr(signal, "signal_confidence", 0.0) or 0.0)
        if confidence < float(getattr(self.config, "sweep_resonance_execution_min_confidence", 0.58)):
            return False, ""
        primary_score = float(signal.bullish_score if direction > 0 else signal.bearish_score)
        minimum_score = float(getattr(self.config, "sweep_resonance_execution_min_score", 74.0))
        if primary_score < minimum_score:
            return False, ""
        return True, f"{trigger_family}:{str(getattr(signal, 'resonance_reason', '') or 'SWEEP_RESONANCE')}"

    def _effective_signal_obv_threshold(self, signal: TrendSignal) -> float:
        if signal.obv_threshold is not None:
            return float(signal.obv_threshold)
        return float(self.config.obv_zscore_threshold)

    def _candidate_ready_side(self, signal: TrendSignal) -> int:
        raw_direction = int(getattr(signal, "raw_direction", 0) or 0)
        if raw_direction > 0:
            return 1
        if raw_direction < 0:
            return -1
        if bool(getattr(signal, "bullish_ready", False)) and not bool(getattr(signal, "bearish_ready", False)):
            return 1
        if bool(getattr(signal, "bearish_ready", False)) and not bool(getattr(signal, "bullish_ready", False)):
            return -1
        if bool(getattr(signal, "early_bearish", False)) or bool(getattr(signal, "weak_bear_bias", False)):
            return -1
        return 1 if bool(getattr(signal, "bullish_ready", False)) else 0

    def _candidate_not_ready_reason(self, signal: TrendSignal, ready_side: int) -> str:
        if ready_side < 0:
            return str(getattr(signal, "blocker_reason", "") or getattr(signal, "long_setup_reason", "") or "bearish_not_ready")
        return str(getattr(signal, "blocker_reason", "") or getattr(signal, "long_setup_reason", "") or "bullish_not_ready")

    def _is_execution_near_ready(self, signal: TrendSignal) -> bool:
        ready_side = self._candidate_ready_side(signal)
        if ready_side == 0:
            return False
        breakout_flag = bool(signal.execution_breakout) if ready_side > 0 else bool(getattr(signal, "execution_breakdown", False))
        return bool(
            (bool(signal.bullish_ready) if ready_side > 0 else bool(getattr(signal, "bearish_ready", False)))
            and (
                int(signal.raw_direction) == ready_side
                or signal.execution_memory_active
                or signal.execution_latch_active
                or signal.execution_frontrun_near_breakout
                or breakout_flag
            )
        )

    def _derive_candidate_state(self, signal: TrendSignal) -> tuple[str, str]:
        ready_side = self._candidate_ready_side(signal)
        if ready_side == 0:
            return "idle", self._candidate_not_ready_reason(signal, ready_side)
        decision = str(getattr(signal, "entry_decider_decision", "") or "")
        decision_reasons = tuple(getattr(signal, "entry_decider_reasons", ()) or ())
        if decision == "watch":
            return "watch", str(
                getattr(signal, "long_setup_reason", "")
                or (decision_reasons[0] if decision_reasons else "")
                or getattr(signal, "execution_trigger_reason", "")
                or getattr(signal, "blocker_reason", "")
                or "watch"
            )
        if int(signal.raw_direction) == ready_side:
            return "trigger_ready", str(signal.execution_trigger_reason or signal.long_setup_reason or signal.blocker_reason or "trigger_ready")
        if self._is_execution_near_ready(signal):
            return "armed", str(signal.long_setup_reason or signal.execution_trigger_reason or signal.blocker_reason or "armed")
        return "setup", str(signal.long_setup_reason or signal.execution_trigger_reason or signal.blocker_reason or "setup")

    def _annotate_watch_sample_persistence(self, signal: TrendSignal) -> TrendSignal:
        ready_side = self._candidate_ready_side(signal)
        if ready_side == 0:
            return signal
        side = "long" if ready_side > 0 else "short"
        now_ts = float(self._time_provider())
        existing = self._recent_watch_samples.get(side)
        promoted = False
        age_seconds: float | None = None
        origin_reason = ""
        if existing is not None:
            origin_reason = str(existing.get("candidate_reason", "") or existing.get("execution_trigger_reason", "") or "")
            try:
                age_seconds = max(0.0, now_ts - float(existing.get("captured_at", now_ts)))
            except Exception:
                age_seconds = None
            promoted = bool(signal.candidate_state == "trigger_ready")
        if signal.candidate_state == "watch":
            self._recent_watch_samples[side] = {
                "captured_at": now_ts,
                "candidate_reason": str(signal.candidate_reason or signal.long_setup_reason or signal.execution_trigger_reason or "watch"),
                "execution_trigger_reason": str(signal.execution_trigger_reason or ""),
            }
            promoted = False
            age_seconds = 0.0
            origin_reason = str(signal.candidate_reason or signal.long_setup_reason or signal.execution_trigger_reason or "watch")
        elif signal.candidate_state == "trigger_ready" and promoted:
            self._recent_watch_samples.pop(side, None)
        elif signal.candidate_state not in {"setup", "armed"} and existing is None:
            age_seconds = None
            origin_reason = ""
        signal.watch_sample_promoted = promoted
        signal.watch_sample_age_seconds = age_seconds
        signal.watch_sample_origin_reason = origin_reason
        return signal

    def _build_signal_heartbeat_payload(self, signal: TrendSignal) -> dict[str, float | str | bool | None]:
        obv = signal.obv_confirmation
        threshold = self._effective_signal_obv_threshold(signal)
        candidate_state, candidate_reason = self._derive_candidate_state(signal)
        return {
            "symbol": self.symbol,
            "candidate_state": candidate_state,
            "candidate_reason": candidate_reason,
            "bullish_ready": bool(signal.bullish_ready),
            "bullish_score": float(signal.bullish_score),
            "bearish_score": float(signal.bearish_score),
            "bullish_threshold": float(signal.bullish_threshold),
            "bearish_threshold": float(signal.bearish_threshold),
            "major_bias_score": float(signal.major_bias_score),
            "weak_bull_bias": bool(signal.weak_bull_bias),
            "weak_bear_bias": bool(signal.weak_bear_bias),
            "early_bullish": bool(signal.early_bullish),
            "early_bearish": bool(signal.early_bearish),
            "entry_size_multiplier": float(signal.entry_size_multiplier),
            "swing_rsi": float(signal.swing_rsi),
            "swing_rsi_slope": float(signal.swing_rsi_slope),
            "raw_direction": int(signal.raw_direction),
            "direction": int(signal.direction),
            "execution_entry_mode": str(signal.execution_entry_mode),
            "entry_pathway": str(signal.entry_pathway.name),
            "signal_quality_tier": str(signal.signal_quality_tier),
            "signal_confidence": float(signal.signal_confidence),
            "signal_strength_bonus": float(signal.signal_strength_bonus),
            "entry_decider_decision": str(signal.entry_decider_decision),
            "entry_decider_score": float(signal.entry_decider_score),
            "entry_decider_reasons": list(signal.entry_decider_reasons),
            "watch_sample_promoted": bool(getattr(signal, "watch_sample_promoted", False)),
            "watch_sample_age_seconds": getattr(signal, "watch_sample_age_seconds", None),
            "watch_sample_origin_reason": str(getattr(signal, "watch_sample_origin_reason", "") or ""),
            "execution_trigger_reason": str(signal.execution_trigger_reason),
            "execution_memory_active": bool(signal.execution_memory_active),
            "execution_latch_active": bool(signal.execution_latch_active),
            "execution_latch_price": signal.execution_latch_price,
            "execution_frontrun_near_breakout": bool(signal.execution_frontrun_near_breakout),
            "execution_memory_bars_ago": signal.execution_memory_bars_ago,
            "obv_current": float(obv.current_obv),
            "obv_sma": float(obv.sma_value),
            "obv_above_sma": bool(obv.above_sma),
            "obv_increment": float(obv.increment_value),
            "obv_increment_mean": float(obv.increment_mean),
            "obv_increment_std": float(obv.increment_std),
            "obv_zscore": float(obv.zscore),
            "obv_zscore_threshold": float(threshold),
            "obv_zscore_gap": float(obv.zscore - float(threshold)),
            "obv_confirmation_passed": bool(signal.obv_confirmation_passed),
            "volume_filter_passed": bool(signal.volume_filter_passed),
            "mtf_aligned": bool(signal.mtf_aligned),
            "risk_percent": float(signal.risk_percent),
            "long_setup_reason": str(signal.long_setup_reason),
            "price": float(signal.price),
            "atr": float(signal.atr),
            "blocker_reason": str(signal.blocker_reason),
            "relaxed_entry": bool(signal.relaxed_entry),
            "relaxed_reasons": list(signal.relaxed_reasons),
            "ml_used_model": bool(signal.ml_used_model),
            "ml_prediction": int(signal.ml_prediction),
            "ml_probability_up": float(signal.ml_probability_up),
            "ml_aligned_confidence": float(signal.ml_aligned_confidence),
            "ml_gate_passed": bool(signal.ml_gate_passed),
            "ml_reason": str(signal.ml_reason),
            "entry_location_score": float(signal.entry_location_score),
            "entry_location_reasons": list(signal.entry_location_reasons),
            "data_alignment_valid": bool(signal.data_alignment_valid),
            "data_mismatch_ms": int(signal.data_mismatch_ms),
        }

    def _maybe_log_signal_heartbeat(self, signal: TrendSignal) -> None:
        interval = float(getattr(self.config, "heartbeat_interval_seconds", 300.0) or 0.0)
        if interval <= 0:
            return
        now = self._time_provider()
        if now - float(self._last_signal_heartbeat_at) < interval:
            return
        self._last_signal_heartbeat_at = now
        logger.info("CTA signal heartbeat | %s", self._build_signal_heartbeat_payload(signal))

    def _request_urgent_wakeup_on_signal_transition(self, signal: TrendSignal) -> None:
        if self.runtime_context is None:
            self._last_major_direction = int(signal.major_direction)
            self._last_bullish_ready = bool(signal.bullish_ready)
            return

        reasons: list[str] = []
        major_direction = int(signal.major_direction)
        bullish_ready = bool(signal.bullish_ready)
        if self._last_major_direction is not None and self._last_major_direction != major_direction:
            reasons.append(f"cta_major_direction:{self._last_major_direction}->{major_direction}")
        if self._last_bullish_ready is not None and self._last_bullish_ready != bullish_ready:
            reasons.append(f"cta_bullish_ready:{self._last_bullish_ready}->{bullish_ready}")
        self._last_major_direction = major_direction
        self._last_bullish_ready = bullish_ready
        if reasons:
            self.runtime_context.request_urgent_wakeup("|".join(reasons))

    def _collect_near_miss_sample(self, signal: TrendSignal) -> None:
        if signal.long_setup_reason != "obv_strength_not_confirmed":
            return
        if not self._is_execution_near_ready(signal):
            return
        threshold = self._effective_signal_obv_threshold(signal)
        candidate_state, candidate_reason = self._derive_candidate_state(signal)
        sample = CTANearMissSample(
            symbol=self.symbol,
            captured_at=float(self._time_provider()),
            candidate_state=candidate_state,
            candidate_reason=candidate_reason,
            execution_trigger_family=str(signal.execution_trigger_family or "waiting"),
            execution_trigger_group=str(signal.execution_trigger_group or "waiting"),
            execution_trigger_reason=str(signal.execution_trigger_reason),
            execution_memory_active=bool(signal.execution_memory_active),
            execution_memory_bars_ago=signal.execution_memory_bars_ago,
            execution_breakout=bool(signal.execution_breakout),
            execution_golden_cross=bool(signal.execution_golden_cross),
            obv_zscore=float(signal.obv_confirmation.zscore),
            obv_threshold=threshold,
            obv_gap=float(threshold - float(signal.obv_confirmation.zscore)),
            price=float(signal.price),
        )
        if self._near_miss_window_started_at is None:
            self._near_miss_window_started_at = sample.captured_at
        self._near_miss_samples.append(sample)
        max_samples = max(1, int(getattr(self.config, "near_miss_report_max_samples", 5) or 5))
        self._near_miss_samples = sorted(
            self._near_miss_samples,
            key=lambda item: (item.obv_gap, -item.obv_zscore, -item.captured_at),
        )[: max_samples * 3]

    def _maybe_flush_near_miss_report(self) -> None:
        interval = float(getattr(self.config, "near_miss_report_interval_seconds", 3600.0) or 0.0)
        if interval <= 0 or not self._near_miss_samples:
            return
        now = float(self._time_provider())
        window_started_at = self._near_miss_window_started_at
        if window_started_at is None or now - float(window_started_at) < interval:
            return
        samples = self._consume_near_miss_samples()
        if not samples:
            return
        min_samples = max(1, int(getattr(self.config, "near_miss_report_min_samples", 2) or 2))
        if len(samples) < min_samples:
            return
        self._last_near_miss_report_at = now
        if self.notifier is not None and hasattr(self.notifier, "notify_cta_near_miss_report"):
            self.notifier.notify_cta_near_miss_report(symbol=self.symbol, samples=samples, window_seconds=interval)

    def _consume_near_miss_samples(self) -> list[CTANearMissSample]:
        if not self._near_miss_samples:
            return []
        max_samples = max(1, int(getattr(self.config, "near_miss_report_max_samples", 5) or 5))
        samples = sorted(
            self._near_miss_samples,
            key=lambda item: (item.obv_gap, -item.obv_zscore, -item.captured_at),
        )[:max_samples]
        self._near_miss_samples = []
        self._near_miss_window_started_at = None
        return samples

    def _expected_reward_risk_ratio(self, signal: TrendSignal, *, reference_price: float, stop_distance: float) -> float | None:
        target_price = self._resolve_expected_reward_target_price(signal, reference_price=reference_price)
        if target_price is None or reference_price <= 0 or stop_distance <= 0:
            return None
        if signal.direction > 0:
            expected_reward = max(0.0, float(target_price) - float(reference_price))
        else:
            expected_reward = max(0.0, float(reference_price) - float(target_price))
        if expected_reward <= 0:
            return 0.0
        return float(expected_reward / stop_distance)

    def _resolve_expected_reward_target_price(self, signal: TrendSignal, *, reference_price: float) -> float | None:
        volume_profile = signal.volume_profile
        if volume_profile is None or reference_price <= 0:
            return None

        entry_mode = str(signal.execution_entry_mode or "").lower()
        breakout_like_entry = entry_mode == "breakout_confirmed"
        atr_extension = self._normalized_atr(reference_price, signal.atr) * float(
            getattr(self.config, "breakout_rr_target_atr_multiplier", 3.0)
        )

        if signal.direction > 0:
            base_target = float(volume_profile.high_price)
            if breakout_like_entry:
                return max(base_target, float(reference_price) + atr_extension)
            return base_target

        base_target = float(volume_profile.low_price)
        if breakout_like_entry:
            return min(base_target, float(reference_price) - atr_extension)
        return base_target

    def _resolve_minimum_expected_rr(self, signal: TrendSignal) -> float:
        minimum_expected_rr = float(getattr(self.config, "minimum_expected_rr", 0.0))
        entry_mode = str(signal.execution_entry_mode or "").lower()
        if signal.relaxed_entry:
            minimum_expected_rr = max(minimum_expected_rr, float(getattr(self.config, "relaxed_entry_minimum_expected_rr", minimum_expected_rr)))
        if signal.relaxed_entry and signal.direction < 0:
            minimum_expected_rr = max(minimum_expected_rr, float(getattr(self.config, "relaxed_short_minimum_expected_rr", minimum_expected_rr)))
        if signal.quick_trade_mode:
            minimum_expected_rr = max(minimum_expected_rr, float(getattr(self.config, "quick_trade_minimum_expected_rr", minimum_expected_rr)))
        if any(marker in entry_mode for marker in ("starter", "frontrun", "scale_in", "early_")):
            minimum_expected_rr = max(minimum_expected_rr, float(getattr(self.config, "starter_entry_minimum_expected_rr", minimum_expected_rr)))
        return minimum_expected_rr

    def _starter_entry_passes_quality_gate(self, signal: TrendSignal) -> tuple[bool, str | None]:
        entry_mode = str(signal.execution_entry_mode or "").lower()
        starter_like = any(marker in entry_mode for marker in ("starter", "frontrun", "scale_in", "early_"))
        resonance_allowance, resonance_tag = self._resonance_execution_allowance(signal)
        if not starter_like:
            return True, None

        primary_score = float(signal.bullish_score if signal.direction > 0 else signal.bearish_score)
        opposing_score = float(signal.bearish_score if signal.direction > 0 else signal.bullish_score)
        minimum_score = float(getattr(self.config, "starter_quality_minimum_score", 72.0))
        if "scale_in" in entry_mode:
            minimum_score = float(getattr(self.config, "scale_in_quality_minimum_score", minimum_score))
        if resonance_allowance:
            score_relaxation = max(0.0, float(getattr(self.config, "sweep_resonance_quality_relaxation", 4.0)))
            minimum_score = max(68.0, minimum_score - min(6.0, score_relaxation))
        if primary_score < minimum_score:
            if resonance_allowance:
                return False, f"starter_entry_low_score[{resonance_tag}]"
            return False, "starter_entry_low_score"

        if signal.direction < 0 and int(signal.major_direction) > 0:
            max_gap = float(getattr(self.config, "starter_countertrend_max_score_gap", 10.0))
            if (opposing_score - primary_score) > max_gap:
                return False, "starter_entry_countertrend"
        if signal.direction > 0 and int(signal.major_direction) < 0:
            max_gap = float(getattr(self.config, "starter_countertrend_max_score_gap", 10.0))
            if (opposing_score - primary_score) > max_gap:
                return False, "starter_entry_countertrend"
        return True, None

    def _score_entry_location(self, signal: TrendSignal) -> tuple[float, tuple[str, ...]]:
        score = 0.0
        reasons: list[str] = []
        volume_profile = signal.volume_profile
        price = float(getattr(signal, "price", 0.0) or 0.0)
        atr = max(float(getattr(signal, "atr", 0.0) or 0.0), price * 0.001, 1e-9)
        breakout_confirmed = bool(signal.execution_breakout) if signal.direction > 0 else bool(signal.execution_breakdown)
        near_breakout = bool(breakout_confirmed or signal.execution_frontrun_near_breakout)

        if near_breakout:
            score += 0.45
            reasons.append("near_breakout")
        else:
            score -= 0.65
            reasons.append("far_from_breakout")

        if volume_profile is None:
            reasons.append("no_volume_profile")
        else:
            inside_value_area = bool(volume_profile.contains_price(price))
            if signal.direction > 0:
                poc_gap_atr = (float(volume_profile.poc_price) - price) / atr
                if bool(volume_profile.above_poc(price)):
                    score += 0.35
                    reasons.append("above_poc")
                elif poc_gap_atr <= 0.35:
                    score += 0.10
                    reasons.append("near_poc_reclaim")
                else:
                    score -= min(0.45, 0.18 + max(0.0, poc_gap_atr - 0.35) * 0.12)
                    reasons.append("below_poc")
            else:
                if inside_value_area:
                    score -= 0.20
                    reasons.append("short_inside_value_area")
                else:
                    score += 0.10
                    reasons.append("short_outside_value_area")

        obv_z = float(getattr(signal.obv_confirmation, "zscore", 0.0) or 0.0)
        obv_threshold = float(self._effective_signal_obv_threshold(signal))
        if signal.direction > 0:
            if obv_z >= obv_threshold:
                score += 0.15
                reasons.append("obv_supportive")
            elif obv_z < max(-0.5, obv_threshold - 1.0):
                score -= 0.15
                reasons.append("obv_weak")
        else:
            if obv_z <= -abs(obv_threshold):
                score += 0.15
                reasons.append("obv_supportive")
            elif obv_z > abs(obv_threshold):
                score -= 0.15
                reasons.append("obv_weak")

        if signal.relaxed_entry:
            score -= 0.10
            reasons.append("relaxed_entry")
        if signal.entry_pathway is EntryPathway.FAST_TRACK:
            score += 0.10
            reasons.append("fast_track")
        elif signal.entry_pathway is EntryPathway.STANDARD:
            score += 0.05
            reasons.append("standard_path")

        return max(-1.0, min(1.0, score)), tuple(reasons)

    def _resolve_entry_location_block_reason(self, signal: TrendSignal) -> str | None:
        entry_mode = str(signal.execution_entry_mode or "").lower()
        structure_confirmed = bool(signal.execution_breakout) if signal.direction > 0 else bool(signal.execution_breakdown)
        near_breakout = bool(structure_confirmed or signal.execution_frontrun_near_breakout)
        resonance_allowance, _resonance_tag = self._resonance_execution_allowance(signal)
        if signal.relaxed_entry and bool(getattr(self.config, "relaxed_entry_require_near_breakout", True)) and not near_breakout:
            return "relaxed_entry_chasing"
        if any(marker in entry_mode for marker in ("starter", "frontrun", "scale_in", "early_")) and bool(getattr(self.config, "starter_entry_require_near_breakout", True)) and not near_breakout:
            return "starter_entry_chasing"
        worst_location_floor = float(getattr(self.config, "entry_location_score_min", -0.60))
        location_score = float(getattr(signal, "entry_location_score", 0.0) or 0.0)
        if location_score <= worst_location_floor:
            if resonance_allowance:
                relaxed_floor = min(-0.35, worst_location_floor - max(0.0, float(getattr(self.config, "sweep_resonance_location_relaxation", 0.12))))
                if near_breakout and location_score > relaxed_floor:
                    return None
            return "entry_location_score_too_low"
        return None

    def _fast_track_hard_block_reason(self, signal: TrendSignal) -> str | None:
        reverse_obv_threshold = max(0.5, abs(float(self._effective_signal_obv_threshold(signal))))
        obv_zscore = float(signal.obv_confirmation.zscore)
        if signal.direction > 0:
            if bool(signal.obv_confirmation.above_sma) is False and obv_zscore <= -reverse_obv_threshold:
                return "fast_track_reverse_obv"
        elif signal.direction < 0:
            if bool(signal.obv_confirmation.above_sma) and obv_zscore >= reverse_obv_threshold:
                return "fast_track_reverse_obv"
        return None

    def _resolve_minimum_expected_rr_for_pathway(self, signal: TrendSignal) -> float:
        minimum_expected_rr = self._resolve_minimum_expected_rr(signal)
        if bool(getattr(signal, "resonance_allowed", False)) and not bool(getattr(signal, "reverse_intercepted", False)):
            relaxation_ratio = min(0.25, max(0.0, float(getattr(self.config, "sweep_resonance_rr_relaxation_ratio", 0.15))))
            minimum_expected_rr *= max(0.0, 1.0 - relaxation_ratio)
        if signal.entry_pathway is EntryPathway.STANDARD:
            standard_floor = float(getattr(self.config, "standard_entry_minimum_expected_rr", minimum_expected_rr))
            if bool(getattr(signal, "resonance_allowed", False)):
                standard_floor *= max(0.0, 1.0 - min(0.25, max(0.0, float(getattr(self.config, "sweep_resonance_rr_relaxation_ratio", 0.15)))))
            return max(minimum_expected_rr, standard_floor)
        if signal.entry_pathway is not EntryPathway.FAST_TRACK:
            return minimum_expected_rr
        if signal.relaxed_entry:
            return minimum_expected_rr
        entry_mode = str(signal.execution_entry_mode or "").lower()
        if not any(marker in entry_mode for marker in ("starter", "frontrun", "scale_in", "early_")):
            return minimum_expected_rr
        starter_floor = float(getattr(self.config, "starter_entry_minimum_expected_rr", minimum_expected_rr))
        return min(minimum_expected_rr, starter_floor)

    def _pathway_skips_strict_order_flow(self, signal: TrendSignal) -> bool:
        if signal.entry_pathway is not EntryPathway.FAST_TRACK:
            return False
        entry_mode = str(signal.execution_entry_mode or "").lower()
        return any(marker in entry_mode for marker in ("starter", "frontrun", "scale_in", "early_"))

    def _apply_fast_track_checks(self, signal: TrendSignal) -> str | None:
        hard_block_reason = self._fast_track_hard_block_reason(signal)
        if hard_block_reason is not None:
            logger.info(
                "CTA fast-track blocked | symbol=%s side=%s reason=%s obv_above_sma=%s obv_z=%.2f threshold=%.2f",
                self.symbol,
                "buy" if signal.direction > 0 else "sell",
                hard_block_reason,
                bool(signal.obv_confirmation.above_sma),
                float(signal.obv_confirmation.zscore),
                float(self._effective_signal_obv_threshold(signal)),
            )
            return "cta:fast_track_blocked"
        return None

    def _resolve_trigger_family_gate_reason(self, signal: TrendSignal) -> str | None:
        trigger_family = str(signal.execution_trigger_family or "waiting")
        pathway = signal.entry_pathway
        confidence = float(getattr(signal, "signal_confidence", 0.0) or 0.0)
        relaxed_reasons = {str(reason or "").upper() for reason in getattr(signal, "relaxed_reasons", ())}
        quality_tier = str(getattr(signal, "signal_quality_tier", "TIER_LOW") or "TIER_LOW")
        execution_entry_mode = str(getattr(signal, "execution_entry_mode", "") or "")

        if bool(getattr(self.config, "disable_weak_scale_in_entries", True)) and execution_entry_mode in {
            "weak_bull_scale_in_limit",
            "weak_bear_scale_in_limit",
        }:
            return f"{execution_entry_mode}_disabled"

        if signal.direction > 0:
            disabled_long_families = {
                "near_breakout_release": bool(getattr(self.config, "disable_near_breakout_release_long", False)),
                "price_led_override": bool(getattr(self.config, "disable_price_led_override_long", True)),
                "trend_continuation_near_breakout": bool(getattr(self.config, "disable_trend_continuation_long", False)),
                "bullish_memory_breakout": bool(getattr(self.config, "disable_bullish_memory_breakout_long", False)),
            }
            if disabled_long_families.get(trigger_family, False):
                return f"{trigger_family}_disabled"

        if signal.direction < 0:
            disabled_short_families = {
                "bearish_retest": bool(getattr(self.config, "disable_bearish_retest_short", True)),
            }
            if disabled_short_families.get(trigger_family, False):
                return f"{trigger_family}_disabled"

        if trigger_family == "trend_continuation_near_breakout":
            minimum_pathway = str(getattr(self.config, "trend_continuation_minimum_entry_pathway", "FAST_TRACK") or "FAST_TRACK").upper()
            try:
                pathway_rank = {EntryPathway.STRICT: 0, EntryPathway.STANDARD: 1, EntryPathway.FAST_TRACK: 2}[pathway]
                minimum_rank = {"STRICT": 0, "STANDARD": 1, "FAST_TRACK": 2}[minimum_pathway]
            except KeyError:
                pathway_rank = {EntryPathway.STRICT: 0, EntryPathway.STANDARD: 1, EntryPathway.FAST_TRACK: 2}[pathway]
                minimum_rank = 2
            if pathway_rank < minimum_rank:
                return "trend_continuation_pathway_too_weak"
            if quality_tier != "TIER_HIGH":
                return "trend_continuation_requires_high_quality"

        if trigger_family == "bullish_memory_breakout" and pathway is EntryPathway.STANDARD:
            minimum_confidence = float(getattr(self.config, "bullish_memory_breakout_standard_min_confidence", 0.70))
            if confidence < minimum_confidence:
                return "bullish_memory_breakout_low_confidence"
            if signal.relaxed_entry and "STANDARD_VA_BYPASS" in relaxed_reasons:
                minimum_confidence = float(getattr(self.config, "bullish_memory_breakout_standard_va_bypass_min_confidence", 0.74))
                if confidence < minimum_confidence:
                    return "bullish_memory_breakout_va_bypass_low_confidence"
            if signal.relaxed_entry and any(reason.startswith("STANDARD_POC_RECLAIM_OK") for reason in relaxed_reasons):
                minimum_confidence = float(getattr(self.config, "bullish_memory_breakout_poc_reclaim_min_confidence", 0.75))
                if confidence < minimum_confidence:
                    return "bullish_memory_breakout_poc_reclaim_low_confidence"
            if signal.relaxed_entry and any(reason.startswith("OBV(") for reason in relaxed_reasons):
                minimum_confidence = float(getattr(self.config, "bullish_memory_breakout_obv_floor_min_confidence", 0.70))
                if confidence < minimum_confidence:
                    return "bullish_memory_breakout_obv_floor_low_confidence"

        if trigger_family == "near_breakout_release" and pathway is EntryPathway.STANDARD and signal.relaxed_entry:
            minimum_confidence = float(getattr(self.config, "near_breakout_release_standard_min_confidence", 0.70))
            if confidence < minimum_confidence:
                return "near_breakout_release_low_confidence"
            if "STANDARD_VA_BYPASS" in relaxed_reasons:
                return "near_breakout_release_value_area_bypass"
            if any(reason.startswith("STANDARD_POC_RECLAIM_OK") for reason in relaxed_reasons):
                return "near_breakout_release_poc_bypass"
            if any(reason.startswith("OBV(") for reason in relaxed_reasons):
                return "near_breakout_release_obv_relaxed"
        return None

    def _apply_standard_checks(self, signal: TrendSignal) -> str | None:
        trigger_family_gate_reason = self._resolve_trigger_family_gate_reason(signal)
        if trigger_family_gate_reason is not None:
            logger.info(
                "CTA trigger family blocked | symbol=%s side=%s trigger_family=%s mode=%s pathway=%s confidence=%.2f reason=%s",
                self.symbol,
                "buy" if signal.direction > 0 else "sell",
                signal.execution_trigger_family,
                signal.execution_entry_mode,
                signal.entry_pathway.name,
                float(signal.signal_confidence),
                trigger_family_gate_reason,
            )
            return "cta:entry_quality_blocked"

        entry_location_block_reason = self._resolve_entry_location_block_reason(signal)
        if entry_location_block_reason is not None:
            logger.info(
                "CTA entry location blocked | symbol=%s side=%s mode=%s pathway=%s reason=%s score=%.2f loc_reasons=%s breakout=%s breakdown=%s near_breakout=%s",
                self.symbol,
                "buy" if signal.direction > 0 else "sell",
                signal.execution_entry_mode,
                signal.entry_pathway.name,
                entry_location_block_reason,
                float(signal.entry_location_score),
                ",".join(signal.entry_location_reasons),
                signal.execution_breakout,
                signal.execution_breakdown,
                signal.execution_frontrun_near_breakout,
            )
            return "cta:entry_location_blocked"

        starter_quality_passed, starter_quality_reason = self._starter_entry_passes_quality_gate(signal)
        if not starter_quality_passed:
            logger.info(
                "CTA starter quality blocked | symbol=%s side=%s mode=%s pathway=%s reason=%s bullish_score=%.1f bearish_score=%.1f major_direction=%s",
                self.symbol,
                "buy" if signal.direction > 0 else "sell",
                signal.execution_entry_mode,
                signal.entry_pathway.name,
                starter_quality_reason,
                float(signal.bullish_score),
                float(signal.bearish_score),
                int(signal.major_direction),
            )
            return "cta:entry_quality_blocked"
        return None

    def _assess_order_flow(self, *, side: str, amount: float) -> OrderFlowAssessment | None:
        if side != "buy" or not self.config.order_flow_enabled:
            return None
        return self.order_flow_sentinel.assess_entry(self.symbol, side, amount)

    def _apply_standard_order_flow_checks(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
        order_flow_assessment: OrderFlowAssessment | None,
    ) -> str | None:
        if order_flow_assessment is None:
            return None
        if order_flow_assessment.entry_allowed:
            return None
        if order_flow_assessment.reason == "empty_order_book":
            logger.info(
                "CTA order flow blocked | symbol=%s side=%s mode=%s pathway=%s trigger=%s amount=%.8f diagnostics=%s",
                self.symbol,
                side,
                signal.execution_entry_mode,
                signal.entry_pathway.name,
                signal.execution_trigger_reason,
                amount,
                order_flow_assessment.diagnostics(),
            )
            return "cta:order_flow_blocked"
        logger.info(
            "CTA standard order flow soft warning | symbol=%s side=%s mode=%s pathway=%s trigger=%s amount=%.8f diagnostics=%s",
            self.symbol,
            side,
            signal.execution_entry_mode,
            signal.entry_pathway.name,
            signal.execution_trigger_reason,
            amount,
            order_flow_assessment.diagnostics(),
        )
        return None

    def _apply_strict_order_flow_checks(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
        order_flow_assessment: OrderFlowAssessment | None,
    ) -> str | None:
        if order_flow_assessment is None:
            return None
        resonance_allowance, _resonance_tag = self._resonance_execution_allowance(signal)
        if not order_flow_assessment.entry_allowed:
            if order_flow_assessment.reason == "empty_order_book":
                logger.info(
                    "CTA order flow blocked | symbol=%s side=%s mode=%s pathway=%s trigger=%s amount=%.8f diagnostics=%s",
                    self.symbol,
                    side,
                    signal.execution_entry_mode,
                    signal.entry_pathway.name,
                    signal.execution_trigger_reason,
                    amount,
                    order_flow_assessment.diagnostics(),
                )
                return "cta:order_flow_blocked"
            if signal.execution_entry_mode not in {"weak_bull_scale_in_limit", "early_bullish_starter_limit", "starter_frontrun_limit", "starter_short_frontrun_limit"}:
                base_confirmation_ratio = max(0.0, float(self.config.order_flow_confirmation_ratio))
                strict_order_flow_families = {
                    "near_breakout_release",
                    "price_led_override",
                    "major_bull_retest",
                    "bullish_memory_breakout",
                }
                trigger_family = str(getattr(signal, "execution_trigger_family", "") or "")
                if trigger_family in strict_order_flow_families or order_flow_assessment.imbalance_ratio < base_confirmation_ratio:
                    logger.info(
                        "CTA order flow blocked | symbol=%s side=%s mode=%s pathway=%s trigger=%s amount=%.8f diagnostics=%s",
                        self.symbol,
                        side,
                        signal.execution_entry_mode,
                        signal.entry_pathway.name,
                        signal.execution_trigger_reason,
                        amount,
                        order_flow_assessment.diagnostics(),
                    )
                    return "cta:order_flow_blocked"
                if resonance_allowance:
                    relaxed_of_floor = max(0.35, float(getattr(self.config, "sweep_resonance_of_relaxation_floor", 0.40)))
                    if order_flow_assessment.imbalance_ratio >= relaxed_of_floor:
                        logger.info(
                            "CTA order flow resonance bypass | symbol=%s side=%s mode=%s pathway=%s trigger=%s imbalance=%.3f floor=%.3f",
                            self.symbol,
                            side,
                            signal.execution_entry_mode,
                            signal.entry_pathway.name,
                            signal.execution_trigger_reason,
                            order_flow_assessment.imbalance_ratio,
                            relaxed_of_floor,
                        )
                        return None
            logger.info(
                "CTA order flow soft warning | symbol=%s side=%s mode=%s pathway=%s trigger=%s amount=%.8f diagnostics=%s",
                self.symbol,
                side,
                signal.execution_entry_mode,
                signal.entry_pathway.name,
                signal.execution_trigger_reason,
                amount,
                order_flow_assessment.diagnostics(),
            )
        return None

    def _apply_entry_pathway_checks(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
        order_flow_assessment: OrderFlowAssessment | None,
    ) -> str | None:
        reverse_intercept_reason = self._resolve_reverse_intercept_reason(signal)
        if reverse_intercept_reason is not None:
            signal.reverse_intercepted = True
            signal.reverse_intercept_reason = reverse_intercept_reason
            logger.info(
                "CTA reverse intercept blocked | symbol=%s side=%s pathway=%s reason=%s trigger_family=%s oi_change_pct=%.2f funding=%.5f",
                self.symbol,
                side,
                signal.entry_pathway.name,
                reverse_intercept_reason,
                signal.execution_trigger_family,
                float(getattr(signal, "oi_change_pct", 0.0)),
                float(getattr(signal, "funding_rate", 0.0)),
            )
            return "cta:reverse_intercept_blocked"
        if signal.entry_pathway is EntryPathway.FAST_TRACK:
            fast_track_result = self._apply_fast_track_checks(signal)
            if fast_track_result is not None:
                return fast_track_result
            if not self._pathway_skips_strict_order_flow(signal):
                strict_result = self._apply_strict_order_flow_checks(
                    signal=signal,
                    side=side,
                    amount=amount,
                    order_flow_assessment=order_flow_assessment,
                )
                if strict_result is not None:
                    return strict_result
            return self._apply_standard_checks(signal)

        if signal.entry_pathway is EntryPathway.STANDARD:
            standard_order_flow_result = self._apply_standard_order_flow_checks(
                signal=signal,
                side=side,
                amount=amount,
                order_flow_assessment=order_flow_assessment,
            )
            if standard_order_flow_result is not None:
                return standard_order_flow_result
            return self._apply_standard_checks(signal)

        strict_result = self._apply_strict_order_flow_checks(
            signal=signal,
            side=side,
            amount=amount,
            order_flow_assessment=order_flow_assessment,
        )
        if strict_result is not None:
            return strict_result
        return self._apply_standard_checks(signal)

    def _check_entry_reward_risk(
        self,
        *,
        signal: TrendSignal,
        side: str,
        reference_price: float,
    ) -> str | None:
        pre_entry_atr = self._normalized_atr(reference_price, signal.atr)
        pre_entry_stop_distance = pre_entry_atr * self._resolve_dynamic_stop_loss_multiplier(signal)
        expected_rr = self._expected_reward_risk_ratio(signal, reference_price=reference_price, stop_distance=pre_entry_stop_distance)
        minimum_expected_rr = self._resolve_minimum_expected_rr_for_pathway(signal)
        if expected_rr is not None and expected_rr < minimum_expected_rr:
            logger.info(
                "CTA reward/risk blocked | symbol=%s side=%s pathway=%s expected_rr=%.2f threshold=%.2f price=%.2f stop_distance=%.2f",
                self.symbol,
                side,
                signal.entry_pathway.name,
                expected_rr,
                minimum_expected_rr,
                reference_price,
                pre_entry_stop_distance,
            )
            return "cta:reward_risk_blocked"
        return None

    def _resolve_entry_amount(self, *, signal: TrendSignal, side: str) -> tuple[str | None, float, bool]:
        amount = self._calculate_entry_amount(signal.price)
        sentiment_halved = False

        amount *= max(0.0, min(1.0, float(signal.entry_size_multiplier)))
        if signal.quick_trade_mode:
            amount *= max(0.0, min(1.0, float(getattr(self.config, 'obv_scalp_entry_fraction', 0.35))))
        amount = self._normalize_order_amount(amount)
        if amount <= 0:
            return "cta:risk_blocked", 0.0, False

        if side == "buy" and self.sentiment_analyst is not None:
            sentiment_decision = self.sentiment_analyst.evaluate_cta_buy(self.symbol)
            if sentiment_decision.blocked:
                return "cta:sentiment_blocked", 0.0, False
            if sentiment_decision.size_multiplier < 1.0:
                amount = self._normalize_order_amount(amount * sentiment_decision.size_multiplier)
                sentiment_halved = amount > 0
                if amount <= 0:
                    return "cta:sentiment_blocked", 0.0, False
        return None, amount, sentiment_halved

    def _check_entry_risk_budget(
        self,
        *,
        signal: TrendSignal,
        amount: float,
        notional_price: float,
    ) -> str | None:
        position_side = "long" if signal.direction > 0 else "short"
        if self.risk_manager is None:
            return None
        requested_notional = self.client.estimate_notional(self.symbol, amount, notional_price)
        allowed, _reason = self.risk_manager.can_open_new_position(
            self.symbol,
            requested_notional,
            strategy_name=self.strategy_name,
            opening_side=position_side,
        )
        if not allowed:
            return "cta:risk_blocked"
        return None

    def _log_trade_open_context(self, *, signal: TrendSignal, side: str) -> None:
        if signal.execution_memory_active and (signal.execution_breakout or signal.weak_bull_bias):
            logger.info(
                "CTA entry trigger | symbol=%s side=%s %s",
                self.symbol,
                side,
                signal.execution_trigger_reason,
            )
        entry_type = "Relaxed_Entry" if signal.relaxed_entry else "Standard_Entry"
        entry_reason = (
            f"Score({float(signal.bullish_score):.0f}) > Threshold({float(getattr(self.config, 'drive_first_tradeable_score', 60.0)):.0f})"
            if signal.relaxed_entry
            else str(signal.execution_trigger_reason)
        )
        if signal.relaxed_entry and signal.relaxed_reasons:
            entry_reason = f"{entry_reason} | Relaxations: {', '.join(signal.relaxed_reasons)}"
        logger.info("[TRADE_OPEN] Type: %s | Pathway: %s | Reason: %s", entry_type, signal.entry_pathway.name, entry_reason)

    def _finalize_entry_fill(
        self,
        *,
        entry_order: EntryOrderResult,
        requested_amount: float,
    ) -> tuple[str | None, float, float]:
        filled_amount = self._normalize_order_amount(entry_order.filled_amount)
        fill_ratio = (filled_amount / requested_amount) if requested_amount > 0 else 0.0
        if filled_amount <= 0:
            return ("cta:low_fill_ratio" if entry_order.used_limit_order else "cta:risk_blocked"), 0.0, 0.0
        if entry_order.used_limit_order and fill_ratio < 0.5:
            return "cta:low_fill_ratio", filled_amount, fill_ratio
        return None, filled_amount, fill_ratio

    def _build_managed_position(
        self,
        *,
        signal: TrendSignal,
        position_side: str,
        filled_amount: float,
        entry_price: float,
    ) -> ManagedPosition:
        atr_value = self._normalized_atr(entry_price, signal.atr)
        stop_loss_multiplier = self._resolve_dynamic_stop_loss_multiplier(signal)
        stop_distance = atr_value * stop_loss_multiplier
        if signal.direction > 0:
            stop_price = entry_price - stop_distance
        else:
            stop_price = entry_price + stop_distance
        sweep_stop_price, sweep_stop_reason = self._resolve_sweep_stop_anchor(signal, entry_price, stop_distance)
        if sweep_stop_price is not None:
            stop_price = float(sweep_stop_price)
            stop_distance = abs(float(entry_price) - float(stop_price))
            logger.info(
                "CTA sweep stop anchored | symbol=%s side=%s reason=%s entry=%.4f stop=%.4f stop_distance=%.4f",
                self.symbol,
                position_side,
                sweep_stop_reason,
                float(entry_price),
                float(stop_price),
                float(stop_distance),
            )
        return ManagedPosition(
            side=position_side,
            entry_price=entry_price,
            initial_size=filled_amount,
            remaining_size=filled_amount,
            stop_price=stop_price,
            best_price=entry_price,
            atr_value=atr_value,
            stop_distance=stop_distance,
            risk_percent=signal.risk_percent,
            quick_trade_mode=signal.quick_trade_mode,
            origin_trigger_family=str(signal.execution_trigger_family or "waiting"),
            origin_trigger_reason=str(signal.execution_trigger_reason or ""),
            origin_pathway=signal.entry_pathway.name,
        )

    def _resolve_filled_entry_price(
        self,
        *,
        entry_order: EntryOrderResult,
        order_flow_assessment: OrderFlowAssessment | None,
        fallback_price: float,
    ) -> float:
        entry_price = (
            float(entry_order.average_price)
            if entry_order.average_price not in (None, 0, "0")
            else self._extract_order_price(
                entry_order.order,
                fallback=(
                    order_flow_assessment.expected_average_price
                    if order_flow_assessment is not None and order_flow_assessment.expected_average_price is not None
                    else fallback_price
                ),
            )
        )
        return float(entry_price)

    def _finalize_open_action(
        self,
        *,
        signal: TrendSignal,
        side: str,
        position_side: str,
        filled_amount: float,
        entry_price: float,
        entry_order: EntryOrderResult,
        sentiment_halved: bool,
    ) -> str:
        self._signal_flip_pending = False
        self._publish_risk_profile(signal)
        if self.notifier is not None and hasattr(self.notifier, "notify_trade"):
            trade_notional = self.client.estimate_notional(self.symbol, filled_amount, entry_price)
            self.notifier.notify_trade(
                side=side,
                price=entry_price,
                size=filled_amount,
                strategy=self.strategy_name,
                signal=f"cta_open_{position_side}",
                symbol=self.symbol,
                notional=trade_notional,
            )
        action = f"cta:open_{position_side}"
        if signal.quick_trade_mode:
            action += "_obv_scalp"
        if entry_order.used_limit_order:
            action += "_limit"
        if sentiment_halved:
            action += "_sentiment_halved"
        return action

    def _prepare_entry_execution_context(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
    ) -> tuple[str | None, str, float, OrderFlowAssessment | None]:
        position_side = "long" if signal.direction > 0 else "short"
        cooldown_remaining = self._cooldown_remaining_seconds(position_side)
        if cooldown_remaining > 0:
            logger.info(
                "CTA same-direction cooldown blocked | symbol=%s side=%s remaining=%ss",
                self.symbol,
                position_side,
                cooldown_remaining,
            )
            return "cta:same_direction_cooldown", position_side, 0.0, None

        fast_track_reuse_remaining = self._fast_track_reuse_remaining_seconds(signal)
        if fast_track_reuse_remaining > 0:
            logger.info(
                "CTA fast-track reuse cooldown blocked | symbol=%s side=%s remaining=%ss trigger=%s",
                self.symbol,
                position_side,
                fast_track_reuse_remaining,
                signal.execution_trigger_reason,
            )
            return "cta:fast_track_reuse_cooldown", position_side, 0.0, None

        repeated_entry_remaining = self._repeated_entry_zone_remaining_seconds(signal)
        if repeated_entry_remaining > 0:
            logger.info(
                "CTA repeated-entry zone cooldown blocked | symbol=%s side=%s remaining=%ss trigger_family=%s price=%.4f anchor_price=%.4f",
                self.symbol,
                position_side,
                repeated_entry_remaining,
                signal.execution_trigger_family,
                float(signal.price),
                float(self._entry_zone_anchor_price.get(position_side) or 0.0),
            )
            return "cta:repeated_entry_zone_cooldown", position_side, 0.0, None

        order_flow_assessment: OrderFlowAssessment | None = self._assess_order_flow(side=side, amount=amount)
        pathway_result = self._apply_entry_pathway_checks(
            signal=signal,
            side=side,
            amount=amount,
            order_flow_assessment=order_flow_assessment,
        )
        if pathway_result is not None:
            return pathway_result, "", 0.0, order_flow_assessment

        notional_price = signal.price
        if order_flow_assessment is not None and order_flow_assessment.reference_price is not None:
            notional_price = max(notional_price, order_flow_assessment.reference_price)
        risk_budget_result = self._check_entry_risk_budget(
            signal=signal,
            amount=amount,
            notional_price=notional_price,
        )
        if risk_budget_result is not None:
            return risk_budget_result, position_side, notional_price, order_flow_assessment

        reward_risk_result = self._check_entry_reward_risk(
            signal=signal,
            side=side,
            reference_price=notional_price,
        )
        if reward_risk_result is not None:
            return reward_risk_result, position_side, notional_price, order_flow_assessment

        self._log_trade_open_context(signal=signal, side=side)
        return None, position_side, notional_price, order_flow_assessment

    def _resolve_final_entry_permit(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
    ) -> FinalEntryPermit:
        action, position_side, notional_price, order_flow_assessment = self._prepare_entry_execution_context(
            signal=signal,
            side=side,
            amount=amount,
        )
        if action is not None:
            stage = "final"
            reason = str(action).replace("cta:", "")
            if action == "cta:same_direction_cooldown":
                stage = "cooldown"
            elif action == "cta:fast_track_reuse_cooldown":
                stage = "cooldown"
            elif action == "cta:repeated_entry_zone_cooldown":
                stage = "cooldown"
            elif action == "cta:risk_blocked":
                stage = "risk_budget"
                reason = "risk_blocked"
            elif action == "cta:reward_risk_blocked":
                stage = "reward_risk"
                reason = "reward_risk_blocked"
            return FinalEntryPermit(
                allowed=False,
                status="blocked",
                action=action,
                stage=stage,
                reason=reason,
                position_side=position_side,
                notional_price=float(notional_price),
                order_flow_assessment=order_flow_assessment,
            )

        limit_price_protected = bool(
            order_flow_assessment is not None
            and getattr(order_flow_assessment, "recommended_limit_price", None) not in (None, 0, "0")
        )
        if order_flow_assessment is not None:
            try:
                setattr(order_flow_assessment, "final_permit", SimpleNamespace(limit_price_protected=limit_price_protected))
            except Exception:
                pass
        return FinalEntryPermit(
            allowed=True,
            status="limit_protect" if limit_price_protected else "allowed",
            action=None,
            stage="final",
            reason="limit_protect" if limit_price_protected else "allowed",
            position_side=position_side,
            notional_price=float(notional_price),
            order_flow_assessment=order_flow_assessment,
        )

    def _execute_entry_and_build_position(
        self,
        *,
        signal: TrendSignal,
        side: str,
        amount: float,
        position_side: str,
        notional_price: float,
        order_flow_assessment: OrderFlowAssessment | None,
        sentiment_halved: bool,
    ) -> str:
        entry_order = self._place_entry_order(
            side=side,
            amount=amount,
            order_flow_assessment=order_flow_assessment,
            execution_entry_mode=signal.execution_entry_mode,
            execution_frame=None,
        )
        fill_result, filled_amount, _fill_ratio = self._finalize_entry_fill(
            entry_order=entry_order,
            requested_amount=amount,
        )
        if fill_result is not None:
            return fill_result

        entry_price = self._resolve_filled_entry_price(
            entry_order=entry_order,
            order_flow_assessment=order_flow_assessment,
            fallback_price=notional_price,
        )
        self.position = self._build_managed_position(
            signal=signal,
            position_side=position_side,
            filled_amount=filled_amount,
            entry_price=entry_price,
        )
        self._journal_event(
            event_type="trade_open",
            side=position_side,
            action="cta:open_position",
            trigger_family=str(signal.execution_trigger_family or "waiting"),
            trigger_reason=str(signal.execution_trigger_reason or ""),
            pathway=signal.entry_pathway.name,
            price=float(entry_price),
            size=float(filled_amount),
            metadata={
                "raw_direction": int(signal.raw_direction),
                "execution_entry_mode": str(signal.execution_entry_mode),
                "signal_confidence": float(signal.signal_confidence),
                "signal_quality_tier": str(signal.signal_quality_tier),
                "relaxed_entry": bool(signal.relaxed_entry),
                "relaxed_reasons": list(signal.relaxed_reasons),
                "risk_percent": float(signal.risk_percent),
                "notional_price": float(notional_price),
                "ml_used_model": bool(signal.ml_used_model),
                "ml_prediction": int(signal.ml_prediction),
                "ml_probability_up": float(signal.ml_probability_up),
                "ml_aligned_confidence": float(signal.ml_aligned_confidence),
                "ml_gate_passed": bool(signal.ml_gate_passed),
                "ml_reason": str(signal.ml_reason),
                "entry_location_score": float(signal.entry_location_score),
                "entry_location_reasons": list(signal.entry_location_reasons),
                "liquidity_sweep": bool(signal.liquidity_sweep),
                "liquidity_sweep_side": str(signal.liquidity_sweep_side),
                "oi_change_pct": float(signal.oi_change_pct),
                "funding_rate": float(signal.funding_rate),
                "resonance_allowed": bool(signal.resonance_allowed),
                "resonance_reason": str(signal.resonance_reason),
                "reverse_intercepted": bool(signal.reverse_intercepted),
                "reverse_intercept_reason": str(signal.reverse_intercept_reason),
                "sweep_extreme_price": signal.sweep_extreme_price,
            },
        )
        self._arm_fast_track_reuse_cooldown(signal)
        self._arm_repeated_entry_zone_cooldown(signal, entry_price)
        return self._finalize_open_action(
            signal=signal,
            side=side,
            position_side=position_side,
            filled_amount=filled_amount,
            entry_price=entry_price,
            entry_order=entry_order,
            sentiment_halved=sentiment_halved,
        )

    def _open_position(self, signal: TrendSignal) -> str:
        side = "buy" if signal.direction > 0 else "sell"
        amount_result, amount, sentiment_halved = self._resolve_entry_amount(signal=signal, side=side)
        if amount_result is not None:
            self._publish_risk_profile(None)
            return amount_result

        permit = self._resolve_final_entry_permit(
            signal=signal,
            side=side,
            amount=amount,
        )
        if not permit.allowed:
            self._publish_risk_profile(None)
            return str(permit.action or "cta:risk_blocked")

        execution_result = self._execute_entry_and_build_position(
            signal=signal,
            side=side,
            amount=amount,
            position_side=permit.position_side,
            notional_price=permit.notional_price,
            order_flow_assessment=permit.order_flow_assessment,
            sentiment_halved=sentiment_halved,
        )
        if execution_result in {"cta:low_fill_ratio", "cta:risk_blocked"}:
            self._publish_risk_profile(None)
        return execution_result

    def _calculate_entry_amount(self, reference_price: float) -> float:
        target_margin = max(0.0, float(self.config.margin_fraction_per_trade))
        target_leverage = max(0.0, float(self.config.nominal_leverage))
        if target_margin <= 0 or target_leverage <= 0 or reference_price <= 0:
            return 0.0

        if not hasattr(self.client, "fetch_total_equity"):
            fallback_amount = float(self.execution_config.cta_order_size)
            logger.info(
                "CTA sizing fallback | symbol=%s reason=no_equity_api amount=%.8f price=%.2f",
                self.symbol,
                fallback_amount,
                reference_price,
            )
            return fallback_amount

        try:
            equity = float(self.client.fetch_total_equity("USDT"))
        except Exception:
            logger.exception("CTA sizing failed to fetch account equity; falling back to configured order size")
            fallback_amount = float(self.execution_config.cta_order_size)
            logger.info(
                "CTA sizing fallback | symbol=%s reason=equity_fetch_failed amount=%.8f price=%.2f",
                self.symbol,
                fallback_amount,
                reference_price,
            )
            return fallback_amount

        target_notional = equity * target_margin * target_leverage
        if target_notional <= 0:
            return 0.0

        unit_notional = self.client.estimate_notional(self.symbol, 1.0, reference_price)
        if unit_notional <= 0:
            return 0.0
        amount = target_notional / unit_notional
        logger.info(
            "CTA sizing | symbol=%s equity=%.4f margin_fraction=%.4f leverage=%.2f target_notional=%.4f ref_price=%.2f raw_amount=%.8f",
            self.symbol,
            equity,
            target_margin,
            target_leverage,
            target_notional,
            reference_price,
            amount,
        )
        return amount

    def _place_entry_order(
        self,
        *,
        side: str,
        amount: float,
        order_flow_assessment: OrderFlowAssessment | None,
        execution_entry_mode: str,
        execution_frame=None,
    ) -> EntryOrderResult:
        fallback_price = (
            order_flow_assessment.expected_average_price
            if order_flow_assessment is not None and order_flow_assessment.expected_average_price is not None
            else None
        )
        minimum_amount = 0.0
        if hasattr(self.client, "get_min_order_amount"):
            minimum_amount = float(self.client.get_min_order_amount(self.symbol))

        smart_retest_mode = execution_entry_mode in {"bullish_retest_limit", "bearish_retest_limit"}
        aggressive_limit_price = self._resolve_aggressive_entry_price(
            side=side,
            order_flow_assessment=order_flow_assessment,
            execution_entry_mode=execution_entry_mode,
        )
        if smart_retest_mode:
            aggressive_limit_price = None
        if aggressive_limit_price is not None:
            params = {"timeInForce": "IOC", "executionMode": "aggressive_limit"}
            if execution_entry_mode == "weak_bull_scale_in_limit":
                params["executionMode"] = "weak_bull_scale_in"
            elif execution_entry_mode == "early_bullish_starter_limit":
                params["executionMode"] = "early_bullish_starter"
            elif execution_entry_mode == "starter_frontrun_limit":
                params["executionMode"] = "starter_frontrun"
            elif execution_entry_mode in {"bullish_retest_limit", "bearish_retest_limit"}:
                params["executionMode"] = execution_entry_mode
            elif order_flow_assessment is not None:
                params["orderFlowImbalance"] = round(order_flow_assessment.imbalance_ratio, 4)
            response = self.client.place_limit_order(self.symbol, side, amount, aggressive_limit_price, params=params)
            limit_response = self._refresh_ioc_fill(response)
            limit_filled = self._normalize_order_amount(
                self._extract_filled_amount(limit_response, 0.0, used_limit_order=True)
            )
            limit_price = self._extract_order_price(limit_response, fallback=fallback_price or response.get("price") or 0.0)
            unfilled_amount = self._normalize_order_amount(max(0.0, float(amount) - limit_filled))
            self._log_entry_fill(
                order=limit_response,
                limit_price=response.get("price"),
                fill_price=limit_price,
                fill_qty=limit_filled,
                unfilled_qty=unfilled_amount,
            )
            if limit_filled > 0 and unfilled_amount <= max(minimum_amount, 0.0):
                return EntryOrderResult(limit_response, True, limit_filled, limit_price)

            # Statistical pricing: try to place unfilled amount at a
            # statistically significant support (long) or resistance (short)
            # instead of chasing with a market order.
            stat_price = None
            if hasattr(self, 'statistical_pricing') and self.statistical_pricing is not None:
                try:
                    stat_price = self.statistical_pricing.resolve_best_limit_price(
                        side=side,
                        execution_frame=execution_frame,
                        volume_profile=None,
                        atr_value=(
                            float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])
                            if execution_frame is not None and len(execution_frame) > 0
                            else None
                        ),
                    )
                except Exception:
                    stat_price = None

            if stat_price is not None and unfilled_amount > 0:
                stat_params = {"timeInForce": "GTC", "executionMode": "statistical_pricing"}
                stat_response = self.client.place_limit_order(
                    self.symbol, side, unfilled_amount, stat_price, params=stat_params
                )
                stat_filled = self._normalize_order_amount(
                    self._extract_filled_amount(stat_response, unfilled_amount, used_limit_order=True)
                )
                stat_price_used = self._extract_order_price(stat_response, fallback=stat_price)
                self._log_entry_fill(
                    order=stat_response,
                    limit_price=stat_price,
                    fill_price=stat_price_used,
                    fill_qty=stat_filled,
                    unfilled_qty=max(0.0, unfilled_amount - stat_filled),
                )
                remaining_stat = max(0.0, unfilled_amount - stat_filled)
                # Only chase with market order if statistical limit also didn't fill
                if stat_filled <= 0:
                    market_response = self.client.place_market_order(
                        self.symbol, side, max(unfilled_amount, amount if limit_filled <= 0 else unfilled_amount)
                    )
                else:
                    market_response = None
                market_filled = self._normalize_order_amount(
                    self._extract_filled_amount(market_response, unfilled_amount if limit_filled > 0 else amount, used_limit_order=False)
                    if market_response else 0.0
                )
                market_price = self._extract_order_price(
                    market_response,
                    fallback=fallback_price or limit_price or aggressive_limit_price,
                ) if market_response else stat_price_used
                remaining_unfilled = max(0.0, amount - limit_filled - stat_filled - market_filled)
                self._log_entry_fill(
                    order=market_response,
                    limit_price=response.get("price") if market_response else None,
                    fill_price=market_price,
                    fill_qty=market_filled,
                    unfilled_qty=remaining_unfilled,
                )
                combined_filled = self._normalize_order_amount(limit_filled + stat_filled + market_filled)
                combined_average = None
                if combined_filled > 0:
                    total_cost = (limit_filled * limit_price) + (stat_filled * stat_price_used) + (market_filled * market_price)
                    combined_average = total_cost / combined_filled
                combined_order = {
                    **limit_response,
                    "filled": combined_filled,
                    "average": combined_average,
                    "amount": amount,
                    "remaining": self._normalize_order_amount(max(0.0, amount - combined_filled)),
                    "info": {
                        **(limit_response.get("info") or {}),
                        "statisticalPricingOrder": stat_response,
                        "marketChaseOrder": market_response,
                    },
                }
                return EntryOrderResult(combined_order, True, combined_filled, combined_average)
            else:
                market_response = self.client.place_market_order(
                    self.symbol, side, max(unfilled_amount, amount if limit_filled <= 0 else unfilled_amount)
                )
                market_filled = self._normalize_order_amount(
                    self._extract_filled_amount(market_response, unfilled_amount if limit_filled > 0 else amount, used_limit_order=False)
                )
                market_price = self._extract_order_price(
                    market_response,
                    fallback=fallback_price or limit_price or aggressive_limit_price,
                )
                remaining_unfilled = self._normalize_order_amount(max(0.0, amount - limit_filled - market_filled))
                self._log_entry_fill(
                    order=market_response,
                    limit_price=response.get("price"),
                    fill_price=market_price,
                    fill_qty=market_filled,
                    unfilled_qty=remaining_unfilled,
                )
                combined_filled = self._normalize_order_amount(limit_filled + market_filled)
                combined_average = None
                if combined_filled > 0:
                    combined_average = ((limit_filled * limit_price) + (market_filled * market_price)) / combined_filled
                combined_order = {
                    **limit_response,
                    "filled": combined_filled,
                    "average": combined_average,
                    "amount": amount,
                    "remaining": self._normalize_order_amount(max(0.0, amount - combined_filled)),
                    "info": {
                        **(limit_response.get("info") or {}),
                        "marketChaseOrder": market_response,
                    },
                }
                return EntryOrderResult(combined_order, True, combined_filled, combined_average)

        if smart_retest_mode and bool(getattr(self.config, "smart_retest_limit_enabled", True)):
            stat_price = None
            atr_value = None
            if execution_frame is not None and len(execution_frame) > 0:
                try:
                    atr_value = float(compute_atr(execution_frame, length=self.config.atr_period).iloc[-1])
                except Exception:
                    atr_value = None
            if hasattr(self, "statistical_pricing") and self.statistical_pricing is not None:
                try:
                    stat_price = self.statistical_pricing.resolve_best_limit_price(
                        side=side,
                        execution_frame=execution_frame,
                        volume_profile=None,
                        atr_value=atr_value,
                    )
                except Exception:
                    stat_price = None
            if stat_price is not None:
                current_price = float(execution_frame["close"].iloc[-1]) if execution_frame is not None and len(execution_frame) > 0 else stat_price
                buffer_ratio = float(getattr(self.config, "smart_retest_price_buffer_ratio", 0.0015))
                if side == "buy":
                    stat_price = min(stat_price, current_price * (1.0 - buffer_ratio))
                else:
                    stat_price = max(stat_price, current_price * (1.0 + buffer_ratio))
                stat_params = {"timeInForce": "GTC", "executionMode": execution_entry_mode}
                stat_response = self.client.place_limit_order(self.symbol, side, amount, stat_price, params=stat_params)
                stat_filled = self._normalize_order_amount(
                    self._extract_filled_amount(stat_response, 0.0, used_limit_order=True)
                )
                stat_price_used = self._extract_order_price(stat_response, fallback=stat_price)
                self._log_entry_fill(
                    order=stat_response,
                    limit_price=stat_price,
                    fill_price=stat_price_used,
                    fill_qty=stat_filled,
                    unfilled_qty=max(0.0, amount - stat_filled),
                )
                return EntryOrderResult(stat_response, True, stat_filled, stat_price_used)

        response = self.client.place_market_order(self.symbol, side, amount)
        filled_amount = self._normalize_order_amount(
            self._extract_filled_amount(response, amount, used_limit_order=False)
        )
        average_price = self._extract_order_price(response, fallback=fallback_price or 0.0) if filled_amount > 0 else None
        return EntryOrderResult(response, False, filled_amount, average_price)

    def _resolve_aggressive_entry_price(
        self,
        *,
        side: str,
        order_flow_assessment: OrderFlowAssessment | None,
        execution_entry_mode: str,
    ) -> float | None:
        if execution_entry_mode in {"weak_bull_scale_in_limit", "early_bullish_starter_limit", "starter_frontrun_limit", "starter_short_frontrun_limit", "near_breakout_release_limit"}:
            reference_price = self._resolve_book_reference_price(side=side)
        else:
            if (
                order_flow_assessment is None
                or not order_flow_assessment.entry_allowed
                or not order_flow_assessment.use_limit_order
            ):
                return None
            reference_price = order_flow_assessment.best_ask if side == "buy" else order_flow_assessment.best_bid
        if reference_price in (None, 0, "0"):
            return None
        tick_size = self._estimate_tick_size(side=side)
        if tick_size <= 0:
            return None
        offset_ticks = 1.0 if execution_entry_mode == "weak_bull_scale_in_limit" else 2.0
        if execution_entry_mode in {"starter_frontrun_limit", "starter_short_frontrun_limit"}:
            offset_ticks = 1.5
        aggressive_price = float(reference_price) + (offset_ticks * tick_size if side == "buy" else -offset_ticks * tick_size)
        if hasattr(self.client, "price_to_precision"):
            aggressive_price = float(self.client.price_to_precision(self.symbol, aggressive_price))
        return aggressive_price

    def _resolve_book_reference_price(self, *, side: str) -> float | None:
        try:
            order_book = self.client.fetch_order_book(self.symbol, limit=1)
        except Exception:
            return None
        levels = list((order_book.get("asks") if side == "buy" else order_book.get("bids")) or [])
        if not levels:
            return None
        return float(levels[0][0])

    def _estimate_tick_size(self, *, side: str) -> float:
        try:
            order_book = self.client.fetch_order_book(self.symbol, limit=3)
        except Exception:
            order_book = {}
        levels = list((order_book.get("asks") if side == "buy" else order_book.get("bids")) or [])
        prices = [float(level[0]) for level in levels if isinstance(level, (list, tuple)) and len(level) >= 2 and float(level[0]) > 0]
        prices = sorted(set(prices))
        if len(prices) >= 2:
            diffs = [abs(b - a) for a, b in zip(prices, prices[1:]) if abs(b - a) > 0]
            if diffs:
                return min(diffs)
        return 0.01

    def _manage_position(self, signal: TrendSignal) -> tuple[list[str], bool]:
        assert self.position is not None
        actions: list[str] = []

        if signal.direction != 0 and signal.direction != self.position.direction:
            if not self._signal_flip_pending:
                reduction_ratio = float(getattr(self.config, "signal_flip_reduce_ratio", 0.50))
                reduction_size = self.position.remaining_size * reduction_ratio
                if reduction_size > 0 and self._reduce_position(reduction_size):
                    self._signal_flip_pending = True
                    self._publish_risk_profile(None if self.position is None else signal)
                    actions.append("cta:signal_flip_reduce")
                    return actions, self.position is None
            self._close_remaining_position(reason="signal_flip")
            self._publish_risk_profile(None)
            actions.append("cta:signal_flip_exit")
            return actions, True
        self._signal_flip_pending = False

        profit_ratio = self.position.profit_ratio(signal.price)
        first_take_profit_pct = float(getattr(self.config, 'obv_scalp_first_take_profit_pct', 0.006)) if self.position.quick_trade_mode else float(self.config.first_take_profit_pct)
        second_take_profit_pct = float(getattr(self.config, 'obv_scalp_second_take_profit_pct', 0.012)) if self.position.quick_trade_mode else float(self.config.second_take_profit_pct)
        first_exit_size = self.position.initial_size * (0.50 if self.position.quick_trade_mode else self.config.first_take_profit_size)
        second_exit_size = self.position.initial_size * (0.50 if self.position.quick_trade_mode else self.config.second_take_profit_size)

        if self.position is not None and not self.position.first_target_hit and profit_ratio >= first_take_profit_pct:
            if self._reduce_position(first_exit_size):
                if self.position is not None:
                    self.position.first_target_hit = True
                actions.append("cta:take_profit_2pct")

        if self.position is not None and not self.position.second_target_hit and profit_ratio >= second_take_profit_pct:
            if self._reduce_position(second_exit_size):
                if self.position is not None:
                    self.position.second_target_hit = True
                actions.append("cta:take_profit_5pct")

        if self.position is None:
            self._publish_risk_profile(None)
            return actions, True

        atr_value = self._normalized_atr(signal.price, signal.atr)
        stop_multiplier = self._resolve_dynamic_stop_loss_multiplier(signal)
        if self.position.quick_trade_mode:
            stop_multiplier *= float(getattr(self.config, 'obv_scalp_stop_multiplier_scale', 0.60))
        self.position.update_dynamic_stop(signal.price, atr_value, stop_multiplier)
        if self.position.stop_hit(signal.price):
            self._close_remaining_position(reason="atr_stop")
            self._publish_risk_profile(None)
            actions.append("cta:atr_stop_all_out")
            return actions, True

        self._publish_risk_profile(signal)
        return actions, False

    def _reduce_position(self, size: float) -> bool:
        if self.position is None:
            return False

        amount = min(self.position.remaining_size, self._round_size(size))
        if amount <= 0:
            return False

        position = self.position
        exit_order = self.client.place_market_order(
            self.symbol,
            position.exit_side,
            amount,
            reduce_only=True,
        )
        self._notify_realized_profit(position=position, amount=amount, exit_order=exit_order, reason="partial_take_profit")
        position.remaining_size = self._round_size(position.remaining_size - amount)
        if position.remaining_size <= 0:
            self.position = None
            self._publish_risk_profile(None)
        else:
            self._publish_risk_profile(None)
        return True

    def _close_remaining_position(self, reason: str) -> None:
        if self.position is None:
            return

        position = self.position
        amount = self._round_size(position.remaining_size)
        if amount > 0:
            exit_order = self.client.place_market_order(
                self.symbol,
                position.exit_side,
                amount,
                reduce_only=True,
                params={"reason": reason},
            )
            self._notify_realized_profit(position=position, amount=amount, exit_order=exit_order, reason=reason)
        self.position = None

    def _apply_runtime_coordination(self, signal: TrendSignal) -> str | None:
        if self.runtime_context is None:
            return None
        if self.position is None:
            return None

        grid_state = self.runtime_context.snapshot_grid()
        if not grid_state.hedge_assist_requested:
            return None
        if grid_state.symbol not in {"", self.symbol}:
            return None
        if grid_state.hedge_assist_target_side not in {self.position.side, None}:
            return None

        reduction_ratio = float(getattr(self.config, "cta_assist_trim_ratio", 0.25))
        reduction_size = self.position.remaining_size * reduction_ratio
        if reduction_size <= 0:
            return None
        if not self._reduce_position(reduction_size):
            return None

        self._publish_risk_profile(signal)
        return f"cta:coordination_reduce_{self.position.side if self.position is not None else 'flat'}|reason={grid_state.hedge_assist_reason or 'grid_inventory_heavy'}"

    def _publish_risk_profile(self, signal: TrendSignal | None) -> None:
        current_side = None
        current_size = 0.0
        strong_trend = False
        trend_strength = 0.0
        if self.position is not None:
            current_side = self.position.side
            current_size = self._round_size(self.position.remaining_size)
        if signal is not None:
            trend_strength = abs(float(signal.major_bias_score or 0.0))
            strong_trend = bool(self.position is not None and signal.direction != 0 and signal.direction == self.position.direction and trend_strength >= float(self.config.strong_bull_bias_score))
        if self.runtime_context is not None:
            self.runtime_context.publish_cta_state(
                symbol=self.symbol,
                side=current_side,
                size=current_size,
                trend_strength=trend_strength,
                strong_trend=strong_trend,
                major_direction=int(signal.major_direction) if signal is not None else 0,
                bullish_ready=bool(signal.bullish_ready) if signal is not None else False,
            )
        if self.risk_manager is None:
            return
        if self.position is None:
            self.risk_manager.update_cta_risk(None)
            return

        atr_value = self.position.atr_value
        if signal is not None:
            atr_value = self._normalized_atr(signal.price, signal.atr)

        self.risk_manager.update_cta_risk(
            CTARiskProfile(
                symbol=self.symbol,
                side=self.position.side,
                stop_price=self.position.stop_price,
                remaining_size=self._round_size(self.position.remaining_size),
                atr_value=atr_value,
                stop_distance=self.position.stop_distance,
            )
        )

    def _refresh_ioc_fill(self, order: dict) -> dict:
        order_id = order.get("id")
        fetch_order = getattr(self.client, "fetch_order", None)
        if order_id in (None, "") or not callable(fetch_order):
            return order

        latest = order
        for attempt in range(2):
            if attempt > 0:
                time.sleep(0.05)
            refreshed = fetch_order(str(order_id), self.symbol)
            if refreshed:
                latest = refreshed
                filled_amount = self._extract_filled_amount(latest, 0.0, used_limit_order=True)
                remaining = latest.get("remaining")
                if filled_amount > 0 or remaining in (0, 0.0, "0"):
                    break
        return latest

    def _journal_event(
        self,
        *,
        event_type: str,
        side: str | None = None,
        action: str | None = None,
        trigger_family: str | None = None,
        trigger_reason: str | None = None,
        pathway: str | None = None,
        price: float | None = None,
        size: float | None = None,
        pnl: float | None = None,
        metadata: dict | None = None,
    ) -> None:
        try:
            self.database.insert_trade_journal(
                TradeJournalRecord(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    strategy_name=self.strategy_name,
                    symbol=self.symbol,
                    event_type=event_type,
                    side=side,
                    action=action,
                    trigger_family=trigger_family,
                    trigger_reason=trigger_reason,
                    pathway=pathway,
                    price=price,
                    size=size,
                    pnl=pnl,
                    metadata=metadata,
                )
            )
        except Exception:
            logger.exception("CTA trade journal insert failed | event_type=%s action=%s", event_type, action)

    def _log_entry_fill(
        self,
        *,
        order: dict | None,
        limit_price: float | None,
        fill_price: float | None,
        fill_qty: float,
        unfilled_qty: float,
    ) -> None:
        order_id = None if not order else order.get("id")
        logger.info(
            "CTA entry fill | order_id=%s limit_price=%s fill_price=%s fill_qty=%.12f unfilled_qty=%.12f",
            order_id,
            limit_price,
            fill_price,
            float(fill_qty),
            float(unfilled_qty),
        )

    def _extract_filled_amount(self, order: dict | None, fallback: float, *, used_limit_order: bool) -> float:
        if not order:
            return 0.0 if used_limit_order else float(fallback)

        explicit_zero_fill = False
        filled = order.get("filled")
        if filled not in (None, ""):
            filled_value = abs(float(filled))
            if used_limit_order and filled_value <= 0:
                explicit_zero_fill = True
            else:
                return filled_value

        info = order.get("info") or {}
        for key in ("accFillSz", "fillSz", "filledSize"):
            value = info.get(key)
            if value not in (None, ""):
                filled_value = abs(float(value))
                if used_limit_order and filled_value <= 0:
                    explicit_zero_fill = True
                    continue
                return filled_value

        amount = order.get("amount")
        remaining = order.get("remaining")
        if amount not in (None, "") and remaining not in (None, ""):
            inferred_filled = max(0.0, abs(float(amount)) - abs(float(remaining)))
            if inferred_filled > 0:
                return inferred_filled

        status = str(order.get("status") or "").lower()
        if used_limit_order:
            if explicit_zero_fill:
                return 0.0
            if status in {"canceled", "cancelled", "expired", "rejected"}:
                return 0.0

        if amount not in (None, ""):
            return abs(float(amount))
        return float(fallback)

    def _extract_order_price(self, order: dict | None, *, fallback: float) -> float:
        if not order:
            return float(fallback)

        for key in ("average", "avgPrice", "price"):
            value = order.get(key)
            if value not in (None, "", 0, "0"):
                return float(value)

        info = order.get("info") or {}
        for key in ("avgPx", "fillPx", "px"):
            value = info.get(key)
            if value not in (None, "", 0, "0"):
                return float(value)
        return float(fallback)

    def _notify_realized_profit(self, *, position: ManagedPosition, amount: float, exit_order: dict | None, reason: str | None = None) -> None:
        exit_amount = self._round_size(amount)
        if exit_amount <= 0:
            return

        fallback_price = None
        if hasattr(self.client, "fetch_last_price"):
            try:
                fallback_price = float(self.client.fetch_last_price(self.symbol))
            except Exception:
                fallback_price = None
        if fallback_price in (None, 0):
            fallback_price = position.entry_price

        exit_price = self._extract_order_price(exit_order, fallback=float(fallback_price))
        if exit_price <= 0 or position.entry_price <= 0:
            return

        contract_value = 1.0
        if hasattr(self.client, "get_contract_value"):
            try:
                contract_value = abs(float(self.client.get_contract_value(self.symbol))) or 1.0
            except Exception:
                contract_value = 1.0

        price_delta = exit_price - position.entry_price
        if position.side == "short":
            price_delta = position.entry_price - exit_price
        pnl = float(price_delta) * float(exit_amount) * contract_value
        entry_notional = abs(float(position.entry_price)) * float(exit_amount) * contract_value
        roi = (pnl / entry_notional * 100.0) if entry_notional > 0 else 0.0

        balance = 0.0
        if hasattr(self.client, "fetch_total_equity"):
            try:
                balance = float(self.client.fetch_total_equity("USDT"))
            except Exception:
                balance = 0.0

        logger.info(
            "[TRADE_CLOSE] side=%s reason=%s entry=%.4f exit=%.4f size=%.8f pnl=%.4f roi=%.4f%% stop=%.4f best=%.4f",
            position.side,
            reason or "unspecified",
            float(position.entry_price),
            float(exit_price),
            float(exit_amount),
            float(pnl),
            float(roi),
            float(position.stop_price),
            float(position.best_price),
        )
        self._journal_event(
            event_type="trade_close",
            side=position.side,
            action=reason or "unspecified",
            trigger_family=str(position.origin_trigger_family or "waiting"),
            trigger_reason=str(position.origin_trigger_reason or ""),
            pathway=str(position.origin_pathway or ""),
            price=float(exit_price),
            size=float(exit_amount),
            pnl=float(pnl),
            metadata={
                "entry_price": float(position.entry_price),
                "exit_price": float(exit_price),
                "roi": float(roi),
                "stop_price": float(position.stop_price),
                "best_price": float(position.best_price),
                "origin_trigger_family": str(position.origin_trigger_family or "waiting"),
                "origin_trigger_reason": str(position.origin_trigger_reason or ""),
                "origin_pathway": str(position.origin_pathway or ""),
            },
        )

        if self.notifier is None or not hasattr(self.notifier, "notify_profit"):
            return

        self.notifier.notify_profit(
            pnl=pnl,
            roi=roi,
            balance=balance,
            strategy=self.strategy_name,
            symbol=self.symbol,
            side=position.side.upper(),
            exit_price=exit_price,
            size=exit_amount,
        )

    def _normalize_order_amount(self, amount: float) -> float:
        normalized = max(0.0, float(amount))
        if hasattr(self.client, "amount_to_precision"):
            normalized = float(self.client.amount_to_precision(self.symbol, normalized))
        minimum_amount = 0.0
        if hasattr(self.client, "get_min_order_amount"):
            minimum_amount = float(self.client.get_min_order_amount(self.symbol))
        normalized = self._round_size(normalized)
        if normalized < minimum_amount - 1e-12:
            return 0.0
        return normalized

    def _normalized_atr(self, price: float, atr: float) -> float:
        return max(float(atr), float(price) * 0.001)

    def _resolve_risk_percent(self, mtf_signal: MTFSignal) -> float:
        boosted_risk = max(float(self.config.risk_percent_per_trade), float(self.config.boosted_risk_percent_per_trade))
        if mtf_signal.fully_aligned and not mtf_signal.weak_bull_bias:
            return boosted_risk
        return float(self.config.risk_percent_per_trade)

    @staticmethod
    def _round_size(size: float) -> float:
        rounded = round(float(size), 12)
        return 0.0 if abs(rounded) < 1e-12 else rounded
