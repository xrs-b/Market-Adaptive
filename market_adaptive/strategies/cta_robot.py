from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

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
from market_adaptive.strategies.mtf_engine import MTFSignal, MultiTimeframeSignalEngine
from market_adaptive.strategies.obv_gate import resolve_dynamic_obv_gate_for_signal
from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment, OrderFlowSentinel
from market_adaptive.strategies.signal_profiler import SignalProfiler

logger = logging.getLogger(__name__)


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
    execution_memory_active: bool = False
    execution_latch_active: bool = False
    execution_latch_price: float | None = None
    execution_frontrun_near_breakout: bool = False
    execution_memory_bars_ago: int | None = None
    execution_trigger_reason: str = ""
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
    execution_trigger_reason: str
    execution_memory_active: bool
    execution_memory_bars_ago: int | None
    execution_breakout: bool
    execution_golden_cross: bool
    obv_zscore: float
    obv_threshold: float
    obv_gap: float
    price: float


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
        current_stop_distance = max(float(atr) * float(stop_multiplier), float(price) * 0.001)
        self.atr_value = float(atr)
        self.stop_distance = current_stop_distance
        if self.side == "long":
            self.best_price = max(self.best_price, price)
            candidate = self.best_price - current_stop_distance
            self.stop_price = max(self.stop_price, candidate)
            return

        self.best_price = min(self.best_price, price)
        candidate = self.best_price + current_stop_distance
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
        self._last_signal_heartbeat_at = 0.0
        self._last_major_direction: int | None = None
        self._last_bullish_ready: bool | None = None
        self._near_miss_samples: list[CTANearMissSample] = []
        self._near_miss_window_started_at: float | None = None
        self._last_near_miss_report_at = 0.0
        self._time_provider = time.time

    def _resolve_obv_gate(self, signal: MTFSignal) -> tuple[float, bool]:
        decision = resolve_dynamic_obv_gate_for_signal(
            signal,
            configured_threshold=float(self.config.obv_zscore_threshold),
        )
        return float(decision.threshold), bool(decision.exempt)

    def _evaluate_high_momentum_clearance(
        self,
        *,
        mtf_signal: MTFSignal,
        inside_value_area: bool,
    ) -> HighMomentumClearanceDecision:
        eligible = bool(float(mtf_signal.bullish_score) >= 75.0 and mtf_signal.execution_trigger.frontrun_near_breakout)
        used_rsi_override = bool(eligible and getattr(mtf_signal, "rsi_blocking_overridden", False))
        used_value_area_override = bool(eligible and inside_value_area)
        return HighMomentumClearanceDecision(
            eligible=eligible,
            used_rsi_override=used_rsi_override,
            used_value_area_override=used_value_area_override,
        )

    def _evaluate_value_area_decision(
        self,
        *,
        volume_profile: VolumeProfileSnapshot | None,
        current_price: float,
        atr_value: float,
        major_direction: int,
        bullish_score: float,
        execution_frontrun_near_breakout: bool,
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
        if float(bullish_score) >= 75.0 and bool(execution_frontrun_near_breakout):
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='High Momentum')

        if int(major_direction) > 0 and float(bullish_score) >= drive_first_score and float(current_price) >= value_area_high - edge_threshold:
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='Edge Proximity')

        if int(major_direction) < 0 and float(current_price) < value_area_low + edge_threshold:
            return ValueAreaDecision(inside_value_area=True, blocked=False, reason='Edge Proximity')

        return ValueAreaDecision(inside_value_area=True, blocked=True)

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
        del reason
        self.position = None
        self._publish_risk_profile(None)

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

        if self.position is None and signal.direction != 0:
            return self._open_position(signal)

        self._publish_risk_profile(signal)
        if self.position is None and signal.long_setup_blocked:
            return "cta:range_filter_blocked"
        if self.position is None and signal.bullish_ready and signal.raw_direction == 0:
            return "cta:bullish_ready"
        return "cta:no_signal" if self.position is None else "cta:hold"

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
        obv_threshold, obv_exempt = self._resolve_obv_gate(mtf_signal)
        drive_first_tradeable = bool(float(mtf_signal.bullish_score) >= float(getattr(self.config, "drive_first_tradeable_score", 60.0)))
        relaxed_obv_allowed = bool(
            raw_direction > 0
            and int(mtf_signal.major_direction) > 0
            and drive_first_tradeable
            and float(obv_confirmation.zscore) > float(obv_threshold)
        )
        volume_filter_passed = False
        if raw_direction > 0:
            volume_filter_passed = bool(obv_exempt or obv_confirmation.buy_confirmed(zscore_threshold=obv_threshold) or relaxed_obv_allowed)
        elif raw_direction < 0:
            volume_filter_passed = bool(obv_exempt or obv_confirmation.sell_confirmed(zscore_threshold=obv_threshold))
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
        )

        final_direction = raw_direction
        long_setup_blocked = False
        long_setup_reason = ""
        obv_confirmation_passed = True
        relaxed_reasons: list[str] = []

        if raw_direction > 0:
            obv_confirmation_passed = volume_filter_passed
            value_area_decision = self._evaluate_value_area_decision(
                volume_profile=volume_profile,
                current_price=current_price,
                atr_value=float(atr_series.iloc[-1]),
                major_direction=int(mtf_signal.major_direction),
                bullish_score=float(mtf_signal.bullish_score),
                execution_frontrun_near_breakout=bool(mtf_signal.execution_trigger.frontrun_near_breakout),
            )
            if not obv_exempt and not obv_confirmation_passed:
                long_setup_blocked = True
                long_setup_reason = "obv_strength_not_confirmed"
            elif not obv_exempt and not obv_confirmation.above_sma:
                if relaxed_obv_allowed:
                    relaxed_reasons.append(f"OBV({float(obv_confirmation.zscore):.2f}) > Floor({float(obv_threshold):.2f})")
                else:
                    long_setup_blocked = True
                    long_setup_reason = "obv_below_sma"
            elif volume_profile is None:
                long_setup_blocked = True
                long_setup_reason = "missing_volume_profile"
            elif not volume_profile.above_poc(current_price):
                long_setup_blocked = True
                long_setup_reason = "below_poc"
            elif value_area_decision.blocked:
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
                final_direction = 0
                long_setup_blocked = True
                long_setup_reason = "obv_strength_not_confirmed"
            elif volume_profile is None:
                final_direction = 0
                long_setup_blocked = True
                long_setup_reason = "missing_volume_profile"
            elif float(current_price) >= float(volume_profile.poc_price):
                final_direction = 0
                long_setup_blocked = True
                long_setup_reason = "above_poc"

        blocker_reason = mtf_signal.blocker_reason
        if long_setup_blocked:
            blocker_reason = f"Blocked_By_{str(long_setup_reason).upper()}"
        if self.signal_profiler is not None:
            grid_center = self.grid_center_provider() if callable(self.grid_center_provider) else None
            self.signal_profiler.record(mtf_signal, grid_center_price=grid_center, blocker_reason=blocker_reason)

        return TrendSignal(
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
            execution_memory_active=mtf_signal.execution_trigger.bullish_memory_active,
            execution_latch_active=mtf_signal.execution_trigger.bullish_latch_active,
            execution_latch_price=mtf_signal.execution_trigger.latch_low_price,
            execution_frontrun_near_breakout=mtf_signal.execution_trigger.frontrun_near_breakout,
            execution_memory_bars_ago=mtf_signal.execution_trigger.bullish_cross_bars_ago,
            execution_trigger_reason=mtf_signal.execution_trigger.reason,
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
        )

    def _effective_signal_obv_threshold(self, signal: TrendSignal) -> float:
        if signal.obv_threshold is not None:
            return float(signal.obv_threshold)
        return float(self.config.obv_zscore_threshold)

    def _build_signal_heartbeat_payload(self, signal: TrendSignal) -> dict[str, float | str | bool]:
        obv = signal.obv_confirmation
        threshold = self._effective_signal_obv_threshold(signal)
        return {
            "symbol": self.symbol,
            "bullish_ready": bool(signal.bullish_ready),
            "bullish_score": float(signal.bullish_score),
            "bullish_threshold": float(signal.bullish_threshold),
            "major_bias_score": float(signal.major_bias_score),
            "weak_bull_bias": bool(signal.weak_bull_bias),
            "early_bullish": bool(signal.early_bullish),
            "entry_size_multiplier": float(signal.entry_size_multiplier),
            "swing_rsi": float(signal.swing_rsi),
            "swing_rsi_slope": float(signal.swing_rsi_slope),
            "raw_direction": int(signal.raw_direction),
            "direction": int(signal.direction),
            "execution_entry_mode": str(signal.execution_entry_mode),
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
            "long_setup_reason": str(signal.long_setup_reason),
            "price": float(signal.price),
            "atr": float(signal.atr),
            "blocker_reason": str(signal.blocker_reason),
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

    def _is_execution_near_ready(self, signal: TrendSignal) -> bool:
        return bool(
            signal.bullish_ready
            and (
                signal.raw_direction > 0
                or signal.execution_memory_active
                or signal.execution_latch_active
                or signal.execution_frontrun_near_breakout
                or signal.execution_breakout
            )
        )

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
        sample = CTANearMissSample(
            symbol=self.symbol,
            captured_at=float(self._time_provider()),
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

    def _open_position(self, signal: TrendSignal) -> str:
        side = "buy" if signal.direction > 0 else "sell"
        amount = self._calculate_entry_amount(signal.price)
        sentiment_halved = False

        amount *= max(0.0, min(1.0, float(signal.entry_size_multiplier)))
        amount = self._normalize_order_amount(amount)
        if amount <= 0:
            self._publish_risk_profile(None)
            return "cta:risk_blocked"

        if side == "buy" and self.sentiment_analyst is not None:
            sentiment_decision = self.sentiment_analyst.evaluate_cta_buy(self.symbol)
            if sentiment_decision.blocked:
                self._publish_risk_profile(None)
                return "cta:sentiment_blocked"
            if sentiment_decision.size_multiplier < 1.0:
                amount = self._normalize_order_amount(amount * sentiment_decision.size_multiplier)
                sentiment_halved = amount > 0
                if amount <= 0:
                    self._publish_risk_profile(None)
                    return "cta:sentiment_blocked"

        order_flow_assessment: OrderFlowAssessment | None = None
        if side == "buy" and self.config.order_flow_enabled:
            order_flow_assessment = self.order_flow_sentinel.assess_entry(self.symbol, side, amount)
            if not order_flow_assessment.entry_allowed and signal.execution_entry_mode not in {"weak_bull_scale_in_limit", "early_bullish_starter_limit", "starter_frontrun_limit"}:
                self._publish_risk_profile(None)
                return "cta:order_flow_blocked"

        position_side = "long" if signal.direction > 0 else "short"
        notional_price = signal.price
        if order_flow_assessment is not None and order_flow_assessment.reference_price is not None:
            notional_price = max(notional_price, order_flow_assessment.reference_price)
        if self.risk_manager is not None:
            requested_notional = self.client.estimate_notional(self.symbol, amount, notional_price)
            allowed, _reason = self.risk_manager.can_open_new_position(
                self.symbol,
                requested_notional,
                strategy_name=self.strategy_name,
                opening_side=position_side,
            )
            if not allowed:
                self._publish_risk_profile(None)
                return "cta:risk_blocked"

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
        logger.info("[TRADE_OPEN] Type: %s | Reason: %s", entry_type, entry_reason)

        entry_order = self._place_entry_order(
            side=side,
            amount=amount,
            order_flow_assessment=order_flow_assessment,
            execution_entry_mode=signal.execution_entry_mode,
        )
        filled_amount = self._normalize_order_amount(entry_order.filled_amount)
        fill_ratio = (filled_amount / amount) if amount > 0 else 0.0
        if filled_amount <= 0:
            self._publish_risk_profile(None)
            return "cta:low_fill_ratio" if entry_order.used_limit_order else "cta:risk_blocked"
        if entry_order.used_limit_order and fill_ratio < 0.5:
            self._publish_risk_profile(None)
            return "cta:low_fill_ratio"

        entry_price = (
            float(entry_order.average_price)
            if entry_order.average_price not in (None, 0, "0")
            else self._extract_order_price(
                entry_order.order,
                fallback=(
                    order_flow_assessment.expected_average_price
                    if order_flow_assessment is not None and order_flow_assessment.expected_average_price is not None
                    else notional_price
                ),
            )
        )
        entry_price = float(entry_price)
        atr_value = self._normalized_atr(entry_price, signal.atr)
        stop_distance = atr_value * self.config.stop_loss_atr
        if signal.direction > 0:
            stop_price = entry_price - stop_distance
        else:
            stop_price = entry_price + stop_distance

        self.position = ManagedPosition(
            side=position_side,
            entry_price=entry_price,
            initial_size=filled_amount,
            remaining_size=filled_amount,
            stop_price=stop_price,
            best_price=entry_price,
            atr_value=atr_value,
            stop_distance=stop_distance,
            risk_percent=signal.risk_percent,
        )
        self._publish_risk_profile(signal)
        action = f"cta:open_{position_side}"
        if entry_order.used_limit_order:
            action += "_limit"
        if sentiment_halved:
            action += "_sentiment_halved"
        return action

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
    ) -> EntryOrderResult:
        fallback_price = (
            order_flow_assessment.expected_average_price
            if order_flow_assessment is not None and order_flow_assessment.expected_average_price is not None
            else None
        )
        minimum_amount = 0.0
        if hasattr(self.client, "get_min_order_amount"):
            minimum_amount = float(self.client.get_min_order_amount(self.symbol))

        aggressive_limit_price = self._resolve_aggressive_entry_price(
            side=side,
            order_flow_assessment=order_flow_assessment,
            execution_entry_mode=execution_entry_mode,
        )
        if aggressive_limit_price is not None:
            params = {"timeInForce": "IOC", "executionMode": "aggressive_limit"}
            if execution_entry_mode == "weak_bull_scale_in_limit":
                params["executionMode"] = "weak_bull_scale_in"
            elif execution_entry_mode == "early_bullish_starter_limit":
                params["executionMode"] = "early_bullish_starter"
            elif execution_entry_mode == "starter_frontrun_limit":
                params["executionMode"] = "starter_frontrun"
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

            market_response = self.client.place_market_order(self.symbol, side, max(unfilled_amount, amount if limit_filled <= 0 else unfilled_amount))
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
        if execution_entry_mode in {"weak_bull_scale_in_limit", "early_bullish_starter_limit", "starter_frontrun_limit"}:
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
        if execution_entry_mode == "starter_frontrun_limit":
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
            self._close_remaining_position(reason="signal_flip")
            self._publish_risk_profile(None)
            actions.append("cta:signal_flip_exit")
            return actions, True

        profit_ratio = self.position.profit_ratio(signal.price)
        first_exit_size = self.position.initial_size * self.config.first_take_profit_size
        second_exit_size = self.position.initial_size * self.config.second_take_profit_size

        if not self.position.first_target_hit and profit_ratio >= self.config.first_take_profit_pct:
            if self._reduce_position(first_exit_size):
                self.position.first_target_hit = True
                actions.append("cta:take_profit_2pct")

        if self.position is not None and not self.position.second_target_hit and profit_ratio >= self.config.second_take_profit_pct:
            if self._reduce_position(second_exit_size):
                self.position.second_target_hit = True
                actions.append("cta:take_profit_5pct")

        if self.position is None:
            self._publish_risk_profile(None)
            return actions, True

        atr_value = self._normalized_atr(signal.price, signal.atr)
        self.position.update_dynamic_stop(signal.price, atr_value, self.config.stop_loss_atr)
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
        self._notify_realized_profit(position=position, amount=amount, exit_order=exit_order)
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
            self._notify_realized_profit(position=position, amount=amount, exit_order=exit_order)
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

    def _notify_realized_profit(self, *, position: ManagedPosition, amount: float, exit_order: dict | None) -> None:
        if self.notifier is None or not hasattr(self.notifier, "notify_profit"):
            return

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

        self.notifier.notify_profit(pnl=pnl, roi=roi, balance=balance)

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
