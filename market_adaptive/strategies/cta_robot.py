from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.config import CTAConfig, ExecutionConfig
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
from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment, OrderFlowSentinel


@dataclass
class TrendSignal:
    direction: int
    raw_direction: int
    major_direction: int
    swing_rsi: float
    bullish_ready: bool
    execution_golden_cross: bool
    execution_breakout: bool
    execution_trigger_reason: str
    mtf_aligned: bool
    obv_bias: int
    obv_confirmation: OBVConfirmationSnapshot
    obv_confirmation_passed: bool
    volume_filter_passed: bool
    volume_profile: VolumeProfileSnapshot | None
    long_setup_blocked: bool
    long_setup_reason: str
    price: float
    atr: float
    risk_percent: float

    @property
    def obv_slope_angle(self) -> float:
        return self.obv_confirmation.roc_pct

    @property
    def obv_slope_passed(self) -> bool:
        return self.obv_confirmation_passed


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

    def __init__(
        self,
        client,
        database,
        config: CTAConfig,
        execution_config: ExecutionConfig,
        notifier=None,
        risk_manager=None,
        sentiment_analyst: SentimentAnalyst | None = None,
    ) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.sentiment_analyst = sentiment_analyst
        self.position: ManagedPosition | None = None
        self.mtf_engine = MultiTimeframeSignalEngine(client, config)
        self.order_flow_sentinel = OrderFlowSentinel(client, config)

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
            roc_period=self.config.obv_roc_period,
            zscore_window=self.config.obv_zscore_window,
            roc_percentile_window=self.config.obv_roc_percentile_window,
            extreme_percentile=self.config.obv_roc_extreme_percentile,
        )
        obv_bias = 1 if obv_confirmation.above_sma else -1 if obv_confirmation.below_sma else 0
        raw_direction = 1 if mtf_signal.fully_aligned else 0
        volume_filter_passed = raw_direction > 0 and obv_confirmation.buy_confirmed(
            zscore_threshold=self.config.obv_zscore_threshold,
        )
        current_price = float(execution_frame["close"].iloc[-1])
        volume_profile = compute_volume_profile(
            execution_frame,
            lookback_hours=self.config.volume_profile_lookback_hours,
            value_area_pct=self.config.volume_profile_value_area_pct,
            bin_count=self.config.volume_profile_bin_count,
        )

        final_direction = raw_direction
        long_setup_blocked = False
        long_setup_reason = ""
        obv_confirmation_passed = True

        if raw_direction > 0:
            obv_confirmation_passed = volume_filter_passed
            if not obv_confirmation.above_sma:
                long_setup_blocked = True
                long_setup_reason = "obv_below_sma"
            elif not obv_confirmation_passed:
                long_setup_blocked = True
                long_setup_reason = "obv_strength_not_confirmed"
            elif volume_profile is None:
                long_setup_blocked = True
                long_setup_reason = "missing_volume_profile"
            elif not volume_profile.above_poc(current_price):
                long_setup_blocked = True
                long_setup_reason = "below_poc"
            elif volume_profile.contains_price(current_price):
                long_setup_blocked = True
                long_setup_reason = "inside_value_area"
            elif not volume_profile.above_value_area(current_price):
                long_setup_blocked = True
                long_setup_reason = "below_value_area_high"

            if long_setup_blocked:
                final_direction = 0

        return TrendSignal(
            direction=final_direction,
            raw_direction=raw_direction,
            major_direction=mtf_signal.major_direction,
            swing_rsi=mtf_signal.swing_rsi,
            bullish_ready=mtf_signal.bullish_ready,
            execution_golden_cross=mtf_signal.execution_trigger.kdj_golden_cross,
            execution_breakout=mtf_signal.execution_trigger.prior_high_break,
            execution_trigger_reason=mtf_signal.execution_trigger.reason,
            mtf_aligned=mtf_signal.fully_aligned,
            obv_bias=obv_bias,
            obv_confirmation=obv_confirmation,
            obv_confirmation_passed=obv_confirmation_passed,
            volume_filter_passed=volume_filter_passed,
            volume_profile=volume_profile,
            long_setup_blocked=long_setup_blocked,
            long_setup_reason=long_setup_reason,
            price=current_price,
            atr=float(atr_series.iloc[-1]),
            risk_percent=self._resolve_risk_percent(mtf_signal),
        )

    def _open_position(self, signal: TrendSignal) -> str:
        side = "buy" if signal.direction > 0 else "sell"
        amount = self.execution_config.cta_order_size
        sentiment_halved = False

        if self.risk_manager is not None:
            amount = self.risk_manager.calculate_position_size(
                self.symbol,
                signal.risk_percent,
                self.config.stop_loss_atr,
                atr_value=signal.atr,
                last_price=signal.price,
            )

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
            if not order_flow_assessment.entry_allowed:
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

        order_response, used_limit_order = self._place_entry_order(
            side=side,
            amount=amount,
            order_flow_assessment=order_flow_assessment,
        )
        filled_amount = self._extract_filled_amount(order_response, amount, used_limit_order=used_limit_order)
        filled_amount = self._normalize_order_amount(filled_amount)
        if filled_amount <= 0:
            self._publish_risk_profile(None)
            return "cta:slippage_blocked" if used_limit_order else "cta:risk_blocked"

        entry_price = self._extract_order_price(
            order_response,
            fallback=(
                order_flow_assessment.expected_average_price
                if order_flow_assessment is not None and order_flow_assessment.expected_average_price is not None
                else notional_price
            ),
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
        if used_limit_order:
            action += "_limit"
        if sentiment_halved:
            action += "_sentiment_halved"
        return action

    def _place_entry_order(
        self,
        *,
        side: str,
        amount: float,
        order_flow_assessment: OrderFlowAssessment | None,
    ) -> tuple[dict, bool]:
        if (
            side == "buy"
            and order_flow_assessment is not None
            and order_flow_assessment.use_limit_order
            and order_flow_assessment.recommended_limit_price is not None
        ):
            response = self.client.place_limit_order(
                self.symbol,
                side,
                amount,
                order_flow_assessment.recommended_limit_price,
                params={
                    "timeInForce": "IOC",
                    "orderFlowImbalance": round(order_flow_assessment.imbalance_ratio, 4),
                },
            )
            return response, True

        response = self.client.place_market_order(self.symbol, side, amount)
        return response, False

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

        self.client.place_market_order(
            self.symbol,
            self.position.exit_side,
            amount,
            reduce_only=True,
        )
        self.position.remaining_size = self._round_size(self.position.remaining_size - amount)
        if self.position.remaining_size <= 0:
            self.position = None
            self._publish_risk_profile(None)
        else:
            self._publish_risk_profile(None)
        return True

    def _close_remaining_position(self, reason: str) -> None:
        if self.position is None:
            return

        amount = self._round_size(self.position.remaining_size)
        if amount > 0:
            self.client.place_market_order(
                self.symbol,
                self.position.exit_side,
                amount,
                reduce_only=True,
                params={"reason": reason},
            )
        self.position = None

    def _publish_risk_profile(self, signal: TrendSignal | None) -> None:
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

    def _extract_filled_amount(self, order: dict | None, fallback: float, *, used_limit_order: bool) -> float:
        if not order:
            return 0.0 if used_limit_order else float(fallback)

        filled = order.get("filled")
        if filled not in (None, ""):
            return abs(float(filled))

        info = order.get("info") or {}
        for key in ("accFillSz", "fillSz", "filledSize"):
            value = info.get(key)
            if value not in (None, ""):
                return abs(float(value))

        amount = order.get("amount")
        remaining = order.get("remaining")
        if amount not in (None, "") and remaining not in (None, ""):
            inferred_filled = max(0.0, abs(float(amount)) - abs(float(remaining)))
            if inferred_filled > 0:
                return inferred_filled

        status = str(order.get("status") or "").lower()
        if used_limit_order and status in {"canceled", "cancelled", "expired", "rejected"}:
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
        if mtf_signal.fully_aligned:
            return boosted_risk
        return float(self.config.risk_percent_per_trade)

    @staticmethod
    def _round_size(size: float) -> float:
        rounded = round(float(size), 12)
        return 0.0 if abs(rounded) < 1e-12 else rounded
