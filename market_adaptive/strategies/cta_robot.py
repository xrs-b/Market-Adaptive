from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.indicators import compute_atr, compute_obv, compute_supertrend, ohlcv_to_dataframe
from market_adaptive.risk import CTARiskProfile, LogicalPositionSnapshot
from market_adaptive.sentiment import SentimentAnalyst
from market_adaptive.strategies.base import BaseStrategyRobot


@dataclass
class TrendSignal:
    direction: int
    lower_direction: int
    higher_direction: int
    obv_bias: int
    volume_filter_passed: bool
    price: float
    atr: float


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

    def should_notify_action(self, action: str) -> bool:
        if action in {"cta:hold", "cta:no_signal", "cta:insufficient_data", "cta:risk_blocked", "skip:inactive"}:
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
        return "cta:no_signal" if self.position is None else "cta:hold"

    def _build_trend_signal(self) -> TrendSignal | None:
        minimum_bars = max(
            self.config.supertrend_period * 3,
            self.config.atr_period * 3,
            self.config.obv_signal_period + 2,
        )
        lower_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.lower_timeframe,
            limit=self.config.lookback_limit,
        )
        higher_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.higher_timeframe,
            limit=self.config.lookback_limit,
        )
        if len(lower_ohlcv) < minimum_bars or len(higher_ohlcv) < minimum_bars:
            return None

        lower_frame = ohlcv_to_dataframe(lower_ohlcv)
        higher_frame = ohlcv_to_dataframe(higher_ohlcv)

        lower_supertrend = compute_supertrend(
            lower_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        higher_supertrend = compute_supertrend(
            higher_frame,
            length=self.config.supertrend_period,
            multiplier=self.config.supertrend_multiplier,
        )
        lower_obv = compute_obv(lower_frame)
        lower_obv_signal = lower_obv.ewm(span=self.config.obv_signal_period, adjust=False).mean()
        atr_series = compute_atr(lower_frame, length=self.config.atr_period)

        lower_direction = int(lower_supertrend["direction"].iloc[-1])
        higher_direction = int(higher_supertrend["direction"].iloc[-1])
        obv_value = float(lower_obv.iloc[-1])
        obv_signal_value = float(lower_obv_signal.iloc[-1])
        obv_bias = 1 if obv_value > obv_signal_value else -1 if obv_value < obv_signal_value else 0
        aligned_direction = lower_direction if lower_direction == higher_direction else 0
        volume_filter_passed = aligned_direction != 0 and obv_bias == aligned_direction

        return TrendSignal(
            direction=aligned_direction if volume_filter_passed else 0,
            lower_direction=lower_direction,
            higher_direction=higher_direction,
            obv_bias=obv_bias,
            volume_filter_passed=volume_filter_passed,
            price=float(lower_frame["close"].iloc[-1]),
            atr=float(atr_series.iloc[-1]),
        )

    def _open_position(self, signal: TrendSignal) -> str:
        side = "buy" if signal.direction > 0 else "sell"
        amount = self.execution_config.cta_order_size
        sentiment_halved = False

        if self.risk_manager is not None:
            amount = self.risk_manager.calculate_position_size(
                self.symbol,
                self.config.risk_percent_per_trade,
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

        if self.risk_manager is not None:
            requested_notional = self.client.estimate_notional(self.symbol, amount, signal.price)
            allowed, _reason = self.risk_manager.can_open_new_position(
                self.symbol,
                requested_notional,
                strategy_name=self.strategy_name,
            )
            if not allowed:
                self._publish_risk_profile(None)
                return "cta:risk_blocked"

        self.client.place_market_order(self.symbol, side, amount)

        atr_value = self._normalized_atr(signal.price, signal.atr)
        stop_distance = atr_value * self.config.stop_loss_atr
        if signal.direction > 0:
            stop_price = signal.price - stop_distance
            position_side = "long"
        else:
            stop_price = signal.price + stop_distance
            position_side = "short"

        self.position = ManagedPosition(
            side=position_side,
            entry_price=signal.price,
            initial_size=amount,
            remaining_size=amount,
            stop_price=stop_price,
            best_price=signal.price,
            atr_value=atr_value,
            stop_distance=stop_distance,
        )
        self._publish_risk_profile(signal)
        action = f"cta:open_{position_side}"
        if sentiment_halved:
            action += "_sentiment_halved"
        return action

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

    @staticmethod
    def _round_size(size: float) -> float:
        rounded = round(float(size), 12)
        return 0.0 if abs(rounded) < 1e-12 else rounded
