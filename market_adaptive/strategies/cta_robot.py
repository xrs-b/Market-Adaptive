from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.indicators import compute_atr, compute_obv, compute_supertrend, ohlcv_to_dataframe
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

    def update_trailing_stop(self, price: float, atr: float, multiplier: float) -> None:
        if self.side == "long":
            self.best_price = max(self.best_price, price)
            candidate = self.best_price - atr * multiplier
            self.stop_price = max(self.stop_price, candidate)
            return

        self.best_price = min(self.best_price, price)
        candidate = self.best_price + atr * multiplier
        self.stop_price = min(self.stop_price, candidate)

    def stop_hit(self, price: float) -> bool:
        if self.side == "long":
            return price <= self.stop_price
        return price >= self.stop_price


class CTARobot(BaseStrategyRobot):
    strategy_name = "cta"
    activation_status = "trend"

    def __init__(self, client, database, config: CTAConfig, execution_config: ExecutionConfig, notifier=None) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.position: ManagedPosition | None = None

    def should_notify_action(self, action: str) -> bool:
        if action in {"cta:hold", "cta:no_signal", "cta:insufficient_data", "skip:inactive"}:
            return False
        return super().should_notify_action(action)

    def flatten_and_cancel_all(self, reason: str) -> None:
        super().flatten_and_cancel_all(reason)
        self.position = None

    def execute_active_cycle(self) -> str:
        signal = self._build_trend_signal()
        if signal is None:
            return "cta:insufficient_data"

        actions: list[str] = []
        closed_position = False

        if self.position is not None:
            actions, closed_position = self._manage_position(signal)
            if actions:
                return "+".join(actions)
            if closed_position:
                return "cta:hold"

        if self.position is None and signal.direction != 0:
            return self._open_position(signal)

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
        self.client.place_market_order(self.symbol, side, self.execution_config.cta_order_size)

        atr_value = self._normalized_atr(signal.price, signal.atr)
        if signal.direction > 0:
            stop_price = signal.price - atr_value * self.config.atr_trailing_multiplier
            position_side = "long"
        else:
            stop_price = signal.price + atr_value * self.config.atr_trailing_multiplier
            position_side = "short"

        self.position = ManagedPosition(
            side=position_side,
            entry_price=signal.price,
            initial_size=self.execution_config.cta_order_size,
            remaining_size=self.execution_config.cta_order_size,
            stop_price=stop_price,
            best_price=signal.price,
        )
        return f"cta:open_{position_side}"

    def _manage_position(self, signal: TrendSignal) -> tuple[list[str], bool]:
        assert self.position is not None
        actions: list[str] = []

        if signal.direction != 0 and signal.direction != self.position.direction:
            self._close_remaining_position(reason="signal_flip")
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
            return actions, True

        atr_value = self._normalized_atr(signal.price, signal.atr)
        self.position.update_trailing_stop(signal.price, atr_value, self.config.atr_trailing_multiplier)
        if self.position.stop_hit(signal.price):
            self._close_remaining_position(reason="trailing_stop")
            actions.append("cta:trailing_stop_exit")
            return actions, True

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

    def _normalized_atr(self, price: float, atr: float) -> float:
        return max(float(atr), float(price) * 0.001)

    @staticmethod
    def _round_size(size: float) -> float:
        rounded = round(float(size), 12)
        return 0.0 if abs(rounded) < 1e-12 else rounded
