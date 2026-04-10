from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone

from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.indicators import compute_bollinger_bands, ohlcv_to_dataframe
from market_adaptive.strategies.base import BaseStrategyRobot


@dataclass
class GridLayer:
    index: int
    side: str
    price: float
    amount: float


class GridRobot(BaseStrategyRobot):
    strategy_name = "grid"
    activation_status = "sideways"

    def __init__(self, client, database, config: GridConfig, execution_config: ExecutionConfig, notifier=None, risk_manager=None) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.last_price: float | None = None
        self.layer_triggers: dict[int, deque[float]] = defaultdict(deque)

    def should_notify_action(self, action: str) -> bool:
        if action in {"grid:risk_blocked", "grid:no_bands", "grid:no_orders"}:
            return False
        return super().should_notify_action(action)

    def execute_active_cycle(self) -> str:
        current_price = self.client.fetch_last_price(self.symbol)
        self.client.cancel_all_orders(self.symbol)

        if self.risk_manager is not None:
            allowed, _reason = self.risk_manager.can_open_new_position(self.symbol, 0.0, strategy_name=self.strategy_name)
            if not allowed:
                return "grid:risk_blocked"

        bounds = self._compute_dynamic_bounds()
        if bounds is None:
            return "grid:no_bands"
        lower_bound, upper_bound = bounds
        layers = self._build_layers(current_price, lower_bound, upper_bound)

        placed_orders = 0
        cooled_layers = 0
        for layer in layers:
            self._register_layer_trigger(layer, current_price)
            if self._layer_on_cooldown(layer.index):
                cooled_layers += 1
                continue
            placed_orders += self._try_place_limit_order(layer)

        placed_orders += self._place_rebalance_order(current_price, upper_bound)
        self.last_price = current_price

        if placed_orders <= 0 and self.risk_manager is not None:
            return "grid:risk_blocked"
        if placed_orders <= 0:
            return "grid:no_orders"
        return f"grid:placed_{placed_orders}_orders@{current_price:.2f}|cooldown={cooled_layers}"

    def _compute_dynamic_bounds(self) -> tuple[float, float] | None:
        ohlcv = self.client.fetch_ohlcv(
            symbol=self.symbol,
            timeframe=self.config.timeframe,
            limit=self.config.lookback_limit,
        )
        if len(ohlcv) < self.config.bollinger_period + 2:
            current_price = self.client.fetch_last_price(self.symbol)
            return (
                current_price * (1 - self.config.range_percent),
                current_price * (1 + self.config.range_percent),
            )

        frame = ohlcv_to_dataframe(ohlcv)
        bands = compute_bollinger_bands(frame, length=self.config.bollinger_period, std=self.config.bollinger_std)
        upper = float(bands["upper"].iloc[-1])
        lower = float(bands["lower"].iloc[-1])
        if upper <= 0 or lower <= 0 or upper <= lower:
            return None
        return lower, upper

    def _build_layers(self, current_price: float, lower_bound: float, upper_bound: float) -> list[GridLayer]:
        levels_per_side = max(1, self.config.levels // 2)
        buy_step = max(1e-12, (current_price - lower_bound) / levels_per_side)
        sell_step = max(1e-12, (upper_bound - current_price) / levels_per_side)
        layers: list[GridLayer] = []

        for index in range(levels_per_side):
            factor = self.config.martingale_factor ** index
            buy_amount = self.execution_config.grid_order_size * factor
            sell_amount = self.execution_config.grid_order_size
            layers.append(GridLayer(index=index, side="buy", price=current_price - buy_step * (index + 1), amount=buy_amount))
            layers.append(GridLayer(index=index, side="sell", price=current_price + sell_step * (index + 1), amount=sell_amount))
        return layers

    def _try_place_limit_order(self, layer: GridLayer) -> int:
        amount = layer.amount
        if self.risk_manager is not None:
            requested_notional = self.client.estimate_notional(self.symbol, amount, layer.price)
            allowed, _reason = self.risk_manager.check_symbol_notional_limit(self.symbol, requested_notional)
            if not allowed:
                return 0

        self.client.place_limit_order(self.symbol, layer.side, amount, layer.price)
        return 1

    def _register_layer_trigger(self, layer: GridLayer, current_price: float) -> None:
        if self.last_price is None:
            return
        crossed = (self.last_price - layer.price) * (current_price - layer.price) <= 0
        if not crossed:
            return
        now = datetime.now(timezone.utc).timestamp()
        history = self.layer_triggers[layer.index]
        history.append(now)
        while history and now - history[0] > self.config.trigger_window_seconds:
            history.popleft()

    def _layer_on_cooldown(self, layer_index: int) -> bool:
        history = self.layer_triggers[layer_index]
        if len(history) < self.config.trigger_limit_per_layer:
            return False
        return history[-1] - history[0] <= self.config.trigger_window_seconds

    def _place_rebalance_order(self, current_price: float, upper_bound: float) -> int:
        if not hasattr(self.client, "fetch_positions"):
            return 0
        positions = self.client.fetch_positions([self.symbol])
        long_size = 0.0
        short_size = 0.0
        for position in positions:
            raw = position.get("contracts") or position.get("info", {}).get("pos") or 0.0
            size = abs(float(raw or 0.0))
            if size <= 0:
                continue
            side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if side == "short":
                short_size += size
            else:
                long_size += size

        total = long_size + short_size
        if total <= 0:
            return 0
        long_ratio = long_size / total
        if long_ratio < self.config.rebalance_threshold_ratio:
            return 0

        rebalance_amount = max((long_size - short_size) / 2.0, 0.0)
        if rebalance_amount <= 0:
            return 0
        price = max(current_price, upper_bound)
        self.client.place_limit_order(self.symbol, "sell", rebalance_amount, price, reduce_only=True)
        return 1
