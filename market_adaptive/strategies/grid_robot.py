from __future__ import annotations

from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.strategies.base import BaseStrategyRobot


class GridRobot(BaseStrategyRobot):
    strategy_name = "grid"
    activation_status = "sideways"

    def __init__(self, client, database, config: GridConfig, execution_config: ExecutionConfig, notifier=None, risk_manager=None) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager

    def should_notify_action(self, action: str) -> bool:
        if action == "grid:risk_blocked":
            return False
        return super().should_notify_action(action)

    def execute_active_cycle(self) -> str:
        current_price = self.client.fetch_last_price(self.symbol)
        self.client.cancel_all_orders(self.symbol)

        if self.risk_manager is not None:
            allowed, _reason = self.risk_manager.can_open_new_position(self.symbol, 0.0, strategy_name=self.strategy_name)
            if not allowed:
                return "grid:risk_blocked"

        lower_bound = current_price * (1 - self.config.range_percent)
        upper_bound = current_price * (1 + self.config.range_percent)
        half_levels = self.config.levels // 2
        step = (current_price - lower_bound) / half_levels

        placed_orders = 0
        for index in range(half_levels):
            buy_price = current_price - step * (index + 1)
            sell_price = current_price + step * (index + 1)
            placed_orders += self._try_place_limit_order("buy", buy_price)
            placed_orders += self._try_place_limit_order("sell", sell_price)

        if placed_orders <= 0 and self.risk_manager is not None:
            return "grid:risk_blocked"
        return f"grid:placed_{placed_orders}_orders@{current_price:.2f}"

    def _try_place_limit_order(self, side: str, price: float) -> int:
        amount = self.execution_config.grid_order_size
        if self.risk_manager is not None:
            requested_notional = self.client.estimate_notional(self.symbol, amount, price)
            allowed, _reason = self.risk_manager.check_symbol_notional_limit(self.symbol, requested_notional)
            if not allowed:
                return 0

        self.client.place_limit_order(
            self.symbol,
            side,
            amount,
            price,
        )
        return 1
