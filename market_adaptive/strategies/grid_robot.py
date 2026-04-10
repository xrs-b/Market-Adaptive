from __future__ import annotations

from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.strategies.base import BaseStrategyRobot


class GridRobot(BaseStrategyRobot):
    strategy_name = "grid"
    activation_status = "sideways"

    def __init__(self, client, database, config: GridConfig, execution_config: ExecutionConfig) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol)
        self.config = config
        self.execution_config = execution_config

    def execute_active_cycle(self) -> str:
        current_price = self.client.fetch_last_price(self.symbol)
        self.client.cancel_all_orders(self.symbol)

        lower_bound = current_price * (1 - self.config.range_percent)
        upper_bound = current_price * (1 + self.config.range_percent)
        half_levels = self.config.levels // 2
        step = (current_price - lower_bound) / half_levels

        for index in range(half_levels):
            buy_price = current_price - step * (index + 1)
            sell_price = current_price + step * (index + 1)
            self.client.place_limit_order(
                self.symbol,
                "buy",
                self.execution_config.grid_order_size,
                buy_price,
            )
            self.client.place_limit_order(
                self.symbol,
                "sell",
                self.execution_config.grid_order_size,
                sell_price,
            )

        return f"grid:placed_{half_levels * 2}_orders@{current_price:.2f}"
