from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.db import DatabaseInitializer, StrategyRuntimeState

logger = logging.getLogger(__name__)


@dataclass
class StrategyRunResult:
    strategy_name: str
    status: str
    active: bool
    action: str


class BaseStrategyRobot:
    strategy_name = "base"
    activation_status = ""

    def __init__(
        self,
        client: OKXClient,
        database: DatabaseInitializer,
        symbol: str,
        notifier: Any | None = None,
    ) -> None:
        self.client = client
        self.database = database
        self.symbol = symbol
        self.notifier = notifier

    def run(self) -> StrategyRunResult:
        market_status = self.database.fetch_latest_market_status(self.symbol)
        if market_status is None:
            return StrategyRunResult(self.strategy_name, "unknown", False, "skip:no_market_status")

        previous_state = self.database.get_strategy_runtime_state(self.strategy_name, self.symbol)
        previous_status = previous_state.last_status if previous_state is not None else None
        current_status = market_status.status

        if previous_status is not None and previous_status != current_status:
            self.flatten_and_cancel_all(reason=f"status_switch:{previous_status}->{current_status}")

        action = "skip:inactive"
        active = current_status == self.activation_status
        if active:
            action = self.execute_active_cycle()
            if self.should_notify_action(action):
                self._notify_action(action, current_status)

        self.database.upsert_strategy_runtime_state(
            StrategyRuntimeState(
                strategy_name=self.strategy_name,
                symbol=self.symbol,
                last_status=current_status,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        return StrategyRunResult(self.strategy_name, current_status, active, action)

    def flatten_and_cancel_all(self, reason: str) -> None:
        logger.info("%s flatten start: symbol=%s reason=%s", self.strategy_name, self.symbol, reason)
        self.client.cancel_all_orders(self.symbol)
        self.client.close_all_positions(self.symbol)
        logger.info("%s flatten done: symbol=%s", self.strategy_name, self.symbol)
        if self.notifier is not None:
            self.notifier.send(
                "Strategy Cleanup",
                f"strategy={self.strategy_name} | symbol={self.symbol} | reason={reason}",
            )

    def execute_active_cycle(self) -> str:
        raise NotImplementedError

    def should_notify_action(self, action: str) -> bool:
        if action == "skip:inactive":
            return False
        if action.startswith("grid:placed_0"):
            return False
        return True

    def _notify_action(self, action: str, status: str) -> None:
        if self.notifier is None:
            return
        self.notifier.send(
            "Strategy Action",
            f"strategy={self.strategy_name} | symbol={self.symbol} | status={status} | action={action}",
        )
