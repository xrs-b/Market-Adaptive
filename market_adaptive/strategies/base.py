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

    def _is_active_for_status(self, current_status: str) -> bool:
        statuses = getattr(self, "activation_statuses", None)
        if statuses is not None:
            return current_status in set(statuses)
        return current_status == self.activation_status

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
        active = self._is_active_for_status(current_status)
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
        result = f"{self.strategy_name}:flatten_all"
        logger.info("%s flatten done: symbol=%s", self.strategy_name, self.symbol)
        if self.notifier is not None:
            if hasattr(self.notifier, "notify_strategy_cleanup"):
                self.notifier.notify_strategy_cleanup(
                    strategy=self.strategy_name,
                    symbol=self.symbol,
                    reason=reason,
                    result=result,
                    overview="策略已完成状态切换清理。",
                )
            else:
                self.notifier.send(
                    "策略清理完成",
                    f"strategy={self.strategy_name} | symbol={self.symbol} | reason={reason} | result={result}",
                )

    def execute_active_cycle(self) -> str:
        raise NotImplementedError

    def should_notify_action(self, action: str) -> bool:
        del action
        return False

    def _notify_action(self, action: str, status: str) -> None:
        if self.notifier is None:
            return
        strategy_label = self.strategy_name.upper() if self.strategy_name.lower() == "cta" else self.strategy_name.capitalize()
        self.notifier.send(
            f"{strategy_label} 执行动作",
            (
                f"交易对：{self.symbol}\n"
                f"策略：{strategy_label}\n"
                f"市场状态：{status}\n"
                f"执行结果：{action}"
            ),
        )
