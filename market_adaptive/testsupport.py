from __future__ import annotations


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.trade_calls: list[dict] = []
        self.profit_calls: list[dict] = []
        self.market_shift_calls: list[dict] = []
        self.error_calls: list[dict] = []

    def send(self, title: str, message: str) -> bool:
        self.messages.append((title, message))
        return True

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str) -> bool:
        self.trade_calls.append(
            {
                "side": side,
                "price": price,
                "size": size,
                "strategy": strategy,
                "signal": signal,
            }
        )
        return True

    def notify_profit(self, pnl: float, roi: float, balance: float, **kwargs) -> bool:
        self.profit_calls.append({"pnl": pnl, "roi": roi, "balance": balance, **kwargs})
        self.messages.append(("已实现盈亏更新", f"pnl={pnl} roi={roi} balance={balance}"))
        return True

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        self.market_shift_calls.append({"old_state": old_state, "new_state": new_state, "reason": reason})
        self.messages.append(("市场状态已切换", f"{old_state}->{new_state}\n{reason}"))
        return True

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        self.error_calls.append({"error_msg": error_msg, "traceback": traceback, "module_name": module_name})
        self.messages.append(("运行异常", error_msg))
        return True

