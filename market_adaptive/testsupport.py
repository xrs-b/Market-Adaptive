from __future__ import annotations


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.trade_calls: list[dict] = []
        self.profit_calls: list[dict] = []
        self.market_shift_calls: list[dict] = []
        self.error_calls: list[dict] = []
        self.near_miss_calls: list[dict] = []
        self.signal_profiler_summary_calls: list[dict] = []
        self.strategy_cleanup_calls: list[dict] = []
        self._pending_strategy_cleanup: dict[tuple[str, str], dict[str, dict]] = {}

    def send(self, title: str, message: str) -> bool:
        self.messages.append((title, message))
        return True

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str, **kwargs) -> bool:
        self.trade_calls.append(
            {
                "side": side,
                "price": price,
                "size": size,
                "strategy": strategy,
                "signal": signal,
                **kwargs,
            }
        )
        return True

    def notify_profit(self, pnl: float, roi: float, balance: float, **kwargs) -> bool:
        self.profit_calls.append({"pnl": pnl, "roi": roi, "balance": balance, **kwargs})
        self.messages.append(("已实现盈亏更新", f"pnl={pnl} roi={roi} balance={balance}"))
        return True

    def notify_strategy_cleanup(self, *, strategy: str, symbol: str, reason: str, result: str, overview: str | None = None) -> bool:
        entry = {"strategy": strategy, "symbol": symbol, "reason": reason, "result": result, "overview": overview}
        self.strategy_cleanup_calls.append(entry)
        bucket = self._pending_strategy_cleanup.setdefault((symbol, reason), {})
        bucket[str(strategy).lower()] = entry
        return True

    def flush_strategy_cleanup_notifications(self) -> None:
        for (symbol, reason), bucket in list(self._pending_strategy_cleanup.items()):
            strategies = [bucket[name] for name in sorted(bucket)]
            if len(strategies) > 1:
                strategy_names = '、'.join(item['strategy'] for item in strategies)
                strategy_results = '；'.join(f"{item['strategy']}={item['result']}" for item in strategies)
                message = (
                    "检测到市场状态切换，相关策略已完成本轮切换清理。\n"
                    f"交易对：{symbol}\n"
                    f"切换原因：{reason}\n"
                    f"清理策略：{strategy_names}\n"
                    f"清理结果：{strategy_results}"
                )
            else:
                item = strategies[0]
                message = (
                    f"{item['overview'] or '检测到市场状态切换，相关策略已完成本轮切换清理。'}\n"
                    f"交易对：{symbol}\n"
                    f"切换原因：{reason}\n"
                    f"清理策略：{item['strategy']}\n"
                    f"清理结果：{item['result']}"
                )
            self.messages.append(("策略切换清理", message))
            del self._pending_strategy_cleanup[(symbol, reason)]

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        self.market_shift_calls.append({"old_state": old_state, "new_state": new_state, "reason": reason})
        self.messages.append(("市场状态已切换", f"{old_state}->{new_state}\n{reason}"))
        return True

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        self.error_calls.append({"error_msg": error_msg, "traceback": traceback, "module_name": module_name})
        self.messages.append(("运行异常", error_msg))
        return True

    def notify_cta_near_miss_report(self, *, symbol: str, samples: list[object], window_seconds: float) -> bool:
        self.near_miss_calls.append({"symbol": symbol, "samples": samples, "window_seconds": window_seconds})
        self.messages.append(("CTA 近失报告", f"symbol={symbol} count={len(samples)} window={window_seconds}"))
        return True

    def notify_signal_profiler_summary(self, *, symbol: str, summary_interval: int, summary: dict) -> bool:
        self.signal_profiler_summary_calls.append({"symbol": symbol, "summary_interval": summary_interval, "summary": summary})
        self.messages.append(("CTA 信号漏斗摘要", f"symbol={symbol} interval={summary_interval} trigger={summary.get('passed_trigger', 0)}/{summary.get('window_cycles', 0)}"))
        return True

