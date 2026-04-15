from __future__ import annotations

import asyncio
import inspect
import logging
import socket
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from typing import Any

import aiohttp

try:
    import psutil
except ImportError:  # pragma: no cover - optional runtime dependency fallback
    psutil = None

from market_adaptive.config import DiscordNotificationConfig

logger = logging.getLogger(__name__)

EMBED_COLOR_GOOD = 0x00FF00
EMBED_COLOR_WARN = 0xFFFF00
EMBED_COLOR_ERROR = 0xFF0000


@dataclass
class TradeAggregationBucket:
    side: str
    strategy: str
    signal: str
    symbol: str
    started_at: datetime
    flush_at: datetime
    trades: list[dict[str, Any]]


@dataclass
class ProfitAggregationBucket:
    strategy: str
    symbol: str
    started_at: datetime
    flush_at: datetime
    profits: list[dict[str, Any]]




@dataclass
class StrategyCleanupEntry:
    strategy: str
    symbol: str
    reason: str
    result: str
    overview: str | None = None


@dataclass
class StrategyCleanupBucket:
    symbol: str
    reason: str
    flush_at: datetime
    entries: dict[str, StrategyCleanupEntry] = field(default_factory=dict)


@dataclass
class CTANearMissPayload:
    symbol: str
    captured_at: float
    execution_trigger_reason: str
    execution_memory_active: bool
    execution_memory_bars_ago: int | None
    execution_breakout: bool
    execution_golden_cross: bool
    obv_zscore: float
    obv_threshold: float
    obv_gap: float
    price: float


class DiscordNotifier:
    def __init__(self, config: DiscordNotificationConfig) -> None:
        self.config = config
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._ready = Event()
        self._trade_buckets: dict[str, TradeAggregationBucket] = {}
        self._profit_buckets: dict[str, ProfitAggregationBucket] = {}
        self._cleanup_buckets: dict[str, StrategyCleanupBucket] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled and bool(self.config.webhook_url)

    def send(self, title: str, message: str) -> bool:
        if not self.enabled:
            return False
        color = EMBED_COLOR_WARN
        normalized = str(title).lower()
        if any(token in normalized for token in {"error", "stop", "risk", "错误", "风控"}):
            color = EMBED_COLOR_ERROR
        elif any(token in normalized for token in {"profit", "trade", "started", "stopped", "cleanup", "action", "盈利", "成交", "启动", "停止", "清理", "动作"}):
            color = EMBED_COLOR_GOOD
        payload = self._build_embed_payload(title=title, description=message, color=color)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str, *, symbol: str | None = None) -> bool:
        if not self.enabled:
            return False

        normalized_strategy = str(strategy or "unknown")
        normalized_signal = str(signal or "trade")
        trade = {
            "side": str(side).upper(),
            "price": float(price),
            "size": float(size),
            "notional": abs(float(price)) * abs(float(size)),
            "captured_at": datetime.now(timezone.utc),
        }

        if normalized_strategy.lower() == "grid" and "fill" in normalized_signal.lower():
            return self._queue_aggregated_grid_trade(normalized_strategy, normalized_signal, trade, symbol=str(symbol or "未知"))

        title = self._resolve_trade_title(normalized_strategy, normalized_signal)
        fields = [
            {"name": "交易对", "value": str(symbol or self._resolve_symbol_from_signal(normalized_signal)), "inline": True},
            {"name": "方向", "value": trade["side"], "inline": True},
            {"name": "策略", "value": self._display_strategy_name(normalized_strategy), "inline": True},
            {"name": "成交价", "value": f"{trade['price']:.4f}", "inline": True},
            {"name": "成交量", "value": f"{trade['size']:.8f}", "inline": True},
            {"name": "成交额", "value": f"{trade['notional']:.4f} USDT", "inline": True},
            {"name": "触发信号", "value": normalized_signal, "inline": False},
        ]
        payload = self._build_embed_payload(title=title, description="订单已成交，请留意仓位变化与后续管理动作。", color=EMBED_COLOR_GOOD, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_profit(
        self,
        pnl: float,
        roi: float,
        balance: float,
        *,
        strategy: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        exit_price: float | None = None,
        size: float | None = None,
    ) -> bool:
        if not self.enabled:
            return False
        normalized_strategy = str(strategy or "cta")
        if normalized_strategy.lower() == "grid":
            return self._queue_aggregated_grid_profit(
                pnl=float(pnl),
                roi=float(roi),
                balance=float(balance),
                strategy=normalized_strategy,
                symbol=str(symbol or "未知"),
                side=str(side or "").upper(),
                exit_price=exit_price,
                size=size,
            )
        strategy_label = self._display_strategy_name(normalized_strategy)
        title = f"{self._display_strategy_title(normalized_strategy)} 已实现{'盈利' if float(pnl) >= 0 else '亏损'}"
        fields = [
            {"name": "交易对", "value": str(symbol or "未知"), "inline": True},
            {"name": "策略", "value": strategy_label, "inline": True},
            {"name": "方向", "value": str(side or "-").upper() or "-", "inline": True},
            {"name": "本次盈亏", "value": f"{float(pnl):+.4f} USDT", "inline": True},
            {"name": "收益率", "value": f"{float(roi):+.2f}%", "inline": True},
            {"name": "账户权益", "value": f"{float(balance):.4f} USDT", "inline": True},
        ]
        if exit_price not in (None, ""):
            fields.append({"name": "平仓价", "value": f"{float(exit_price):.4f}", "inline": True})
        if size not in (None, ""):
            fields.append({"name": "平仓量", "value": f"{abs(float(size)):.8f}", "inline": True})
        payload = self._build_embed_payload(title=title, description="仓位已部分或全部平仓，以下为最新已实现盈亏。", color=EMBED_COLOR_GOOD if float(pnl) >= 0 else EMBED_COLOR_ERROR, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_strategy_cleanup(
        self,
        *,
        strategy: str,
        symbol: str,
        reason: str,
        result: str,
        overview: str | None = None,
    ) -> bool:
        if not self.enabled:
            return False
        normalized_reason = str(reason)
        if not normalized_reason.startswith("status_switch:"):
            payload = self._build_strategy_cleanup_payload(
                symbol=str(symbol),
                reason=normalized_reason,
                entries=[StrategyCleanupEntry(strategy=str(strategy), symbol=str(symbol), reason=normalized_reason, result=str(result), overview=overview)],
            )
            return self._submit_coroutine(self._post_payload(payload))

        bucket_key = f"{str(symbol).upper()}::{normalized_reason}"
        bucket = self._cleanup_buckets.get(bucket_key)
        now = datetime.now(timezone.utc)
        if bucket is None or now >= bucket.flush_at:
            bucket = StrategyCleanupBucket(symbol=str(symbol), reason=normalized_reason, flush_at=now + timedelta(seconds=1))
            self._cleanup_buckets[bucket_key] = bucket
            self._submit_coroutine(self._flush_cleanup_bucket_after_delay(bucket_key, 1.0))
        bucket.entries[str(strategy).lower()] = StrategyCleanupEntry(
            strategy=str(strategy),
            symbol=str(symbol),
            reason=normalized_reason,
            result=str(result),
            overview=overview,
        )
        return True

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        if not self.enabled:
            return False
        payload = self._build_embed_payload(
            title="市场状态已切换",
            description="检测到市场节奏变化，策略模式已同步更新。",
            color=EMBED_COLOR_WARN,
            fields=[
                {"name": "原状态", "value": str(old_state or "无"), "inline": True},
                {"name": "新状态", "value": str(new_state), "inline": True},
                {"name": "触发说明", "value": str(reason), "inline": False},
            ],
        )
        return self._submit_coroutine(self._post_payload(payload))

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        if not self.enabled:
            return False
        resolved_module = module_name or self._resolve_calling_module()
        description = str(error_msg)
        fields = [{"name": "模块", "value": resolved_module, "inline": True}]
        if traceback:
            fields.append({"name": "堆栈", "value": self._truncate(str(traceback), 1000), "inline": False})
        payload = self._build_embed_payload(title="运行异常", description=description, color=EMBED_COLOR_ERROR, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_cta_near_miss_report(self, *, symbol: str, samples: list[Any], window_seconds: float) -> bool:
        if not self.enabled or not samples:
            return False
        normalized_samples = [self._coerce_near_miss_sample(symbol=symbol, sample=sample) for sample in samples]
        closest = normalized_samples[0]
        fields = [
            {"name": "交易对", "value": str(symbol), "inline": True},
            {"name": "近失机会数", "value": str(len(normalized_samples)), "inline": True},
            {"name": "统计窗口", "value": self._format_window_seconds(window_seconds), "inline": True},
            {"name": "最接近样本", "value": f"OBV Z-Score {closest.obv_zscore:.2f} / 阈值 {closest.obv_threshold:.2f} / 差距 {closest.obv_gap:.2f}", "inline": False},
        ]
        detail_lines = []
        for index, sample in enumerate(normalized_samples, start=1):
            memory_suffix = "未激活"
            if sample.execution_memory_active:
                bars_ago = sample.execution_memory_bars_ago
                memory_suffix = f"激活（{bars_ago} 根前）" if bars_ago is not None else "激活"
            detail_lines.append(
                f"{index}. OBV Z-Score {sample.obv_zscore:.2f} / 阈值 {sample.obv_threshold:.2f} / 差距 {sample.obv_gap:.2f}\n"
                f"   执行触发：{sample.execution_trigger_reason}｜Memory：{memory_suffix}｜突破：{'是' if sample.execution_breakout else '否'}｜KDJ 金叉：{'是' if sample.execution_golden_cross else '否'}"
            )
        fields.append({"name": "样本详情", "value": self._truncate("\n".join(detail_lines), 1000), "inline": False})
        payload = self._build_embed_payload(
            title="CTA 近失报告",
            description="本小时 CTA 基础触发已接近成立，但被 OBV 强度确认拦住。可用来观察当前 OBV 阈值是否偏严。",
            color=EMBED_COLOR_WARN,
            fields=fields,
        )
        return self._submit_coroutine(self._post_payload(payload))

    def notify_signal_profiler_summary(self, *, symbol: str, summary_interval: int, summary: dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        blocker_lines = [f"{name} × {count}" for name, count in summary.get("top_blockers", [])]
        if not blocker_lines:
            blocker_lines = ["PASSED × 0"]
        dominant_label = self._format_signal_profiler_blocking_label(summary)
        dominant_count = max(0, int(summary.get("dominant_blocking_count", 0)))
        fields = [
            {"name": "当前主阻塞层", "value": f"{dominant_label}（{dominant_count} 次）", "inline": False},
            {"name": "交易对", "value": str(symbol), "inline": True},
            {"name": "统计窗口", "value": f"最近 {int(summary_interval)} 个 CTA 周期", "inline": True},
            {"name": "累计周期", "value": str(int(summary.get("total_cycles", 0))), "inline": True},
            {"name": "通过 Regime", "value": f"{int(summary.get('passed_regime', 0))}/{int(summary.get('window_cycles', 0))} ({float(summary.get('regime_pass_rate_pct', 0.0)):.1f}%)", "inline": True},
            {"name": "通过 Swing", "value": f"{int(summary.get('passed_swing', 0))}/{int(summary.get('window_cycles', 0))} ({float(summary.get('swing_pass_rate_pct', 0.0)):.1f}%)", "inline": True},
            {"name": "通过 Trigger", "value": f"{int(summary.get('passed_trigger', 0))}/{int(summary.get('window_cycles', 0))} ({float(summary.get('trigger_pass_rate_pct', 0.0)):.1f}%)", "inline": True},
            {"name": "最近 OBV 强度", "value": self._format_signal_profiler_obv(summary), "inline": True},
            {"name": "最近价格", "value": self._format_signal_profiler_price(summary.get('latest_execution_price')), "inline": True},
            {"name": "距离网格中心", "value": self._format_signal_profiler_gap(summary.get('latest_grid_center_gap')), "inline": True},
            {"name": "主要拦截原因", "value": self._truncate("\n".join(blocker_lines), 1000), "inline": False},
            {"name": "最近一次结果", "value": str(summary.get("latest_blocker_reason") or "PASSED"), "inline": False},
        ]
        payload = self._build_embed_payload(
            title=f"CTA 信号漏斗摘要｜主阻塞：{dominant_label}",
            description=f"按周期汇总 SignalProfiler 漏斗表现，当前主阻塞层：{dominant_label}。只推送关键统计，避免逐轮刷屏。",
            color=EMBED_COLOR_WARN,
            fields=fields,
        )
        return self._submit_coroutine(self._post_payload(payload))

    def close(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._loop = None

    async def _flush_cleanup_bucket_after_delay(self, bucket_key: str, delay_seconds: float) -> bool:
        await asyncio.sleep(max(0.0, delay_seconds))
        bucket = self._cleanup_buckets.pop(bucket_key, None)
        if bucket is None or not bucket.entries:
            return False
        ordered_entries = [bucket.entries[name] for name in sorted(bucket.entries)]
        payload = self._build_strategy_cleanup_payload(symbol=bucket.symbol, reason=bucket.reason, entries=ordered_entries)
        return await self._post_payload(payload)

    def _queue_aggregated_grid_trade(self, strategy: str, signal: str, trade: dict[str, Any], *, symbol: str) -> bool:
        now = datetime.now(timezone.utc)
        bucket_key = f"{strategy.lower()}::{signal.lower()}::{trade['side']}::{symbol}"
        bucket = self._trade_buckets.get(bucket_key)
        if bucket is None or now >= bucket.flush_at:
            bucket = TradeAggregationBucket(
                side=trade["side"],
                strategy=strategy,
                signal=signal,
                symbol=symbol,
                started_at=now,
                flush_at=now + timedelta(minutes=1),
                trades=[],
            )
            self._trade_buckets[bucket_key] = bucket
            self._submit_coroutine(self._flush_grid_trade_bucket_after_delay(bucket_key, 60.0))
        bucket.trades.append(trade)
        return True

    async def _flush_grid_trade_bucket_after_delay(self, bucket_key: str, delay_seconds: float) -> None:
        await asyncio.sleep(delay_seconds)
        bucket = self._trade_buckets.pop(bucket_key, None)
        if bucket is None or not bucket.trades:
            return

        total_size = sum(float(item["size"]) for item in bucket.trades)
        total_notional = sum(float(item["notional"]) for item in bucket.trades)
        avg_price = total_notional / total_size if total_size > 0 else 0.0
        latest_trade = bucket.trades[-1]
        fields = [
            {"name": "交易对", "value": bucket.symbol, "inline": True},
            {"name": "方向", "value": bucket.side, "inline": True},
            {"name": "成交笔数", "value": str(len(bucket.trades)), "inline": True},
            {"name": "统计窗口", "value": "60秒", "inline": True},
            {"name": "成交均价", "value": f"{avg_price:.4f}", "inline": True},
            {"name": "累计成交量", "value": f"{total_size:.8f}", "inline": True},
            {"name": "累计成交额", "value": f"{total_notional:.4f} USDT", "inline": True},
            {"name": "策略", "value": self._display_strategy_name(bucket.strategy), "inline": True},
            {"name": "触发信号", "value": bucket.signal, "inline": True},
            {"name": "最近成交时间", "value": self._format_timestamp(latest_trade["captured_at"]), "inline": True},
        ]
        payload = self._build_embed_payload(
            title=self._resolve_trade_title(bucket.strategy, bucket.signal, aggregated=True),
            description="过去 60 秒网格成交已合并展示，方便快速查看执行情况。",
            color=EMBED_COLOR_GOOD,
            fields=fields,
        )
        await self._post_payload(payload)

    def _queue_aggregated_grid_profit(
        self,
        *,
        pnl: float,
        roi: float,
        balance: float,
        strategy: str,
        symbol: str,
        side: str,
        exit_price: float | None,
        size: float | None,
    ) -> bool:
        now = datetime.now(timezone.utc)
        bucket_key = f"{strategy.lower()}::{symbol}"
        bucket = self._profit_buckets.get(bucket_key)
        if bucket is None or now >= bucket.flush_at:
            bucket = ProfitAggregationBucket(
                strategy=strategy,
                symbol=symbol,
                started_at=now,
                flush_at=now + timedelta(minutes=1),
                profits=[],
            )
            self._profit_buckets[bucket_key] = bucket
            self._submit_coroutine(self._flush_grid_profit_bucket_after_delay(bucket_key, 60.0))
        bucket.profits.append(
            {
                "pnl": pnl,
                "roi": roi,
                "balance": balance,
                "side": side,
                "exit_price": exit_price,
                "size": size,
                "captured_at": now,
            }
        )
        return True

    async def _flush_grid_profit_bucket_after_delay(self, bucket_key: str, delay_seconds: float) -> None:
        await asyncio.sleep(delay_seconds)
        bucket = self._profit_buckets.pop(bucket_key, None)
        if bucket is None or not bucket.profits:
            return

        total_pnl = sum(float(item["pnl"]) for item in bucket.profits)
        total_size = sum(abs(float(item.get("size") or 0.0)) for item in bucket.profits)
        last_balance = float(bucket.profits[-1]["balance"])
        avg_roi = sum(float(item["roi"]) for item in bucket.profits) / len(bucket.profits)
        latest = bucket.profits[-1]
        title = f"网格已实现{'盈利' if total_pnl >= 0 else '亏损'}"
        fields = [
            {"name": "策略", "value": self._display_strategy_name(bucket.strategy), "inline": True},
            {"name": "交易对", "value": bucket.symbol, "inline": True},
            {"name": "统计窗口", "value": "60秒", "inline": True},
            {"name": "平仓笔数", "value": str(len(bucket.profits)), "inline": True},
            {"name": "累计已实现盈亏", "value": f"{total_pnl:+.4f} USDT", "inline": True},
            {"name": "参考收益率", "value": f"{avg_roi:+.2f}%", "inline": True},
            {"name": "累计平仓量", "value": f"{total_size:.8f}", "inline": True},
            {"name": "最近平仓方向", "value": latest.get("side") or "-", "inline": True},
            {"name": "最近平仓价", "value": f"{float(latest['exit_price']):.4f}" if latest.get("exit_price") not in (None, "") else "-", "inline": True},
            {"name": "账户权益", "value": f"{last_balance:.4f} USDT", "inline": True},
        ]
        payload = self._build_embed_payload(
            title=title,
            description="过去 60 秒网格止盈/减仓结果已汇总，方便评估整体兑现表现。",
            color=EMBED_COLOR_GOOD if total_pnl >= 0 else EMBED_COLOR_ERROR,
            fields=fields,
        )
        await self._post_payload(payload)

    def _coerce_near_miss_sample(self, *, symbol: str, sample: Any) -> CTANearMissPayload:
        if isinstance(sample, CTANearMissPayload):
            return sample
        values = sample if isinstance(sample, dict) else vars(sample)
        return CTANearMissPayload(
            symbol=str(values.get("symbol") or symbol),
            captured_at=float(values.get("captured_at") or 0.0),
            execution_trigger_reason=str(values.get("execution_trigger_reason") or ""),
            execution_memory_active=bool(values.get("execution_memory_active")),
            execution_memory_bars_ago=values.get("execution_memory_bars_ago"),
            execution_breakout=bool(values.get("execution_breakout")),
            execution_golden_cross=bool(values.get("execution_golden_cross")),
            obv_zscore=float(values.get("obv_zscore") or 0.0),
            obv_threshold=float(values.get("obv_threshold") or 0.0),
            obv_gap=float(values.get("obv_gap") or 0.0),
            price=float(values.get("price") or 0.0),
        )

    def _format_window_seconds(self, seconds: float) -> str:
        total = max(0, int(seconds))
        if total % 3600 == 0 and total >= 3600:
            return f"{total // 3600} 小时"
        if total % 60 == 0 and total >= 60:
            return f"{total // 60} 分钟"
        return f"{total} 秒"

    def _format_signal_profiler_price(self, value: Any) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "暂无有效样本"
        if numeric <= 0:
            return "暂无有效样本"
        return f"{numeric:.4f}"

    def _format_signal_profiler_gap(self, value: Any) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "未进入执行层"
        return f"{numeric:+.4f}"

    def _format_signal_profiler_blocking_label(self, summary: dict[str, Any]) -> str:
        label = str(summary.get("dominant_blocking_label") or "").strip()
        if label:
            return label
        layer = str(summary.get("dominant_blocking_layer") or "PASSED").upper()
        fallback = {
            "REGIME": "Regime（趋势层）",
            "SWING": "Swing（摆动层）",
            "TRIGGER": "Trigger（触发层）",
            "OBV": "OBV（执行过滤层）",
            "DATA": "数据同步",
            "PASSED": "已通过",
        }
        return fallback.get(layer, layer or "已通过")

    def _format_signal_profiler_obv(self, summary: dict[str, Any]) -> str:
        try:
            zscore_raw = summary.get('latest_execution_obv_zscore')
            threshold_raw = summary.get('latest_execution_obv_threshold')
            if zscore_raw in (None, '') or threshold_raw in (None, ''):
                return '未进入执行层'
            zscore = float(zscore_raw)
            threshold = float(threshold_raw)
        except (TypeError, ValueError):
            return '未进入执行层'
        return f"{zscore:.2f} / 阈值 {threshold:.2f}"

    def _build_embed_payload(
        self,
        *,
        title: str,
        description: str,
        color: int,
        fields: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc)
        formatted_timestamp = self._format_timestamp(timestamp)
        embed = {
            "title": self._truncate(str(title), 256),
            "description": self._truncate(str(description), 4000),
            "color": int(color),
            "fields": fields or [],
            "footer": {
                "text": (
                    f"通知时间：{formatted_timestamp}\n"
                    f"运行时长：{self._get_uptime_text()}\n"
                    f"主机：{socket.gethostname()}"
                ),
            },
        }
        return {"username": self.config.username, "embeds": [embed]}

    async def _post_payload(self, payload: dict[str, Any]) -> bool:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.config.webhook_url,
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "Market-Adaptive/1.0",
                    },
                ) as response:
                    if 200 <= response.status < 300:
                        return True
                    body = await response.text()
                    logger.warning("Discord webhook failed with HTTP %s: %s", response.status, body)
                    return False
        except Exception as exc:  # pragma: no cover
            logger.warning("Discord notification unexpected failure: %s", exc)
            return False

    def _submit_coroutine(self, coro: asyncio.Future | asyncio.coroutines | Any) -> bool:
        try:
            loop = self._ensure_loop()
            asyncio.run_coroutine_threadsafe(coro, loop)
            return True
        except Exception as exc:  # pragma: no cover
            logger.warning("Discord notification scheduling failure: %s", exc)
            return False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None and self._thread is not None and self._thread.is_alive():
            return self._loop

        self._ready.clear()

        def runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._ready.set()
            loop.run_forever()
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

        self._thread = Thread(target=runner, daemon=True, name="discord-notifier")
        self._thread.start()
        self._ready.wait(timeout=2.0)
        assert self._loop is not None
        return self._loop

    def _resolve_calling_module(self) -> str:
        for frame_info in inspect.stack()[2:]:
            module = inspect.getmodule(frame_info.frame)
            if module is None:
                continue
            name = module.__name__
            if name != __name__:
                return name
        return __name__

    def _get_uptime_text(self) -> str:
        if psutil is not None:
            try:
                seconds = max(0, int(datetime.now(timezone.utc).timestamp() - psutil.boot_time()))
                return self._format_duration(seconds)
            except Exception:
                pass
        return "unknown"

    def _build_strategy_cleanup_payload(self, *, symbol: str, reason: str, entries: list[StrategyCleanupEntry]) -> dict[str, Any]:
        previous_status, new_status = self._parse_status_switch_reason(reason)
        strategy_names = [self._display_strategy_name(entry.strategy) for entry in entries]
        if len(entries) > 1:
            description = "检测到市场状态切换，相关策略已完成本轮切换清理，以下为合并概览。"
            strategy_value = "、".join(strategy_names)
        else:
            description = entries[0].overview or "检测到市场状态切换，相关策略已完成本轮切换清理。"
            strategy_value = strategy_names[0]
        result_lines = [f"{self._display_strategy_name(entry.strategy)}：{entry.result}" for entry in entries]
        transition_summary = (
            f"市场状态由 {previous_status or '未知'} 切换到 {new_status or '未知'}"
            if previous_status is not None or new_status is not None
            else str(reason)
        )
        fields = [
            {"name": "交易对", "value": str(symbol), "inline": True},
            {"name": "清理策略", "value": strategy_value, "inline": True},
            {"name": "触发说明", "value": transition_summary, "inline": False},
            {"name": "清理结果", "value": self._truncate("\n".join(result_lines), 1000), "inline": False},
        ]
        if previous_status is not None or new_status is not None:
            fields.insert(1, {"name": "状态切换", "value": f"{previous_status or '未知'} → {new_status or '未知'}", "inline": True})
        return self._build_embed_payload(title="策略切换清理", description=description, color=EMBED_COLOR_GOOD, fields=fields)

    def _resolve_trade_title(self, strategy: str, signal: str, *, aggregated: bool = False) -> str:
        normalized_strategy = str(strategy or "").strip().lower()
        normalized_signal = str(signal or "").strip().lower()
        strategy_title = self._display_strategy_title(normalized_strategy)

        if normalized_strategy == "cta":
            if normalized_signal.startswith("cta_open_"):
                return f"{strategy_title} 开仓成交"
            if normalized_signal.startswith("cta_close_") or "exit" in normalized_signal:
                return f"{strategy_title} 平仓成交"
            return f"{strategy_title} 成交回报"

        if normalized_strategy == "grid":
            if aggregated and "websocket" in normalized_signal:
                return f"{strategy_title} 对冲成交汇总"
            if "websocket" in normalized_signal:
                return f"{strategy_title} 对冲成交"
            return f"{strategy_title} 成交汇总" if aggregated else f"{strategy_title} 成交回报"

        return f"{strategy_title} 成交汇总" if aggregated else f"{strategy_title} 成交回报"

    def _parse_status_switch_reason(self, reason: str) -> tuple[str | None, str | None]:
        normalized = str(reason)
        prefix = "status_switch:"
        if not normalized.startswith(prefix):
            return None, None
        payload = normalized[len(prefix):]
        if "->" not in payload:
            return payload or None, None
        previous, current = payload.split("->", 1)
        return previous or None, current or None

    def _resolve_symbol_from_signal(self, signal: str) -> str:
        del signal
        return "未知"

    def _display_strategy_title(self, strategy: str | None) -> str:
        normalized = str(strategy or "").strip().lower()
        if normalized == "grid":
            return "Grid"
        if normalized == "cta":
            return "CTA"
        return str(strategy or "未知策略")

    def _display_strategy_name(self, strategy: str | None) -> str:
        normalized = str(strategy or "").strip().lower()
        if normalized == "grid":
            return "网格策略"
        if normalized == "cta":
            return "CTA 策略"
        return str(strategy or "未知策略")

    def _truncate(self, value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."

    def _format_duration(self, seconds: int) -> str:
        days, remainder = divmod(int(seconds), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours or parts:
            parts.append(f"{hours}h")
        if minutes or parts:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    def _format_timestamp(self, value: datetime) -> str:
        return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


class NullNotifier:
    def send(self, title: str, message: str) -> bool:
        logger.debug("Notification skipped: %s | %s", title, message)
        return False

    def notify_strategy_cleanup(self, *, strategy: str, symbol: str, reason: str, result: str, overview: str | None = None) -> bool:
        logger.debug("Strategy cleanup notification skipped: %s %s %s %s %s", strategy, symbol, reason, result, overview)
        return False

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str, **kwargs) -> bool:
        logger.debug("Trade notification skipped: %s %s %s %s %s %s", side, price, size, strategy, signal, kwargs)
        return False

    def notify_profit(self, pnl: float, roi: float, balance: float, **kwargs) -> bool:
        logger.debug("Profit notification skipped: %s %s %s %s", pnl, roi, balance, kwargs)
        return False

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        logger.debug("Market shift notification skipped: %s %s %s", old_state, new_state, reason)
        return False

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        logger.debug("Error notification skipped: %s | %s | %s", module_name, error_msg, traceback)
        return False

    def notify_cta_near_miss_report(self, *, symbol: str, samples: list[Any], window_seconds: float) -> bool:
        logger.debug("CTA near-miss notification skipped: %s %s %s", symbol, len(samples), window_seconds)
        return False

    def notify_signal_profiler_summary(self, *, symbol: str, summary_interval: int, summary: dict[str, Any]) -> bool:
        logger.debug("Signal profiler summary skipped: %s %s %s", symbol, summary_interval, summary)
        return False
