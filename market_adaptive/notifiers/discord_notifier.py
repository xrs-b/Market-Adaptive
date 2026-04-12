from __future__ import annotations

import asyncio
import inspect
import logging
import socket
from dataclasses import dataclass
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
    started_at: datetime
    flush_at: datetime
    trades: list[dict[str, Any]]


class DiscordNotifier:
    def __init__(self, config: DiscordNotificationConfig) -> None:
        self.config = config
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._ready = Event()
        self._trade_buckets: dict[str, TradeAggregationBucket] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled and bool(self.config.webhook_url)

    def send(self, title: str, message: str) -> bool:
        if not self.enabled:
            return False
        color = EMBED_COLOR_WARN
        normalized = str(title).lower()
        if any(token in normalized for token in {"error", "stop", "risk"}):
            color = EMBED_COLOR_ERROR
        elif any(token in normalized for token in {"profit", "trade", "started", "stopped", "cleanup", "action"}):
            color = EMBED_COLOR_GOOD
        payload = self._build_embed_payload(title=title, description=message, color=color)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str) -> bool:
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
            return self._queue_aggregated_grid_trade(normalized_strategy, normalized_signal, trade)

        title = f"{normalized_strategy.upper()} Trade Executed"
        fields = [
            {"name": "Side", "value": trade["side"], "inline": True},
            {"name": "Price", "value": f"{trade['price']:.4f}", "inline": True},
            {"name": "Size", "value": f"{trade['size']:.8f}", "inline": True},
            {"name": "Position Notional", "value": f"{trade['notional']:.4f} USDT", "inline": True},
            {"name": "Strategy", "value": normalized_strategy, "inline": True},
            {"name": "Signal", "value": normalized_signal, "inline": True},
        ]
        payload = self._build_embed_payload(title=title, description="Trade execution update", color=EMBED_COLOR_GOOD, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_profit(self, pnl: float, roi: float, balance: float) -> bool:
        if not self.enabled:
            return False
        title = "Net Profit Update"
        fields = [
            {"name": "Net Profit", "value": f"{float(pnl):+.4f} USDT", "inline": True},
            {"name": "ROI", "value": f"{float(roi):+.2f}%", "inline": True},
            {"name": "Balance", "value": f"{float(balance):.4f} USDT", "inline": True},
        ]
        payload = self._build_embed_payload(title=title, description="Portfolio performance snapshot", color=EMBED_COLOR_GOOD, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        if not self.enabled:
            return False
        payload = self._build_embed_payload(
            title="Market Regime Shift",
            description="Adaptive market state transition detected",
            color=EMBED_COLOR_WARN,
            fields=[
                {"name": "From", "value": str(old_state or "none"), "inline": True},
                {"name": "To", "value": str(new_state), "inline": True},
                {"name": "Reason", "value": str(reason), "inline": False},
            ],
        )
        return self._submit_coroutine(self._post_payload(payload))

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        if not self.enabled:
            return False
        resolved_module = module_name or self._resolve_calling_module()
        description = str(error_msg)
        fields = [{"name": "Module", "value": resolved_module, "inline": True}]
        if traceback:
            fields.append({"name": "Traceback", "value": self._truncate(str(traceback), 1000), "inline": False})
        payload = self._build_embed_payload(title="Runtime Error", description=description, color=EMBED_COLOR_ERROR, fields=fields)
        return self._submit_coroutine(self._post_payload(payload))

    def close(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._loop = None

    def _queue_aggregated_grid_trade(self, strategy: str, signal: str, trade: dict[str, Any]) -> bool:
        now = datetime.now(timezone.utc)
        bucket_key = f"{strategy.lower()}::{signal.lower()}::{trade['side']}"
        bucket = self._trade_buckets.get(bucket_key)
        if bucket is None or now >= bucket.flush_at:
            bucket = TradeAggregationBucket(
                side=trade["side"],
                strategy=strategy,
                signal=signal,
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
            {"name": "Side", "value": bucket.side, "inline": True},
            {"name": "Fills", "value": str(len(bucket.trades)), "inline": True},
            {"name": "Window", "value": "60s", "inline": True},
            {"name": "Average Price", "value": f"{avg_price:.4f}", "inline": True},
            {"name": "Total Size", "value": f"{total_size:.8f}", "inline": True},
            {"name": "Total Notional", "value": f"{total_notional:.4f} USDT", "inline": True},
            {"name": "Strategy", "value": bucket.strategy, "inline": True},
            {"name": "Signal", "value": bucket.signal, "inline": True},
            {"name": "Last Fill", "value": latest_trade["captured_at"].isoformat(), "inline": True},
        ]
        payload = self._build_embed_payload(
            title=f"{bucket.strategy.upper()} Grid Fill Summary",
            description="Aggregated grid fills to reduce Discord noise",
            color=EMBED_COLOR_GOOD,
            fields=fields,
        )
        await self._post_payload(payload)

    def _build_embed_payload(
        self,
        *,
        title: str,
        description: str,
        color: int,
        fields: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc)
        embed = {
            "title": self._truncate(str(title), 256),
            "description": self._truncate(str(description), 4000),
            "color": int(color),
            "fields": fields or [],
            "footer": {
                "text": f"{timestamp.isoformat()} | uptime {self._get_uptime_text()} | host {socket.gethostname()}",
            },
            "timestamp": timestamp.isoformat(),
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


class NullNotifier:
    def send(self, title: str, message: str) -> bool:
        logger.debug("Notification skipped: %s | %s", title, message)
        return False

    def notify_trade(self, side: str, price: float, size: float, strategy: str, signal: str) -> bool:
        logger.debug("Trade notification skipped: %s %s %s %s %s", side, price, size, strategy, signal)
        return False

    def notify_profit(self, pnl: float, roi: float, balance: float) -> bool:
        logger.debug("Profit notification skipped: %s %s %s", pnl, roi, balance)
        return False

    def notify_market_shift(self, old_state: str | None, new_state: str, reason: str) -> bool:
        logger.debug("Market shift notification skipped: %s %s %s", old_state, new_state, reason)
        return False

    def notify_error(self, error_msg: str, traceback: str | None = None, module_name: str | None = None) -> bool:
        logger.debug("Error notification skipped: %s | %s | %s", module_name, error_msg, traceback)
        return False
