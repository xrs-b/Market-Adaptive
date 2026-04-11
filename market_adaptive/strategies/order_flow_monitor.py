from __future__ import annotations

import asyncio
import logging
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol

from market_adaptive.clients.okx_ws_client import (
    CCXTProUnavailableError,
    OKXCCXTProWebsocketClient,
)
from market_adaptive.config import OKXConfig

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


class AsyncOrderBookWatcher(Protocol):
    async def watch_order_book(self, symbol: str, limit: int | None = None) -> Mapping[str, Any]: ...

    async def close(self) -> Any: ...


class OKXCCXTProOrderBookWatcher(OKXCCXTProWebsocketClient):
    """Dedicated alias for the order-book watcher path used by strategies."""


def build_okx_order_book_watcher(config: OKXConfig) -> OKXCCXTProOrderBookWatcher:
    return OKXCCXTProOrderBookWatcher(config)


@dataclass(frozen=True)
class OrderFlowSnapshot:
    symbol: str
    depth_levels: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    bid_sum: float
    ask_sum: float
    obi: float
    best_bid: float | None
    best_ask: float | None
    largest_bid_level_size: float
    largest_ask_level_size: float
    largest_bid_level_share: float
    largest_ask_level_share: float
    updated_at: float

    @property
    def total_depth(self) -> float:
        return self.bid_sum + self.ask_sum

    def has_large_wall(self, side: str, wall_ratio: float = 0.40) -> bool:
        normalized_side = str(side).strip().lower()
        if normalized_side == "buy":
            return self.largest_ask_level_share > float(wall_ratio)
        if normalized_side == "sell":
            return self.largest_bid_level_share > float(wall_ratio)
        raise ValueError(f"Unsupported signal side: {side}")

    def to_order_book(self) -> dict[str, list[list[float]]]:
        return {
            "bids": [[price, size] for price, size in self.bids],
            "asks": [[price, size] for price, size in self.asks],
        }


@dataclass(frozen=True)
class OrderFlowValidation:
    side: str
    threshold: float
    wall_ratio: float
    valid: bool
    reason: str
    snapshot: OrderFlowSnapshot | None

    @property
    def has_snapshot(self) -> bool:
        return self.snapshot is not None


class OrderFlowMonitor:
    """Async websocket order-flow monitor with OBI and opposite-wall interception.

    Designed to sit on its own asyncio task (or a dedicated daemon thread with a private
    event loop) so the existing threaded robots can keep running without blocking.
    """

    def __init__(
        self,
        watcher: AsyncOrderBookWatcher,
        *,
        symbol: str = "BTC/USDT",
        depth_levels: int = 20,
        signal_threshold: float = 0.5,
        wall_ratio: float = 0.40,
        reconnect_delay_seconds: float = 3.0,
        log_interval_seconds: float = 3.0,
        logger: logging.Logger | logging.LoggerAdapter | None = None,
        watcher_factory: Callable[[], AsyncOrderBookWatcher] | None = None,
    ) -> None:
        self.watcher = watcher
        self.symbol = symbol
        self.depth_levels = max(1, int(depth_levels))
        self.signal_threshold = max(0.0, float(signal_threshold))
        self.wall_ratio = max(0.0, float(wall_ratio))
        self.reconnect_delay_seconds = max(0.1, float(reconnect_delay_seconds))
        self.log_interval_seconds = max(0.0, float(log_interval_seconds))
        self.logger = logger or logging.LoggerAdapter(logging.getLogger(__name__), {"robot": "order_flow"})
        self.watcher_factory = watcher_factory

        self._snapshot: OrderFlowSnapshot | None = None
        self._runner_task: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._background_thread: threading.Thread | None = None
        self._last_log_at = 0.0
        self._last_log_signature: tuple[str, str] | None = None
        self._pending_log_signature: tuple[str, str] | None = None
        self._pending_log_since = 0.0
        self._closed = False

    @property
    def snapshot(self) -> OrderFlowSnapshot | None:
        return self._snapshot

    @property
    def is_running(self) -> bool:
        return self._runner_task is not None and not self._runner_task.done()

    def latest_order_book(self) -> dict[str, list[list[float]]] | None:
        if self._snapshot is None:
            return None
        return self._snapshot.to_order_book()

    def ingest_order_book(self, order_book: Mapping[str, Any]) -> OrderFlowSnapshot:
        bids = self._normalize_levels(order_book.get("bids"), self.depth_levels)
        asks = self._normalize_levels(order_book.get("asks"), self.depth_levels)
        bid_sum = sum(size for _, size in bids)
        ask_sum = sum(size for _, size in asks)
        total = bid_sum + ask_sum
        obi = (bid_sum - ask_sum) / total if total > 0 else 0.0

        largest_bid_level_size = max((size for _, size in bids), default=0.0)
        largest_ask_level_size = max((size for _, size in asks), default=0.0)
        largest_bid_level_share = largest_bid_level_size / bid_sum if bid_sum > 0 else 0.0
        largest_ask_level_share = largest_ask_level_size / ask_sum if ask_sum > 0 else 0.0

        snapshot = OrderFlowSnapshot(
            symbol=self.symbol,
            depth_levels=self.depth_levels,
            bids=tuple(bids),
            asks=tuple(asks),
            bid_sum=bid_sum,
            ask_sum=ask_sum,
            obi=obi,
            best_bid=bids[0][0] if bids else None,
            best_ask=asks[0][0] if asks else None,
            largest_bid_level_size=largest_bid_level_size,
            largest_ask_level_size=largest_ask_level_size,
            largest_bid_level_share=largest_bid_level_share,
            largest_ask_level_share=largest_ask_level_share,
            updated_at=time.time(),
        )
        self._snapshot = snapshot
        self._emit_state_log(snapshot)
        return snapshot

    def validate_signal(
        self,
        side: str,
        *,
        threshold: float | None = None,
        wall_ratio: float | None = None,
    ) -> OrderFlowValidation:
        snapshot = self._snapshot
        normalized_side = str(side).strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported signal side: {side}")

        effective_threshold = self.signal_threshold if threshold is None else max(0.0, float(threshold))
        effective_wall_ratio = self.wall_ratio if wall_ratio is None else max(0.0, float(wall_ratio))
        if snapshot is None:
            return OrderFlowValidation(
                side=normalized_side,
                threshold=effective_threshold,
                wall_ratio=effective_wall_ratio,
                valid=False,
                reason="snapshot_unavailable",
                snapshot=None,
            )

        if normalized_side == "buy":
            if snapshot.obi < effective_threshold:
                return OrderFlowValidation(
                    side=normalized_side,
                    threshold=effective_threshold,
                    wall_ratio=effective_wall_ratio,
                    valid=False,
                    reason="obi_below_threshold",
                    snapshot=snapshot,
                )
            if snapshot.has_large_wall("buy", effective_wall_ratio):
                return OrderFlowValidation(
                    side=normalized_side,
                    threshold=effective_threshold,
                    wall_ratio=effective_wall_ratio,
                    valid=False,
                    reason="ask_wall_detected",
                    snapshot=snapshot,
                )
        else:
            if snapshot.obi > -effective_threshold:
                return OrderFlowValidation(
                    side=normalized_side,
                    threshold=effective_threshold,
                    wall_ratio=effective_wall_ratio,
                    valid=False,
                    reason="obi_above_threshold",
                    snapshot=snapshot,
                )
            if snapshot.has_large_wall("sell", effective_wall_ratio):
                return OrderFlowValidation(
                    side=normalized_side,
                    threshold=effective_threshold,
                    wall_ratio=effective_wall_ratio,
                    valid=False,
                    reason="bid_wall_detected",
                    snapshot=snapshot,
                )

        return OrderFlowValidation(
            side=normalized_side,
            threshold=effective_threshold,
            wall_ratio=effective_wall_ratio,
            valid=True,
            reason="confirmed",
            snapshot=snapshot,
        )

    def is_signal_valid(self, side: str, threshold: float | None = None) -> bool:
        return self.validate_signal(side, threshold=threshold).valid

    async def start(self) -> asyncio.Task[Any]:
        if self.is_running:
            assert self._runner_task is not None
            return self._runner_task
        self._loop = asyncio.get_running_loop()
        self._runner_task = asyncio.create_task(self.run_forever(), name=f"order-flow:{self.symbol}")
        return self._runner_task

    async def stop(self) -> None:
        runner = self._runner_task
        if runner is None:
            await self._close_watcher()
            return
        if runner is asyncio.current_task():
            return
        runner.cancel()
        with suppress(asyncio.CancelledError):
            await runner

    async def run_forever(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._runner_task = asyncio.current_task()
        self._closed = False
        self.logger.info(
            "OrderFlowMonitor started | symbol=%s depth=%s threshold=%.3f wall_ratio=%.2f",
            self.symbol,
            self.depth_levels,
            self.signal_threshold,
            self.wall_ratio,
        )
        reconnect_attempt = 0
        try:
            while True:
                try:
                    order_book = await self.watcher.watch_order_book(self.symbol, self.depth_levels)
                    self.ingest_order_book(order_book)
                    reconnect_attempt = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reconnect_attempt += 1
                    delay = self._compute_reconnect_delay(reconnect_attempt)
                    self.logger.warning(
                        "OrderFlowMonitor websocket error | symbol=%s error=%s reconnect_in=%.1fs attempt=%s",
                        self.symbol,
                        exc,
                        delay,
                        reconnect_attempt,
                    )
                    if self.watcher_factory is not None:
                        try:
                            await self._replace_watcher()
                        except Exception as rebuild_exc:
                            self.logger.warning(
                                "OrderFlowMonitor watcher rebuild failed | symbol=%s error=%s",
                                self.symbol,
                                rebuild_exc,
                            )
                    await asyncio.sleep(delay)
        finally:
            await self._close_watcher()
            self.logger.info("OrderFlowMonitor stopped | symbol=%s", self.symbol)
            self._runner_task = None
            self._loop = None

    def start_background(self, *, name: str | None = None) -> None:
        if self._background_thread is not None and self._background_thread.is_alive():
            return

        thread_name = name or f"order-flow-{self.symbol.replace('/', '-') }"

        def runner() -> None:
            with suppress(asyncio.CancelledError):
                asyncio.run(self.run_forever())

        self._background_thread = threading.Thread(target=runner, daemon=True, name=thread_name)
        self._background_thread.start()

    def stop_background(self, timeout: float = 5.0) -> None:
        loop = self._loop
        if loop is not None:
            future = asyncio.run_coroutine_threadsafe(self.stop(), loop)
            future.result(timeout=timeout)
        if self._background_thread is not None:
            self._background_thread.join(timeout=timeout)
            if not self._background_thread.is_alive():
                self._background_thread = None

    async def _replace_watcher(self) -> None:
        try:
            await self._close_watcher()
        finally:
            self._closed = False
            self.watcher = self.watcher_factory() if self.watcher_factory is not None else self.watcher

    async def _close_watcher(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_method = getattr(self.watcher, "close", None)
        if close_method is None:
            return
        result = close_method()
        if hasattr(result, "__await__"):
            await result

    def _compute_reconnect_delay(self, reconnect_attempt: int) -> float:
        max_delay = max(self.reconnect_delay_seconds, min(30.0, self.reconnect_delay_seconds * 10))
        return min(max_delay, self.reconnect_delay_seconds * (2 ** max(0, reconnect_attempt - 1)))

    def _emit_state_log(self, snapshot: OrderFlowSnapshot) -> None:
        state = "neutral"
        if snapshot.obi >= self.signal_threshold:
            state = "strong_bids"
        elif snapshot.obi <= -self.signal_threshold:
            state = "strong_asks"

        wall_state = "ask_wall" if snapshot.has_large_wall("buy", self.wall_ratio) else "none"
        if wall_state == "none" and snapshot.has_large_wall("sell", self.wall_ratio):
            wall_state = "bid_wall"

        signature = (state, wall_state)
        now = time.monotonic()

        if self._last_log_signature is None:
            self._write_state_log(snapshot, state, wall_state)
            self._last_log_signature = signature
            self._last_log_at = now
            self._pending_log_signature = None
            self._pending_log_since = 0.0
            return

        if signature == self._last_log_signature:
            self._pending_log_signature = None
            self._pending_log_since = 0.0
            return

        if self._pending_log_signature != signature:
            self._pending_log_signature = signature
            self._pending_log_since = now
            return

        if now - self._pending_log_since < self.log_interval_seconds:
            return

        self._write_state_log(snapshot, state, wall_state)
        self._last_log_signature = signature
        self._last_log_at = now
        self._pending_log_signature = None
        self._pending_log_since = 0.0

    def _write_state_log(self, snapshot: OrderFlowSnapshot, state: str, wall_state: str) -> None:
        if state == "strong_bids":
            self.logger.info(
                "%sStrong bids | symbol=%s obi=%.3f bid_sum=%.4f ask_sum=%.4f best_bid=%s best_ask=%s%s",
                GREEN,
                snapshot.symbol,
                snapshot.obi,
                snapshot.bid_sum,
                snapshot.ask_sum,
                snapshot.best_bid,
                snapshot.best_ask,
                RESET,
            )
        elif state == "strong_asks":
            self.logger.info(
                "%sStrong asks | symbol=%s obi=%.3f bid_sum=%.4f ask_sum=%.4f best_bid=%s best_ask=%s%s",
                RED,
                snapshot.symbol,
                snapshot.obi,
                snapshot.bid_sum,
                snapshot.ask_sum,
                snapshot.best_bid,
                snapshot.best_ask,
                RESET,
            )

        if wall_state == "ask_wall":
            self.logger.warning(
                "%sOpposite ask wall intercepted | symbol=%s share=%.2f%% level_size=%.4f ask_sum=%.4f%s",
                YELLOW,
                snapshot.symbol,
                snapshot.largest_ask_level_share * 100,
                snapshot.largest_ask_level_size,
                snapshot.ask_sum,
                RESET,
            )
        elif wall_state == "bid_wall":
            self.logger.warning(
                "%sOpposite bid wall intercepted | symbol=%s share=%.2f%% level_size=%.4f bid_sum=%.4f%s",
                YELLOW,
                snapshot.symbol,
                snapshot.largest_bid_level_share * 100,
                snapshot.largest_bid_level_size,
                snapshot.bid_sum,
                RESET,
            )

    @staticmethod
    def _normalize_levels(levels: Any, depth_levels: int) -> list[tuple[float, float]]:
        normalized: list[tuple[float, float]] = []
        for level in list(levels or [])[:depth_levels]:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = float(level[0])
            size = abs(float(level[1]))
            if price <= 0 or size <= 0:
                continue
            normalized.append((price, size))
        return normalized
