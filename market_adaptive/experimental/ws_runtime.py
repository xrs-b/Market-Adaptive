"""Experimental/isolated websocket runtime.

Moved under ``market_adaptive.experimental`` to make the non-mainline status explicit.
Legacy imports from ``market_adaptive.ws_runtime`` are preserved via a compatibility wrapper.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from contextlib import suppress
from typing import Any, Awaitable, Callable, Iterable, Union

from market_adaptive.clients.okx_ws_client import build_okx_websocket_client
from market_adaptive.config import OKXConfig, WebsocketRuntimeConfig
from market_adaptive.experimental.order_flow_monitor import OrderFlowMonitor

StreamCallback = Callable[[Any], Union[Any, Awaitable[Any]]]


class AsyncWebsocketRuntime:
    """Runs ccxt.pro websocket streams concurrently on a dedicated asyncio loop."""

    def __init__(
        self,
        *,
        okx_config: OKXConfig,
        websocket_config: WebsocketRuntimeConfig,
        market_symbol: str,
        tracked_symbols: Iterable[str],
        order_flow_monitors: Iterable[OrderFlowMonitor] | None = None,
        on_ticker: StreamCallback | None = None,
        on_orders: StreamCallback | None = None,
        on_positions: StreamCallback | None = None,
        logger: logging.Logger | logging.LoggerAdapter | None = None,
    ) -> None:
        self.okx_config = okx_config
        self.websocket_config = websocket_config
        self.market_symbol = market_symbol
        self.tracked_symbols = sorted(set(tracked_symbols))
        self.order_flow_monitors = [monitor for monitor in order_flow_monitors or [] if monitor is not None]
        self.on_ticker = on_ticker
        self.on_orders = on_orders
        self.on_positions = on_positions
        self.logger = logger or logging.LoggerAdapter(logging.getLogger(__name__), {"robot": "ws_runtime"})

        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner_task: asyncio.Task[Any] | None = None
        self._background_thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        return self._runner_task is not None and not self._runner_task.done()

    def start_background(self, *, name: str = "ws-runtime") -> None:
        if self._background_thread is not None and self._background_thread.is_alive():
            return

        def runner() -> None:
            with suppress(asyncio.CancelledError):
                asyncio.run(self.run_forever())

        self._background_thread = threading.Thread(target=runner, daemon=True, name=name)
        self._background_thread.start()

    def stop_background(self, timeout: float = 8.0) -> None:
        loop = self._loop
        if loop is not None:
            future = asyncio.run_coroutine_threadsafe(self.stop(), loop)
            future.result(timeout=timeout)
        if self._background_thread is not None:
            self._background_thread.join(timeout=timeout)
            if not self._background_thread.is_alive():
                self._background_thread = None

    async def stop(self) -> None:
        runner = self._runner_task
        if runner is None:
            return
        if runner is asyncio.current_task():
            return
        runner.cancel()
        with suppress(asyncio.CancelledError):
            await runner

    async def run_forever(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._runner_task = asyncio.current_task()
        tasks: list[asyncio.Task[Any]] = []
        try:
            if self.websocket_config.ticker_enabled and self.on_ticker is not None:
                tasks.append(asyncio.create_task(self._run_ticker_stream(), name="ws:ticker"))
            if self.websocket_config.orders_enabled and self.on_orders is not None:
                tasks.append(asyncio.create_task(self._run_orders_stream(), name="ws:orders"))
            if self.websocket_config.positions_enabled and self.on_positions is not None:
                tasks.append(asyncio.create_task(self._run_positions_stream(), name="ws:positions"))
            for index, monitor in enumerate(self.order_flow_monitors, start=1):
                tasks.append(asyncio.create_task(monitor.run_forever(), name=f"ws:orderbook:{index}"))

            if not tasks:
                self.logger.info("Async websocket runtime skipped: no enabled websocket tasks")
                return

            self.logger.info(
                "Async websocket runtime started | ticker=%s orders=%s positions=%s orderbooks=%s symbols=%s",
                self.websocket_config.ticker_enabled,
                self.websocket_config.orders_enabled,
                self.websocket_config.positions_enabled,
                len(self.order_flow_monitors),
                ",".join(self.tracked_symbols),
            )
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.logger.info("Async websocket runtime stopped")
            self._runner_task = None
            self._loop = None

    async def _run_ticker_stream(self) -> None:
        await self._run_stream(
            name="ticker",
            fetch=lambda client: client.watch_ticker(self.market_symbol),
            callback=self.on_ticker,
        )

    async def _run_orders_stream(self) -> None:
        await self._run_stream(
            name="orders",
            fetch=lambda client: client.watch_orders(),
            callback=self.on_orders,
        )

    async def _run_positions_stream(self) -> None:
        await self._run_stream(
            name="positions",
            fetch=lambda client: client.watch_positions(self.tracked_symbols),
            callback=self.on_positions,
        )

    async def _run_stream(
        self,
        *,
        name: str,
        fetch: Callable[[Any], Awaitable[Any]],
        callback: StreamCallback | None,
    ) -> None:
        reconnect_attempt = 0
        while True:
            client = build_okx_websocket_client(self.okx_config)
            try:
                while True:
                    payload = await fetch(client)
                    reconnect_attempt = 0
                    if callback is not None:
                        await self._dispatch(callback, payload)
            except asyncio.CancelledError:
                await client.close()
                raise
            except Exception as exc:
                reconnect_attempt += 1
                delay = self._compute_reconnect_delay(reconnect_attempt)
                self.logger.warning(
                    "Websocket stream error | stream=%s error=%s reconnect_in=%.1fs attempt=%s",
                    name,
                    exc,
                    delay,
                    reconnect_attempt,
                )
                with suppress(Exception):
                    await client.close()
                await asyncio.sleep(delay)

    async def _dispatch(self, callback: StreamCallback, payload: Any) -> Any:
        result = callback(payload)
        if inspect.isawaitable(result):
            return await result
        return result

    def _compute_reconnect_delay(self, reconnect_attempt: int) -> float:
        base = self.websocket_config.reconnect_delay_seconds
        max_delay = self.websocket_config.reconnect_max_delay_seconds
        return min(max_delay, base * (2 ** max(0, reconnect_attempt - 1)))
