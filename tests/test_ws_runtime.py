from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from market_adaptive.config import OKXConfig, WebsocketRuntimeConfig
from market_adaptive.ws_runtime import AsyncWebsocketRuntime


class DummyTickerClient:
    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = list(payloads)
        self.closed = False

    async def watch_ticker(self, symbol: str) -> dict:
        del symbol
        if self.payloads:
            return self.payloads.pop(0)
        await asyncio.sleep(3600)
        return {}

    async def close(self) -> None:
        self.closed = True


class DummyOrdersClient:
    def __init__(self, should_fail_first: bool, payload: list[dict] | None) -> None:
        self.should_fail_first = should_fail_first
        self.payload = payload
        self.closed = False
        self.calls = 0

    async def watch_orders(self):
        self.calls += 1
        if self.should_fail_first and self.calls == 1:
            raise RuntimeError("temporary websocket error")
        if self.payload is not None:
            payload = self.payload
            self.payload = None
            return payload
        await asyncio.sleep(3600)
        return []

    async def close(self) -> None:
        self.closed = True


class AsyncWebsocketRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_dispatches_ticker_callback(self) -> None:
        ticker_events: list[dict] = []
        client = DummyTickerClient([{"last": 100.0}])
        runtime = AsyncWebsocketRuntime(
            okx_config=OKXConfig(api_key="", api_secret="", passphrase=""),
            websocket_config=WebsocketRuntimeConfig(enabled=True, orders_enabled=False, positions_enabled=False),
            market_symbol="BTC/USDT",
            tracked_symbols=["BTC/USDT"],
            on_ticker=lambda payload: ticker_events.append(payload),
        )

        with patch("market_adaptive.ws_runtime.build_okx_websocket_client", return_value=client):
            task = asyncio.create_task(runtime.run_forever())
            try:
                for _ in range(50):
                    if ticker_events:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(ticker_events, [{"last": 100.0}])
            finally:
                await runtime.stop()
                await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(client.closed)

    async def test_runtime_reconnects_orders_stream_after_transient_error(self) -> None:
        order_events: list[list[dict]] = []
        clients = [
            DummyOrdersClient(True, []),
            DummyOrdersClient(False, [{"id": "1", "filled": 1.0}]),
        ]
        runtime = AsyncWebsocketRuntime(
            okx_config=OKXConfig(api_key="", api_secret="", passphrase=""),
            websocket_config=WebsocketRuntimeConfig(
                enabled=True,
                ticker_enabled=False,
                positions_enabled=False,
                reconnect_delay_seconds=0.01,
                reconnect_max_delay_seconds=0.05,
            ),
            market_symbol="BTC/USDT",
            tracked_symbols=["BTC/USDT"],
            on_orders=lambda payload: order_events.append(payload),
        )

        with patch("market_adaptive.ws_runtime.build_okx_websocket_client", side_effect=clients):
            task = asyncio.create_task(runtime.run_forever())
            try:
                for _ in range(100):
                    if order_events:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(order_events, [[{"id": "1", "filled": 1.0}]])
            finally:
                await runtime.stop()
                await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(clients[0].closed)
        self.assertTrue(clients[1].closed)


if __name__ == "__main__":
    unittest.main()
