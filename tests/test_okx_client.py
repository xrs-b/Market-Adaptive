from __future__ import annotations

import unittest

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import ExecutionConfig, OKXConfig


class MockExchange:
    def __init__(self) -> None:
        self.margin_mode_calls = []
        self.leverage_calls = []
        self.order_book_calls = []

    def set_sandbox_mode(self, enabled: bool) -> None:
        self.sandbox_mode = enabled

    def set_margin_mode(self, margin_mode: str, symbol: str) -> None:
        self.margin_mode_calls.append((margin_mode, symbol))

    def set_leverage(self, leverage: int, symbol: str, params=None) -> None:
        self.leverage_calls.append((leverage, symbol, params or {}))

    def fetch_position_mode(self, symbol: str) -> dict[str, bool]:
        del symbol
        return {"hedged": False}

    def fetch_order_book(self, symbol: str, limit=None) -> dict:
        self.order_book_calls.append((symbol, limit))
        return {"bids": [[100.0, 1.0]], "asks": [[100.1, 1.0]]}

    def price_to_precision(self, symbol: str, price: float) -> str:
        del symbol
        return f"{price:.2f}"


class DummyOKXClient(OKXClient):
    def _build_exchange(self):
        return MockExchange()


class OKXClientTests(unittest.TestCase):
    def test_normalize_symbol_maps_plain_pair_to_okx_swap_symbol(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        self.assertEqual(client._normalize_symbol("BTC/USDT"), "BTC/USDT:USDT")
        self.assertEqual(client._normalize_symbol("BTC/USDT:USDT"), "BTC/USDT:USDT")

    def test_ensure_futures_settings_sets_margin_mode_and_leverage_once(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(td_mode="cross"),
        )

        client.ensure_futures_settings("BTC/USDT", leverage=5, margin_mode="cross")
        client.ensure_futures_settings("BTC/USDT", leverage=5, margin_mode="cross")

        self.assertEqual(client.exchange.margin_mode_calls, [("cross", "BTC/USDT:USDT")])
        self.assertEqual(
            client.exchange.leverage_calls,
            [(5, "BTC/USDT:USDT", {"mgnMode": "cross"})],
        )

    def test_fetch_order_book_normalizes_swap_symbol(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )

        order_book = client.fetch_order_book("BTC/USDT", limit=20)

        self.assertEqual(client.exchange.order_book_calls, [("BTC/USDT:USDT", 20)])
        self.assertEqual(order_book["asks"][0][0], 100.1)

    def test_price_to_precision_uses_exchange_formatter(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )

        self.assertEqual(client.price_to_precision("BTC/USDT", 100.126), 100.13)

    def test_get_position_liquidation_price_reads_okx_info_field(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )

        liquidation_price = client.get_position_liquidation_price({"info": {"liqPx": "94.5"}})

        self.assertEqual(liquidation_price, 94.5)


if __name__ == "__main__":
    unittest.main()
