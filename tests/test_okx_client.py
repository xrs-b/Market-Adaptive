from __future__ import annotations

import unittest

import ccxt

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import ExecutionConfig, OKXConfig


class MockExchange:
    def __init__(self) -> None:
        self.margin_mode_calls = []
        self.leverage_calls = []
        self.order_book_calls = []
        self.raise_on_set_margin_mode = None
        self.open_orders = []
        self.ticker = {"last": 100.0}
        self.cancel_order_calls = []
        self.raise_on_cancel_order = None
        self.balance = {}
        self.positions = []

    def set_sandbox_mode(self, enabled: bool) -> None:
        self.sandbox_mode = enabled

    def set_margin_mode(self, margin_mode: str, symbol: str) -> None:
        self.margin_mode_calls.append((margin_mode, symbol))
        if self.raise_on_set_margin_mode is not None:
            raise self.raise_on_set_margin_mode

    def set_leverage(self, leverage: int, symbol: str, params=None) -> None:
        self.leverage_calls.append((leverage, symbol, params or {}))

    def fetch_position_mode(self, symbol: str) -> dict[str, bool]:
        del symbol
        return {"hedged": False}

    def fetch_balance(self) -> dict:
        return dict(self.balance)

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def fetch_open_orders(self, symbol: str):
        del symbol
        return list(self.open_orders)

    def cancel_order(self, order_id: str, symbol: str):
        self.cancel_order_calls.append((order_id, symbol))
        if self.raise_on_cancel_order is not None:
            raise self.raise_on_cancel_order
        return {"id": order_id, "status": "canceled"}

    def fetch_order_book(self, symbol: str, limit=None) -> dict:
        self.order_book_calls.append((symbol, limit))
        return {"bids": [[100.0, 1.0]], "asks": [[100.1, 1.0]]}

    def fetch_ticker(self, symbol: str) -> dict:
        del symbol
        return dict(self.ticker)

    def load_markets(self) -> None:
        return None

    def market(self, symbol: str) -> dict:
        del symbol
        return {"contractSize": 1.0}

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

    def test_ensure_futures_settings_ignores_okx_margin_mode_lever_validation_and_still_sets_leverage(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(td_mode="isolated"),
        )
        client.exchange.raise_on_set_margin_mode = ccxt.BadRequest(
            'okx setMarginMode() params["lever"] should be between 1 and 125'
        )

        client.ensure_futures_settings("BTC/USDT", leverage=3, margin_mode="isolated")

        self.assertEqual(client.exchange.margin_mode_calls, [("isolated", "BTC/USDT:USDT")])
        self.assertEqual(
            client.exchange.leverage_calls,
            [(3, "BTC/USDT:USDT", {"mgnMode": "isolated"})],
        )

    def test_cancel_all_orders_ignores_filled_or_missing_order_race(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        client.exchange.open_orders = [{"id": "123"}]
        client.exchange.raise_on_cancel_order = ccxt.OrderNotFound(
            'okx {"sMsg":"Order cancellation failed as the order has been filled, canceled or does not exist."}'
        )

        responses = client.cancel_all_orders("BTC/USDT")

        self.assertEqual(client.exchange.cancel_order_calls, [("123", "BTC/USDT:USDT")])
        self.assertEqual(responses, [{"id": "123", "status": "already_closed"}])

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

    def test_fetch_account_risk_snapshot_uses_maintenance_margin_over_okx_mgn_ratio(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        client.exchange.balance = {
            "total": {"USDT": 95_495.6007},
            "info": {
                "data": [
                    {
                        "details": [{"ccy": "USDT", "eq": "95495.6007"}],
                    }
                ]
            },
        }
        client.exchange.positions = [
            {
                "symbol": "BTC/USDT:USDT",
                "notional": 14.6181,
                "info": {
                    "mgnRatio": "74.0465",
                    "mmr": "0.0584584",
                },
            }
        ]

        snapshot = client.fetch_account_risk_snapshot(["BTC/USDT"])

        self.assertAlmostEqual(snapshot["maintenance_margin"], 0.0584584)
        self.assertAlmostEqual(snapshot["position_notional"], 14.6181)
        self.assertAlmostEqual(snapshot["open_order_notional"], 0.0)
        self.assertAlmostEqual(snapshot["total_notional"], 14.6181)
        self.assertAlmostEqual(snapshot["margin_ratio"], 0.0584584 / 95_495.6007)


    def test_fetch_account_risk_snapshot_includes_opening_order_notional(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        client.exchange.balance = {
            "total": {"USDT": 1_000.0},
            "info": {"data": [{"details": [{"ccy": "USDT", "eq": "1000"}]}]},
        }
        client.exchange.positions = [{"symbol": "BTC/USDT:USDT", "notional": 120.0, "info": {}}]
        client.exchange.open_orders = [
            {"amount": 2.0, "price": 50.0, "reduceOnly": False},
            {"amount": 1.0, "price": 80.0, "reduceOnly": True},
        ]

        snapshot = client.fetch_account_risk_snapshot(["BTC/USDT"])

        self.assertAlmostEqual(snapshot["position_notional"], 120.0)
        self.assertAlmostEqual(snapshot["open_order_notional"], 100.0)
        self.assertAlmostEqual(snapshot["total_notional"], 220.0)

    def test_fetch_symbol_open_order_notional_excludes_string_false_reduce_only_bug_and_true_reduce_only(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        client.exchange.open_orders = [
            {"amount": 2.0, "price": 50.0, "reduceOnly": "false", "info": {"reduceOnly": "false"}},
            {"amount": 1.0, "price": 80.0, "reduceOnly": "true", "info": {"reduceOnly": "true"}},
        ]

        self.assertAlmostEqual(client.fetch_symbol_open_order_notional("BTC/USDT"), 100.0)

    def test_fetch_account_risk_snapshot_falls_back_to_decimal_ratio_when_no_maintenance_margin(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        client.exchange.balance = {
            "total": {"USDT": 1_000.0},
            "info": {
                "data": [
                    {
                        "marginRatio": "0.42",
                        "details": [{"ccy": "USDT", "eq": "1000"}],
                    }
                ]
            },
        }

        snapshot = client.fetch_account_risk_snapshot(["BTC/USDT"])

        self.assertAlmostEqual(snapshot["margin_ratio"], 0.42)

    def test_get_position_liquidation_price_reads_okx_info_field(self) -> None:
        client = DummyOKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )

        liquidation_price = client.get_position_liquidation_price({"info": {"liqPx": "94.5"}})

        self.assertEqual(liquidation_price, 94.5)


if __name__ == "__main__":
    unittest.main()
