from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from market_adaptive.strategies.dynamic_grid_robot import DynamicGridConfig, DynamicGridRobot


class DummyClient:
    def __init__(self) -> None:
        self.limit_orders = []
        self.market_orders = []
        self.cancel_all_calls = []
        self.close_all_calls = []
        self.futures_settings_calls = []
        self._equity = 10_000.0
        self._contract_value = 1.0
        self._min_amount = 0.01
        self.positions = []
        self.ticker = {"last": 100.0, "info": {"markPx": 100.0}}

    def ensure_futures_settings(self, symbol: str, leverage: int, margin_mode: str | None = None) -> None:
        self.futures_settings_calls.append((symbol, leverage, margin_mode))

    def fetch_ticker(self, symbol: str):
        del symbol
        return dict(self.ticker)

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        del quote_currency
        return self._equity

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def position_notional(self, symbol: str, position: dict) -> float:
        del symbol
        return abs(float(position.get("notional", 0.0)))

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return self._contract_value

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return self._min_amount

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 2)

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, "price": price, **kwargs}
        self.limit_orders.append(payload)
        return payload

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, **kwargs}
        self.market_orders.append(payload)
        return payload

    def cancel_all_orders(self, symbol: str):
        self.cancel_all_calls.append(symbol)
        return []

    def close_all_positions(self, symbol: str):
        self.close_all_calls.append(symbol)
        return []


class DynamicGridRobotTests(unittest.IsolatedAsyncioTestCase):
    async def test_initialize_grid_places_symmetric_ten_orders(self) -> None:
        client = DummyClient()
        robot = DynamicGridRobot(
            client=client,
            okx_config=object(),
            config=DynamicGridConfig(symbol="BTC/USDT", base_order_amount=0.01),
            atr_provider=lambda: 20.0,
        )

        await robot.initialize_grid()

        self.assertEqual(client.futures_settings_calls, [("BTC/USDT", 3, "isolated")])
        self.assertEqual(len(client.limit_orders), 10)
        self.assertEqual(sum(1 for order in client.limit_orders if order["side"] == "buy"), 5)
        self.assertEqual(sum(1 for order in client.limit_orders if order["side"] == "sell"), 5)
        self.assertAlmostEqual(robot.state.lower_bound, 50.0)
        self.assertAlmostEqual(robot.state.upper_bound, 150.0)
        self.assertAlmostEqual(robot.state.step_size, 10.0)

    async def test_filled_buy_places_reduce_only_sell_counter_order(self) -> None:
        client = DummyClient()
        robot = DynamicGridRobot(
            client=client,
            okx_config=object(),
            config=DynamicGridConfig(symbol="BTC/USDT", base_order_amount=0.01),
            atr_provider=lambda: 20.0,
        )
        await robot.initialize_grid()
        client.limit_orders.clear()

        await robot.handle_order_update(
            {
                "side": "buy",
                "status": "closed",
                "price": 90.0,
                "filled": 0.01,
                "reduceOnly": False,
            }
        )

        self.assertEqual(len(client.limit_orders), 1)
        self.assertEqual(client.limit_orders[0]["side"], "sell")
        self.assertTrue(client.limit_orders[0]["reduce_only"])
        self.assertAlmostEqual(client.limit_orders[0]["price"], 100.0)

    async def test_flash_crash_protection_cancels_orders_and_enters_cooldown(self) -> None:
        client = DummyClient()
        now = datetime(2026, 4, 11, 3, 0, tzinfo=timezone.utc)
        robot = DynamicGridRobot(
            client=client,
            okx_config=object(),
            config=DynamicGridConfig(
                symbol="BTC/USDT",
                base_order_amount=0.01,
                flash_crash_atr_multiplier=1.5,
                flash_crash_cooldown_seconds=300,
            ),
            atr_provider=lambda: 10.0,
            now_provider=lambda: now,
        )
        await robot.initialize_grid()
        client.cancel_all_calls.clear()

        for price in (100.0, 118.0, 101.0):
            await robot.handle_ticker({"last": price, "info": {"markPx": price}})

        self.assertEqual(client.cancel_all_calls, ["BTC/USDT"])
        self.assertIsNotNone(robot.state.paused_until)
        assert robot.state.paused_until is not None
        self.assertEqual(robot.state.paused_until, now + timedelta(seconds=300))

    async def test_directional_exposure_blocks_same_side_opening(self) -> None:
        client = DummyClient()
        client.positions = [{"side": "long", "notional": 5100.0}]
        robot = DynamicGridRobot(
            client=client,
            okx_config=object(),
            config=DynamicGridConfig(symbol="BTC/USDT", base_order_amount=0.01),
            atr_provider=lambda: 20.0,
        )

        allowed_buy = await robot._can_open_direction("buy", 100.0, 0.01)
        allowed_sell = await robot._can_open_direction("sell", 100.0, 0.01)

        self.assertFalse(allowed_buy)
        self.assertTrue(allowed_sell)


if __name__ == "__main__":
    unittest.main()
