from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import CTAConfig, ExecutionConfig, GridConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.strategies import CTARobot, GridRobot, HandsCoordinator


class DummyClient:
    def __init__(self) -> None:
        self.market_orders = []
        self.limit_orders = []
        self.cancel_all_calls = []
        self.close_all_calls = []
        self.last_price = 100.0
        self.ohlcv = []

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        return self.ohlcv[-limit:]

    def fetch_last_price(self, symbol: str) -> float:
        return self.last_price

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, **kwargs}
        self.market_orders.append(payload)
        return payload

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, "price": price, **kwargs}
        self.limit_orders.append(payload)
        return payload

    def cancel_all_orders(self, symbol: str):
        self.cancel_all_calls.append(symbol)
        return []

    def close_all_positions(self, symbol: str):
        self.close_all_calls.append(symbol)
        return []


class TheHandsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.client = DummyClient()
        self.execution = ExecutionConfig(cta_order_size=0.02, grid_order_size=0.03)
        self.cta_config = CTAConfig(symbol="BTC/USDT")
        self.grid_config = GridConfig(symbol="BTC/USDT", range_percent=0.02, levels=10)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_status(self, status: str, timestamp: str = "2026-04-10T03:00:00+00:00") -> None:
        self.database.insert_market_status(
            MarketStatusRecord(
                timestamp=timestamp,
                symbol="BTC/USDT",
                status=status,
                adx_value=25.0,
                volatility=0.02,
            )
        )

    def _load_bullish_cross_ohlcv(self) -> None:
        base = 1_700_000_000_000
        closes = [120, 118, 116, 114, 112, 110, 108, 106, 104, 102, 100, 98, 96, 94, 92, 90, 88, 86, 84, 82, 80, 85, 95, 110, 130]
        payload = []
        for index, close in enumerate(closes):
            payload.append([base + index * 900_000, close - 1, close + 1, close - 2, close, 100 + index])
        self.client.ohlcv = payload

    def test_cta_robot_places_market_buy_only_in_trend(self) -> None:
        self._insert_status("trend")
        self._load_bullish_cross_ohlcv()
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:market_buy")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertEqual(self.client.market_orders[0]["side"], "buy")

    def test_grid_robot_places_ten_orders_only_in_sideways(self) -> None:
        self._insert_status("sideways")
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "grid:placed_10_orders@100.00")
        self.assertEqual(len(self.client.limit_orders), 10)
        self.assertEqual(self.client.cancel_all_calls, ["BTC/USDT"])

    def test_status_switch_triggers_flatten_before_inactive_cycle(self) -> None:
        self._insert_status("trend", "2026-04-10T03:00:00+00:00")
        cta = CTARobot(self.client, self.database, self.cta_config, self.execution)
        self._load_bullish_cross_ohlcv()
        cta.run()

        self._insert_status("sideways", "2026-04-10T03:05:00+00:00")
        result = cta.run()

        self.assertFalse(result.active)
        self.assertIn("BTC/USDT", self.client.cancel_all_calls)
        self.assertIn("BTC/USDT", self.client.close_all_calls)

    def test_hands_coordinator_runs_both_robots(self) -> None:
        self._insert_status("sideways")
        coordinator = HandsCoordinator(
            cta_robot=CTARobot(self.client, self.database, self.cta_config, self.execution),
            grid_robot=GridRobot(self.client, self.database, self.grid_config, self.execution),
        )

        summary = coordinator.run_once()

        self.assertFalse(summary.cta.active)
        self.assertTrue(summary.grid.active)


if __name__ == "__main__":
    unittest.main()
