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
        self.ohlcv_by_timeframe = {}

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        payload = self.ohlcv_by_timeframe.get(timeframe, self.ohlcv)
        return payload[-limit:]

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
        self.cta_config = CTAConfig(
            symbol="BTC/USDT",
            lower_timeframe="15m",
            higher_timeframe="1h",
            atr_trailing_multiplier=1.0,
            first_take_profit_size=0.5,
            second_take_profit_size=0.25,
        )
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

    def _set_ohlcv(self, timeframe: str, closes: list[float], step_ms: int) -> None:
        base = 1_700_000_000_000
        payload = []
        for index, close in enumerate(closes):
            payload.append([base + index * step_ms, close - 0.3, close + 0.4, close - 0.5, close, 100 + index * 3])
        self.client.ohlcv_by_timeframe[timeframe] = payload

    def _load_bullish_signal(self, lower_last_close: float = 100.0, higher_last_close: float = 140.0) -> None:
        lower_closes = [lower_last_close - 0.4 * (59 - index) for index in range(60)]
        higher_closes = [higher_last_close - 1.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("15m", lower_closes, 900_000)
        self._set_ohlcv("1h", higher_closes, 3_600_000)

    def _load_pullback_after_rally(self, latest_close: float) -> None:
        closes = [80 + index * 0.45 for index in range(56)] + [103.6, 104.8, 106.0, latest_close]
        self._set_ohlcv("15m", closes, 900_000)
        higher_closes = [140 - 1.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("1h", higher_closes, 3_600_000)

    def test_cta_robot_opens_long_only_in_trend(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:open_long")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertEqual(self.client.market_orders[0]["side"], "buy")

    def test_cta_robot_scales_out_and_uses_trailing_stop(self) -> None:
        self._insert_status("trend")
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        self._load_bullish_signal(lower_last_close=100.0)
        first_result = robot.run()
        self.assertEqual(first_result.action, "cta:open_long")

        self._load_bullish_signal(lower_last_close=102.5)
        second_result = robot.run()
        self.assertEqual(second_result.action, "cta:take_profit_2pct")
        self.assertEqual(self.client.market_orders[1]["side"], "sell")
        self.assertTrue(self.client.market_orders[1]["reduce_only"])
        self.assertAlmostEqual(self.client.market_orders[1]["amount"], 0.01)

        self._load_bullish_signal(lower_last_close=106.0)
        third_result = robot.run()
        self.assertEqual(third_result.action, "cta:take_profit_5pct")
        self.assertAlmostEqual(self.client.market_orders[2]["amount"], 0.005)
        self.assertIsNotNone(robot.position)

        robot.position.stop_price = 104.0
        self._load_pullback_after_rally(latest_close=103.0)
        fourth_result = robot.run()
        self.assertEqual(fourth_result.action, "cta:trailing_stop_exit")
        self.assertAlmostEqual(self.client.market_orders[3]["amount"], 0.005)
        self.assertIsNone(robot.position)

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
        self._load_bullish_signal(lower_last_close=100.0)
        cta.run()
        self.assertIsNotNone(cta.position)

        self._insert_status("sideways", "2026-04-10T03:05:00+00:00")
        result = cta.run()

        self.assertFalse(result.active)
        self.assertIn("BTC/USDT", self.client.cancel_all_calls)
        self.assertIn("BTC/USDT", self.client.close_all_calls)
        self.assertIsNone(cta.position)

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
