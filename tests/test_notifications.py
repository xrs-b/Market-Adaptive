from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import CTAConfig, ExecutionConfig, GridConfig, MarketOracleConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.oracles.market_oracle import MarketOracle
from market_adaptive.strategies import CTARobot, GridRobot
from market_adaptive.testsupport import DummyNotifier


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


class NotificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.notifier = DummyNotifier()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_market_oracle_notifies_on_status_change(self) -> None:
        client = DummyClient()
        oracle = MarketOracle(client, self.database, MarketOracleConfig(), notifier=self.notifier)
        oracle.collect_market_snapshot = lambda: type('Snapshot', (), {
            'symbol': 'BTC/USDT', 'strongest_adx': 30.0, 'strongest_volatility': 0.02,
            'higher': type('I', (), {'adx_value': 30.0, 'bb_width_expanding': True})(),
            'lower': type('I', (), {'adx_value': 18.0, 'bb_width_expanding': False})(),
        })()

        oracle.run_once()

        self.assertEqual(len(self.notifier.messages), 1)
        self.assertEqual(self.notifier.messages[0][0], 'Market Status Switched')

    def test_cta_robot_notifies_on_trade_action(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'trend', 28.0, 0.02))
        closes = [120, 118, 116, 114, 112, 110, 108, 106, 104, 102, 100, 98, 96, 94, 92, 90, 88, 86, 84, 82, 80, 85, 95, 110, 130]
        base = 1_700_000_000_000
        client.ohlcv = [[base + i * 900_000, c - 1, c + 1, c - 2, c, 100 + i] for i, c in enumerate(closes)]

        robot = CTARobot(client, db, CTAConfig(), ExecutionConfig(), notifier=notifier)
        robot.run()

        self.assertTrue(any(title == 'Strategy Action' for title, _ in notifier.messages))

    def test_grid_robot_notifies_on_cleanup(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'trend', 28.0, 0.02))
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier)
        robot.run()
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:05:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        robot.run()

        self.assertTrue(any(title == 'Strategy Cleanup' for title, _ in notifier.messages))


if __name__ == '__main__':
    unittest.main()
