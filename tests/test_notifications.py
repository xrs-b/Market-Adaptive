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
        self.ohlcv_by_timeframe = {}
        self.order_book = {
            "bids": [[100.0 - index * 0.1, 1.6] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        payload = self.ohlcv_by_timeframe.get(timeframe, self.ohlcv)
        return payload[-limit:]

    def fetch_last_price(self, symbol: str) -> float:
        return self.last_price

    def fetch_order_book(self, symbol: str, limit: int | None = None):
        del symbol
        if limit is None:
            return self.order_book
        return {
            "bids": list(self.order_book.get("bids", []))[:limit],
            "asks": list(self.order_book.get("asks", []))[:limit],
        }

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

    def _set_bullish_ohlcv(self, client: DummyClient, lower_last_close: float = 100.0) -> None:
        base = 1_700_000_000_000
        lower_closes = []
        base_price = lower_last_close - 8.0
        pattern = [0.0, 0.4, -0.3, 0.5, -0.2, 0.3, -0.1, 0.2]
        for index in range(52):
            lower_closes.append(base_price + pattern[index % len(pattern)])
        lower_closes.extend(
            [
                lower_last_close - 5.6,
                lower_last_close - 4.8,
                lower_last_close - 4.0,
                lower_last_close - 3.2,
                lower_last_close - 2.4,
                lower_last_close - 1.6,
                lower_last_close - 0.8,
                lower_last_close,
            ]
        )
        higher_closes = [140 - 1.0 * (59 - index) for index in range(60)]
        major_closes = [220 - 2.0 * (59 - index) for index in range(60)]
        client.ohlcv_by_timeframe["15m"] = [
            [base + index * 900_000, close - 0.3, close + 0.4, close - 0.5, close, 100 + index * 3]
            for index, close in enumerate(lower_closes)
        ]
        client.ohlcv_by_timeframe["1h"] = [
            [base + index * 3_600_000, close - 0.5, close + 0.8, close - 0.7, close, 200 + index * 5]
            for index, close in enumerate(higher_closes)
        ]
        client.ohlcv_by_timeframe["4h"] = [
            [base + index * 14_400_000, close - 0.8, close + 1.0, close - 1.1, close, 260 + index * 7]
            for index, close in enumerate(major_closes)
        ]

    def test_market_oracle_notifies_on_status_change(self) -> None:
        client = DummyClient()
        oracle = MarketOracle(client, self.database, MarketOracleConfig(), notifier=self.notifier)
        oracle.collect_market_snapshot = lambda: type('Snapshot', (), {
            'symbol': 'BTC/USDT', 'strongest_adx': 30.0, 'strongest_volatility': 0.02,
            'higher_timeframe': '1h',
            'lower_timeframe': '15m',
            'higher': type('I', (), {
                'adx_value': 30.0,
                'adx_rising': True,
                'adx_trend_label': 'rising',
                'di_gap': 14.0,
                'plus_di_value': 32.0,
                'minus_di_value': 18.0,
                'bb_width_expanding': True,
            })(),
            'lower': type('I', (), {
                'adx_value': 18.0,
                'adx_rising': False,
                'adx_trend_label': 'flat',
                'di_gap': 3.0,
                'plus_di_value': 21.0,
                'minus_di_value': 18.0,
                'bb_width_expanding': False,
            })(),
        })()

        oracle.run_once()

        self.assertEqual(len(self.notifier.messages), 1)
        self.assertEqual(self.notifier.messages[0][0], 'Market Status Switched')

    def test_cta_robot_notifies_on_trade_action(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'trend', 28.0, 0.02))
        self._set_bullish_ohlcv(client, lower_last_close=100.0)

        robot = CTARobot(client, db, CTAConfig(), ExecutionConfig(), notifier=notifier)
        robot.run()

        self.assertTrue(any(title == 'Strategy Action' for title, _ in notifier.messages))
        self.assertTrue(any('cta:open_long' in body for _, body in notifier.messages))

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

    def test_grid_robot_notifies_on_flash_crash_trigger(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        client.ohlcv_by_timeframe['1h'] = [
            [1_700_000_000_000 + i * 3_600_000, 100.0, 105.0, 95.0, 100.0, 120.0]
            for i in range(80)
        ]
        client.ohlcv_by_timeframe['1m'] = [
            [1_700_000_000_000, 100.0, 100.1, 99.9, 100.0, 100.0],
            [1_700_000_060_000, 100.0, 118.0, 98.0, 100.0, 120.0],
        ]
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier, market_oracle=None, use_dynamic_range=False)

        result = robot.run()

        self.assertEqual(result.action.split('|')[0], 'grid:flash_crash_triggered')
        self.assertTrue(any(title == 'Grid Risk Alert' for title, _ in notifier.messages))
        self.assertTrue(any('flash_crash_triggered' in body for _, body in notifier.messages))


if __name__ == '__main__':
    unittest.main()
