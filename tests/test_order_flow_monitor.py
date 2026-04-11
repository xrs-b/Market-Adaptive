from __future__ import annotations

import asyncio
import io
import logging
import unittest
from unittest.mock import patch

from market_adaptive.config import OKXConfig
from market_adaptive.strategies.order_flow_monitor import (
    CCXTProUnavailableError,
    GREEN,
    RED,
    YELLOW,
    OKXCCXTProOrderBookWatcher,
    OrderFlowMonitor,
)


class QueueWatcher:
    def __init__(self, order_books: list[dict]) -> None:
        self.order_books = list(order_books)
        self.closed = False

    async def watch_order_book(self, symbol: str, limit: int | None = None) -> dict:
        del symbol, limit
        if self.order_books:
            return self.order_books.pop(0)
        await asyncio.sleep(3600)
        return {}

    async def close(self) -> None:
        self.closed = True


class OrderFlowMonitorTests(unittest.TestCase):
    def build_logger(self) -> tuple[logging.Logger, io.StringIO]:
        stream = io.StringIO()
        logger = logging.getLogger(f"test-order-flow-{id(stream)}")
        logger.handlers.clear()
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        return logger, stream

    def test_ingest_order_book_computes_obi_and_blocks_buy_when_large_ask_wall_exists(self) -> None:
        logger, _stream = self.build_logger()
        monitor = OrderFlowMonitor(
            QueueWatcher([]),
            symbol="BTC/USDT",
            depth_levels=20,
            signal_threshold=0.5,
            wall_ratio=0.40,
            log_interval_seconds=0.0,
            logger=logger,
        )
        order_book = {
            "bids": [[100.0 - index * 0.1, 4.0] for index in range(20)],
            "asks": [[100.1, 12.0]] + [[100.2 + index * 0.1, 1.0] for index in range(8)],
        }

        snapshot = monitor.ingest_order_book(order_book)
        validation = monitor.validate_signal("buy")

        self.assertAlmostEqual(snapshot.bid_sum, 80.0)
        self.assertAlmostEqual(snapshot.ask_sum, 20.0)
        self.assertAlmostEqual(snapshot.obi, 0.6)
        self.assertGreater(snapshot.largest_ask_level_share, 0.4)
        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "ask_wall_detected")
        self.assertFalse(monitor.is_signal_valid("buy"))

    def test_sell_signal_requires_negative_obi_and_no_large_bid_wall(self) -> None:
        monitor = OrderFlowMonitor(
            QueueWatcher([]),
            symbol="BTC/USDT",
            depth_levels=20,
            signal_threshold=0.5,
            wall_ratio=0.40,
            log_interval_seconds=0.0,
        )
        order_book = {
            "bids": [[100.0 - index * 0.1, 1.0] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 4.0] for index in range(20)],
        }

        snapshot = monitor.ingest_order_book(order_book)
        validation = monitor.validate_signal("sell")

        self.assertAlmostEqual(snapshot.obi, -0.6)
        self.assertFalse(snapshot.has_large_wall("sell", 0.40))
        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "confirmed")
        self.assertTrue(monitor.is_signal_valid("sell"))

    def test_validate_signal_reports_snapshot_unavailable_before_first_tick(self) -> None:
        monitor = OrderFlowMonitor(
            QueueWatcher([]),
            symbol="BTC/USDT",
            depth_levels=20,
            signal_threshold=0.5,
            wall_ratio=0.40,
            log_interval_seconds=0.0,
        )

        validation = monitor.validate_signal("buy")

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "snapshot_unavailable")
        self.assertFalse(validation.has_snapshot)

    def test_logging_uses_requested_colors_for_strong_bids_strong_asks_and_walls(self) -> None:
        logger, stream = self.build_logger()
        monitor = OrderFlowMonitor(
            QueueWatcher([]),
            symbol="BTC/USDT",
            depth_levels=20,
            signal_threshold=0.5,
            wall_ratio=0.40,
            log_interval_seconds=0.0,
            logger=logger,
        )

        monitor.ingest_order_book(
            {
                "bids": [[100.0 - index * 0.1, 4.0] for index in range(20)],
                "asks": [[100.1, 12.0]] + [[100.2 + index * 0.1, 1.0] for index in range(8)],
            }
        )
        bearish_book = {
            "bids": [[100.0 - index * 0.1, 1.0] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 4.0] for index in range(20)],
        }
        monitor.ingest_order_book(bearish_book)
        monitor.ingest_order_book(bearish_book)

        output = stream.getvalue()
        self.assertIn(GREEN, output)
        self.assertIn(RED, output)
        self.assertIn(YELLOW, output)

    def test_ccxtpro_watcher_raises_clear_error_when_optional_package_is_missing(self) -> None:
        with patch("market_adaptive.clients.okx_ws_client.importlib.import_module", side_effect=ModuleNotFoundError):
            with self.assertRaises(CCXTProUnavailableError):
                OKXCCXTProOrderBookWatcher(OKXConfig(api_key="", api_secret="", passphrase=""))


class OrderFlowMonitorAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_start_consumes_watcher_and_stop_closes_it(self) -> None:
        watcher = QueueWatcher(
            [
                {
                    "bids": [[100.0 - index * 0.1, 3.0] for index in range(20)],
                    "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
                }
            ]
        )
        monitor = OrderFlowMonitor(
            watcher,
            symbol="BTC/USDT",
            depth_levels=20,
            signal_threshold=0.5,
            wall_ratio=0.40,
            reconnect_delay_seconds=0.1,
            log_interval_seconds=0.0,
        )

        await monitor.start()
        for _ in range(50):
            if monitor.snapshot is not None:
                break
            await asyncio.sleep(0.01)

        self.assertIsNotNone(monitor.snapshot)
        self.assertTrue(monitor.is_signal_valid("buy"))

        await monitor.stop()
        self.assertTrue(watcher.closed)


if __name__ == "__main__":
    unittest.main()
