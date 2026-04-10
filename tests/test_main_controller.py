from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from market_adaptive.config import (
    AppConfig,
    CTAConfig,
    DatabaseConfig,
    ExecutionConfig,
    GridConfig,
    MarketOracleConfig,
    OKXConfig,
    RuntimeConfig,
)
from market_adaptive.controller import MainController
from market_adaptive.db import DatabaseInitializer


class DummyAccountClient:
    def __init__(self, equity: float, pnl: float = 0.0) -> None:
        self.equity = equity
        self.pnl = pnl
        self.cancelled_symbols = []
        self.closed_symbols = []

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        return self.equity

    def fetch_total_unrealized_pnl(self, symbols=None) -> float:
        return self.pnl

    def cancel_all_orders_for_symbols(self, symbols):
        self.cancelled_symbols.extend(sorted(set(symbols)))
        return []

    def close_all_positions_for_symbols(self, symbols):
        self.closed_symbols.extend(sorted(set(symbols)))
        return []


class MainControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.config = AppConfig(
            okx=OKXConfig(api_key="", api_secret="", passphrase=""),
            database=DatabaseConfig(path=Path(self.temp_dir.name) / "market_adaptive.sqlite3"),
            runtime=RuntimeConfig(),
            market_oracle=MarketOracleConfig(),
            execution=ExecutionConfig(),
            cta=CTAConfig(),
            grid=GridConfig(),
            config_path=Path(self.temp_dir.name) / "config.yaml",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_risk_manager_stops_when_drawdown_exceeds_five_percent(self) -> None:
        controller = MainController(self.config, self.database)
        controller.starting_equity = 100.0
        controller.risk_client = DummyAccountClient(equity=94.0, pnl=-6.0)
        controller.shutdown_client = DummyAccountClient(equity=94.0, pnl=-6.0)

        controller.monitor_risk_once()

        self.assertTrue(controller.stop_event.is_set())
        self.assertIn("BTC/USDT", controller.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", controller.shutdown_client.closed_symbols)

    def test_shutdown_persists_checkpoint(self) -> None:
        controller = MainController(self.config, self.database)
        controller.shutdown_client = DummyAccountClient(equity=100.0, pnl=0.0)
        controller.stop_event = threading.Event()

        controller._shutdown()

        checkpoint = self.database.get_system_state("last_shutdown_at")
        self.assertIsNotNone(checkpoint)
        self.assertIn("BTC/USDT", controller.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", controller.shutdown_client.closed_symbols)


if __name__ == "__main__":
    unittest.main()
