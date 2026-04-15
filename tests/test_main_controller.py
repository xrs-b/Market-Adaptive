from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from market_adaptive.config import (
    AppConfig,
    CTAConfig,
    DatabaseConfig,
    DiscordNotificationConfig,
    ExecutionConfig,
    GridConfig,
    MarketOracleConfig,
    NotificationConfig,
    OKXConfig,
    RiskControlConfig,
    RuntimeConfig,
    SentimentConfig,
)
from market_adaptive.controller import MainController, WorkerSpec
from market_adaptive.db import DatabaseInitializer, SystemStateRecord
from market_adaptive.testsupport import DummyNotifier


class DummyAccountClient:
    def __init__(self, equity: float, pnl: float = 0.0, margin_ratio: float = 0.0, positions=None) -> None:
        self.equity = equity
        self.pnl = pnl
        self.margin_ratio = margin_ratio
        self.positions = positions or []
        self.cancelled_symbols = []
        self.closed_symbols = []

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        del quote_currency
        return self.equity

    def fetch_total_unrealized_pnl(self, symbols=None) -> float:
        del symbols
        return self.pnl

    def fetch_account_risk_snapshot(self, symbols=None) -> dict[str, float]:
        del symbols
        return {
            "equity": self.equity,
            "margin_ratio": self.margin_ratio,
            "maintenance_margin": self.margin_ratio * self.equity,
            "position_notional": 0.0,
            "open_order_notional": 0.0,
            "total_notional": 0.0,
        }

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return 100.0

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return 0.01

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return float(amount)

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return 0.0

    def fetch_symbol_position_notional(self, symbol: str) -> float:
        del symbol
        return 0.0

    def fetch_symbol_open_order_notional(self, symbol: str) -> float:
        del symbol
        return 0.0

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(amount) * abs(price) * 0.01

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def position_notional(self, symbol: str, position: dict) -> float:
        del symbol
        return abs(float(position.get("notional", 0.0)))

    def cancel_all_orders(self, symbol):
        self.cancelled_symbols.append(symbol)
        return []

    def close_all_positions(self, symbol):
        self.closed_symbols.append(symbol)
        return []

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
            notification=NotificationConfig(discord=DiscordNotificationConfig(enabled=False)),
            runtime=RuntimeConfig(),
            risk_control=RiskControlConfig(),
            sentiment=SentimentConfig(enabled=False),
            market_oracle=MarketOracleConfig(),
            execution=ExecutionConfig(),
            cta=CTAConfig(),
            grid=GridConfig(),
            config_path=Path(self.temp_dir.name) / "config.yaml",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_risk_manager_stops_when_daily_drawdown_exceeds_five_percent(self) -> None:
        controller = MainController(self.config, self.database)
        controller.notifier = DummyNotifier()
        controller.risk_control.notifier = controller.notifier
        current_date = datetime.now(timezone.utc).astimezone(ZoneInfo(self.config.runtime.timezone)).date().isoformat()
        timestamp = datetime.now(timezone.utc).isoformat()
        self.database.upsert_system_state(SystemStateRecord("risk_daily_start_date", current_date, timestamp))
        self.database.upsert_system_state(SystemStateRecord("risk_daily_start_equity", "100.0", timestamp))
        controller.risk_control.client = DummyAccountClient(equity=94.0, pnl=-6.0)
        controller.risk_control.shutdown_client = DummyAccountClient(equity=94.0, pnl=-6.0)

        controller.monitor_risk_once()

        self.assertTrue(controller.stop_event.is_set())
        self.assertIn("BTC/USDT", controller.risk_control.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", controller.risk_control.shutdown_client.closed_symbols)
        self.assertTrue(any(title == "硬风控已触发" for title, _ in controller.notifier.messages))
        self.assertEqual(self.database.get_system_state("system_status").state_value, "OFF")

    def test_recovery_closes_rogue_exchange_position(self) -> None:
        controller = MainController(self.config, self.database)
        controller.risk_control.client = DummyAccountClient(
            equity=100.0,
            positions=[{"contracts": 2.0, "side": "long", "notional": 200.0}],
        )
        controller.risk_control.shutdown_client = DummyAccountClient(equity=100.0)

        result = controller.recover_orders_once()

        self.assertIn("closed_rogue_position", result)
        self.assertIn("BTC/USDT", controller.risk_control.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", controller.risk_control.shutdown_client.closed_symbols)

    def test_shutdown_persists_checkpoint(self) -> None:
        controller = MainController(self.config, self.database)
        controller.notifier = DummyNotifier()
        controller.shutdown_client = DummyAccountClient(equity=100.0, pnl=0.0)
        controller.stop_event = threading.Event()

        controller._shutdown()

        checkpoint = self.database.get_system_state("last_shutdown_at")
        self.assertIsNotNone(checkpoint)
        self.assertIn("BTC/USDT", controller.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", controller.shutdown_client.closed_symbols)
        self.assertTrue(any(title == "系统已停止" for title, _ in controller.notifier.messages))

    def test_build_account_equity_report_includes_total_and_daily_pnl(self) -> None:
        self.config.runtime.account_initial_equity = 95400.0
        controller = MainController(self.config, self.database)
        controller.risk_client = DummyAccountClient(equity=96000.0, pnl=0.0)
        controller.risk_control.client = controller.risk_client
        timestamp = datetime.now(timezone.utc).isoformat()
        self.database.upsert_system_state(SystemStateRecord("risk_daily_start_date", datetime.now(timezone.utc).astimezone(ZoneInfo(self.config.runtime.timezone)).date().isoformat(), timestamp))
        self.database.upsert_system_state(SystemStateRecord("risk_daily_start_equity", "95800.0", timestamp))

        report = controller.build_account_equity_report(current_equity=96000.0)

        self.assertIn("初始资金：95400.0000 USDT", report)
        self.assertIn("总盈亏：+600.0000 USDT", report)
        self.assertIn("总盈亏率：+0.63%", report)
        self.assertIn("今日起始资金：95800.0000 USDT", report)
        self.assertIn("今日盈亏：+200.0000 USDT", report)
        self.assertIn("今日盈亏率：+0.21%", report)

    def test_runtime_context_is_shared_between_cta_and_grid(self) -> None:
        controller = MainController(self.config, self.database)

        self.assertIs(controller.cta_robot.runtime_context, controller.runtime_context)
        self.assertIs(controller.grid_robot.runtime_context, controller.runtime_context)

        controller.runtime_context.publish_cta_state(
            symbol="BTC/USDT",
            side="long",
            size=1.2,
            trend_strength=2.0,
            strong_trend=True,
        )
        snapshot = controller.grid_robot.runtime_context.snapshot_cta()
        self.assertEqual(snapshot.side, "long")
        self.assertTrue(snapshot.strong_trend)
        self.assertAlmostEqual(snapshot.size, 1.2)

    def test_shutdown_stops_grid_websocket_with_runtime_timeout(self) -> None:
        controller = MainController(self.config, self.database)
        controller.notifier = DummyNotifier()
        controller.shutdown_client = DummyAccountClient(equity=100.0, pnl=0.0)
        captured: list[float] = []

        def fake_stop_background_websocket(timeout: float = 5.0) -> None:
            captured.append(timeout)

        controller.grid_robot.stop_background_websocket = fake_stop_background_websocket
        controller._shutdown()

        self.assertEqual(captured, [float(self.config.runtime.shutdown_join_timeout_seconds)])

    def test_worker_loop_wakes_early_on_runtime_urgent_event(self) -> None:
        controller = MainController(self.config, self.database)
        run_markers: list[float] = []
        completed = threading.Event()

        def target() -> str:
            run_markers.append(threading.get_native_id())
            if len(run_markers) >= 2:
                completed.set()
                controller.stop_event.set()
            return "ok"

        worker = threading.Thread(
            target=controller._worker_loop,
            args=(WorkerSpec("test", 10, target),),
            daemon=True,
        )
        worker.start()
        self.assertFalse(completed.wait(0.2))

        controller.runtime_context.request_urgent_wakeup("test_wakeup")

        self.assertTrue(completed.wait(1.0))
        worker.join(timeout=1.0)
        self.assertGreaterEqual(len(run_markers), 2)

    def test_stop_requests_urgent_wakeup_for_waiting_workers(self) -> None:
        controller = MainController(self.config, self.database)

        controller.stop()

        self.assertTrue(controller.stop_event.is_set())
        self.assertTrue(controller.runtime_context.urgent_wakeup.is_set())
        self.assertEqual(controller.runtime_context.urgent_wakeup_reason, "controller_stop")

if __name__ == "__main__":
    unittest.main()
