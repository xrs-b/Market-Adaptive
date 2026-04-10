from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import RiskControlConfig, RuntimeConfig
from market_adaptive.db import DatabaseInitializer
from market_adaptive.risk import CTARiskProfile, GridRiskProfile, LogicalPositionSnapshot, RiskControlManager
from market_adaptive.testsupport import DummyNotifier


class DummyRiskClient:
    def __init__(
        self,
        *,
        equity: float = 1_000.0,
        pnl: float = 0.0,
        margin_ratio: float = 0.0,
        maintenance_margin: float = 0.0,
        position_notional: float = 0.0,
        order_notional: float = 0.0,
        positions=None,
        contract_value: float = 0.01,
        last_price: float = 100.0,
    ) -> None:
        self.equity = equity
        self.pnl = pnl
        self.margin_ratio = margin_ratio
        self.maintenance_margin = maintenance_margin
        self.position_notional_value = position_notional
        self.order_notional_value = order_notional
        self.positions = positions or []
        self.contract_value = contract_value
        self.last_price = last_price
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
            "maintenance_margin": self.maintenance_margin,
            "total_notional": self.position_notional_value,
        }

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return self.last_price

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return self.contract_value

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return 0.0

    def fetch_symbol_position_notional(self, symbol: str) -> float:
        del symbol
        return self.position_notional_value

    def fetch_symbol_open_order_notional(self, symbol: str) -> float:
        del symbol
        return self.order_notional_value

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(amount) * abs(price) * self.contract_value

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def position_notional(self, symbol: str, position: dict) -> float:
        del symbol
        return abs(float(position.get("notional", 0.0)))

    def get_position_liquidation_price(self, position: dict) -> float | None:
        liquidation_price = position.get("liquidationPrice") or position.get("info", {}).get("liqPx")
        if liquidation_price in (None, "", 0, "0"):
            return None
        return abs(float(liquidation_price))

    def cancel_all_orders(self, symbol: str):
        self.cancelled_symbols.append(symbol)
        return []

    def close_all_positions(self, symbol: str):
        self.closed_symbols.append(symbol)
        return []

    def cancel_all_orders_for_symbols(self, symbols):
        self.cancelled_symbols.extend(sorted(set(symbols)))
        return []

    def close_all_positions_for_symbols(self, symbols):
        self.closed_symbols.extend(sorted(set(symbols)))
        return []


class RiskControlManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.client = DummyRiskClient()
        self.shutdown_client = DummyRiskClient()
        self.stop_called = False
        self.grid_reduce_events: list[tuple[str, float]] = []
        self.cta_exit_reasons: list[str] = []
        self.logical_positions = {"BTC/USDT": None}
        self.manager = RiskControlManager(
            config=RiskControlConfig(
                default_symbol_max_notional=10_000.0,
                grid_margin_ratio_warning=0.45,
                grid_deviation_reduce_ratio=0.25,
                grid_liquidation_warning_ratio=0.08,
                grid_reduction_step_pct=0.25,
                grid_reduction_cooldown_seconds=1,
            ),
            runtime_config=RuntimeConfig(timezone="Asia/Shanghai"),
            database=self.database,
            client=self.client,
            shutdown_client=self.shutdown_client,
            symbols=["BTC/USDT"],
            notifier=DummyNotifier(),
            stop_callback=self._mark_stopped,
            reduce_grid_exposure_callback=lambda reason, step: self.grid_reduce_events.append((reason, step)),
            flatten_cta_position_callback=self.cta_exit_reasons.append,
            logical_position_provider=lambda: self.logical_positions,
        )
        self.manager.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _mark_stopped(self) -> None:
        self.stop_called = True

    def test_calculate_position_size_uses_equity_atr_and_contract_value(self) -> None:
        size = self.manager.calculate_position_size(
            "BTC/USDT",
            risk_percent=0.01,
            stop_loss_atr=2.0,
            atr_value=50.0,
            last_price=20_000.0,
        )

        self.assertAlmostEqual(size, 10.0)

    def test_calculate_position_size_caps_by_symbol_notional_limit(self) -> None:
        self.manager.config.default_symbol_max_notional = 1_000.0
        self.client.position_notional_value = 600.0
        self.client.order_notional_value = 200.0

        size = self.manager.calculate_position_size(
            "BTC/USDT",
            risk_percent=0.05,
            stop_loss_atr=2.0,
            atr_value=10.0,
            last_price=20_000.0,
        )

        self.assertAlmostEqual(size, 1.0)

    def test_margin_ratio_blocks_new_openings(self) -> None:
        self.client.margin_ratio = 0.65

        snapshot = self.manager.monitor_once()
        allowed, reason = self.manager.can_open_new_position("BTC/USDT", requested_notional=50.0)

        self.assertFalse(allowed)
        self.assertIn("margin_ratio", reason)
        self.assertEqual(snapshot.block_reason, reason)
        self.assertEqual(self.database.get_system_state("risk_new_openings").state_value, "OFF")
        self.assertEqual(self.grid_reduce_events, [])

    def test_cta_stop_hit_triggers_immediate_full_exit(self) -> None:
        self.client.last_price = 97.0
        self.manager.update_cta_risk(
            CTARiskProfile(
                symbol="BTC/USDT",
                side="long",
                stop_price=98.0,
                remaining_size=1.5,
                atr_value=2.0,
                stop_distance=4.0,
            )
        )

        self.manager.monitor_once()

        self.assertEqual(len(self.cta_exit_reasons), 1)
        self.assertIn("cta_atr_stop_hit", self.cta_exit_reasons[0])

    def test_grid_break_below_lower_bound_blocks_new_openings_and_observes(self) -> None:
        self.client.last_price = 95.0
        self.manager.update_grid_risk(GridRiskProfile(symbol="BTC/USDT", lower_bound=97.0, upper_bound=103.0))

        allowed, reason = self.manager.can_open_new_position(
            "BTC/USDT",
            requested_notional=0.0,
            strategy_name="grid",
        )

        self.assertFalse(allowed)
        self.assertIn("grid_observe_lower_break", reason)
        self.assertEqual(self.grid_reduce_events, [])

    def test_grid_liquidation_warning_reduces_exposure_in_steps(self) -> None:
        self.client.last_price = 95.0
        self.client.positions = [{"contracts": 2.0, "side": "long", "liquidationPrice": 92.0, "notional": 200.0}]
        self.manager.update_grid_risk(GridRiskProfile(symbol="BTC/USDT", lower_bound=97.0, upper_bound=103.0))

        self.manager.monitor_once()

        self.assertEqual(len(self.grid_reduce_events), 1)
        reason, step = self.grid_reduce_events[0]
        self.assertIn("grid_liquidation_warning", reason)
        self.assertAlmostEqual(step, 0.25)

    def test_recovery_resets_local_state_when_exchange_is_flat(self) -> None:
        self.logical_positions["BTC/USDT"] = LogicalPositionSnapshot(
            symbol="BTC/USDT",
            side="long",
            size=2.0,
            strategy_name="cta",
        )
        reset_events = []
        self.manager.local_position_reset_callback = lambda symbol, reason: reset_events.append((symbol, reason))

        result = self.manager.recover_positions_once()

        self.assertIn("reset_local_state", result)
        self.assertEqual(reset_events, [("BTC/USDT", "exchange_flat")])

    def test_daily_loss_circuit_breaker_flattens_and_stops(self) -> None:
        self.client.equity = 940.0
        self.manager.daily_start_equity = 1_000.0
        self.manager.daily_start_date = "2026-04-10"

        self.manager.monitor_once()

        self.assertTrue(self.manager.circuit_breaker_triggered)
        self.assertTrue(self.stop_called)
        self.assertIn("BTC/USDT", self.shutdown_client.cancelled_symbols)
        self.assertIn("BTC/USDT", self.shutdown_client.closed_symbols)
        self.assertEqual(self.database.get_system_state("system_status").state_value, "OFF")


if __name__ == "__main__":
    unittest.main()
