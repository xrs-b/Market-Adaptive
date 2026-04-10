from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import load_config
from market_adaptive.db import DatabaseInitializer


class MarketAdaptiveBootstrapTests(unittest.TestCase):
    def test_load_config_contains_okx_demo_headers(self) -> None:
        config = load_config(Path("config/config.yaml.example"))
        self.assertEqual(config.okx.headers["x-simulated-id"], "1")
        self.assertEqual(config.okx.headers["x-simulated-trading"], "1")
        self.assertTrue(config.okx.sandbox)
        self.assertEqual(config.grid.timeframe, "1h")
        self.assertEqual(config.grid.bollinger_period, 20)
        self.assertEqual(config.grid.trigger_limit_per_layer, 3)
        self.assertEqual(config.grid.layer_cooldown_seconds, 300)
        self.assertEqual(config.grid.max_rebalance_orders, 2)
        self.assertEqual(config.grid.leverage, 3)
        self.assertEqual(config.grid.range_percent, 0.03)
        self.assertEqual(config.grid.liquidation_protection_ratio, 0.05)
        self.assertTrue(config.sentiment.enabled)
        self.assertEqual(config.sentiment.timeframe, "5m")
        self.assertEqual(config.sentiment.extreme_bullish_ratio, 2.5)
        self.assertEqual(config.sentiment.normalized_cta_buy_action, "block")
        self.assertEqual(config.cta.major_timeframe, "4h")
        self.assertEqual(config.cta.swing_timeframe, "1h")
        self.assertEqual(config.cta.execution_timeframe, "15m")
        self.assertEqual(config.cta.swing_rsi_period, 14)
        self.assertEqual(config.cta.swing_rsi_ready_threshold, 50.0)
        self.assertEqual(config.cta.kdj_length, 9)
        self.assertEqual(config.cta.execution_breakout_lookback, 3)
        self.assertEqual(config.cta.obv_slope_window, 8)
        self.assertEqual(config.cta.obv_slope_threshold_degrees, 30.0)
        self.assertEqual(config.cta.risk_percent_per_trade, 0.02)
        self.assertEqual(config.cta.boosted_risk_percent_per_trade, 0.03)
        self.assertEqual(config.cta.volume_profile_lookback_hours, 24)
        self.assertEqual(config.cta.volume_profile_bin_count, 24)
        self.assertEqual(config.cta.volume_profile_value_area_pct, 0.70)
        self.assertEqual(config.runtime.fast_risk_check_interval_seconds, 1)
        self.assertEqual(config.risk_control.cta_single_trade_equity_multiple, 1.5)
        self.assertEqual(config.risk_control.max_directional_leverage, 8.0)
        self.assertEqual(config.risk_control.grid_margin_ratio_warning, 0.45)
        self.assertEqual(config.risk_control.grid_deviation_reduce_ratio, 0.25)
        self.assertEqual(config.risk_control.grid_liquidation_warning_ratio, 0.10)
        self.assertEqual(config.risk_control.grid_reduction_step_pct, 0.25)

    def test_database_initializer_creates_market_status_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "market_adaptive.sqlite3"
            DatabaseInitializer(db_path).initialize()

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='market_status'"
                ).fetchone()

            self.assertIsNotNone(row)


if __name__ == "__main__":
    unittest.main()
