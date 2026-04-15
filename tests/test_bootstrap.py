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
        self.assertEqual(config.grid.equity_allocation_ratio, 0.40)
        self.assertEqual(config.grid.layer_cooldown_seconds, 300)
        self.assertEqual(config.grid.max_rebalance_orders, 2)
        self.assertEqual(config.grid.levels, 8)
        self.assertEqual(config.grid.leverage, 3)
        self.assertEqual(config.grid.range_percent, 0.03)
        self.assertEqual(config.grid.min_spacing_ratio, 0.007)
        self.assertEqual(config.grid.atr_spacing_floor_multiplier, 0.5)
        self.assertEqual(config.grid.fee_rate, 0.001)
        self.assertEqual(config.market_oracle.relaxed_trend_adx_buffer, 1.5)
        self.assertEqual(config.market_oracle.relaxed_trend_di_gap_bonus, 1.0)
        self.assertEqual(config.market_oracle.bb_width_contraction_tolerance_ratio, 0.03)
        self.assertTrue(config.market_oracle.short_regime_thaw_enabled)
        self.assertEqual(config.market_oracle.short_regime_thaw_adx_floor, 15.0)
        self.assertEqual(config.market_oracle.short_regime_thaw_di_gap_floor, 6.0)
        self.assertEqual(config.market_oracle.short_regime_thaw_volatility_floor, 0.008)
        self.assertEqual(config.grid.sideways_neutral_bias_threshold, 0.12)
        self.assertEqual(config.grid.bearish_directional_bias_threshold, 0.30)
        self.assertEqual(config.grid.liquidation_protection_ratio, 0.05)
        self.assertEqual(config.grid.heavy_inventory_threshold, 0.60)
        self.assertFalse(config.grid.active_hedge_mode_enabled)
        self.assertEqual(config.grid.active_hedge_min_inventory_ratio, 0.45)
        self.assertTrue(config.grid.active_hedge_requires_cta_position)
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
        self.assertEqual(config.cta.strong_bull_bias_score, 60.0)
        self.assertEqual(config.cta.magnetism_score_bonus, 20.0)
        self.assertEqual(config.cta.kdj_memory_score_bonus, 10.0)
        self.assertEqual(config.cta.weak_bull_memory_score_bonus, 0.0)
        self.assertEqual(config.cta.early_bullish_score_bonus, 10.0)
        self.assertTrue(config.cta.starter_frontrun_enabled)
        self.assertEqual(config.cta.starter_frontrun_fraction, 0.20)
        self.assertEqual(config.cta.starter_frontrun_breakout_buffer_ratio, 0.002)
        self.assertEqual(config.cta.bullish_memory_retest_breakout_buffer_ratio, 0.0026)
        self.assertEqual(config.cta.obv_signal_window, 8)
        self.assertEqual(config.cta.obv_signal_threshold_degrees, 30.0)
        self.assertEqual(config.cta.obv_slope_window, 8)
        self.assertEqual(config.cta.obv_slope_threshold_degrees, 30.0)
        self.assertEqual(config.cta.obv_zscore_threshold, 1.0)
        self.assertEqual(config.cta.risk_percent_per_trade, 0.02)
        self.assertEqual(config.cta.boosted_risk_percent_per_trade, 0.03)
        self.assertEqual(config.cta.volume_profile_lookback_hours, 24)
        self.assertEqual(config.cta.volume_profile_bin_count, 24)
        self.assertEqual(config.cta.volume_profile_value_area_pct, 0.70)
        self.assertTrue(config.cta.order_flow_enabled)
        self.assertEqual(config.cta.order_flow_depth_levels, 20)
        self.assertEqual(config.cta.order_flow_confirmation_ratio, 1.5)
        self.assertEqual(config.cta.order_flow_high_conviction_ratio, 2.0)
        self.assertEqual(config.cta.order_flow_limit_buffer_bps, 3.0)
        self.assertEqual(config.cta.order_flow_max_slippage_bps, 12.0)
        self.assertEqual(config.cta.cta_assist_trim_ratio, 0.25)
        self.assertEqual(config.runtime.fast_risk_check_interval_seconds, 1)
        self.assertTrue(config.runtime.start_grid_websocket_on_boot)
        self.assertEqual(config.runtime.shutdown_join_timeout_seconds, 5.0)
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

    def test_database_initializer_migrates_legacy_status_check_constraints(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "market_adaptive.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_status (
                        timestamp TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('trend', 'sideways')),
                        adx_value REAL NOT NULL,
                        volatility REAL NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE strategy_runtime_state (
                        strategy_name TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        last_status TEXT NOT NULL CHECK(last_status IN ('trend', 'sideways')),
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (strategy_name, symbol)
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO market_status (timestamp, symbol, status, adx_value, volatility) VALUES (?, ?, ?, ?, ?)",
                    ("2026-04-15T00:00:00+00:00", "BTC/USDT", "trend", 25.0, 0.02),
                )
                conn.execute(
                    "INSERT INTO strategy_runtime_state (strategy_name, symbol, last_status, updated_at) VALUES (?, ?, ?, ?)",
                    ("cta", "BTC/USDT", "sideways", "2026-04-15T00:00:00+00:00"),
                )
                conn.commit()

            DatabaseInitializer(db_path).initialize()

            with sqlite3.connect(db_path) as conn:
                market_status_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='market_status'"
                ).fetchone()[0]
                runtime_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='strategy_runtime_state'"
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO market_status (timestamp, symbol, status, adx_value, volatility) VALUES (?, ?, ?, ?, ?)",
                    ("2026-04-15T00:05:00+00:00", "BTC/USDT", "trend_impulse", 28.0, 0.03),
                )
                conn.execute(
                    "INSERT INTO strategy_runtime_state (strategy_name, symbol, last_status, updated_at) VALUES (?, ?, ?, ?)\n                     ON CONFLICT(strategy_name, symbol) DO UPDATE SET last_status=excluded.last_status, updated_at=excluded.updated_at",
                    ("cta", "BTC/USDT", "trend_impulse", "2026-04-15T00:05:00+00:00"),
                )
                status_count = conn.execute("SELECT COUNT(*) FROM market_status").fetchone()[0]
                conn.commit()

            self.assertIn("trend_impulse", market_status_sql)
            self.assertIn("trend_impulse", runtime_sql)
            self.assertEqual(status_count, 2)


if __name__ == "__main__":
    unittest.main()
