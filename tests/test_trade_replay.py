from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "analyze_trade_opportunities.py"
spec = importlib.util.spec_from_file_location("analyze_trade_opportunities", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
analyze_trade_opportunities = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = analyze_trade_opportunities
spec.loader.exec_module(analyze_trade_opportunities)


class StopReplay(Exception):
    pass


class ReplayTriggerClassificationTests(unittest.TestCase):
    def test_replay_imports_same_waiting_trigger_classifier_as_live_engine(self) -> None:
        from market_adaptive.strategies.mtf_engine import classify_waiting_execution_trigger

        self.assertIs(analyze_trade_opportunities.classify_waiting_execution_trigger, classify_waiting_execution_trigger)

    def test_replay_imports_same_short_regime_thaw_helper_as_live_oracle(self) -> None:
        from market_adaptive.oracles.market_oracle import snapshot_supports_short_regime_thaw

        self.assertIs(analyze_trade_opportunities.snapshot_supports_short_regime_thaw, snapshot_supports_short_regime_thaw)


class ReplayExecutionCandlePreferenceTests(unittest.TestCase):
    def test_replay_cta_respects_execution_candle_preference(self) -> None:
        cfg = SimpleNamespace(
            okx=SimpleNamespace(),
            execution=SimpleNamespace(),
            cta=SimpleNamespace(
                symbol="BTC/USDT",
                major_timeframe="4h",
                swing_timeframe="1h",
                execution_timeframe="15m",
                prefer_closed_major_timeframe_candles=True,
                prefer_closed_swing_timeframe_candles=True,
                prefer_closed_execution_timeframe_candles=False,
            ),
            market_oracle=SimpleNamespace(
                higher_timeframe="1h",
                lower_timeframe="15m",
                prefer_closed_higher_timeframe_candles=True,
                prefer_closed_lower_timeframe_candles=False,
            ),
            sentiment=SimpleNamespace(timeframe="5m"),
        )
        recorded_calls: list[tuple[str, bool]] = []

        def fake_fetch_ohlcv_df(client, symbol, timeframe, *, limit_per_call=200, prefer_closed=True):
            del client, symbol, limit_per_call
            recorded_calls.append((timeframe, prefer_closed))
            if len(recorded_calls) >= 5:
                raise StopReplay
            return pd.DataFrame({"timestamp": []})

        with (
            patch.object(analyze_trade_opportunities, "load_config", return_value=cfg),
            patch.object(analyze_trade_opportunities, "OKXClient"),
            patch.object(analyze_trade_opportunities, "fetch_ohlcv_df", side_effect=fake_fetch_ohlcv_df),
        ):
            with self.assertRaises(StopReplay):
                analyze_trade_opportunities.replay_cta(Path("config/config.yaml"), hours=24)

        self.assertEqual(
            recorded_calls,
            [
                ("4h", True),
                ("1h", True),
                ("15m", False),
                ("1h", True),
                ("15m", False),
            ],
        )


class ReplayBearishPathTests(unittest.TestCase):
    def test_replay_cta_handles_bearish_breakdown_path_without_prior_low_nameerror(self) -> None:
        timestamps = pd.date_range("2026-04-14", periods=130, freq="15min", tz="UTC")
        execution = pd.DataFrame({
            "timestamp": timestamps,
            "open": [100.0] * 129 + [91.0],
            "high": [101.0] * 129 + [92.0],
            "low": [100.0] * 129 + [89.0],
            "close": [100.5] * 129 + [90.0],
            "volume": [1000.0] * 130,
        })
        major = execution.copy()
        swing = execution.copy()
        oracle = execution.copy()

        cfg = SimpleNamespace(
            okx=SimpleNamespace(),
            execution=SimpleNamespace(),
            risk_control=SimpleNamespace(resolve_symbol_notional_limit=lambda symbol: 1_000_000.0),
            cta=SimpleNamespace(
                symbol="BTC/USDT",
                major_timeframe="4h",
                swing_timeframe="1h",
                execution_timeframe="15m",
                prefer_closed_major_timeframe_candles=True,
                prefer_closed_swing_timeframe_candles=True,
                prefer_closed_execution_timeframe_candles=True,
                lookback_limit=120,
                supertrend_period=10,
                supertrend_multiplier=3.0,
                swing_rsi_period=14,
                recovery_rsi_sma_period=3,
                recovery_rsi_floor=45.0,
                dynamic_rsi_floor=45.0,
                dynamic_rsi_trend_score=10.0,
                rsi_rebound_lookback=3,
                rsi_oversold_threshold=30.0,
                rsi_rebound_confirmation_level=40.0,
                dynamic_rsi_rebound_score=5.0,
                recovery_ema_period=5,
                recovery_ema_slope_lookback=2,
                recovery_ema_flat_tolerance_atr_ratio=0.1,
                strong_bull_bias_score=60.0,
                weak_bull_bias_score=20.0,
                early_bullish_lower_band_slope_atr_threshold=0.5,
                early_bullish_score_bonus=10.0,
                swing_rsi_ready_threshold=45.0,
                bullish_ready_score_threshold=55.0,
                drive_first_tradeable_score=60.0,
                kdj_length=9,
                kdj_k_smoothing=3,
                kdj_d_smoothing=3,
                kdj_signal_memory_bars=3,
                execution_breakout_lookback=3,
                starter_frontrun_breakout_buffer_ratio=0.002,
                starter_frontrun_impulse_bars=3,
                starter_frontrun_volume_window=12,
                starter_frontrun_volume_multiplier=1.15,
                obv_sma_period=5,
                obv_zscore_window=5,
                obv_zscore_threshold=0.6,
                atr_period=14,
                magnetism_rail_atr_multiplier=1.0,
                magnetism_obv_zscore_threshold=0.5,
                bullish_memory_retest_breakout_buffer_ratio=0.0026,
                volume_profile_lookback_hours=24,
                volume_profile_value_area_pct=0.7,
                volume_profile_bin_count=24,
                margin_fraction_per_trade=0.01,
                nominal_leverage=1.0,
            ),
            market_oracle=SimpleNamespace(
                higher_timeframe="1h",
                lower_timeframe="15m",
                prefer_closed_higher_timeframe_candles=True,
                prefer_closed_lower_timeframe_candles=True,
                lookback_limit=120,
                adx_length=14,
                bb_length=20,
                bb_std=2.0,
            ),
            sentiment=SimpleNamespace(timeframe="5m", extreme_bullish_ratio=2.0, normalized_cta_buy_action="block"),
        )

        class DummyClient:
            def fetch_long_short_account_ratio_history(self, symbol, timeframe="5m", limit=1):
                return []

            def fetch_total_equity(self, currency):
                return 1000.0

        class DummyIndicatorSnapshot:
            adx_trend_label = "rising"

        class DummyOBVSnapshot:
            zscore = -1.0
            above_sma = False
            below_sma = True

            def buy_confirmed(self, zscore_threshold=0.6):
                return False

            def sell_confirmed(self, zscore_threshold=0.6):
                return True

        class DummyVolumeProfile:
            poc_price = 95.0
            value_area_low = 90.0
            value_area_high = 98.0

            def contains_price(self, price):
                return False

            def above_poc(self, price):
                return False

            def above_value_area(self, price):
                return False

        def fake_fetch_ohlcv_df(client, symbol, timeframe, *, limit_per_call=200, prefer_closed=True):
            del client, symbol, limit_per_call, prefer_closed
            if timeframe == cfg.cta.major_timeframe:
                return major
            if timeframe == cfg.cta.swing_timeframe:
                return swing
            if timeframe == cfg.cta.execution_timeframe:
                return execution
            return oracle

        def fake_supertrend(frame, length, multiplier):
            del length, multiplier
            direction = -1 if len(frame) >= 120 else 1
            return pd.DataFrame({
                "direction": [direction] * len(frame),
                "lower_band": [80.0] * len(frame),
                "upper_band": [110.0] * len(frame),
                "atr": [2.0] * len(frame),
            })

        def fake_rsi(frame, length):
            del frame, length
            return pd.Series([50.0] * 128 + [44.0, 40.0])

        def fake_kdj(frame, length, k_smoothing, d_smoothing):
            del frame, length, k_smoothing, d_smoothing
            values = [60.0] * 128 + [70.0, 20.0]
            ds = [55.0] * 128 + [60.0, 40.0]
            return pd.DataFrame({"k": values, "d": ds})

        with (
            patch.object(analyze_trade_opportunities, "load_config", return_value=cfg),
            patch.object(analyze_trade_opportunities, "OKXClient", return_value=DummyClient()),
            patch.object(analyze_trade_opportunities, "fetch_ohlcv_df", side_effect=fake_fetch_ohlcv_df),
            patch.object(analyze_trade_opportunities, "compute_indicator_snapshot", return_value=DummyIndicatorSnapshot()),
            patch.object(analyze_trade_opportunities, "indicator_confirms_trend", return_value=True),
            patch.object(analyze_trade_opportunities, "compute_supertrend", side_effect=fake_supertrend),
            patch.object(analyze_trade_opportunities, "compute_rsi", side_effect=fake_rsi),
            patch.object(analyze_trade_opportunities, "compute_kdj", side_effect=fake_kdj),
            patch.object(analyze_trade_opportunities, "compute_obv", return_value=pd.Series([1.0] * 130)),
            patch.object(analyze_trade_opportunities, "compute_obv_confirmation_snapshot", return_value=DummyOBVSnapshot()),
            patch.object(analyze_trade_opportunities, "compute_atr", return_value=pd.Series([2.0] * 130)),
            patch.object(analyze_trade_opportunities, "compute_volume_profile", return_value=DummyVolumeProfile()),
        ):
            result = analyze_trade_opportunities.replay_cta(Path("config/config.yaml"), hours=12)

        self.assertGreater(result["bearish_ready_count"], 0)
        self.assertGreater(result["bearish_trigger_count"], 0)
        self.assertGreater(result["bearish_live_direction_count"], 0)
        self.assertEqual(result["direction_counts"].get(-1), result["bearish_live_direction_count"])

    def test_replay_short_recovery_grace_preserves_recent_bearish_obv_confirmation(self) -> None:
        from market_adaptive.strategies.obv_gate import detect_recent_short_obv_confirmation, resolve_dynamic_obv_gate

        exec_frame = pd.DataFrame(
            {
                "open": [100.0, 99.0, 98.0],
                "high": [101.0, 100.0, 99.0],
                "low": [99.0, 98.0, 97.0],
                "close": [99.0, 98.0, 98.2],
                "volume": [10.0, 10.0, 5.0],
            }
        )

        gate = resolve_dynamic_obv_gate(
            bullish_score=50.0,
            configured_threshold=0.6,
            side="short",
            early_bearish=True,
            execution_entry_mode="early_bearish_starter_limit",
            trigger_reason="early_bearish: distribution rollover after failed push",
            recent_short_obv_confirmation=detect_recent_short_obv_confirmation(exec_frame, sma_period=2, zscore_window=2),
        )

        self.assertTrue(gate.short_recovery_grace_active)

    
if __name__ == "__main__":
    unittest.main()
