from __future__ import annotations

import unittest
from unittest.mock import patch

from market_adaptive.config import CTAConfig
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.mtf_engine import MultiTimeframeSignalEngine, classify_waiting_execution_trigger


class DummyClient:
    def __init__(self) -> None:
        self.ohlcv_by_timeframe: dict[str, list[list[float]]] = {}
        self.server_time_ms: int | None = 1_700_000_123_000

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        del symbol, since
        return self.ohlcv_by_timeframe.get(timeframe, [])[-limit:]

    def fetch_server_time(self) -> int | None:
        return self.server_time_ms


class WaitingExecutionTriggerClassificationTests(unittest.TestCase):
    def test_classifies_near_breakout_waiting(self) -> None:
        reason = classify_waiting_execution_trigger(
            bullish_ready=True,
            state_label="ARMED_READY",
            bullish_memory_active=False,
            bullish_latch_active=False,
            bullish_urgency_active=False,
            prior_high_break=False,
            frontrun_near_breakout=True,
            frontrun_gap_ratio=0.0015,
            execution_trigger_proximity_budget_ratio=0.0026,
        )

        self.assertEqual(reason, "waiting_execution_trigger_near_breakout")

    def test_classifies_memory_desync_waiting(self) -> None:
        reason = classify_waiting_execution_trigger(
            bullish_ready=True,
            state_label="WAITING_SETUP",
            bullish_memory_active=True,
            bullish_latch_active=False,
            bullish_urgency_active=False,
            prior_high_break=False,
            frontrun_near_breakout=False,
            frontrun_gap_ratio=0.0020,
            execution_trigger_proximity_budget_ratio=0.0026,
        )

        self.assertEqual(reason, "waiting_execution_trigger_memory_desync")

    def test_classifies_drift_waiting(self) -> None:
        reason = classify_waiting_execution_trigger(
            bullish_ready=True,
            state_label="WAITING_SETUP",
            bullish_memory_active=False,
            bullish_latch_active=False,
            bullish_urgency_active=False,
            prior_high_break=False,
            frontrun_near_breakout=False,
            frontrun_gap_ratio=0.0035,
            execution_trigger_proximity_budget_ratio=0.0026,
        )

        self.assertEqual(reason, "waiting_execution_trigger_drift")

    def test_downgrades_stale_memory_desync_to_drift_when_price_has_left_retest_zone(self) -> None:
        reason = classify_waiting_execution_trigger(
            bullish_ready=True,
            state_label="WAITING_SETUP",
            bullish_memory_active=True,
            bullish_latch_active=True,
            bullish_urgency_active=True,
            prior_high_break=False,
            frontrun_near_breakout=False,
            frontrun_gap_ratio=0.0042,
            execution_trigger_proximity_budget_ratio=0.0026,
        )

        self.assertEqual(reason, "waiting_execution_trigger_drift")


class MTFEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DummyClient()
        self.config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            execution_breakout_lookback=3,
            kdj_length=5,
        )
        self.engine = MultiTimeframeSignalEngine(self.client, self.config)

    def _set_ohlcv(self, timeframe: str, closes: list[float], step_ms: int, volumes: list[float] | None = None) -> None:
        common_end = 1_700_086_400_000
        base = common_end - len(closes) * step_ms
        payload = []
        for index, close in enumerate(closes):
            volume = volumes[index] if volumes is not None else 100 + index * 5
            payload.append([base + index * step_ms, close - 0.3, close + 0.4, close - 0.6, close, volume])
        self.client.ohlcv_by_timeframe[timeframe] = payload

    def _load_bullish_major_and_swing(self) -> None:
        swing_closes = [140 - 1.0 * (59 - index) for index in range(60)]
        major_closes = [220 - 2.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("4h", major_closes, 14_400_000)

    def test_engine_builds_bullish_ready_without_execution_trigger(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(55)] + [104.0, 103.4, 102.9, 102.4, 101.9]
        self._set_ohlcv("15m", execution_closes, 900_000)

        signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertGreater(signal.bullish_score, signal.bullish_threshold)
        self.assertFalse(signal.weak_bull_bias)
        self.assertFalse(signal.execution_trigger.kdj_golden_cross)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_drift")

    def test_engine_confirms_entry_when_execution_breaks_prior_high(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = []
        base_price = 92.0
        pattern = [0.0, 0.4, -0.3, 0.5, -0.2, 0.3, -0.1, 0.2]
        for index in range(52):
            execution_closes.append(base_price + pattern[index % len(pattern)])
        execution_closes.extend([94.4, 95.2, 96.1, 97.0, 98.0, 99.0, 99.4, 100.0])
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=1)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("Price Breakout NOW", signal.execution_trigger.reason)

    def test_engine_allows_major_bull_retest_entry_when_kdj_memory_is_fresh_and_price_is_near_breakout(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.execution_trigger.bullish_memory_active)
        self.assertTrue(signal.fully_aligned)
        self.assertTrue(any(tag in signal.execution_trigger.reason for tag in ("major_bull_retest_ready", "price_led_override")))

    def test_engine_allows_slightly_wider_major_bull_retest_window_only_with_active_bullish_memory(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.93]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertTrue(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertFalse(signal.execution_trigger.frontrun_near_breakout)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("major_bull_retest_ready", signal.execution_trigger.reason)

    def test_engine_keeps_major_bull_retest_blocked_when_kdj_memory_has_expired_even_if_price_is_near_breakout(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.93]
        self._set_ohlcv("15m", execution_closes, 900_000)

        import pandas as pd
        mocked_kdj = pd.DataFrame({"k": [56.0] * 60, "d": [51.0] * 60})
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_drift")

    def test_engine_keeps_short_decaying_urgency_window_after_memory_expires_near_breakout(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            kdj_signal_memory_bars=5,
            kdj_urgency_decay_bars=2,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.93]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=5)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertTrue(signal.execution_trigger.bullish_urgency_active)
        self.assertEqual(signal.execution_trigger.bullish_urgency_decay_step, 1)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("decaying urgency window step=1/2", signal.execution_trigger.reason)

    def test_engine_expires_decaying_urgency_window_after_configured_bars(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            kdj_signal_memory_bars=5,
            kdj_urgency_decay_bars=2,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.93]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=7)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.bullish_urgency_active)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_near_breakout")

    def test_engine_blocks_decaying_urgency_window_on_kdj_dead_cross(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            kdj_signal_memory_bars=5,
            kdj_urgency_decay_bars=2,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.93]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=5)
        mocked_kdj.loc[58, ["k", "d"]] = [56.0, 51.0]
        mocked_kdj.loc[59, ["k", "d"]] = [49.0, 54.0]
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.bullish_urgency_active)
        self.assertTrue(signal.execution_trigger.bearish_latch_active)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_drift")

    def test_engine_allows_major_bull_impulse_reclaim_after_breakout_when_kdj_memory_expired(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.2 for index in range(54)] + [100.0, 100.4, 100.9, 101.7, 102.8, 103.9]
        execution_volumes = [100.0 + index for index in range(54)] + [140.0, 150.0, 160.0, 240.0, 260.0, 280.0]
        self._set_ohlcv("15m", execution_closes, 900_000, volumes=execution_volumes)

        import pandas as pd
        mocked_kdj = pd.DataFrame({"k": [56.0] * 60, "d": [51.0] * 60})
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.execution_trigger.frontrun_impulse_confirmed)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("major_bull_impulse_reclaim_ready", signal.execution_trigger.reason)

    def test_engine_allows_major_bull_impulse_reclaim_near_breakout_with_obv_when_kdj_memory_expired(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            starter_frontrun_breakout_buffer_ratio=0.002,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        execution_volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        self._set_ohlcv("15m", execution_closes, 900_000, volumes=execution_volumes)

        import pandas as pd
        mocked_kdj = pd.DataFrame({"k": [56.0] * 60, "d": [51.0] * 60})
        confirmed_obv = OBVConfirmationSnapshot(
            current_obv=10.0,
            sma_value=1.0,
            increment_value=2.0,
            increment_mean=0.5,
            increment_std=0.3,
            zscore=2.0,
        )
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj), patch(
            "market_adaptive.strategies.mtf_engine.compute_obv_confirmation_snapshot",
            return_value=confirmed_obv,
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.execution_trigger.frontrun_near_breakout)
        self.assertTrue(signal.execution_trigger.frontrun_impulse_confirmed)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("major_bull_impulse_reclaim_ready", signal.execution_trigger.reason)

    def test_engine_allows_weak_bull_bias_before_major_supertrend_flip(self) -> None:
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0] * 20 + [100.2, 100.4, 100.7, 101.0, 101.3, 101.8, 102.2, 102.7, 103.1, 103.5, 103.9, 104.4, 104.8, 105.2, 105.7, 106.1, 106.5, 106.9, 107.2, 107.5, 107.9, 108.2, 108.5, 108.9, 109.2, 109.6, 110.0, 110.3, 110.7, 111.0, 111.4, 111.8, 112.2, 112.5, 112.9, 113.3, 113.6, 114.0, 114.4, 114.8]
        execution_closes = [100.0] * 56 + [100.2, 100.3, 100.4, 100.5]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertLess(signal.major_direction, 0)
        self.assertTrue(signal.weak_bull_bias)
        self.assertFalse(signal.bullish_ready)
        self.assertEqual(signal.execution_entry_mode, "weak_bull_scale_in_limit")
        self.assertFalse(signal.fully_aligned)
        self.assertIn("scale-in allowed before breakout", signal.execution_trigger.reason)

    def test_engine_flags_early_bullish_when_fast_supertrend_leads_and_major_lower_band_flattens(self) -> None:
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        self._set_ohlcv("1h", [100.0 + 0.2 * index for index in range(60)], 3_600_000)
        self._set_ohlcv("15m", [100.0] * 59 + [106.0], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 59 + [1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.early_bullish)
        self.assertTrue(signal.fully_aligned)
        self.assertEqual(signal.execution_entry_mode, "early_bullish_starter_limit")
        self.assertAlmostEqual(signal.entry_size_multiplier, self.config.early_bullish_starter_fraction)

    def test_engine_does_not_flag_early_bullish_when_major_lower_band_slope_is_still_too_negative(self) -> None:
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        self._set_ohlcv("1h", [100.0 + 0.2 * index for index in range(60)], 3_600_000)
        self._set_ohlcv("15m", [100.0] * 59 + [106.0], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 103.8],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 59 + [1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.early_bullish)
        self.assertNotEqual(signal.execution_entry_mode, "early_bullish_starter_limit")

    def test_engine_adds_early_bullish_recovery_bonus_only_when_rsi_structure_is_still_supported(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            swing_supertrend_bullish_score=30.0,
            dynamic_rsi_trend_score=15.0,
            early_bullish_score_bonus=10.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 45 + [99.8, 99.6, 99.7, 99.9, 100.2, 100.6, 101.1, 101.7, 102.4, 103.2, 104.1, 105.0, 106.0, 107.0, 108.0]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", [100.0] * 59 + [106.0], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 59 + [1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.early_bullish)
        self.assertEqual(signal.bullish_score, 55.0)
        self.assertTrue(signal.bullish_ready)

    def test_engine_keeps_early_bullish_bonus_off_when_rsi_falls_back_under_its_sma(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            swing_supertrend_bullish_score=30.0,
            early_bullish_score_bonus=10.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 45 + [100.2, 100.6, 101.1, 101.7, 102.4, 103.2, 104.1, 105.0, 106.0, 107.0, 108.0, 107.2, 106.6, 106.1, 105.7]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", [100.0] * 59 + [106.0], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 59 + [1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.early_bullish)
        self.assertLess(signal.swing_rsi_slope, 0.0)
        self.assertEqual(signal.bullish_score, 30.0)
        self.assertFalse(signal.bullish_ready)

    def test_engine_dynamic_rsi_ready_on_positive_slope_above_45(self) -> None:
        self._set_ohlcv("4h", [220 - 2.0 * (59 - index) for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 45 + [99.8, 99.6, 99.7, 99.9, 100.2, 100.6, 101.1, 101.7, 102.4, 103.2, 104.1, 105.0, 106.0, 107.0, 108.0]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", [100.0] * 56 + [100.4, 100.6, 100.8, 101.4], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=3)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertGreater(signal.swing_rsi, 45.0)
        self.assertGreater(signal.swing_rsi_slope, 0.0)
        self.assertTrue(signal.bullish_ready)

    def test_engine_allows_magnetism_ready_before_major_supertrend_flip(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            prefer_closed_major_timeframe_candles=False,
            prefer_closed_swing_timeframe_candles=False,
            strong_bull_bias_score=60.0,
            swing_supertrend_bullish_score=30.0,
            dynamic_rsi_trend_score=15.0,
            kdj_memory_score_bonus=10.0,
            magnetism_score_bonus=20.0,
            bullish_ready_score_threshold=55.0,
            magnetism_obv_zscore_threshold=1.2,
            magnetism_rail_atr_multiplier=0.6,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0] * 45 + [99.8, 99.9, 100.0, 100.1, 100.3, 100.6, 100.9, 101.2, 101.6, 102.0, 102.3, 102.7, 103.0, 103.3, 103.6]
        execution_closes = [100.0] * 59 + [173.2]
        execution_volumes = [100.0] * 59 + [1000.0]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000, volumes=execution_volumes)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertLess(signal.major_direction, 0)
        self.assertGreaterEqual(signal.bullish_score, signal.bullish_threshold)
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.fully_aligned)
        self.assertIn("磁吸力预判：距离轨道", signal.execution_trigger.reason)
        self.assertIn("OBV 已确认", signal.execution_trigger.reason)


    def test_engine_scores_weak_bull_stack_to_threshold_and_logs_breakdown(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            swing_supertrend_bullish_score=30.0,
            dynamic_rsi_trend_score=15.0,
            dynamic_rsi_rebound_score=15.0,
            kdj_memory_score_bonus=10.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0] * 20 + [100.2, 100.4, 100.7, 101.0, 101.3, 101.8, 102.2, 102.7, 103.1, 103.5, 103.9, 104.4, 104.8, 105.2, 105.7, 106.1, 106.5, 106.9, 107.2, 107.5, 107.9, 108.2, 108.5, 108.9, 109.2, 109.6, 110.0, 110.3, 110.7, 111.0, 111.4, 111.8, 112.2, 112.5, 112.9, 113.3, 113.6, 114.0, 114.4, 114.8]
        execution_closes = [100.0] * 56 + [100.2, 100.3, 100.4, 100.5]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with self.assertLogs("market_adaptive.strategies.mtf_engine", level="INFO") as logs:
            with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
                signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.weak_bull_bias)
        self.assertTrue(signal.execution_trigger.bullish_memory_active)
        self.assertEqual(signal.bullish_score, 40.0)
        self.assertFalse(signal.bullish_ready)
        joined_logs = "\n".join(logs.output)
        self.assertIn("Bullish Score: 40/55 [4H: 0, 1H: 30, Magnet: 0, RSI: 0, Early: 0, KDJ: 10]", joined_logs)

    def test_drive_first_major_trend_does_not_report_rsi_block_when_score_is_tradeable(self) -> None:
        signal = self.engine._resolve_blocker_reason(
            data_alignment_valid=True,
            major_direction=1,
            weak_bull_bias=False,
            early_bullish=False,
            swing_score=0.0,
            bullish_ready=True,
            fully_aligned=False,
            execution_reason="waiting_execution_trigger",
            bullish_score=60.0,
            execution_frontrun_near_breakout=False,
            drive_first_tradeable=True,
            rsi_rollover_blocked=False,
        )

        self.assertEqual(signal, "Blocked_By_Trigger:waiting_execution_trigger")

    def test_drive_first_major_trend_blocks_only_extreme_rsi_rollover(self) -> None:
        signal = self.engine._resolve_blocker_reason(
            data_alignment_valid=True,
            major_direction=1,
            weak_bull_bias=False,
            early_bullish=False,
            swing_score=0.0,
            bullish_ready=True,
            fully_aligned=False,
            execution_reason="waiting_execution_trigger",
            bullish_score=72.0,
            execution_frontrun_near_breakout=False,
            drive_first_tradeable=True,
            rsi_rollover_blocked=True,
        )

        self.assertEqual(signal, "Blocked_By_RSI_ROLLOVER")

    def test_engine_scores_bullish_ready_before_major_flip_via_swing_and_memory_stack(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=60.0,
            swing_supertrend_bullish_score=30.0,
            dynamic_rsi_trend_score=15.0,
            kdj_memory_score_bonus=10.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0 + 0.35 * index for index in range(60)]
        execution_closes = [100.0] * 56 + [100.2, 100.35, 100.5, 100.65]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertLess(signal.major_direction, 0)
        self.assertEqual(signal.bullish_score, 40.0)
        self.assertFalse(signal.bullish_ready)
        self.assertFalse(signal.fully_aligned)

    def test_engine_scores_structural_recovery_proxy_when_price_reclaims_ema21_and_slope_is_flat_up(self) -> None:
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 30 + [99.5, 99.2, 99.0, 99.1, 99.3, 99.6, 100.0, 100.4, 100.9, 101.4, 101.9, 102.3, 102.7, 103.0, 103.3, 103.6, 103.9, 104.2, 104.5, 104.8, 105.0, 105.2, 105.4, 105.6, 105.8, 106.0, 106.2, 106.4, 106.6, 106.8]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", [100.0] * 60, 900_000)

        import pandas as pd
        major_supertrend = pd.DataFrame({"direction": [-1] * 60, "lower_band": [100.0] * 60, "upper_band": [180.0] * 60, "supertrend": [180.0] * 60, "atr": [2.0] * 60})
        swing_supertrend = pd.DataFrame({"direction": [-1] * 60, "lower_band": [95.0] * 60, "upper_band": [120.0] * 60, "supertrend": [120.0] * 60, "atr": [1.0] * 60})
        with patch("market_adaptive.strategies.mtf_engine.compute_supertrend", side_effect=[major_supertrend, swing_supertrend, swing_supertrend]):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.weak_bull_bias)
        self.assertEqual(signal.bullish_score, float(self.config.weak_bull_bias_score + self.config.dynamic_rsi_trend_score))

    def test_engine_scores_momentum_recovery_proxy_when_rsi_reclaims_its_sma_above_40(self) -> None:
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 45 + [99.8, 99.6, 99.7, 99.9, 100.2, 100.6, 101.1, 101.7, 102.4, 103.2, 104.1, 105.0, 106.0, 107.0, 108.0]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", [100.0] * 60, 900_000)

        import pandas as pd
        major_supertrend = pd.DataFrame({"direction": [-1] * 60, "lower_band": [100.0] * 60, "upper_band": [180.0] * 60, "supertrend": [180.0] * 60, "atr": [2.0] * 60})
        swing_supertrend = pd.DataFrame({"direction": [-1] * 60, "lower_band": [95.0] * 60, "upper_band": [120.0] * 60, "supertrend": [120.0] * 60, "atr": [1.0] * 60})
        with patch("market_adaptive.strategies.mtf_engine.compute_supertrend", side_effect=[major_supertrend, swing_supertrend, swing_supertrend]):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertGreater(signal.swing_rsi, 40.0)
        self.assertGreater(signal.swing_rsi_slope, 0.0)
        self.assertEqual(signal.bullish_score, float(self.config.weak_bull_bias_score + self.config.dynamic_rsi_trend_score))

    def test_engine_unlocks_bullish_ready_via_recovery_proxies_plus_magnetism(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            prefer_closed_major_timeframe_candles=False,
            prefer_closed_swing_timeframe_candles=False,
            weak_bull_bias_score=22.0,
            dynamic_rsi_trend_score=15.0,
            magnetism_score_bonus=20.0,
            bullish_ready_score_threshold=55.0,
            magnetism_obv_zscore_threshold=1.2,
            magnetism_rail_atr_multiplier=0.6,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._set_ohlcv("4h", [200 - 0.5 * index for index in range(60)], 14_400_000)
        swing_closes = [100.0] * 45 + [99.8, 99.6, 99.7, 99.9, 100.2, 100.6, 101.1, 101.7, 102.4, 103.2, 104.1, 105.0, 106.0, 107.0, 108.0]
        execution_closes = [100.0] * 59 + [173.2]
        execution_volumes = [100.0] * 59 + [1000.0]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000, volumes=execution_volumes)

        signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.weak_bull_bias)
        self.assertGreaterEqual(signal.bullish_score, signal.bullish_threshold)
        self.assertTrue(signal.bullish_ready)
        self.assertGreaterEqual(signal.bullish_score, config.weak_bull_bias_score + config.dynamic_rsi_trend_score + config.magnetism_score_bonus)


    def test_engine_uses_price_led_override_for_high_confidence_near_breakout_without_memory(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=75.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        self._set_ohlcv("15m", execution_closes, 900_000)

        import pandas as pd
        mocked_kdj = pd.DataFrame({"k": [56.0] * 60, "d": [51.0] * 60})
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertTrue(signal.execution_trigger.frontrun_near_breakout)
        self.assertTrue(signal.fully_aligned)
        self.assertTrue(signal.rsi_blocking_overridden)
        self.assertEqual(signal.blocker_reason, "PASSED")
        self.assertTrue(any(tag in signal.execution_trigger.reason for tag in ("price_led_override", "trend_continuation_near_breakout_ready")))

    def test_engine_allows_trend_continuation_near_breakout_with_positive_obv_support_after_memory_expiry(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=75.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        self._set_ohlcv("15m", execution_closes, 900_000)

        import pandas as pd
        mocked_kdj = pd.DataFrame({"k": [56.0] * 60, "d": [51.0] * 60})
        supportive_but_not_confirmed_obv = OBVConfirmationSnapshot(
            current_obv=0.0,
            sma_value=1.0,
            increment_value=0.2,
            increment_mean=0.0,
            increment_std=1.0,
            zscore=0.2,
        )
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj), patch(
            "market_adaptive.strategies.mtf_engine.compute_obv_confirmation_snapshot",
            return_value=supportive_but_not_confirmed_obv,
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.execution_trigger.frontrun_near_breakout)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("trend_continuation_near_breakout_ready", signal.execution_trigger.reason)

    def test_engine_keeps_trend_continuation_near_breakout_blocked_on_dead_cross(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=75.0,
            bullish_ready_score_threshold=55.0,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        self._set_ohlcv("15m", execution_closes, 900_000)

        import pandas as pd
        dead_cross_kdj = pd.DataFrame({"k": [56.0] * 58 + [56.0, 49.0], "d": [51.0] * 58 + [51.0, 54.0]})
        supportive_but_not_confirmed_obv = OBVConfirmationSnapshot(
            current_obv=0.0,
            sma_value=1.0,
            increment_value=0.2,
            increment_mean=0.0,
            increment_std=1.0,
            zscore=0.2,
        )
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=dead_cross_kdj), patch(
            "market_adaptive.strategies.mtf_engine.compute_obv_confirmation_snapshot",
            return_value=supportive_but_not_confirmed_obv,
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertTrue(signal.execution_trigger.kdj_dead_cross)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_near_breakout")

    def test_engine_uses_soft_latch_breakout_for_medium_confidence_breakout_after_memory_expiry(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=55.0,
            bullish_ready_score_threshold=55.0,
            kdj_signal_memory_bars=3,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.22 for index in range(54)] + [101.0, 101.4, 101.9, 102.2, 102.6, 103.2]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=4)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertTrue(signal.execution_trigger.bullish_latch_active)
        self.assertIsNotNone(signal.execution_trigger.latch_low_price)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("soft_latch_breakout", signal.execution_trigger.reason)

    def test_engine_resets_soft_latch_after_defended_low_breaks(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            strong_bull_bias_score=55.0,
            bullish_ready_score_threshold=55.0,
            kdj_signal_memory_bars=3,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.22 for index in range(54)] + [101.0, 101.4, 100.8, 101.2, 101.5, 103.2]
        self._set_ohlcv("15m", execution_closes, 900_000)

        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=4)
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertFalse(signal.execution_trigger.bullish_latch_active)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger_near_breakout")

    def test_engine_flags_starter_frontrun_when_breakout_is_within_last_point_two_percent(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            starter_frontrun_enabled=True,
            starter_frontrun_fraction=0.2,
            starter_frontrun_breakout_buffer_ratio=0.002,
            starter_frontrun_impulse_bars=3,
            starter_frontrun_volume_window=12,
            starter_frontrun_volume_multiplier=1.1,
            prefer_closed_execution_timeframe_candles=False,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        execution_volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        self._set_ohlcv("15m", execution_closes, 900_000, volumes=execution_volumes)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.execution_trigger.frontrun_near_breakout)
        self.assertTrue(signal.execution_trigger.frontrun_impulse_confirmed)
        self.assertTrue(signal.execution_trigger.frontrun_obv_confirmed)
        self.assertTrue(signal.execution_trigger.frontrun_ready)
        self.assertTrue(signal.fully_aligned)
        self.assertEqual(signal.execution_entry_mode, "starter_frontrun_limit")
        self.assertAlmostEqual(signal.entry_size_multiplier, 0.2)
        self.assertIn("starter_frontrun", signal.execution_trigger.reason)

    def test_engine_prefers_closed_major_and_swing_candles_but_keeps_live_execution_by_default(self) -> None:
        config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            prefer_closed_major_timeframe_candles=True,
            prefer_closed_swing_timeframe_candles=True,
            prefer_closed_execution_timeframe_candles=False,
        )
        engine = MultiTimeframeSignalEngine(self.client, config)
        self._set_ohlcv("4h", [200.0 + index for index in range(60)] + [999.0], 14_400_000)
        self._set_ohlcv("1h", [100.0 + index for index in range(60)] + [777.0], 3_600_000)
        self._set_ohlcv("15m", [50.0 + index for index in range(60)] + [555.0], 900_000)
        mocked_kdj = self._mock_execution_kdj(bars=61, golden_cross_bar_from_end=10)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(float(signal.major_frame["close"].iloc[-1]), 259.0)
        self.assertEqual(float(signal.swing_frame["close"].iloc[-1]), 159.0)
        self.assertEqual(float(signal.execution_frame["close"].iloc[-1]), 555.0)

    def _mock_execution_kdj(self, bars: int, golden_cross_bar_from_end: int):
        k_values = [40.0] * bars
        d_values = [50.0] * bars
        cross_index = bars - golden_cross_bar_from_end - 1
        k_values[cross_index - 1] = 45.0
        d_values[cross_index - 1] = 50.0
        k_values[cross_index] = 55.0
        d_values[cross_index] = 50.0
        for index in range(cross_index + 1, bars):
            k_values[index] = 56.0
            d_values[index] = 51.0
        import pandas as pd
        return pd.DataFrame({"k": k_values, "d": d_values})

    def test_engine_flags_data_mismatch_and_safe_gates_entry(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [100.0 + index * 0.2 for index in range(60)]
        self._set_ohlcv("15m", execution_closes, 900_000)
        # push the major frame far enough ahead to exceed one 15m period
        shifted_major = []
        for row in self.client.ohlcv_by_timeframe["4h"]:
            shifted_major.append([row[0] + 1_800_000, *row[1:]])
        self.client.ohlcv_by_timeframe["4h"] = shifted_major
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=1)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.data_alignment_valid)
        self.assertGreater(signal.data_mismatch_ms, 900_000)
        self.assertFalse(signal.bullish_ready)
        self.assertFalse(signal.fully_aligned)
        self.assertEqual(signal.blocker_reason, "DATA_MISMATCH_WARNING")

    def test_engine_exposes_audit_fields_for_profiler(self) -> None:
        self._load_bullish_major_and_swing()
        execution_closes = [90 + index * 0.25 for index in range(55)] + [104.0, 103.4, 102.9, 102.4, 101.9]
        self._set_ohlcv("15m", execution_closes, 900_000)

        signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertIsInstance(signal.major_timestamp_ms, int)
        self.assertIsInstance(signal.swing_timestamp_ms, int)
        self.assertIsInstance(signal.execution_timestamp_ms, int)
        self.assertTrue(signal.data_alignment_valid)
        self.assertIsInstance(signal.server_local_skew_ms, int)
        self.assertGreater(signal.execution_atr, 0.0)
        self.assertGreaterEqual(signal.atr_price_ratio_pct, 0.0)


if __name__ == "__main__":
    unittest.main()

class ShortSideMTFEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DummyClient()
        self.config = CTAConfig(symbol="BTC/USDT", major_timeframe="4h", swing_timeframe="1h", execution_timeframe="15m", execution_breakout_lookback=3, kdj_length=5)
        self.engine = MultiTimeframeSignalEngine(self.client, self.config)

    def _set_ohlcv(self, timeframe: str, closes: list[float], step_ms: int) -> None:
        common_end = 1_700_086_400_000
        base = common_end - len(closes) * step_ms
        self.client.ohlcv_by_timeframe[timeframe] = [
            [base + index * step_ms, close + 0.3, close + 0.6, close - 0.4, close, 100 + index * 5]
            for index, close in enumerate(closes)
        ]

    def _mock_bearish_kdj(self, bars: int, dead_cross_bar_from_end: int):
        import pandas as pd
        k_values = [60.0] * bars
        d_values = [50.0] * bars
        cross_index = bars - dead_cross_bar_from_end - 1
        k_values[cross_index - 1] = 55.0
        d_values[cross_index - 1] = 50.0
        k_values[cross_index] = 45.0
        d_values[cross_index] = 50.0
        for index in range(cross_index + 1, bars):
            k_values[index] = 44.0
            d_values[index] = 49.0
        return pd.DataFrame({"k": k_values, "d": d_values})

    def test_engine_can_emit_bearish_ready_breakdown_path(self) -> None:
        self._set_ohlcv("4h", [260 - 2.0 * index for index in range(60)], 14_400_000)
        self._set_ohlcv("1h", [160 - 1.0 * index for index in range(60)], 3_600_000)
        execution_closes = [120 - index * 0.25 for index in range(55)] + [106.0, 105.2, 104.8, 104.1, 103.4]
        self._set_ohlcv("15m", execution_closes, 900_000)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=self._mock_bearish_kdj(bars=60, dead_cross_bar_from_end=1)):
            signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bearish_ready)
        self.assertGreater(signal.bearish_score, 0.0)
        self.assertTrue(signal.execution_trigger.prior_low_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("Bearish", signal.execution_trigger.reason)
