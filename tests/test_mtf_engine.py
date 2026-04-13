from __future__ import annotations

import unittest
from unittest.mock import patch

from market_adaptive.config import CTAConfig
from market_adaptive.strategies.mtf_engine import MultiTimeframeSignalEngine


class DummyClient:
    def __init__(self) -> None:
        self.ohlcv_by_timeframe: dict[str, list[list[float]]] = {}

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        del symbol, since
        return self.ohlcv_by_timeframe.get(timeframe, [])[-limit:]


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
        base = 1_700_000_000_000
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
        self.assertEqual(signal.execution_trigger.reason, "waiting_execution_trigger")

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
        self.assertTrue(signal.bullish_ready)
        self.assertEqual(signal.execution_entry_mode, "weak_bull_scale_in_limit")
        self.assertTrue(signal.fully_aligned)
        self.assertIn("scale-in allowed before breakout", signal.execution_trigger.reason)

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
            strong_bull_bias_score=0.7,
            dynamic_rsi_trend_score=0.7,
            bullish_ready_score_threshold=1.6,
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
        self.assertTrue(signal.bullish_ready)
        self.assertFalse(signal.fully_aligned)
        self.assertIn("磁吸力预判：距离轨道", signal.execution_trigger.reason)
        self.assertIn("OBV 已确认", signal.execution_trigger.reason)

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


if __name__ == "__main__":
    unittest.main()
