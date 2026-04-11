from __future__ import annotations

import math
import unittest

from market_adaptive.indicators import (
    compute_atr,
    compute_bollinger_bands,
    compute_indicator_snapshot,
    compute_kdj,
    compute_obv,
    compute_obv_slope_angle,
    compute_rsi,
    compute_supertrend,
    compute_volume_profile,
    ohlcv_to_dataframe,
)


class IndicatorTests(unittest.TestCase):
    def _build_ohlcv(self, start_price: float, step: float, length: int = 120, step_ms: int = 3_600_000):
        base_timestamp = 1_700_000_000_000
        ohlcv = []
        price = start_price
        for index in range(length):
            price += step
            open_price = price - 0.3
            close_price = price + 0.4
            high_price = close_price + 0.6
            low_price = open_price - 0.5
            volume = 100 + index
            ohlcv.append([base_timestamp + index * step_ms, open_price, high_price, low_price, close_price, volume])
        return ohlcv

    def test_compute_indicator_snapshot_returns_positive_metrics(self) -> None:
        ohlcv = self._build_ohlcv(start_price=100.0, step=0.8)

        snapshot = compute_indicator_snapshot(ohlcv)
        self.assertTrue(math.isfinite(snapshot.adx_value))
        self.assertTrue(math.isfinite(snapshot.plus_di_value))
        self.assertTrue(math.isfinite(snapshot.minus_di_value))
        self.assertTrue(math.isfinite(snapshot.bb_width))
        self.assertTrue(math.isfinite(snapshot.volatility))
        self.assertGreaterEqual(snapshot.adx_value, 0.0)
        self.assertGreaterEqual(snapshot.di_gap, 0.0)
        self.assertGreaterEqual(snapshot.bb_width, 0.0)
        self.assertGreaterEqual(snapshot.volatility, 0.0)

    def test_supertrend_atr_and_obv_follow_uptrend(self) -> None:
        frame = ohlcv_to_dataframe(self._build_ohlcv(start_price=80.0, step=1.2))

        atr = compute_atr(frame, length=14)
        obv = compute_obv(frame)
        supertrend = compute_supertrend(frame, length=10, multiplier=3.0)

        self.assertGreater(float(atr.iloc[-1]), 0.0)
        self.assertGreater(float(obv.iloc[-1]), 0.0)
        self.assertEqual(int(supertrend["direction"].iloc[-1]), 1)

    def test_supertrend_and_obv_flip_negative_in_downtrend(self) -> None:
        frame = ohlcv_to_dataframe(self._build_ohlcv(start_price=220.0, step=-1.1))

        obv = compute_obv(frame)
        supertrend = compute_supertrend(frame, length=10, multiplier=3.0)

        self.assertLess(float(obv.iloc[-1]), 0.0)
        self.assertEqual(int(supertrend["direction"].iloc[-1]), -1)

    def test_compute_bollinger_bands_returns_valid_bounds(self) -> None:
        frame = ohlcv_to_dataframe(self._build_ohlcv(start_price=100.0, step=0.5, length=40))
        bands = compute_bollinger_bands(frame, length=20, std=2.0)
        self.assertGreater(float(bands["upper"].iloc[-1]), float(bands["lower"].iloc[-1]))
        self.assertGreaterEqual(float(bands["width"].iloc[-1]), 0.0)

    def test_compute_rsi_is_above_fifty_in_sustained_uptrend(self) -> None:
        frame = ohlcv_to_dataframe(self._build_ohlcv(start_price=100.0, step=0.9, length=60, step_ms=3_600_000))

        rsi = compute_rsi(frame, length=14)

        self.assertGreater(float(rsi.iloc[-1]), 50.0)

    def test_compute_kdj_detects_latest_golden_cross(self) -> None:
        closes = [100.0, 99.5, 99.0, 98.5, 98.0, 97.5, 97.0, 97.2, 97.0, 99.0]
        base_timestamp = 1_700_000_000_000
        ohlcv = []
        for index, close in enumerate(closes):
            ohlcv.append([base_timestamp + index * 900_000, close - 0.4, close + 0.5, close - 0.7, close, 150 + index * 10])
        frame = ohlcv_to_dataframe(ohlcv)

        kdj = compute_kdj(frame, length=5, k_smoothing=3, d_smoothing=3)

        self.assertLessEqual(float(kdj["k"].iloc[-2]), float(kdj["d"].iloc[-2]))
        self.assertGreater(float(kdj["k"].iloc[-1]), float(kdj["d"].iloc[-1]))

    def test_compute_obv_slope_angle_is_positive_for_breakout_sequence(self) -> None:
        frame = ohlcv_to_dataframe(self._build_ohlcv(start_price=90.0, step=1.0, length=40, step_ms=900_000))

        angle = compute_obv_slope_angle(frame, window=8)

        self.assertGreater(angle, 30.0)

    def test_compute_volume_profile_returns_poc_and_value_area(self) -> None:
        base_timestamp = 1_700_000_000_000
        ohlcv = []
        for index in range(48):
            close = 100.0 + (0.2 if index % 2 == 0 else -0.2)
            ohlcv.append([base_timestamp + index * 900_000, 99.8, 100.4, 99.6, close, 200.0])
        for index in range(8):
            price = 104.0 + index * 0.5
            ohlcv.append([base_timestamp + (48 + index) * 900_000, price - 0.3, price + 0.4, price - 0.5, price, 120.0])

        frame = ohlcv_to_dataframe(ohlcv)
        profile = compute_volume_profile(frame, lookback_hours=24, bin_count=20)

        self.assertIsNotNone(profile)
        assert profile is not None
        self.assertGreater(profile.total_volume, 0.0)
        self.assertLess(profile.value_area_low, profile.value_area_high)
        self.assertGreater(profile.poc_price, 99.0)
        self.assertLess(profile.poc_price, 101.5)
        self.assertTrue(profile.contains_price(100.0))
        self.assertTrue(profile.above_value_area(108.0))


if __name__ == "__main__":
    unittest.main()
