from __future__ import annotations

import math
import unittest

from market_adaptive.indicators import (
    compute_atr,
    compute_bollinger_bands,
    compute_indicator_snapshot,
    compute_obv,
    compute_supertrend,
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
        self.assertTrue(math.isfinite(snapshot.bb_width))
        self.assertTrue(math.isfinite(snapshot.volatility))
        self.assertGreaterEqual(snapshot.adx_value, 0.0)
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


if __name__ == "__main__":
    unittest.main()
