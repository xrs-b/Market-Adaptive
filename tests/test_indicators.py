from __future__ import annotations

import math
import unittest

from market_adaptive.indicators import compute_indicator_snapshot


class IndicatorTests(unittest.TestCase):
    def test_compute_indicator_snapshot_returns_positive_metrics(self) -> None:
        base_timestamp = 1_700_000_000_000
        ohlcv = []
        price = 100.0
        for index in range(120):
            price += 0.8 + (index % 5) * 0.05
            open_price = price - 0.3
            close_price = price + 0.4
            high_price = close_price + 0.6
            low_price = open_price - 0.5
            volume = 100 + index
            ohlcv.append([base_timestamp + index * 3_600_000, open_price, high_price, low_price, close_price, volume])

        snapshot = compute_indicator_snapshot(ohlcv)
        self.assertTrue(math.isfinite(snapshot.adx_value))
        self.assertTrue(math.isfinite(snapshot.bb_width))
        self.assertTrue(math.isfinite(snapshot.volatility))
        self.assertGreaterEqual(snapshot.adx_value, 0.0)
        self.assertGreaterEqual(snapshot.bb_width, 0.0)
        self.assertGreaterEqual(snapshot.volatility, 0.0)


if __name__ == "__main__":
    unittest.main()
