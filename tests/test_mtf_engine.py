from __future__ import annotations

import unittest

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

    def _set_ohlcv(self, timeframe: str, closes: list[float], step_ms: int) -> None:
        base = 1_700_000_000_000
        payload = []
        for index, close in enumerate(closes):
            payload.append([base + index * step_ms, close - 0.3, close + 0.4, close - 0.6, close, 100 + index * 5])
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

        signal = self.engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.bullish_ready)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("prior_high_break", signal.execution_trigger.reason)


if __name__ == "__main__":
    unittest.main()
