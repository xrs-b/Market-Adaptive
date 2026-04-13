from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import MarketOracleConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.indicators import IndicatorSnapshot
from market_adaptive.oracles.market_oracle import MarketOracle, MultiTimeframeMarketSnapshot


class DummyOKXClient:
    def __init__(self, payloads: dict[str, list[list[float]]]) -> None:
        self.payloads = payloads

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200, since: int | None = None):
        del symbol, since
        return self.payloads[timeframe][-limit:]


class MarketOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MarketOracleConfig()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _snapshot(self, higher: IndicatorSnapshot, lower: IndicatorSnapshot) -> MultiTimeframeMarketSnapshot:
        return MultiTimeframeMarketSnapshot(
            symbol="BTC/USDT",
            higher_timeframe="1h",
            lower_timeframe="15m",
            higher=higher,
            lower=lower,
        )

    def _impulse_payload(self, bullish: bool = True) -> list[list[float]]:
        base = 1_700_000_000_000
        candles = []
        closes = [100.0, 100.1, 100.2, 100.5, 100.9, 101.4]
        volumes = [100, 105, 110, 160, 170, 180] if bullish else [100, 105, 110, 120, 115, 118]
        for idx, close in enumerate(closes):
            open_price = close - 0.2 if bullish or idx < 3 else close + 0.1
            candles.append([base + idx * 60_000, open_price, close + 0.1, open_price - 0.1, close, volumes[idx]])
        return candles

    def test_determine_status_returns_trend_when_adx_is_rising_and_di_gap_is_clear(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(30.0, 28.0, 26.0, 32.0, 18.0, 0.12, 0.09, 0.02),
            lower=IndicatorSnapshot(18.0, 17.0, 16.0, 22.0, 19.0, 0.08, 0.07, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "trend")

    def test_determine_status_returns_sideways_when_both_adx_are_low(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(15.0, 16.0, 17.0, 19.0, 17.0, 0.04, 0.05, 0.01),
            lower=IndicatorSnapshot(19.5, 20.0, 21.0, 18.0, 16.0, 0.03, 0.03, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "sideways")

    def test_determine_status_returns_trend_impulse_when_short_burst_appears(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(True)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(15.0, 16.0, 17.0, 19.0, 17.0, 0.04, 0.05, 0.01),
            lower=IndicatorSnapshot(18.5, 19.0, 19.5, 18.0, 16.0, 0.03, 0.03, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "trend_impulse")

    def test_determine_status_falls_back_to_previous_status_when_signal_is_mixed(self) -> None:
        self.database.insert_market_status(
            MarketStatusRecord(
                timestamp="2026-04-10T03:00:00+00:00",
                symbol="BTC/USDT",
                status="trend",
                adx_value=26.0,
                volatility=0.02,
            )
        )
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(27.0, 26.0, 25.0, 22.0, 10.0, 0.06, 0.06, 0.01),
            lower=IndicatorSnapshot(22.0, 21.0, 20.0, 19.0, 17.0, 0.05, 0.05, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "trend")

    def test_determine_status_downgrades_high_but_falling_adx_to_sideways(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(31.0, 32.0, 33.0, 30.0, 12.0, 0.12, 0.10, 0.02),
            lower=IndicatorSnapshot(26.0, 27.0, 28.0, 25.0, 19.0, 0.08, 0.08, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "sideways")

    def test_determine_status_downgrades_high_adx_with_small_di_gap_to_sideways(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(30.0, 29.0, 28.0, 24.0, 20.5, 0.11, 0.09, 0.02),
            lower=IndicatorSnapshot(24.0, 23.0, 22.0, 21.0, 18.5, 0.07, 0.06, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "sideways")

    def test_snapshot_exposes_positive_bias_value_when_di_favors_bulls(self) -> None:
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(30.0, 28.0, 26.0, 36.0, 18.0, 0.12, 0.09, 0.02),
            lower=IndicatorSnapshot(20.0, 19.0, 18.0, 28.0, 20.0, 0.08, 0.07, 0.01),
        )

        self.assertGreater(snapshot.bias_value, 0.0)

    def test_current_bias_value_uses_latest_snapshot(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({"1m": self._impulse_payload(False)}), database=self.database, config=self.config)
        oracle._last_snapshot = self._snapshot(
            higher=IndicatorSnapshot(30.0, 28.0, 26.0, 34.0, 20.0, 0.12, 0.09, 0.02),
            lower=IndicatorSnapshot(22.0, 21.0, 20.0, 27.0, 21.0, 0.08, 0.07, 0.01),
        )

        self.assertGreater(oracle.current_bias_value(), 0.0)


if __name__ == "__main__":
    unittest.main()
