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

    def test_determine_status_returns_trend_when_adx_and_bandwidth_expand(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(30.0, 0.12, 0.09, 0.02),
            lower=IndicatorSnapshot(18.0, 0.08, 0.07, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "trend")

    def test_determine_status_returns_sideways_when_both_adx_are_low(self) -> None:
        oracle = MarketOracle(client=DummyOKXClient({}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(15.0, 0.04, 0.05, 0.01),
            lower=IndicatorSnapshot(19.5, 0.03, 0.03, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "sideways")

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
        oracle = MarketOracle(client=DummyOKXClient({}), database=self.database, config=self.config)
        snapshot = self._snapshot(
            higher=IndicatorSnapshot(23.0, 0.06, 0.06, 0.01),
            lower=IndicatorSnapshot(22.0, 0.05, 0.05, 0.01),
        )
        self.assertEqual(oracle.determine_status(snapshot), "trend")


if __name__ == "__main__":
    unittest.main()
