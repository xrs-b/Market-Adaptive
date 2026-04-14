from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.strategies.intrabar_replay import (
    IntrabarReplayFrames,
    build_execution_replay_frame,
    replay_signal_with_intrabar_scan,
)


class IntrabarReplayFrameTests(unittest.TestCase):
    def test_build_execution_replay_frame_appends_partial_current_candle(self) -> None:
        execution = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:00:00Z"), "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 10.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:15:00Z"), "open": 101.0, "high": 103.0, "low": 100.0, "close": 102.0, "volume": 11.0},
            ]
        )
        intrabar = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:30:00Z"), "open": 102.0, "high": 103.0, "low": 101.0, "close": 102.5, "volume": 2.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:31:00Z"), "open": 102.5, "high": 104.0, "low": 102.0, "close": 103.8, "volume": 3.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:32:00Z"), "open": 103.8, "high": 105.0, "low": 103.5, "close": 104.5, "volume": 4.0},
            ]
        )

        replay = build_execution_replay_frame(
            execution_frame=execution,
            intrabar_frame=intrabar,
            evaluation_ts=pd.Timestamp("2026-04-14T01:31:00Z"),
            execution_timeframe="15m",
        )

        self.assertEqual(len(replay), 3)
        partial = replay.iloc[-1]
        self.assertEqual(partial["timestamp"], pd.Timestamp("2026-04-14T01:31:00Z"))
        self.assertEqual(partial["open"], 102.0)
        self.assertEqual(partial["high"], 104.0)
        self.assertEqual(partial["low"], 101.0)
        self.assertEqual(partial["close"], 103.8)
        self.assertEqual(partial["volume"], 5.0)


class IntrabarReplaySignalScanTests(unittest.TestCase):
    def test_replay_signal_with_intrabar_scan_evaluates_each_minute_in_target_bar(self) -> None:
        cfg = CTAConfig(symbol="BTC/USDT", lookback_limit=5)
        base = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:00:00Z"), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:15:00Z"), "open": 100.5, "high": 101.5, "low": 100.0, "close": 101.0, "volume": 10.0},
            ]
        )
        intrabar = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:30:00Z"), "open": 101.0, "high": 101.2, "low": 100.8, "close": 101.1, "volume": 1.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:31:00Z"), "open": 101.1, "high": 101.4, "low": 101.0, "close": 101.3, "volume": 1.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:32:00Z"), "open": 101.3, "high": 101.6, "low": 101.2, "close": 101.5, "volume": 1.0},
            ]
        )
        frames = IntrabarReplayFrames(major=base, swing=base, execution=base, intrabar=intrabar)

        captured = []

        def fake_build_signal(self):
            captured.append(self.client.evaluation_ts)
            return object()

        with patch("market_adaptive.strategies.mtf_engine.MultiTimeframeSignalEngine.build_signal", new=fake_build_signal):
            signals = replay_signal_with_intrabar_scan(
                config=cfg,
                frames=frames,
                target_bar_ts=pd.Timestamp("2026-04-14T01:30:00Z"),
            )

        self.assertEqual(captured, intrabar["timestamp"].tolist())
        self.assertEqual(len(signals), 3)


if __name__ == "__main__":
    unittest.main()
