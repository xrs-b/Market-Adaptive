from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from market_adaptive.config import CTAConfig
from market_adaptive.strategies.intrabar_replay import (
    IntrabarReplayFrames,
    _IntrabarReplayClient,
    build_execution_replay_frame,
    replay_open_position_at_timestamp,
    replay_signal_with_intrabar_scan,
    replay_trend_signal_at_timestamp,
    replay_trend_signal_with_intrabar_scan,
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
        self.assertEqual(partial["timestamp"], pd.Timestamp("2026-04-14T01:30:00Z"))
        self.assertEqual(partial["open"], 102.0)
        self.assertEqual(partial["high"], 103.0)
        self.assertEqual(partial["low"], 101.0)
        self.assertEqual(partial["close"], 102.5)
        self.assertEqual(partial["volume"], 2.0)


class IntrabarReplayClientTests(unittest.TestCase):
    def test_replay_client_fetches_execution_frame_with_partial_current_candle_only(self) -> None:
        cfg = CTAConfig(symbol="BTC/USDT", lookback_limit=5)
        execution = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:00:00Z"), "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 10.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:15:00Z"), "open": 101.0, "high": 103.0, "low": 100.0, "close": 102.0, "volume": 11.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:30:00Z"), "open": 102.0, "high": 106.0, "low": 101.0, "close": 105.0, "volume": 20.0},
            ]
        )
        intrabar = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-04-14T01:30:00Z"), "open": 102.0, "high": 103.0, "low": 101.0, "close": 102.8, "volume": 2.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:31:00Z"), "open": 102.8, "high": 104.0, "low": 102.5, "close": 103.9, "volume": 3.0},
                {"timestamp": pd.Timestamp("2026-04-14T01:32:00Z"), "open": 103.9, "high": 106.0, "low": 103.7, "close": 105.0, "volume": 4.0},
            ]
        )
        frames = IntrabarReplayFrames(major=execution, swing=execution, execution=execution, intrabar=intrabar)
        client = _IntrabarReplayClient(
            symbol="BTC/USDT",
            config=cfg,
            frames=frames,
            evaluation_ts=pd.Timestamp("2026-04-14T01:31:00Z"),
        )

        replay_rows = client.fetch_ohlcv("BTC/USDT", cfg.execution_timeframe, limit=10)

        self.assertEqual(len(replay_rows), 3)
        self.assertEqual(replay_rows[-1][0], pd.Timestamp("2026-04-14T01:30:00Z"))
        self.assertEqual(replay_rows[-1][1], 102.0)
        self.assertEqual(replay_rows[-1][2], 103.0)
        self.assertEqual(replay_rows[-1][3], 101.0)
        self.assertEqual(replay_rows[-1][4], 102.8)
        self.assertEqual(replay_rows[-1][5], 2.0)


class IntrabarReplaySignalOpenPositionTests(unittest.TestCase):
    def _build_live_like_frames(self) -> IntrabarReplayFrames:
        execution_rows: list[dict[str, float | pd.Timestamp]] = []
        lower_last_close = 100.0
        base_price = lower_last_close - 8.0
        pattern = [0.0, 0.4, -0.3, 0.5, -0.2, 0.3, -0.1, 0.2]
        lower_closes: list[float] = []
        for index in range(112):
            lower_closes.append(base_price + pattern[index % len(pattern)])
        lower_closes.extend(
            [
                lower_last_close - 5.6,
                lower_last_close - 4.8,
                lower_last_close - 5.2,
                lower_last_close - 4.6,
                lower_last_close - 7.0,
                lower_last_close - 6.5,
                lower_last_close - 6.0,
                lower_last_close,
            ]
        )
        execution_ts = pd.date_range("2026-04-14T00:00:00Z", periods=len(lower_closes), freq="15min", tz="UTC")
        for index, (timestamp, close) in enumerate(zip(execution_ts, lower_closes)):
            volume = 100.0 + index * 2.0
            if index >= len(lower_closes) - 4:
                volume *= 8.0
            execution_rows.append(
                {
                    "timestamp": timestamp,
                    "open": close - 0.3,
                    "high": close + 0.4,
                    "low": close - 0.5,
                    "close": close,
                    "volume": volume,
                }
            )
        execution = pd.DataFrame(execution_rows)

        swing_ts = pd.date_range("2026-04-11T00:00:00Z", periods=130, freq="1h", tz="UTC")
        swing = pd.DataFrame(
            {
                "timestamp": swing_ts,
                "open": [81.0 + index for index in range(130)],
                "high": [81.7 + index for index in range(130)],
                "low": [80.5 + index for index in range(130)],
                "close": [81.3 + index for index in range(130)],
                "volume": [120.0 + index for index in range(130)],
            }
        )
        major_ts = pd.date_range("2026-03-20T00:00:00Z", periods=130, freq="4h", tz="UTC")
        major = pd.DataFrame(
            {
                "timestamp": major_ts,
                "open": [102.0 + 2.0 * index for index in range(130)],
                "high": [102.8 + 2.0 * index for index in range(130)],
                "low": [101.4 + 2.0 * index for index in range(130)],
                "close": [102.3 + 2.0 * index for index in range(130)],
                "volume": [150.0 + index for index in range(130)],
            }
        )

        bucket_start = pd.Timestamp(execution.iloc[-1]["timestamp"])
        intrabar = pd.DataFrame(
            [
                {
                    "timestamp": bucket_start,
                    "open": 94.0,
                    "high": 94.4,
                    "low": 93.5,
                    "close": 94.0,
                    "volume": 500.0,
                },
                {
                    "timestamp": bucket_start + pd.Timedelta(minutes=1),
                    "open": 94.0,
                    "high": 96.5,
                    "low": 93.9,
                    "close": 96.0,
                    "volume": 700.0,
                },
                {
                    "timestamp": bucket_start + pd.Timedelta(minutes=2),
                    "open": 96.0,
                    "high": 100.8,
                    "low": 95.8,
                    "close": 100.6,
                    "volume": 1200.0,
                },
            ]
        )
        return IntrabarReplayFrames(major=major, swing=swing, execution=execution, intrabar=intrabar)

    def test_replay_trend_signal_can_flow_into_open_position(self) -> None:
        cfg = CTAConfig(
            symbol="BTC/USDT",
            lookback_limit=120,
            order_flow_enabled=False,
            disable_near_breakout_release_long=False,
            disable_price_led_override_long=False,
        )
        frames = self._build_live_like_frames()
        evaluation_ts = pd.Timestamp(frames.intrabar.iloc[-1]["timestamp"]) + pd.Timedelta(minutes=1)

        signal = replay_trend_signal_at_timestamp(
            config=cfg,
            frames=frames,
            evaluation_ts=evaluation_ts,
        )
        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)

        replay_signal, action, robot = replay_open_position_at_timestamp(
            config=cfg,
            frames=frames,
            evaluation_ts=evaluation_ts,
        )

        assert replay_signal is not None
        self.assertEqual(replay_signal.direction, 1)
        self.assertNotEqual(action, "cta:entry_decider_block")


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

        expected = [ts + pd.Timedelta(minutes=1) for ts in intrabar["timestamp"].tolist()]
        self.assertEqual(captured, expected)
        self.assertEqual(len(signals), 3)

    def test_replay_trend_signal_with_intrabar_scan_evaluates_robot_path_minute_by_minute(self) -> None:
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
            ]
        )
        frames = IntrabarReplayFrames(major=base, swing=base, execution=base, intrabar=intrabar)

        captured = []

        def fake_build_trend_signal(self):
            captured.append(self.client.evaluation_ts)
            return object()

        with patch("market_adaptive.strategies.cta_robot.CTARobot._build_trend_signal", new=fake_build_trend_signal):
            signals = replay_trend_signal_with_intrabar_scan(
                config=cfg,
                frames=frames,
                target_bar_ts=pd.Timestamp("2026-04-14T01:30:00Z"),
            )

        expected = [ts + pd.Timedelta(minutes=1) for ts in intrabar["timestamp"].tolist()]
        self.assertEqual(captured, expected)
        self.assertEqual(len(signals), 2)


if __name__ == "__main__":
    unittest.main()
