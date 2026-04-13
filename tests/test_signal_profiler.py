from __future__ import annotations

import unittest

from market_adaptive.strategies.signal_profiler import SignalProfiler
from market_adaptive.testsupport import DummyNotifier


class SignalProfilerTests(unittest.TestCase):
    def test_profiler_accumulates_funnel_counters(self) -> None:
        profiler = SignalProfiler(summary_interval=10)

        class DummySignal:
            server_time_iso = "2026-04-13T00:00:00+00:00"
            local_time_iso = "2026-04-13T00:00:01+00:00"
            server_local_skew_ms = 1000
            major_direction = 1
            weak_bull_bias = False
            early_bullish = False
            swing_rsi = 55.0
            execution_obv_zscore = 1.3
            execution_obv_threshold = 1.0
            current_price = 100.0
            execution_atr = 2.0
            atr_price_ratio_pct = 2.0
            major_timestamp_ms = 100
            swing_timestamp_ms = 110
            execution_timestamp_ms = 120
            data_alignment_valid = True
            data_mismatch_ms = 20
            blocker_reason = "PASSED"
            bullish_ready = True
            fully_aligned = True

        profiler.record(DummySignal(), grid_center_price=99.0)

        self.assertEqual(profiler.counters.total_cycles, 1)
        self.assertEqual(profiler.counters.passed_regime, 1)
        self.assertEqual(profiler.counters.passed_swing, 1)
        self.assertEqual(profiler.counters.passed_trigger, 1)

    def test_profiler_notifies_summary_on_interval_boundary(self) -> None:
        notifier = DummyNotifier()
        profiler = SignalProfiler(summary_interval=2, notifier=notifier, symbol="BTC/USDT")

        class PassingSignal:
            server_time_iso = "2026-04-13T00:00:00+00:00"
            local_time_iso = "2026-04-13T00:00:01+00:00"
            server_local_skew_ms = 1000
            major_direction = 1
            weak_bull_bias = False
            early_bullish = False
            swing_rsi = 55.0
            execution_obv_zscore = 1.3
            execution_obv_threshold = 1.0
            current_price = 100.0
            execution_atr = 2.0
            atr_price_ratio_pct = 2.0
            major_timestamp_ms = 100
            swing_timestamp_ms = 110
            execution_timestamp_ms = 120
            data_alignment_valid = True
            data_mismatch_ms = 20
            blocker_reason = "PASSED"
            bullish_ready = True
            fully_aligned = True

        class BlockedSignal:
            server_time_iso = "2026-04-13T00:05:00+00:00"
            local_time_iso = "2026-04-13T00:05:01+00:00"
            server_local_skew_ms = 1000
            major_direction = 1
            weak_bull_bias = False
            early_bullish = False
            swing_rsi = 48.0
            execution_obv_zscore = 0.7
            execution_obv_threshold = 1.0
            current_price = 98.0
            execution_atr = 2.1
            atr_price_ratio_pct = 2.1
            major_timestamp_ms = 200
            swing_timestamp_ms = 210
            execution_timestamp_ms = 220
            data_alignment_valid = True
            data_mismatch_ms = 30
            blocker_reason = "Blocked_By_OBV_STRENGTH_NOT_CONFIRMED"
            bullish_ready = False
            fully_aligned = False

        profiler.record(PassingSignal(), grid_center_price=99.0)
        self.assertEqual(len(notifier.signal_profiler_summary_calls), 0)

        profiler.record(BlockedSignal(), grid_center_price=101.0)

        self.assertEqual(len(notifier.signal_profiler_summary_calls), 1)
        call = notifier.signal_profiler_summary_calls[0]
        self.assertEqual(call["symbol"], "BTC/USDT")
        self.assertEqual(call["summary_interval"], 2)
        self.assertEqual(call["summary"]["window_cycles"], 2)
        self.assertEqual(call["summary"]["passed_regime"], 2)
        self.assertEqual(call["summary"]["passed_swing"], 1)
        self.assertEqual(call["summary"]["passed_trigger"], 1)
        self.assertEqual(call["summary"]["top_blockers"][0], ("Blocked_By_OBV_STRENGTH_NOT_CONFIRMED", 1))
        self.assertEqual(call["summary"]["latest_blocker_reason"], "Blocked_By_OBV_STRENGTH_NOT_CONFIRMED")
        self.assertAlmostEqual(call["summary"]["latest_execution_price"], 98.0)
        self.assertAlmostEqual(call["summary"]["latest_grid_center_gap"], -3.0)

    def test_profiler_summary_keeps_latest_real_samples_when_current_snapshot_is_unavailable(self) -> None:
        notifier = DummyNotifier()
        profiler = SignalProfiler(summary_interval=2, notifier=notifier, symbol="BTC/USDT")

        class ValidSignal:
            server_time_iso = "2026-04-13T00:00:00+00:00"
            local_time_iso = "2026-04-13T00:00:01+00:00"
            server_local_skew_ms = 1000
            major_direction = 1
            weak_bull_bias = False
            early_bullish = False
            swing_rsi = 55.0
            execution_obv_zscore = 1.3
            execution_obv_threshold = 1.0
            current_price = 100.0
            execution_atr = 2.0
            atr_price_ratio_pct = 2.0
            major_timestamp_ms = 100
            swing_timestamp_ms = 110
            execution_timestamp_ms = 120
            data_alignment_valid = True
            data_mismatch_ms = 20
            blocker_reason = "PASSED"
            bullish_ready = True
            fully_aligned = True

        class MissingCurrentSnapshotSignal:
            server_time_iso = "2026-04-13T00:05:00+00:00"
            local_time_iso = "2026-04-13T00:05:01+00:00"
            server_local_skew_ms = 1000
            major_direction = 1
            weak_bull_bias = False
            early_bullish = False
            swing_rsi = 48.0
            execution_obv_zscore = 0.7
            execution_obv_threshold = 1.0
            current_price = 0.0
            execution_atr = 2.1
            atr_price_ratio_pct = 2.1
            major_timestamp_ms = 200
            swing_timestamp_ms = 210
            execution_timestamp_ms = 220
            data_alignment_valid = True
            data_mismatch_ms = 30
            blocker_reason = "Blocked_By_OBV_STRENGTH_NOT_CONFIRMED"
            bullish_ready = False
            fully_aligned = False

        profiler.record(ValidSignal(), grid_center_price=99.0)
        profiler.record(MissingCurrentSnapshotSignal(), grid_center_price=None)

        self.assertEqual(len(notifier.signal_profiler_summary_calls), 1)
        call = notifier.signal_profiler_summary_calls[0]
        self.assertAlmostEqual(call["summary"]["latest_execution_price"], 100.0)
        self.assertAlmostEqual(call["summary"]["latest_grid_center_gap"], 1.0)


if __name__ == "__main__":
    unittest.main()
