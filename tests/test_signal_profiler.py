from __future__ import annotations

import unittest

from market_adaptive.strategies.signal_profiler import SignalProfiler


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


if __name__ == "__main__":
    unittest.main()
