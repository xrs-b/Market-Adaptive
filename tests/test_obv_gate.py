from __future__ import annotations

import unittest

import pandas as pd

from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.mtf_engine import ExecutionTriggerSnapshot, MTFSignal
from market_adaptive.strategies.obv_gate import resolve_dynamic_obv_gate, resolve_dynamic_obv_gate_for_signal


class OBVGateTests(unittest.TestCase):
    def _build_signal(self, **overrides) -> MTFSignal:
        base = dict(
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            major_direction=1,
            major_bias_score=0.0,
            weak_bull_bias=False,
            early_bullish=False,
            entry_size_multiplier=1.0,
            swing_rsi=55.0,
            swing_rsi_slope=0.0,
            bullish_score=55.0,
            bullish_threshold=60.0,
            bullish_ready=False,
            execution_entry_mode="breakout_confirmed",
            execution_trigger=ExecutionTriggerSnapshot(
                kdj_golden_cross=False,
                kdj_dead_cross=False,
                bullish_memory_active=False,
                bearish_memory_active=False,
                bullish_cross_bars_ago=None,
                bearish_cross_bars_ago=None,
                prior_high_break=False,
                prior_low_break=False,
                prior_high=None,
                prior_low=None,
                reason="waiting_execution_trigger",
            ),
            fully_aligned=False,
            current_price=100.0,
            execution_obv_zscore=0.0,
            execution_obv_threshold=0.6,
            execution_atr=1.0,
            atr_price_ratio_pct=1.0,
            server_time_iso="",
            local_time_iso="",
            server_local_skew_ms=None,
            major_timestamp_ms=0,
            swing_timestamp_ms=0,
            execution_timestamp_ms=0,
            data_alignment_valid=True,
            data_mismatch_ms=0,
            blocker_reason="",
            major_frame=pd.DataFrame(),
            swing_frame=pd.DataFrame(),
            execution_frame=pd.DataFrame(),
        )
        base.update(overrides)
        return MTFSignal(**base)

    def test_dynamic_gate_tiers_match_intended_policy(self) -> None:
        low = resolve_dynamic_obv_gate(bullish_score=55.0, configured_threshold=1.0)
        mid = resolve_dynamic_obv_gate(bullish_score=65.0, configured_threshold=1.0)
        high = resolve_dynamic_obv_gate(bullish_score=80.0, configured_threshold=1.0)

        self.assertEqual((low.threshold, low.exempt), (0.6, False))
        self.assertEqual((mid.threshold, mid.exempt), (0.0, False))
        self.assertEqual((high.threshold, high.exempt), (-1.0, True))

    def test_recovery_context_relaxes_low_score_gate_to_non_negative_obv(self) -> None:
        early = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            early_bullish=True,
        )
        weak = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            weak_bull_bias=True,
        )

        self.assertEqual((early.threshold, early.exempt), (0.0, False))
        self.assertEqual((weak.threshold, weak.exempt), (0.0, False))

    def test_trigger_reason_and_entry_mode_can_mark_recovery_context(self) -> None:
        by_reason = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            trigger_reason="early_bullish: lower band flattening recovery",
        )
        by_mode = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            execution_entry_mode="weak_bull_scale_in_limit",
        )

        self.assertEqual((by_reason.threshold, by_reason.exempt), (0.0, False))
        self.assertEqual((by_mode.threshold, by_mode.exempt), (0.0, False))

    def test_signal_wrapper_uses_context_aware_recovery_gate(self) -> None:
        signal = self._build_signal(
            major_direction=-1,
            early_bullish=True,
            execution_entry_mode="early_bullish_starter_limit",
            execution_trigger=ExecutionTriggerSnapshot(
                kdj_golden_cross=False,
                kdj_dead_cross=False,
                bullish_memory_active=False,
                bearish_memory_active=False,
                bullish_cross_bars_ago=None,
                bearish_cross_bars_ago=None,
                prior_high_break=False,
                prior_low_break=False,
                prior_high=None,
                prior_low=None,
                reason="early_bullish: 1h supertrend bullish + recovery",
            ),
        )

        gate = resolve_dynamic_obv_gate_for_signal(signal, configured_threshold=1.0)

        self.assertEqual((gate.threshold, gate.exempt), (0.0, False))

    def test_exempt_gate_bypasses_below_sma_and_negative_zscore(self) -> None:
        gate = resolve_dynamic_obv_gate(bullish_score=85.0, configured_threshold=0.6)
        snapshot = OBVConfirmationSnapshot(
            current_obv=900.0,
            sma_value=1000.0,
            increment_value=-20.0,
            increment_mean=5.0,
            increment_std=10.0,
            zscore=-0.4,
        )

        self.assertTrue(gate.exempt)
        self.assertTrue(gate.passed(snapshot))

    def test_mid_tier_gate_requires_non_negative_zscore_confirmation(self) -> None:
        gate = resolve_dynamic_obv_gate(bullish_score=70.0, configured_threshold=0.6)
        passing = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, 0.1)
        failing = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, -0.1)

        self.assertFalse(gate.exempt)
        self.assertTrue(gate.passed(passing))
        self.assertFalse(gate.passed(failing))


if __name__ == "__main__":
    unittest.main()
