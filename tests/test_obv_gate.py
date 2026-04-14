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

        self.assertEqual((low.threshold, low.exempt, low.side), (0.6, False, "long"))
        self.assertEqual((mid.threshold, mid.exempt, mid.side), (0.0, False, "long"))
        self.assertEqual((high.threshold, high.exempt, high.side), (-1.0, True, "long"))

    def test_long_recovery_context_relaxes_low_score_gate_to_non_negative_obv(self) -> None:
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

        self.assertEqual((early.threshold, early.exempt, early.side), (0.0, False, "long"))
        self.assertEqual((weak.threshold, weak.exempt, weak.side), (0.0, False, "long"))

    def test_short_recovery_context_relaxes_low_score_gate_to_neutral_obv_ceiling(self) -> None:
        early = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            side="short",
            early_bearish=True,
        )
        weak = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            side="short",
            weak_bear_bias=True,
        )

        self.assertEqual((early.threshold, early.exempt, early.side), (0.0, False, "short"))
        self.assertEqual((weak.threshold, weak.exempt, weak.side), (0.0, False, "short"))

    def test_trigger_reason_and_entry_mode_can_mark_recovery_context_for_both_sides(self) -> None:
        long_by_reason = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            trigger_reason="early_bullish: lower band flattening recovery",
        )
        long_by_mode = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            execution_entry_mode="weak_bull_scale_in_limit",
        )
        short_by_reason = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            side="short",
            trigger_reason="early_bearish: topping distribution rolling over",
        )
        short_by_mode = resolve_dynamic_obv_gate(
            bullish_score=40.0,
            configured_threshold=1.0,
            side="short",
            execution_entry_mode="weak_bear_scale_in_limit",
        )

        self.assertEqual((long_by_reason.threshold, long_by_reason.exempt, long_by_reason.side), (0.0, False, "long"))
        self.assertEqual((long_by_mode.threshold, long_by_mode.exempt, long_by_mode.side), (0.0, False, "long"))
        self.assertEqual((short_by_reason.threshold, short_by_reason.exempt, short_by_reason.side), (0.0, False, "short"))
        self.assertEqual((short_by_mode.threshold, short_by_mode.exempt, short_by_mode.side), (0.0, False, "short"))

    def test_signal_wrapper_preserves_long_recovery_context_even_when_major_regime_is_still_bearish(self) -> None:
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

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.0, False, "long"))

    def test_signal_wrapper_infers_short_side_from_bearish_context_markers(self) -> None:
        signal = self._build_signal(
            major_direction=-1,
            execution_entry_mode="weak_bear_scale_in_limit",
            execution_trigger=ExecutionTriggerSnapshot(
                kdj_golden_cross=False,
                kdj_dead_cross=False,
                bullish_memory_active=False,
                bearish_memory_active=True,
                bullish_cross_bars_ago=None,
                bearish_cross_bars_ago=2,
                prior_high_break=False,
                prior_low_break=True,
                prior_high=None,
                prior_low=None,
                reason="early_bearish: distribution rollover after failed push",
            ),
        )

        gate = resolve_dynamic_obv_gate_for_signal(signal, configured_threshold=1.0)

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.0, False, "short"))

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

    def test_mid_tier_long_gate_requires_non_negative_zscore_confirmation(self) -> None:
        gate = resolve_dynamic_obv_gate(bullish_score=70.0, configured_threshold=0.6)
        passing = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, 0.1)
        failing = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, -0.1)

        self.assertFalse(gate.exempt)
        self.assertTrue(gate.passed(passing))
        self.assertFalse(gate.passed(failing))
        self.assertEqual(gate.check_summary(passing), "[Long] OBV (0.10) >= Dynamic Threshold (0.0) -> Passed")

    def test_mid_tier_short_gate_requires_non_positive_zscore_confirmation(self) -> None:
        gate = resolve_dynamic_obv_gate(bullish_score=70.0, configured_threshold=0.6, side="short")
        passing = OBVConfirmationSnapshot(900.0, 1000.0, -5.0, -1.0, 2.0, -0.02)
        failing = OBVConfirmationSnapshot(900.0, 1000.0, -5.0, -1.0, 2.0, 0.02)

        self.assertFalse(gate.exempt)
        self.assertTrue(gate.passed(passing))
        self.assertFalse(gate.passed(failing))
        self.assertEqual(gate.check_summary(passing), "[Short] OBV (-0.02) <= Dynamic Threshold (0.0) -> Passed")

    def test_high_quality_post_trigger_long_softens_threshold_without_below_sma_exception(self) -> None:
        gate = resolve_dynamic_obv_gate(
            bullish_score=60.0,
            configured_threshold=0.6,
            trigger_reason="major_bull_retest_ready: gap=0.120% + KDJ memory 2 bars ago",
        )
        softened_pass = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, 0.55)
        strict_fail = OBVConfirmationSnapshot(1100.0, 1000.0, 5.0, 1.0, 2.0, 0.49)
        below_sma_fail = OBVConfirmationSnapshot(990.0, 1000.0, 5.0, 1.0, 2.0, 0.55)

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.5, False, "long"))
        self.assertTrue(gate.passed(softened_pass))
        self.assertFalse(gate.passed(strict_fail))
        self.assertFalse(gate.passed(below_sma_fail))

    def test_memory_breakout_softening_does_not_unblock_negative_recovery_style_case(self) -> None:
        gate = resolve_dynamic_obv_gate(
            bullish_score=60.0,
            configured_threshold=0.6,
            trigger_reason="Triggered via Memory Window: KDJ crossed 2 bars ago + Price Breakout NOW",
        )
        clearly_negative = OBVConfirmationSnapshot(1100.0, 1000.0, -5.0, -1.0, 2.0, -0.20)

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.5, False, "long"))
        self.assertFalse(gate.passed(clearly_negative))

    def test_frontrun_near_breakout_relaxes_gate_to_non_negative_obv_even_below_mid_tier(self) -> None:
        gate = resolve_dynamic_obv_gate(
            bullish_score=55.0,
            configured_threshold=1.0,
            execution_frontrun_near_breakout=True,
        )

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.0, False, "long"))

    def test_recovery_context_does_not_receive_post_trigger_softening(self) -> None:
        gate = resolve_dynamic_obv_gate(
            bullish_score=60.0,
            configured_threshold=0.6,
            early_bullish=True,
            trigger_reason="major_bull_retest_ready: gap=0.120% + KDJ memory 2 bars ago",
        )

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.0, False, "long"))

    def test_strict_short_gate_preserves_downside_confirmation_requirement(self) -> None:
        gate = resolve_dynamic_obv_gate(bullish_score=55.0, configured_threshold=1.0, side="short")
        passing = OBVConfirmationSnapshot(900.0, 1000.0, -5.0, -1.0, 2.0, -0.7)
        failing = OBVConfirmationSnapshot(900.0, 1000.0, -5.0, -1.0, 2.0, -0.4)

        self.assertEqual((gate.threshold, gate.exempt, gate.side), (0.6, False, "short"))
        self.assertTrue(gate.passed(passing))
        self.assertFalse(gate.passed(failing))
        self.assertEqual(gate.check_summary(passing), "[Short] OBV (-0.70) <= Dynamic Threshold (-0.6) -> Passed")


if __name__ == "__main__":
    unittest.main()
