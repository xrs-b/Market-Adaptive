from __future__ import annotations

import unittest

from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.obv_gate import resolve_dynamic_obv_gate


class OBVGateTests(unittest.TestCase):
    def test_dynamic_gate_tiers_match_intended_policy(self) -> None:
        low = resolve_dynamic_obv_gate(bullish_score=55.0, configured_threshold=1.0)
        mid = resolve_dynamic_obv_gate(bullish_score=65.0, configured_threshold=1.0)
        high = resolve_dynamic_obv_gate(bullish_score=80.0, configured_threshold=1.0)

        self.assertEqual((low.threshold, low.exempt), (0.6, False))
        self.assertEqual((mid.threshold, mid.exempt), (0.0, False))
        self.assertEqual((high.threshold, high.exempt), (-1.0, True))

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
