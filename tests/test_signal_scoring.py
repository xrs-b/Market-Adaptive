from __future__ import annotations

import unittest

from market_adaptive.config import SignalScoringConfig
from market_adaptive.strategies.signal_scoring import build_signal_score


class SignalScoringTests(unittest.TestCase):
    def test_score_below_threshold_is_ignored(self) -> None:
        snapshot = build_signal_score(
            SignalScoringConfig(),
            trend_confirmed=True,
            volume_confirmed=False,
            timeframe_confirmed=False,
            order_flow_confirmed=True,
            obv_slope_confirmed=False,
            execution_trigger_confirmed=False,
        )

        self.assertAlmostEqual(snapshot.total_score, 2.0)
        self.assertEqual(snapshot.tier, "ignore")
        self.assertFalse(snapshot.trade_allowed)

    def test_score_reaches_standard_tier_at_three_points(self) -> None:
        snapshot = build_signal_score(
            SignalScoringConfig(),
            trend_confirmed=True,
            volume_confirmed=True,
            timeframe_confirmed=False,
            order_flow_confirmed=False,
            obv_slope_confirmed=False,
            execution_trigger_confirmed=False,
        )

        self.assertAlmostEqual(snapshot.total_score, 3.0)
        self.assertEqual(snapshot.tier, "standard")
        self.assertTrue(snapshot.trade_allowed)
        self.assertFalse(snapshot.high_quality)

    def test_score_reaches_high_quality_tier_at_five_points(self) -> None:
        snapshot = build_signal_score(
            SignalScoringConfig(),
            trend_confirmed=True,
            volume_confirmed=True,
            timeframe_confirmed=True,
            order_flow_confirmed=False,
            obv_slope_confirmed=True,
            execution_trigger_confirmed=False,
        )

        self.assertAlmostEqual(snapshot.total_score, 6.0)
        self.assertEqual(snapshot.tier, "high_quality")
        self.assertTrue(snapshot.high_quality)
        self.assertAlmostEqual(snapshot.component_score("obv_slope"), 1.0)
        self.assertAlmostEqual(snapshot.component_score("execution_trigger"), 0.0)


if __name__ == "__main__":
    unittest.main()
