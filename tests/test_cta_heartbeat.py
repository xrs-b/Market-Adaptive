from __future__ import annotations

import unittest

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.strategies.cta_robot import CTARobot, TrendSignal
from market_adaptive.strategies.signal_scoring import SignalScoreSnapshot, SignalScoreComponent


class DummyClient:
    pass


class DummyDatabase:
    pass


class CTAHeartbeatTests(unittest.TestCase):
    def test_build_signal_heartbeat_payload_contains_threshold_gaps(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_slope_threshold_degrees=30.0, heartbeat_interval_seconds=300.0),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
            order_flow_monitor=None,
        )
        signal = TrendSignal(
            direction=0,
            raw_direction=1,
            major_direction=1,
            swing_rsi=58.0,
            bullish_ready=True,
            execution_golden_cross=False,
            execution_breakout=False,
            execution_trigger_reason="waiting_execution_trigger",
            mtf_aligned=False,
            ema_fast=101.0,
            ema_slow=99.0,
            ema_bullish=True,
            obv_bias=1,
            obv_slope_angle=24.0,
            obv_slope_passed=False,
            volume_filter_passed=True,
            volume_breakout_passed=False,
            volume_profile=None,
            long_setup_blocked=True,
            long_setup_reason="obv_slope_too_flat",
            price=100.0,
            atr=1.2,
            execution_rsi=55.0,
            execution_adx=23.0,
            obv_signal_value=12345.0,
        )
        snapshot = SignalScoreSnapshot(
            total_score=2.0,
            max_score=8.0,
            min_trade_score=3.0,
            high_quality_score=5.0,
            tier="ignore",
            components=(
                SignalScoreComponent(name="trend", weight=1.0, passed=True, score=1.0, detail="ema_fast_above_slow"),
                SignalScoreComponent(name="volume", weight=2.0, passed=False, score=0.0, detail="price_above_poc_and_value_area"),
                SignalScoreComponent(name="timeframe_resonance", weight=2.0, passed=False, score=0.0, detail="1h_and_15m_confluence"),
                SignalScoreComponent(name="order_flow", weight=1.0, passed=True, score=1.0, detail="buy_side_order_book_dominance"),
                SignalScoreComponent(name="obv_slope", weight=1.0, passed=False, score=0.0, detail="obv_slope_angle_threshold"),
                SignalScoreComponent(name="execution_trigger", weight=1.0, passed=False, score=0.0, detail="15m_kdj_or_breakout_trigger"),
            ),
        )

        payload = robot._build_signal_heartbeat_payload(signal, snapshot)

        self.assertEqual(payload["symbol"], "BTC/USDT")
        self.assertTrue(payload["bullish_ready"])
        self.assertAlmostEqual(payload["obv_slope_gap"], -6.0)
        self.assertAlmostEqual(payload["swing_rsi_gap"], 8.0)
        self.assertAlmostEqual(payload["score_gap"], -1.0)
        self.assertEqual(payload["long_setup_reason"], "obv_slope_too_flat")
        self.assertIn("execution_trigger", payload["score_components"])


if __name__ == "__main__":
    unittest.main()
