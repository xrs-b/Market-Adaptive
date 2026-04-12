from __future__ import annotations

import unittest

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.cta_robot import CTARobot, TrendSignal


class DummyClient:
    pass


class DummyDatabase:
    pass


class CTAHeartbeatTests(unittest.TestCase):
    def test_build_signal_heartbeat_payload_contains_zscore_gaps(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=1.5, heartbeat_interval_seconds=300.0),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=0,
            raw_direction=1,
            major_direction=1,
            swing_rsi=58.0,
            bullish_ready=True,
            execution_golden_cross=False,
            execution_breakout=True,
            execution_memory_active=True,
            execution_memory_bars_ago=3,
            execution_trigger_reason="Triggered via Memory Window: KDJ crossed 3 bars ago + Price Breakout NOW",
            mtf_aligned=True,
            obv_bias=1,
            obv_confirmation=OBVConfirmationSnapshot(
                current_obv=2310.0,
                sma_value=261.1,
                increment_value=277.0,
                increment_mean=38.5,
                increment_std=191.26,
                zscore=1.2,
            ),
            obv_confirmation_passed=False,
            volume_filter_passed=False,
            volume_profile=None,
            long_setup_blocked=True,
            long_setup_reason="obv_strength_not_confirmed",
            price=100.0,
            atr=1.2,
            risk_percent=0.03,
        )

        payload = robot._build_signal_heartbeat_payload(signal)

        self.assertEqual(payload["symbol"], "BTC/USDT")
        self.assertTrue(payload["bullish_ready"])
        self.assertAlmostEqual(payload["obv_zscore_gap"], -0.3)
        self.assertFalse(payload["obv_confirmation_passed"])
        self.assertEqual(payload["long_setup_reason"], "obv_strength_not_confirmed")
        self.assertTrue(payload["obv_above_sma"])


if __name__ == "__main__":
    unittest.main()
