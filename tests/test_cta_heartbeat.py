from __future__ import annotations

import unittest

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.coordination import StrategyRuntimeContext
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.cta_robot import CTANearMissSample, CTARobot, TrendSignal


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



    def test_collects_and_flushes_obv_near_miss_report_hourly(self) -> None:
        class CapturingNotifier:
            def __init__(self) -> None:
                self.calls = []

            def notify_cta_near_miss_report(self, *, symbol: str, samples: list[CTANearMissSample], window_seconds: float) -> bool:
                self.calls.append({"symbol": symbol, "samples": samples, "window_seconds": window_seconds})
                return True

        notifier = CapturingNotifier()
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(
                symbol="BTC/USDT",
                obv_zscore_threshold=1.0,
                near_miss_report_interval_seconds=3600.0,
                near_miss_report_max_samples=2,
            ),
            execution_config=ExecutionConfig(),
            notifier=notifier,
            risk_manager=None,
            sentiment_analyst=None,
        )
        now = 10_000.0
        robot._time_provider = lambda: now
        signal = TrendSignal(
            direction=0,
            raw_direction=1,
            major_direction=1,
            swing_rsi=58.0,
            bullish_ready=True,
            execution_golden_cross=True,
            execution_breakout=True,
            execution_memory_active=True,
            execution_memory_bars_ago=1,
            execution_trigger_reason="Triggered via Memory Window",
            mtf_aligned=True,
            obv_bias=1,
            obv_confirmation=OBVConfirmationSnapshot(
                current_obv=2000.0,
                sma_value=1500.0,
                increment_value=120.0,
                increment_mean=60.0,
                increment_std=50.0,
                zscore=0.85,
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
        robot._collect_near_miss_sample(signal)
        self.assertEqual(len(robot._near_miss_samples), 1)

        robot._maybe_flush_near_miss_report()
        self.assertEqual(len(notifier.calls), 0)

        now += 1800.0
        robot._collect_near_miss_sample(signal)
        robot._maybe_flush_near_miss_report()
        self.assertEqual(len(notifier.calls), 0)

        now += 1801.0
        robot._maybe_flush_near_miss_report()
        self.assertEqual(len(notifier.calls), 1)
        report = notifier.calls[0]
        self.assertEqual(report["symbol"], "BTC/USDT")
        self.assertEqual(report["window_seconds"], 3600.0)
        self.assertEqual(len(report["samples"]), 2)
        self.assertAlmostEqual(report["samples"][0].obv_zscore, 0.85)
        self.assertAlmostEqual(report["samples"][0].obv_threshold, 1.0)
        self.assertAlmostEqual(report["samples"][0].obv_gap, 0.15)
        self.assertEqual(robot._near_miss_samples, [])

    def test_requests_urgent_wakeup_on_major_direction_and_bullish_ready_transition(self) -> None:
        runtime_context = StrategyRuntimeContext()
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
            runtime_context=runtime_context,
        )
        baseline = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=-1,
            bullish_ready=False,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 1.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )
        shifted = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=1,
            bullish_ready=True,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 1.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )

        robot._request_urgent_wakeup_on_signal_transition(baseline)
        self.assertFalse(runtime_context.urgent_wakeup.is_set())

        robot._request_urgent_wakeup_on_signal_transition(shifted)
        self.assertTrue(runtime_context.urgent_wakeup.is_set())
        self.assertIn("cta_major_direction:-1->1", runtime_context.urgent_wakeup_reason or "")
        self.assertIn("cta_bullish_ready:False->True", runtime_context.urgent_wakeup_reason or "")

    def test_ignores_non_obv_or_not_ready_near_miss_candidates(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        base_signal = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=1,
            swing_rsi=58.0,
            bullish_ready=True,
            execution_golden_cross=False,
            execution_breakout=False,
            execution_memory_active=False,
            execution_memory_bars_ago=None,
            execution_trigger_reason="waiting_execution_trigger",
            mtf_aligned=False,
            obv_bias=1,
            obv_confirmation=OBVConfirmationSnapshot(
                current_obv=1.0,
                sma_value=1.0,
                increment_value=1.0,
                increment_mean=1.0,
                increment_std=1.0,
                zscore=0.9,
            ),
            obv_confirmation_passed=False,
            volume_filter_passed=False,
            volume_profile=None,
            long_setup_blocked=True,
            long_setup_reason="obv_strength_not_confirmed",
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )

        robot._collect_near_miss_sample(base_signal)
        self.assertEqual(robot._near_miss_samples, [])

        robot._collect_near_miss_sample(TrendSignal(**{**base_signal.__dict__, "raw_direction": 1, "long_setup_reason": "inside_value_area"}))
        self.assertEqual(robot._near_miss_samples, [])

if __name__ == "__main__":
    unittest.main()
