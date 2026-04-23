"""Tests for watch-sample persistence and short-side candidate mirroring."""
from __future__ import annotations

import unittest
from market_adaptive.strategies.cta_robot import CTARobot, TrendSignal
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.config import CTAConfig, ExecutionConfig


class DummyClient:
    pass


class DummyDatabase:
    pass


class ShortCandidateStateTests(unittest.TestCase):
    def test_derive_candidate_state_returns_watch_when_entry_decider_says_watch(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=-1,
            raw_direction=-1,
            major_direction=-1,
            bullish_ready=False,
            bearish_ready=True,
            early_bearish=True,
            execution_entry_mode="early_bearish_starter_limit",
            execution_trigger_reason="early_bearish: distribution rollover",
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="watch",
            entry_decider_reasons=("weak_edge",),
            execution_memory_active=False,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            execution_breakdown=False,
        )
        state, reason = robot._derive_candidate_state(signal)
        self.assertEqual(state, "watch")
        self.assertIn("weak_edge", reason)

    def test_derive_candidate_state_returns_idle_when_no_ready_side(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=0,
            bullish_ready=False,
            bearish_ready=False,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="block",
            entry_decider_reasons=(),
        )
        state, reason = robot._derive_candidate_state(signal)
        self.assertEqual(state, "idle")

    def test_derive_candidate_state_short_trigger_ready(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=-1,
            raw_direction=-1,
            major_direction=-1,
            bullish_ready=False,
            bearish_ready=True,
            early_bearish=True,
            execution_entry_mode="early_bearish_starter_limit",
            execution_trigger_reason="early_bearish",
            execution_breakdown=True,
            execution_memory_active=False,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="allow",
            entry_decider_reasons=(),
        )
        state, reason = robot._derive_candidate_state(signal)
        self.assertEqual(state, "trigger_ready")

    def test_derive_candidate_state_short_armed(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        # raw_direction=0 but memory active → armed
        signal = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=-1,
            bullish_ready=False,
            bearish_ready=True,
            early_bearish=True,
            execution_entry_mode="early_bearish_starter_limit",
            execution_trigger_reason="early_bearish",
            execution_memory_active=True,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            execution_breakdown=False,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="block",
            entry_decider_reasons=("entry_quality_not_clean",),
        )
        state, reason = robot._derive_candidate_state(signal)
        self.assertEqual(state, "armed")
        self.assertIn("early_bearish", reason)

    def test_is_execution_near_ready_short_uses_execution_breakdown(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=-1,
            raw_direction=-1,
            major_direction=-1,
            bullish_ready=False,
            bearish_ready=True,
            execution_breakdown=True,
            execution_memory_active=False,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )
        self.assertTrue(robot._is_execution_near_ready(signal))

    def test_candidate_ready_side_prefers_bearish_when_early_bearish(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=0,
            raw_direction=0,
            major_direction=1,
            bullish_ready=True,
            bearish_ready=True,
            early_bearish=True,
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )
        side = robot._candidate_ready_side(signal)
        self.assertEqual(side, -1)


class WatchPersistenceViaHeartbeatPayloadTests(unittest.TestCase):
    def test_watch_sample_persistence_promotes_prior_watch_into_trigger_ready_payload(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        now = 1_000.0
        robot._time_provider = lambda: now

        watch_signal = TrendSignal(
            direction=1,
            raw_direction=1,
            major_direction=1,
            bullish_ready=True,
            execution_trigger_reason="watching_breakout",
            obv_confirmation=OBVConfirmationSnapshot(1.0, 0.5, 0.5, 0.0, 1.0, 1.5),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="watch",
            entry_decider_score=62.0,
            entry_decider_reasons=("weak_edge",),
            candidate_state="watch",
            candidate_reason="weak_edge",
        )
        robot._annotate_watch_sample_persistence(watch_signal)

        now = 1_030.0
        ready_signal = TrendSignal(
            direction=1,
            raw_direction=1,
            major_direction=1,
            bullish_ready=True,
            execution_trigger_reason="triggered",
            execution_memory_active=True,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            execution_breakout=True,
            obv_confirmation=OBVConfirmationSnapshot(1.0, 0.5, 0.5, 0.0, 1.0, 1.5),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="allow",
            entry_decider_score=80.0,
            entry_decider_reasons=(),
            candidate_state="trigger_ready",
            candidate_reason="triggered",
        )
        annotated = robot._annotate_watch_sample_persistence(ready_signal)
        payload = robot._build_signal_heartbeat_payload(annotated)
        self.assertTrue(payload["watch_sample_promoted"])
        self.assertEqual(payload["watch_sample_origin_reason"], "weak_edge")
        self.assertEqual(payload["watch_sample_age_seconds"], 30.0)
        self.assertEqual(payload["candidate_state"], "trigger_ready")

    def test_heartbeat_payload_contains_entry_decider_fields(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT"),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=1,
            raw_direction=1,
            major_direction=1,
            bullish_ready=True,
            execution_trigger_reason="triggered",
            execution_memory_active=True,
            execution_latch_active=False,
            execution_frontrun_near_breakout=False,
            execution_breakout=True,
            obv_confirmation=OBVConfirmationSnapshot(1.0, 0.5, 0.5, 0.0, 1.0, 1.5),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
            entry_decider_decision="allow",
            entry_decider_score=80.0,
            entry_decider_reasons=(),
        )
        payload = robot._build_signal_heartbeat_payload(signal)
        self.assertEqual(payload["entry_decider_decision"], "allow")
        self.assertEqual(payload["entry_decider_score"], 80.0)
        self.assertEqual(payload["candidate_state"], "trigger_ready")


if __name__ == "__main__":
    unittest.main()
