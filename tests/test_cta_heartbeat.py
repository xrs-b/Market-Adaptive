from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.coordination import StrategyRuntimeContext
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.cta_robot import CTANearMissSample, CTARobot, TrendSignal
from market_adaptive.strategies.mtf_engine import ExecutionTriggerSnapshot, MTFSignal, SignalQualityTier


class DummyClient:
    pass


class DummyDatabase:
    pass


class CTAHeartbeatTests(unittest.TestCase):
    def _build_mtf_signal(self, *, bullish_score: float, fully_aligned: bool = True, weak_bull_bias: bool = False, early_bullish: bool = False, execution_entry_mode: str = "breakout_confirmed", trigger_reason: str = "Triggered via Memory Window", frontrun_near_breakout: bool = False, major_direction: int = 1, rsi_blocking_overridden: bool = False, signal_quality_tier: SignalQualityTier = SignalQualityTier.TIER_LOW, signal_confidence: float = 0.0, signal_strength_bonus: float = 0.0) -> MTFSignal:
        execution_frame = pd.DataFrame({
            "timestamp": pd.to_datetime([1_700_000_000_000], unit="ms", utc=True),
            "open": [99.0],
            "high": [101.0],
            "low": [98.0],
            "close": [100.0],
            "volume": [1000.0],
        })
        trigger = ExecutionTriggerSnapshot(
            kdj_golden_cross=True,
            kdj_dead_cross=False,
            bullish_memory_active=True,
            bearish_memory_active=False,
            bullish_cross_bars_ago=1,
            bearish_cross_bars_ago=None,
            prior_high_break=True,
            prior_low_break=False,
            prior_high=99.5,
            prior_low=98.0,
            reason=trigger_reason,
            frontrun_near_breakout=frontrun_near_breakout,
        )
        return MTFSignal(
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            major_direction=major_direction,
            major_bias_score=60.0,
            weak_bull_bias=weak_bull_bias,
            early_bullish=early_bullish,
            entry_size_multiplier=1.0,
            swing_rsi=58.0,
            swing_rsi_slope=1.2,
            bullish_score=bullish_score,
            bullish_threshold=55.0,
            bullish_ready=True,
            execution_entry_mode=execution_entry_mode,
            execution_trigger=trigger,
            fully_aligned=fully_aligned,
            current_price=100.0,
            execution_obv_zscore=0.0,
            execution_obv_threshold=0.6,
            execution_atr=1.2,
            atr_price_ratio_pct=1.2,
            server_time_iso="",
            local_time_iso="",
            server_local_skew_ms=0,
            major_timestamp_ms=1,
            swing_timestamp_ms=1,
            execution_timestamp_ms=1,
            data_alignment_valid=True,
            data_mismatch_ms=0,
            blocker_reason="",
            major_frame=execution_frame.copy(),
            swing_frame=execution_frame.copy(),
            execution_frame=execution_frame,
            rsi_blocking_overridden=rsi_blocking_overridden,
            signal_quality_tier=signal_quality_tier,
            signal_confidence=signal_confidence,
            signal_strength_bonus=signal_strength_bonus,
        )


    def test_high_momentum_near_breakout_can_override_inside_value_area_block(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=75.0, frontrun_near_breakout=True)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=5.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=1.0,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 99.0,
            "value_area_low": 98.5,
            "value_area_high": 100.5,
            "above_poc": lambda self, price: True,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
            patch("market_adaptive.strategies.cta_robot.logger") as mock_logger,
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertEqual(signal.long_setup_reason, "")
        self.assertEqual(signal.blocker_reason, "")
        mock_logger.info.assert_any_call(
            "Passed: VA Override [Reason: %s] [%s]",
            "High Momentum",
            "POC: 99.0000, VAH: 100.5000, VAL: 98.5000, Price: 100.0000",
        )

    def test_edge_proximity_can_treat_price_as_effectively_exiting_value_area(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=60.0)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=5.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=1.0,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 99.0,
            "value_area_low": 98.0,
            "value_area_high": 100.9,
            "above_poc": lambda self, price: True,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
            patch("market_adaptive.strategies.cta_robot.logger") as mock_logger,
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        mock_logger.info.assert_any_call(
            "Passed: VA Override [Reason: %s] [%s]",
            "Edge Proximity",
            "POC: 99.0000, VAH: 100.9000, VAL: 98.0000, Price: 100.0000",
        )

    def test_inside_value_area_block_logs_boundaries_when_no_override_applies(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=60.0)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=5.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=1.0,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 99.0,
            "value_area_low": 98.0,
            "value_area_high": 101.5,
            "above_poc": lambda self, price: True,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
            patch("market_adaptive.strategies.cta_robot.logger") as mock_logger,
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.direction, 0)
        self.assertTrue(signal.long_setup_blocked)
        self.assertEqual(signal.long_setup_reason, "inside_value_area")
        self.assertEqual(signal.blocker_reason, "Blocked_By_INSIDE_VALUE_AREA")
        mock_logger.info.assert_any_call(
            "Blocked: Inside VA [%s]",
            "POC: 99.0000, VAH: 101.5000, VAL: 98.0000, Price: 100.0000",
        )

    def test_standard_path_can_bypass_value_area_and_below_vah_blocks(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(
            bullish_score=72.0,
            signal_quality_tier=SignalQualityTier.TIER_MEDIUM,
            signal_confidence=0.61,
        )
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=0.5,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=0.2,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 99.0,
            "value_area_low": 98.0,
            "value_area_high": 101.5,
            "above_poc": lambda self, price: True,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.entry_pathway.name, "STANDARD")
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertIn("STANDARD_VA_BYPASS", signal.relaxed_reasons)

    def test_standard_path_can_tolerate_small_below_poc_gap_near_breakout(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(
            bullish_score=72.0,
            frontrun_near_breakout=True,
            signal_quality_tier=SignalQualityTier.TIER_MEDIUM,
            signal_confidence=0.64,
        )
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=0.5,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=0.2,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 100.6,
            "value_area_low": 98.0,
            "value_area_high": 101.5,
            "above_poc": lambda self, price: False,
            "contains_price": lambda self, price: False,
            "above_value_area": lambda self, price: True,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.entry_pathway.name, "STANDARD")
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertTrue(any(reason.startswith("STANDARD_POC_RECLAIM_OK") for reason in signal.relaxed_reasons))

    def test_resolve_obv_gate_uses_bullish_score_tiers(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )

        low = robot._resolve_obv_gate(self._build_mtf_signal(bullish_score=55.0, trigger_reason="waiting_execution_trigger"))
        mid = robot._resolve_obv_gate(self._build_mtf_signal(bullish_score=65.0))
        high = robot._resolve_obv_gate(self._build_mtf_signal(bullish_score=80.0))

        self.assertEqual((low.threshold, low.exempt), (0.6, False))
        self.assertEqual((mid.threshold, mid.exempt), (-0.1, False))
        self.assertEqual((high.threshold, high.exempt), (-1.0, True))

    def test_high_quality_post_trigger_signal_gets_narrow_obv_softening(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )

        gate = robot._resolve_obv_gate(
            self._build_mtf_signal(
                bullish_score=60.0,
                trigger_reason="major_bull_retest_ready: gap=0.120% + KDJ memory 2 bars ago",
            )
        )

        self.assertEqual((gate.threshold, gate.exempt), (-0.1, False))

    def test_recovery_context_relaxes_obv_gate_even_when_score_is_below_mid_tier(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )

        weak_gate = robot._resolve_obv_gate(
            self._build_mtf_signal(
                bullish_score=40.0,
                weak_bull_bias=True,
                execution_entry_mode="weak_bull_scale_in_limit",
                trigger_reason="Weak bull bias active: scale-in allowed before breakout",
            )
        )
        self.assertEqual((weak_gate.threshold, weak_gate.exempt), (0.0, False))
        early_gate = robot._resolve_obv_gate(
            self._build_mtf_signal(
                bullish_score=40.0,
                early_bullish=True,
                execution_entry_mode="early_bullish_starter_limit",
                trigger_reason="early_bullish: 1h supertrend bullish + recovery",
            )
        )
        self.assertEqual((early_gate.threshold, early_gate.exempt), (0.0, False))

    def test_logs_final_high_momentum_clearance_only_when_rsi_and_value_area_overrides_both_apply(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(
            bullish_score=75.0,
            frontrun_near_breakout=True,
            rsi_blocking_overridden=True,
        )
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=5.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=0.1,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 99.0,
            "value_area_low": 98.5,
            "value_area_high": 100.5,
            "above_poc": lambda self, price: True,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
            patch("market_adaptive.strategies.cta_robot.logger") as mock_logger,
        ):
            signal = robot._build_trend_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        mock_logger.info.assert_any_call("[FINAL_TRIGGER_OVERRIDE] Full Clearance - All Guards Relaxed for High Momentum Breakout")

    def test_high_score_signal_is_not_blocked_by_negative_obv_when_exempt(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=85.0)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=900.0,
            sma_value=1000.0,
            increment_value=-20.0,
            increment_mean=5.0,
            increment_std=10.0,
            zscore=-0.4,
        )

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch(
                "market_adaptive.strategies.cta_robot.compute_volume_profile",
                return_value=type("VolumeProfile", (), {
                    "above_poc": lambda self, price: True,
                    "contains_price": lambda self, price: False,
                    "above_value_area": lambda self, price: True,
                })(),
            ),
        ):
            signal = robot._build_trend_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertTrue(signal.obv_confirmation_passed)
        self.assertAlmostEqual(signal.obv_threshold, -1.0)

    def test_mid_tier_signal_uses_relaxed_zero_obv_threshold(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=70.0)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=1100.0,
            sma_value=1000.0,
            increment_value=5.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=0.1,
        )

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch(
                "market_adaptive.strategies.cta_robot.compute_volume_profile",
                return_value=type("VolumeProfile", (), {
                    "above_poc": lambda self, price: True,
                    "contains_price": lambda self, price: False,
                    "above_value_area": lambda self, price: True,
                })(),
            ),
        ):
            signal = robot._build_trend_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertTrue(signal.obv_confirmation_passed)
        self.assertAlmostEqual(signal.obv_threshold, -0.1)
        self.assertAlmostEqual(robot._effective_signal_obv_threshold(signal), -0.1)

    def test_heartbeat_payload_uses_relaxed_zero_obv_threshold_without_falling_back(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6, heartbeat_interval_seconds=300.0),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        signal = TrendSignal(
            direction=0,
            raw_direction=1,
            major_direction=1,
            bullish_ready=True,
            execution_breakout=True,
            execution_memory_active=True,
            execution_trigger_reason="memory+breakout",
            mtf_aligned=True,
            obv_bias=1,
            obv_confirmation=OBVConfirmationSnapshot(
                current_obv=2310.0,
                sma_value=261.1,
                increment_value=277.0,
                increment_mean=38.5,
                increment_std=191.26,
                zscore=0.1,
            ),
            obv_threshold=0.0,
            obv_confirmation_passed=True,
            volume_filter_passed=True,
            long_setup_blocked=False,
            price=100.0,
            atr=1.2,
            risk_percent=0.03,
        )

        payload = robot._build_signal_heartbeat_payload(signal)

        self.assertEqual(payload["obv_zscore_threshold"], 0.0)
        self.assertAlmostEqual(payload["obv_zscore_gap"], 0.1)

    def test_drive_first_relaxed_obv_can_pass_below_sma_when_not_dumping(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        mtf_signal = self._build_mtf_signal(bullish_score=72.0, major_direction=1)
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=990.0,
            sma_value=1000.0,
            increment_value=2.0,
            increment_mean=1.0,
            increment_std=2.0,
            zscore=-0.05,
        )

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch(
                "market_adaptive.strategies.cta_robot.compute_volume_profile",
                return_value=type("VolumeProfile", (), {
                    "above_poc": lambda self, price: True,
                    "contains_price": lambda self, price: False,
                    "above_value_area": lambda self, price: True,
                })(),
            ),
        ):
            signal = robot._build_trend_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, 1)
        self.assertFalse(signal.long_setup_blocked)
        self.assertTrue(signal.relaxed_entry)
        self.assertIn("OBV(-0.05) > Floor(-0.10)", signal.relaxed_reasons)

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
        self.assertEqual(payload["candidate_state"], "trigger_ready")
        self.assertEqual(payload["candidate_reason"], "Triggered via Memory Window: KDJ crossed 3 bars ago + Price Breakout NOW")
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
        self.assertEqual(robot._near_miss_samples[0].candidate_state, "trigger_ready")

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


    def test_candidate_state_progresses_from_setup_to_armed_to_trigger_ready(self) -> None:
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
            bullish_ready=True,
            execution_trigger_reason="waiting_execution_trigger",
            obv_confirmation=OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 1.0, 0.0),
            price=100.0,
            atr=1.0,
            risk_percent=0.02,
        )

        self.assertEqual(robot._derive_candidate_state(base_signal)[0], "setup")
        armed_signal = TrendSignal(**{**base_signal.__dict__, "execution_memory_active": True, "long_setup_reason": "obv_strength_not_confirmed"})
        self.assertEqual(robot._derive_candidate_state(armed_signal)[0], "armed")
        ready_signal = TrendSignal(**{**armed_signal.__dict__, "raw_direction": 1, "execution_breakout": True, "execution_trigger_reason": "Triggered via Memory Window"})
        self.assertEqual(robot._derive_candidate_state(ready_signal)[0], "trigger_ready")

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

class CTAShortSignalTests(unittest.TestCase):
    def test_build_trend_signal_can_emit_short_direction_from_normal_path(self) -> None:
        robot = CTARobot(
            client=DummyClient(),
            database=DummyDatabase(),
            config=CTAConfig(symbol="BTC/USDT", obv_zscore_threshold=0.6),
            execution_config=ExecutionConfig(),
            notifier=None,
            risk_manager=None,
            sentiment_analyst=None,
        )
        execution_frame = pd.DataFrame({
            "timestamp": pd.to_datetime([1_700_000_000_000], unit="ms", utc=True),
            "open": [100.0],
            "high": [101.0],
            "low": [98.0],
            "close": [99.0],
            "volume": [1000.0],
        })
        trigger = ExecutionTriggerSnapshot(
            kdj_golden_cross=False,
            kdj_dead_cross=True,
            bullish_memory_active=False,
            bearish_memory_active=True,
            bullish_cross_bars_ago=None,
            bearish_cross_bars_ago=1,
            prior_high_break=False,
            prior_low_break=True,
            prior_high=101.5,
            prior_low=99.5,
            reason="Triggered via Bearish Memory Window: KDJ crossed 1 bars ago + Price Breakdown NOW",
            frontrun_near_breakout=False,
        )
        mtf_signal = MTFSignal(
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            major_direction=-1,
            major_bias_score=60.0,
            weak_bull_bias=False,
            early_bullish=False,
            entry_size_multiplier=1.0,
            swing_rsi=42.0,
            swing_rsi_slope=-1.2,
            bullish_score=0.0,
            bullish_threshold=55.0,
            bullish_ready=False,
            execution_entry_mode="breakout_confirmed",
            execution_trigger=trigger,
            fully_aligned=True,
            current_price=99.0,
            execution_obv_zscore=-1.0,
            execution_obv_threshold=0.6,
            execution_atr=1.2,
            atr_price_ratio_pct=1.2,
            server_time_iso="",
            local_time_iso="",
            server_local_skew_ms=0,
            major_timestamp_ms=1,
            swing_timestamp_ms=1,
            execution_timestamp_ms=1,
            data_alignment_valid=True,
            data_mismatch_ms=0,
            blocker_reason="",
            major_frame=execution_frame.copy(),
            swing_frame=execution_frame.copy(),
            execution_frame=execution_frame,
            weak_bear_bias=False,
            early_bearish=False,
            bearish_score=70.0,
            bearish_threshold=55.0,
            bearish_ready=True,
        )
        obv_snapshot = OBVConfirmationSnapshot(
            current_obv=900.0,
            sma_value=1000.0,
            increment_value=-5.0,
            increment_mean=-1.0,
            increment_std=2.0,
            zscore=-1.0,
        )
        volume_profile = type("VolumeProfile", (), {
            "poc_price": 100.0,
            "value_area_low": 98.0,
            "value_area_high": 101.0,
            "above_poc": lambda self, price: False,
            "contains_price": lambda self, price: True,
            "above_value_area": lambda self, price: False,
        })()

        with (
            patch.object(robot.mtf_engine, "build_signal", return_value=mtf_signal),
            patch("market_adaptive.strategies.cta_robot.compute_obv", return_value=pd.Series([1.0])),
            patch("market_adaptive.strategies.cta_robot.compute_atr", return_value=pd.Series([1.2])),
            patch("market_adaptive.strategies.cta_robot.compute_obv_confirmation_snapshot", return_value=obv_snapshot),
            patch("market_adaptive.strategies.cta_robot.compute_volume_profile", return_value=volume_profile),
        ):
            signal = robot._build_trend_signal()

        assert signal is not None
        self.assertEqual(signal.direction, -1)
        self.assertEqual(signal.raw_direction, -1)
        self.assertTrue(signal.bearish_ready)
