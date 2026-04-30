from __future__ import annotations

from types import SimpleNamespace

from market_adaptive.config import CTAConfig
from market_adaptive.ml_signal_engine import MLSignalDecision
from market_adaptive.strategies.cta_robot import CTARobot, EntryPathway, TrendSignal
from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment


def _robot() -> CTARobot:
    robot = CTARobot.__new__(CTARobot)
    robot.config = SimpleNamespace(
        minimum_expected_rr=0.0,
        relaxed_entry_minimum_expected_rr=0.8,
        relaxed_short_minimum_expected_rr=1.15,
        quick_trade_minimum_expected_rr=1.35,
        starter_entry_minimum_expected_rr=0.6,
        standard_entry_minimum_expected_rr=1.4,
        fast_track_reuse_cooldown_seconds=300,
        relaxed_short_minimum_score=48.0,
        relaxed_short_max_countertrend_score_gap=12.0,
        relaxed_short_require_early_or_breakdown=True,
        starter_quality_minimum_score=72.0,
        scale_in_quality_minimum_score=68.0,
        starter_countertrend_max_score_gap=10.0,
        ml_min_confidence=0.6,
        entry_location_score_min=-0.60,
    )
    robot.symbol = "BTC/USDT"
    return robot


def _signal(**overrides) -> TrendSignal:
    base = dict(
        direction=-1,
        raw_direction=-1,
        major_direction=1,
        major_bias_score=0.0,
        weak_bull_bias=False,
        weak_bear_bias=True,
        early_bullish=False,
        early_bearish=True,
        entry_size_multiplier=1.0,
        swing_rsi=50.0,
        swing_rsi_slope=0.0,
        bullish_score=58.0,
        bearish_score=50.0,
        bullish_threshold=60.0,
        bearish_threshold=40.0,
        bullish_ready=True,
        bearish_ready=True,
        execution_entry_mode="weak_bear_scale_in_limit",
        execution_golden_cross=False,
        execution_breakout=False,
        execution_breakdown=False,
        execution_memory_active=False,
        execution_latch_active=False,
        execution_latch_price=None,
        execution_frontrun_near_breakout=False,
        execution_memory_bars_ago=None,
        execution_trigger_family="bearish",
        execution_trigger_reason="early_bearish",
        mtf_aligned=True,
        obv_bias=-1,
        obv_confirmation=SimpleNamespace(zscore=0.0, above_sma=False),
        obv_threshold=1.0,
        obv_confirmation_passed=True,
        volume_filter_passed=True,
        volume_profile=None,
        long_setup_blocked=False,
        long_setup_reason="",
        price=75000.0,
        atr=300.0,
        risk_percent=0.01,
        blocker_reason="PASSED",
        data_alignment_valid=True,
        data_mismatch_ms=0,
        relaxed_entry=True,
        relaxed_reasons=("SHORT_SIDEWAYS_EXCEPTION",),
        quick_trade_mode=False,
    )
    base.update(overrides)
    return TrendSignal(**base)


def test_quality_filter_blocks_low_score_relaxed_short() -> None:
    robot = _robot()
    signal = _signal(bearish_score=44.0)

    filtered = robot._quality_filter_short_signal(signal)

    assert filtered.direction == 0
    assert filtered.long_setup_reason == "relaxed_short_low_quality"
    assert filtered.relaxed_entry is False


def test_quality_filter_blocks_overly_countertrend_relaxed_short() -> None:
    robot = _robot()
    signal = _signal(bullish_score=70.0, bearish_score=50.0)

    filtered = robot._quality_filter_short_signal(signal)

    assert filtered.direction == 0
    assert filtered.blocker_reason == "Blocked_By_RELAXED_SHORT_LOW_QUALITY"


def test_resolve_minimum_expected_rr_raises_floor_for_relaxed_short() -> None:
    robot = _robot()
    signal = _signal(relaxed_entry=True, quick_trade_mode=False)

    assert robot._resolve_minimum_expected_rr(signal) == 1.15


def test_resolve_minimum_expected_rr_raises_floor_for_quick_trade() -> None:
    robot = _robot()
    signal = _signal(relaxed_entry=True, quick_trade_mode=True)

    assert robot._resolve_minimum_expected_rr(signal) == 1.35


def test_starter_quality_gate_blocks_low_score_starter() -> None:
    robot = _robot()
    signal = _signal(direction=1, raw_direction=1, execution_entry_mode="starter_frontrun_limit", bullish_score=68.0, bearish_score=20.0)

    passed, reason = robot._starter_entry_passes_quality_gate(signal)

    assert passed is False
    assert reason == "starter_entry_low_score"


def test_starter_quality_gate_blocks_countertrend_gap() -> None:
    robot = _robot()
    signal = _signal(direction=-1, raw_direction=-1, execution_entry_mode="weak_bear_scale_in_limit", bearish_score=69.0, bullish_score=82.0, major_direction=1)

    passed, reason = robot._starter_entry_passes_quality_gate(signal)

    assert passed is False
    assert reason == "starter_entry_countertrend"


def test_starter_quality_gate_allows_good_quality_starter() -> None:
    robot = _robot()
    signal = _signal(direction=-1, raw_direction=-1, execution_entry_mode="weak_bear_scale_in_limit", bearish_score=74.0, bullish_score=60.0, major_direction=1)

    passed, reason = robot._starter_entry_passes_quality_gate(signal)

    assert passed is True
    assert reason is None


def test_resolve_entry_pathway_fast_track_for_high_quality_aligned_signal() -> None:
    robot = _robot()
    robot.config.tier_high_confidence_threshold = 0.8
    mtf_signal = SimpleNamespace(
        signal_quality_tier=SimpleNamespace(name="TIER_HIGH"),
        signal_confidence=0.86,
        fully_aligned=True,
    )

    assert robot._resolve_entry_pathway(mtf_signal) == EntryPathway.FAST_TRACK


def test_resolve_entry_pathway_standard_for_medium_quality_signal() -> None:
    robot = _robot()
    mtf_signal = SimpleNamespace(
        signal_quality_tier=SimpleNamespace(name="TIER_MEDIUM"),
        signal_confidence=0.55,
        fully_aligned=False,
    )

    assert robot._resolve_entry_pathway(mtf_signal) == EntryPathway.STANDARD


def test_resolve_entry_pathway_standard_for_major_bull_retest_even_when_high_quality() -> None:
    robot = _robot()
    robot.config.tier_high_confidence_threshold = 0.8
    mtf_signal = SimpleNamespace(
        signal_quality_tier=SimpleNamespace(name="TIER_HIGH"),
        signal_confidence=1.0,
        fully_aligned=True,
        execution_trigger=SimpleNamespace(family="major_bull_retest"),
    )

    assert robot._resolve_entry_pathway(mtf_signal) == EntryPathway.STANDARD


def test_resolve_entry_pathway_strict_for_low_quality_signal() -> None:
    robot = _robot()
    mtf_signal = SimpleNamespace(
        signal_quality_tier=SimpleNamespace(name="TIER_LOW"),
        signal_confidence=0.0,
        fully_aligned=False,
    )

    assert robot._resolve_entry_pathway(mtf_signal) == EntryPathway.STRICT


def test_fast_track_uses_lighter_rr_floor() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        relaxed_entry=False,
        quick_trade_mode=False,
        execution_entry_mode="breakout_confirmed",
        entry_pathway=EntryPathway.FAST_TRACK,
    )

    assert robot._resolve_minimum_expected_rr_for_pathway(signal) == 0.0


def test_standard_path_uses_its_own_rr_floor() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        relaxed_entry=False,
        quick_trade_mode=False,
        execution_entry_mode="breakout_confirmed",
        entry_pathway=EntryPathway.STANDARD,
    )

    assert robot._resolve_minimum_expected_rr_for_pathway(signal) == 1.4


def test_apply_ml_entry_gate_blocks_low_confidence_signal() -> None:
    robot = _robot()
    robot.ml_engine = SimpleNamespace(
        evaluate=lambda **kwargs: MLSignalDecision(
            used_model=True,
            prediction=0,
            probability_up=0.18,
            aligned_confidence=0.18,
            gate_passed=False,
            reason="ml_low_confidence_or_counter_direction",
        )
    )

    direction, blocked, reason, ml_decision = robot._apply_ml_entry_gate(
        execution_frame=SimpleNamespace(),
        final_direction=1,
        long_setup_blocked=False,
        long_setup_reason="",
    )

    assert direction == 0
    assert blocked is True
    assert reason == "ml_gate_blocked:ml_low_confidence_or_counter_direction"
    assert ml_decision.used_model is True
    assert ml_decision.gate_passed is False


def test_apply_ml_entry_gate_preserves_signal_when_model_missing() -> None:
    robot = _robot()
    robot.ml_engine = SimpleNamespace(
        evaluate=lambda **kwargs: MLSignalDecision(
            used_model=False,
            prediction=0,
            probability_up=0.5,
            aligned_confidence=0.5,
            gate_passed=True,
            reason="ml_model_missing",
        )
    )

    direction, blocked, reason, ml_decision = robot._apply_ml_entry_gate(
        execution_frame=SimpleNamespace(),
        final_direction=-1,
        long_setup_blocked=False,
        long_setup_reason="",
    )

    assert direction == -1
    assert blocked is False
    assert reason == ""
    assert ml_decision.reason == "ml_model_missing"


def test_major_bull_retest_long_no_longer_requires_prior_high_break() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=True,
        weak_bear_bias=False,
        early_bullish=True,
        early_bearish=False,
        bullish_score=72.0,
        bearish_score=20.0,
        bullish_threshold=55.0,
        bearish_threshold=45.0,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_breakout=False,
        execution_trigger_family="major_bull_retest",
        execution_trigger_reason="major_bull_retest_ready: gap=0.120% + KDJ memory 2 bars ago",
        entry_pathway=EntryPathway.STANDARD,
        relaxed_entry=False,
        relaxed_reasons=(),
    )

    assert robot._resolve_trigger_family_gate_reason(signal) is None


def test_near_breakout_release_long_is_not_disabled_by_legacy_missing_flag_default() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=True,
        weak_bear_bias=False,
        early_bullish=True,
        early_bearish=False,
        bullish_score=86.0,
        bearish_score=12.0,
        bullish_threshold=55.0,
        bearish_threshold=45.0,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_breakout=False,
        execution_trigger_family="near_breakout_release",
        execution_trigger_reason="near_breakout_release: bullish_score=86 + gap=0.080% + latch_or_memory_active",
        entry_pathway=EntryPathway.FAST_TRACK,
        relaxed_entry=False,
        relaxed_reasons=(),
    )

    assert robot._resolve_trigger_family_gate_reason(signal) is None


def test_fast_track_blocks_on_strong_reverse_obv() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        entry_pathway=EntryPathway.FAST_TRACK,
        obv_confirmation=SimpleNamespace(zscore=-1.2, above_sma=False),
        obv_threshold=1.0,
    )
    robot.symbol = "BTC/USDT"

    assert robot._apply_fast_track_checks(signal) == "cta:fast_track_blocked"


def test_major_bull_retest_fast_track_keeps_upstream_relaxed_confirmation() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=False,
        early_bearish=False,
        bullish_score=98.0,
        bearish_score=20.0,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_breakout=False,
        execution_trigger_family="major_bull_retest",
        execution_trigger_reason="major_bull_retest_ready: gap=0.225% + KDJ memory 2 bars ago",
        entry_pathway=EntryPathway.FAST_TRACK,
        relaxed_entry=True,
        relaxed_reasons=("VA:Edge Proximity",),
    )

    assert robot._resolve_trigger_family_gate_reason(signal) is None


def test_standard_order_flow_soft_passes_on_non_empty_book_warning() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.STANDARD)
    assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=20,
        bid_sum=10.0,
        ask_sum=9.0,
        imbalance_ratio=1.11,
        best_bid=100.0,
        best_ask=100.1,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="imbalance_decay_detected",
        history_mean=1.3,
        history_sigma=0.1,
        health_floor=1.2,
        confirmation_threshold=1.2,
        high_conviction_threshold=1.5,
        decay_detected=True,
    )

    assert robot._apply_standard_order_flow_checks(signal=signal, side="buy", amount=1.0, order_flow_assessment=assessment) is None


def test_standard_order_flow_blocks_on_empty_order_book() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.STANDARD)
    assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=20,
        bid_sum=0.0,
        ask_sum=0.0,
        imbalance_ratio=0.0,
        best_bid=0.0,
        best_ask=0.0,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="empty_order_book",
        history_mean=0.0,
        history_sigma=0.0,
        health_floor=0.0,
        confirmation_threshold=1.2,
        high_conviction_threshold=1.5,
        decay_detected=False,
    )

    assert robot._apply_standard_order_flow_checks(signal=signal, side="buy", amount=1.0, order_flow_assessment=assessment) == "cta:order_flow_blocked"


def test_strict_order_flow_keeps_major_bull_retest_on_strict_confirmation_path() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot.config.order_flow_confirmation_ratio = 0.60
    signal = _signal(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=False,
        early_bearish=False,
        bullish_score=84.0,
        bearish_score=18.0,
        bearish_ready=False,
        execution_trigger_family="major_bull_retest",
        execution_trigger_reason="major_bull_retest_ready: gap=0.120% + KDJ memory 2 bars ago",
        entry_pathway=EntryPathway.STANDARD,
        relaxed_entry=True,
        execution_breakout=False,
    )
    assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=20,
        bid_sum=12.0,
        ask_sum=11.0,
        imbalance_ratio=0.55,
        best_bid=95000.0,
        best_ask=95010.0,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="normal_decay",
        history_mean=0.0,
        history_sigma=0.0,
        health_floor=0.0,
        confirmation_threshold=0.60,
        high_conviction_threshold=0.80,
        decay_detected=False,
    )

    assert robot._apply_strict_order_flow_checks(signal=signal, side="buy", amount=1.0, order_flow_assessment=assessment) == "cta:order_flow_blocked"


def test_strict_order_flow_softens_trend_continuation_near_breakout_when_base_ratio_still_holds() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot.config.order_flow_confirmation_ratio = 0.60
    signal = _signal(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=False,
        early_bearish=False,
        bullish_score=84.0,
        bearish_score=18.0,
        bearish_ready=False,
        execution_trigger_family="trend_continuation_near_breakout",
        execution_trigger_reason="trend_continuation_near_breakout_ready: bullish_score=84 + gap=0.120% + obv_support=confirmed",
        entry_pathway=EntryPathway.FAST_TRACK,
        relaxed_entry=False,
        execution_breakout=True,
    )
    assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=20,
        bid_sum=13.0,
        ask_sum=12.0,
        imbalance_ratio=0.61,
        best_bid=95000.0,
        best_ask=95010.0,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="normal_decay",
        history_mean=0.0,
        history_sigma=0.0,
        health_floor=0.0,
        confirmation_threshold=0.60,
        high_conviction_threshold=0.80,
        decay_detected=False,
    )

    assert robot._apply_strict_order_flow_checks(signal=signal, side="buy", amount=1.0, order_flow_assessment=assessment) is None


def test_reset_local_position_arms_same_direction_cooldown_for_position_mismatch() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    from collections import deque
    robot._same_direction_stop_events = {"long": deque(), "short": deque()}
    robot.position = SimpleNamespace(side="long")
    robot._publish_risk_profile = lambda signal: None

    robot.reset_local_position("position_mismatch")

    assert robot._same_direction_cooldown_until["long"] > 1000.0


def test_prepare_entry_execution_context_blocks_same_direction_during_cooldown() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 1200.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.FAST_TRACK)

    result, position_side, notional_price, order_flow = robot._prepare_entry_execution_context(signal=signal, side="buy", amount=1.0)

    assert result == "cta:same_direction_cooldown"
    assert position_side == "long"
    assert notional_price == 0.0
    assert order_flow is None


def test_resolve_final_entry_permit_blocks_same_direction_during_cooldown() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 1200.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.FAST_TRACK)

    permit = robot._resolve_final_entry_permit(signal=signal, side="buy", amount=1.0)

    assert permit.allowed is False
    assert permit.status == "blocked"
    assert permit.action == "cta:same_direction_cooldown"
    assert permit.stage == "cooldown"
    assert permit.reason == "same_direction_cooldown"
    assert permit.position_side == "long"


def test_resolve_final_entry_permit_returns_limit_protect_when_order_flow_recommends_limit() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    robot._assess_order_flow = lambda **kwargs: OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=20,
        bid_sum=20.0,
        ask_sum=8.0,
        imbalance_ratio=2.5,
        best_bid=100.0,
        best_ask=100.1,
        confirmation_passed=True,
        high_conviction=True,
        recommended_limit_price=100.2,
        expected_average_price=100.15,
        depth_boundary_price=100.25,
        reason="confirmed_high_conviction",
        history_mean=1.8,
        history_sigma=0.1,
        health_floor=1.7,
        confirmation_threshold=1.5,
        high_conviction_threshold=2.0,
        decay_detected=False,
    )
    robot._apply_entry_pathway_checks = lambda **kwargs: None
    robot._check_entry_risk_budget = lambda **kwargs: None
    robot._check_entry_reward_risk = lambda **kwargs: None
    robot._log_trade_open_context = lambda **kwargs: None
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.STANDARD, relaxed_entry=False, price=100.0)

    permit = robot._resolve_final_entry_permit(signal=signal, side="buy", amount=1.0)

    assert permit.allowed is True
    assert permit.status == "limit_protect"
    assert permit.stage == "final"
    assert permit.reason == "limit_protect"
    assert permit.position_side == "long"
    assert permit.notional_price == 100.1
    assert permit.order_flow_assessment is not None
    assert permit.order_flow_assessment.final_permit.limit_price_protected is True


def test_resolve_final_entry_permit_blocks_on_reward_risk() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    robot._assess_order_flow = lambda **kwargs: None
    robot._apply_entry_pathway_checks = lambda **kwargs: None
    robot._check_entry_risk_budget = lambda **kwargs: None
    robot._check_entry_reward_risk = lambda **kwargs: "cta:reward_risk_blocked"
    robot._log_trade_open_context = lambda **kwargs: None
    signal = _signal(direction=1, raw_direction=1, entry_pathway=EntryPathway.STANDARD, relaxed_entry=False, price=100.0)

    permit = robot._resolve_final_entry_permit(signal=signal, side="buy", amount=1.0)

    assert permit.allowed is False
    assert permit.status == "blocked"
    assert permit.action == "cta:reward_risk_blocked"
    assert permit.stage == "reward_risk"
    assert permit.reason == "reward_risk_blocked"
    assert permit.position_side == "long"
    assert permit.notional_price == 100.0


def test_arm_fast_track_reuse_cooldown_blocks_repeated_fast_track_long_setup() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    signal = _signal(
        direction=1,
        raw_direction=1,
        entry_pathway=EntryPathway.FAST_TRACK,
        execution_trigger_family="bullish_memory_breakout",
        execution_trigger_reason="Triggered via Memory Window: KDJ crossed 4 bars ago + Price Breakout NOW",
        execution_memory_bars_ago=4,
    )
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}

    robot._arm_fast_track_reuse_cooldown(signal)
    result, position_side, notional_price, order_flow = robot._prepare_entry_execution_context(signal=signal, side="buy", amount=1.0)

    assert result == "cta:fast_track_reuse_cooldown"
    assert position_side == "long"
    assert notional_price == 0.0
    assert order_flow is None


def test_trigger_ready_signal_can_be_blocked_into_candidate_semantics_by_final_permit() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._time_provider = lambda: 1000.0
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    robot._assess_order_flow = lambda **kwargs: None
    robot._apply_entry_pathway_checks = lambda **kwargs: None
    robot._check_entry_risk_budget = lambda **kwargs: None
    robot._check_entry_reward_risk = lambda **kwargs: "cta:reward_risk_blocked"
    robot._log_trade_open_context = lambda **kwargs: None
    signal = _signal(
        direction=1,
        raw_direction=1,
        bullish_ready=True,
        execution_memory_active=True,
        execution_breakout=True,
        execution_trigger_reason="Triggered via Memory Window",
    )
    signal = TrendSignal(**{**signal.__dict__, "candidate_state": robot._derive_candidate_state(signal)[0], "candidate_reason": robot._derive_candidate_state(signal)[1]})

    permit = robot._resolve_final_entry_permit(signal=signal, side="buy", amount=1.0)

    assert signal.candidate_state == "trigger_ready"
    assert permit.allowed is False
    assert permit.reason == "reward_risk_blocked"


def test_missing_legacy_disable_flags_do_not_block_trend_continuation_or_memory_breakout() -> None:
    robot = _robot()

    trend_signal = _signal(
        direction=1,
        raw_direction=1,
        entry_pathway=EntryPathway.FAST_TRACK,
        execution_entry_mode="breakout_confirmed",
        execution_trigger_family="trend_continuation_near_breakout",
        signal_quality_tier="TIER_HIGH",
        relaxed_entry=False,
    )
    memory_signal = _signal(
        direction=1,
        raw_direction=1,
        entry_pathway=EntryPathway.STANDARD,
        execution_entry_mode="breakout_confirmed",
        execution_trigger_family="bullish_memory_breakout",
        signal_confidence=0.75,
        relaxed_entry=False,
    )

    assert robot._resolve_trigger_family_gate_reason(trend_signal) is None
    assert robot._resolve_trigger_family_gate_reason(memory_signal) is None


def test_near_ready_blocked_signal_maps_to_armed_candidate_state() -> None:
    robot = _robot()
    signal = _signal(
        direction=0,
        raw_direction=0,
        bullish_ready=True,
        execution_memory_active=True,
        execution_trigger_reason="Triggered via Memory Window",
        long_setup_blocked=True,
        long_setup_reason="obv_strength_not_confirmed",
        blocker_reason="Blocked_By_OBV_STRENGTH_NOT_CONFIRMED",
    )

    candidate_state, candidate_reason = robot._derive_candidate_state(signal)

    assert candidate_state == "armed"
    assert candidate_reason == "obv_strength_not_confirmed"


def _signal_sweep(**overrides) -> TrendSignal:
    base = dict(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=True,
        early_bearish=False,
        entry_size_multiplier=1.0,
        swing_rsi=55.0,
        swing_rsi_slope=0.0,
        bullish_score=78.0,
        bearish_score=20.0,
        bullish_threshold=55.0,
        bearish_threshold=45.0,
        bullish_ready=True,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_golden_cross=False,
        execution_breakout=True,
        execution_breakdown=False,
        execution_memory_active=False,
        execution_latch_active=False,
        execution_latch_price=None,
        execution_frontrun_near_breakout=False,
        execution_memory_bars_ago=None,
        execution_trigger_family="spring_reclaim",
        execution_trigger_reason="spring_reclaim: swept prior low",
        liquidity_sweep=True,
        liquidity_sweep_side="long",
        oi_change_pct=0.25,
        funding_rate=0.0003,
        is_short_squeeze=False,
        is_long_liquidation=False,
        resonance_allowed=False,
        resonance_reason="",
        reverse_intercepted=False,
        reverse_intercept_reason="",
        sweep_extreme_price=99.5,
        price=100.0,
        atr=0.5,
        risk_percent=0.01,
        blocker_reason="PASSED",
        data_alignment_valid=True,
        data_mismatch_ms=0,
        relaxed_entry=False,
        relaxed_reasons=(),
        entry_pathway=EntryPathway.FAST_TRACK,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        signal_strength_bonus=5.0,
        obv_confirmation=SimpleNamespace(zscore=0.8, above_sma=True),
        obv_threshold=1.0,
        obv_confirmation_passed=True,
        volume_filter_passed=True,
        volume_profile=None,
        long_setup_blocked=False,
        long_setup_reason="",
    )
    base.update(overrides)
    return TrendSignal(**base)


def test_is_breakout_style_signal_detects_breakout_mode() -> None:
    robot = _robot()
    signal = _signal(execution_entry_mode="breakout_confirmed", execution_breakout=True)
    assert robot._is_breakout_style_signal(signal) is True


def test_is_breakout_style_signal_detects_sweep_family() -> None:
    robot = _robot()
    signal = _signal(execution_trigger_family="spring_reclaim", execution_entry_mode="standard")
    assert robot._is_breakout_style_signal(signal) is True


def test_is_breakout_style_signal_false_for_pullback() -> None:
    robot = _robot()
    signal = _signal(execution_entry_mode="pullback_entry", execution_trigger_family="waiting")
    assert robot._is_breakout_style_signal(signal) is False


def test_reverse_intercept_blocks_long_into_short_squeeze() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        execution_entry_mode="breakout_confirmed",
        execution_breakout=True,
        execution_trigger_family="breakout",
        is_short_squeeze=True,
        is_long_liquidation=False,
    )
    reason = robot._resolve_reverse_intercept_reason(signal)
    assert reason == "breakout_long_into_short_squeeze"


def test_reverse_intercept_blocks_short_into_long_liquidation() -> None:
    robot = _robot()
    signal = _signal(
        direction=-1,
        raw_direction=-1,
        execution_entry_mode="breakdown_confirmed",
        execution_breakdown=True,
        execution_trigger_family="breakdown",
        is_short_squeeze=False,
        is_long_liquidation=True,
    )
    reason = robot._resolve_reverse_intercept_reason(signal)
    assert reason == "breakout_short_into_long_liquidation"


def test_reverse_intercept_allows_non_breakout_signals() -> None:
    robot = _robot()
    signal = _signal(
        direction=1,
        raw_direction=1,
        execution_entry_mode="pullback_entry",
        execution_breakout=False,
        is_short_squeeze=True,
        is_long_liquidation=False,
    )
    reason = robot._resolve_reverse_intercept_reason(signal)
    assert reason is None


def test_sweep_resonance_long_oi_turn() -> None:
    robot = _robot()
    allowed, reason = robot._supports_sweep_resonance(
        direction=1,
        sweep_side="long",
        oi_change_pct=0.25,
        is_short_squeeze=False,
        is_long_liquidation=False,
    )
    assert allowed is True
    assert "SWEEP_RESONANCE_LONG_OI_TURN" in reason


def test_sweep_resonance_short_squeeze() -> None:
    robot = _robot()
    allowed, reason = robot._supports_sweep_resonance(
        direction=-1,
        sweep_side="short",
        oi_change_pct=-0.5,
        is_short_squeeze=True,
        is_long_liquidation=False,
    )
    assert allowed is True
    assert "SWEEP_RESONANCE_SHORT_SQUEEZE_FLUSH" in reason


def test_sweep_resonance_rejected_when_mismatched() -> None:
    robot = _robot()
    allowed, reason = robot._supports_sweep_resonance(
        direction=1,
        sweep_side="short",
        oi_change_pct=0.25,
        is_short_squeeze=False,
        is_long_liquidation=False,
    )
    assert allowed is False
    assert reason == ""


def test_sweep_resonance_rejected_below_threshold() -> None:
    robot = _robot()
    allowed, reason = robot._supports_sweep_resonance(
        direction=1,
        sweep_side="long",
        oi_change_pct=0.05,
        is_short_squeeze=False,
        is_long_liquidation=False,
    )
    assert allowed is False


def test_sweep_stop_anchor_long() -> None:
    robot = _robot()
    signal = _signal_sweep(direction=1, liquidity_sweep=True, liquidity_sweep_side="long", sweep_extreme_price=99.5)
    stop, reason = robot._resolve_sweep_stop_anchor(signal, entry_price=100.0, fallback_stop_distance=1.0)
    assert stop is not None
    assert stop < 100.0
    assert stop > 99.5


def test_sweep_stop_anchor_short() -> None:
    robot = _robot()
    signal = _signal_sweep(
        direction=-1,
        liquidity_sweep=True,
        liquidity_sweep_side="short",
        sweep_extreme_price=100.5,
        execution_breakdown=True,
        execution_trigger_family="upthrust_reclaim",
    )
    stop, reason = robot._resolve_sweep_stop_anchor(signal, entry_price=100.0, fallback_stop_distance=1.0)
    assert stop is not None
    assert stop > 100.0
    assert stop < 100.5


def test_sweep_stop_returns_none_when_not_sweep() -> None:
    robot = _robot()
    signal = _signal_sweep(liquidity_sweep=False)
    stop, reason = robot._resolve_sweep_stop_anchor(signal, entry_price=100.0, fallback_stop_distance=1.0)
    assert stop is None
    assert reason is None


def test_resonance_relaxes_rr_for_fast_track() -> None:
    robot = _robot()
    robot.config.sweep_resonance_rr_relaxation_ratio = 0.20
    signal = _signal_sweep(
        entry_pathway=EntryPathway.FAST_TRACK,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_entry_mode="starter_frontrun_limit",
        relaxed_entry=False,
    )
    rr = robot._resolve_minimum_expected_rr_for_pathway(signal)
    assert rr < 0.6


def test_apply_entry_pathway_checks_blocks_reverse_intercept() -> None:
    robot = _robot()
    robot.symbol = "BTC/USDT"
    robot._same_direction_cooldown_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_until = {"long": 0.0, "short": 0.0}
    robot._fast_track_reuse_signature = {"long": None, "short": None}
    robot._repeated_entry_zone_until = {"long": 0.0, "short": 0.0}
    signal = _signal(
        direction=1,
        raw_direction=1,
        entry_pathway=EntryPathway.FAST_TRACK,
        execution_entry_mode="breakout_confirmed",
        execution_breakout=True,
        execution_trigger_family="breakout",
        is_short_squeeze=True,
        is_long_liquidation=False,
        liquidity_sweep=False,
        liquidity_sweep_side="",
        oi_change_pct=0.0,
        funding_rate=0.0,
    )
    result = robot._apply_entry_pathway_checks(signal=signal, side="buy", amount=1.0, order_flow_assessment=None)
    assert result == "cta:reverse_intercept_blocked"


def _signal_sweep_high(**overrides) -> TrendSignal:
    """High-quality sweep signal for resonance tests."""
    base = dict(
        direction=1,
        raw_direction=1,
        major_direction=1,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=True,
        early_bearish=False,
        entry_size_multiplier=1.0,
        swing_rsi=55.0,
        swing_rsi_slope=0.0,
        bullish_score=78.0,
        bearish_score=20.0,
        bullish_threshold=55.0,
        bearish_threshold=45.0,
        bullish_ready=True,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_golden_cross=False,
        execution_breakout=True,
        execution_breakdown=False,
        execution_memory_active=False,
        execution_latch_active=False,
        execution_latch_price=None,
        execution_frontrun_near_breakout=False,
        execution_memory_bars_ago=None,
        execution_trigger_family="spring_reclaim",
        execution_trigger_reason="spring_reclaim: swept prior low",
        liquidity_sweep=True,
        liquidity_sweep_side="long",
        oi_change_pct=0.25,
        funding_rate=0.0003,
        is_short_squeeze=False,
        is_long_liquidation=False,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        reverse_intercepted=False,
        reverse_intercept_reason="",
        sweep_extreme_price=99.5,
        price=100.0,
        atr=0.5,
        risk_percent=0.01,
        blocker_reason="PASSED",
        data_alignment_valid=True,
        data_mismatch_ms=0,
        relaxed_entry=False,
        relaxed_reasons=(),
        entry_pathway=EntryPathway.FAST_TRACK,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        signal_strength_bonus=5.0,
        obv_confirmation=SimpleNamespace(zscore=0.8, above_sma=True),
        obv_threshold=1.0,
        obv_confirmation_passed=True,
        volume_filter_passed=True,
        volume_profile=None,
        long_setup_blocked=False,
        long_setup_reason="",
    )
    base.update(overrides)
    return TrendSignal(**base)


def test_resonance_execution_allowance_passes_for_high_quality_spring() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        execution_trigger_family="spring_reclaim",
        liquidity_sweep_side="long",
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        direction=1,
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        bullish_score=78.0,
    )
    allowed, tag = robot._resonance_execution_allowance(signal)
    assert allowed is True
    assert "spring_reclaim" in tag


def test_resonance_execution_allowance_passes_for_high_quality_upthrust() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        execution_trigger_family="upthrust_reclaim",
        liquidity_sweep_side="short",
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_SHORT_SQUEEZE_FLUSH",
        direction=-1,
        major_direction=-1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.82,
        bullish_score=78.0,
        bearish_score=75.0,
    )
    allowed, tag = robot._resonance_execution_allowance(signal)
    assert allowed is True
    assert "upthrust_reclaim" in tag


def test_resonance_execution_allowance_fails_for_wrong_side() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        direction=-1,
        liquidity_sweep_side="long",
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="spring_reclaim",
        major_direction=-1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
    )
    allowed, tag = robot._resonance_execution_allowance(signal)
    assert allowed is False


def test_resonance_execution_allowance_fails_for_low_confidence() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="spring_reclaim",
        major_direction=1,
        signal_quality_tier="TIER_MEDIUM",
        signal_confidence=0.50,
    )
    allowed, _tag = robot._resonance_execution_allowance(signal)
    assert allowed is False


def test_resonance_execution_allowance_fails_for_low_tier() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="spring_reclaim",
        major_direction=1,
        signal_quality_tier="TIER_LOW",
        signal_confidence=0.70,
    )
    allowed, _tag = robot._resonance_execution_allowance(signal)
    assert allowed is False


def test_resonance_execution_allowance_fails_for_wrong_trigger_family() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="near_breakout_release",
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
    )
    allowed, _tag = robot._resonance_execution_allowance(signal)
    assert allowed is False


def test_resonance_execution_allowance_fails_when_resonance_not_allowed() -> None:
    robot = _robot()
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=False,
        resonance_reason="",
        execution_trigger_family="spring_reclaim",
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
    )
    allowed, tag = robot._resonance_execution_allowance(signal)
    assert allowed is False
    assert tag == ""


def test_resonance_relaxes_starter_quality_gate() -> None:
    robot = _robot()
    robot.config.sweep_resonance_quality_relaxation = 4.0
    signal = _signal_sweep_high(
        execution_entry_mode="starter_frontrun_limit",
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        direction=1,
        bullish_score=70.0,
        bearish_score=20.0,
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        execution_trigger_family="spring_reclaim",
        liquidity_sweep_side="long",
    )
    robot.config.sweep_resonance_execution_min_score = 68.0
    passed, reason = robot._starter_entry_passes_quality_gate(signal)
    assert passed is True


def test_resonance_does_not_relax_starter_quality_when_resonance_not_active() -> None:
    robot = _robot()
    robot.config.sweep_resonance_quality_relaxation = 4.0
    signal = _signal_sweep_high(
        execution_entry_mode="starter_frontrun_limit",
        resonance_allowed=False,
        resonance_reason="",
        direction=1,
        bullish_score=70.0,
        bearish_score=20.0,
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        execution_trigger_family="spring_reclaim",
        liquidity_sweep_side="long",
    )
    passed, reason = robot._starter_entry_passes_quality_gate(signal)
    assert passed is False
    assert "starter_entry_low_score" in str(reason)


def test_resonance_bypasses_order_flow_soft_warning() -> None:
    robot = _robot()
    robot.config.sweep_resonance_of_relaxation_floor = 0.40
    from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment
    of_assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=10,
        bid_sum=50000.0,
        ask_sum=75000.0,
        imbalance_ratio=0.45,
        best_bid=95000.0,
        best_ask=95010.0,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="normal_decay",
        history_mean=0.0,
        history_sigma=0.0,
        health_floor=0.0,
        confirmation_threshold=0.60,
        high_conviction_threshold=0.80,
        decay_detected=False,
    )
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="spring_reclaim",
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        execution_entry_mode="starter_frontrun_limit",
        liquidity_sweep_side="long",
    )
    result = robot._apply_strict_order_flow_checks(
        signal=signal,
        side="buy",
        amount=1.0,
        order_flow_assessment=of_assessment,
    )
    assert result is None


def test_resonance_order_flow_still_blocks_empty_book() -> None:
    robot = _robot()
    robot.config.sweep_resonance_of_relaxation_floor = 0.40
    from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment
    of_assessment = OrderFlowAssessment(
        symbol="BTC/USDT",
        side="buy",
        depth_levels=10,
        bid_sum=0.0,
        ask_sum=0.0,
        imbalance_ratio=0.0,
        best_bid=None,
        best_ask=None,
        confirmation_passed=False,
        high_conviction=False,
        recommended_limit_price=None,
        expected_average_price=None,
        depth_boundary_price=None,
        reason="empty_order_book",
        history_mean=0.0,
        history_sigma=0.0,
        health_floor=0.0,
        confirmation_threshold=0.60,
        high_conviction_threshold=0.80,
        decay_detected=False,
    )
    signal = _signal_sweep_high(
        direction=1,
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        execution_trigger_family="spring_reclaim",
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        execution_entry_mode="starter_frontrun_limit",
        liquidity_sweep_side="long",
    )
    result = robot._apply_strict_order_flow_checks(
        signal=signal,
        side="buy",
        amount=1.0,
        order_flow_assessment=of_assessment,
    )
    assert result == "cta:order_flow_blocked"


def test_resonance_relaxes_entry_location_block() -> None:
    robot = _robot()
    robot.config.sweep_resonance_location_relaxation = 0.12
    signal = _signal_sweep_high(
        execution_entry_mode="starter_frontrun_limit",
        resonance_allowed=True,
        resonance_reason="SWEEP_RESONANCE_LONG_OI_TURN(0.25%)",
        direction=1,
        major_direction=1,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.85,
        execution_trigger_family="spring_reclaim",
        liquidity_sweep_side="long",
        execution_breakout=True,
        entry_location_score=-0.50,
    )
    reason = robot._resolve_entry_location_block_reason(signal)
    assert reason is None
