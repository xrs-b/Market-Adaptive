from __future__ import annotations

from types import SimpleNamespace

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
    )
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
