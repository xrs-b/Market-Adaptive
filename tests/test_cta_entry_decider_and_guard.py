from __future__ import annotations

from types import SimpleNamespace

from market_adaptive.strategies.bad_entry_guard import BadEntryGuard
from market_adaptive.strategies.cta_robot import CTARobot, EntryPathway, TrendSignal
from market_adaptive.strategies.entry_decider_lite import EntryDeciderLite


class _Config(SimpleNamespace):
    entry_decider_allow_min_score = 72.0
    entry_decider_watch_min_score = 58.0
    entry_decider_block_max_score = 45.0
    entry_decider_conflict_gap_allow = 12.0
    entry_decider_conflict_gap_watch = 6.0
    bad_entry_long_falling_knife_rsi = 38.0
    bad_entry_short_falling_knife_rsi = 62.0
    bad_entry_support_guard_atr_ratio = 0.35


def _signal(**overrides):
    base = dict(
        direction=1,
        raw_direction=1,
        major_direction=1,
        major_bias_score=0.0,
        weak_bull_bias=False,
        weak_bear_bias=False,
        early_bullish=True,
        early_bearish=False,
        entry_size_multiplier=1.0,
        swing_rsi=54.0,
        swing_rsi_slope=1.0,
        bullish_score=82.0,
        bearish_score=24.0,
        bullish_threshold=60.0,
        bearish_threshold=40.0,
        bullish_ready=True,
        bearish_ready=False,
        execution_entry_mode="breakout_confirmed",
        execution_golden_cross=True,
        execution_breakout=True,
        execution_breakdown=False,
        execution_memory_active=False,
        execution_latch_active=False,
        execution_latch_price=None,
        execution_frontrun_near_breakout=False,
        execution_memory_bars_ago=None,
        execution_trigger_family="breakout",
        execution_trigger_group="confirmed",
        execution_trigger_reason="breakout_confirmed",
        pullback_near_support=False,
        volatility_squeeze_breakout=False,
        stretch_value=0.0,
        stretch_blocked=False,
        pending_retest=False,
        exhaustion_penalty_applied=False,
        mtf_aligned=True,
        obv_bias=1,
        obv_confirmation=SimpleNamespace(zscore=1.6, above_sma=True),
        obv_threshold=1.0,
        obv_confirmation_passed=True,
        volume_filter_passed=True,
        volume_profile=SimpleNamespace(poc_price=100.0, value_area_low=98.0, value_area_high=104.0),
        long_setup_blocked=False,
        long_setup_reason="",
        price=105.0,
        atr=2.0,
        risk_percent=0.01,
        blocker_reason="",
        data_alignment_valid=True,
        data_mismatch_ms=0,
        relaxed_entry=False,
        relaxed_reasons=(),
        quick_trade_mode=False,
        entry_pathway=EntryPathway.FAST_TRACK,
        signal_quality_tier="TIER_HIGH",
        signal_confidence=0.88,
        signal_strength_bonus=4.0,
        ml_model_used=True,
        ml_prediction=1,
        ml_probability_up=0.76,
        ml_aligned_confidence=0.76,
        ml_gate_passed=True,
        ml_gate_reason="pass",
    )
    base.update(overrides)
    return TrendSignal(**base)


def test_entry_decider_lite_allow_watch_block_semantics():
    decider = EntryDeciderLite(_Config())

    allow_result = decider.evaluate(_signal())
    watch_result = decider.evaluate(_signal(entry_pathway=EntryPathway.STANDARD, bullish_score=70.0, bearish_score=66.0, signal_confidence=0.61))
    block_result = decider.evaluate(_signal(major_direction=-1, bullish_score=54.0, bearish_score=52.0, obv_confirmation_passed=False, volume_filter_passed=False, signal_confidence=0.42, entry_pathway=EntryPathway.STRICT, relaxed_entry=True))

    assert allow_result.decision == "allow"
    assert watch_result.decision == "watch"
    assert block_result.decision == "block"


def test_bad_entry_guard_blocks_falling_knife_long_with_opposing_volume():
    guard = BadEntryGuard(_Config())

    result = guard.evaluate(
        _signal(
            major_direction=-1,
            swing_rsi=31.0,
            obv_bias=-1,
            obv_confirmation_passed=False,
            volume_filter_passed=False,
            price=96.0,
            volume_profile=SimpleNamespace(poc_price=100.0, value_area_low=98.0, value_area_high=103.0),
        )
    )

    assert result.blocked is True
    assert "falling_knife" in result.triggers
    assert "counter_trend" in result.triggers
    assert "opposing_volume" in result.triggers


def test_open_position_chain_keeps_watch_advisory_and_continues_to_sizing():
    robot = CTARobot.__new__(CTARobot)
    robot.symbol = "BTC/USDT"
    robot.entry_decider_lite = EntryDeciderLite(_Config())
    robot.bad_entry_guard = BadEntryGuard(_Config())
    published = []
    robot._publish_risk_profile = lambda payload: published.append(payload)
    robot._resolve_entry_amount = lambda signal, side: (None, 0.01, False)
    robot._resolve_final_entry_permit = lambda signal, side, amount: SimpleNamespace(allowed=True, action=None, position_side="long", notional_price=signal.price, order_flow_assessment=None)
    robot._execute_entry_and_build_position = lambda **kwargs: "cta:open_long"

    signal = _signal(entry_pathway=EntryPathway.STANDARD, bullish_score=70.0, bearish_score=66.0, signal_confidence=0.61)

    action = robot._open_position(signal)

    assert action == "cta:open_long"
    assert published == []


def test_open_position_chain_blocks_bad_trade_after_allow_decision():
    robot = CTARobot.__new__(CTARobot)
    robot.symbol = "BTC/USDT"
    robot.entry_decider_lite = EntryDeciderLite(_Config())
    robot.bad_entry_guard = BadEntryGuard(_Config())
    published = []
    robot._publish_risk_profile = lambda payload: published.append(payload)
    robot._resolve_entry_amount = lambda signal, side: (_ for _ in ()).throw(AssertionError("should not reach sizing"))
    robot._evaluate_entry_decision = lambda signal: SimpleNamespace(decision="allow", score=90.0, reasons=())

    signal = _signal(
        major_direction=-1,
        swing_rsi=31.0,
        obv_bias=-1,
        obv_confirmation_passed=False,
        volume_filter_passed=False,
        price=96.0,
        volume_profile=SimpleNamespace(poc_price=100.0, value_area_low=98.0, value_area_high=103.0),
        bullish_score=90.0,
        bearish_score=10.0,
        signal_confidence=0.95,
    )

    action = robot._open_position(signal)

    assert action == "cta:bad_entry_falling_knife"
    assert published == [None]
