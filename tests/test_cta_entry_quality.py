from __future__ import annotations

from types import SimpleNamespace

from market_adaptive.strategies.cta_robot import CTARobot, TrendSignal


def _robot() -> CTARobot:
    robot = CTARobot.__new__(CTARobot)
    robot.config = SimpleNamespace(
        minimum_expected_rr=0.0,
        relaxed_entry_minimum_expected_rr=0.8,
        relaxed_short_minimum_expected_rr=1.15,
        quick_trade_minimum_expected_rr=1.35,
        starter_entry_minimum_expected_rr=0.6,
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
