from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from market_adaptive.indicators import OBVConfirmationSnapshot, compute_obv_confirmation_snapshot
from market_adaptive.strategies.mtf_engine import MTFSignal


_LONG_RECOVERY_MARKERS = (
    "early_bullish",
    "weak_bull",
    "weak bull",
    "recovery",
    "scale_in",
    "starter",
)

_SHORT_RECOVERY_MARKERS = (
    "early_bearish",
    "weak_bear",
    "weak bear",
    "distribution",
    "topping",
    "cover",
    "fade",
)

_HIGH_QUALITY_LONG_TRIGGER_MARKERS = (
    "major_bull_retest_ready",
    "memory+breakout",
    "triggered via memory window",
)

_MAJOR_BULL_RETEST_LONG_TRIGGER_MARKERS = (
    "major_bull_retest_ready",
)

_POST_TRIGGER_SOFT_OBV_THRESHOLD = 0.50
_MAJOR_BULL_RETEST_HIGH_SCORE_OBV_THRESHOLD = 0.25
_SHORT_RECOVERY_OBV_GRACE_ZSCORE_CEILING = 0.26
_SHORT_RECOVERY_OBV_GRACE_LOOKBACK_BARS = 2


def _contains_any_marker(text: str, markers: tuple[str, ...]) -> bool:
    haystack = str(text or "").lower()
    return any(marker in haystack for marker in markers)


def _resolve_obv_side(
    *,
    side: str = "long",
    early_bullish: bool = False,
    weak_bull_bias: bool = False,
    early_bearish: bool = False,
    weak_bear_bias: bool = False,
    trigger_reason: str = "",
    execution_entry_mode: str = "",
    major_direction: int | None = None,
) -> str:
    explicit_side = str(side or "long").lower()
    if explicit_side in {"long", "short"}:
        return explicit_side
    if bool(early_bullish) or bool(weak_bull_bias):
        return "long"
    if bool(early_bearish) or bool(weak_bear_bias):
        return "short"
    if _contains_any_marker(trigger_reason, _SHORT_RECOVERY_MARKERS) or _contains_any_marker(execution_entry_mode, _SHORT_RECOVERY_MARKERS):
        return "short"
    if _contains_any_marker(trigger_reason, _LONG_RECOVERY_MARKERS) or _contains_any_marker(execution_entry_mode, _LONG_RECOVERY_MARKERS):
        return "long"
    if major_direction is not None and int(major_direction) < 0:
        return "short"
    return "long"


def _is_recovery_context(
    *,
    side: str = "long",
    early_bullish: bool = False,
    weak_bull_bias: bool = False,
    early_bearish: bool = False,
    weak_bear_bias: bool = False,
    trigger_reason: str = "",
    execution_entry_mode: str = "",
) -> bool:
    if str(side).lower() == "short":
        if bool(early_bearish) or bool(weak_bear_bias):
            return True
        return _contains_any_marker(trigger_reason, _SHORT_RECOVERY_MARKERS) or _contains_any_marker(execution_entry_mode, _SHORT_RECOVERY_MARKERS)
    if bool(early_bullish) or bool(weak_bull_bias):
        return True
    return _contains_any_marker(trigger_reason, _LONG_RECOVERY_MARKERS) or _contains_any_marker(execution_entry_mode, _LONG_RECOVERY_MARKERS)


def _is_high_quality_long_post_trigger_context(
    *,
    side: str = "long",
    bullish_score: float,
    early_bullish: bool = False,
    weak_bull_bias: bool = False,
    early_bearish: bool = False,
    weak_bear_bias: bool = False,
    trigger_reason: str = "",
    execution_entry_mode: str = "",
) -> bool:
    if str(side).lower() != "long":
        return False
    if bool(early_bullish) or bool(weak_bull_bias) or bool(early_bearish) or bool(weak_bear_bias):
        return False
    score = float(bullish_score)
    if score < 55.0 or score >= 65.0:
        return False
    if _contains_any_marker(execution_entry_mode, _LONG_RECOVERY_MARKERS) or _contains_any_marker(trigger_reason, _LONG_RECOVERY_MARKERS):
        return False
    return _contains_any_marker(trigger_reason, _HIGH_QUALITY_LONG_TRIGGER_MARKERS)


def _is_major_bull_retest_long_context(*, side: str = "long", trigger_reason: str = "") -> bool:
    return str(side).lower() == "long" and _contains_any_marker(trigger_reason, _MAJOR_BULL_RETEST_LONG_TRIGGER_MARKERS)


def detect_recent_short_obv_confirmation(
    execution_frame: pd.DataFrame,
    *,
    sma_period: int,
    zscore_window: int,
    lookback_bars: int = _SHORT_RECOVERY_OBV_GRACE_LOOKBACK_BARS,
) -> bool:
    frame = execution_frame if execution_frame is not None else pd.DataFrame()
    if len(frame) < 2:
        return False
    max_lookback = max(1, int(lookback_bars))
    end_index = len(frame) - 2
    start_index = max(0, end_index - max_lookback + 1)
    for idx in range(end_index, start_index - 1, -1):
        snapshot = compute_obv_confirmation_snapshot(
            frame.iloc[: idx + 1],
            sma_period=int(sma_period),
            zscore_window=int(zscore_window),
        )
        if snapshot.below_sma and float(snapshot.zscore) < 0.0:
            return True
    return False


@dataclass(frozen=True)
class OBVGateDecision:
    threshold: float
    exempt: bool
    side: str = "long"
    short_recovery_grace_active: bool = False
    short_recovery_grace_zscore_ceiling: float = _SHORT_RECOVERY_OBV_GRACE_ZSCORE_CEILING

    def passed(self, snapshot: OBVConfirmationSnapshot) -> bool:
        if self.exempt:
            return True
        if str(self.side).lower() == "short":
            short_confirmed = bool(snapshot.sell_confirmed(zscore_threshold=self.threshold))
            if short_confirmed:
                return True
            return bool(
                self.short_recovery_grace_active
                and bool(snapshot.below_sma)
                and float(snapshot.zscore) <= float(self.short_recovery_grace_zscore_ceiling)
            )
        return bool(snapshot.buy_confirmed(zscore_threshold=self.threshold))

    def check_summary(self, snapshot: OBVConfirmationSnapshot) -> str:
        if str(self.side).lower() == "short":
            comparison = "<="
            threshold_value = -float(self.threshold)
            label = "Short"
        else:
            comparison = ">="
            threshold_value = float(self.threshold)
            label = "Long"
        if abs(threshold_value) < 1e-12:
            threshold_value = 0.0
        outcome = "Passed" if self.passed(snapshot) else "Blocked"
        return f"[{label}] OBV ({float(snapshot.zscore):.2f}) {comparison} Dynamic Threshold ({threshold_value:.1f}) -> {outcome}"


def resolve_dynamic_obv_gate(
    *,
    bullish_score: float,
    configured_threshold: float,
    side: str = "long",
    major_direction: int | None = None,
    early_bullish: bool = False,
    weak_bull_bias: bool = False,
    early_bearish: bool = False,
    weak_bear_bias: bool = False,
    execution_frontrun_near_breakout: bool = False,
    trigger_reason: str = "",
    execution_entry_mode: str = "",
    recent_short_obv_confirmation: bool = False,
) -> OBVGateDecision:
    resolved_side = _resolve_obv_side(
        side=side,
        major_direction=major_direction,
        early_bullish=early_bullish,
        weak_bull_bias=weak_bull_bias,
        early_bearish=early_bearish,
        weak_bear_bias=weak_bear_bias,
        trigger_reason=trigger_reason,
        execution_entry_mode=execution_entry_mode,
    )
    strict_threshold = min(float(configured_threshold), 0.60)
    major_bull_retest_long_context = _is_major_bull_retest_long_context(
        side=resolved_side,
        trigger_reason=trigger_reason,
    )
    if float(bullish_score) >= 80.0 and not major_bull_retest_long_context:
        return OBVGateDecision(threshold=-1.0, exempt=True, side=resolved_side)
    recovery_context = _is_recovery_context(
        side=resolved_side,
        early_bullish=early_bullish,
        weak_bull_bias=weak_bull_bias,
        early_bearish=early_bearish,
        weak_bear_bias=weak_bear_bias,
        trigger_reason=trigger_reason,
        execution_entry_mode=execution_entry_mode,
    )
    short_recovery_grace_active = bool(
        resolved_side == "short"
        and recovery_context
        and bool(recent_short_obv_confirmation)
    )
    if recovery_context:
        return OBVGateDecision(
            threshold=0.0,
            exempt=False,
            side=resolved_side,
            short_recovery_grace_active=short_recovery_grace_active,
        )
    if major_bull_retest_long_context and float(bullish_score) >= 80.0:
        return OBVGateDecision(
            threshold=_MAJOR_BULL_RETEST_HIGH_SCORE_OBV_THRESHOLD,
            exempt=False,
            side=resolved_side,
        )
    if bool(execution_frontrun_near_breakout):
        return OBVGateDecision(threshold=-0.1 if resolved_side == "long" else 0.0, exempt=False, side=resolved_side)
    if resolved_side == "long" and major_direction is not None and int(major_direction) > 0 and float(bullish_score) >= 60.0:
        return OBVGateDecision(threshold=-0.1, exempt=False, side=resolved_side)
    if float(bullish_score) >= 65.0:
        return OBVGateDecision(threshold=0.0, exempt=False, side=resolved_side)
    if _is_high_quality_long_post_trigger_context(
        side=resolved_side,
        bullish_score=float(bullish_score),
        early_bullish=early_bullish,
        weak_bull_bias=weak_bull_bias,
        early_bearish=early_bearish,
        weak_bear_bias=weak_bear_bias,
        trigger_reason=trigger_reason,
        execution_entry_mode=execution_entry_mode,
    ):
        return OBVGateDecision(threshold=min(strict_threshold, _POST_TRIGGER_SOFT_OBV_THRESHOLD), exempt=False, side=resolved_side)
    return OBVGateDecision(threshold=strict_threshold, exempt=False, side=resolved_side)


def resolve_dynamic_obv_gate_for_signal(
    signal: MTFSignal,
    *,
    configured_threshold: float,
    obv_sma_period: int,
    obv_zscore_window: int,
) -> OBVGateDecision:
    return resolve_dynamic_obv_gate(
        bullish_score=float(signal.bullish_score),
        configured_threshold=float(configured_threshold),
        side="auto",
        major_direction=int(signal.major_direction),
        early_bullish=bool(signal.early_bullish),
        weak_bull_bias=bool(signal.weak_bull_bias),
        early_bearish=bool(getattr(signal, "early_bearish", False)),
        weak_bear_bias=bool(getattr(signal, "weak_bear_bias", False)),
        execution_frontrun_near_breakout=bool(signal.execution_trigger.frontrun_near_breakout),
        trigger_reason=str(signal.execution_trigger.reason),
        execution_entry_mode=str(signal.execution_entry_mode),
        recent_short_obv_confirmation=detect_recent_short_obv_confirmation(
            signal.execution_frame,
            sma_period=int(obv_sma_period),
            zscore_window=int(obv_zscore_window),
        ),
    )
