from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.mtf_engine import MTFSignal


def _is_recovery_context(*, early_bullish: bool = False, weak_bull_bias: bool = False, trigger_reason: str = "", execution_entry_mode: str = "") -> bool:
    if bool(early_bullish) or bool(weak_bull_bias):
        return True
    reason = str(trigger_reason or "").lower()
    entry_mode = str(execution_entry_mode or "").lower()
    recovery_markers = (
        "early_bullish",
        "weak_bull",
        "weak bull",
        "recovery",
        "scale_in",
        "starter",
    )
    return any(marker in reason or marker in entry_mode for marker in recovery_markers)


@dataclass(frozen=True)
class OBVGateDecision:
    threshold: float
    exempt: bool

    def passed(self, snapshot: OBVConfirmationSnapshot) -> bool:
        if self.exempt:
            return True
        return bool(snapshot.buy_confirmed(zscore_threshold=self.threshold))


def resolve_dynamic_obv_gate(
    *,
    bullish_score: float,
    configured_threshold: float,
    early_bullish: bool = False,
    weak_bull_bias: bool = False,
    trigger_reason: str = "",
    execution_entry_mode: str = "",
) -> OBVGateDecision:
    strict_threshold = min(float(configured_threshold), 0.60)
    if float(bullish_score) >= 80.0:
        return OBVGateDecision(threshold=-1.0, exempt=True)
    if _is_recovery_context(
        early_bullish=early_bullish,
        weak_bull_bias=weak_bull_bias,
        trigger_reason=trigger_reason,
        execution_entry_mode=execution_entry_mode,
    ):
        return OBVGateDecision(threshold=0.0, exempt=False)
    if float(bullish_score) >= 65.0:
        return OBVGateDecision(threshold=0.0, exempt=False)
    return OBVGateDecision(threshold=strict_threshold, exempt=False)


def resolve_dynamic_obv_gate_for_signal(signal: MTFSignal, *, configured_threshold: float) -> OBVGateDecision:
    return resolve_dynamic_obv_gate(
        bullish_score=float(signal.bullish_score),
        configured_threshold=float(configured_threshold),
        early_bullish=bool(signal.early_bullish),
        weak_bull_bias=bool(signal.weak_bull_bias),
        trigger_reason=str(signal.execution_trigger.reason),
        execution_entry_mode=str(signal.execution_entry_mode),
    )
