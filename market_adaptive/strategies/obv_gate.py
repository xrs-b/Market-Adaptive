from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.strategies.mtf_engine import MTFSignal


@dataclass(frozen=True)
class OBVGateDecision:
    threshold: float
    exempt: bool

    def passed(self, snapshot: OBVConfirmationSnapshot) -> bool:
        if self.exempt:
            return True
        return bool(snapshot.buy_confirmed(zscore_threshold=self.threshold))


def resolve_dynamic_obv_gate(*, bullish_score: float, configured_threshold: float) -> OBVGateDecision:
    strict_threshold = min(float(configured_threshold), 0.60)
    if float(bullish_score) >= 80.0:
        return OBVGateDecision(threshold=-1.0, exempt=True)
    if float(bullish_score) >= 65.0:
        return OBVGateDecision(threshold=0.0, exempt=False)
    return OBVGateDecision(threshold=strict_threshold, exempt=False)


def resolve_dynamic_obv_gate_for_signal(signal: MTFSignal, *, configured_threshold: float) -> OBVGateDecision:
    return resolve_dynamic_obv_gate(
        bullish_score=float(signal.bullish_score),
        configured_threshold=float(configured_threshold),
    )
