from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EntryDecisionLiteResult:
    decision: str
    score: float
    reasons: tuple[str, ...] = field(default_factory=tuple)
    breakdown: dict[str, float] = field(default_factory=dict)

    @property
    def allowed(self) -> bool:
        return self.decision == "allow"


class EntryDeciderLite:
    """Small CTA-focused gate that maps raw signal quality into allow/watch/block."""

    def __init__(self, config: Any) -> None:
        self.config = config

    def _market_regime_coefficient(self, signal: Any, direction: int) -> float:
        if not bool(getattr(self.config, "market_regime_adaptation_enabled", True)):
            return 1.0
        regime = str(getattr(signal, "market_regime", getattr(signal, "regime", "unknown")) or "unknown")
        family = str(getattr(signal, "execution_trigger_family", "") or "")
        major_direction = int(getattr(signal, "major_direction", 0) or 0)
        coef = 1.0
        if regime in {"trend", "trend_impulse"}:
            if major_direction != 0 and int(direction) == major_direction:
                coef *= float(getattr(self.config, "entry_decider_trend_follow_regime_coefficient", 1.06))
            elif major_direction != 0:
                coef *= float(getattr(self.config, "entry_decider_countertrend_regime_coefficient", 0.92))
        elif regime == "sideways":
            reversal_families = {"spring_reclaim", "upthrust_reclaim", "bullish_retest_entry", "bearish_retest_entry", "pullback_support_entry", "pullback_resistance_entry"}
            breakout_families = {"bullish_memory_breakout", "bearish_memory_breakdown", "starter_frontrun", "starter_short_frontrun", "trend_continuation_near_breakout", "near_breakout_release", "price_led_override"}
            if family in reversal_families:
                coef *= float(getattr(self.config, "entry_decider_sideways_reversal_regime_coefficient", 1.05))
            elif family in breakout_families:
                coef *= float(getattr(self.config, "entry_decider_sideways_breakout_regime_coefficient", 0.94))
        return max(0.80, min(1.20, float(coef)))

    def evaluate(self, signal: Any) -> EntryDecisionLiteResult:
        direction = int(getattr(signal, "direction", 0) or 0)
        if direction == 0:
            return EntryDecisionLiteResult(
                decision="block",
                score=0.0,
                reasons=("no_direction",),
                breakdown={"base": 0.0},
            )

        directional_score = float(
            getattr(signal, "bullish_score", 0.0) if direction > 0 else getattr(signal, "bearish_score", 0.0)
        )
        opposing_score = float(
            getattr(signal, "bearish_score", 0.0) if direction > 0 else getattr(signal, "bullish_score", 0.0)
        )
        major_direction = int(getattr(signal, "major_direction", 0) or 0)
        entry_pathway = str(getattr(getattr(signal, "entry_pathway", None), "name", getattr(signal, "entry_pathway", "STRICT")))
        signal_confidence = float(getattr(signal, "signal_confidence", 0.0) or 0.0)
        signal_quality_tier = str(getattr(getattr(signal, "signal_quality_tier", None), "name", getattr(signal, "signal_quality_tier", "TIER_LOW")))
        strength_bonus = float(getattr(signal, "signal_strength_bonus", 0.0) or 0.0)
        obv_passed = bool(getattr(signal, "obv_confirmation_passed", False))
        volume_passed = bool(getattr(signal, "volume_filter_passed", False))
        aligned = bool(getattr(signal, "mtf_aligned", False))
        relaxed_entry = bool(getattr(signal, "relaxed_entry", False))
        ml_used = bool(getattr(signal, "ml_model_used", False))
        ml_gate_passed = bool(getattr(signal, "ml_gate_passed", True))
        ml_aligned_confidence = float(getattr(signal, "ml_aligned_confidence", 0.5) or 0.5)
        trigger_reason = str(getattr(signal, "execution_trigger_reason", "") or "")

        reasons: list[str] = []
        breakdown = {
            "directional_score": directional_score,
            "confidence": signal_confidence * 100.0,
            "strength_bonus": strength_bonus,
            "ml_aligned_confidence": ml_aligned_confidence * 100.0,
        }

        score = directional_score
        score += min(8.0, max(0.0, strength_bonus))
        score += min(10.0, signal_confidence * 12.0)
        score += 6.0 if aligned else -8.0
        score += 4.0 if obv_passed else -10.0
        score += 4.0 if volume_passed else -8.0

        if entry_pathway == "FAST_TRACK":
            score += 8.0
        elif entry_pathway == "STANDARD":
            score += 3.0
        else:
            score -= 4.0

        if major_direction != 0 and major_direction == direction:
            score += 8.0
        elif major_direction != 0:
            countertrend_penalty = float(getattr(self.config, "entry_decider_countertrend_penalty", 8.0))
            score -= countertrend_penalty
            if signal_quality_tier == "TIER_HIGH" or signal_confidence >= 0.75:
                score += float(getattr(self.config, "entry_decider_countertrend_high_quality_bonus", 4.0))
                reasons.append("counter_trend_high_quality")
            elif signal_quality_tier == "TIER_MEDIUM" or signal_confidence >= 0.60:
                score += float(getattr(self.config, "entry_decider_countertrend_medium_quality_bonus", 1.0))
                reasons.append("counter_trend_medium_quality")
            else:
                reasons.append("counter_major_trend")

        conflict_gap = directional_score - opposing_score
        breakdown["conflict_gap"] = conflict_gap
        if conflict_gap < float(getattr(self.config, "entry_decider_conflict_gap_watch", 6.0)):
            score -= 10.0
            reasons.append("score_conflict")
        elif conflict_gap < float(getattr(self.config, "entry_decider_conflict_gap_allow", 12.0)):
            score -= 4.0
            reasons.append("weak_edge")

        if relaxed_entry:
            # relaxed_entry is not a disqualifying reason - it's a valid signal path
            pass

        if "memory" in trigger_reason.lower() or "waiting" in trigger_reason.lower():
            score -= 2.0

        if ml_used:
            if ml_gate_passed:
                score += min(6.0, max(0.0, (ml_aligned_confidence - 0.5) * 20.0))
            else:
                score -= 15.0
                reasons.append("ml_gate_failed")

        regime_coefficient = self._market_regime_coefficient(signal, direction)
        breakdown["market_regime_coefficient"] = regime_coefficient
        if regime_coefficient != 1.0:
            score *= regime_coefficient
            reasons.append(f"regime_coef={regime_coefficient:.2f}")

        score = max(0.0, min(100.0, score))

        block_max = float(getattr(self.config, "entry_decider_block_max_score", 45.0))
        allow_min = float(getattr(self.config, "entry_decider_allow_min_score", 72.0))
        watch_min = float(getattr(self.config, "entry_decider_watch_min_score", 58.0))

        if score <= block_max:
            decision = "block"
        elif score >= allow_min and not reasons:
            decision = "allow"
        elif score >= watch_min:
            decision = "watch"
        else:
            decision = "block"

        if decision != "allow" and not reasons:
            reasons.append("entry_quality_not_clean")

        return EntryDecisionLiteResult(
            decision=decision,
            score=score,
            reasons=tuple(dict.fromkeys(reasons)),
            breakdown=breakdown,
        )
