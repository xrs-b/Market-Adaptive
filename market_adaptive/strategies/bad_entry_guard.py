from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class BadEntryGuardResult:
    blocked: bool
    reason: str | None = None
    triggers: tuple[str, ...] = field(default_factory=tuple)
    details: dict[str, float | bool | str] = field(default_factory=dict)


class BadEntryGuard:
    """Hard block known bad CTA entries before live order placement."""

    def __init__(self, config: Any) -> None:
        self.config = config

    def evaluate(self, signal: Any) -> BadEntryGuardResult:
        direction = int(getattr(signal, "direction", 0) or 0)
        if direction == 0:
            return BadEntryGuardResult(blocked=True, reason="no_direction", triggers=("no_direction",))

        price = float(getattr(signal, "price", 0.0) or 0.0)
        atr = max(0.0, float(getattr(signal, "atr", 0.0) or 0.0))
        swing_rsi = float(getattr(signal, "swing_rsi", 50.0) or 50.0)
        major_direction = int(getattr(signal, "major_direction", 0) or 0)
        obv_bias = int(getattr(signal, "obv_bias", 0) or 0)
        obv_confirmation_passed = bool(getattr(signal, "obv_confirmation_passed", False))
        volume_filter_passed = bool(getattr(signal, "volume_filter_passed", False))
        trigger_reason = str(getattr(signal, "execution_trigger_reason", "") or "")
        volume_profile = getattr(signal, "volume_profile", None)

        triggers: list[str] = []
        details: dict[str, float | bool | str] = {
            "price": price,
            "atr": atr,
            "swing_rsi": swing_rsi,
            "major_direction": major_direction,
            "obv_bias": obv_bias,
            "obv_confirmation_passed": obv_confirmation_passed,
            "volume_filter_passed": volume_filter_passed,
        }

        if direction > 0:
            if major_direction < 0 and swing_rsi <= float(getattr(self.config, "bad_entry_long_falling_knife_rsi", 38.0)):
                triggers.append("falling_knife")
            if major_direction < 0 and not bool(getattr(signal, "pullback_near_support", False)):
                triggers.append("counter_trend")
            if obv_bias < 0 or not obv_confirmation_passed or not volume_filter_passed:
                triggers.append("opposing_volume")
            if volume_profile is not None and price > 0:
                poc = float(getattr(volume_profile, "poc_price", 0.0) or 0.0)
                vah = float(getattr(volume_profile, "value_area_high", 0.0) or 0.0)
                tolerance = max(float(getattr(self.config, "bad_entry_support_guard_atr_ratio", 0.35)) * atr, price * 0.001)
                details.update({"poc_price": poc, "value_area_high": vah, "resistance_tolerance": tolerance})
                if poc > 0 and price + tolerance < poc:
                    triggers.append("opposing_support_or_volume")
                elif vah > 0 and price + tolerance < vah:
                    triggers.append("opposing_support_or_volume")
        else:
            if major_direction > 0 and swing_rsi >= float(getattr(self.config, "bad_entry_short_falling_knife_rsi", 62.0)):
                triggers.append("falling_knife")
            if major_direction > 0 and "obv_scalp" not in trigger_reason.lower():
                triggers.append("counter_trend")
            if obv_bias > 0 or not obv_confirmation_passed or not volume_filter_passed:
                triggers.append("opposing_volume")
            if volume_profile is not None and price > 0:
                poc = float(getattr(volume_profile, "poc_price", 0.0) or 0.0)
                val = float(getattr(volume_profile, "value_area_low", 0.0) or 0.0)
                tolerance = max(float(getattr(self.config, "bad_entry_support_guard_atr_ratio", 0.35)) * atr, price * 0.001)
                details.update({"poc_price": poc, "value_area_low": val, "support_tolerance": tolerance})
                if poc > 0 and price - tolerance > poc:
                    triggers.append("opposing_support_or_volume")
                elif val > 0 and price - tolerance > val:
                    triggers.append("opposing_support_or_volume")

        ordered = tuple(dict.fromkeys(triggers))
        return BadEntryGuardResult(
            blocked=bool(ordered),
            reason=ordered[0] if ordered else None,
            triggers=ordered,
            details=details,
        )
