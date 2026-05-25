from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ProfessionalSignalDecision:
    allowed: bool
    action: str | None = None
    reason: str = ""
    setup_family: str = ""
    htf_bias: str = "unknown"
    location_score: float = 0.0
    rr: float = 0.0
    entry: float = 0.0
    stop: float = 0.0
    target: float = 0.0
    nearest_support: float | None = None
    nearest_resistance: float | None = None
    reasons: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


class ProfessionalSignalEngine:
    """Trader-style gate: HTF context -> location -> setup -> RR.

    The legacy MTF engine may produce candidates; this engine decides whether the
    candidate is worth trading from a professional trade-plan perspective.
    """

    def __init__(self, client: Any, config: Any, *, symbol: str) -> None:
        self.client = client
        self.config = config
        self.symbol = symbol

    def evaluate(self, signal: Any, *, side: str) -> ProfessionalSignalDecision:
        position_side = "long" if int(getattr(signal, "direction", 0) or 0) > 0 else "short"
        trigger_family = str(getattr(signal, "execution_trigger_family", "") or "waiting")
        price = float(getattr(signal, "price", 0.0) or 0.0)
        atr = max(float(getattr(signal, "atr", 0.0) or 0.0), price * 0.001 if price > 0 else 0.0)
        if price <= 0 or atr <= 0:
            return self._block("cta:pro_signal_invalid_price", "invalid_price_or_atr", trigger_family=trigger_family)

        frames = self._load_context_frames(signal)
        structure_payload = self._resolve_market_structure(frames)
        bias_payload = self._resolve_htf_bias(frames, structure_payload=structure_payload)
        htf_bias = bias_payload["htf_bias"]
        location = self._resolve_location(frames, price=price, side=position_side, atr=atr)
        setup_family = self._map_setup_family(trigger_family, htf_bias=htf_bias, location=location, side=position_side)
        trade_plan = self._build_trade_plan(
            frames,
            side=position_side,
            price=price,
            atr=atr,
            setup_family=setup_family,
        )
        reasons = [*bias_payload["reasons"], *structure_payload["reasons"], *location["reasons"], f"setup={setup_family}"]

        setup_valid, setup_reason = self._validate_setup(
            setup_family=setup_family,
            side=position_side,
            trigger_family=trigger_family,
            structure_payload=structure_payload,
            frames=frames,
            price=price,
            atr=atr,
            reasons=reasons,
        )
        if setup_family == "unsupported":
            return self._decision(False, "cta:pro_signal_unsupported_setup", "unsupported_setup", setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)
        if not self._bias_allows_side(htf_bias, position_side, setup_family):
            return self._decision(False, "cta:pro_signal_htf_bias_blocked", "htf_bias_blocked", setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)
        if not setup_valid:
            return self._decision(False, "cta:pro_signal_setup_structure_blocked", setup_reason, setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)
        if float(location["score"]) < float(getattr(self.config, "pro_signal_min_location_score", 0.35)):
            return self._decision(False, "cta:pro_signal_bad_location", "bad_location", setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)
        min_rr = float(getattr(self.config, "pro_signal_min_rr", 1.8))
        if float(trade_plan["rr"]) < min_rr:
            return self._decision(False, "cta:pro_signal_rr_blocked", "rr_blocked", setup_family, htf_bias, location, trade_plan, reasons + [f"rr<{min_rr:.2f}"], structure_payload=structure_payload)
        if self._low_volume_block(signal, setup_family):
            return self._decision(False, "cta:pro_signal_low_volume", "low_volume", setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)
        return self._decision(True, None, "allowed", setup_family, htf_bias, location, trade_plan, reasons, structure_payload=structure_payload)

    def _load_context_frames(self, signal: Any) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {
            "4h": getattr(signal, "major_frame", pd.DataFrame()),
            "1h": getattr(signal, "swing_frame", pd.DataFrame()),
            "15m": getattr(signal, "execution_frame", pd.DataFrame()),
        }
        for tf in ("1w", "1d"):
            try:
                ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=tf, limit=120)
                frames[tf] = self._ohlcv_to_frame(ohlcv)
            except Exception:
                frames[tf] = pd.DataFrame()
        return frames

    @staticmethod
    def _ohlcv_to_frame(ohlcv: Any) -> pd.DataFrame:
        if isinstance(ohlcv, pd.DataFrame):
            return ohlcv.copy()
        frame = pd.DataFrame(ohlcv or [], columns=["timestamp", "open", "high", "low", "close", "volume"])
        return frame

    @staticmethod
    def _ema(frame: pd.DataFrame, span: int = 15) -> float | None:
        if frame is None or frame.empty or "close" not in frame or len(frame) < max(3, span):
            return None
        return float(pd.to_numeric(frame["close"], errors="coerce").ewm(span=span, adjust=False).mean().iloc[-1])

    @staticmethod
    def _last(frame: pd.DataFrame, column: str) -> float | None:
        if frame is None or frame.empty or column not in frame:
            return None
        return float(pd.to_numeric(frame[column], errors="coerce").iloc[-1])

    def _resolve_htf_bias(self, frames: dict[str, pd.DataFrame], *, structure_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        states = {}
        reasons = []
        for tf in ("1w", "1d", "4h", "1h"):
            close = self._last(frames.get(tf, pd.DataFrame()), "close")
            ema15 = self._ema(frames.get(tf, pd.DataFrame()), 15)
            if close is None or ema15 is None:
                states[tf] = "unknown"
                reasons.append(f"{tf}:unknown")
            elif close > ema15:
                states[tf] = "bull"
                reasons.append(f"{tf}:close>ema15")
            else:
                states[tf] = "bear"
                reasons.append(f"{tf}:close<ema15")
        h4_structure = (structure_payload or {}).get("4h", {}).get("trend", "unknown")
        if states.get("1w") == states.get("1d") == states.get("4h") == "bull" and h4_structure in {"uptrend", "unknown"}:
            htf_bias = "strong_long"
        elif states.get("1w") == states.get("1d") == states.get("4h") == "bear" and h4_structure in {"downtrend", "unknown"}:
            htf_bias = "strong_short"
        elif states.get("1d") == states.get("4h") == "bull" and h4_structure != "downtrend":
            htf_bias = "long"
        elif states.get("1d") == states.get("4h") == "bear" and h4_structure != "uptrend":
            htf_bias = "short"
        else:
            htf_bias = "mixed"
        if h4_structure in {"uptrend", "downtrend"}:
            reasons.append(f"4h_structure={h4_structure}")
        return {"htf_bias": htf_bias, "states": states, "reasons": reasons}

    def _resolve_market_structure(self, frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
        payload: dict[str, Any] = {"reasons": []}
        for tf in ("1d", "4h", "1h", "15m"):
            structure = self._structure_for_frame(frames.get(tf, pd.DataFrame()))
            payload[tf] = structure
            if structure.get("trend") != "unknown":
                payload["reasons"].append(f"{tf}_structure={structure.get('trend')}")
            if structure.get("bos"):
                payload["reasons"].append(f"{tf}_BOS_{structure.get('bos')}")
            if structure.get("choch"):
                payload["reasons"].append(f"{tf}_CHoCH_{structure.get('choch')}")
        return payload

    @classmethod
    def _structure_for_frame(cls, frame: pd.DataFrame) -> dict[str, Any]:
        if frame is None or frame.empty or len(frame) < 12 or not {"high", "low", "close"}.issubset(frame.columns):
            return {"trend": "unknown", "bos": None, "choch": None, "last_swing_high": None, "last_swing_low": None}
        swings = cls._swing_points(frame, left=2, right=2)
        highs = swings["highs"][-3:]
        lows = swings["lows"][-3:]
        trend = "unknown"
        if len(highs) >= 2 and len(lows) >= 2:
            hh = highs[-1][1] > highs[-2][1]
            hl = lows[-1][1] > lows[-2][1]
            lh = highs[-1][1] < highs[-2][1]
            ll = lows[-1][1] < lows[-2][1]
            if hh and hl:
                trend = "uptrend"
            elif lh and ll:
                trend = "downtrend"
            else:
                trend = "range"
        close = float(pd.to_numeric(frame["close"], errors="coerce").iloc[-1])
        last_high = highs[-1][1] if highs else None
        last_low = lows[-1][1] if lows else None
        bos = None
        choch = None
        if last_high is not None and close > last_high:
            bos = "up"
            if trend == "downtrend":
                choch = "up"
        if last_low is not None and close < last_low:
            bos = "down"
            if trend == "uptrend":
                choch = "down"
        return {"trend": trend, "bos": bos, "choch": choch, "last_swing_high": last_high, "last_swing_low": last_low}

    @staticmethod
    def _swing_points(frame: pd.DataFrame, *, left: int = 2, right: int = 2) -> dict[str, list[tuple[int, float]]]:
        highs = pd.to_numeric(frame["high"], errors="coerce").tolist()
        lows = pd.to_numeric(frame["low"], errors="coerce").tolist()
        swing_highs: list[tuple[int, float]] = []
        swing_lows: list[tuple[int, float]] = []
        for i in range(left, len(frame) - right):
            window_high = highs[i - left : i + right + 1]
            window_low = lows[i - left : i + right + 1]
            if highs[i] == max(window_high) and window_high.count(highs[i]) == 1:
                swing_highs.append((i, float(highs[i])))
            if lows[i] == min(window_low) and window_low.count(lows[i]) == 1:
                swing_lows.append((i, float(lows[i])))
        return {"highs": swing_highs, "lows": swing_lows}

    def _resolve_location(self, frames: dict[str, pd.DataFrame], *, price: float, side: str, atr: float) -> dict[str, Any]:
        h4 = frames.get("4h", pd.DataFrame())
        d1 = frames.get("1d", pd.DataFrame())
        h4_ema15 = self._ema(h4, 15)
        h1_ema21 = self._ema(frames.get("1h", pd.DataFrame()), 21)
        support, resistance = self._nearest_levels([h4, d1], price)
        score = 0.0
        reasons: list[str] = []
        if side == "long":
            if h4_ema15 and price >= h4_ema15:
                score += 0.25; reasons.append("long_above_h4_ema15")
            if h1_ema21 and abs(price - h1_ema21) <= max(atr * 1.2, price * 0.006):
                score += 0.25; reasons.append("near_h1_ema21")
            if support and abs(price - support) <= max(atr * 1.5, price * 0.008):
                score += 0.30; reasons.append("near_htf_support")
            if resistance and resistance > price:
                score += 0.20; reasons.append("room_to_resistance")
        else:
            if h4_ema15 and price <= h4_ema15:
                score += 0.25; reasons.append("short_below_h4_ema15")
            if h1_ema21 and abs(price - h1_ema21) <= max(atr * 1.2, price * 0.006):
                score += 0.25; reasons.append("near_h1_ema21")
            if resistance and abs(price - resistance) <= max(atr * 1.5, price * 0.008):
                score += 0.30; reasons.append("near_htf_resistance")
            if support and support < price:
                score += 0.20; reasons.append("room_to_support")
        return {"score": min(1.0, score), "support": support, "resistance": resistance, "reasons": reasons}

    @staticmethod
    def _nearest_levels(frames: list[pd.DataFrame], price: float) -> tuple[float | None, float | None]:
        highs: list[float] = []
        lows: list[float] = []
        for frame in frames:
            if frame is None or frame.empty:
                continue
            if "high" in frame:
                highs.extend(float(x) for x in pd.to_numeric(frame["high"], errors="coerce").tail(80).dropna().tolist())
            if "low" in frame:
                lows.extend(float(x) for x in pd.to_numeric(frame["low"], errors="coerce").tail(80).dropna().tolist())
        support_candidates = [x for x in lows if x < price]
        resistance_candidates = [x for x in highs if x > price]
        return (max(support_candidates) if support_candidates else None, min(resistance_candidates) if resistance_candidates else None)

    @staticmethod
    def _map_setup_family(trigger_family: str, *, htf_bias: str, location: dict[str, Any], side: str) -> str:
        if trigger_family in {"trend_continuation_near_breakout", "near_breakout_release", "bullish_memory_breakout", "bearish_memory_breakdown"}:
            return "breakout_retest_continuation"
        if trigger_family in {"major_bull_retest", "bullish_retest_entry", "bearish_retest_entry", "pullback_support_entry", "pullback_resistance_entry", "early_bullish", "early_bearish"}:
            return "trend_pullback_reclaim"
        if trigger_family in {"spring_reclaim", "upthrust_reclaim"}:
            return "htf_liquidity_sweep_reversal"
        return "unsupported"

    def _validate_setup(
        self,
        *,
        setup_family: str,
        side: str,
        trigger_family: str,
        structure_payload: dict[str, Any],
        frames: dict[str, pd.DataFrame],
        price: float,
        atr: float,
        reasons: list[str],
    ) -> tuple[bool, str]:
        if setup_family == "unsupported":
            return False, "unsupported_setup"
        if setup_family == "trend_pullback_reclaim":
            return self._validate_trend_pullback(side=side, structure_payload=structure_payload, reasons=reasons)
        if setup_family == "breakout_retest_continuation":
            return self._validate_breakout_retest(side=side, structure_payload=structure_payload, reasons=reasons)
        if setup_family == "htf_liquidity_sweep_reversal":
            return self._validate_htf_sweep(side=side, frames=frames, price=price, atr=atr, trigger_family=trigger_family, reasons=reasons)
        return False, "unknown_setup"

    @staticmethod
    def _validate_trend_pullback(*, side: str, structure_payload: dict[str, Any], reasons: list[str]) -> tuple[bool, str]:
        h4_trend = structure_payload.get("4h", {}).get("trend")
        h1_trend = structure_payload.get("1h", {}).get("trend")
        m15 = structure_payload.get("15m", {})
        if side == "long":
            if h4_trend == "downtrend" or h1_trend == "downtrend":
                return False, "pullback_against_structure"
            if m15.get("choch") not in {"up", None}:
                return False, "pullback_no_15m_bullish_shift"
            reasons.append("pullback_structure_ok_long")
            return True, "pullback_structure_ok"
        if h4_trend == "uptrend" or h1_trend == "uptrend":
            return False, "pullback_against_structure"
        if m15.get("choch") not in {"down", None}:
            return False, "pullback_no_15m_bearish_shift"
        reasons.append("pullback_structure_ok_short")
        return True, "pullback_structure_ok"

    @staticmethod
    def _validate_breakout_retest(*, side: str, structure_payload: dict[str, Any], reasons: list[str]) -> tuple[bool, str]:
        h4_bos = structure_payload.get("4h", {}).get("bos")
        h1_bos = structure_payload.get("1h", {}).get("bos")
        wanted = "up" if side == "long" else "down"
        if h4_bos != wanted and h1_bos != wanted:
            return False, "breakout_without_bos"
        reasons.append(f"breakout_bos_{wanted}")
        return True, "breakout_structure_ok"

    def _validate_htf_sweep(
        self,
        *,
        side: str,
        frames: dict[str, pd.DataFrame],
        price: float,
        atr: float,
        trigger_family: str,
        reasons: list[str],
    ) -> tuple[bool, str]:
        if trigger_family not in {"spring_reclaim", "upthrust_reclaim"}:
            return False, "not_sweep_trigger"
        swept = self._detect_htf_sweep(frames=frames, side=side, price=price, atr=atr)
        if not swept:
            return False, "no_htf_swing_sweep"
        reasons.append("htf_swing_sweep_confirmed")
        return True, "htf_sweep_ok"

    @classmethod
    def _detect_htf_sweep(cls, *, frames: dict[str, pd.DataFrame], side: str, price: float, atr: float) -> bool:
        for tf in ("1d", "4h"):
            frame = frames.get(tf, pd.DataFrame())
            if frame is None or frame.empty or len(frame) < 12:
                continue
            swings = cls._swing_points(frame, left=2, right=2)
            highs = swings["highs"][-5:]
            lows = swings["lows"][-5:]
            last = frame.iloc[-1]
            high = float(last.get("high", price))
            low = float(last.get("low", price))
            close = float(last.get("close", price))
            buffer = max(atr * 0.15, price * 0.001)
            if side == "long":
                for _, level in lows:
                    if low < level - buffer and close > level:
                        return True
            else:
                for _, level in highs:
                    if high > level + buffer and close < level:
                        return True
        return False

    @staticmethod
    def _bias_allows_side(htf_bias: str, side: str, setup_family: str) -> bool:
        if htf_bias in {"strong_long", "long"} and side == "short":
            return setup_family == "htf_liquidity_sweep_reversal" and htf_bias != "strong_long"
        if htf_bias in {"strong_short", "short"} and side == "long":
            return setup_family == "htf_liquidity_sweep_reversal" and htf_bias != "strong_short"
        if htf_bias == "mixed":
            return setup_family in {"htf_liquidity_sweep_reversal", "trend_pullback_reclaim"}
        return True

    def _build_trade_plan(self, frames: dict[str, pd.DataFrame], *, side: str, price: float, atr: float, setup_family: str) -> dict[str, float]:
        support, resistance = self._nearest_levels([frames.get("4h", pd.DataFrame()), frames.get("1d", pd.DataFrame())], price)
        if side == "long":
            stop = min(price - atr * 1.2, support - atr * 0.2 if support else price - atr * 1.5)
            target = resistance if resistance and resistance > price else price + atr * 3.0
            risk = max(price - stop, 1e-9)
            reward = max(target - price, 0.0)
        else:
            stop = max(price + atr * 1.2, resistance + atr * 0.2 if resistance else price + atr * 1.5)
            target = support if support and support < price else price - atr * 3.0
            risk = max(stop - price, 1e-9)
            reward = max(price - target, 0.0)
        return {"entry": price, "stop": stop, "target": target, "rr": reward / risk, "support": support, "resistance": resistance}

    @staticmethod
    def _low_volume_block(signal: Any, setup_family: str) -> bool:
        reason = str(getattr(signal, "execution_trigger_reason", "") or "")
        # Existing trigger reason often includes "vol=0.24x". Treat very low-volume breakouts/sweeps as noise.
        marker = "vol="
        if marker not in reason:
            return False
        try:
            vol = float(reason.split(marker, 1)[1].split("x", 1)[0].strip())
        except Exception:
            return False
        if vol < 0.5:
            return True
        if setup_family == "breakout_retest_continuation" and vol < 0.8:
            return True
        return False

    def _block(self, action: str, reason: str, *, trigger_family: str) -> ProfessionalSignalDecision:
        return ProfessionalSignalDecision(False, action=action, reason=reason, setup_family=trigger_family)

    @staticmethod
    def _decision(allowed: bool, action: str | None, reason: str, setup_family: str, htf_bias: str, location: dict[str, Any], trade_plan: dict[str, float], reasons: list[str], structure_payload: dict[str, Any] | None = None) -> ProfessionalSignalDecision:
        return ProfessionalSignalDecision(
            allowed=allowed,
            action=action,
            reason=reason,
            setup_family=setup_family,
            htf_bias=htf_bias,
            location_score=float(location.get("score", 0.0)),
            rr=float(trade_plan.get("rr", 0.0)),
            entry=float(trade_plan.get("entry", 0.0)),
            stop=float(trade_plan.get("stop", 0.0)),
            target=float(trade_plan.get("target", 0.0)),
            nearest_support=trade_plan.get("support"),
            nearest_resistance=trade_plan.get("resistance"),
            reasons=tuple(reasons),
            metadata={"pro_signal_reason": reason, "pro_signal_reasons": list(reasons), "market_structure": structure_payload or {}},
        )
