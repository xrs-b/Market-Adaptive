from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.config import SignalScoringConfig


@dataclass(frozen=True)
class SignalScoreComponent:
    name: str
    weight: float
    passed: bool
    score: float
    detail: str = ""


@dataclass(frozen=True)
class SignalScoreSnapshot:
    total_score: float
    max_score: float
    min_trade_score: float
    high_quality_score: float
    tier: str
    components: tuple[SignalScoreComponent, ...]

    @property
    def trade_allowed(self) -> bool:
        return self.tier != "ignore"

    @property
    def high_quality(self) -> bool:
        return self.tier == "high_quality"

    def component_score(self, name: str) -> float:
        for component in self.components:
            if component.name == name or (name == "obv_slope" and component.name == "obv_signal"):
                return component.score
        return 0.0


def build_signal_score(
    config: SignalScoringConfig,
    *,
    trend_confirmed: bool,
    volume_confirmed: bool,
    timeframe_confirmed: bool,
    order_flow_confirmed: bool,
    obv_signal_confirmed: bool = False,
    obv_slope_confirmed: bool | None = None,
    execution_trigger_confirmed: bool = False,
) -> SignalScoreSnapshot:
    components = (
        SignalScoreComponent(
            name="trend",
            weight=float(config.trend_weight),
            passed=bool(trend_confirmed),
            score=float(config.trend_weight) if trend_confirmed else 0.0,
            detail="ema_fast_above_slow",
        ),
        SignalScoreComponent(
            name="volume",
            weight=float(config.volume_weight),
            passed=bool(volume_confirmed),
            score=float(config.volume_weight) if volume_confirmed else 0.0,
            detail="price_above_poc_and_value_area",
        ),
        SignalScoreComponent(
            name="timeframe_resonance",
            weight=float(config.timeframe_weight),
            passed=bool(timeframe_confirmed),
            score=float(config.timeframe_weight) if timeframe_confirmed else 0.0,
            detail="1h_and_15m_confluence",
        ),
        SignalScoreComponent(
            name="order_flow",
            weight=float(config.order_flow_weight),
            passed=bool(order_flow_confirmed),
            score=float(config.order_flow_weight) if order_flow_confirmed else 0.0,
            detail="buy_side_order_book_dominance",
        ),
        SignalScoreComponent(
            name="obv_signal",
            weight=float(config.obv_slope_weight),
            passed=bool(obv_signal_confirmed if obv_slope_confirmed is None else obv_slope_confirmed),
            score=float(config.obv_slope_weight) if (obv_signal_confirmed if obv_slope_confirmed is None else obv_slope_confirmed) else 0.0,
            detail="obv_zscore_threshold",
        ),
        SignalScoreComponent(
            name="execution_trigger",
            weight=float(config.execution_trigger_weight),
            passed=bool(execution_trigger_confirmed),
            score=float(config.execution_trigger_weight) if execution_trigger_confirmed else 0.0,
            detail="15m_kdj_or_breakout_trigger",
        ),
    )
    total_score = sum(component.score for component in components)
    max_score = sum(component.weight for component in components)

    tier = "ignore"
    if total_score >= float(config.high_quality_score) - 1e-12:
        tier = "high_quality"
    elif total_score >= float(config.min_trade_score) - 1e-12:
        tier = "standard"

    return SignalScoreSnapshot(
        total_score=float(total_score),
        max_score=float(max_score),
        min_trade_score=float(config.min_trade_score),
        high_quality_score=float(config.high_quality_score),
        tier=tier,
        components=components,
    )
