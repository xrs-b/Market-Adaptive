from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, RLock
from time import time


@dataclass
class CTACoordinationState:
    symbol: str = ""
    side: str | None = None
    size: float = 0.0
    trend_strength: float = 0.0
    strong_trend: bool = False
    major_direction: int = 0
    bullish_ready: bool = False
    updated_at: float = 0.0


@dataclass
class GridCoordinationState:
    symbol: str = ""
    net_position_size: float = 0.0
    inventory_bias_side: str | None = None
    inventory_bias_ratio: float = 0.0
    heavy_inventory: bool = False
    hedge_assist_requested: bool = False
    hedge_assist_reason: str | None = None
    hedge_assist_target_side: str | None = None
    updated_at: float = 0.0


@dataclass
class MarketCoordinationState:
    symbol: str = ""
    regime: str | None = None
    bias_value: float = 0.0
    updated_at: float = 0.0


@dataclass
class StrategyRuntimeContext:
    cta: CTACoordinationState = field(default_factory=CTACoordinationState)
    grid: GridCoordinationState = field(default_factory=GridCoordinationState)
    market: MarketCoordinationState = field(default_factory=MarketCoordinationState)
    urgent_wakeup: Event = field(default_factory=Event, init=False, repr=False)
    urgent_wakeup_reason: str | None = None
    urgent_wakeup_requested_at: float = 0.0
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def publish_cta_state(
        self,
        *,
        symbol: str,
        side: str | None,
        size: float,
        trend_strength: float,
        strong_trend: bool,
        major_direction: int = 0,
        bullish_ready: bool = False,
    ) -> None:
        with self._lock:
            self.cta = CTACoordinationState(
                symbol=symbol,
                side=side,
                size=float(size),
                trend_strength=float(trend_strength),
                strong_trend=bool(strong_trend),
                major_direction=int(major_direction),
                bullish_ready=bool(bullish_ready),
                updated_at=time(),
            )

    def publish_grid_inventory(
        self,
        *,
        symbol: str,
        net_position_size: float,
        inventory_bias_side: str | None,
        inventory_bias_ratio: float,
        heavy_inventory: bool,
        hedge_assist_requested: bool,
        hedge_assist_reason: str | None,
        hedge_assist_target_side: str | None,
    ) -> None:
        with self._lock:
            self.grid = GridCoordinationState(
                symbol=symbol,
                net_position_size=float(net_position_size),
                inventory_bias_side=inventory_bias_side,
                inventory_bias_ratio=float(inventory_bias_ratio),
                heavy_inventory=bool(heavy_inventory),
                hedge_assist_requested=bool(hedge_assist_requested),
                hedge_assist_reason=hedge_assist_reason,
                hedge_assist_target_side=hedge_assist_target_side,
                updated_at=time(),
            )

    def publish_market_state(
        self,
        *,
        symbol: str,
        regime: str | None,
        bias_value: float,
    ) -> None:
        with self._lock:
            self.market = MarketCoordinationState(
                symbol=symbol,
                regime=regime,
                bias_value=float(bias_value),
                updated_at=time(),
            )

    def request_urgent_wakeup(self, reason: str) -> None:
        with self._lock:
            self.urgent_wakeup_reason = str(reason)
            self.urgent_wakeup_requested_at = time()
            self.urgent_wakeup.set()

    def clear_urgent_wakeup(self) -> None:
        self.urgent_wakeup.clear()

    def snapshot_cta(self) -> CTACoordinationState:
        with self._lock:
            return CTACoordinationState(**self.cta.__dict__)

    def snapshot_grid(self) -> GridCoordinationState:
        with self._lock:
            return GridCoordinationState(**self.grid.__dict__)

    def snapshot_market(self) -> MarketCoordinationState:
        with self._lock:
            return MarketCoordinationState(**self.market.__dict__)
