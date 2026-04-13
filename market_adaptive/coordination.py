from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from time import time


@dataclass
class CTACoordinationState:
    symbol: str = ""
    side: str | None = None
    size: float = 0.0
    trend_strength: float = 0.0
    strong_trend: bool = False
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
class StrategyRuntimeContext:
    cta: CTACoordinationState = field(default_factory=CTACoordinationState)
    grid: GridCoordinationState = field(default_factory=GridCoordinationState)
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def publish_cta_state(
        self,
        *,
        symbol: str,
        side: str | None,
        size: float,
        trend_strength: float,
        strong_trend: bool,
    ) -> None:
        with self._lock:
            self.cta = CTACoordinationState(
                symbol=symbol,
                side=side,
                size=float(size),
                trend_strength=float(trend_strength),
                strong_trend=bool(strong_trend),
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

    def snapshot_cta(self) -> CTACoordinationState:
        with self._lock:
            return CTACoordinationState(**self.cta.__dict__)

    def snapshot_grid(self) -> GridCoordinationState:
        with self._lock:
            return GridCoordinationState(**self.grid.__dict__)
