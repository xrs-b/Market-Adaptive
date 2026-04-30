"""Legacy hands coordinator kept for run_the_hands and compatibility tests.

Not on the newer main CTA/Grid bootstrapping path; keep as a thin wrapper until
callers are migrated or removed.
"""

from __future__ import annotations

from dataclasses import dataclass

from market_adaptive.strategies.base import StrategyRunResult
from market_adaptive.strategies.cta_robot import CTARobot
from market_adaptive.strategies.grid_robot import GridRobot


@dataclass
class HandsRunSummary:
    cta: StrategyRunResult
    grid: StrategyRunResult


class HandsCoordinator:
    def __init__(self, cta_robot: CTARobot, grid_robot: GridRobot) -> None:
        self.cta_robot = cta_robot
        self.grid_robot = grid_robot

    def run_once(self) -> HandsRunSummary:
        cta_result = self.cta_robot.run()
        grid_result = self.grid_robot.run()
        return HandsRunSummary(cta=cta_result, grid=grid_result)
