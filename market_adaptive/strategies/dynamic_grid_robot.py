"""Legacy compatibility wrapper for the quarantined dynamic grid robot.

The implementation was moved out of the active trading namespace during medium-risk
cleanup, but the legacy import path is kept for existing tests and any residual callers.
"""

from market_adaptive._quarantine_legacy.strategies.dynamic_grid_robot import *  # noqa: F401,F403
