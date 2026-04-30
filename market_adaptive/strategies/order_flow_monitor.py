"""Legacy compatibility wrapper for the experimental order-flow monitor.

The implementation now lives under ``market_adaptive.experimental.order_flow_monitor``
to keep websocket/order-book logic outside the active trading mainline namespace.
"""

from market_adaptive.experimental.order_flow_monitor import *  # noqa: F401,F403
