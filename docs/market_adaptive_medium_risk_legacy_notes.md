# market_adaptive medium-risk legacy/residual notes

This note records the current status of several medium-risk modules that looked legacy-ish,
but are not all safe to delete.

Checked on 2026-04-30.

## Checked targets

- `market_adaptive/strategies/coordinator.py`
- `market_adaptive/strategies/intrabar_replay.py`
- `market_adaptive/cta_quality.py`
- `market_adaptive/ws_runtime.py`
- `market_adaptive/strategies/order_flow_monitor.py`
- `market_adaptive/strategies/entry_decider_lite.py`
- `market_adaptive/strategies/signal_scoring.py`
- `market_adaptive/strategies/bad_entry_guard.py`

## Findings

### `strategies/coordinator.py`
Still used by:
- `scripts/run_the_hands.py`
- `tests/test_the_hands.py`
- exported from `market_adaptive/strategies/__init__.py`

Action:
- kept in place
- marked as a legacy compatibility wrapper in module docstring

### `strategies/intrabar_replay.py`
Not dead code.
Used by:
- `tests/test_intrabar_replay.py`
- `scripts/run_ml_replay_compare.py`
- `scripts/test_permit_replay_ml_gate.py`

Action:
- left untouched

### `cta_quality.py`
Not dead code.
Used by:
- `tests/test_cta_quality.py`

Action:
- left untouched

### `ws_runtime.py`
Not currently on the obvious main bootstrap path, but it is maintained by:
- `tests/test_ws_runtime.py`
- maintenance docs (`docs/MAINTENANCE_HUMAN.md`, `docs/MAINTENANCE_AI.md`)
- internal coupling to `strategies/order_flow_monitor.py`

Action:
- kept compatibility import path in place
- implementation also exposed from `market_adaptive.experimental.ws_runtime`
- marked as isolated/experimental in module docstring

### `strategies/order_flow_monitor.py`
Not dead code.
Used by:
- `ws_runtime.py`
- `tests/test_order_flow_monitor.py`

Action:
- implementation moved to `market_adaptive.experimental.order_flow_monitor`
- `strategies/order_flow_monitor.py` is now a thin compatibility wrapper
- both paths are fully functional; tests pass

### `strategies/entry_decider_lite.py`
Active helper.
Used by:
- `tests/test_cta_entry_decider_and_guard.py`
- `scripts/run_ml_replay_compare.py`
- `scripts/analyze_trade_opportunities.py`
- `scripts/test_permit_replay_ml_gate.py`

Action:
- left untouched

### `strategies/signal_scoring.py`
Active helper.
Used by:
- `tests/test_signal_scoring.py`

Action:
- left untouched

### `strategies/bad_entry_guard.py`
Still exercised by:
- `tests/test_cta_entry_decider_and_guard.py`

Action:
- left untouched

## Safe next-step candidates

If a future cleanup wants to go further, do it in this order:

1. Migrate/remove `scripts/run_the_hands.py`, then retire `strategies/coordinator.py` and its re-export.
2. Confirm whether websocket runtime is intended for production. If not, move `ws_runtime.py` + `order_flow_monitor.py` under an explicit `experimental` or `legacy` namespace with compatibility imports.
3. Only remove replay/quality/entry-guard helpers after scripts/tests are migrated or deleted.
