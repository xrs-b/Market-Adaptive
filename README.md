# Market-Adaptive

[English](./README.md) | [简体中文](./README.zh-CN.md)

Market-Adaptive is a modular trading system for **OKX-first crypto execution**, built around:

- **CTA trend trading**
- **Grid / range trading**
- **account-level risk control**
- **main-controller orchestration**
- **replay, diagnostics, and audit tooling**
- **admin API + web dashboard**

Rather than a single “strategy script,” this repository is structured as a **full trading runtime**: observable, tunable, replayable, and designed for iterative improvement.

---

## Overview

Market-Adaptive brings multiple layers together into one system:

- detects market regime (`trend`, `sideways`, `trend_impulse`, etc.)
- routes execution logic between CTA and Grid components
- enforces shared account-level risk limits
- records strategy decisions and blocker reasons for later review
- supports backtesting, replay, family-level attribution, and tuning

In practice, this repo is best viewed as a **trading engine plus diagnostics platform**, not just a collection of indicators.

---

## Core components

### CTA engine
A trend-following execution stack with layered signal evaluation:

- multi-timeframe structure (4H / 1H / 15m)
- signal quality and confidence scoring
- entry-decider and location scoring
- OBV gating
- volume profile / POC / value area context
- order-flow and trigger-family diagnostics
- optional ML gate

Main files:
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/entry_decider_lite.py`
- `market_adaptive/strategies/obv_gate.py`
- `market_adaptive/strategies/order_flow_sentinel.py`
- `market_adaptive/strategies/signal_profiler.py`
- `market_adaptive/cta_dashboard.py`

### Grid engine
Range-oriented execution with adaptive grid behavior.

Main files:
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/strategies/dynamic_grid_robot.py`

### Risk layer
Shared account protection across strategy modules.

Main files:
- `market_adaptive/risk.py`

### Main controller
Coordinates market oracle, CTA, Grid, risk, recovery, and system heartbeat workers.

Main files:
- `market_adaptive/controller.py`
- `scripts/run_main_controller.py`
- `scripts/restart_main_controller.sh`

### Admin stack
Operational visibility and configuration management:
- `admin-api/`
- `admin-web/`

---

## Repository layout

```text
.
├── admin-api/
├── admin-web/
├── config/
├── docs/
├── market_adaptive/
├── scripts/
├── tests/
└── README.md
```

Key runtime areas:
- `market_adaptive/` → source code
- `config/` → local configuration
- `logs/` and `data/` → runtime output
- `.venv/` → local virtual environment

---

## Quick start

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Prepare configuration

```bash
cp config/config.yaml.example config/config.yaml
```

### 3. Initialize storage

```bash
python scripts/init_app.py --config config/config.yaml
```

### 4. Run the main controller

```bash
python scripts/run_main_controller.py --config config/config.yaml
```

Preferred restart path:

```bash
./scripts/restart_main_controller.sh config/config.yaml
```

---

## Useful scripts

### Runtime
```bash
python scripts/run_main_controller.py --config config/config.yaml
python scripts/run_market_oracle.py --config config/config.yaml --once
python scripts/run_the_hands.py --config config/config.yaml
```

### Backtest / replay / validation
```bash
python scripts/cta_backtest_sandbox.py
python scripts/run_cta_backtest_segmented.py
python scripts/cta_multiwindow_validator.py
python scripts/cta_trade_quality_report.py
python scripts/cta_family_report.py
python scripts/replay_single_trade_timestamp.py
python scripts/review_trade_journal.py
```

### ML-related
```bash
python scripts/train_ml_signal_model.py
python scripts/run_ml_replay_compare.py
```

---

## Testing

Run the full suite:

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=$(pwd) \
.venv/bin/pytest -q tests
```

Examples:

```bash
pytest -q tests/test_cta_entry_quality.py
pytest -q tests/test_cta_entry_decider_and_guard.py
pytest -q tests/test_cta_quality.py
pytest -q tests/test_ws_runtime.py
```

---

## Current focus

The infrastructure is already fairly mature.

The main active work is **CTA execution quality**, especially:
- letting strong short candidates become tradable
- reducing false blocks in the final gating chain
- improving signal quality without opening the door to fee-heavy low-quality trades

In other words, the goal is not simply to add more rules, but to:

> help genuinely high-quality candidates survive the decision chain and prove themselves through replay and live diagnostics.

---

## Best suited for

This repository is a good fit if you want:
- a local OKX-oriented trading runtime
- multi-strategy coordination instead of one-off scripts
- replayable decision logs
- strategy-family diagnostics and blocker visibility
- a codebase you can iterate on at the execution-logic level

It is **not** a plug-and-play retail bot with guaranteed profitability.

---

## Notes

- The project is evolving quickly.
- CTA logic is under active tuning.
- Logs, replay tools, and dashboard signals are a core part of using it effectively.

For operational details, see:
- `docs/MAINTENANCE_AI.md`
- `docs/MAINTENANCE_HUMAN.md`
- `docs/market-adaptive-7day-validation.md`
