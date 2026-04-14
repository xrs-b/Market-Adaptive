# Intrabar replay findings (2026-04-14)

## What was added
- `market_adaptive/strategies/intrabar_replay.py`
  - builds a partial live-like 15m execution candle from 1m data
  - replays the MTF engine minute-by-minute inside a target 15m bar
- `tests/test_intrabar_replay.py`
  - verifies partial-candle aggregation
  - verifies intrabar scan walks each 1m step in the target execution bar

## Evidence run
- Test command:
  - `PYTHONPATH=. .venv/bin/pytest tests/test_intrabar_replay.py tests/test_trade_replay.py -q`
- Result:
  - `3 passed`

## Replay result on current market window
Using current config and OKX sandbox candles:
- 24h intrabar-vs-close comparison found **20 bars where a full 15m close says PASSED but no minute inside that same bar produced a PASSED signal yet**.
- Representative examples:
  - `2026-04-13 20:00 UTC` close: `PASSED` / `Triggered via Memory Window: KDJ crossed 2 bars ago + Price Breakout NOW`
  - `2026-04-14 02:00 UTC` close: `PASSED` / `Triggered via Memory Window: KDJ crossed 0 bars ago + Price Breakout NOW`
  - `2026-04-14 02:45 UTC` close: `PASSED` / `major_bull_retest_ready: gap=0.113% + KDJ memory 3 bars ago`

Interpretation:
- The strategy currently still looks **bar-close dependent** on many bullish passes.
- Minute-level live replay did **not** prove a clean earlier intra-bar open for those bars.

## +265.94 winner status
**Still unresolved.**

Why unresolved:
1. The sandbox/exposed history in this repo did not give a trustworthy direct handle to the specific `+265.94` trade timestamp/order id.
2. The new intrabar replay can now test a target 15m bar faithfully with 1m sequencing, but without the exact winner bar/time we still cannot conclusively map it to that specific trade.
3. In the bars that currently pass on 15m close, the minute-by-minute replay often does **not** pass before the full bar is finished, which weakens the case that the winner was truly available live under the current rules.

## Current no-trade reasons
- signal only becomes `PASSED` at full 15m close, not during minute-by-minute replay inside the bar
- trigger remains dependent on finalized breakout / memory state at bar completion
- exact `+265.94` reference trade timestamp is missing from accessible artifacts

## Current strategy pain points
- execution logic is still highly sensitive to finalized 15m candle shape
- execution timestamp semantics are ambiguous (bucket-open timestamp vs live evaluation instant)
- replay validation lacks a canonical trade ledger / order id / open timestamp for the target winner

## Remaining unknowns for external model brainstorming
1. What is the exact open timestamp and side of the `+265.94` winner?
2. Was that trade opened on first breakout touch, near-breakout hold, or only after the 15m candle completed?
3. Should live execution semantics treat a forming 15m candle as tradable for KDJ/memory/breakout checks, or should some checks stay close-confirmed only?
4. Would a hybrid trigger (1m breakout + 15m context frozen from prior closed bars) reproduce the winner better than the current partial-15m approach?
