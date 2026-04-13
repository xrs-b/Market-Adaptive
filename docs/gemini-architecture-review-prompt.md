# Gemini Architecture Review Prompt

You are reviewing a Python quant trading system called **Market-Adaptive**.

Your task is to analyze the current architecture and propose **minimal-invasive, high-value improvements**. Do **not** suggest a full rewrite unless absolutely necessary.

## Current System
The system has these core modules:
- `MarketOracle` — market regime detection
- `CTARobot` — trend-following strategy
- `GridRobot` — sideways/grid strategy
- `RiskControlManager` — global risk / recovery / blocking
- `MainController` — orchestration

## Important Facts About The Current Code

### 1. Multi-timeframe support exists
The system already uses multiple timeframes in parallel:
- `4h` → major trend / major bias
- `1h` → swing readiness / weak bias
- `15m` → execution trigger
- `1m` → impulse detection in MarketOracle

### 2. SuperTrend returns both direction and price bands
The SuperTrend function returns:
- `supertrend`
- `direction`
- `upper_band`
- `lower_band`
- `atr`

So the system already has access to both:
- trend direction
- actual rail/band price levels

### 3. CTA and Grid are coordinated, but there is no full message bus
Both strategies run under the same `MainController` and share:
- market state
- risk control
- runtime/database state
- notifier

But there is **no explicit global state bus** for rich CTA↔Grid communication.

### 4. Grid is dynamic, but still mostly symmetric
Grid currently supports:
- dynamic bounds
- center price
- fee-aware close pricing
- dynamic sizing
- 8 levels
- 40% equity allocation
- min spacing floor 0.8%

But it still uses mostly symmetric layer generation, not explicit asymmetric settings like `6 buy / 4 sell`.

### 5. CTA has already been partially upgraded
CTA is no longer the old strict version.
It now includes:
- early bias / weak bull logic
- dynamic RSI logic
- KDJ memory window
- breakout + OBV confirmation
- aggressive entry path (market / aggressive IOC)
- near-miss report support

### 6. Current observed bottleneck
Recent replay analysis suggests CTA is **not mainly blocked by OBV anymore**.
The dominant blocker is still the **upstream regime / bullish-ready layer**.

## What You Should Analyze
Please answer these questions:

1. Is the current **CTA upstream bias / bullish-ready** logic still too slow?
2. How should `MarketOracle` evolve into a stronger **global direction engine**?
3. What is the best **minimal-invasive CTA↔Grid coordination model** in this architecture?
4. Should Grid support **asymmetric order ladders** or **center-price skew** based on CTA bias?
5. What are the **top 3 improvements** you would implement first, ranked by:
   - impact on performance
   - risk control safety
   - implementation cost

## Output Format
Please structure your answer as:

1. Overall assessment of the system
2. Biggest CTA weakness
3. Biggest Grid weakness
4. CTA improvement suggestions
5. Grid improvement suggestions
6. CTA/Grid coordination suggestions
7. Top 3 recommended changes
8. Exact modules / classes / functions that should be changed first

## Constraints
- Be concrete and technical
- Avoid generic finance theory
- Assume this is a real Python production-ish codebase
- Prefer evolution over rewrite
- Use the existing architecture as much as possible
