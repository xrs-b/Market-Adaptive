# Market-Adaptive

Market-Adaptive 是一个面向 OKX 模拟盘的模块化量化交易系统骨架。

## 当前已完成
- YAML 全局配置加载
- OKX 模拟盘连接配置（含 `x-simulated-id` 与 `x-simulated-trading` 请求头）
- SQLite 初始化模块
- `market_status` / `strategy_runtime_state` / `system_state` 表
- 可复用的 `OKXClient`
- `Market-Oracle` 行情感知机器人
- `CTARobot` / `GridRobot` 双核心策略机器人
- `SentimentAnalyst` 情绪模块（接入 OKX `long_short_accounts_ratio`）
- `SuperTrend` / `ATR` / `OBV` / 24h Volume Profile 指标管线与可测试策略状态机
- `MainController` 总控与 `RiskControlManager` 风控模块
- Discord 通知模块

## 目录结构

```text
Market-Adaptive/
├── config/
│   └── config.yaml.example
├── market_adaptive/
│   ├── clients/
│   │   └── okx_client.py
│   ├── notifiers/
│   │   └── discord_notifier.py
│   ├── oracles/
│   │   └── market_oracle.py
│   ├── strategies/
│   │   ├── cta_robot.py
│   │   ├── grid_robot.py
│   │   └── coordinator.py
│   ├── bootstrap.py
│   ├── config.py
│   ├── controller.py
│   ├── db.py
│   ├── indicators.py
│   ├── logging_utils.py
│   └── sentiment.py
├── scripts/
│   ├── init_app.py
│   ├── run_main_controller.py
│   ├── run_market_oracle.py
│   └── run_the_hands.py
└── requirements.txt
```

## 快速开始

> macOS（尤其 Apple Silicon / M4）建议使用官网安装的 Python 3.11+，不要依赖系统自带 Python。

```bash
cd Market-Adaptive
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/config.yaml.example config/config.yaml
python3 scripts/init_app.py --config config/config.yaml
python3 scripts/run_market_oracle.py --config config/config.yaml --once
```

持续运行行情感知机器人：

```bash
python3 scripts/run_market_oracle.py --config config/config.yaml
```

运行双核心策略机器人：

```bash
python3 scripts/run_the_hands.py --config config/config.yaml
```

运行主程序总控模块：

```bash
python3 scripts/run_main_controller.py --config config/config.yaml
```

## CTA 策略关键配置

```yaml
risk_control:
  daily_loss_cutoff_pct: 0.05
  max_margin_ratio: 0.60
  recovery_check_interval_seconds: 60
  default_symbol_max_notional: 0
  symbol_notional_limits:
    BTC/USDT: 1500
  cta_single_trade_equity_multiple: 1.5
  max_directional_leverage: 8.0
  grid_margin_ratio_warning: 0.45
  grid_deviation_reduce_ratio: 0.25
  grid_liquidation_warning_ratio: 0.10
  grid_reduction_step_pct: 0.25
  grid_reduction_cooldown_seconds: 300

sentiment:
  enabled: true
  symbol: "BTC/USDT"
  timeframe: "5m"
  lookback_limit: 1
  extreme_bullish_ratio: 2.5
  cta_buy_action: "block"  # 或 "halve"

cta:
  symbol: "BTC/USDT"
  major_timeframe: "4h"
  swing_timeframe: "1h"
  execution_timeframe: "15m"
  # 兼容旧配置：execution_timeframe = lower_timeframe，swing_timeframe = higher_timeframe
  lower_timeframe: "15m"
  higher_timeframe: "1h"
  lookback_limit: 200
  supertrend_period: 10
  supertrend_multiplier: 3.0
  swing_rsi_period: 14
  swing_rsi_ready_threshold: 50.0
  kdj_length: 9
  kdj_k_smoothing: 3
  kdj_d_smoothing: 3
  execution_breakout_lookback: 3
  obv_signal_period: 8
  obv_slope_window: 8
  obv_slope_threshold_degrees: 30.0
  atr_period: 14
  atr_trailing_multiplier: 2.5
  stop_loss_atr: 2.0
  risk_percent_per_trade: 0.02
  boosted_risk_percent_per_trade: 0.03
  first_take_profit_pct: 0.02
  first_take_profit_size: 0.50
  second_take_profit_pct: 0.05
  second_take_profit_size: 0.25
  volume_profile_lookback_hours: 24
  volume_profile_bin_count: 24
  volume_profile_value_area_pct: 0.70

grid:
  symbol: "BTC/USDT"
  bollinger_timeframe: "1h"
  lookback_limit: 120
  bollinger_length: 20
  bollinger_std: 2.0
  levels: 10
  leverage: 3  # 可选: 3 / 5
  martingale_factor: 1.25
  layer_trigger_window_seconds: 300
  layer_trigger_limit: 3
  layer_cooldown_seconds: 300
  rebalance_exposure_threshold: 2.0
  max_rebalance_orders: 2
  price_band_ratio: 0.03
  liquidation_protection_ratio: 0.05
```

> Grid 保证金模式沿用 `execution.td_mode`，可配置为 `isolated` 或 `cross`。

> 兼容旧配置中的 `timeframe` / `lower_timeframe` / `higher_timeframe` / `fast_ema` / `slow_ema` 字段；新版本 CTA 已升级为 `4h SuperTrend + 1h RSI + 15m KDJ/Breakout` 的 MTF Engine，并继续串接 `OBV + ATR + Volume Profile + Sentiment + Hybrid Shield`。

## Discord 通知配置

沿用旧项目配置键名：`notification.discord.channel_id / webhook_url / bot_token`

```yaml
notification:
  discord:
    enabled: true
    channel_id: "your_channel_id"
    webhook_url: ""
    bot_token: "your_bot_token"
    username: "Market-Adaptive"
```

- 若配置 `webhook_url`，优先走 webhook
- 否则走 `bot_token + channel_id`
- 默认通知 4 类事件：
  - 系统启动 / 停止
  - 行情状态切换
  - CTA / Grid 开仓、清场
  - 风控触发

## 规则摘要

### 行情判定
- 任一周期 `ADX > 25` 且布林带带宽较上一根 K 线放大 => `trend`
- 两个周期 `ADX < 20` => `sideways`
- 中间模糊区间则沿用上一条数据库状态；若无历史，则默认 `sideways`

### 策略规则
- `CTARobot` 只在 `trend` 激活，并改为走三周期 `MTF Engine`：`4h` 用 `SuperTrend` 识别 major trend，`1h` 用 `RSI` 判定 swing readiness，`15m` 只负责 execution trigger（`KDJ` 金叉或突破最近 `execution_breakout_lookback` 根 K 线前高）
- 当 `4h SuperTrend` 看多且 `1h RSI > swing_rsi_ready_threshold`（默认 `50`）时，CTA 进入 `bullish ready`；只有 `15m` 执行触发成立后，才允许进入后续多头过滤链路
- 即使 MTF 已 ready / trigger，多头仍必须继续通过现有 CTA stack：`OBV` 信号线偏多、`OBV` 归一化斜率角度高于 `obv_slope_threshold_degrees`（默认约等于 `30°`），以及基于最近约 24 小时 intraday OHLCV 构造的 `Volume Profile` breakout 条件（突破 `POC` 并站上 `Value Area High`）；若价格仍在 value area 内或低于 POC，则直接跳过开仓
- 当 `4h`、`1h` 与 `15m` 三层信号 fully aligned 时，CTA 会把单笔风险从基线 `risk_percent_per_trade`（默认 `2%`）提升到 `boosted_risk_percent_per_trade`（默认 `3%`），再交给 `RiskControlManager` / Hybrid Shield 做最终仓位约束
- CTA 多头开仓前会额外读取 OKX 公共 `long_short_accounts_ratio`；当零售多头情绪比值大于 `2.5` 时，默认阻止买入信号（也可配置为把多头仓位减半）
- CTA 持仓使用 `ATR` 动态止损距离作为主风控参数：开仓与后续上移/下移止损均以 `stop_loss_atr` 为准；默认在 `+2%` 先止盈 `50%`，`+5%` 再止盈 `25%`；`RiskControlManager` 还会通过快速风控 worker 高频盯防 ATR 硬止损，一旦击穿立即对剩余仓位执行 all-out 全部退出
- CTA 单笔开仓 notional 可用 `cta_single_trade_equity_multiple` 约束为账户权益倍数上限，避免趋势单笔过重
- `GridRobot` 只在 `sideways` 激活，先显式向 OKX 同步保证金模式（`execution.td_mode`）与 3x / 5x 杠杆，然后按当前价上下各 3% 的中性区间重建双边网格限价单；在同一方向持仓过重时会补充 reduce-only 再平衡单
- Grid 由 `RiskControlManager` 接管分层风险治理：当价格跌破下沿或突破上沿时会进入 observe 模式并暂停新增网格开仓，直到价格回到原带宽或市场状态切换；当账户风险率、边界偏离度或最近强平价安全缓冲（默认 10%）触发阈值时，会优先修剪最危险 / 最远离现价的网格敞口，再按 `grid_reduction_step_pct` 逐步减仓
- 若数据库状态发生切换，`RiskControlManager` 会协调旧策略清理；Grid 会先撤销挂单，再对盈利持仓优先挂 reduce-only limit，其他仓位直接市价退出，为下一只机器人腾出保证金

### 总控与风控
- `MainController` 使用 `threading` 并发启动 `MarketOracle`、`CTARobot`、`GridRobot`，以及可配置的 CTA 快速风控 worker（`runtime.fast_risk_check_interval_seconds`）
- `RiskControlManager` 每分钟检查一次账户日内起始权益、总浮盈亏、维护保证金 / 风险率，并执行仓位恢复检查；CTA 硬止损则走高频 fast-check，避免只靠分钟级轮询
- CTA 开仓使用 `calculate_position_size(symbol, risk_percent, stop_loss_atr)`，基于账户权益、ATR 动态止损距离、单笔权益倍数上限与 OKX 合约面值动态换算下单张数
- `RiskControlManager` 会汇总 CTA + Grid 的净多 / 净空 notional；若同方向投影杠杆超过 `max_directional_leverage`（默认 8x），会拒绝继续加同向仓
- 当日内回撤超过 `5%` 时，会立即撤单、强平、写入数据库 `system_status=OFF`、停止机器人并触发通知
- Grid 风控会先用 observe 模式阻止新增挂单；当账户风险率达到 `max_margin_ratio`、价格偏离网格边界超过 `grid_deviation_reduce_ratio`，或当前价距离最近强平价小于 `grid_liquidation_warning_ratio` 时，会按 `grid_reduction_step_pct` 逐步减仓并通过通知回报
- 日志支持颜色区分、CPU 占用率输出、Ctrl/Cmd+C 优雅退出与 shutdown checkpoint 落库

## 可复用入口
- `market_adaptive.config.load_config`
- `market_adaptive.db.DatabaseInitializer`
- `market_adaptive.clients.OKXClient`
- `market_adaptive.notifiers.DiscordNotifier`
- `market_adaptive.oracles.MarketOracle`
- `market_adaptive.sentiment.SentimentAnalyst`
- `market_adaptive.strategies.CTARobot`
- `market_adaptive.strategies.GridRobot`
- `market_adaptive.strategies.HandsCoordinator`
- `market_adaptive.bootstrap.MarketAdaptiveBootstrap`
