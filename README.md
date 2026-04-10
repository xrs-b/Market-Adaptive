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
- `SuperTrend` / `ATR` / `OBV` 指标管线与可测试策略状态机
- `MainController` 总控与风控模块
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
│   └── logging_utils.py
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
cta:
  symbol: "BTC/USDT"
  lower_timeframe: "15m"
  higher_timeframe: "1h"
  lookback_limit: 200
  supertrend_period: 10
  supertrend_multiplier: 3.0
  obv_signal_period: 8
  atr_period: 14
  atr_trailing_multiplier: 2.5
  first_take_profit_pct: 0.02
  first_take_profit_size: 0.50
  second_take_profit_pct: 0.05
  second_take_profit_size: 0.25
```

> 兼容旧配置中的 `timeframe` / `fast_ema` / `slow_ema` 字段，但新版本 CTA 已改为 SuperTrend + OBV + ATR 模型。

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
- `CTARobot` 只在 `trend` 激活，同时读取 `15m` 与 `1h` 两个周期；只有当双周期 `SuperTrend` 方向一致且 `OBV` 相对其信号线方向一致时，才会发出 OKX 模拟盘合约市价单
- CTA 持仓使用 `ATR` 追踪止损模型，默认在 `+2%` 先止盈 `50%`，`+5%` 再止盈 `25%`，剩余仓位交给 trailing stop 管理
- `GridRobot` 只在 `sideways` 激活，在当前价上下各 2% 范围布 10 层等差网格限价单
- 若数据库状态发生切换，对应机器人会先尝试撤销该 symbol 全部挂单并平掉全部持仓，再进入新周期

### 总控与风控
- `MainController` 使用 `threading` 并发启动 `MarketOracle`、`CTARobot`、`GridRobot`
- `RiskManager` 每分钟检查一次账户总权益与总浮盈亏
- 当账户总回撤超过 `5%` 时，会立即强制清仓并停止程序
- 日志支持颜色区分、CPU 占用率输出、Ctrl/Cmd+C 优雅退出与 shutdown checkpoint 落库

## 可复用入口
- `market_adaptive.config.load_config`
- `market_adaptive.db.DatabaseInitializer`
- `market_adaptive.clients.OKXClient`
- `market_adaptive.notifiers.DiscordNotifier`
- `market_adaptive.oracles.MarketOracle`
- `market_adaptive.strategies.CTARobot`
- `market_adaptive.strategies.GridRobot`
- `market_adaptive.strategies.HandsCoordinator`
- `market_adaptive.bootstrap.MarketAdaptiveBootstrap`
