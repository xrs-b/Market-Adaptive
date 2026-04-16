# Market-Adaptive

Market-Adaptive 是一个面向 **OKX 模拟盘 / 永续合约** 的模块化量化交易系统。

当前项目不是“策略骨架”阶段，而是已经进入 **持续运行 + 迭代优化 + 复盘驱动** 阶段：
- `MarketOracle` 负责市场状态判定（trend / sideways / trend_impulse / range_breakout_ready）
- `CTARobot` 负责趋势型 CTA 机会
- `GridRobot` 负责横盘网格
- `RiskControlManager` 负责账户级风控、方向敞口约束、分层减仓
- `MainController` 负责统一调度、启动、状态切换与恢复

---

## 当前状态概览

当前仓库已具备以下能力：

### 核心系统
- YAML 配置加载与应用启动引导
- SQLite 状态 / 运行时数据库
- OKX REST 客户端
- 总控 `MainController`
- Discord 通知
- 风控、恢复、日志、状态持久化

### CTA（趋势策略）
- 4H / 1H / 15m 多周期信号链路
- `SuperTrend + RSI + KDJ + OBV + Volume Profile + Order Flow + Sentiment`
- 动态 ATR 止损
- 分级止盈 / 分级退出
- 最低预期 RR 过滤
- `relaxed / starter / early` 入口收紧
- short-side 对称 starter frontrun 路径
- trigger family / group 标签化，方便后续复盘与统计

### Grid（横盘策略）
- 动态网格上下边界
- 高周期趋势守门
- directional skew
- 风险减仓 / observe / cleanup
- 可拆分 Grid 拦截原因（例如 oracle ADX vs 高周期趋势守门）

### 运维 / 复盘
- CTA signal heartbeat
- SignalProfiler 漏斗审计
- near miss 报告
- 主控日志重启前自动归档，避免复盘样本被覆盖

---

## 最近一轮关键增强

以下是最近几轮已经落地的、对当前系统最重要的增强：

### 利润质量改造（CTA）
- 最低预期 RR 过滤（`minimum_expected_rr`）
- 减少过早止盈：首次 TP 从 50% 降到 25%
- `signal_flip` 改成分级退出：先减仓，再二次确认全退

### 入口质量收紧
- `relaxed_entry` 使用更高 RR 门槛
- `starter / frontrun / scale_in / early_*` 使用更严 RR 门槛
- `early / starter` 增加方向稳定性确认
- `relaxed / starter` 增加 near-breakout 位置质量过滤
- `early / starter` 增加 score floor，减少垃圾 starter 单

### 多空信号对称性补强
- 增加 `starter_short_frontrun`
- 增加 short-side impulse 确认
- short-side 执行路径与 long-side 更对称

### 解释层与复盘能力增强
- `execution_trigger.family`
- `execution_trigger.group`
- family / group 已接入 heartbeat / profiler / near-miss 相关链路

### 运维增强
- `scripts/restart_main_controller.sh` 会在重启前自动归档旧 `main_controller.log`
- 配置加载增加重复 YAML key 检测，防止 `cta:` / `grid:` 重复段静默覆盖

---

## 目录结构

```text
.
├── config/
│   └── config.yaml.example
├── docs/
│   └── market-adaptive-7day-validation.md
├── market_adaptive/
│   ├── clients/
│   ├── notifiers/
│   ├── oracles/
│   ├── strategies/
│   │   ├── cta_robot.py
│   │   ├── grid_robot.py
│   │   ├── mtf_engine.py
│   │   ├── signal_profiler.py
│   │   ├── order_flow_sentinel.py
│   │   └── obv_gate.py
│   ├── bootstrap.py
│   ├── config.py
│   ├── controller.py
│   ├── db.py
│   ├── indicators.py
│   ├── logging_utils.py
│   ├── risk.py
│   ├── sentiment.py
│   └── ws_runtime.py
├── scripts/
│   ├── init_app.py
│   ├── restart_main_controller.sh
│   ├── run_main_controller.py
│   ├── run_market_oracle.py
│   └── run_the_hands.py
├── tests/
└── README.md
```

---

## 快速开始

> 建议使用 Python 3.13 对应的虚拟环境；本项目当前主要在 macOS + venv 环境下运行。

### 1. 创建虚拟环境并安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 复制配置模板

```bash
cp config/config.yaml.example config/config.yaml
```

### 3. 初始化数据库

```bash
python scripts/init_app.py --config config/config.yaml
```

### 4. 单次运行 / 常用入口

#### 只跑 Market Oracle 一次
```bash
python scripts/run_market_oracle.py --config config/config.yaml --once
```

#### 跑 CTA + Grid 协调器
```bash
python scripts/run_the_hands.py --config config/config.yaml
```

#### 跑主控
```bash
python scripts/run_main_controller.py --config config/config.yaml
```

#### 重启主控（推荐）
```bash
scripts/restart_main_controller.sh config/config.yaml
```

> `restart_main_controller.sh` 现会在重启前自动归档旧日志到 `logs/archive/`。

---

## 关键配置（当前推荐关注）

### CTA
重点字段：
- `minimum_expected_rr`
- `relaxed_entry_minimum_expected_rr`
- `starter_entry_minimum_expected_rr`
- `early_entry_minimum_score`
- `starter_frontrun_minimum_score`
- `early_entry_direction_confirmation_bars`
- `relaxed_entry_require_near_breakout`
- `starter_entry_require_near_breakout`
- `margin_fraction_per_trade`
- `nominal_leverage`
- `early_bullish_starter_fraction`
- `starter_frontrun_fraction`
- `signal_flip_reduce_ratio`

### Grid
重点字段：
- `equity_allocation_ratio`
- `higher_timeframe_trend_guard_enabled`
- `higher_timeframe_trend_distance_atr_threshold`
- `directional_skew_enabled`
- `bullish_buy_levels / bullish_sell_levels`
- `bearish_buy_levels / bearish_sell_levels`
- `flash_crash_*`

### Risk
重点字段：
- `daily_loss_cutoff_pct`
- `max_margin_ratio`
- `cta_single_trade_equity_multiple`
- `max_directional_leverage`
- `grid_deviation_reduce_ratio`
- `grid_liquidation_warning_ratio`
- `grid_reduction_step_pct`

---

## 运行时观察重点

当前阶段最值得观察的不是“有没有信号”，而是：

### CTA
- 哪些 `trigger family / group` 最常被拦
- `waiting_execution_trigger_near_breakout` 是否长期卡住执行
- `order_flow_blocked` 是否在通过前置条件后成为最后挡板
- 亏损单是否仍然集中在：
  - 手续费型薄单
  - 刚开即反手
  - 位置差 / chasing 单

### Grid
- `grid:oracle_adx_trend_blocked`
- `grid:higher_timeframe_trend_guard_blocked`
- `grid:adx_trend_not_ready` 是否已被更细粒度原因替代
- sideways 阶段是否仍长期不挂单

---

## 日志与复盘

### 主日志
默认主控日志：
```text
logs/main_controller.log
```

### 归档日志
重启前自动归档到：
```text
logs/archive/main_controller-YYYYMMDD-HHMMSS.log
```

### 当前复盘建议
- 优先看 `CTA signal heartbeat`
- 看 `Strategy audit snapshot`
- 看 `[TRADE_OPEN] / [TRADE_CLOSE]`
- 配合 `docs/market-adaptive-7day-validation.md` 做阶段验证

---

## 配置与安全说明

### 不要提交真实配置
请勿把以下内容提交到仓库：
- 真实 `config/config.yaml`
- API key / secret / passphrase
- Discord bot token / webhook

仓库里应只维护：
- `config/config.yaml.example`
- 代码层面的配置加载逻辑

### 当前已加的保护
- YAML 重复 key 会直接报错
- 可避免 `cta:` / `grid:` 段重复导致静默覆盖

---

## 当前开发策略

项目当前采用：
> **小步、低风险、可验证、可回退**

即：
- 一次只做一类增强
- 先验证、再推进下一节点
- 优先避免“理论上更强，但实盘样本更少”的过度优化

---

## 当前不建议直接做的事
以下方向并非永远不做，但当前不是优先级最高：
- 直接加 1D 硬过滤
- 大规模重写 RSI 评分链
- 一口气放宽 OBV 阈值
- 在样本不足时继续大改 trigger 边界

当前更推荐：
- 先用真实日志 + family/group 分桶观察
- 再决定下一轮优化切哪一层

---

## GitHub / 运行差异说明
仓库中的代码会持续更新到 `main`，但本地运行环境可能还有：
- 本地私有配置
- 模拟盘凭证
- 本地日志归档

因此：
> GitHub `main` 代表**安全可共享代码状态**，不代表你本机完整私有运行环境。

---

## 下一步最建议的工作流
1. 保持主控稳定运行一段样本
2. 观察 CTA / Grid 的 family/group 分布
3. 用 `7day-validation` 文档更新阶段结果
4. 再决定下一轮优化是否启动

---

如果你正在接手这个项目，最重要的一句话是：
> **不要一口气重写整个策略；先看真实样本，再针对主矛盾下刀。**
