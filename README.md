# Market-Adaptive

Market-Adaptive 是一套面向 **OKX / 模拟盘优先** 的模块化量化交易系统，核心目标不是“堆很多策略”，而是把：

- 市场状态识别
- CTA 趋势交易
- Grid 区间交易
- 账户级风险控制
- 主控调度 / 恢复 / 审计
- 回放 / 诊断 / 验证工具
- 管理端（API + Web）

串成一条能持续迭代的完整链路。

当前仓库已经不是早期策略骨架，而是一个可运行、可回放、可审计、可继续压实策略质量的主线项目。

---

## 当前项目状态

### 已经比较成熟的部分
- **Grid 基础设施**：区间策略、趋势守门、风控联动、状态管理已经成体系。
- **主控 / 风控 / 恢复链路**：主控调度、风险 heartbeat、状态恢复、日志归档基本完整。
- **CTA 诊断能力**：现在不只是“有没有信号”，而是能看到：
  - quality / confidence / pathway
  - entry decider
  - OBV / Volume Profile / RSI / KDJ
  - blocker_reason
  - family / trigger / near-miss / 回放数据
- **回测与归因工具**：已经具备按窗口、按家族、按 trade quality 做系统性分析的能力。

### 当前仍在重点优化的部分
- **CTA 实盘可开性与质量稳定性**。

当前 CTA 的主问题不再只是“没信号”，而是更细的几层：
- short 候选能否成型；
- 成型后会不会被 OBV / RSI / alignment / trigger 链路错杀；
- 放宽后是否真的带来值得开的单，而不是手续费型烂单。

换句话说，项目现在最重要的工作是：

> **把“看起来像好单”的 CTA short，变成真正能稳定放行且可验证盈利质量的交易机会。**

---

## 代码主线概览

### 1. Main Controller
统一调度系统各个 worker：
- market oracle
- CTA
- grid
- risk
- recovery
- 主控 heartbeat

主要文件：
- `market_adaptive/controller.py`
- `scripts/run_main_controller.py`
- `scripts/restart_main_controller.sh`

### 2. Market Oracle
负责市场状态识别，例如：
- `trend`
- `sideways`
- `trend_impulse`
- `range_breakout_ready`

主要文件：
- `market_adaptive/oracles/market_oracle.py`

### 3. CTA Trend Engine
CTA 现在是“多层判定 + 多层放行 / 拦截”的架构，核心组件包括：

- 多周期引擎（4H / 1H / 15m）
- signal quality / confidence
- entry decider
- dynamic OBV gate
- volume profile / POC / value area
- order flow sentinel
- ML gate（可开关）
- family / trigger / pathway 归因
- CTA dashboard snapshot

主要文件：
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/entry_decider_lite.py`
- `market_adaptive/strategies/obv_gate.py`
- `market_adaptive/strategies/order_flow_sentinel.py`
- `market_adaptive/strategies/signal_profiler.py`
- `market_adaptive/cta_dashboard.py`
- `market_adaptive/cta_quality.py`
- `market_adaptive/ml_signal_engine.py`

### 4. Grid Engine
负责偏区间 / 偏网格的交易逻辑与动态调节。

主要文件：
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/strategies/dynamic_grid_robot.py`

### 5. Risk Layer
负责账户级统一风控：
- 净值与回撤
- 保证金与敞口
- 新开仓阻断
- CTA / Grid 联动限流

主要文件：
- `market_adaptive/risk.py`

### 6. Runtime / Client / Persistence
负责：
- OKX REST / WS 接入
- 数据库存储
- 状态同步
- runtime 兼容层

主要文件：
- `market_adaptive/clients/okx_client.py`
- `market_adaptive/clients/okx_ws_client.py`
- `market_adaptive/db.py`
- `market_adaptive/ws_runtime.py`
- `market_adaptive/experimental/ws_runtime.py`

### 7. Admin Stack
管理端分为：
- `admin-api/`：后端 API
- `admin-web/`：前端界面

用于：
- 查看状态
- 调整配置
- 读运行日志
- 辅助监控和运维

---

## 目录结构

```text
.
├── admin-api/
├── admin-web/
├── config/
│   ├── config.yaml
│   └── config.yaml.example
├── docs/
├── logs/
├── data/
├── market_adaptive/
│   ├── clients/
│   ├── experimental/
│   ├── notifiers/
│   ├── oracles/
│   ├── strategies/
│   ├── bootstrap.py
│   ├── config.py
│   ├── controller.py
│   ├── coordination.py
│   ├── cta_dashboard.py
│   ├── cta_quality.py
│   ├── db.py
│   ├── indicators.py
│   ├── ml_signal_engine.py
│   ├── risk.py
│   └── ws_runtime.py
├── scripts/
├── tests/
└── README.md
```

---

## 运行环境说明

### 单目录约定
当前仓库约定是：

- `market_adaptive/`：主线源码
- `config/`：配置
- `logs/` / `data/`：本地运行产物
- `.venv/`：仓库内虚拟环境

也就是说，部署和维护都应该围绕**当前仓库目录本身**，不要再依赖仓库外的旧运行资产目录。

### 正式启动链
当前正式主控通常通过 **launchd + wrapper 脚本** 启动：
- launchd plist
- `~/.openclaw/scripts/run_market_adaptive_main_controller.sh`
- 实际入口：`scripts/run_main_controller.py`

日常重启优先使用：

```bash
./scripts/restart_main_controller.sh config/config.yaml
```

而不是手工 `pkill`，避免和守护进程管理打架。

---

## 快速开始

### 1. 安装依赖

推荐使用仓库内虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 准备配置

```bash
cp config/config.yaml.example config/config.yaml
```

### 3. 初始化数据库

```bash
python scripts/init_app.py --config config/config.yaml
```

### 4. 运行入口

#### 跑主控
```bash
python scripts/run_main_controller.py --config config/config.yaml
```

#### 正式重启主控
```bash
./scripts/restart_main_controller.sh config/config.yaml
```

#### 只跑 Market Oracle 一次
```bash
python scripts/run_market_oracle.py --config config/config.yaml --once
```

#### 跑 CTA + Grid 协调器
```bash
python scripts/run_the_hands.py --config config/config.yaml
```

---

## 回放 / 验证 / 诊断工具

### CTA 回测与窗口验证
```bash
python scripts/cta_backtest_sandbox.py
python scripts/run_cta_backtest_segmented.py
python scripts/cta_multiwindow_validator.py
```

### CTA 家族 / 质量归因
```bash
python scripts/cta_family_report.py
python scripts/cta_family_window_compare.py
python scripts/cta_trade_quality_report.py
```

### 单次交易 / 时间点回放
```bash
python scripts/replay_single_trade_timestamp.py
python scripts/review_trade_journal.py
python scripts/analyze_trade_opportunities.py
```

### ML 相关
```bash
python scripts/train_ml_signal_model.py
python scripts/run_ml_replay_compare.py
python scripts/test_permit_replay_ml_gate.py
```

### 其它分析工具
```bash
python scripts/analyze_mbr_gate.py
python scripts/logic_pressure_test.py
python scripts/profitable_day_drag_report.py
python scripts/cta_grid_drag_report_localday.py
```

---

## 测试

全量测试：

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=$(pwd) \
.venv/bin/pytest -q tests
```

常见定向测试：

```bash
pytest -q tests/test_cta_entry_quality.py
pytest -q tests/test_cta_entry_decider_and_guard.py
pytest -q tests/test_cta_quality.py
pytest -q tests/test_cta_multiwindow_validator.py
pytest -q tests/test_ws_runtime.py
```

---

## 当前 CTA 重点说明

最新主线代码已经不再是单纯“调 trigger”，而是在做 **可交易 short 的全链路修正**。当前你在代码里能看到这些方向：

### 1. 候选质量可观测化
日志 / trade journal / signal profiler 里已经能持续看到：
- `signal_quality_tier`
- `signal_confidence`
- `entry_pathway`
- `entry_decider_decision`
- `entry_decider_score`
- `entry_location_score`
- `blocker_reason`
- `market_regime`

### 2. Short 成型链修正
最近一轮代码已经补过几类定向修正：
- short 的 `OBV_ABOVE_SMA` 定向 override
- short 的 `OBV_STRENGTH_NOT_CONFIRMED` 定向 override
- short 的 `RSI_Threshold` 定向 override
- `weak_bull_bias` 对 short 方向权的压制修正
- 针对高质量 `FAST_TRACK / STANDARD` short 的最终执行放行

### 3. 当前真实目标
项目现在不是盲目增加开仓数，而是优先解决：

> **那些已经被识别为高质量、high-confidence、RR 足够的 short，为什么还会在最后一跳被错杀。**

---

## 常用观测点

### 主控日志
- `logs/main_controller.log`
- `~/.openclaw/logs/main_controller_launchd.log`
- `~/.openclaw/logs/main_controller_launchd_error.log`

### 重点看这些关键词
- `Strategy audit snapshot`
- `CTA signal heartbeat`
- `Blocked_By_...`
- `cta:open_...`
- `grid:...`
- `Risk heartbeat`

### 重启后优先确认
- `Main Controller started`
- `cta Worker started`
- `grid Worker started`
- `risk Worker started`
- heartbeat 是否持续追加

---

## 文档

- `docs/MAINTENANCE_AI.md`
- `docs/MAINTENANCE_HUMAN.md`
- `docs/market-adaptive-7day-validation.md`

---

## 项目当前定位

这套系统当前最成熟的价值，不是“CTA 已经彻底证明自己”，而是：

- **交易基础设施已经足够完整**
- **Grid / 风控 / 主控链条已经能稳定支撑迭代**
- **CTA 已经进入“可精确诊断、可定向修正”的阶段**

所以接下来最重要的不是继续无脑堆规则，而是：

> **持续把“看起来像好单”的 CTA 候选，筛成真正能稳定开、且值得开的单。**
