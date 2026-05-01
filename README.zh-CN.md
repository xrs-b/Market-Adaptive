# Market-Adaptive

[English](./README.md) | [简体中文](./README.zh-CN.md)

Market-Adaptive 是一套面向 **OKX 优先** 的模块化量化交易系统，核心围绕：

- **CTA 趋势交易**
- **Grid / 区间交易**
- **账户级风险控制**
- **主控调度与运行编排**
- **回放、诊断与审计工具**
- **管理后台（admin API + Web）**

它不是一个单独的“策略脚本”，而是一套更完整的**交易运行时系统**：可观测、可调参、可回放、可持续迭代。

---

## 项目概览

Market-Adaptive 试图把多层能力整合到一套系统里：

- 识别市场状态（`trend`、`sideways`、`trend_impulse` 等）
- 在 CTA 与 Grid 之间切换 / 分配执行逻辑
- 统一管理账户级风险限制
- 记录策略决策过程与 blocker 原因，方便复盘
- 支持回测、回放、family 归因与调参验证

所以这个仓库更适合被理解为：

> **交易引擎 + 诊断平台**

而不只是若干指标文件的拼装。

---

## 核心模块

### CTA 引擎
一套多层判定的趋势交易执行栈，包含：

- 多周期结构（4H / 1H / 15m）
- 信号质量与置信度评分
- entry decider 与位置评分
- OBV gate
- volume profile / POC / value area 上下文
- order-flow 与 trigger-family 诊断
- 可选 ML gate

主要文件：
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/entry_decider_lite.py`
- `market_adaptive/strategies/obv_gate.py`
- `market_adaptive/strategies/order_flow_sentinel.py`
- `market_adaptive/strategies/signal_profiler.py`
- `market_adaptive/cta_dashboard.py`

### Grid 引擎
面向区间行情的执行模块，支持动态网格行为。

主要文件：
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/strategies/dynamic_grid_robot.py`

### 风控层
负责 CTA / Grid 之上的统一账户保护。

主要文件：
- `market_adaptive/risk.py`

### 主控
统一协调 market oracle、CTA、Grid、risk、recovery 以及 heartbeat worker。

主要文件：
- `market_adaptive/controller.py`
- `scripts/run_main_controller.py`
- `scripts/restart_main_controller.sh`

### 管理后台
用于运行状态观察与配置管理：
- `admin-api/`
- `admin-web/`

---

## 仓库结构

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

关键运行区域：
- `market_adaptive/` → 主线源码
- `config/` → 本地配置
- `logs/` / `data/` → 运行产物
- `.venv/` → 本地虚拟环境

---

## 快速开始

### 1. 创建虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 准备配置

```bash
cp config/config.yaml.example config/config.yaml
```

### 3. 初始化存储

```bash
python scripts/init_app.py --config config/config.yaml
```

### 4. 启动主控

```bash
python scripts/run_main_controller.py --config config/config.yaml
```

推荐的正式重启方式：

```bash
./scripts/restart_main_controller.sh config/config.yaml
```

---

## 常用脚本

### 运行类
```bash
python scripts/run_main_controller.py --config config/config.yaml
python scripts/run_market_oracle.py --config config/config.yaml --once
python scripts/run_the_hands.py --config config/config.yaml
```

### 回测 / 回放 / 验证
```bash
python scripts/cta_backtest_sandbox.py
python scripts/run_cta_backtest_segmented.py
python scripts/cta_multiwindow_validator.py
python scripts/cta_trade_quality_report.py
python scripts/cta_family_report.py
python scripts/replay_single_trade_timestamp.py
python scripts/review_trade_journal.py
```

### ML 相关
```bash
python scripts/train_ml_signal_model.py
python scripts/run_ml_replay_compare.py
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
pytest -q tests/test_ws_runtime.py
```

---

## 当前重点

项目的基础设施已经相对成熟。

当前主线工作集中在 **CTA 执行质量**，尤其是：
- 让强势 short 候选真正能进入可交易状态
- 减少最后一跳 gating 链中的误杀
- 在不放出大量手续费型烂单的前提下提高候选质量

换句话说，现在最重要的不是继续堆规则，而是：

> **让真正高质量的候选，能穿过决策链并在回放与实时诊断中证明自己。**

---

## 适合谁使用

如果你想要的是：
- 本地化、偏 OKX 的交易运行时
- 多策略协调，而不是单个脚本
- 可回放的决策日志
- family 级别的诊断与 blocker 可视化
- 能在执行逻辑层持续迭代的代码库

那这个仓库很适合你。

如果你想要的是：
- 即装即用
- 不需要理解内部结构
- 保证赚钱

那它并不是这种产品。

---

## 说明

- 项目迭代速度较快
- CTA 逻辑仍在持续调优
- 日志、回放工具和 dashboard 是高效使用本系统的重要组成部分

如果你想看更偏运维和实操的说明，请继续阅读：
- `docs/USAGE_AND_OPERATIONS_MANUAL.md`
- `docs/MAINTENANCE_HUMAN.md`
- `docs/MAINTENANCE_AI.md`
