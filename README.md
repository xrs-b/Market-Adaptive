# Market-Adaptive

Market-Adaptive 是一个面向 **OKX 永续合约 / 模拟盘优先** 的模块化量化交易系统。

它现在不是“策略骨架”，而是一套已经包含：
- 市场状态识别
- CTA 趋势交易
- Grid 横盘交易
- 账户级风险控制
- 主控调度与状态恢复
- 通知、日志、复盘、回测沙盒

的完整运行框架。

---

## 当前结论

### 已经稳定有价值的部分
- **Grid**：目前仍然是系统里最稳定、最有明确正反馈的盈利模块。
- **主控 / 风控 / 通知 / 日志 / 审计**：基础设施已经比较完整，可继续复用。
- **CTA 回测沙盒**：已经能做分段验证、行为归因与诊断，不再是只能靠肉眼看日志。

### 当前最需要谨慎的部分
- **CTA**：经过多轮 trigger / regime / OBV / RR / order-flow 改造后，仍未稳定证明自己优于旧信号型策略。
- 当前 CTA 的主要问题不是“完全没信号”，而是：
  - 信号很多，但高质量信号密度不够；
  - 过滤链路太长，容易错杀；
  - 一旦放松，又容易退化成手续费型烂单。

### 最近一轮 CTA 调整方向
最新代码已进一步收紧：
- relaxed short 质量门槛
- quick trade / OBV scalp 的触发条件与 RR 下限
- starter / frontrun / scale-in 的最低质量门槛
- same-direction stop cooldown

目标很明确：
**减少烂单、降低手续费、提高 relaxed short 与 starter 类入场的质量。**

---

## 系统架构

### 1. Market Oracle
负责判定市场状态，例如：
- `trend`
- `sideways`
- `trend_impulse`
- `range_breakout_ready`

主要文件：
- `market_adaptive/oracles/market_oracle.py`

### 2. CTA Robot
负责趋势型交易机会识别与执行。

当前 CTA 使用的核心输入包括：
- 多周期结构（4H / 1H / 15m）
- SuperTrend
- RSI / KDJ
- OBV 与动态 gate
- Volume Profile / POC / Value Area
- Order Flow
- 风险回报比过滤

主要文件：
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/obv_gate.py`
- `market_adaptive/strategies/order_flow_sentinel.py`
- `market_adaptive/strategies/signal_profiler.py`

### 3. Grid Robot
负责横盘区间型盈利。

主要能力：
- 动态区间
- 高周期趋势守门
- directional skew
- 风险减仓 / 观察 / 清理

主要文件：
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/strategies/dynamic_grid_robot.py`

### 4. Risk Control
负责账户级统一风险管理。

包括：
- 账户净值与浮盈亏监控
- drawdown 分级控制
- 方向敞口限制
- CTA / Grid 联动降风险

主要文件：
- `market_adaptive/risk.py`

### 5. Main Controller
统一调度：
- Market Oracle
- CTA
- Grid
- Risk
- 恢复 / 状态同步 / 日志归档

主要文件：
- `market_adaptive/controller.py`

---

## 项目结构

```text
.
├── config/
│   └── config.yaml.example
├── docs/
│   ├── market-adaptive-7day-validation.md
│   ├── MAINTENANCE_AI.md
│   └── MAINTENANCE_HUMAN.md
├── market_adaptive/
│   ├── clients/
│   ├── notifiers/
│   ├── oracles/
│   ├── strategies/
│   ├── bootstrap.py
│   ├── config.py
│   ├── controller.py
│   ├── db.py
│   ├── indicators.py
│   ├── risk.py
│   └── ws_runtime.py
├── scripts/
│   ├── cta_backtest_sandbox.py
│   ├── download_okx_klines.py
│   ├── run_cta_backtest_segmented.py
│   ├── run_main_controller.py
│   ├── run_market_oracle.py
│   └── run_the_hands.py
├── tests/
└── README.md
```

---

## 快速开始

### 1. 安装依赖

建议使用已有虚拟环境：

```bash
source Market-Adaptive/.venv/bin/activate
pip install -r requirements.txt
```

如果要新建：

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

---

## 回测与验证

### 单次 CTA 沙盒
```bash
python scripts/cta_backtest_sandbox.py
```

### 分段 CTA 回测
```bash
python scripts/run_cta_backtest_segmented.py
```

支持环境变量：

- `CTA_BACKTEST_OUTDIR`：输出目录名
- `CTA_SEGMENT_START`：起始段
- `CTA_SEGMENT_END`：结束段

例子：

```bash
CTA_BACKTEST_OUTDIR=cta_backtest_segments_run1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

> 说明：分段脚本已经支持按段范围跑，避免整轮回测被系统中断后完全无结果。

---

## 测试

全量测试：

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=/Users/oink/.openclaw/workspace \
/Users/oink/.openclaw/workspace/Market-Adaptive/.venv/bin/pytest -q tests
```

常见定向测试：

```bash
pytest -q tests/test_cta_entry_quality.py
pytest -q tests/test_mtf_engine.py
pytest -q tests/test_ws_runtime.py
```

---

## 当前维护重点

### CTA
优先关注：
- 高质量信号是否真的在增加
- starter / frontrun / scale-in 是否显著减少烂单
- 手续费型薄单是否下降
- relaxed short 是否从“能开”变成“值得开”

### Grid
优先关注：
- `grid:skip:inactive`
- `grid:oracle_adx_trend_blocked`
- 高周期趋势守门是否过严
- live 状态下是否出现长时间完全不挂单

### 运维
优先关注：
- 主控是否持续运行
- `logs/main_controller.log` 是否正常追加
- 重启前是否完成归档
- 风控 heartbeat 是否稳定输出

---

## 相关文档

- AI 维护文档：`docs/MAINTENANCE_AI.md`
- 人类维护文档：`docs/MAINTENANCE_HUMAN.md`
- 7 天验证表：`docs/market-adaptive-7day-validation.md`

---

## 最后一句

这套系统当前最成熟的价值，不在“CTA 已经证明自己很强”，而在：
- **Grid 已经能跑出明确价值**
- **基础设施已经足够支撑持续迭代**
- **CTA 现在终于能被更系统地诊断，而不是靠感觉修**

接下来真正重要的，不是继续堆规则，而是：
**持续验证哪些信号真的赚钱，哪些只是看起来聪明。**
