# Market-Adaptive 使用与维护手册

> 面向：人类操作者、运维者、策略维护者、低干预平台管理者
>
> 目的：让接手系统的人，能在**不依赖口口相传**的情况下完成启动、观察、调参、回放、切盘、排障与日常维护。

---

# 目录

- [1. 快速索引](#1-快速索引)
- [2. 先建立正确认知](#2-先建立正确认知)
- [3. 目录与关键文件](#3-目录与关键文件)
- [4. 如何启动、停止、重启主控和后台](#4-如何启动停止重启主控和后台)
- [5. 脚本速查表](#5-脚本速查表)
- [6. 各脚本的用途、用法、结果怎么看](#6-各脚本的用途用法结果怎么看)
- [7. 执行完脚本后去哪里看结果](#7-执行完脚本后去哪里看结果)
- [8. 管理后台怎么用](#8-管理后台怎么用)
- [9. 什么时候该调参数，什么时候不要乱动](#9-什么时候该调参数什么时候不要乱动)
- [10. 如何从模拟盘切到实盘](#10-如何从模拟盘切到实盘)
- [11. 日常维护建议](#11-日常维护建议)
- [12. 常见问题 FAQ](#12-常见问题-faq)
- [13. 排障顺序](#13-排障顺序)
- [14. 提交代码和推送前注意事项](#14-提交代码和推送前注意事项)
- [15. 最后一句](#15-最后一句)

---

# 1. 快速索引

## 1.1 最常用命令

### 重启主控（推荐）
```bash
./scripts/restart_main_controller.sh config/config.yaml
```

### 前台直接跑主控（调试）
```bash
python scripts/run_main_controller.py --config config/config.yaml
```

### 看主控日志
```bash
tail -f logs/main_controller.log
```

### 看 launchd 正式启动链日志
```bash
tail -f ~/.openclaw/logs/main_controller_launchd_error.log
```

### 初始化数据库
```bash
python scripts/init_app.py --config config/config.yaml
```

### 单次 Oracle 检查
```bash
python scripts/run_market_oracle.py --config config/config.yaml --once
```

### 分段 CTA 回测
```bash
CTA_BACKTEST_OUTDIR=cta_backtest_segments_batch1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

### 全量测试
```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=$(pwd) \
.venv/bin/pytest -q tests
```

---

## 1.2 最常看的文件

- 主控日志：`logs/main_controller.log`
- 正式启动链日志：`~/.openclaw/logs/main_controller_launchd_error.log`
- 配置文件：`config/config.yaml`
- 数据库：`data/market_adaptive.sqlite3`
- 主控源码：`market_adaptive/controller.py`
- CTA 主线：`market_adaptive/strategies/cta_robot.py`
- MTF 决策引擎：`market_adaptive/strategies/mtf_engine.py`
- 风控：`market_adaptive/risk.py`

---

# 2. 先建立正确认知

## 2.1 这不是单个机器人脚本

Market-Adaptive 不是“一个策略文件 + 一个交易所 key”那种轻量 bot。
它是一整套运行系统，包含：

- 主控（Main Controller）
- 市场状态识别（Market Oracle）
- CTA 趋势交易
- Grid 区间交易
- 风控层（Risk）
- 状态恢复（Recovery）
- 日志 / 诊断 / 回放 / 回测
- 管理后台（admin-api + admin-web）

所以实际维护时，不要只盯某个策略文件。很多“没开单 / 没挂单 / 没动作”的问题，根本原因可能是：
- 主控没跑
- worker 异常退出
- 风控阻断开仓
- 市场状态识别把策略判成 inactive
- 管理后台读到了旧日志

---

## 2.2 当前项目的现实状态

### 比较成熟的
- 主控 / 风控 / 恢复 / 日志 / 审计链路
- Grid 基础设施
- CTA 的诊断能力（quality、confidence、blocker、family、trigger）
- 回放、家族分析、trade quality 分析工具

### 还在持续优化的
- CTA，尤其是 **short 候选成型 → 放行执行** 这条链

建议始终带着这个心态维护：

> 当前系统最有价值的部分，不是“CTA 已经很强”，而是“它已经足够可观测、可回放、可定向修正”。

---

# 3. 目录与关键文件

## 3.1 核心目录

```text
.
├── admin-api/
├── admin-web/
├── config/
├── docs/
├── logs/
├── data/
├── market_adaptive/
├── scripts/
├── tests/
└── README.md
```

## 3.2 关键目录说明

### `market_adaptive/`
主线源码目录。

日常最常改的是这里，尤其：
- `market_adaptive/controller.py`
- `market_adaptive/risk.py`
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/config.py`
- `market_adaptive/db.py`

### `config/`
配置文件目录。

常见文件：
- `config/config.yaml`
- `config/config.yaml.example`

### `logs/`
主控日志和历史日志归档。

最重要的文件：
- `logs/main_controller.log`

### `data/`
数据库、模型文件、分析输出等。

### `scripts/`
各种可执行脚本：启动、回测、复盘、分析、训练、报告。

### `admin-api/` / `admin-web/`
管理后台后端和前端。

---

## 3.3 最容易踩的坑

默认应该只维护：

```text
/Users/oink/.openclaw/workspace/market_adaptive/
```

不要把修复只打到旧副本目录。

---

# 4. 如何启动、停止、重启主控和后台

## 4.1 主控是什么

主控会统一拉起和调度这些 worker：
- market oracle
- CTA
- Grid
- Risk
- Recovery
- main heartbeat

如果主控没跑，系统基本就等于没跑。

---

## 4.2 推荐的正式重启方式

**优先使用：**

```bash
./scripts/restart_main_controller.sh config/config.yaml
```

原因：
- 它优先走 launchd 管理链
- 避免手工杀进程后被守护进程拉起另一套环境
- 避免同时跑多个 main_controller 进程
- 避免日志和真实运行链不一致

---

## 4.3 直接运行主控（调试时）

```bash
python scripts/run_main_controller.py --config config/config.yaml
```

适合：
- 本地前台调试
- 快速看启动报错
- 开发阶段单次运行

不适合长期替代正式启动链。

---

## 4.4 只跑 Market Oracle

```bash
python scripts/run_market_oracle.py --config config/config.yaml --once
```

适合：
- 验证市场状态识别是否正常
- 看当前是 `trend`、`sideways` 还是其它状态

---

## 4.5 跑 CTA + Grid 协调器

```bash
python scripts/run_the_hands.py --config config/config.yaml
```

更偏旧式/局部运行；日常还是以 main controller 为主。

---

## 4.6 如何判断主控有没有真的跑起来

日志里优先看：
- `Main Controller started`
- `cta Worker started`
- `grid Worker started`
- `risk Worker started`
- `recovery Worker started`

确认日志持续追加：

```bash
tail -f logs/main_controller.log
```

如果你走 launchd 路径，还看：
- `~/.openclaw/logs/main_controller_launchd.log`
- `~/.openclaw/logs/main_controller_launchd_error.log`

---

## 4.7 如何停止主控

如果只是运维层面，优先通过正式管理链停止/重启，而不是裸 `pkill`。

原因是 launchd 存在时，你停掉一个进程，它可能立刻又被拉起。

结论：

> **不要把 `pkill` 当默认重启方案。**

---

## 4.8 管理后台怎么启动

### admin-api
一般在 `admin-api/` 目录里启动（按你当前本地方式运行）。

### admin-web
一般在 `admin-web/` 目录里启动开发服务器或构建后部署。

如果只是使用现有环境，通常不需要频繁手工重启；只有修改了 API / 前端代码，才需要重启对应服务。

---

# 5. 脚本速查表

| 脚本 | 用途 | 常见场景 |
|---|---|---|
| `run_main_controller.py` | 启动主控 | 前台调试 |
| `restart_main_controller.sh` | 正式重启主控 | 日常运维 |
| `run_market_oracle.py` | 单次状态识别 | 验证市场状态 |
| `run_the_hands.py` | CTA + Grid 协调入口 | 局部运行 |
| `init_app.py` | 初始化数据库 / 应用状态 | 首次部署 |
| `cta_backtest_sandbox.py` | 单次 CTA 沙盒回测 | 快速验改动 |
| `run_cta_backtest_segmented.py` | 分段 CTA 回测 | 长周期验证 |
| `cta_multiwindow_validator.py` | 多窗口比较 | 稳定性验证 |
| `cta_trade_quality_report.py` | 交易质量报告 | 看烂单/好单 |
| `cta_family_report.py` | family 归因 | 看哪类 trigger 真赚钱 |
| `cta_family_window_compare.py` | family 跨窗口比较 | 看稳定性 |
| `replay_single_trade_timestamp.py` | 单笔/单时点回放 | 分析具体异常样本 |
| `review_trade_journal.py` | 查 trade journal | 查 blocked/open/close |
| `analyze_trade_opportunities.py` | 分析错过机会 | 查“明明有机会为什么没开” |
| `analyze_mbr_gate.py` | 分析某类 gate | 定向排障 |
| `train_ml_signal_model.py` | 训练 ML 模型 | ML 路线 |
| `run_ml_replay_compare.py` | ML 对比回放 | 比较 ML gate 前后 |

> 说明：这是“速查表”。详细解释见下一章。

---

# 6. 各脚本的用途、用法、结果怎么看

## 6.1 基础运行类

### `scripts/run_main_controller.py`
**用途：** 启动主控

```bash
python scripts/run_main_controller.py --config config/config.yaml
```

**结果怎么看：**
- 终端输出
- `logs/main_controller.log`

---

### `scripts/restart_main_controller.sh`
**用途：** 正式重启主控（推荐）

```bash
./scripts/restart_main_controller.sh config/config.yaml
```

**结果怎么看：**
- shell 输出
- `logs/main_controller.log`
- `~/.openclaw/logs/main_controller_launchd_error.log`

---

### `scripts/run_market_oracle.py`
**用途：** 单独验证市场状态识别

```bash
python scripts/run_market_oracle.py --config config/config.yaml --once
```

**看结果：**
- 终端输出当前状态
- 日志里看 `Market status updated`

---

### `scripts/run_the_hands.py`
**用途：** 跑 CTA + Grid 协调入口

```bash
python scripts/run_the_hands.py --config config/config.yaml
```

**看结果：**
- 终端输出
- 日志

---

## 6.2 回测 / 验证类

### `scripts/cta_backtest_sandbox.py`
**用途：** 单次 CTA 沙盒回测

```bash
python scripts/cta_backtest_sandbox.py
```

**适合：**
- 快速验证某次改动有没有明显副作用
- 快速看策略行为而不是跑长周期全量验证

**结果怎么看：**
- 终端输出
- 指定输出目录（如果脚本配置了 outdir）
- 配合 trade quality / family report 继续读

---

### `scripts/run_cta_backtest_segmented.py`
**用途：** 分段 CTA 回测

推荐用法：

```bash
CTA_BACKTEST_OUTDIR=cta_backtest_segments_batch1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

**为什么分段：**
- 全量一次跑太久，容易被系统中断
- 分段跑更稳，也更容易比较不同时间窗口

**结果怎么看：**
- 输出目录下的 JSON / 汇总文件
- 搭配 family / quality / validator 脚本继续看

---

### `scripts/cta_multiwindow_validator.py`
**用途：** 多窗口比较 CTA 表现

```bash
python scripts/cta_multiwindow_validator.py
```

**适合：**
- 看不同窗口下的稳定性
- 防止某个改动只在单窗口看起来好看

---

## 6.3 复盘 / 分析类

### `scripts/cta_trade_quality_report.py`
**用途：** 看 CTA 交易质量

```bash
python scripts/cta_trade_quality_report.py
```

**重点看：**
- 好单 / 烂单结构差异
- quality / confidence / RR / family

---

### `scripts/cta_family_report.py`
**用途：** 按 trigger family 统计表现

```bash
python scripts/cta_family_report.py
```

**重点看：**
- family 次数
- 收益/亏损
- 稳定性

---

### `scripts/cta_family_window_compare.py`
**用途：** 比较不同窗口里 family 表现

```bash
python scripts/cta_family_window_compare.py
```

**重点看：**
- 哪些 family 是稳定 edge
- 哪些只是个别窗口好看

---

### `scripts/review_trade_journal.py`
**用途：** 检查 trade_journal 中的交易与 blocker 记录

```bash
python scripts/review_trade_journal.py
```

**适合：**
- 用户说“昨晚都没开单”
- 想知道到底是没候选，还是候选被 blocker 杀了

---

### `scripts/replay_single_trade_timestamp.py`
**用途：** 回放某个具体时间点 / 交易样本

```bash
python scripts/replay_single_trade_timestamp.py
```

**适合：**
- 分析一笔本来该开却没开的单
- 分析一笔开了但明显不该开的单

---

### `scripts/analyze_trade_opportunities.py`
**用途：** 分析错过的机会 / 候选质量

```bash
python scripts/analyze_trade_opportunities.py
```

---

### `scripts/analyze_mbr_gate.py`
**用途：** 分析某一类 gate / family 的行为

```bash
python scripts/analyze_mbr_gate.py
```

---

## 6.4 ML 相关

### `scripts/train_ml_signal_model.py`
**用途：** 训练 ML 信号模型

```bash
python scripts/train_ml_signal_model.py
```

### `scripts/run_ml_replay_compare.py`
**用途：** 比较 ML gate 前后效果

```bash
python scripts/run_ml_replay_compare.py
```

### `scripts/test_permit_replay_ml_gate.py`
**用途：** 测试 ML gate 的许可回放逻辑

```bash
python scripts/test_permit_replay_ml_gate.py
```

---

## 6.5 其它脚本

### `scripts/download_okx_klines.py`
下载行情 K 线。

### `scripts/logic_pressure_test.py`
逻辑压力测试。

### `scripts/profitable_day_drag_report.py`
某日收益拖累分析。

### `scripts/cta_grid_drag_report_localday.py`
CTA / Grid 本地日拖累对比。

### `scripts/write_final_analysis.py`
生成 / 汇总分析结论（按脚本具体内容使用）。

---

# 7. 执行完脚本后去哪里看结果

## 7.1 第一层：终端输出
很多脚本会直接打印摘要。

## 7.2 第二层：日志
主控相关优先看：

```bash
logs/main_controller.log
```

如果是 launchd 路径，还看：

```bash
~/.openclaw/logs/main_controller_launchd.log
~/.openclaw/logs/main_controller_launchd_error.log
```

## 7.3 第三层：数据 / 报告文件
通常在：
- `data/`
- `tmp/`
- 你通过环境变量指定的输出目录

## 7.4 第四层：数据库
核心数据库通常是：
- `data/market_adaptive.sqlite3`

重点表常见有：
- `trade_journal`
- `market_status`
- `strategy_runtime_state`
- `system_state`

---

# 8. 管理后台怎么用

## 8.1 后台最适合做什么

### 1）确认系统是否在线
看：
- 主控是否在线
- CTA / Grid / Risk 状态
- 最近日志是否持续刷新

### 2）看 CTA 驾驶舱 / 诊断数据
重点看：
- family 排名
- blocker 分布
- missed opportunities
- bad releases
- tuning snapshot

### 3）做小步调参
适合改的是：
- confidence floor
- family 自适应速度
- 某个 family 开关
- 某个明显过严 / 过松的 gate

---

## 8.2 后台不是拿来干嘛的

后台不适合在没有证据时做这些事：
- 看见没开单就乱调参数
- 一次改一大堆门槛
- 没搞清楚 blocker 就直接“放宽全部”

---

# 9. 什么时候该调参数，什么时候不要乱动

## 9.1 可以调参数的情况
- 已确认某个 blocker 在系统性错杀
- 某类 family 长期误放 / 错杀已有证据
- 回放、trade journal、dashboard 三者指向同一个问题

## 9.2 不建议调参数的情况
- 只是看到“昨晚没开单”
- 只是觉得“交易数太少”
- 没分清是 `inactive`、`risk_blocked`、`blocker` 还是主控停了

## 9.3 低干预管理者的正确姿势

### 场景 A：长期没开单
排查顺序：
1. 主控还在不在跑
2. 日志有没有持续追加
3. CTA 是 `skip:inactive` 还是 `cta:no_signal`
4. 有没有 `blocked_signal`
5. blocker 是什么

### 场景 B：开单很多但手续费高 / 烂单多
优先收紧：
- confidence floor
- starter / quick trade 质量门槛
- relaxed short 放行条件

### 场景 C：高质量 short 总被错杀
优先看：
- OBV 相关 gate
- RSI threshold
- weak_bull_bias / alignment 链
- raw_direction 是否被压成 0

---

# 10. 如何从模拟盘切到实盘

## 10.1 当前模拟盘相关配置在哪
配置里会看到：

```yaml
okx:
  sandbox: true
  simulated_id: '1'
  simulated_trading: true
```

对应代码：
- `market_adaptive/config.py`
- `market_adaptive/clients/okx_client.py`

默认是模拟盘优先。

---

## 10.2 切到实盘前必须确认的事

1. **API key 权限正确**
   - 是不是实盘 key
   - 有没有下单权限
   - 有没有读取仓位/订单权限

2. **配置文件指向正确**
   - 没有还在用模拟盘 account id
   - 没有混用旧配置

3. **风控参数准备好**
   - 杠杆、单笔风险、最大开仓数量
   - 新开仓阻断条件是否合理

4. **当前策略是否真的值得上实盘**
   - 不要因为“能跑了”就直接上真金白银

---

## 10.3 建议的切换步骤

### 第一步：备份配置
```bash
cp config/config.yaml config/config.yaml.bak
```

### 第二步：修改 OKX 配置
把：

```yaml
sandbox: true
simulated_trading: true
```

改为对应实盘配置（通常是 false）。

### 第三步：检查 API key / secret / passphrase
确认是实盘账户对应的凭据。

### 第四步：缩小风险参数
不要一上来就用大仓位。

### 第五步：重启主控
```bash
./scripts/restart_main_controller.sh config/config.yaml
```

### 第六步：先观察，不要急着追求开单
先确认：
- 主控正常启动
- 风控 heartbeat 正常
- 查询余额、仓位、订单不报错
- 不会出现异常频繁的下单/撤单

---

## 10.4 实盘切换后的首轮观察重点

- 是否能正常获取账户信息
- 是否能正常看到 open orders / positions
- risk heartbeat 是否合理
- 有没有异常 blocker
- 有没有错误重复下单 / 无意义撤单
- 有没有因为参数太松导致立刻乱开仓

---

# 11. 日常维护建议

## 11.1 每天 / 每次上线后建议看

### 1）主控日志是否在写
```bash
tail -f logs/main_controller.log
```

### 2）Risk heartbeat
看：
- `position_notional`
- `open_order_notional`
- `new_openings_blocked`

### 3）CTA 最近 blocker 分布
看最近主要是：
- `OBV_ABOVE_SMA`
- `OBV_STRENGTH_NOT_CONFIRMED`
- `RSI_Threshold`
- `Bullish_Score`
- `Trigger:...`

### 4）Grid 有没有长期不挂单
如果长时间 `skip:inactive`，先判断是：
- 市场状态不适合
- oracle 守门过严
- 还是主控问题

---

## 11.2 改策略前后至少做什么

### 改前
- 记录为什么改
- 记录当前 blocker / family / quality 现象

### 改后
至少做一项：
- 小范围回放
- trade journal 检查
- dashboard 观察
- 定向测试

### 常用测试
```bash
pytest -q tests/test_cta_entry_quality.py
pytest -q tests/test_cta_entry_decider_and_guard.py
pytest -q tests/test_cta_quality.py
pytest -q tests/test_ws_runtime.py
```

---

# 12. 常见问题 FAQ

## Q1：昨晚一整晚没开单，是不是代码没加载？
不一定。先查：
1. 主控是不是在跑
2. 日志是不是在追加
3. CTA 是 `inactive` 还是 `blocked_signal`
4. blocker 是什么

## Q2：没挂单是不是 Grid 坏了？
不一定。先看：
- `grid` worker 是否活着
- `grid` action 是 `skip:inactive` 还是 `oracle_adx_trend_blocked`

## Q3：为什么不要一次改很多参数？
因为改完以后你根本不知道是哪一刀生效，还是哪一刀把系统搞坏。

## Q4：什么时候适合上实盘？
当你已经确认：
- 启动链稳定
- 风控正常
- 回放和诊断结果支持当前策略
- 你愿意接受真实资金风险

## Q5：README、运维手册、AI 手册分别看哪个？
- `README.md` / `README.zh-CN.md`：项目首页说明
- `docs/USAGE_AND_OPERATIONS_MANUAL.md`：人类使用与维护落地手册
- `docs/MAINTENANCE_HUMAN.md`：维护现实判断
- `docs/MAINTENANCE_AI.md`：给 Agent / AI 的维护说明

---

# 13. 排障顺序

## 13.1 用户说：昨晚没开单
按这个顺序查：
1. 主控在不在跑
2. 日志有没有追加
3. CTA 是否长期 `skip:inactive`
4. 有没有 `blocked_signal`
5. 最新 blocker 是什么
6. 有没有 `trade_open`

## 13.2 用户说：Grid 没挂单
先看：
- `grid` worker 是否活着
- `grid` action 是什么
- `grid:higher_timeframe_trend_guard_blocked` 是否大量出现

## 13.3 用户说：改了代码但没效果
先查：
- 当前跑的是哪条启动链
- 是 launchd 版本还是手工前台版本
- 是否已经重启主控
- 日志里是否出现新版本特征字段

## 13.4 用户说：后台显示异常 / 状态不对
先确认：
- admin-api 读的是不是正确日志
- 主控和后台是不是同一套运行环境
- API 是否读到了旧缓存 / 旧状态

---

# 14. 提交代码和推送前注意事项

提交前先看：

```bash
git status --short
```

重点避免误提交：
- `logs/`
- `data/` 里的本地产物
- `.openclaw/`
- `.worktrees/`
- `tmp/`
- memory 或私人文档
- 其它不相关项目文件

---

# 15. 最后一句

维护这套系统时，最容易犯的错是：

> 看到没开单，就以为应该继续放宽。

但更成熟的做法通常是：
- 先看系统是不是活着
- 再看候选有没有成型
- 再看是谁在最后一跳杀掉它
- 最后才决定改哪一刀

也就是：

> **先搞清楚问题在哪一层，再动手。不要把所有问题都当成“阈值太严”。**
