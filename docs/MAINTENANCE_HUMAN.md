# MAINTENANCE_HUMAN.md

给后续人类维护者的项目说明。

## 1. 这个项目现在是什么状态

Market-Adaptive 不是“没做完”，而是已经有一整套能运行的交易框架：
- 主控
- 风控
- 网格
- CTA
- 日志
- 通知
- 回测沙盒

但这不代表每个模块都已经成熟。

### 当前最稳的部分
- **Grid**：目前仍然是最有明确正反馈的盈利模块。
- **基础设施**：主控、风控、日志、状态恢复、通知，都具备继续迭代价值。

### 当前最不稳的部分
- **CTA**：已经做了很多轮调整，但还没有稳定证明自己优于更简单的旧信号策略。

---

## 2. 目前最重要的现实判断

不要被“CTA 逻辑很多”误导。
逻辑多，不等于策略强。

当前 CTA 的实际问题是：
- 会产生很多信号
- 但高质量信号不足
- 过滤链过长，容易错杀
- 一旦放宽，又容易做出烂单
- 手续费拖累明显

所以最近维护重点已经转向：
- 减少烂单
- 压手续费
- 提高 relaxed short / starter / quick trade 的质量

---

## 3. 仓库结构里最容易踩的坑

当前 workspace 里可能同时看到：
- `market_adaptive/`
- `Market-Adaptive/market_adaptive/`

真正当前主仓、应该优先维护的是：

```text
/Users/oink/.openclaw/workspace/market_adaptive/
```

除非明确是做副本实验，否则不要只改到另一个目录里。

---

## 4. 关键文件在哪里

### 主控
- `market_adaptive/controller.py`

### 配置定义
- `market_adaptive/config.py`

### CTA
- `market_adaptive/strategies/cta_robot.py`
- `market_adaptive/strategies/mtf_engine.py`
- `market_adaptive/strategies/obv_gate.py`
- `market_adaptive/strategies/order_flow_sentinel.py`

### Grid
- `market_adaptive/strategies/grid_robot.py`
- `market_adaptive/strategies/dynamic_grid_robot.py`

### 风控
- `market_adaptive/risk.py`

### websocket runtime
- `market_adaptive/ws_runtime.py`

### 回测脚本
- `scripts/cta_backtest_sandbox.py`
- `scripts/run_cta_backtest_segmented.py`

### 测试
- `tests/`

---

## 5. 现在怎么判断项目有没有跑正常

### 第一看日志
主日志：
```text
logs/main_controller.log
```

如果用户说：
- 没挂单
- 没开 CTA
- 网格一直没动作

先看日志是不是还在追加。

### 第二看 grid 的 action
常见状态：
- `skip:inactive`
- `grid:oracle_adx_trend_blocked`
- 高周期趋势守门 block

### 第三看 risk heartbeat
关注：
- `open_order_notional`
- `position_notional`
- `new_openings_blocked`

有时候不是策略坏了，而是系统已经停了，或者被风控拦住了。

---

## 6. 当前 CTA 最近加了什么

最近对 CTA 做的不是“更激进”，而是**更保守**：

### 已增加
- relaxed short 的质量门槛
- quick trade / OBV scalp 的收紧
- starter / frontrun / scale-in 的质量门槛
- same-direction stop cooldown

### 目的
不是增加交易数，而是：
- 少做不值得做的单
- 减少被手续费吃掉的单
- 提高放行信号的平均质量

---

## 7. 回测怎么跑比较稳

全量 17 段 segmented backtest 容易因为时间过长被系统杀掉。

现在脚本已经支持按段范围跑：

```bash
CTA_BACKTEST_OUTDIR=cta_backtest_segments_batch1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

建议：
- 先跑 1~4 段
- 再跑 5~8 段
- 分批汇总

不要再赌一次性全跑完。

---

## 8. 维护时最该盯住的指标

### CTA
- 总交易数是否下降
- 手续费是否下降
- 已实现亏损是否下降
- 高质量信号占比是否上升
- starter / quick trade 是否明显更少更精

### Grid
- 是否恢复稳定挂单
- 长时间不挂单时，是 inactive、oracle blocked，还是主控停了

---

## 9. 提交代码前要注意什么

提交前请先确认：
- 只提交主仓相关文件
- 不要把私人 workspace 文件、缓存、node_modules、tmp、memory 之类一起推上去

推荐先看：
```bash
git status --short
```

---

## 10. 这项目接下来最理性的路线

### 对 Grid
继续保住盈利能力，别轻易破坏。

### 对 CTA
不要再默认“再多打几个补丁就一定能成”。
更现实的做法是：
- 用回测和实盘日志继续确认哪些单真的赚钱
- 找出哪些 blocker 在错杀
- 找出哪些 entry mode 天生质量差
- 用证据删逻辑，而不是只会加逻辑

如果未来证据持续表明 CTA 不如更简单的旧信号策略，那就应该接受这个结论。
