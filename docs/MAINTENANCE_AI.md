# MAINTENANCE_AI.md

给后续 AI / Agent 的维护说明。

## 1. 先搞清楚你在改哪套代码

当前 workspace 里有两份相似目录：
- `market_adaptive/` → **live / 当前主仓实际代码**
- `Market-Adaptive/market_adaptive/` → 曾被用来做副本 / 子目录实验

**默认只改顶层 `market_adaptive/`。**
除非明确需要对副本做实验，否则不要把修复只打到 `Market-Adaptive/market_adaptive/` 那套里。

---

## 2. 当前项目真实状态

### 已经比较成熟的
- Grid 策略主线价值明确
- 风控 / 主控 / 状态恢复 / 通知 / 日志体系可用
- CTA 回测沙盒可用
- 测试体系存在且覆盖面不算差

### 仍然脆弱的
- CTA 高质量信号识别能力
- segmented backtest 全量跑完的稳定性
- 旧副本目录与 live 目录混淆
- 某些配置 dataclass 与测试用例的同步完整性

---

## 3. 当前 CTA 的核心问题

不要再把 CTA 问题理解成“没信号”。
更准确是：

- 前端评分能给出很多“像样”的信号
- 后端过滤链条太长
- 真正能落地且能赚钱的高质量信号不够密集
- 一放松，又容易退化成手续费型烂单

当前最有价值的工作，不是继续无脑加规则，而是：

### 优先做
- 盈利单 vs 亏损单 的开仓前特征对比
- blocker 的错杀分析
- entry mode 的质量分层

### 少做
- 无依据继续新增更多 gate
- 无依据继续大范围放宽 gate
- 在没有新验证结果前宣称 CTA 已显著改善

---

## 4. 近期已加的 CTA 收紧逻辑

在 `market_adaptive/config.py` / `market_adaptive/strategies/cta_robot.py` 中，近期增加了：

### relaxed short 质量约束
- `relaxed_short_minimum_score`
- `relaxed_short_minimum_expected_rr`
- `relaxed_short_max_countertrend_score_gap`
- `relaxed_short_require_early_or_breakdown`

### OBV scalp / quick trade 质量约束
- `obv_scalp_min_bearish_score`
- `obv_scalp_max_bullish_score`
- `obv_scalp_require_early_bearish`
- `obv_scalp_max_positive_obv_zscore`
- `quick_trade_minimum_expected_rr`

### starter / frontrun / scale-in 质量约束
- `starter_quality_minimum_score`
- `scale_in_quality_minimum_score`
- `starter_countertrend_max_score_gap`

### 同向止损冷却
- `same_direction_stop_cooldown_*`

这些调整的目标只有一个：
**减少烂单、降低手续费、提高 starter / relaxed short / quick trade 质量。**

---

## 5. 回测脚本现状

### 可用脚本
- `scripts/cta_backtest_sandbox.py`
- `scripts/run_cta_backtest_segmented.py`

### 注意事项
全量 segmented backtest 容易因运行时间过长被 SIGTERM。

### 已有改进
`run_cta_backtest_segmented.py` 现在支持：
- `CTA_BACKTEST_OUTDIR`
- `CTA_SEGMENT_START`
- `CTA_SEGMENT_END`

建议总是分批跑，不要一上来全 17 段。

推荐方式：
```bash
CTA_BACKTEST_OUTDIR=cta_backtest_segments_batch1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

---

## 6. 测试维护规则

每次改策略逻辑后，至少跑：

```bash
pytest -q tests/test_cta_entry_quality.py
pytest -q tests/test_mtf_engine.py
pytest -q tests/test_ws_runtime.py
```

能跑全量时再跑：

```bash
pytest -q tests
```

如果全量不过，先分清是：
- 新改动引起的
- 旧缺口（例如 config dataclass 缺字段）
- 环境问题

不要把旧问题也误报成“本次改坏”。

---

## 7. 日志排障优先顺序

先看：
1. `logs/main_controller.log` 是否持续写入
2. `[main]` 是否正常启动 / 停机
3. `[grid]` 是 `skip:inactive` 还是 `oracle_adx_trend_blocked`
4. `[risk]` 是否阻止开仓
5. `[cta_robot]` 的 blocker / RR / order flow 日志

如果用户说“昨晚到现在没挂单”：
- 先确认主控是不是停了
- 再确认 grid 是 inactive / blocked / worker exited
- 最后才看是不是策略逻辑改坏

---

## 8. 提交代码时的纪律

只提交当前任务真正相关的文件。
尤其注意不要把这些误带进提交：
- `.openclaw/`
- `.worktrees/`
- `node_modules/`
- `dist/`
- `tmp/`
- memory / 私人文档 / 其他项目目录

提交前先用：
```bash
git status --short
```

---

## 9. 最重要的一条

不要再默认“只要继续打补丁，CTA 最终一定会变强”。

更合理的维护心态是：
- 让系统更可测
- 让信号更可解释
- 让烂单更少
- 让好单更清楚地被识别出来

如果未来证据证明 CTA 仍然不如更简单的旧信号型策略，就应该接受这个结论。
