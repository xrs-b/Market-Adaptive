# CTA Tiered Refactor Execution Guide (2026-04-20)

> 目的：把用户提供的 CTA 改造方案，整理成一份可逐步执行、逐步验收、逐步回滚的实施指引。
>
> 范围：`market_adaptive/strategies/mtf_engine.py`、`market_adaptive/strategies/cta_robot.py`、`market_adaptive/config.py`、相关测试与回测脚本。
>
> 注意：本指南**不等于盲目照抄用户提供的代码片段**。用户方案是方向性蓝图；实施时必须结合现有代码结构、测试约束、字段兼容性和日志链路做适配。

---

## 0. 这次改造的核心目标

当前 CTA 的核心矛盾：

1. `mtf_engine.py` 给出的信号评分过于粗糙，导致：
   - 很多信号“分数够了”，但质量并不高；
   - 分数对最终可交易性的解释力不足；
   - 后端 `cta_robot.py` 被迫用大量串联 gate 去兜底。

2. `cta_robot.py` 当前入场审核链路过长，导致：
   - 判断慢；
   - 拦截率高；
   - 很多明明方向清晰的机会也会被过度审核干掉；
   - 很难区分“高质量机会”和“低质量机会”。

### 这次改造的目标不是：
- 一上来就提升盈利；
- 一上来就提升开单数；
- 一上来就替换所有旧逻辑。

### 这次改造的真正目标是：
- 给 CTA 建立 **统一的信号质量分层**；
- 让高质量信号走 **更短路径**；
- 让低质量信号继续走 **严格路径**；
- 降低“前端评分宽、后端过滤重”的结构性冲突；
- 为后续回测和实盘验证提供更清晰的解释维度。

---

## 1. 总体实施原则

### 1.1 只改 live 主线代码
默认只改：
- `market_adaptive/`

不要把改动只打到：
- `Market-Adaptive/market_adaptive/`

除非明确是做副本实验。

### 1.2 采用“分阶段落地”而不是一次性大换血
本改造必须拆成阶段：

- Phase A：只做 `mtf_engine.py` 的评分与质量标签，不改 CTA 放行逻辑
- Phase B：给 `cta_robot.py` 接入 quality tier 分流，但先保守接入
- Phase C：逐步把 FAST_TRACK / STANDARD / STRICT 的路径差异拉开
- Phase D：回测、日志、验收、收尾

### 1.3 保持向后兼容
必须确保：
- 旧字段仍然存在；
- 旧日志链路尽量不断；
- 旧测试能通过，或在必要时做**有依据**的测试更新；
- 配置新增字段必须全部有默认值。

### 1.4 不在一个 PR 里同时做“逻辑重构 + 策略激进化 + 回测框架改造”
每一步都要可验证。

---

## 2. 实施分解

# Phase A：重构 mtf_engine 评分系统（只加质量标签，不改变交易门）

## A1. 新增质量分层结构
在 `mtf_engine.py` 顶部新增：
- `SignalQualityTier(Enum)`
- `EnhancedSignalScore(dataclass)`

### 目标
建立统一的质量语言：
- `TIER_HIGH`
- `TIER_MEDIUM`
- `TIER_LOW`

### 验收标准
- 枚举和 dataclass 可 import；
- 不影响现有 engine 初始化；
- 相关测试能够加载模块。

---

## A2. 在 `MTFSignal` 中新增字段
新增字段：
- `signal_quality_tier`
- `signal_confidence`
- `signal_strength_bonus`

### 原则
- 默认值必须安全；
- 不破坏现有依赖 `MTFSignal` 的地方；
- 如果测试里手工构造 `MTFSignal`，要同步补默认值或修测试。

### 验收标准
- `build_signal()` 返回对象包含这 3 个字段；
- 不因 dataclass 字段扩展导致旧测试崩掉。

---

## A3. 抽离“基础分”与“强度加分”
### 基础分保留现有主骨架
保留已有：
- trend / supertrend
- RSI
- KDJ memory / urgency
- early / weak bias 等

但把它们收敛成：
- `base_score`

### 强度加分新增
根据用户方案，优先从**当前已有指标可稳定获取的信息**里提取强度，不要先引入太多新计算依赖。

推荐第一版强度来源：
1. major trend 持续 bars（趋势持续性）
2. SuperTrend band position / band width（方向清晰度）
3. execution frame 波动扩张（可先用 range expansion）
4. OBV zscore（已有）

### 注意
用户方案里写了 “ADX/DI/布林”，但如果当前代码没有成熟、稳定、被测试覆盖的 ADX/DI/布林序列，就不要在第一刀里一次性全上。

### 建议落地顺序
- 第一版先做“替代型强度认证”：
  - bars since reversal
  - band clarity
  - range expansion
  - obv strength
- 第二版再决定是否显式引入 ADX / DI / Bollinger width

### 验收标准
- `build_signal()` 日志里可以看到：
  - base score
  - strength bonus
  - total score
  - tier
  - confidence
- 同一批信号在分层上有明显差异，不是全都落到同一层。

---

## A4. 新增 `_compute_signal_quality()`
按用户方案实现，但要做一点现实修正：

### 推荐规则
- `TIER_HIGH`
  - total score >= `tier_high_minimum_score`
  - `bullish_ready/bearish_ready == True`
  - `fully_aligned == True`
- `TIER_MEDIUM`
  - total score >= `tier_medium_minimum_score`
  - ready == True
- `TIER_LOW`
  - 其余

### 建议新增配置项
放进 `CTAConfig`：
- `tier_high_minimum_score: float = 85.0`
- `tier_medium_minimum_score: float = 70.0`
- `tier_high_confidence_threshold: float = 0.8`
- `signal_strength_trend_bonus_cap: float = 5.0`
- `signal_strength_direction_bonus_cap: float = 10.0`
- `signal_strength_volatility_bonus_cap: float = 5.0`
- `signal_strength_obv_bonus_cap: float = 10.0`

### 验收标准
- quality tier 可稳定输出；
- confidence 在 0~1 区间；
- rejection reason 在 TIER_LOW 情况下可读。

---

# Phase B：让 cta_robot 按 quality tier 分流

## B1. 新增 `EntryPathway(Enum)`
在 `cta_robot.py` 中新增：
- `FAST_TRACK`
- `STANDARD`
- `STRICT`

### 目标
让“评分系统”和“执行系统”之间的接口明确化。

---

## B2. 新增 `_resolve_entry_pathway()`
根据质量等级和 `fully_aligned/confidence` 选择路径：

### 初版建议规则
- `TIER_HIGH + fully_aligned + confidence >= tier_high_confidence_threshold`
  - `FAST_TRACK`
- `TIER_MEDIUM`
  - `STANDARD`
- 其余
  - `STRICT`

### 验收标准
日志中能明确看到：
- quality
- confidence
- pathway

例如：
```text
CTA Signal | quality=TIER_HIGH pathway=FAST_TRACK score=89.5 confidence=0.86
```

---

## B3. 重构 `_build_trend_signal()` 为“分层审核入口”
目标不是立刻砍掉全部旧逻辑，而是把当前“大长串判断”拆成：

- `_apply_fast_track_checks(...)`
- `_apply_standard_checks(...)`
- `_apply_strict_checks(...)`

### Phase B 原则
第一版必须：
- **先保留原有逻辑能力**；
- 只是把路径拆出来；
- 严格路径尽量复用当前旧逻辑；
- 快速路径和标准路径先做最小差异。

不要第一刀就把所有旧逻辑推翻，不然测试和实盘行为会一起炸。

---

# Phase C：逐步拉开三条路径的差异

## C1. FAST_TRACK（高质量）
用户目标：
- 只做必要检查
- 快速放行

### 推荐第一版检查
#### 多头
- OBV 不能明显强反向
- 连续 3 根明显 bearish close 时拦一下
- 不做完整 VP / order flow / RR 链式审核

#### 空头
- 对称处理
- 如果 `obv_bias` 明显反向 + zscore 强烈逆向，则 block
- 连续强 bullish bars 可 block

### 注意
FAST_TRACK 不等于“不做风控”。
它只是减少策略层审核，不应绕过账户级风险控制。

---

## C2. STANDARD（中质量）
目标：
- 保留必要质量确认
- 不做所有重检查

### 推荐检查
- OBV（宽松版）
- 价格结构（例如支撑/阻力位置）
- 简化的 POC / VA 检查

### 不建议第一版就做的
- 完整 Order Flow
- 完整 RR 链式审核
- 太多 secondary blockers

---

## C3. STRICT（低质量）
目标：
- 基本保留现有完整链路
- 作为低质量信号的保守兜底

### 包含
- OBV
- Volume Profile / Value Area / POC
- Order Flow
- RR
- 其他现有风控辅助逻辑

### 说明
这条路径本质上是旧系统逻辑的“保守保留区”。

---

# Phase D：简化 `_open_position()`

## D1. 不要一下子砍掉所有现有保护逻辑
用户给了一个极简 `_open_position()` 草案，但实际实施时要谨慎。

### 建议保留
- 风险管理的最后一层 notional / size 约束
- amount normalize
- fill ratio 检查
- 持仓状态构建

### 可以简化的
- 不要把过多策略层判断堆在 `_open_position()` 里
- `_open_position()` 应该更多承担“执行”而不是“再判断一次信号”

### 推荐目标
- `_build_trend_signal()` 负责决定“是否值得进”
- `_open_position()` 负责“怎么执行进场”

---

## D2. RR 检查怎么处理
用户方案里想让高质量信号跳过大部分检查，但 RR 完全取消风险太大。

### 推荐折中
- `FAST_TRACK`：只保留 very lightweight RR 下限（例如更宽松）
- `STANDARD`：保留中等 RR
- `STRICT`：保留完整 RR 逻辑

也就是说，**不是全取消，而是分层差异化**。

---

## D3. Order Flow 怎么处理
### 推荐策略
- `FAST_TRACK`：默认跳过或只做 hard reject
- `STANDARD`：可选轻量检查
- `STRICT`：保留原完整检查

这样更符合“高质量信号走短路径”的目标。

---

## 3. 建议的实际执行顺序

### Step 1
只改 `mtf_engine.py`：
- 新增 tier / confidence / strength bonus
- 不改 cta_robot 路径
- 先确保测试和日志都稳定

### Step 2
在 `cta_robot.py` 中新增 pathway 选择和日志，但：
- `FAST_TRACK/ STANDARD/ STRICT` 先都调用旧逻辑
- 确认分流稳定

### Step 3
逐步把 `FAST_TRACK` 从旧逻辑中“剥离”出来
- 先减少 VP / order flow 依赖
- 再减少 RR 依赖

### Step 4
逐步把 `STANDARD` 做成中间态
- 宽松 OBV
- 简化结构检查

### Step 5
保留 `STRICT` 作为旧逻辑基线

### Step 6
跑分段回测 / 实盘日志验证

---

## 4. 需要新增/修改的测试

## 4.1 mtf_engine 侧
新增测试：
- `build_signal()` 包含 `signal_quality_tier`
- high / medium / low tier 判定正确
- confidence 区间正确
- strength bonus 不会无限放大

## 4.2 cta_robot 侧
新增测试：
- `TIER_HIGH` 走 `FAST_TRACK`
- `TIER_MEDIUM` 走 `STANDARD`
- `TIER_LOW` 走 `STRICT`
- high quality 信号不会再被完整 strict 链路误伤
- strict path 仍保持旧逻辑基线行为

## 4.3 回归测试
保留现有关键测试：
- starter / frontrun / scale-in
- relaxed short / OBV scalp
- risk control
- signal profiler
- ws runtime

---

## 5. 回测与验证策略

## 5.1 不要再直接跑整轮 17 段
当前 segmented backtest 容易被 SIGTERM 中断。

### 推荐跑法
分批：
```bash
CTA_BACKTEST_OUTDIR=cta_tier_refactor_batch1 \
CTA_SEGMENT_START=1 \
CTA_SEGMENT_END=4 \
python scripts/run_cta_backtest_segmented.py
```

然后批次推进：
- 1~4
- 5~8
- 9~12
- 13~17

---

## 5.2 验收指标
### 核心不要只看 pnl
要一起看：
- `opened_actions`
- `total_trades`
- `total_fees_paid`
- `blocked_obv`
- `directional_ready`
- 各 pathway 数量
- 各 pathway 胜率 / 盈亏比

### 新增建议统计
后续可以考虑把回测诊断里增加：
- `fast_track_signals`
- `standard_signals`
- `strict_signals`
- `fast_track_opened`
- `standard_opened`
- `strict_opened`

---

## 6. 实施中的风险点

## 风险 1：质量分层和现有 score 语义不一致
当前 score 本身就有历史包袱，所以一旦硬套 tier，可能出现：
- 看起来高分，其实质量不高
- 看起来低分，其实是可交易机会

### 对策
第一版先把 tier 当“实验性解释层”，不要立即把它当绝对真理。

## 风险 2：FAST_TRACK 放得太猛
如果直接跳过太多检查，可能会短期提升开单数，但带来烂单暴增。

### 对策
FAST_TRACK 第一版只删最重的链路，不是删所有保护。

## 风险 3：STANDARD 路径定义不清，变成另一个 STRICT
如果 STANDARD 又塞进太多判断，就失去意义。

### 对策
明确 STANDARD 只保留：
- OBV
- 结构
- 轻量位置检查

## 风险 4：测试大量失败
`MTFSignal`、`TrendSignal`、`signal_profiler`、`the_hands`、`main_controller` 这些测试都很容易受影响。

### 对策
每一步都小步提交，小步测。

---

## 7. 推荐执行 checklist

## Phase A checklist
- [ ] 新增 `SignalQualityTier`
- [ ] 新增 `EnhancedSignalScore`
- [ ] `MTFSignal` 增加 3 个字段
- [ ] `build_signal()` 输出 quality / confidence / strength bonus
- [ ] 日志新增 quality 信息
- [ ] 相关单测通过

## Phase B checklist
- [ ] 新增 `EntryPathway`
- [ ] 新增 `_resolve_entry_pathway()`
- [ ] `_build_trend_signal()` 分为 3 条路径
- [ ] 日志打印 pathway
- [ ] 暂时仍可保持旧逻辑兜底

## Phase C checklist
- [ ] `FAST_TRACK` 最小化检查完成
- [ ] `STANDARD` 轻量检查完成
- [ ] `STRICT` 保留旧逻辑
- [ ] 回归测试通过

## Phase D checklist
- [ ] `_open_position()` 角色收缩到执行层
- [ ] 重检查从 `_open_position()` 移除
- [ ] amount / fill / risk 逻辑仍完整
- [ ] 回测脚本分批验证

---

## 8. 建议的落地顺序（我们后续就按这个来）

### 第 1 轮
只做 `mtf_engine.py` 的 quality tier 改造。

### 第 2 轮
让 `cta_robot.py` 接入 pathway，但先不大砍旧逻辑。

### 第 3 轮
逐步把 FAST_TRACK / STANDARD / STRICT 真正拉开。

### 第 4 轮
补日志、补测试、补回测统计。

### 第 5 轮
分批回测和实盘日志对照验收。

---

## 9. 后续协作约定

后面每一步都按以下格式推进：

1. 先说明本轮只做哪一小步
2. 修改代码
3. 跑对应测试
4. 给出结果
5. 再决定下一步

不要一口气把整份方案一次性改完。

---

## 10. 本文档结论

用户给的方案方向是对的，核心价值在于：
- **把“信号质量”前置到 `mtf_engine`**
- **把 `cta_robot` 从“一刀切长审核链”改成“按质量分流”**

但实施上必须：
- 分阶段
- 保持兼容
- 严格测试
- 分批回测

否则很容易把 CTA 再次改成“逻辑更复杂，但结果更不可控”。
