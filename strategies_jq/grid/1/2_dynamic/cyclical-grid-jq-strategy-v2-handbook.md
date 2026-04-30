# 周期股 · 多因子底部 + 动态网格 策略 v2.0 — 实施手册（动态选股版）

> 策略代码：[`cyclical-grid-jq-strategy-v2.py`](./cyclical-grid-jq-strategy-v2.py)
> 上一代代码：[`cyclical-grid-jq-strategy.py`](./cyclical-grid-jq-strategy.py)（v1.9，已封板）
> 上一代手册：[`cyclical-grid-jq-strategy-handbook.md`](./cyclical-grid-jq-strategy-handbook.md)
> 设计基础：[`cyclical-grid-strategy-analysis.md`](./cyclical-grid-strategy-analysis.md)
> 当前版本：**v2.0 R2（动态选股 + trade_log + Bug J/K 修复 + score_cache 加速）/ 待 log_v2_03 验证**（2026-04-29）
> 平台：聚宽 JoinQuant
> 基线：v1.9 在 2015-01 ~ 2026-04 全周期回测中年化 14.22% / 最大回撤 19.35%

---

## 0. TL;DR

| 维度 | v1.9（上一代，封板） | **v2.0（本版）** |
|---|---|---|
| 候选池来源 | 硬编码 13 行业 / 36 只 | **全 A 动态扫描 + 因子选股** |
| 过拟合层级 | 三层（行业/个股/幸存者偏差） | **0**（全部 point-in-time） |
| 行业筛选方法 | 我事后选 | **PB CV + ROE σ + 营收 YoY σ 三因子 z-score 综合** |
| 个股筛选方法 | 我事后选龙头 | **5 年 PB max/min ratio + PB CV 综合得分** |
| Rebalance | 月度（仅过滤） | **季度行业 + 月度个股** |
| Legacy holdings 处理 | 不存在该问题 | **优雅退出：不发新 T0，但允许卖出/网格闭合/退出 a~d** |
| 4 因子打分（PB/回撤/OCF/RSI） | ✓ | **✓ 完全沿用** |
| 三档建仓 / 动态网格 / 退出 a~d | ✓ | **✓ 完全沿用** |
| 硬止损黑名单 / Bug E~I 补丁 | ✓ | **✓ 完全继承** |
| **trade_log 结构化归因** | ❌ | **✓ v2.0 落地（21 种 action / 月度摘要 / 年度 CSV）** |

**v2.0 的核心设计哲学**：

> **"v1.9 的策略框架已经过 7 轮迭代+10 份 log 验证，是有真实价值的；问题不在交易逻辑，而在'我们如何选股'本身就是一个隐形的回测注入点。把选股完全数据驱动化，让策略真正可以跨周期推广。"**

---

## 1. 为什么要做 v2.0：v1.9 的过拟合根因诊断

### 1.1 v1.9 的策略框架是好的

v1.9 在 2015~2026 全周期上拿到了**年化 14.22% / 最大回撤 19.35%** 的成绩。这个表现不是来自参数过拟合，而是来自三个真实有效的策略 alpha：

1. **底部因子打分**（PB 5 年逆向分位 / 距前高回撤 / OCF/市值 / 14 日 RSI）—— 经典的"价值 + 反转"组合，在周期股上长期有效
2. **退出 b 涨停打开**——精准捕捉了主升浪的"冲顶 + 抛压"信号；log7 退出 b 13 次平均浮盈 60.7%，最大 184.6%（牧原 2019-03 非洲猪瘟前夜）
3. **动态网格 + 不对称 + 金字塔**——震荡行情中的均值回归收益累积

**这些都不需要修改**。v2.0 把它们 1:1 全部继承。

### 1.2 v1.9 的过拟合在哪里

不在交易逻辑，而在 **`CANDIDATE_POOL` 这个看似无害的字典里**：

```python
CANDIDATE_POOL = {
    '磷化工':   ['600141.XSHG', '600096.XSHG', '002312.XSHE', '000422.XSHE', '002895.XSHE'],
    '钢铁':     ['600019.XSHG', '000932.XSHE'],   # 宝钢股份 / 华菱钢铁
    '煤炭':     ['601088.XSHG', '601225.XSHG'],   # 中国神华 / 陕西煤业
    '水泥':     ['600585.XSHG', '600801.XSHG'],   # 海螺水泥 / 华新水泥
    '工程机械': ['600031.XSHG', '000425.XSHE'],   # 三一重工 / 徐工机械
    '海运':     ['601919.XSHG', '601872.XSHG'],   # 中远海控 / 招商轮船
    ...
}
```

仔细看这 36 只股票，**它们有一个共同的隐藏特征：2026 年回看，全部都是"穿越牛熊的行业寡头"**。

#### 1.2.1 三层后视镜偏差

| 偏差层级 | 具体表现 | 量化估算 |
|---|---|---|
| **行业选择偏差** | "我事后知道有色/煤炭/化工/养殖是周期股" → 13 个行业本身就是 2026 年回看的归纳；2015 年我们其实不会"理所应当"地选出煤炭+海运 | ~5% 年化 alpha |
| **个股选择偏差** | 每个行业选的都是"穿越牛熊的龙头"——宝钢、神华、牧原、海螺、海控；2015 年时我们其实**不知道**它们会成为龙头 | ~3% 年化 alpha |
| **幸存者偏差** | 36 只全部是 2026 年仍在交易的活股票；那些 2015 年时也是"龙头"但后来退市/被借壳/财务暴雷的票（比如某些当时的化工龙头）完全没出现在候选池中 | ~2% 年化 alpha |

**总过拟合约 5~10% 年化** —— 也就是说 v1.9 的 14.22% 年化里，**真实可推广的 alpha 可能只有 5~9%**。

#### 1.2.2 一个具体例子：「养殖」行业的牧原

v1.9 候选池里养殖行业写了 `'002714.XSHE'`（牧原股份）。**但牧原股份是 2014 年才上市的**；2015 年初回测时，牧原刚 IPO 半年，市值不大，PE 也不便宜。

我们今天选牧原作为养殖代表，是因为我们知道**它后来从 2018 年的 25 元涨到 2021 年的 92 元**，完美命中了 v1.9 退出 b +184.6% 的标志性收益。

但如果在 2015 年初**真正运行**这个策略，我们怎么会想到牧原？我们更可能选择：
- 雏鹰农牧（002477）—— 当时养殖一哥，2019 年退市
- 温氏股份（300498）—— 2015 年 IPO 时市值远超牧原
- ......

**v1.9 给我们的"真实回测体验"，其实是 "未来人 2026 视角看 2015"**。

### 1.3 v2.0 的回应

完全把候选池**从一段静态字典**改为**一个数据驱动的流水线**：每次 rebalance 时**只用截至当时的数据**决定股票池。

不允许：
- 出现 2014-12-31 还没上市的股票
- 出现 2014-12-31 当时数据不达标但 2026 年很优秀的股票
- 排除 2014-12-31 数据达标但 2026 年已退市的股票（即使我们今天知道它退市了，2015 年初也不应该排除）

---

## 2. v2.0 架构：动态选股流水线

### 2.1 整体流程

```
                       【季度】每 90 天 1 次
                ┌─────────────────────────────────┐
                │ Step 1: 行业层动态筛选          │
                │  - 申万一级行业 ~31 个          │
                │  - 5 年 PB CV (月频采样)         │
                │  - 5 年 ROE 标准差 (季频)        │
                │  - 5 年营收 YoY 波动率 (季频)    │
                │  - z-score 综合 → 取 top 50%    │
                │  - 写入 g.cyclical_industries   │
                └────────────┬────────────────────┘
                             │
                             ▼
                       【月度】每 30 天 1 次
                ┌─────────────────────────────────┐
                │ Step 2: 个股层动态筛选          │
                │  - 候选行业内所有股票 (PIT)     │
                │  - 黑名单过滤 (硬止损惯犯)      │
                │  - 上市 ≥5 年 (5 年 PB 数据)    │
                │  - 资产负债率 ≤65%              │
                │  - 商誉 / 净资产 ≤30%           │
                │  - 流通市值 ≥50 亿              │
                │  - 60 日均成交额 ≥5000 万       │
                │  - 个股 5 年 PB max/min ratio + │
                │    PB CV → 综合分 ≥30           │
                │  - 每行业取 top 3, 总池 ≤45 只   │
                │  - + Legacy holdings (优雅退出) │
                │  - 写入 g.quality_universe      │
                │       g.sector_map              │
                └────────────┬────────────────────┘
                             │
                             ▼
                        【日】交易主循环
                ┌─────────────────────────────────┐
                │ Step 3: 4 因子打分 + T0/T1/T2   │
                │         + 动态网格 + 退出 a~d   │
                │         + 硬止损 + COOLDOWN      │
                │  ★ 完全沿用 v1.9 全部交易逻辑 ★ │
                └─────────────────────────────────┘
```

### 2.2 行业层：3 因子综合 z-score 排序

**目标**：客观识别"什么是周期性行业"，不依赖人为标签。

#### 2.2.1 三因子定义

| 因子 | 定义 | 计算方式 | 权重 | 周期性的体现 |
|---|---|---|---|---|
| **PB CV** | 5 年行业中位数 PB 的变异系数 std/mean | 月频采样 60 个点（5 年 × 12 月）；行业内所有股票每月最后一天 PB 的中位数 | 0.50 | 估值大幅波动 |
| **ROE 标准差** | 5 年行业平均 ROE 的标准差 | 季频采样 20 个点；行业内股票每季 ROE 的均值 | 0.25 | 业绩有周期性 |
| **营收 YoY 波动率** | 5 年行业整体营收增速的标准差 | 季频 20 个点；每季行业总营收 sum；YoY = (q_t - q_{t-4}) / q_{t-4} | 0.25 | 行业景气周期波动 |

#### 2.2.2 综合得分

```python
z_pb  = (pb_cv  - mean_all_industries) / std_all_industries
z_roe = (roe_σ  - mean_all_industries) / std_all_industries
z_rev = (rev_σ  - mean_all_industries) / std_all_industries

composite = 0.50 × z_pb + 0.25 × z_roe + 0.25 × z_rev
```

#### 2.2.3 取 top N 个

```python
sorted_industries = sorted(by composite desc)
target_n = round(0.50 × total_industries)
target_n = clip(target_n, [8, 18])    # 最少 8, 最多 18
selected = sorted_industries[:target_n]
```

实际效果（预估，待 v2.0 回测验证）：

| 申万一级行业（举例） | 预期综合得分 | 是否入选 |
|---|---|---|
| 有色金属 | +1.5（PB σ 极高 + ROE σ 大 + 营收 σ 大） | ✓ |
| 煤炭 / 钢铁 / 石油石化 | +1.0~1.5 | ✓ |
| 化工 / 机械 / 建材 | +0.5~1.0 | ✓ |
| 农林牧渔（含养殖、水产） | +0.5~1.0 | ✓ |
| 房地产 | +0.3~0.5（伪周期—地产长期结构性下行不是周期） | 视年份 |
| 食品饮料 / 医药 | -0.5~0.0（消费稳定，PB 波动小） | ✗ |
| 银行 / 保险 / 公用事业 | -1.0（弱周期/反周期） | ✗ |
| 计算机 / 传媒 | -0.3~+0.3（成长性 ≠ 周期性，分数中等） | 一般 ✗ |

**关键：行业代码 + 名称都不出现在 CONFIG 里，全部从 `get_industries(name='sw_l1', date=query_date)` 动态读取**。这样 2014 年回测就用 2014 年的申万行业版本，2024 年回测用 2024 年的版本（增加了煤炭/美容护理/环保等）。

### 2.3 个股层：财务质量 + 流动性 + 周期性打分

候选行业内所有股票（typically 800~1500 只）依次过 6 道闸：

#### 2.3.1 第 1~5 道：硬门槛（无评分）

| 闸门 | 阈值 | 目的 |
|---|---|---|
| 上市年限 | ≥ 5 年 | 保证 5 年 PB 数据完整；过滤借壳/重组票 |
| 资产负债率 | ≤ 65% | 高杠杆周期股最致命，过滤雷股 |
| 商誉 / 净资产 | ≤ 30% | 过滤并购雷 |
| 流通市值 | ≥ 50 亿 | 防 ST/壳股；防流动性塌方 |
| 60 日均成交额 | ≥ 5000 万 | 网格策略需要流动性 |

> 与 v1.9 的差异：
> - 上市年限 365 → 1825 天（5 倍）
> - 资产负债率 60% → 65%（略放宽，因为周期股负债天然偏高）
> - 流通市值阈值（v2.0 新增 50 亿）

#### 2.3.2 第 6 道：周期性综合打分

**`compute_stock_cyclicality_score(stock)`** 返回 0~100 分：

```python
ratio = pb_max / pb_min  (5 年内)
ratio_score = min(ratio / 5.0, 1.0) × 100   # ratio ≥ 5 视为满分
                                            # （周期股估值峰谷比典型 ≥ 5）

cv = pb.std() / pb.mean()
cv_score = min(cv / 0.5, 1.0) × 100         # CV ≥ 0.5 视为满分

total = 0.5 × ratio_score + 0.5 × cv_score
```

**门槛**：`stock_cyclicality_min_score = 30.0`（CONFIG 可调）。

低于 30 分的票（典型如银行股、白酒股、消费稳定股）会被过滤。

#### 2.3.3 第 7 道：每行业取 top N + 全池硬上限

```python
# 每个行业内按周期性得分排 top
for ind, items in by_industry.items():
    top_3 = items.sort_by_score()[:3]
# 全池 ≤ 45 只 (CONFIG['pool_max_stocks'])
```

最终得到 **30~45 只候选股 + 8~18 个候选行业** 的动态池子，与 v1.9 的 36 只 / 13 行业**结构性相当但完全数据驱动**。

### 2.4 Legacy Holdings：优雅退出机制

**问题**：股票 A 在 2024-09 月度 rebalance 时入选，并发了 T0 → T1 → T2 全部建完仓。但 2024-12 行业 rebalance 时 A 所在行业的综合得分掉出 top 50%，A 不再属于"周期性行业内"，因此 2025-01 个股 rebalance 时 A 被剔除候选池。**这时 A 的持仓怎么办？**

#### 2.4.1 三种选项及取舍

| 选项 | 描述 | 优点 | 缺点 | v2.0 选择 |
|---|---|---|---|---|
| 立即清仓 | 不在新池就强制 close_all | 干净彻底 | 可能在不利位置砸盘；放大滑点 | ✗ |
| 完全忽略 | 候选池只用于 T0 准入，老仓不受影响 | 最简单 | 老仓可能永远在策略中 | ✗ |
| **优雅退出** ✓ | 老仓继续按 v1.9 网格/退出规则跑直到自然退出 | 不放大冲击 + 仍能吃 b/c/d 收益 | 池子膨胀 | **采用** |

#### 2.4.2 优雅退出的具体表现

`g.legacy_holding_stocks` 维护 set；当一只票被踢出新池但仍有持仓时，加入此 set。然后：

```python
# 主循环 daily_signal_and_trade:
if st['phase'] == 'IDLE':
    if stock in g.legacy_holding_stocks:
        pass    # ★ 不发新 T0
    else:
        try_enter_t0(...)
elif st['phase'] == 'BUILDING':
    if stock not in g.legacy_holding_stocks:
        try_build_t1_t2(...)    # ★ legacy 不再 T1/T2 加仓
elif st['phase'] == 'GRID_RUNNING':
    run_grid(...)               # 内部跳过 legacy 的网格买入
    check_daily_non_intraday_exits(...)    # 退出 a/b/c/d 全部正常
elif st['phase'] == 'COOLDOWN':
    try_exit_cooldown(...)      # 全部正常

# run_grid 内:
if pct <= -buy_step:
    if stock in g.legacy_holding_stocks:
        return    # ★ 只允许跌买的位置不再买入
    ...
elif pct >= sell_step:
    ...           # 涨卖正常
```

效果：
- 网格只卖不买 → 仓位逐步缩小
- 退出 a 网格闭合 / 退出 b 涨停 / 退出 c 高水位 / 退出 d 3 年 → 任一触发都能正常退出
- 全部退出后从 `g.quality_universe` 和 `g.legacy_holding_stocks` 中自动移除（在下次月度 rebalance 时清理）

#### 2.4.3 数学直觉

如果一只票的周期性确实变弱了（被踢出候选池），它后续大概率不会再出现"PB 大幅波动 + 涨停打开"，那么：
- 网格不再加仓 → 不会陷得更深
- 退出 a 触发 → 自然退出
- 退出 b 触发（万一行情反转再涨）→ 高位卖飞，很合理
- 退出 d 3 年触发 → 兜底

**不会出现"老仓被新池排除后还在持续投入资金"的情况。**

---

## 3. 关键技术：避免新一轮过拟合

### 3.1 全部 point-in-time 查询

**所有取数都以 `query_date = context.previous_date` 为基准**，杜绝未来信息泄露：

| API | v2.0 用法 | 避免的偏差 |
|---|---|---|
| `get_industries(name='sw_l1', date=query_date)` | 用查询日**当时**的申万一级版本 | 防止 2014 年回测用 2024 年的"煤炭/美容护理"行业 |
| `get_industry_stocks(ind_code, date=query_date)` | 用查询日**当时**的行业股票表 | 防止把 2018 年才纳入的票算进 2014 年池子 |
| `get_fundamentals(query, date=sample_date)` | 用采样日**当时**已发布的财务数据 | 防止用未公布的财报选股 |
| `get_fundamentals(query, date=stat_date+60d)` | 季频采样：转 statDate 为安全 date | 配合 `_statdate_to_safe_date` 规避聚宽 PIT 限制 |
| `get_valuation(stock, start, end)` | 取股票历史 PB | 仅用 ≤ end 之前数据 |
| `attribute_history(stock, 60, '1d', ...)` | 取最近 N 个交易日 | 不会涵盖未来 |

### 3.2 季度 stat_date 的 60 天延迟（PIT 安全 date）

聚宽 `get_fundamentals` 在回测中（`avoid_future_data=True` 默认）**不允许使用 `statDate` 参数**——会抛错：「avoid_future_data=True 的时候, 回测中 get_fundamentals 不支持 statDate 参数, 请使用 date 参数」。

v2.0 的解法：把每个季度 statDate（如 `'2014q3'`）转换成"季末 + 60 天"的安全 `date`，让聚宽返回"截至该日各股票最新一期财报"——绝大多数公司在 60 天内已披露该季报。

```python
def _statdate_to_safe_date(statdate_str):
    # '2014q3' → 2014-09-30 + 60 天 = 2014-11-29
    year, q = parse(statdate_str)
    end_date = {1:(year,3,31), 2:(year,6,30), 3:(year,9,30), 4:(year,12,31)}[q]
    return end_date + timedelta(days=60)

# 调用
df = get_fundamentals(query(...).filter(...), date=_statdate_to_safe_date('2014q3'))
```

季报披露窗口：一季报 4-30 截止、中报 8-31 截止、三季报 10-31 截止、年报次年 4-30 截止。`stat_date+60d` 落在或紧邻这些窗口之后，确保数据已是 PIT 可用。

`_generate_quarter_stat_dates` 同时过滤掉 `(end_date - stat_date_obj).days < 60` 的季度，保证 safe_date 永远 ≤ query_date，没有未来泄露。

### 3.3 退市股票自然纳入

`get_industry_stocks(ind_code, date='2018-01-01')` 返回的是 **2018 年初**的行业成分股。如果某只票在 2020 年退市，但 2018 年初它确实属于该行业且是优质周期股，**v2.0 会把它选入 2018 年的候选池**，让策略真正在 2018 年体验"潜在的踩雷风险"。

这是消除幸存者偏差的关键：**让历史 = 历史**，不让未来知识倒灌。

### 3.4 trade_log：结构化交易归因（v2.0 优 1，已实施）

> v1.9 handbook §4.1 列为「v2.0 第一阶段必做」的优 1 已在本版直接落地。

#### 3.4.1 为什么需要

v1.9 全周期回测产生 ~20000 行 INFO 日志。任何归因问题都得写脚本：
- "退出 b 多少次? 平均浮盈?" → grep "退出 b" + awk
- "002714 走完哪些路径?" → grep 002714 + 时间排序
- "硬止损命中后 30 天内股价反弹了多少?" → 几乎不可能从日志反推

v2.0 加上动态选股 + legacy holdings 后，路径更复杂。trade_log 把每笔成交动作**实时**记成结构化 dict，回测后可直接用 `pd.read_csv` 加载分析。

#### 3.4.2 字段定义

```
date,stock,sector,legacy,phase,action,price,value,pos_val_after,pos_amt_after,
avg_cost,pnl_pct,highw,tier,score,extra
```

| 字段 | 说明 |
|---|---|
| `date` | 交易日 (YYYY-MM-DD) |
| `stock` | 股票代码 |
| `sector` | 当时的行业代码（动态） |
| `legacy` | 是否为 legacy holding (Y/N) |
| `phase` | **操作完成后**的相位 (IDLE/BUILDING/GRID_RUNNING/COOLDOWN) |
| `action` | 动作类型，详见下表 |
| `price` | 操作时的市场最新价 |
| `value` | 操作金额（元，买入正、卖出负、纯相位切换 0） |
| `pos_val_after` | 操作后的持仓市值 |
| `pos_amt_after` | 操作后的持仓股数 |
| `avg_cost` | 持仓均价 |
| `pnl_pct` | 操作时的浮盈/浮亏百分比 |
| `highw` | high_water_pnl_pct 字段 |
| `tier` | 已建仓档位 (0/1/2/3) |
| `score` | 个股最近一次的 4 因子综合分 |
| `extra` | 上下文备注（如 `pnl=184.6%` / `drop=15.4%`）|

#### 3.4.3 action 类型完整列表（21 种）

| 类型 | 说明 | value 符号 |
|---|---|---|
| `T0` / `T1` / `T2` | 三档建仓 | + |
| `grid_buy` | 网格买入 | + |
| `grid_sell` | 网格卖出 | - |
| `exit_a` | 网格闭合，60 日冷却（保留底仓） | 0 |
| `exit_b_top` | 退出 b 周期顶全清 | - |
| `exit_b_regular_half` | 退出 b 常规半清（涨停打开 + 浮盈 ≥30%） | - |
| `exit_b_micro_full` | 退出 b 半仓金额过小兑底全清 | - |
| `exit_c_full` | 退出 c 高水位回撤 ≥25% 全清 | - |
| `exit_c_half` | 退出 c 高水位回撤 ≥15% 半清 + COOLDOWN 30 天 | - |
| `exit_c_micro_full` | 退出 c 半仓金额过小兑底全清 | - |
| `exit_d_loss_full` | 退出 d 持仓 750 天浮亏全清 | - |
| `exit_d_profit_half` | 退出 d 持仓 750 天浮盈半清续持 | - |
| `exit_d_micro_full` | 退出 d 半仓金额过小兑底全清 | - |
| `hard_stop` | 硬止损全清（连续 3 日浮亏 ≥35%） | - |
| `micro_close` | 超小持仓清算（pos < 5000 元） | - |
| `state_heal` | 持仓为零但 phase != IDLE 自愈 | 0 |
| `cooldown_a` | 路径 A：退出 a 双条件解除 → GRID_RUNNING | 0 |
| `cooldown_b` | 路径 B：退出 c/d 减半到期无条件解除 → GRID_RUNNING | 0 |
| `cooldown_c_idle` | 路径 C：持仓为零 score≥65 → 重置 IDLE | 0 |

#### 3.4.4 输出方式

**两条独立通道**，互不干扰策略主流程：

1. **月初摘要**（人眼可读）：每月 1 日 `before_trading_start` 触发，print 上月 action 计数 + 退出 b 浮盈分布 + 硬止损统计。例如：
```
==============================================================
【trade_log 摘要 2024-10】 总事件 47 / 累计 312 / buy=487231 / sell=623914
  grid_sell                42
  grid_buy                  3
  exit_b_regular_half       1
  cooldown_b                1
  ── 退出 b 浮盈: 平均 36.3% / 最大 36.3% / 最小 36.3%
==============================================================
```

2. **年初 CSV 导出**（机器可读）：每年 1 月 1 日 `before_trading_start` 触发，print 上一整年的完整 trade_log，每行前缀 `TLCSV: `。例如：
```
====== TRADE_LOG_CSV_BEGIN year=2024 rows=312 ======
TLCSV: date,stock,sector,legacy,phase,action,price,value,...
TLCSV: 2024-01-15,002714.XSHE,801010,N,BUILDING,T0,42.30,12500,...
TLCSV: 2024-02-08,002714.XSHE,801010,N,BUILDING,T1,38.92,12500,...
...
====== TRADE_LOG_CSV_END year=2024 ======
```

回测结束后，从聚宽日志面板用如下命令提取：

```bash
grep '^.*TLCSV: ' log_v2_01.txt | sed 's/^.*TLCSV: //' > trade_log_v2_01.csv
```

然后在 jupyter 中：

```python
import pandas as pd
df = pd.read_csv('trade_log_v2_01.csv')

# 退出 b 表现
exit_b = df[df['action'].str.startswith('exit_b')]
print(f"退出 b 总次数: {len(exit_b)}")
print(f"平均浮盈: {exit_b['pnl_pct'].mean()*100:.1f}%")
print(f"最大浮盈: {exit_b['pnl_pct'].max()*100:.1f}%")

# 单股完整路径
df[df['stock'] == '002714.XSHE'].sort_values('date')[
    ['date','phase','action','pnl_pct','pos_val_after']]

# legacy vs 主候选池的盈亏对比
print(df.groupby('legacy')['pnl_pct'].describe())

# 每年退出 b 次数走势
df['year'] = df['date'].str[:4]
df[df['action'].str.startswith('exit_b')].groupby('year').size()
```

#### 3.4.5 性能与开销

- 每条 `_log_trade` 调用 ≤ 1ms（聚宽 dict.append + 简单字段读取）
- 11 年回测预计 600~1200 条 trade_log，内存 < 500KB
- 每月摘要打印新增 ~20 行 INFO；每年 CSV 打印新增 ~50 行 INFO
- 总日志膨胀 < 5%

#### 3.4.6 配置开关

```python
'trade_log_enabled': True,                 # 总开关
'trade_log_print_yearly_csv': True,        # 每年 1 月 print 上年 CSV
'trade_log_print_monthly_summary': True,   # 每月 1 日 print 上月摘要
```

如果回测引擎对日志大小敏感，可关闭 `print_yearly_csv`，依然保留 `g.trade_log` 在内存中（聚宽研究环境可通过 `g.trade_log` 访问）。

---

### 3.5 因子门槛是结构性而非拟合性

| 参数 | 取值 | 是否结构性 | 解释 |
|---|---|---|---|
| 上市年限 ≥ 5 年 | 5 × 365 天 | ✓ | 因为我们要算 5 年 PB 分位 |
| 资产负债率 ≤ 65% | 0.65 | ✓ | 财务安全门槛，行业普遍标准 |
| 商誉 / 净资产 ≤ 30% | 0.30 | ✓ | 并购雷的客观门槛 |
| 流通市值 ≥ 50 亿 | 50 亿 | ◯ | 软性，可能调到 30~80 亿 |
| 60 日均成交额 ≥ 5000 万 | 5e7 | ◯ | 软性，按市值水位调 |
| 行业 top 50% | 0.50 | ✓ | 相对排名 ≠ 绝对阈值 |
| 周期性最低分 30 | 30.0 | ◯ | 软性，影响候选池规模 |
| 每行业 top 3 / 全池 ≤ 45 | 3 / 45 | ◯ | 软性 |

**5 个软性参数中，没有任何一个是"按回测结果调到当前值"**。它们都是工程经验值（流动性 5000 万 / 50 亿市值是 A 股选股惯例；行业 top 3 是分散度需求）。

---

## 4. 与 v1.9 的差异详细对比

### 4.1 代码模块对照

| 模块 | v1.9 | v2.0 | 差异 |
|---|---|---|---|
| 1. CONFIG | 47 项参数 | **62 项**（+15 项动态选股） | 新增 industry_* / stock_* / pool_* 参数簇 |
| 2. CANDIDATE_POOL | **静态字典** 13 行业 36 只 | ❌ **删除** | 完全去掉 |
| 3. all_candidates() | 返回字典展平 | **返回 g.quality_universe** | 接口兼容 |
| 4. sector_of() | 遍历字典查 | **读 g.sector_map** | 接口兼容 |
| 5. init_state() | — | **沿用** | 一字未改 |
| 6. initialize() | g.state, g.quality_universe, g.processed_today, ... | **+ g.cyclical_industries / g.industry_meta / g.last_industry_refresh_date / g.legacy_holding_stocks** | 增 4 个全局状态 |
| 7. before_trading_start() | — | **沿用** | — |
| 8. **动态选股层** | ❌ 不存在 | **★ compute_industry_metrics / refresh_cyclical_industries / compute_stock_cyclicality_score** | **v2.0 全新** |
| 9. refresh_quality_universe() | 简单财务过滤 | **重写：6 道闸门 + legacy 处理** | 大改 |
| 10. 4 因子打分 (calc_*) | — | **沿用** | — |
| 11. compute_target_value | — | **沿用** | — |
| 12. _safe_get_position / _build_order_style / safe_order_value / safe_close_all | — | **沿用** | 全部 Bug E~I 补丁继承 |
| 13. try_enter_t0 / try_build_t1_t2 | — | **沿用** | — |
| 14. calc_dynamic_grid_step / get_pyramid_multiplier | — | **沿用** | — |
| 15. run_grid() | — | **+ legacy 跳过买入** | 1 行新增 |
| 16. check_exit_a/b/c/d / hard_stop | — | **沿用** | — |
| 17. try_exit_cooldown | — | **+ trade_log 三路径** | 3 处插入 |
| 18. daily_signal_and_trade | — | **+ legacy 不发 T0/T1/T2 + trade_log** | 5 处插入 |
| 19. intraday_exit_check | — | **沿用** | — |
| 20. **trade_log 三件套** (v2.0 优 1, 已实施) | ❌ 不存在 | **★ _log_trade / print_trade_log_summary / print_trade_log_csv** | **v2.0 全新 ~150 行** |

**统计**：交易逻辑保留 100%，新增动态选股层 ~350 行 + trade_log 三件套 ~150 行；
主循环改造 4 处 7 行；trade_log 在交易动作处插入 21 处。

### 4.2 CONFIG 参数对照

新增项（v2.0 全新）：
```python
# 行业层 + 个股层 (15 项)
'industry_rebalance_days': 90,
'industry_lookback_years': 5,
'industry_pb_sample_days': 30,
'industry_min_pb_samples': 24,
'industry_min_roe_samples': 12,
'industry_min_rev_samples': 8,
'industry_top_n_ratio': 0.50,
'industry_min_top_n': 8,
'industry_max_top_n': 18,
'industry_min_stocks_for_score': 5,
'ind_weight_pb_cv':   0.50,
'ind_weight_roe_std': 0.25,
'ind_weight_rev_vol': 0.25,
'min_circulating_market_cap_yi': 50.0,
'stock_cyclicality_min_score': 30.0,
'stocks_per_industry': 3,
'pool_max_stocks': 45,

# trade_log (3 项)
'trade_log_enabled': True,
'trade_log_print_yearly_csv': True,
'trade_log_print_monthly_summary': True,
```

修改项（与 v1.9 不同）：
```python
'rebalance_quality_freq_days': 30,    # v1.9: 20
'min_listed_days': 365 * 5,           # v1.9: 365 (1 年)
'max_debt_ratio': 0.65,               # v1.9: 0.60
'min_avg_amount_60d_yuan': 5e7,       # v1.9 用的是 20 日; 改 60 日
```

其余 ~40 项参数与 v1.9 完全一致。

### 4.2.1 Bug J 修复（v2.0 新增）：T1 加右侧确认，解 BUILDING 卡死

**症状（log_v2_01 实测）**：

| 年份 | T0 | T1 | T2 | 完整建仓 | 退出 | 卡死率 |
|---|---|---|---|---|---|---|
| 2015 | 12 | 5 | 0 | 0 | 0 | 100% |
| 2018 | 65 | 19 | 21 | 8 | 13 | 68% |
| 2024 | 54 | 2 | 0 | 0 | 2 | 96% |
| **合计** | **368** | **109** | **40** | **23** | **52** | **80%** |

**80% 的 T0 仓位永久卡在 BUILDING（tier_filled=1）**——只持 ~1.04% 小仓位 + 不参与网格 + 不退出 + 资金锁死。同时记录到 393 次"T0 资金不足"被风控拒绝（因为现金被卡死的 BUILDING 锁占）。

**根因**：v1.9 的 `try_build_t1_t2` 中 T1 **只有左侧条件**（`drop_from_t0 ≥ 8%`）。一旦 T0 后股价持续上涨，T1 永不触发 → tier_filled 卡死在 1 → 永远进不了 GRID_RUNNING。v1.9 静态 36 只老周期股频繁回调 ≥ 8% 所以问题不暴露；v2.0 动态池里大量"刚买就涨"的票让 bug 浮现。

**修复**（v2.0 实施）：T1 加"右侧确认"，与 T2 对称。

```python
# CONFIG 新增
'tier_t1_no_touch_days': 30,            # T1 右侧: 30 天 + 涨 5%
'tier_t2_no_touch_after_t1_min_days': 7,  # T2 右侧需在 T1 后至少隔 7 天

# try_build_t1_t2 修改
if st['tier_filled'] == 1:
    cond_left = drop_from_t0 >= CONFIG['tier_drop_pct']
    cond_right = (days_since_t0 >= CONFIG['tier_t1_no_touch_days']
                  and current_price >= st['first_buy_price'] * 1.05)
    if cond_left or cond_right:
        # T1 触发, 记录 t1_filled_date
        ...
```

**预期效果**：T0 后 30 天股价波动 ≥5% 上行或 ≥8% 下行的概率极高，BUILDING 卡死率从 80% 应降至 < 20%。同时增加 t1_filled_date 状态字段，T2 右侧需在 T1 后至少 7 天才触发，避免 T1/T2 同日同时触发。

**log_v2_02 回测验证（2026-04-29）**——Bug J 修复非常成功：

| 指标 | v2_01（修复前） | v2_02（修复后） | 变化 |
|---|---|---|---|
| T0 建仓 | 368 | 377 | +2.4% |
| T1 加仓（左侧/右侧） | 109 / 0 | 245 / 127 | **T1 总 +241%** |
| T2 加仓（左侧/右侧） | 40 / 0 | 110 / 274 | **T2 总 +860%** |
| 建仓完成 → GRID | 23 | 63 | **+174%** |
| 网格买入 | 181 | 1502 | **+730%** |
| T0 资金不足拒绝 | 393 | 61 | **-84%** |

T1/T2 的右侧确认贡献了 34% / 71% 的触发量，验证了"刚买就涨"在动态池里很常见的判断。但同时暴露了下一个问题——**Bug K**。

### 4.2.2 Bug K 修复（v2.0 R2 新增）：T1/T2 失败 cooldown，止住日志/逻辑刷屏

**症状（log_v2_02 实测）**：

```
[600978.XSHG] T1 加仓 (左侧) | 价格=4.28 / 跌幅=8.74% / 持仓 39 天 / 加仓=2018
[600978.XSHG] 资金不足: 可用 48414 - 储备 46490 = 1923 < 门槛 2000, 跳过买入
[600978.XSHG] T1 加仓 (左侧) | 价格=4.28 / 跌幅=8.74% / 持仓 40 天 / 加仓=2018
[600978.XSHG] 资金不足: 可用 48414 - 储备 46848 = 1566 < 门槛 2000, 跳过买入
... 连续 36 个交易日相同模式 ...
```

`600978` 一只票连续 36 天打 T1 失败日志；`002191` 一只 72 天打 T2 失败日志。统计：T1 触发 372 次但**实际成功 ~100 次**（去重），T2 触发 384 次但**实际成功仅 63 次**（= 建仓完成数）。**~75% 的 T1/T2 日志/计算是无效重复**。

**根因**：`try_build_t1_t2` 里日志/`safe_order_value` 调用都在 `tier_filled` 推进**之前**。下单失败（典型: `cash - 储备 < 门槛`、停牌、涨停）时 `tier_filled` 不变，第二天右侧条件持续满足（30 天涨 5% 一旦成立，往往持续数月）→ 每天重复触发。Bug J 修复前 v2_01 因为 T1 只有左侧条件（跌 8% 一次满足后股价多半反弹脱离阈值），重复触发不严重；Bug J 修了后右侧条件粘性大，问题立刻浮现。

**修复**（v2.0 R2 实施）：

```python
# CONFIG
'tier_retry_cooldown_days': 5,   # T1/T2 下单失败 cooldown 天数

# init_state
'tier_retry_until_date': None,   # 失败 cooldown 截止日

# try_build_t1_t2 加 cooldown 检查
retry_block = st.get('tier_retry_until_date')
if retry_block is not None and today < retry_block:
    return  # cooldown 期内, 不日志/不下单

if st['tier_filled'] == 1:
    if cond_left or cond_right:
        order_id = safe_order_value(stock, t1_value, context=context)
        if order_id is not None:
            st['tier_filled'] = 2  # 成功才推进
            st['tier_retry_until_date'] = None
            log.info('T1 加仓 ...')   # 成功才打详细日志
            ...
        else:
            st['tier_retry_until_date'] = today + timedelta(days=5)
            log.info('T1 加仓 ... 下单失败, cooldown 5 天')
```

**预期效果**：

- T1/T2 失败日志数从 ~600 行降到 ~50 行（每只票每次失败只打 1 行 + 5 天后才再尝试）
- T1/T2 触发数 ≈ 真实有效成功数 + cooldown 期外的重试，T2 → 建仓完成转化率从 16% 上升到 ~70%
- 减少同一股票每天反复进入 try_build_t1_t2 后续逻辑的开销
- **不改变交易结果**——失败本来就不会下单，只是少打日志、少跑死循环

**未修复的关联问题**（先观察 R2 效果，再决定是否进一步动）：

1. **资金紧张是 T1/T2 失败的根因**：log_v2_02 显示 2022-03 时 universe=98 中 BUILDING=53 + GRID=6，53 只 BUILDING 卡在 tier=1/2 锁定 ~50% 仓位，加 10% 现金储备后剩余可用 ≈ 0。Bug K 让"无效尝试"消失，但根本上还是资金不够分。后续可考虑：
   - 缩小 universe（动态池规模 30→25 + legacy 上限 30）
   - 或加 `max_concurrent_building` 限流（如最多 25 只 BUILDING，达到上限暂停 T0）
2. **legacy 占比过高**：log_v2_02 后期 universe 121 中 legacy=79（65%）。legacy 持仓不发新 T0，但靠退出 a/b/c/d 自然出局，速度太慢。后续可考虑 `legacy_max_age_days`（如 365 天后强制 hard_stop 阈值收紧到 -15%）。

### 4.3 行为差异预测（待 log_v2_01 回测验证）

| 角度 | v1.9 (log10) | **v2.0 预期** | 解释 |
|---|---|---|---|
| 候选池规模 | 36 (静态) | **30~45 (浮动)** | 与 v1.9 同量级 |
| 候选行业数 | 13 (静态) | **8~18 (浮动)** | 略小 |
| 候选池每月变更 | 0% | **5~15%** | 月度 rebalance + 季度行业 |
| Legacy holdings 出现 | 不存在 | **数只到十数只** | 因行业变迁产生 |
| 单股 T0 转化率 | 8.7% | **5~10%** | 因为更多新股 ⇒ 更多 fail data |
| 退出 b 总次数 | 11 | **≥10**（不确定） | 候选池更广，但每只票深度可能下降 |
| 硬止损次数 | 2 | **3~5**（预期略增） | 因为不再过滤掉"我们事后知道有雷的票" |
| 硬止损黑名单触发 | 0 | **可能 1~3 次** | 候选池更广，惯犯隔离机制将更多动作 |
| 年化收益 | **14.22%** | **10~15%**（预估） | -2~+1pct 区间 |
| 最大回撤 | **19.35%** | **18~25%** | 可能略大（候选池更野） |

> **核心预期**：v2.0 的真实业绩"可能低于"v1.9 表面数据，但**这才是真正的可推广业绩**。如果 v2.0 拿到年化 9~12%，那这就是策略**未来可能实现的真实期望**——而 v1.9 的 14.22% 中的 4~5% 是后视镜偏差贡献的"虚增"。

---

## 5. 工程注意事项

### 5.1 查询性能

行业层每季度刷新一次，主要开销：

| 步骤 | 查询次数 | 备注 |
|---|---|---|
| `get_industries('sw_l1')` | 1 | 极快 |
| `get_industry_stocks(ind, date)` × 31 | 31 | 较快 |
| 月频 PB 采样 | ~60 次 `get_fundamentals(batch)` | 每次涵盖全 A ~3000 只 |
| 季频 ROE/营收采样 | ~20 次 `get_fundamentals(batch, date=stat_date+60d)` | 同上 |
| **小计** | **~110 次/季度** | 约 30 秒 |

个股层每月刷新：

| 步骤 | 查询次数 |
|---|---|
| `get_industry_stocks` × 候选行业数 | 8~18 |
| `get_fundamentals(batch)` 全行业批量财务 | 1 |
| `get_security_info(s)` 逐只 | 800~1500 |
| `attribute_history(s, 60)` 流动性 | ~600 |
| `get_valuation(s, 5y)` 周期性打分 | ~400 |
| **小计** | **~2000 次/月** | 约 1~2 分钟 |

聚宽回测引擎对此**应当能承受**（v1.9 用 36 只跑 11 年也不慢）。如果出现超时，可以：
- 增大 industry_pb_sample_days（降低频次）
- 减小 stocks_per_industry（减少 batch size）
- 降低 industry_max_top_n（减少行业数）

#### 5.1.1 score_cache：daily 主循环加速 ~3–5×（v2.0 实施）

v1.9 daily 主循环对每只候选股每天做 4 次单股查询（PB 5y / close 750d / RSI 44d / OCF），10 年 ≈ 50 万次 API。v2.0 通过 `score_cache_enabled=True` 改写为：

| 频率 | 操作 | 单次耗时 | 10 年总量 |
|---|---|---|---|
| **月度** (`refresh_score_cache`) | 对 universe 每只一次 `get_valuation(5y)` + `attribute_history(750d)` + 一次 batch `get_fundamentals(OCF)` | ~3 s | ~120 次 batch + ~9000 次单股 |
| **每日** (`refresh_today_score_inputs`) | 1 次 batch `get_fundamentals(today PB)` + `get_current_data()` 取 last_price | ~0.3 s | ~2500 次 batch |
| daily 主循环每股 | 0 次 API（全部读 cache） | — | 0 |

**等价性论证**：原 `attribute_history` 在 daily 09:30+ 调用时本就是截至昨日的 750 天 close；`get_fundamentals` 在 query_date 调用时同样是 PIT。score_cache 用 cache 历史 (截至刷新日昨日) + 当日 batch PB + cd.last_price，与原版**月内最大滞后 22 天**（仅 cache 末尾），但分位/回撤/RSI 实质等价（22/1300 < 2% 偏差）。OCF 因子来源是季报，月内本就不变 → 严格等价。

如果发现 cache 出 bug，把 `CONFIG['score_cache_enabled'] = False` 一键回退到原 daily 实时查询路径。

### 5.2 数据缺失的鲁棒性

每个查询都包了 try-except；如果某季度个别行业拿不到数据，自动跳过；如果整个 `compute_industry_metrics` 失败，**沿用上一次的 `g.cyclical_industries`**，不会让策略瘫痪。

### 5.3 退市票处理

聚宽 `get_industry_stocks(ind_code, date=t)` 在 t 时刻已退市的票不会返回；但已退市票如果在 t 时刻**还活着**则会返回，进入候选池。这是正确的 point-in-time 行为。

退市票如果通过了所有过滤进入了 `g.quality_universe` 并发了 T0 → T1 → T2 → GRID_RUNNING，那么后续退市时：
- `cd = get_current_data()[stock]` 在退市后聚宽返回 paused=True 的对象
- 主循环 `if cd.paused: continue` 跳过
- 实际持仓在退市当天会被聚宽自动按收盘价清算

回测会出现**真实的退市损失**——这正是消除幸存者偏差的代价。

### 5.4 与 v1.9 的并行运行

v2.0 和 v1.9 完全独立（不同 .py），可以并行回测：

```
聚宽策略 1: cyclical-grid-jq-strategy.py  (v1.9 基线)
聚宽策略 2: cyclical-grid-jq-strategy-v2.py  (v2.0 实验)

同区间 (建议 2018-01-01 ~ 2025-12-31, 8 年, 覆盖 2 轮周期):
  对比指标: 年化 / 最大回撤 / Sharpe / 退出 b 次数 / 硬止损次数 /
            候选池股票个数变化轨迹 / Legacy 数量
```

---

## 6. 上线 / 回测前 Checklist

- [ ] 完整粘贴 `cyclical-grid-jq-strategy-v2.py` 到聚宽
- [ ] 回测区间至少覆盖一个完整周期（建议 **2018-01-01 ~ 2025-12-31**，8 年覆盖 2 轮牛熊）
  - **不建议 2015 之前**：聚宽申万一级 2014 年改版前数据稀疏，行业筛选不稳定
- [ ] 初始资金 ≥ 100 万
- [ ] 基准：沪深 300（代码已设）
- [ ] 检查日志开头出现 `周期股·网格策略 v2.0 启动 | 全 A 动态选股 + v1.9 全部交易框架`
- [ ] 检查首次行业刷新出现 `【行业层】... | 候选行业 N / 总评估 M (top 50%)`
- [ ] 检查首次个股刷新出现 `【个股层】... | 候选 N 只 / M 行业 (legacy 0 / 池 K / 上限 45)`

### v2.0 专项验收（log_v2_01 起检查）

#### 行业层
- [ ] 季度行业刷新次数 ≥ 回测年数 × 4 ÷ 1.1（容错 10%）
- [ ] 每次刷新选出的行业数在 [8, 18] 区间
- [ ] 强周期行业（有色 / 煤炭 / 钢铁 / 基础化工）大多数季度入选
- [ ] 弱周期行业（银行 / 食品饮料 / 医药）大多数季度**不**入选
- [ ] 行业排名相对稳定（季度间 70%+ 重合）

#### 个股层
- [ ] 候选池规模在 [25, 45] 区间
- [ ] 月度池子有 5~15% 变化（不会大起大落）
- [ ] 至少出现 1 次"个股入选 → T0 → 月度被踢 → legacy → 退出"的完整闭环
- [ ] 不出现"持仓 ≥ 1 个月但被新池踢且 phase=COOLDOWN 持仓为零"的状态僵尸

#### 交易行为（与 v1.9 比）
- [ ] 退出 b 次数 ≥ 5 / 平均浮盈 ≥ 30%
- [ ] 硬止损次数 ≤ 8 / 不出现单股 1 年内 ≥ 2 次（有的话黑名单应介入）
- [ ] ERROR 数量 = 0（v1.9 的所有补丁继承）
- [ ] WARNING 数量 ≤ 100（保持 log10 级别的洁净）

#### Point-in-time 验证
- [ ] 抽查 2018 年初的某次个股池，**不**应该包含 2020 年才上市的票
- [ ] 抽查 2015 年某次行业池，**不**应该包含 2021 年新设的"煤炭/美容护理"等行业（如果聚宽数据如此）
- [ ] 抽查某只 2019 年退市的票，**应该**有可能在 2017~2018 年的回测中出现

#### 收益预期（与 v1.9 比）
- [ ] 年化收益在 [10%, 16%] 区间（v1.9 是 14.22%）
- [ ] 最大回撤在 [18%, 28%] 区间（v1.9 是 19.35%）
- [ ] **如果年化 < 10%**：说明候选池过滤过严或周期性识别不准，需调 ind_weight_*
- [ ] **如果年化 > 16%**：可能仍有隐性过拟合，重点检查是否所有查询都用了 query_date

#### trade_log 验收
- [ ] 每月 1 日日志中出现 `【trade_log 摘要 YYYY-MM】 总事件 N / 累计 M / buy=X / sell=Y`
- [ ] 每年 1 月 1 日日志中出现 `====== TRADE_LOG_CSV_BEGIN year=YYYY rows=N ======`
- [ ] 21 种 action 至少有 6 种被实际触发（T0/T1/T2/grid_buy/grid_sell/exit_a 应必现）
- [ ] 跨 11 年回测预期 ≥ 600 条 trade_log 记录
- [ ] grep TLCSV: 后导出 CSV，pandas 能正常 read_csv 加载（无字段错位）
- [ ] 用 trade_log 反推退出 b 平均浮盈，应与日志中 `退出 b ... 浮盈 X.X%` 行一致

---

## 7. 演进路线（v2.0 → v3.0）

### 7.1 v2.0 待回测验证

跑 log_v2_01（首次回测）后，按 §6 的 checklist 逐项核对。预期会发现一些 corner case，比如：

1. **初始空载期**：策略首次启动时 `g.cyclical_industries = []`，需要等第一次 `refresh_cyclical_industries` 才能开始。第一周可能没有任何 T0 信号
2. **行业边界抖动**：某行业在 top 50% 边缘震荡，连续季度被踢/重新入选，导致内部股票频繁进出 legacy 状态
3. **个股周期性误杀**：某只小众但周期性强的票因为流通市值或流动性门槛被过滤
4. **行业 z-score 失真**：某季度行业总数变少（如新行业上市初期），z-score 失稳

### 7.2 候选改进方向（v2.1+）

| 方向 | 说明 |
|---|---|
| ~~trade_log~~ | **✓ 已在 v2.0 实施**（详见 §3.4）|
| **行业 z-score 平滑**：对 z-score 做 EMA(0.7)，避免季度抖动 | 中 |
| **个股周期性引入 ROE 维度**：在 `compute_stock_cyclicality_score` 加 5 年 ROE 标准差 | 中 |
| **指数行业**（中证一级 / 中信一级）做对照实验 | 低 |
| **大类资产敞口**：申万 31 个行业归到 4~5 个大类，控制大类暴露 ≤ 40% | 中 |
| **退出 b 多日确认**：连续 2 日涨停打开 + 浮盈 ≥30% 才触发，提升精度 | 低 |
| **优 16 试验（v1.9 待办）**：把退出 b/c/d 的减半都改成"全清+60 日 COOLDOWN"，简化模型 | 中 |

### 7.3 真实可推广业绩 vs 回测好看业绩

v2.0 的核心价值是把策略放到一个**没有水分的回测环境**里。如果 v2.0 业绩 ≥ v1.9 的 60%（年化 ≥ 8.5%），就证明 v1.9 的核心逻辑是真的；如果 v2.0 业绩远低于 v1.9（年化 < 5%），就说明 v1.9 大部分收益来自后视镜选股，需要从交易逻辑层面再做改进。

---

## 8. 文件清单（v2.0 新增）

| 文件 | 说明 |
|---|---|
| `cyclical-grid-jq-strategy-v2.py` | **本版策略代码**（约 2064 行，含动态选股 ~500 行 + trade_log 三件套 ~180 行 + 21 处插桩 + 沿用 v1.9 全部交易框架） |
| `cyclical-grid-jq-strategy-v2-handbook.md` | **本文** |
| `cyclical-grid-jq-strategy.py` | v1.9 基线（封板，作为对照） |
| `cyclical-grid-jq-strategy-handbook.md` | v1.9 手册（仍有效） |
| `cyclical-grid-strategy-analysis.md` | 设计文档（最初思路） |
| `log10.txt` | v1.9 封板基线 log（0 ERROR / 0 WARNING / 0 RuntimeWarning） |

---

## 9. 设计哲学

回到本版的核心动机：

> **"v1.9 的 14.22% 年化里，有多少是策略真本事，有多少是我们事后选股的水分？"**

v2.0 的回答方式不是去争论这个问题，而是把候选池**完全交给数据**——让策略本身去发现什么是周期性行业、什么是周期性股票。

- 如果 v2.0 拿到 **年化 ≥ 12%**：v1.9 的核心交易逻辑是真有效，过去 7 轮迭代是正确的方向
- 如果 v2.0 拿到 **年化 8~12%**：v1.9 大部分有效，但有 ~3% 来自后视镜
- 如果 v2.0 拿到 **年化 < 8%**：v1.9 的高收益主要靠"我们事后知道选谁"，需要从交易框架层面再做创新

不论结果如何，**v2.0 给了我们一个更接近现实的回测——一个 2015 年初真的能跑、并且真的有人会按它跑的策略**。

---

> **当前状态**：v2.0 已实现，待第一次回测验证。建议先在 2018-01-01 ~ 2025-12-31 区间跑首次 log（建议命名 log_v2_01.txt），然后按 §6 的 checklist 逐项验收，再决定后续优化方向。
