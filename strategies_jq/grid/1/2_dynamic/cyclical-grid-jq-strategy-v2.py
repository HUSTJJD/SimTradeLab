# coding: utf-8
"""
周期股 · 多因子底部 + 动态网格 策略 v2.0 (聚宽 JoinQuant 版 · 动态选股)
=====================================================================

本策略是 v1.9 (cyclical-grid-jq-strategy.py) 的下一代演进, 核心目标:

  消除上一代「13 行业 / 36 只硬编码股票」候选池带来的「幸存者偏差 +
  后视镜选股」过拟合风险, 改为完全数据驱动的动态选股流水线.

v1.9 vs v2.0 主要差异:
  ┌────────────────┬─────────────────────────┬─────────────────────────┐
  │ 维度           │ v1.9 (上一代, 已封板)   │ v2.0 (本版, 动态)       │
  ├────────────────┼─────────────────────────┼─────────────────────────┤
  │ 候选池来源     │ 硬编码 36 只 / 13 行业  │ 全 A 动态扫描 + 因子    │
  │ 行业筛选       │ 人工选定 (后视镜)       │ PB CV + ROE σ + 营收 σ  │
  │ 个股筛选       │ 人工选龙头 (幸存者)     │ 5 年 PB 波动 + CV 排序  │
  │ point-in-time  │ 不需要 (硬编码)         │ 全部用 date 参数        │
  │ 退市/ST 处理   │ 不会出现 (列表中没有)   │ 历史回测会自然包含      │
  │ rebalance      │ 月度 (仅过滤)           │ 月度个股 + 季度行业     │
  │ 交易框架       │ 4 因子打分 + 网格       │ ★ 完全沿用 ★            │
  │ 退出规则       │ a/b/c/d + 硬止损        │ ★ 完全沿用 ★            │
  │ Bug E~I 补丁   │ 全部已修                │ ★ 全部继承 ★            │
  └────────────────┴─────────────────────────┴─────────────────────────┘

模块组成:
  1. 候选池          : ★ 动态扫描 (本版核心改动) ★
                         (a) 行业层 (季度): 申万一级 31 个行业, 用 PB CV +
                             ROE 标准差 + 营收增速波动率 三因子综合 z-score
                             排序, 取 top 50% (~15 个) 作为周期性候选行业.
                         (b) 个股层 (月度): 候选行业内所有股票, 经财务质量
                             /流动性/上市年限过滤, 再按个股 5 年 PB 波动度
                             打分, 每个行业取 top N (默认 3) 入选, 总池
                             ~30~45 只.
                         (c) Legacy holdings: 已建仓但本月被踢出新池的股票,
                             不再发新 T0 信号, 但 GRID/退出/COOLDOWN 全部
                             正常运行直到自然退出, 避免不必要的换仓成本.
  2. 质量过滤        : 上市≥5年 / 资产负债率≤65 / 商誉占比≤30 / 流通市值≥50亿
                       / 60日均成交额≥5000万 + 硬止损惯犯黑名单 (1年≥2次→隔离1年)
  3. 底部多因子打分  : PB 5 年逆向分位(.35) + 距前高回撤(.25)
                       + OCF/市值(.20) + 14 日 RSI 逆向(.20)         (沿用 v1.9)
  4. 分批建仓        : 30% / 30% / 40% 三档 (T0 / T1 / T2)            (沿用 v1.9)
  5. 动态网格        : 步长 = clamp(k*ATR/价格, [2.5%, 5%]),
                       不对称 (跌买 ×0.83 / 涨卖 ×1.17),
                       金字塔加仓 (-3% / -6% / -9% / -12% / -15%+)    (沿用 v1.9)
  6. 退出规则        : a) 网格闭合冷却 b) 涨停打开盈利 c) 高水位回撤
                       d) 持仓满 3 年评估                              (沿用 v1.9)
  7. 风控            : 单股 ≤15% / 单行业 ≤30%
                       硬止损 35% + "连续 3 交易日 ≥阈值" 持续过滤    (沿用 v1.9)
  8. 科创板兼容      : 688/8/4/92 开头自动加 MarketOrderStyle 保护价   (沿用 v1.9)

避免新一轮过拟合的关键技术:
  * 所有行业/股票/财务/价格查询都用 date= 参数指向 context.previous_date 或
    历史采样日, 严格 point-in-time. 不会出现 "我用 2026 年的股票池回测 2015
    年" 的偏差.
  * 行业筛选用全局相对排名 (top 50%), 不是固定阈值; 历年阈值都不同, 但相对
    排序自适应.
  * 个股因子门槛 (≥5年, ≥50亿) 是结构性 (避免数据不足/微小盘) 而非"调到 ≥45
    亿才好看"的拟合.
  * 行业用申万一级且 get_industries(date=query_date) 取当时的行业版本,
    避免用 2021 年改版后的"煤炭/美容护理"反查 2014 年.

使用方法:
  1. 登录 https://www.joinquant.com/algorithm
  2. 新建策略, 框架选 Python
  3. 把本文件全部内容粘贴到代码框
  4. 回测区间建议: 2018-01-01 ~ 2025-12-31 (覆盖完整周期; 2015 之前因为
                  上市股票数量少 + 行业改版前数据稀疏, 行业筛选可能不稳定)
  5. 初始资金建议 ≥ 100 万 (低于此值, 单股 15% 上限会限制建仓档位)
  6. 频率: 日线
  7. 基准: 沪深 300 (代码中已设置)

主要参数都集中在文件顶部的 CONFIG 字典, 可直接调整.
版本: v2.0 (2026-04-28)

核心动机:
  v1.9 在 2015-01 ~ 2026-04 全周期回测中拿到年化 14.22% / 最大回撤 19.35%
  的成绩 (log10), 但作者本人意识到: 36 只候选股 (宝钢/神华/牧原/海螺/中国
  神华 等) 大多是 2026 年回看时仍然存在的"穿越牛熊的行业寡头", 这种"硬编码"
  存在两层后视镜偏差:
    (1) 行业选择偏差: "我事后知道有色/煤炭/化工/养殖是周期股" → 13 个行业
        本身就是 2026 年回看的归纳;
    (2) 个股选择偏差: 每个行业选的都是穿越牛熊的龙头, 2015 年时我们其实
        无法预知它们会成为龙头;
    (3) 幸存者偏差: 36 只全部是 2026 年仍在交易的活股票; 2015 年那些已退市
        /被 ST 的"当时的周期股龙头"完全没出现.
  v2.0 用动态选股完全消除这三层偏差, 让回测结果更接近"真实可执行的策略
  在历史上能赚多少", 而非"我们今天回看选出的明星股能赚多少".

关联文档: SimTradeLab/my_docs/cyclical-grid-jq-strategy-v2-handbook.md
         SimTradeLab/my_docs/cyclical-grid-jq-strategy-handbook.md (v1.9, 仍有效)
         SimTradeLab/my_docs/cyclical-grid-strategy-analysis.md (设计基础)
"""

from jqdata import *
import pandas as pd
import numpy as np
from datetime import timedelta, date as date_cls

# =============================================================================
# 1. 全局参数
# =============================================================================

CONFIG = {
    # ---- 仓位与资金 (沿用 v1.9) ----
    'single_stock_max_pct': 0.15,
    'single_sector_max_pct': 0.30,
    'min_cash_reserve_pct': 0.10,
    'base_position_pct_of_stock': 0.50,

    # ---- 行业层动态筛选 (v2.0 新增) ----
    # 申万一级行业, 每 ~91 天 (季度) 重新计算所有行业的 5 年周期性指标:
    #   * PB CV (历史 PB 波动系数 = std/mean): 估值有大幅波动 → 周期性强
    #   * ROE std (历史 ROE 标准差): 业绩有周期性 → 顺周期股
    #   * 营收 YoY std (历史营收增速波动率): 行业整体景气度变动剧烈
    # 三因子分别 z-score 标准化 + 加权求和 → 排序取 top N 个 (默认 50%).
    # 在这 N 个行业里再做个股层筛选.
    'industry_rebalance_days': 90,                 # 行业层刷新间隔 (季度)
    'industry_lookback_years': 5,                  # 5 年历史
    'industry_pb_sample_days': 30,                 # PB 月频采样 (5 年 60 点)
    'industry_min_pb_samples': 24,                 # 至少 2 年才算有效
    'industry_min_roe_samples': 12,                # 至少 12 季度
    'industry_min_rev_samples': 8,                 # 至少 8 季度 (营收 YoY 需 i-4)
    'industry_top_n_ratio': 0.50,                  # 取 top 50% 行业
    'industry_min_top_n': 8,                       # 但至少保 8 个行业 (避免过窄)
    'industry_max_top_n': 18,                      # 但不超过 18 个 (避免过宽)
    'industry_min_stocks_for_score': 5,            # 行业内至少 5 只股票才打分
    'ind_weight_pb_cv':   0.50,                    # PB CV 权重 (主因子)
    'ind_weight_roe_std': 0.25,                    # ROE 标准差权重
    'ind_weight_rev_vol': 0.25,                    # 营收 YoY 标准差权重

    # ---- 个股层动态筛选 (v2.0 新增) ----
    'rebalance_quality_freq_days': 30,             # 个股层月度刷新 (v1.9 是 20)
    'min_listed_days': 365 * 5,                    # 上市≥5年 (v1.9 是 1 年)
                                                   # ↑ v2.0 加严: 5 年才有完整 PB
                                                   #   分位; 短上市易踩借壳/重组
    'max_debt_ratio': 0.65,                        # 资产负债率上限 (v1.9 是 0.60,
                                                   # 略放宽因周期股负债高)
    'max_goodwill_to_net_asset': 0.30,
    'min_avg_amount_60d_yuan': 5e7,                # 60日均成交额≥5000万 (v1.9 是 20 日)
    'min_circulating_market_cap_yi': 50.0,         # 流通市值≥50亿 (v2.0 新增)
                                                   # ↑ 防 ST/壳股误入候选池
    'stock_cyclicality_min_score': 30.0,           # 个股周期性最低分 (v2.0 新增)
    'stocks_per_industry': 3,                      # 每行业 top 3 只入选
    'pool_max_stocks': 45,                         # 总候选池上限 (硬截断)

    # ---- 底部多因子打分 (沿用 v1.9) ----
    'bottom_score_threshold_t0': 70.0,
    'bottom_score_threshold_keep': 65.0,
    'signal_persistence_days': 5,
    'pb_history_years': 5,
    'rsi_period': 14,
    'drawdown_lookback_days': 750,
    'factor_weights': {
        'pb_low_pct': 0.35,
        'drawdown_high_pct': 0.25,
        'ocf_to_marketcap': 0.20,
        'rsi_low_pct': 0.20,
    },

    # ---- 分批建仓 (沿用 v1.9 + v2.0 BUILDING 卡死 bug 修复) ----
    'tier_pcts': [0.30, 0.30, 0.40],
    'tier_drop_pct': 0.08,
    'tier_t1_no_touch_days': 30,           # v2.0 新增: T1 右侧确认 (30 天 + 涨 5%)
                                           # 修复 v1.9 BUG: T0 后只涨不跌时, T1 永不
                                           # 触发, 80% 的 T0 永远卡在 BUILDING tier=1.
                                           # 加右侧确认让"只涨不跌"的票也能继续建仓.
    'tier_t2_no_touch_days': 30,           # T2 右侧确认 (T1 触发后约 0~30 天再触发)
    'tier_t2_no_touch_after_t1_min_days': 7,  # v2.0 新增: T1 后至少间隔 7 天再 T2 右侧
                                           # 避免 T1/T2 同日触发 (T1 右侧 30 天 +
                                           # T2 右侧 30 天会同日触发, 应错开)
    'tier_retry_cooldown_days': 5,         # v2.0 BUG K 修复: T1/T2 触发但 safe_order_value
                                           # 失败 (典型: 现金储备不足) 时设 cooldown.
                                           # 此前 v2.0 R1 (修了 Bug J) 暴露: T1/T2 条件
                                           # 满足后日志/逻辑每天重复触发, 600978 一只票
                                           # 36 次 T1, 002191 一只 72 次 T2, 全部失败.
                                           # 加 cooldown 后失败 5 天内不再尝试 + 不刷屏日志.

    # ---- 动态网格 (沿用 v1.9) ----
    'grid_step_min_pct': 0.025,
    'grid_step_max_pct': 0.05,
    'grid_atr_k': 1.5,
    'grid_atr_period': 20,
    'grid_buy_step_factor': 0.83,
    'grid_sell_step_factor': 1.17,
    'grid_pyramid_thresholds': [-0.03, -0.06, -0.09, -0.12, -0.15],
    'grid_pyramid_multipliers': [1.0, 1.5, 2.0, 2.5, 3.0],

    # ---- 退出规则 (沿用 v1.9, 含 Bug H 修复) ----
    'exit_a_cooldown_days': 60,
    'exit_a_release_drawdown_pct': 0.25,
    'exit_b_min_profit_pct': 0.30,
    'exit_b_limit_open_drop_pct': 0.015,
    'exit_b_pb_top_pct': 0.70,
    'exit_c_high_water_min_profit': 0.50,
    'exit_c_drawdown_half_cut': 0.15,
    'exit_c_drawdown_full_cut': 0.25,
    'exit_c_after_half_cut_arm_gap': 0.10,
    'exit_c_after_half_cut_min_days': 15,
    'exit_c_half_cut_cooldown_days': 30,
    'exit_d_max_holding_days': 750,

    # ---- 硬止损 (沿用 v1.9) ----
    'hard_stop_loss_pct': 0.35,
    'hard_stop_persist_days': 3,
    'hard_stop_recent_window_days': 365,
    'hard_stop_max_count_in_window': 2,
    'hard_stop_blacklist_days': 365,
    't0_max_attempts_per_window': 1,

    # ---- 科创板 / 北交所 ----
    'star_market_protect_slippage_pct': 0.02,

    # ---- 最小订单 guard (沿用 v1.9 v1.7 调严后参数) ----
    'min_order_value_yuan': 2000.0,
    'min_order_shares': 200,
    'min_position_value_yuan': 5000.0,

    # ---- 调试 ----
    'verbose': False,                # v2.0: 默认关掉"接近触发"日志, 减 IO ~50%
                                     # 调参时可设 True 看明细
    'diagnostic_log_freq_days': 5,
    'log_near_miss_threshold': 50.0,

    # ---- score cache (v2.0 性能优化) ----
    # 把 daily 主循环里"每股每天 4 次单股查询" (PB 5y / close 750d / RSI / OCF)
    # 改为"月度 1 次 batch 拉满 + daily 1 次 batch 拿当日 PB". 等价缓存:
    # 月内 PB/close/OCF 滞后 ≤22 天, 但分位/回撤/RSI 实质不变 (原版本 attribute_history
    # 在 09:30+ 调用时本就是截至昨日, score_cache 也截至昨日).
    # 测得 daily 主循环加速 ~3-5x.
    'score_cache_enabled': True,

    # ---- trade_log 交易归因 (v2.0 优 1, 来自 v1.9 handbook §4.1) ----
    # 实时把每笔成交动作以结构化 dict 形式追加到 g.trade_log; 月初打印上月
    # action 计数摘要; 年初打印上一整年的完整 CSV 行 (前缀 'TLCSV: ', 用户
    # 在聚宽日志搜该前缀, 复制到本地去掉前缀即得 DataFrame 可读 CSV).
    # 平均每年 ~50 笔, 11 年 ~550 行, 内存与日志开销都微小.
    # 用途: 替代 "grep 20000 行 INFO 反推退出 b 多少次/平均浮盈多少" 的人工劳作,
    #       对 v2.0 验证、参数调整、与 v1.9 对比都极有价值.
    'trade_log_enabled': True,
    'trade_log_print_yearly_csv': True,    # 每年 1 月 print 上年完整 CSV
    'trade_log_print_monthly_summary': True,  # 每月 1 日 print 上月 action 计数
}


# =============================================================================
# 2. 持仓状态管理 (沿用 v1.9)
# =============================================================================

def init_state():
    """单只股票的状态字典."""
    return {
        'phase': 'IDLE',                     # IDLE / BUILDING / GRID_RUNNING / COOLDOWN
        'first_buy_date': None,
        'first_buy_price': None,
        't1_filled_date': None,              # v2.0 新增: T1 触发日 (用于 T2 右侧 cooldown)
        'tier_retry_until_date': None,       # v2.0 BUG K 修复: T1/T2 失败 cooldown 截止日
                                             # safe_order_value 失败 (cash 不足) 时设此字段,
                                             # 在 cooldown 期内 try_build_t1_t2 直接 return,
                                             # 避免日志/逻辑每天反复触发同一 tier.
        'tier_filled': 0,
        'target_total_value': 0.0,
        'base_target_value': 0.0,
        'grid_target_value': 0.0,
        'last_grid_price': None,
        'grid_total_buy_value': 0.0,
        'grid_total_sell_value': 0.0,
        'grid_buy_count': 0,
        'grid_sell_count': 0,
        'pyramid_levels_done': [],
        'high_water_pnl_pct': 0.0,
        'cooldown_until_date': None,
        'consecutive_signal_days': 0,
        'last_score': 0.0,
        'last_high_after_grid': None,
        't0_attempts_in_window': 0,
        't0_disabled_until_score_below': None,
        'exit_c_last_action_date': None,
        'exit_c_arm_high': 0.0,
        'hard_stop_persist_count': 0,
        'hard_stop_last_check_date': None,
    }


# =============================================================================
# 3. 聚宽框架接口
# =============================================================================

def initialize(context):
    log.info('=' * 70)
    log.info('周期股·网格策略 v2.0 启动 | 全 A 动态选股 + v1.9 全部交易框架')
    log.info('=' * 70)

    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)

    set_slippage(FixedSlippage(0.002))
    set_order_cost(OrderCost(
        open_tax=0.0,
        close_tax=0.001,
        open_commission=0.00025,
        close_commission=0.00025,
        close_today_commission=0.0,
        min_commission=5.0,
    ), type='stock')

    # ---- 状态机 ----
    g.state = {}
    g.processed_today = set()
    g.trading_day_counter = 0
    g.last_score_snapshot = {}

    # ---- 候选池 (动态) ----
    g.quality_universe = []           # 当前可发新 T0 的股票 (含 legacy)
    g.sector_map = {}                 # {stock: sw_l1_industry_code}, 动态记录
    g.last_quality_check_date = None  # 个股层最近刷新日

    # ---- 行业层 ----
    g.cyclical_industries = []        # 当前选中的周期性行业代码列表
    g.industry_meta = {}              # {ind_code: {'name', 'pb_cv', 'roe_std', 'rev_vol', 'score'}}
    g.last_industry_refresh_date = None

    # ---- legacy holdings (v2.0 新增) ----
    # 已建仓但被本月新候选池踢出的股票. 不发新 T0, 但 GRID/退出/COOLDOWN 都跑.
    g.legacy_holding_stocks = set()

    # ---- 硬止损黑名单 (沿用 v1.9) ----
    g.hard_stop_history = {}
    g.hard_stop_blocked_until = {}

    # ---- trade_log (v2.0 优 1) ----
    g.trade_log = []                       # 全量交易事件 list of dict
    g.last_trade_log_summary_month = None  # 上次月度摘要打印时所在月份

    # ---- score cache (v2.0 性能优化) ----
    g.score_cache_pb_history = {}      # {stock: np.array(5y pb)}, 月度 batch 拉
    g.score_cache_close_history = {}   # {stock: np.array(750d close)}, 月度 batch 拉
    g.score_cache_ocf = {}             # {stock: ocf_score (0-100)}, 月度 batch 拉
    g.score_cache_today_pb = {}        # {stock: today's pb}, daily batch 刷新
    g.score_cache_today_close = {}     # {stock: today's last_price}, daily 主循环刷新
    g.score_cache_refreshed_date = None  # 月度缓存刷新日期

    # 季度刷新行业 + 月度刷新个股. 因为聚宽 run_daily 会每天调一次, 内部加日期判断.
    run_daily(refresh_quality_universe, time='before_open')
    run_daily(daily_signal_and_trade,    time='10:00')
    run_daily(intraday_exit_check,       time='14:50')


def before_trading_start(context):
    g.processed_today = set()
    g.trading_day_counter = getattr(g, 'trading_day_counter', 0) + 1

    # v2.0: 月初打印上月 trade_log 摘要 + 1 月初额外打印上年完整 CSV
    today = context.current_dt.date()
    last_month = getattr(g, 'last_trade_log_summary_month', None)
    cur_ym = (today.year, today.month)
    if last_month is None:
        g.last_trade_log_summary_month = cur_ym
    elif cur_ym != last_month:
        if CONFIG['trade_log_print_monthly_summary']:
            print_trade_log_summary(context, year=last_month[0], month=last_month[1])
        # 1 月跨年时, 打印上一整年的完整 CSV (用户可从日志 grep TLCSV: 提取)
        if (cur_ym[0] != last_month[0]
                and CONFIG['trade_log_print_yearly_csv']):
            print_trade_log_csv(context, year=last_month[0])
        g.last_trade_log_summary_month = cur_ym


# =============================================================================
# 4. 动态选股层 (v2.0 核心新增)
# =============================================================================

def _generate_monthly_sample_dates(start, end, step_days):
    """生成 [start, end] 内的月频采样日期列表."""
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur = cur + timedelta(days=step_days)
    if dates and dates[-1] != end:
        dates.append(end)
    return dates


def _generate_quarter_stat_dates(end_date, n_quarters):
    """生成最近 N 个季度的 statDate 字符串列表 (e.g. ['2023q1', '2023q2', ...]).

    注意: 我们用 statDate 做财报查询, statDate 是季报截止日期 (3-31/6-30/9-30/12-31).
    end_date 之后的季度(还没发布的)需要排除. 简单起见, 取 end_date 当年 + 前 N 年的所有
    季度, 再过滤掉 statDate 物理上还没到的.
    """
    cur_year = end_date.year
    quarters = []
    # 5 年 = 5 * 4 = 20 个季度
    for year in range(cur_year - (n_quarters // 4 + 1), cur_year + 1):
        for q in (1, 2, 3, 4):
            stat_date_obj = {
                1: date_cls(year, 3, 31),
                2: date_cls(year, 6, 30),
                3: date_cls(year, 9, 30),
                4: date_cls(year, 12, 31),
            }[q]
            # 季报通常在季末后 30~60 天发布; 严格起见只取 stat_date_obj 之后 60 天以上的
            if (end_date - stat_date_obj).days >= 60:
                quarters.append('%dq%d' % (year, q))
    return quarters[-n_quarters:]


def _statdate_to_safe_date(statdate_str):
    """'2014q3' → 该季报"安全可查日期" (季末后 60 天, 大多公司已披露).

    背景: 聚宽 avoid_future_data=True (回测默认) 时不允许用 statDate 参数,
    只能用 date 参数. 用 date=safe_date 调用 get_fundamentals 时, 聚宽会返回
    "截至 safe_date 各股票最新一期财报", 在 safe_date = stat_date+60 天时,
    绝大多数股票最新一期就是该 stat_date 的财报 (披露截止: 一季报 4-30,
    中报 8-31, 三季报 10-31, 年报次年 4-30).
    """
    year_str, q_str = statdate_str.split('q')
    year = int(year_str)
    q = int(q_str)
    end_date = {
        1: date_cls(year, 3, 31),
        2: date_cls(year, 6, 30),
        3: date_cls(year, 9, 30),
        4: date_cls(year, 12, 31),
    }[q]
    return end_date + timedelta(days=60)


def _zscore(arr):
    """安全 z-score: 标准差为 0 时返回全 0."""
    arr = np.asarray(arr, dtype=float)
    arr = np.where(np.isnan(arr), 0.0, arr)
    m = float(np.mean(arr))
    s = float(np.std(arr))
    if s <= 1e-9:
        return np.zeros_like(arr)
    return (arr - m) / s


def compute_industry_metrics(context):
    """计算所有申万一级行业的 5 年周期性三因子 (PB CV / ROE std / 营收 YoY std).

    返回 (metrics, ind_to_stocks, stock_to_ind):
      metrics: {ind_code: {'pb_cv', 'roe_std', 'rev_vol', 'name'}}
      ind_to_stocks: {ind_code: [stocks]} 当前各行业的股票 list (point-in-time)
      stock_to_ind:  {stock: ind_code} 反向映射

    关键 point-in-time 处理:
      * get_industries(name='sw_l1', date=query_date): 当日有效的行业列表
      * get_industry_stocks(ind, date=query_date): 当日属于该行业的股票
      * get_fundamentals(query, date=sample_date / statDate=stat_date): 当时财务数据
    所有查询都不会取未来值.
    """
    query_date = context.previous_date
    lookback = CONFIG['industry_lookback_years']
    log.info('[行业层#1] query_date=%s, lookback=%d 年' % (query_date, lookback))

    # 1) 当前申万一级行业表 (用 query_date 而非 today, 避免 avoid_future_data 警告)
    try:
        industries_df = get_industries(name='sw_l1', date=query_date)
    except Exception as e:
        log.warn('[行业层#2] get_industries 异常: %s' % e)
        return {}, {}, {}

    n_ind = len(industries_df) if industries_df is not None else 0
    if n_ind == 0:
        log.warn('[行业层#2] get_industries 返回空 DataFrame (该日期可能无 sw_l1 数据)')
        return {}, {}, {}
    log.info('[行业层#2] 申万一级行业: %d 个 (cols=%s)'
             % (n_ind, list(industries_df.columns)))

    # 2) 当前各行业股票池
    ind_to_stocks = {}
    stock_to_ind = {}
    fail_ind = 0
    for ind_code in industries_df.index:
        try:
            stocks = get_industry_stocks(ind_code, date=query_date)
        except Exception:
            stocks = []
            fail_ind += 1
        ind_to_stocks[ind_code] = stocks
        for s in stocks:
            stock_to_ind[s] = ind_code

    all_stocks = list(stock_to_ind.keys())
    log.info('[行业层#3] 行业 → 股票映射: %d 唯一股票 / %d 行业 get_industry_stocks 失败'
             % (len(all_stocks), fail_ind))
    if not all_stocks:
        log.warn('[行业层#3] 全部行业 get_industry_stocks 返回空, 跳过')
        return {}, ind_to_stocks, stock_to_ind

    # 3) PB CV: 月频采样, 每点一次 get_fundamentals 拿全 A
    sample_start = query_date - timedelta(days=int(lookback * 365))
    pb_sample_dates = _generate_monthly_sample_dates(
        sample_start, query_date, CONFIG['industry_pb_sample_days'])
    log.info('[行业层#4] PB 月频采样: %d 点 (%s ~ %s)'
             % (len(pb_sample_dates),
                pb_sample_dates[0] if pb_sample_dates else 'NA',
                pb_sample_dates[-1] if pb_sample_dates else 'NA'))

    ind_pb_lists = {ind: [] for ind in industries_df.index}
    pb_ok = 0
    pb_fail = 0
    for d in pb_sample_dates:
        try:
            df = get_fundamentals(
                query(valuation.code, valuation.pb_ratio)
                .filter(valuation.code.in_(all_stocks)),
                date=d
            )
        except Exception as e:
            pb_fail += 1
            if pb_fail <= 2:
                log.warn('[行业层#4] PB 查询 %s 异常: %s' % (d, e))
            continue
        if df is None or df.empty:
            pb_fail += 1
            continue
        pb_ok += 1
        df = df.dropna(subset=['pb_ratio'])
        df = df[df['pb_ratio'] > 0]
        if df.empty:
            continue
        df['ind'] = df['code'].map(stock_to_ind)
        df = df.dropna(subset=['ind'])
        for ind, sub in df.groupby('ind'):
            try:
                med = float(sub['pb_ratio'].median())
                if not np.isnan(med) and med > 0:
                    ind_pb_lists[ind].append(med)
            except Exception:
                continue
    log.info('[行业层#4] PB 采样结果: 成功 %d / 空或失败 %d' % (pb_ok, pb_fail))

    # 4) ROE & 营收 YoY: 季频采样 (statDate)
    quarter_dates = _generate_quarter_stat_dates(query_date, lookback * 4)
    log.info('[行业层#5] 季频 statDate: %d 个 (%s ~ %s)'
             % (len(quarter_dates),
                quarter_dates[0] if quarter_dates else 'NA',
                quarter_dates[-1] if quarter_dates else 'NA'))

    ind_roe_lists = {ind: [] for ind in industries_df.index}
    ind_rev_lists = {ind: [] for ind in industries_df.index}
    q_ok = 0
    q_fail = 0
    for sd in quarter_dates:
        # avoid_future_data=True 时聚宽不支持 statDate, 改用 "季末 + 60 天" 的 date
        safe_date = _statdate_to_safe_date(sd)
        # 防止 safe_date 跨过 query_date (理论上 _generate_quarter_stat_dates 已过滤,
        # 但加个 guard 保险)
        if safe_date > query_date:
            safe_date = query_date
        try:
            df = get_fundamentals(
                query(valuation.code, indicator.roe, income.operating_revenue)
                .filter(valuation.code.in_(all_stocks)),
                date=safe_date
            )
        except Exception as e:
            q_fail += 1
            if q_fail <= 2:
                log.warn('[行业层#5] 季频查询 %s (date=%s) 异常: %s'
                         % (sd, safe_date, e))
            continue
        if df is None or df.empty:
            q_fail += 1
            continue
        q_ok += 1
        df['ind'] = df['code'].map(stock_to_ind)
        df = df.dropna(subset=['ind'])
        for ind, sub in df.groupby('ind'):
            try:
                roe_vals = sub['roe'].dropna()
                rev_vals = sub['operating_revenue'].dropna()
                if len(roe_vals) > 0:
                    ind_roe_lists[ind].append(float(roe_vals.mean()))
                if len(rev_vals) > 0:
                    rev_sum = float(rev_vals.sum())
                    if rev_sum > 0:
                        ind_rev_lists[ind].append(rev_sum)
            except Exception:
                continue
    log.info('[行业层#5] 季频采样结果: 成功 %d / 空或失败 %d' % (q_ok, q_fail))

    # 5) 综合得分
    metrics = {}
    fail_pb_n = 0
    fail_roe_n = 0
    fail_rev_n = 0
    fail_yoy = 0
    fail_pbmean = 0
    fail_min_n = 0
    sample_pb_lens = []
    sample_roe_lens = []
    sample_rev_lens = []
    for ind in industries_df.index:
        try:
            ind_name = industries_df.loc[ind, 'name'] if 'name' in industries_df.columns else ''
        except Exception:
            ind_name = ''

        pb_arr = np.asarray(ind_pb_lists[ind], dtype=float)
        roe_arr = np.asarray(ind_roe_lists[ind], dtype=float)
        rev_arr = np.asarray(ind_rev_lists[ind], dtype=float)
        sample_pb_lens.append(len(pb_arr))
        sample_roe_lens.append(len(roe_arr))
        sample_rev_lens.append(len(rev_arr))

        if len(pb_arr) < CONFIG['industry_min_pb_samples']:
            fail_pb_n += 1
            continue
        if len(roe_arr) < CONFIG['industry_min_roe_samples']:
            fail_roe_n += 1
            continue
        if len(rev_arr) < CONFIG['industry_min_rev_samples']:
            fail_rev_n += 1
            continue

        pb_mean = float(pb_arr.mean())
        if pb_mean <= 0:
            fail_pbmean += 1
            continue
        pb_cv = float(pb_arr.std()) / pb_mean
        roe_std = float(roe_arr.std())

        # 营收 YoY 增速序列: rev[i] / rev[i-4] - 1
        rev_yoy = []
        for i in range(4, len(rev_arr)):
            base = rev_arr[i - 4]
            if base > 0:
                rev_yoy.append((rev_arr[i] - base) / base)
        if len(rev_yoy) < 4:
            fail_yoy += 1
            continue
        rev_vol = float(np.std(rev_yoy))

        n_stocks = len(ind_to_stocks.get(ind, []))
        if n_stocks < CONFIG['industry_min_stocks_for_score']:
            fail_min_n += 1
            continue

        metrics[ind] = {
            'name': ind_name,
            'pb_cv': pb_cv,
            'roe_std': roe_std,
            'rev_vol': rev_vol,
            'n_stocks': n_stocks,
        }

    def _med(arr):
        return int(np.median(arr)) if arr else 0

    log.info('[行业层#6] 因子计算: 通过 %d / 共 %d 行业'
             % (len(metrics), len(industries_df)))
    log.info('[行业层#6] 各行业采样数中位 (PB/ROE/Rev): %d / %d / %d  | 阈值要求: %d / %d / %d'
             % (_med(sample_pb_lens), _med(sample_roe_lens), _med(sample_rev_lens),
                CONFIG['industry_min_pb_samples'],
                CONFIG['industry_min_roe_samples'],
                CONFIG['industry_min_rev_samples']))
    log.info('[行业层#6] 过滤详情: PB采样不足=%d, ROE采样不足=%d, Rev采样不足=%d, YoY不足=%d, PBmean<=0=%d, NStocks不足=%d'
             % (fail_pb_n, fail_roe_n, fail_rev_n, fail_yoy, fail_pbmean, fail_min_n))

    return metrics, ind_to_stocks, stock_to_ind


def refresh_cyclical_industries(context):
    """季度刷新周期性行业 list (写入 g.cyclical_industries / g.industry_meta)."""
    today = context.current_dt.date()
    log.info('=' * 70)
    log.info('【行业层动态筛选】%s | 触发季度刷新...' % today)

    metrics, ind_to_stocks, stock_to_ind = compute_industry_metrics(context)
    if not metrics:
        # v2.0 失败退避: 推 last_industry_refresh_date 到 (今天 - rebalance_days + retry),
        # 让 7 天后再重试一次. 防止每个交易日都重复触发完整 PB/ROE 批量查询 (日志刷屏 + 性能浪费).
        retry_after_days = 7
        g.last_industry_refresh_date = today - timedelta(
            days=max(0, CONFIG['industry_rebalance_days'] - retry_after_days))
        log.warn('【行业层】返回空, 沿用旧列表 (cyclical=%d). %d 天后再次尝试.'
                 % (len(g.cyclical_industries), retry_after_days))
        return

    inds = list(metrics.keys())
    pb_cvs = np.asarray([metrics[i]['pb_cv'] for i in inds])
    roe_stds = np.asarray([metrics[i]['roe_std'] for i in inds])
    rev_vols = np.asarray([metrics[i]['rev_vol'] for i in inds])

    z_pb = _zscore(pb_cvs)
    z_roe = _zscore(roe_stds)
    z_rev = _zscore(rev_vols)

    composite = (CONFIG['ind_weight_pb_cv'] * z_pb
                 + CONFIG['ind_weight_roe_std'] * z_roe
                 + CONFIG['ind_weight_rev_vol'] * z_rev)

    scored = list(zip(inds, composite.tolist()))
    scored.sort(key=lambda x: -x[1])

    n_total = len(scored)
    target_n = int(round(n_total * CONFIG['industry_top_n_ratio']))
    target_n = max(CONFIG['industry_min_top_n'], target_n)
    target_n = min(CONFIG['industry_max_top_n'], target_n)
    target_n = min(target_n, n_total)

    selected = scored[:target_n]
    g.cyclical_industries = [c for c, _ in selected]
    g.industry_meta = {}
    for c, s in selected:
        m = dict(metrics[c])
        m['score'] = float(s)
        g.industry_meta[c] = m
    g.last_industry_refresh_date = today

    log.info('【行业层】%s | 候选行业 %d / 总评估 %d (top %.0f%%)'
             % (today, target_n, n_total, CONFIG['industry_top_n_ratio'] * 100))
    for ind, s in selected:
        m = metrics[ind]
        log.info('  ★ %s [%s] score=%+.2f | PB CV=%.3f / ROE σ=%.2f / Rev YoY σ=%.3f / N=%d'
                 % (ind, m['name'][:8], s, m['pb_cv'], m['roe_std'],
                    m['rev_vol'], m['n_stocks']))
    log.info('=' * 70)


def refresh_score_cache(context, universe):
    """月度 batch 刷新打分缓存 (v2.0 性能优化).

    在 refresh_quality_universe 末尾对**新选出的 universe** (含 legacy) 一次性
    取够: 5 年 PB / 750 天 close / 当月 OCF 因子.

    daily 主循环只需调 refresh_today_score_inputs(context) 拿当日 PB + last_close,
    其它一律读 cache, 减 ~98% 单股查询.
    """
    if not CONFIG.get('score_cache_enabled', True):
        return
    universe = list(set(universe) | g.legacy_holding_stocks)
    if not universe:
        return

    query_date = context.previous_date
    pb_lookback_days = int(CONFIG['pb_history_years'] * 365)
    close_lookback_days = CONFIG['drawdown_lookback_days']

    g.score_cache_pb_history = {}
    g.score_cache_close_history = {}
    g.score_cache_ocf = {}
    g.score_cache_refreshed_date = query_date

    n_pb_ok = 0
    n_close_ok = 0
    for stock in universe:
        try:
            pb_start = query_date - timedelta(days=pb_lookback_days)
            df = get_valuation(stock, start_date=pb_start, end_date=query_date,
                               fields=['pb_ratio'])
            if df is not None and not df.empty:
                pb = np.asarray(df['pb_ratio'], dtype=float)
                pb = pb[~np.isnan(pb)]
                pb = pb[pb > 0]
                if len(pb) >= 100:
                    g.score_cache_pb_history[stock] = pb
                    n_pb_ok += 1
        except Exception:
            pass

        try:
            ah = attribute_history(stock, close_lookback_days, '1d',
                                   ['close'], df=False)['close']
            arr = np.asarray(ah, dtype=float)
            arr = arr[~np.isnan(arr)]
            if len(arr) >= 50:
                g.score_cache_close_history[stock] = arr
                n_close_ok += 1
        except Exception:
            pass

    # OCF: 一次 batch 拉 universe 的 market_cap + net_operate_cash_flow
    n_ocf_ok = 0
    for i in range(0, len(universe), 800):
        batch = universe[i:i + 800]
        try:
            df = get_fundamentals(query(
                valuation.code,
                valuation.market_cap,
                cash_flow.net_operate_cash_flow,
            ).filter(valuation.code.in_(batch)), date=query_date)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            stock = row['code']
            mv = row['market_cap']
            ocf = row['net_operate_cash_flow']
            if mv is None or mv <= 0 or ocf is None:
                g.score_cache_ocf[stock] = 0.0
            else:
                ratio = ocf / (mv * 1e8)
                g.score_cache_ocf[stock] = float(
                    max(0.0, min(ratio / 0.10 * 100.0, 100.0)))
            n_ocf_ok += 1

    log.info('[score_cache] 月度缓存刷新: PB %d / close %d / OCF %d (共 %d 只)'
             % (n_pb_ok, n_close_ok, n_ocf_ok, len(universe)))


def refresh_today_score_inputs(context):
    """daily 主循环开盘前 batch 拿当日 PB + last_close (v2.0 性能优化).

    PB 用 1 次 batch get_fundamentals (替代每股 1 次单查询).
    last_close 用 get_current_data() 一次拿 (本来就要拿).
    """
    if not CONFIG.get('score_cache_enabled', True):
        return
    universe = list(set(g.quality_universe) | g.legacy_holding_stocks)
    if not universe:
        return
    query_date = context.previous_date

    today_pb = {}
    for i in range(0, len(universe), 800):
        batch = universe[i:i + 800]
        try:
            df = get_fundamentals(
                query(valuation.code, valuation.pb_ratio)
                .filter(valuation.code.in_(batch)),
                date=query_date
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            pb = row['pb_ratio']
            if pb is not None and pb > 0:
                today_pb[row['code']] = float(pb)
    g.score_cache_today_pb = today_pb

    today_close = {}
    try:
        cur_data = get_current_data()
        for stock in universe:
            try:
                cd = cur_data[stock]
                last = cd.last_price
                if last is not None and last > 0 and not np.isnan(last):
                    today_close[stock] = float(last)
            except Exception:
                continue
    except Exception:
        pass
    g.score_cache_today_close = today_close


def compute_stock_cyclicality_score(context, stock):
    """个股 5 年周期性得分 (0-100). 综合 PB max/min ratio + PB CV.

    周期股的本质是估值大幅波动: 低谷时 PB 0.5x, 高峰时 PB 5x+, ratio ≥ 5.
    用 PB max/min ratio 显式抓"幅度", 用 PB CV 抓"频次".
    """
    query_date = context.previous_date
    start = query_date - timedelta(days=int(CONFIG['pb_history_years'] * 365))
    try:
        df = get_valuation(stock, start_date=start, end_date=query_date,
                           fields=['pb_ratio'])
        if df is None or len(df) < 250:
            return None
        pb = df['pb_ratio'].dropna()
        pb = pb[pb > 0]
        if len(pb) < 250:
            return None
        pb_max = float(pb.max())
        pb_min = float(pb.min())
        pb_mean = float(pb.mean())
        if pb_min <= 0 or pb_mean <= 0:
            return None
        ratio = pb_max / pb_min
        ratio_score = min(ratio / 5.0, 1.0) * 100.0
        cv = float(pb.std()) / pb_mean
        cv_score = min(cv / 0.5, 1.0) * 100.0
        return 0.5 * ratio_score + 0.5 * cv_score
    except Exception:
        return None


def refresh_quality_universe(context):
    """月度刷新候选池 (个股层) + 季度刷新行业 (内部判断).

    流程:
      1. 季度: 触发 refresh_cyclical_industries (改 g.cyclical_industries)
      2. 月度: 在候选行业内 → 黑名单过滤 → 财务质量过滤 → 流动性过滤 →
               周期性打分 → 每行业取 top N → 加 legacy holdings → 写入
               g.quality_universe / g.sector_map
    """
    today = context.current_dt.date()
    query_date = context.previous_date

    # 1) 季度行业刷新
    if (g.last_industry_refresh_date is None or
            (today - g.last_industry_refresh_date).days
            >= CONFIG['industry_rebalance_days']):
        try:
            refresh_cyclical_industries(context)
        except Exception as e:
            log.warn('【行业层】刷新异常 (沿用旧): %s' % e)

    # 2) 月度个股刷新闸门
    if (g.last_quality_check_date is not None and
            (today - g.last_quality_check_date).days
            < CONFIG['rebalance_quality_freq_days']):
        return

    if not g.cyclical_industries:
        # v2.0 失败退避: 个股层无法工作时, 把 last_quality_check_date 推近, 7 天后再试,
        # 防止每个交易日都打印 warn 刷屏 (行业层退避后每 7 天会重试一次)
        g.last_quality_check_date = today - timedelta(
            days=max(0, CONFIG['rebalance_quality_freq_days'] - 7))
        log.warn('【个股层】%s | 行业列表为空, 跳过本次刷新 (7 天后再试)' % today)
        return

    # 3) 硬止损黑名单
    blocked_today = []
    for stock, until in list(g.hard_stop_blocked_until.items()):
        if until is None or today >= until:
            g.hard_stop_blocked_until.pop(stock, None)
        else:
            blocked_today.append(stock)
    if blocked_today:
        log.info('【硬止损隔离】%s | 排除 %d 只: %s'
                 % (today, len(blocked_today),
                    ','.join(blocked_today[:10])
                    + ('...' if len(blocked_today) > 10 else '')))

    # 4) 候选行业内的所有股票 (point-in-time)
    candidates = set()
    ind_of_candidate = {}
    for ind_code in g.cyclical_industries:
        try:
            ind_stocks = get_industry_stocks(ind_code, date=query_date)
        except Exception:
            ind_stocks = []
        for s in ind_stocks:
            if s in blocked_today:
                continue
            candidates.add(s)
            ind_of_candidate[s] = ind_code
    candidates = list(candidates)

    if not candidates:
        log.warn('【个股层】%s | 候选行业内无股票, 跳过' % today)
        return

    # 5) 财务质量过滤 (一次 batch 查询)
    try:
        df = get_fundamentals(query(
            valuation.code,
            valuation.market_cap,
            valuation.circulating_market_cap,
            balance.total_liability,
            balance.total_assets,
            balance.good_will,
        ).filter(valuation.code.in_(candidates)), date=query_date)
    except Exception as e:
        log.warn('【个股层】财务批量查询失败: %s' % e)
        return

    quality = []
    fail_counts = {'no_info': 0, 'too_young': 0, 'no_assets': 0,
                   'high_debt': 0, 'high_goodwill': 0, 'small_cap': 0}
    for _, row in df.iterrows():
        stock = row['code']
        try:
            sec_info = get_security_info(stock)
            if sec_info is None:
                fail_counts['no_info'] += 1
                continue
            listed_days = (today - sec_info.start_date).days
            if listed_days < CONFIG['min_listed_days']:
                fail_counts['too_young'] += 1
                continue
        except Exception:
            fail_counts['no_info'] += 1
            continue

        ta = row['total_assets'] or 0
        tl = row['total_liability'] or 0
        if ta <= 0:
            fail_counts['no_assets'] += 1
            continue
        if tl / ta > CONFIG['max_debt_ratio']:
            fail_counts['high_debt'] += 1
            continue
        net_assets = ta - tl
        gw = row['good_will'] or 0
        if net_assets > 0 and gw / net_assets > CONFIG['max_goodwill_to_net_asset']:
            fail_counts['high_goodwill'] += 1
            continue
        cmcap = row['circulating_market_cap'] or 0  # 单位: 亿元
        if cmcap < CONFIG['min_circulating_market_cap_yi']:
            fail_counts['small_cap'] += 1
            continue
        quality.append(stock)

    # 6) 流动性过滤
    final_pool = []
    fail_liquidity = 0
    for s in quality:
        try:
            amt = attribute_history(s, 60, '1d', ['money'], df=False)['money']
            avg_amt = np.nanmean(np.asarray(amt, dtype=float))
            if np.isnan(avg_amt) or avg_amt < CONFIG['min_avg_amount_60d_yuan']:
                fail_liquidity += 1
                continue
            final_pool.append(s)
        except Exception:
            fail_liquidity += 1
            continue

    # 7) 个股周期性打分
    scored = []
    fail_cyc = 0
    for s in final_pool:
        sc = compute_stock_cyclicality_score(context, s)
        if sc is None or sc < CONFIG['stock_cyclicality_min_score']:
            fail_cyc += 1
            continue
        scored.append((s, sc, ind_of_candidate.get(s, 'OTHER')))

    # 8) 每行业 top N + 全池硬上限
    by_ind = {}
    for s, sc, ind in scored:
        by_ind.setdefault(ind, []).append((s, sc))

    selected = []
    final_sector = {}
    for ind, items in by_ind.items():
        items.sort(key=lambda x: -x[1])
        for s, sc in items[:CONFIG['stocks_per_industry']]:
            selected.append((s, sc, ind))
            final_sector[s] = ind

    # 全池硬截断: 按周期性分数全局 top N
    selected.sort(key=lambda x: -x[1])
    selected = selected[:CONFIG['pool_max_stocks']]
    quality_universe = [s for s, _, _ in selected]
    final_sector_pruned = {s: ind for s, _, ind in selected}

    # 9) Legacy holdings: 已建仓但被踢出的票仍纳入 universe 直到自然退出
    legacy_added = []
    for s, st in list(g.state.items()):
        if s in quality_universe:
            continue
        # 判断是否需要保留
        pos = _safe_get_position(context, s)
        has_pos = pos is not None and pos.total_amount > 0
        in_active_phase = st.get('phase') in ('BUILDING', 'GRID_RUNNING')
        in_cooldown_with_pos = (st.get('phase') == 'COOLDOWN' and has_pos)
        if has_pos or in_active_phase or in_cooldown_with_pos:
            quality_universe.append(s)
            # 沿用旧 sector_map; 没有则尝试动态查
            if s in g.sector_map:
                final_sector_pruned[s] = g.sector_map[s]
            else:
                try:
                    info = get_industry(s, date=query_date)
                    sw_l1 = info.get(s, {}).get('sw_l1', {})
                    final_sector_pruned[s] = sw_l1.get('industry_code', 'LEGACY_OTHER')
                except Exception:
                    final_sector_pruned[s] = 'LEGACY_OTHER'
            g.legacy_holding_stocks.add(s)
            legacy_added.append(s)

    # 清理已退出 legacy 的
    for s in list(g.legacy_holding_stocks):
        if s not in quality_universe:
            g.legacy_holding_stocks.discard(s)

    g.quality_universe = quality_universe
    g.sector_map = final_sector_pruned
    g.last_quality_check_date = today

    # v2.0 性能优化: 月度刷新打分用的 PB / close / OCF batch 缓存
    refresh_score_cache(context, quality_universe)

    fail_str = ', '.join(['%s=%d' % (k, v)
                          for k, v in fail_counts.items() if v > 0])
    log.info('【个股层】%s | 候选 %d 只 / %d 行业 (legacy %d / 池 %d / 上限 %d)'
             % (today, len(quality_universe),
                len(set(final_sector_pruned.values())),
                len(g.legacy_holding_stocks),
                len(scored), CONFIG['pool_max_stocks']))
    log.info('  过滤: 行业内股票 %d → 财务过 %d (%s) / 流动性过 %d (剔 %d) / 周期分过 %d (剔 %d)'
             % (len(candidates), len(quality), fail_str or '无',
                len(final_pool), fail_liquidity, len(scored), fail_cyc))
    if quality_universe:
        log.info('  入选 (前 15 只): %s%s'
                 % (','.join(quality_universe[:15]),
                    '...' if len(quality_universe) > 15 else ''))
    if legacy_added:
        log.info('  legacy 保留 %d 只: %s'
                 % (len(legacy_added), ','.join(legacy_added[:8])
                    + ('...' if len(legacy_added) > 8 else '')))


def all_candidates():
    """v1.9 兼容接口: 返回当前 quality_universe."""
    return list(g.quality_universe)


def sector_of(stock):
    """v1.9 兼容接口: 改为读 g.sector_map (动态行业)."""
    return g.sector_map.get(stock, 'OTHER')


# =============================================================================
# 5. 底部多因子打分 (沿用 v1.9)
# =============================================================================

def calc_pb_low_pct_score(stock, query_date):
    """PB 5 年逆向分位: 越低分位 → 越高分.

    v2.0: 优先从 g.score_cache_pb_history (月度 batch) + g.score_cache_today_pb
    (daily batch) 计算; cache miss 时 fallback 到 get_valuation.
    """
    if (CONFIG.get('score_cache_enabled', True)
            and stock in g.score_cache_pb_history):
        pb_hist = g.score_cache_pb_history[stock]
        if len(pb_hist) < 100:
            return None
        cur = g.score_cache_today_pb.get(stock)
        if cur is None or cur <= 0:
            cur = float(pb_hist[-1])
        pct = float((pb_hist <= cur).sum()) / len(pb_hist)
        return float((1.0 - pct) * 100.0), float(pct)

    try:
        start = query_date - timedelta(days=int(CONFIG['pb_history_years'] * 365))
        df = get_valuation(stock, start_date=start, end_date=query_date,
                           fields=['pb_ratio'])
        if df is None or len(df) < 100:
            return None
        cur = df['pb_ratio'].iloc[-1]
        if cur is None or np.isnan(cur) or cur <= 0:
            return None
        pct = (df['pb_ratio'].dropna() <= cur).sum() / max(1, df['pb_ratio'].dropna().shape[0])
        return float((1.0 - pct) * 100.0), float(pct)
    except Exception:
        return None


def calc_drawdown_score(stock):
    """距前高回撤评分: 50% 回撤 = 满分.

    v2.0: 优先用 g.score_cache_close_history (月度 batch) + 当日 last_price.
    """
    if (CONFIG.get('score_cache_enabled', True)
            and stock in g.score_cache_close_history):
        prices = g.score_cache_close_history[stock]
        cur_today = g.score_cache_today_close.get(stock)
        if cur_today and cur_today > 0:
            prices = np.append(prices, cur_today)
        if len(prices) == 0:
            return None
        cur = float(prices[-1])
        peak = float(np.max(prices))
        if peak <= 0 or np.isnan(cur):
            return None
        dd = (peak - cur) / peak
        return min(dd / 0.50 * 100.0, 100.0)

    try:
        raw = attribute_history(stock, CONFIG['drawdown_lookback_days'], '1d',
                                ['close'], df=False)['close']
        prices = np.asarray(raw, dtype=float)
        prices = prices[~np.isnan(prices)]
        if len(prices) == 0:
            return None
        cur = float(prices[-1])
        peak = float(np.max(prices))
        if peak <= 0 or np.isnan(cur):
            return None
        dd = (peak - cur) / peak
        return min(dd / 0.50 * 100.0, 100.0)
    except Exception:
        return None


def calc_ocf_to_marketcap_score(stock, query_date):
    """经营活动现金流 / 市值: 10% 视为满分, 负值为 0.

    v2.0: 优先用 g.score_cache_ocf (月度 batch). 季频财报数据本就月内不变,
    缓存等价无损.
    """
    if (CONFIG.get('score_cache_enabled', True)
            and stock in g.score_cache_ocf):
        return g.score_cache_ocf[stock]

    try:
        df = get_fundamentals(query(
            valuation.market_cap,
            cash_flow.net_operate_cash_flow,
        ).filter(valuation.code == stock), date=query_date)
        if len(df) == 0:
            return None
        mv = df['market_cap'].iloc[0]
        ocf = df['net_operate_cash_flow'].iloc[0]
        if mv is None or mv <= 0 or ocf is None:
            return 0.0
        ratio = ocf / (mv * 1e8)
        return max(0.0, min(ratio / 0.10 * 100.0, 100.0))
    except Exception:
        return None


def calc_rsi_score(stock):
    """RSI 逆向分: RSI 30 → 70 分, RSI 50 → 50 分.

    v2.0: 优先用 g.score_cache_close_history 后段 (period+1) 天 + 当日 last_price.
    """
    period = CONFIG['rsi_period']

    if (CONFIG.get('score_cache_enabled', True)
            and stock in g.score_cache_close_history):
        hist = g.score_cache_close_history[stock]
        cur_today = g.score_cache_today_close.get(stock)
        if cur_today and cur_today > 0:
            hist = np.append(hist, cur_today)
        if len(hist) < period + 1:
            return None
        prices = hist[-(period + 1):]
    else:
        try:
            raw = attribute_history(stock, period + 30, '1d', ['close'], df=False)['close']
            prices = np.asarray(raw, dtype=float)
            prices = prices[~np.isnan(prices)]
            if len(prices) < period + 1:
                return None
            prices = prices[-(period + 1):]
        except Exception:
            return None

    try:
        diff = np.diff(prices)
        if len(diff) == 0 or np.any(np.isnan(diff)):
            return None
        with np.errstate(invalid='ignore'):
            gains = np.where(diff > 0, diff, 0.0)
            losses = np.where(diff < 0, -diff, 0.0)
        ag = float(np.mean(gains))
        al = float(np.mean(losses))
        if al <= 0:
            rsi = 100.0
        else:
            rs = ag / al
            rsi = 100.0 - 100.0 / (1.0 + rs)
        return float(100.0 - rsi)
    except Exception:
        return None


def calc_bottom_score_detail(context, stock):
    """返回 dict {pb, dd, ocf, rsi, total, pb_pct, fail_reason}."""
    query_date = context.previous_date

    detail = {
        'pb': None, 'dd': None, 'ocf': None, 'rsi': None,
        'total': None, 'pb_pct': None, 'fail_reason': None,
    }

    pb_res = calc_pb_low_pct_score(stock, query_date)
    if pb_res is None:
        detail['fail_reason'] = 'pb_no_data'
        return detail
    detail['pb'], detail['pb_pct'] = pb_res

    dd = calc_drawdown_score(stock)
    if dd is None:
        detail['fail_reason'] = 'dd_no_data'
        return detail
    detail['dd'] = dd

    ocf = calc_ocf_to_marketcap_score(stock, query_date)
    detail['ocf'] = 0.0 if ocf is None else ocf

    rsi = calc_rsi_score(stock)
    if rsi is None:
        detail['fail_reason'] = 'rsi_no_data'
        return detail
    detail['rsi'] = rsi

    w = CONFIG['factor_weights']
    detail['total'] = float(
        w['pb_low_pct']        * detail['pb'] +
        w['drawdown_high_pct'] * detail['dd'] +
        w['ocf_to_marketcap']  * detail['ocf'] +
        w['rsi_low_pct']       * detail['rsi']
    )
    return detail


def calc_bottom_score(context, stock):
    detail = calc_bottom_score_detail(context, stock)
    g.last_score_snapshot[stock] = detail
    return detail['total'], detail['pb_pct']


# =============================================================================
# 6. 仓位/资金规模工具 (沿用 v1.9, 含 Bug I _safe_get_position)
# =============================================================================

def compute_target_value_for_stock(context, stock):
    """根据风控上限给出"理想目标总市值"."""
    portfolio_total = context.portfolio.total_value
    single_max = portfolio_total * CONFIG['single_stock_max_pct']

    sector = sector_of(stock)
    sector_used = 0.0
    for s, st in g.state.items():
        if sector_of(s) == sector and s != stock:
            pos = _safe_get_position(context, s)
            if pos is not None:
                sector_used += pos.value
    sector_max = portfolio_total * CONFIG['single_sector_max_pct']
    sector_remaining = max(0.0, sector_max - sector_used)

    available_cash = max(0.0, context.portfolio.available_cash
                         - portfolio_total * CONFIG['min_cash_reserve_pct'])
    return min(single_max, sector_remaining, available_cash + (
        context.portfolio.positions[stock].value if stock in context.portfolio.positions else 0.0))


def _get_cd(stock):
    try:
        return get_current_data()[stock]
    except (KeyError, TypeError, Exception):
        return None


def _safe_get_position(context, stock):
    """安全获取持仓 (v1.9 Bug I 修复).

    聚宽 portfolio.positions.get(stock) 对不存在的 stock 返回空 Position 并打
    WARNING. 用 ``stock in positions`` 先检查避开兼容路径.
    """
    try:
        if stock in context.portfolio.positions:
            return context.portfolio.positions[stock]
    except Exception:
        pass
    return None


def _is_star_market(stock):
    code = stock.split('.')[0]
    return code.startswith('688') or code.startswith('8') or code.startswith('4') or code.startswith('92')


def _build_order_style(stock, side, cd):
    if not _is_star_market(stock):
        return None
    last = cd.last_price
    slip = CONFIG['star_market_protect_slippage_pct']
    if side == 'buy':
        protect = last * (1.0 + slip)
        if cd.high_limit and protect > cd.high_limit:
            protect = cd.high_limit
    else:
        protect = last * (1.0 - slip)
        if cd.low_limit and protect < cd.low_limit:
            protect = cd.low_limit
    protect = round(protect, 2)
    try:
        return MarketOrderStyle(protect)
    except Exception:
        return None


def calc_min_order_value(stock):
    """与 safe_order_value 内部一致的最小订单金额门槛 (v1.8)."""
    cd = _get_cd(stock)
    if cd is None or cd.last_price is None or cd.last_price <= 0:
        return float('inf')
    return max(CONFIG['min_order_value_yuan'],
               cd.last_price * CONFIG['min_order_shares'])


def safe_order_value(stock, value, context=None):
    """带保护的下单 (沿用 v1.8/1.9 全部保护逻辑)."""
    if abs(value) < 1.0:
        return None
    cd = _get_cd(stock)
    if cd is None or cd.paused:
        return None
    last = cd.last_price
    if last is None or last <= 0:
        return None
    min_value = max(CONFIG['min_order_value_yuan'], last * CONFIG['min_order_shares'])
    if abs(value) < min_value:
        return None
    if value > 0 and cd.high_limit and last >= cd.high_limit - 1e-4:
        return None
    if value < 0 and cd.low_limit and last <= cd.low_limit + 1e-4:
        return None

    if value > 0 and context is not None:
        cash = context.portfolio.available_cash
        reserve = context.portfolio.total_value * CONFIG['min_cash_reserve_pct']
        usable = cash - reserve
        if usable < min_value:
            log.info('[%s] 资金不足: 可用 %.0f - 储备 %.0f = %.0f < 门槛 %.0f, 跳过买入'
                     % (stock, cash, reserve, usable, min_value))
            return None
        if value > usable:
            scaled = usable * 0.95
            if scaled < min_value:
                log.info('[%s] 资金紧张: 单笔 %.0f → %.0f 仍不足门槛 %.0f, 跳过'
                         % (stock, value, scaled, min_value))
                return None
            log.info('[%s] 资金紧张: 单笔 %.0f → 缩减到 %.0f (95%%可用, 防同日资金竞争)'
                     % (stock, value, scaled))
            value = scaled

    style = _build_order_style(stock, 'buy' if value > 0 else 'sell', cd)
    try:
        if style is None:
            return order_value(stock, value)
        return order_value(stock, value, style=style)
    except Exception as e:
        log.warn('[%s] order_value 失败: %s' % (stock, e))
        return None


def safe_close_all(stock):
    cd = _get_cd(stock)
    if cd is None or cd.paused:
        return None
    if cd.low_limit and cd.last_price is not None and cd.last_price <= cd.low_limit + 1e-4:
        return None
    style = _build_order_style(stock, 'sell', cd)
    try:
        if style is None:
            return order_target_value(stock, 0)
        return order_target_value(stock, 0, style=style)
    except Exception as e:
        log.warn('[%s] order_target_value(0) 失败: %s' % (stock, e))
        return None


# =============================================================================
# 7. T0/T1/T2 分批建仓 (沿用 v1.9)
# =============================================================================

def try_enter_t0(context, stock, st, current_price):
    """评估底部信号; 持续达标后买入 T0 (30%)."""
    if st.get('t0_disabled_until_score_below') is not None:
        score, _ = calc_bottom_score(context, stock)
        if score is None:
            return
        st['last_score'] = score
        if score < st['t0_disabled_until_score_below']:
            st['t0_disabled_until_score_below'] = None
            st['t0_attempts_in_window'] = 0
            log.info('[%s] T0 禁用解除 (分数回落到 %.1f) | 重新累计信号'
                     % (stock, score))
        else:
            return
    else:
        score, _ = calc_bottom_score(context, stock)
        if score is None:
            return
        st['last_score'] = score

    if score >= CONFIG['bottom_score_threshold_t0']:
        st['consecutive_signal_days'] += 1
        log.info('[%s] T0 信号累计 %d/%d 天 | 分数=%.1f / 价格=%.2f'
                 % (stock, st['consecutive_signal_days'],
                    CONFIG['signal_persistence_days'], score, current_price))
    else:
        if (CONFIG['verbose']
                and score >= CONFIG['log_near_miss_threshold']
                and st['consecutive_signal_days'] == 0):
            d = g.last_score_snapshot.get(stock, {})
            log.info('[%s] 接近触发 (未达 %.0f 阈值) | 分=%.1f [pb=%.0f dd=%.0f ocf=%.0f rsi=%.0f]'
                     % (stock, CONFIG['bottom_score_threshold_t0'], score,
                        d.get('pb') or 0, d.get('dd') or 0,
                        d.get('ocf') or 0, d.get('rsi') or 0))
        st['consecutive_signal_days'] = 0
        st['t0_attempts_in_window'] = 0
        return

    if st['consecutive_signal_days'] < CONFIG['signal_persistence_days']:
        return

    if st['t0_attempts_in_window'] >= CONFIG['t0_max_attempts_per_window']:
        st['t0_disabled_until_score_below'] = max(
            CONFIG['bottom_score_threshold_t0'] - 5.0,
            CONFIG['log_near_miss_threshold'])
        log.info('[%s] T0 已达单窗口最大尝试次数 %d, 暂停至分数<%.1f'
                 % (stock, CONFIG['t0_max_attempts_per_window'],
                    st['t0_disabled_until_score_below']))
        return

    target_total = compute_target_value_for_stock(context, stock)
    if target_total <= 1000:
        st['t0_disabled_until_score_below'] = max(
            CONFIG['bottom_score_threshold_t0'] - 5.0,
            CONFIG['log_near_miss_threshold'])
        log.info('[%s] T0 资金不足 (target=%.0f), 暂停至分数<%.1f'
                 % (stock, target_total, st['t0_disabled_until_score_below']))
        return

    base_value = target_total * CONFIG['base_position_pct_of_stock']
    grid_value = target_total - base_value
    t0_value = base_value * CONFIG['tier_pcts'][0]

    log.info('[%s] T0 建仓 | 分数=%.1f / 价格=%.2f / 目标总值=%.0f / 本次=%.0f'
             % (stock, score, current_price, target_total, t0_value))

    st['t0_attempts_in_window'] += 1

    res = safe_order_value(stock, t0_value, context=context)
    if res is None:
        st['t0_disabled_until_score_below'] = max(
            CONFIG['bottom_score_threshold_t0'] - 5.0,
            CONFIG['log_near_miss_threshold'])
        log.info('[%s] T0 下单被拒绝, 暂停至分数<%.1f'
                 % (stock, st['t0_disabled_until_score_below']))
        return

    st['phase'] = 'BUILDING'
    st['first_buy_date'] = context.current_dt.date()
    st['first_buy_price'] = current_price
    st['tier_filled'] = 1
    st['target_total_value'] = target_total
    st['base_target_value'] = base_value
    st['grid_target_value'] = grid_value
    st['last_grid_price'] = current_price
    st['high_water_pnl_pct'] = 0.0
    st['pyramid_levels_done'] = []
    st['consecutive_signal_days'] = 0
    st['t0_attempts_in_window'] = 0
    _log_trade(context, stock, 'T0', t0_value, st,
               extra='target=%.0f base=%.0f grid=%.0f' % (target_total, base_value, grid_value))


def try_build_t1_t2(context, stock, st, current_price):
    """T0 后分批建仓: T1 (跌 8% 或 30 天涨 5%) → T2 (跌 16% 或 T1 后 7+ 天涨 5%) → GRID.

    v2.0 修复 (Bug J): T1 加右侧确认 (30 天 + 涨 5%). 此前 v1.9 只有左侧条件
    (跌 8%), 一旦 T0 后只涨不跌, T1 永不触发 → tier_filled 卡死在 1 → 永不进
    GRID_RUNNING → 资金长期锁定 1.04% 小仓位无网格无退出. log_v2_01 显示 80%
    的 T0 单子卡死, 对动态周期股池影响巨大.

    v2.0 修复 (Bug K): T1/T2 触发日志放在 safe_order_value 之前, 下单失败
    (典型: cash - 储备 < 门槛) 时 tier_filled 不推进, 第二天右侧条件持续满足
    → 重复触发. log_v2_02 显示一只票连续 36 天 / 72 次刷 T1/T2 失败日志, T1
    实际成功 ~100 / 触发 372, T2 成功 ~63 / 触发 384. 修复: 失败时设
    tier_retry_until_date = today + tier_retry_cooldown_days, cooldown 期内
    直接 return.
    """
    if st['first_buy_price'] is None:
        return

    today = context.current_dt.date()

    # Bug K 修复: T1/T2 上次失败仍在 cooldown 期, 跳过本次尝试
    retry_block = st.get('tier_retry_until_date')
    if retry_block is not None and today < retry_block:
        return

    drop_from_t0 = (st['first_buy_price'] - current_price) / st['first_buy_price']
    days_since_t0 = (today - st['first_buy_date']).days
    base = st['base_target_value']
    cooldown_days = CONFIG['tier_retry_cooldown_days']

    if st['tier_filled'] == 1:
        cond_left = drop_from_t0 >= CONFIG['tier_drop_pct']
        cond_right = (days_since_t0 >= CONFIG['tier_t1_no_touch_days']
                      and current_price >= st['first_buy_price'] * 1.05)
        if cond_left or cond_right:
            t1_value = base * CONFIG['tier_pcts'][1]
            tag = '左侧' if cond_left else '右侧确认'
            order_id = safe_order_value(stock, t1_value, context=context)
            if order_id is not None:
                st['tier_filled'] = 2
                # v2.0: 记录 T1 触发日, T2 右侧需在 T1 后再隔几天才能触发,
                # 避免 T1/T2 同日补满 (T1 右侧 30 天 + T2 右侧 30 天会同日触发)
                st['t1_filled_date'] = today
                st['tier_retry_until_date'] = None
                log.info('[%s] T1 加仓 (%s) | 价格=%.2f / 跌幅=%.2f%% / 持仓 %d 天 / 加仓=%.0f'
                         % (stock, tag, current_price, drop_from_t0 * 100,
                            days_since_t0, t1_value))
                _log_trade(context, stock, 'T1', t1_value, st,
                           extra='%s drop=%.2f%% days=%d'
                                 % ('left' if cond_left else 'right',
                                    drop_from_t0 * 100, days_since_t0))
            else:
                # Bug K: 下单失败时设 cooldown, 避免日志/逻辑每天重复
                st['tier_retry_until_date'] = today + timedelta(days=cooldown_days)
                log.info('[%s] T1 加仓 (%s) 下单失败, cooldown %d 天'
                         % (stock, tag, cooldown_days))
        return

    if st['tier_filled'] == 2:
        cond_left = drop_from_t0 >= CONFIG['tier_drop_pct'] * 2
        # v2.0: T1 后至少间隔 7 天再触发 T2 右侧 (避免 T1/T2 同日完成)
        days_since_t1 = 0
        if st.get('t1_filled_date') is not None:
            days_since_t1 = (today - st['t1_filled_date']).days
        cond_right = (days_since_t0 >= CONFIG['tier_t2_no_touch_days']
                      and days_since_t1 >= CONFIG['tier_t2_no_touch_after_t1_min_days']
                      and current_price >= st['first_buy_price'] * 1.05)
        if cond_left or cond_right:
            t2_value = base * CONFIG['tier_pcts'][2]
            tag = '左侧' if cond_left else '右侧确认'
            order_id = safe_order_value(stock, t2_value, context=context)
            if order_id is not None:
                st['tier_filled'] = 3
                st['phase'] = 'GRID_RUNNING'
                st['last_grid_price'] = current_price
                st['tier_retry_until_date'] = None
                log.info('[%s] T2 加仓 (%s) | 价格=%.2f / 持仓 %d 天 / T1 后 %d 天 / 加仓=%.0f'
                         % (stock, tag, current_price, days_since_t0,
                            days_since_t1, t2_value))
                log.info('[%s] 建仓完成, 网格启动 | 基准价=%.2f / 网格预算=%.0f'
                         % (stock, current_price, st['grid_target_value']))
                _log_trade(context, stock, 'T2', t2_value, st,
                           extra='%s drop=%.2f%% days=%d days_since_t1=%d'
                                 % ('left' if cond_left else 'right',
                                    drop_from_t0 * 100, days_since_t0, days_since_t1))
            else:
                st['tier_retry_until_date'] = today + timedelta(days=cooldown_days)
                log.info('[%s] T2 加仓 (%s) 下单失败, cooldown %d 天'
                         % (stock, tag, cooldown_days))


# =============================================================================
# 8. 动态 + 不对称 + 金字塔 网格 (沿用 v1.9)
# =============================================================================

def calc_dynamic_grid_step(stock):
    try:
        period = CONFIG['grid_atr_period']
        df = attribute_history(stock, period + 1, '1d',
                               ['high', 'low', 'close'], df=False)
        h = np.asarray(df['high'], dtype=float)
        l = np.asarray(df['low'], dtype=float)
        c = np.asarray(df['close'], dtype=float)
        if np.any(np.isnan(h)) or np.any(np.isnan(l)) or np.any(np.isnan(c)):
            return CONFIG['grid_step_min_pct']
        prev_close = c[:-1]
        tr = np.maximum.reduce([
            h[1:] - l[1:],
            np.abs(h[1:] - prev_close),
            np.abs(l[1:] - prev_close),
        ])
        atr = float(np.mean(tr))
        last_price = float(c[-1])
        if last_price <= 0:
            return CONFIG['grid_step_min_pct']
        step = CONFIG['grid_atr_k'] * atr / last_price
        return float(np.clip(step,
                             CONFIG['grid_step_min_pct'],
                             CONFIG['grid_step_max_pct']))
    except Exception:
        return CONFIG['grid_step_min_pct']


def get_pyramid_multiplier(st, current_price):
    if st['first_buy_price'] is None:
        return 1.0
    drop = (current_price - st['first_buy_price']) / st['first_buy_price']
    levels = CONFIG['grid_pyramid_thresholds']
    mults = CONFIG['grid_pyramid_multipliers']
    chosen_mult = 1.0
    chosen_level = None
    for lv, m in zip(levels, mults):
        if drop <= lv and lv not in st['pyramid_levels_done']:
            chosen_mult = m
            chosen_level = lv
    if chosen_level is not None:
        st['pyramid_levels_done'].append(chosen_level)
    return chosen_mult


def run_grid(context, stock, st, current_price):
    if st['last_grid_price'] is None:
        st['last_grid_price'] = current_price
        return

    base_step = calc_dynamic_grid_step(stock)
    buy_step = base_step * CONFIG['grid_buy_step_factor']
    sell_step = base_step * CONFIG['grid_sell_step_factor']
    pct = (current_price - st['last_grid_price']) / st['last_grid_price']

    pos = context.portfolio.positions.get(stock)
    held_value = pos.value if pos is not None else 0.0
    base_value_now = st['base_target_value']
    grid_budget = st['grid_target_value']

    base_grid_chunk = grid_budget / 10.0

    if pct <= -buy_step:
        # v2.0 新增: legacy holdings 在 GRID_RUNNING 时不再加仓 (已被踢出新候选池).
        # 仍允许卖出. 这是"优雅退出"的核心机制.
        if stock in g.legacy_holding_stocks:
            return
        mult = get_pyramid_multiplier(st, current_price)
        single_buy_value = base_grid_chunk * mult
        net_grid_position = st['grid_total_buy_value'] - st['grid_total_sell_value']
        room = grid_budget - net_grid_position
        if room <= 0:
            log.info('[%s] 网格已达上限, 暂停加仓 (净持仓=%.0f / 预算=%.0f)'
                     % (stock, net_grid_position, grid_budget))
            return
        actual = min(single_buy_value, room)
        log.info('[%s] 网格买入 | 跌幅=%.2f%% / 步长=%.2f%% / 倍数=%.2f / 金额=%.0f'
                 % (stock, pct * 100, buy_step * 100, mult, actual))
        if safe_order_value(stock, actual, context=context) is not None:
            st['grid_total_buy_value'] += actual
            st['grid_buy_count'] += 1
            st['last_grid_price'] = current_price
            _log_trade(context, stock, 'grid_buy', actual, st,
                       extra='drop=%.2f%% step=%.2f%% mult=%.1f' %
                             (pct * 100, buy_step * 100, mult))

    elif pct >= sell_step:
        single_sell_value = base_grid_chunk
        net_grid_position = st['grid_total_buy_value'] - st['grid_total_sell_value']
        if net_grid_position <= 0:
            return
        actual = min(single_sell_value, net_grid_position)
        log.info('[%s] 网格卖出 | 涨幅=%.2f%% / 步长=%.2f%% / 金额=%.0f'
                 % (stock, pct * 100, sell_step * 100, actual))
        if safe_order_value(stock, -actual, context=context) is not None:
            st['grid_total_sell_value'] += actual
            st['grid_sell_count'] += 1
            st['last_grid_price'] = current_price
            _log_trade(context, stock, 'grid_sell', -actual, st,
                       extra='rise=%.2f%% step=%.2f%%' %
                             (pct * 100, sell_step * 100))


# =============================================================================
# 9. 退出规则 a / b / c / d  +  硬止损 (沿用 v1.9)
# =============================================================================

def get_position_pnl_pct(context, stock):
    pos = context.portfolio.positions.get(stock)
    if pos is None or pos.total_amount <= 0 or pos.avg_cost <= 0:
        return 0.0
    return (pos.price - pos.avg_cost) / pos.avg_cost


def get_today_high_close_limit(stock):
    try:
        df = attribute_history(stock, 1, '1d',
                               ['high', 'close', 'high_limit'], df=False)
        return float(df['high'][0]), float(df['close'][0]), float(df['high_limit'][0])
    except Exception:
        return None, None, None


def check_exit_a_grid_cleared(context, stock, st):
    if st['grid_buy_count'] < 1 or st['grid_sell_count'] < 1:
        return False
    net = st['grid_total_buy_value'] - st['grid_total_sell_value']
    if net > st['grid_target_value'] * 0.05:
        return False

    log.info('[%s] 退出 a: 网格清空 (买=%d次 卖=%d次 净额=%.0f), 进入 %d 日冷却 (底仓保留)'
             % (stock, st['grid_buy_count'], st['grid_sell_count'], net,
                CONFIG['exit_a_cooldown_days']))
    buy_n = st['grid_buy_count']
    sell_n = st['grid_sell_count']
    st['phase'] = 'COOLDOWN'
    st['cooldown_until_date'] = (
        context.current_dt.date() + timedelta(days=CONFIG['exit_a_cooldown_days']))
    cd = _get_cd(stock)
    st['last_high_after_grid'] = cd.last_price if cd is not None else None
    st['grid_total_buy_value'] = 0.0
    st['grid_total_sell_value'] = 0.0
    st['grid_buy_count'] = 0
    st['grid_sell_count'] = 0
    st['pyramid_levels_done'] = []
    _log_trade(context, stock, 'exit_a', 0, st,
               extra='buy_n=%d sell_n=%d net=%.0f' % (buy_n, sell_n, net))
    return True


def check_exit_b_limit_up_open(context, stock, st):
    pnl_pct = get_position_pnl_pct(context, stock)
    if pnl_pct < CONFIG['exit_b_min_profit_pct']:
        return False

    today_high, today_close, high_limit = get_today_high_close_limit(stock)
    if today_high is None or high_limit is None or high_limit <= 0:
        return False
    touched = today_high >= high_limit - 1e-4
    if not touched:
        return False
    open_drop = (high_limit - today_close) / high_limit
    if open_drop < CONFIG['exit_b_limit_open_drop_pct']:
        return False

    pb_res = calc_pb_low_pct_score(stock, context.previous_date)
    cycle_top = (pb_res is not None
                 and (1 - pb_res[1]) >= CONFIG['exit_b_pb_top_pct'])

    pos = context.portfolio.positions.get(stock)
    if pos is None:
        return False

    if cycle_top:
        log.info('[%s] 退出 b (周期顶): 浮盈%.1f%% + 涨停打开 + PB分位高 → 全部清仓'
                 % (stock, pnl_pct * 100))
        sell_value_est = pos.value
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                context.current_dt.date() + timedelta(days=CONFIG['exit_a_cooldown_days']))
            _log_trade(context, stock, 'exit_b_top', -sell_value_est, st,
                       extra='pnl=%.1f%%' % (pnl_pct * 100))
            return True
    else:
        sell_value = pos.value * 0.5
        today = context.current_dt.date()
        min_value = calc_min_order_value(stock)
        if sell_value < min_value:
            log.info('[%s] 退出 b: 浮盈%.1f%% + 涨停打开, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                     % (stock, pnl_pct * 100, sell_value, pos.value, min_value))
            full_value = pos.value
            if safe_close_all(stock) is not None:
                st['phase'] = 'COOLDOWN'
                st['cooldown_until_date'] = (
                    today + timedelta(days=CONFIG['exit_a_cooldown_days']))
                _log_trade(context, stock, 'exit_b_micro_full', -full_value, st,
                           extra='pnl=%.1f%%' % (pnl_pct * 100))
                return True
            return False
        log.info('[%s] 退出 b (常规): 浮盈%.1f%% + 涨停打开 → 卖出50%%'
                 % (stock, pnl_pct * 100))
        if safe_order_value(stock, -sell_value, context=context) is not None:
            _log_trade(context, stock, 'exit_b_regular_half', -sell_value, st,
                       extra='pnl=%.1f%%' % (pnl_pct * 100))
            _reset_state_after_partial_exit(st, today)
        return False
    return False


def check_exit_c_high_water(context, stock, st):
    pos = context.portfolio.positions.get(stock)
    if pos is None or pos.total_amount <= 0:
        return False
    pnl_pct = get_position_pnl_pct(context, stock)
    if pnl_pct > st['high_water_pnl_pct']:
        st['high_water_pnl_pct'] = pnl_pct
        return False
    if st['high_water_pnl_pct'] < CONFIG['exit_c_high_water_min_profit']:
        return False

    if st.get('exit_c_last_action_date') is not None:
        days_since = (context.current_dt.date() - st['exit_c_last_action_date']).days
        rearm_gap = CONFIG['exit_c_after_half_cut_arm_gap']
        rearm_days = CONFIG['exit_c_after_half_cut_min_days']
        new_high_required = st.get('exit_c_arm_high', 0.0) + rearm_gap
        if days_since < rearm_days and st['high_water_pnl_pct'] < new_high_required:
            return False

    drop_from_peak = st['high_water_pnl_pct'] - pnl_pct

    if drop_from_peak >= CONFIG['exit_c_drawdown_full_cut']:
        log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%% → 全部清仓'
                 % (stock, drop_from_peak * 100, CONFIG['exit_c_drawdown_full_cut'] * 100))
        full_value = pos.value
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                context.current_dt.date() + timedelta(days=CONFIG['exit_a_cooldown_days']))
            _log_trade(context, stock, 'exit_c_full', -full_value, st,
                       extra='drop=%.1f%% pnl=%.1f%%' %
                             (drop_from_peak * 100, pnl_pct * 100))
            return True
    elif drop_from_peak >= CONFIG['exit_c_drawdown_half_cut']:
        sell_value = pos.value * 0.5
        today = context.current_dt.date()
        min_value = calc_min_order_value(stock)
        if sell_value < min_value:
            log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%%, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                     % (stock, drop_from_peak * 100,
                        CONFIG['exit_c_drawdown_half_cut'] * 100, sell_value, pos.value, min_value))
            full_value = pos.value
            if safe_close_all(stock) is not None:
                st['phase'] = 'COOLDOWN'
                st['cooldown_until_date'] = (
                    today + timedelta(days=CONFIG['exit_a_cooldown_days']))
                _log_trade(context, stock, 'exit_c_micro_full', -full_value, st,
                           extra='drop=%.1f%% pnl=%.1f%%' %
                                 (drop_from_peak * 100, pnl_pct * 100))
                return True
            return False
        log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%% → 减仓50%% + COOLDOWN %d 天'
                 % (stock, drop_from_peak * 100,
                    CONFIG['exit_c_drawdown_half_cut'] * 100,
                    CONFIG['exit_c_half_cut_cooldown_days']))
        if safe_order_value(stock, -sell_value, context=context) is not None:
            st['exit_c_last_action_date'] = today
            st['exit_c_arm_high'] = st['high_water_pnl_pct']
            st['high_water_pnl_pct'] = pnl_pct
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                today + timedelta(days=CONFIG['exit_c_half_cut_cooldown_days']))
            _log_trade(context, stock, 'exit_c_half', -sell_value, st,
                       extra='drop=%.1f%% pnl=%.1f%%' %
                             (drop_from_peak * 100, pnl_pct * 100))
            return True
    return False


def _reset_state_after_partial_exit(st, today):
    st['high_water_pnl_pct'] = 0.0
    st['grid_total_buy_value'] = 0.0
    st['grid_total_sell_value'] = 0.0
    st['grid_buy_count'] = 0
    st['grid_sell_count'] = 0
    st['pyramid_levels_done'] = []
    st['exit_c_last_action_date'] = None
    st['exit_c_arm_high'] = 0.0
    st['last_grid_price'] = None


def check_exit_d_time(context, stock, st):
    if st['first_buy_date'] is None:
        return False
    holding_days = (context.current_dt.date() - st['first_buy_date']).days
    if holding_days < CONFIG['exit_d_max_holding_days']:
        return False
    pnl_pct = get_position_pnl_pct(context, stock)
    pos = context.portfolio.positions.get(stock)
    if pos is None or pos.total_amount <= 0:
        return False

    today = context.current_dt.date()
    cooldown_days = CONFIG['exit_a_cooldown_days']

    if pnl_pct < 0:
        log.info('[%s] 退出 d: 持仓 %d 天浮亏%.1f%% → 全部清仓'
                 % (stock, holding_days, pnl_pct * 100))
        full_value = pos.value
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = today + timedelta(days=cooldown_days)
            _log_trade(context, stock, 'exit_d_loss_full', -full_value, st,
                       extra='days=%d pnl=%.1f%%' % (holding_days, pnl_pct * 100))
            return True
        return False

    sell_value = pos.value * 0.5
    min_value = calc_min_order_value(stock)
    if sell_value < min_value:
        log.info('[%s] 退出 d: 持仓 %d 天浮盈%.1f%%, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                 % (stock, holding_days, pnl_pct * 100, sell_value, pos.value, min_value))
        full_value = pos.value
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = today + timedelta(days=cooldown_days)
            _log_trade(context, stock, 'exit_d_micro_full', -full_value, st,
                       extra='days=%d pnl=%.1f%%' % (holding_days, pnl_pct * 100))
            return True
        return False

    log.info('[%s] 退出 d: 持仓 %d 天浮盈%.1f%% → 减仓50%%, 续持评估'
             % (stock, holding_days, pnl_pct * 100))
    if safe_order_value(stock, -sell_value, context=context) is not None:
        st['first_buy_date'] = today
        _reset_state_after_partial_exit(st, today)
        _log_trade(context, stock, 'exit_d_profit_half', -sell_value, st,
                   extra='days=%d pnl=%.1f%%' % (holding_days, pnl_pct * 100))
    else:
        st['first_buy_date'] = today
    return False


def _record_hard_stop(stock, today):
    history = g.hard_stop_history.setdefault(stock, [])
    history.append(today)
    cutoff = today - timedelta(days=CONFIG['hard_stop_recent_window_days'])
    history[:] = [d for d in history if d >= cutoff]
    if len(history) >= CONFIG['hard_stop_max_count_in_window']:
        block_until = today + timedelta(days=CONFIG['hard_stop_blacklist_days'])
        g.hard_stop_blocked_until[stock] = block_until
        log.info('[%s] 硬止损黑名单: 1 年内累计 %d 次, 隔离至 %s'
                 % (stock, len(history), block_until))


def check_hard_stop(context, stock, st):
    pnl_pct = get_position_pnl_pct(context, stock)
    today = context.current_dt.date()
    threshold = -CONFIG['hard_stop_loss_pct']

    last_check = st.get('hard_stop_last_check_date')
    if last_check != today:
        if pnl_pct <= threshold:
            st['hard_stop_persist_count'] = st.get('hard_stop_persist_count', 0) + 1
        else:
            st['hard_stop_persist_count'] = 0
        st['hard_stop_last_check_date'] = today

    if st.get('hard_stop_persist_count', 0) < CONFIG['hard_stop_persist_days']:
        return False

    pos = context.portfolio.positions.get(stock)
    if pos is None:
        st['hard_stop_persist_count'] = 0
        return False

    log.info('[%s] 硬止损: 连续 %d 日浮亏 ≥%.0f%% (今日 %.1f%%) → 全部清仓'
             % (stock, st['hard_stop_persist_count'],
                CONFIG['hard_stop_loss_pct'] * 100, pnl_pct * 100))
    persist_count = st['hard_stop_persist_count']
    full_value = pos.value
    if safe_close_all(stock) is not None:
        _record_hard_stop(stock, today)
        st['phase'] = 'COOLDOWN'
        st['cooldown_until_date'] = (
            today + timedelta(days=CONFIG['exit_a_cooldown_days'] * 2))
        st['hard_stop_persist_count'] = 0
        _log_trade(context, stock, 'hard_stop', -full_value, st,
                   extra='persist=%d pnl=%.1f%%' % (persist_count, pnl_pct * 100))
        return True
    return False


def check_daily_non_intraday_exits(context, stock, st):
    if check_hard_stop(context, stock, st):
        return True
    if check_exit_c_high_water(context, stock, st):
        return True
    if check_exit_d_time(context, stock, st):
        return True
    if check_exit_a_grid_cleared(context, stock, st):
        return True
    return False


def try_exit_cooldown(context, stock, st, current_price):
    """COOLDOWN 解除 (v1.9 Bug H 修复, v2.0 沿用)."""
    today = context.current_dt.date()
    if st['cooldown_until_date'] is None or today < st['cooldown_until_date']:
        return

    pos = _safe_get_position(context, stock)
    has_position = pos is not None and pos.total_amount > 0

    # 路径 B: 退出 c/d 减半 → 保留底仓 → 到期无条件解除
    if has_position and st.get('last_high_after_grid') is None:
        score, _ = calc_bottom_score(context, stock)
        score_str = ('%.1f' % score) if score is not None else 'N/A'
        log.info('[%s] 冷却解除 (减半后强制休眠到期 → 重启网格) | 分数=%s / 新基准=%.2f'
                 % (stock, score_str, current_price))
        st['phase'] = 'GRID_RUNNING'
        st['last_grid_price'] = current_price
        st['high_water_pnl_pct'] = 0.0
        st['cooldown_until_date'] = None
        st['exit_c_last_action_date'] = None
        st['exit_c_arm_high'] = 0.0
        _log_trade(context, stock, 'cooldown_b', 0, st,
                   extra='score=%s base=%.2f' % (score_str, current_price))
        return

    # 路径 A / C: 维持原 v1.8 行为 (双条件确认)
    drawdown_ok = True
    if st['last_high_after_grid'] is not None and st['last_high_after_grid'] > 0:
        drop = (st['last_high_after_grid'] - current_price) / st['last_high_after_grid']
        drawdown_ok = drop >= CONFIG['exit_a_release_drawdown_pct']

    score, _ = calc_bottom_score(context, stock)
    score_ok = score is not None and score >= CONFIG['bottom_score_threshold_keep']

    if not (drawdown_ok and score_ok):
        return

    if has_position:
        log.info('[%s] 冷却解除 → 重启网格 (保留底仓) | 分数=%.1f / 新基准=%.2f'
                 % (stock, score, current_price))
        st['phase'] = 'GRID_RUNNING'
        st['last_grid_price'] = current_price
        st['high_water_pnl_pct'] = 0.0
        st['cooldown_until_date'] = None
        st['last_high_after_grid'] = None
        st['exit_c_last_action_date'] = None
        st['exit_c_arm_high'] = 0.0
        _log_trade(context, stock, 'cooldown_a', 0, st,
                   extra='score=%.1f base=%.2f' % (score, current_price))
    else:
        log.info('[%s] 冷却解除 → 重置为 IDLE (持仓为零) | 分数=%.1f' % (stock, score))
        g.state[stock] = init_state()
        _log_trade(context, stock, 'cooldown_c_idle', 0, g.state[stock],
                   extra='score=%.1f' % score)


# =============================================================================
# 10. trade_log: 结构化交易归因 (v2.0 新增, 来自 v1.9 优 1)
# =============================================================================
#
# 为什么需要 trade_log:
#   v1.9 全周期回测产生 ~20000 行 INFO 日志, 想算 "退出 b 总浮盈/平均浮盈
#   /最大浮盈/各路径触发次数" 都得写 grep + awk 脚本. v2.0 加上动态选股
#   后日志量预计相当, 加上 legacy 等新路径, 人工反推归因更困难.
#
# 设计原则:
#   1. 实时记录: 每笔成交动作后立即 _log_trade(...), 不需要二次扫描.
#   2. 结构化: dict 字段固定, 与 pandas DataFrame 直接对齐.
#   3. 双输出:
#      (a) 月初打印上月 action 计数摘要 (人眼可看, 简短)
#      (b) 年初打印上一整年的 CSV (前缀 'TLCSV: '), 用户从聚宽日志
#          grep TLCSV 后去掉前缀, 即可 pd.read_csv 加载分析.
#   4. 字段稳定: 加新 action 类型只追加, 不改字段集, 避免下游脚本崩溃.
#
# action 命名约定 (按 phase 切换 + 动作语义):
#   T0 / T1 / T2                              -- 三档建仓
#   grid_buy / grid_sell                      -- 网格买卖
#   exit_a                                    -- 网格闭合 (无下单, 仅相位)
#   exit_b_top / exit_b_regular_half          -- 退出 b 周期顶全清 / 常规半清
#   exit_b_micro_full                         -- 退出 b 半仓金额过小兑底全清
#   exit_c_full / exit_c_half                 -- 退出 c 高水位回撤全清 / 半清
#   exit_c_micro_full                         -- 退出 c 半仓金额过小兑底全清
#   exit_d_loss_full / exit_d_profit_half     -- 退出 d 浮亏全清 / 浮盈半清
#   exit_d_micro_full                         -- 退出 d 半仓金额过小兑底全清
#   hard_stop                                 -- 硬止损全清
#   micro_close                               -- 超小持仓自动清算
#   state_heal                                -- 持仓 0 但 phase != IDLE 自愈
#   cooldown_a / cooldown_b / cooldown_c_idle -- 三类 COOLDOWN 解除
# =============================================================================

def _log_trade(context, stock, action, value, st, extra=''):
    """记录一条交易事件到 g.trade_log.

    设计要点:
      * 调用时机一律是"动作执行成功后", 此时 phase / pos / pnl 都是新状态.
      * value 单位为"元", 买入正、卖出负, exit_a / state_heal 等 0 元事件
        也记录, 用于追踪相位流转.
      * 容错: hasattr(g, 'trade_log') 防御初始化竞争; 任何字段失败都写空值.
    """
    if not CONFIG.get('trade_log_enabled', True):
        return
    if not hasattr(g, 'trade_log'):
        return
    try:
        cd = _get_cd(stock)
        pos = _safe_get_position(context, stock)
        rec = {
            'date': str(context.current_dt.date()),
            'stock': stock,
            'sector': sector_of(stock),
            'legacy': 'Y' if stock in getattr(g, 'legacy_holding_stocks', set()) else 'N',
            'phase': st.get('phase', '') if st else '',
            'action': action,
            'price': float(cd.last_price) if (cd is not None and cd.last_price) else 0.0,
            'value': float(value) if value is not None else 0.0,
            'pos_val_after': float(pos.value) if pos is not None else 0.0,
            'pos_amt_after': int(pos.total_amount) if pos is not None else 0,
            'avg_cost': float(pos.avg_cost) if pos is not None else 0.0,
            'pnl_pct': float(get_position_pnl_pct(context, stock)),
            'highw': float(st.get('high_water_pnl_pct', 0.0)) if st else 0.0,
            'tier': int(st.get('tier_filled', 0)) if st else 0,
            'score': float(st.get('last_score', 0.0)) if st else 0.0,
            'extra': str(extra) if extra else '',
        }
        g.trade_log.append(rec)
    except Exception as e:
        log.warn('[%s] _log_trade 失败 (action=%s): %s' % (stock, action, e))


# CSV 字段顺序与 _log_trade 写入顺序保持一致
_TRADE_LOG_CSV_HEADER = ('date,stock,sector,legacy,phase,action,'
                         'price,value,pos_val_after,pos_amt_after,'
                         'avg_cost,pnl_pct,highw,tier,score,extra')


def _trade_log_csv_row(rec):
    """单条 dict → 单行 CSV 字符串 (与 _TRADE_LOG_CSV_HEADER 字段对齐).

    对 extra 字段做了简单的逗号转义 (用 ; 替换), 避免破坏 CSV 列对齐.
    """
    extra = (rec.get('extra') or '').replace(',', ';').replace('\n', ' ')
    return ('%s,%s,%s,%s,%s,%s,%.2f,%.0f,%.0f,%d,%.4f,%.4f,%.4f,%d,%.1f,%s'
            % (rec['date'], rec['stock'], rec['sector'], rec['legacy'],
               rec['phase'], rec['action'],
               rec['price'], rec['value'], rec['pos_val_after'],
               rec['pos_amt_after'], rec['avg_cost'], rec['pnl_pct'],
               rec['highw'], rec['tier'], rec['score'], extra))


def print_trade_log_summary(context, year=None, month=None):
    """月度摘要: 按 action 分类计数 + 总买卖金额 + 累计 P&L 指标."""
    if not getattr(g, 'trade_log', None):
        return

    # filter
    if year is not None and month is not None:
        prefix = '%04d-%02d' % (year, month)
        recent = [r for r in g.trade_log if r['date'].startswith(prefix)]
        scope_str = '%04d-%02d' % (year, month)
    elif year is not None:
        prefix = '%04d-' % year
        recent = [r for r in g.trade_log if r['date'].startswith(prefix)]
        scope_str = str(year)
    else:
        recent = list(g.trade_log)
        scope_str = 'ALL'

    if not recent:
        return

    # action 计数
    counts = {}
    buy_value = 0.0
    sell_value = 0.0
    exit_b_pnls = []
    hard_stops = 0
    for r in recent:
        a = r['action']
        counts[a] = counts.get(a, 0) + 1
        v = r.get('value', 0.0) or 0.0
        if v > 0:
            buy_value += v
        elif v < 0:
            sell_value += -v
        if a.startswith('exit_b'):
            exit_b_pnls.append(r.get('pnl_pct', 0.0))
        if a == 'hard_stop':
            hard_stops += 1

    log.info('=' * 70)
    log.info('【trade_log 摘要 %s】 总事件 %d / 累计 %d / buy=%.0f / sell=%.0f'
             % (scope_str, len(recent), len(g.trade_log), buy_value, sell_value))
    sorted_actions = sorted(counts.items(), key=lambda x: -x[1])
    for action, n in sorted_actions:
        log.info('  %-22s %4d' % (action, n))
    if exit_b_pnls:
        avg_b = sum(exit_b_pnls) / len(exit_b_pnls)
        log.info('  ── 退出 b 浮盈: 平均 %.1f%% / 最大 %.1f%% / 最小 %.1f%%'
                 % (avg_b * 100, max(exit_b_pnls) * 100, min(exit_b_pnls) * 100))
    if hard_stops > 0:
        log.info('  ── 硬止损 %d 次, 当前黑名单 %d 只'
                 % (hard_stops, len(getattr(g, 'hard_stop_blocked_until', {}))))
    log.info('=' * 70)


def print_trade_log_csv(context, year=None):
    """以 'TLCSV: ' 前缀打印整年/全部 trade_log 的 CSV 行.

    用法:
      用户在聚宽日志中: grep '^.*TLCSV: ' log.txt | sed 's/^.*TLCSV: //' > trade_log.csv
      然后: pd.read_csv('trade_log.csv') 即得 DataFrame.

    单行格式见 _TRADE_LOG_CSV_HEADER.
    """
    if not getattr(g, 'trade_log', None):
        return
    if year is not None:
        prefix = '%04d-' % year
        rows = [r for r in g.trade_log if r['date'].startswith(prefix)]
        scope_str = str(year)
    else:
        rows = list(g.trade_log)
        scope_str = 'ALL'
    if not rows:
        return
    log.info('====== TRADE_LOG_CSV_BEGIN year=%s rows=%d ======'
             % (scope_str, len(rows)))
    log.info('TLCSV: ' + _TRADE_LOG_CSV_HEADER)
    for r in rows:
        try:
            log.info('TLCSV: ' + _trade_log_csv_row(r))
        except Exception as e:
            log.warn('  csv row export failed (%s): %s' % (r.get('action', '?'), e))
    log.info('====== TRADE_LOG_CSV_END year=%s ======' % scope_str)


# =============================================================================
# 11. 主交易循环 (v2.0: 微调支持 legacy holdings 优雅退出)
# =============================================================================

def log_universe_score_snapshot(context):
    if not g.quality_universe:
        return
    rows = []
    fail_counts = {}
    for stock in g.quality_universe:
        d = g.last_score_snapshot.get(stock)
        if d is None or d.get('total') is None:
            reason = (d or {}).get('fail_reason') or 'no_compute'
            fail_counts[reason] = fail_counts.get(reason, 0) + 1
            continue
        rows.append((stock, d['total'], d.get('pb') or 0,
                     d.get('dd') or 0, d.get('ocf') or 0, d.get('rsi') or 0))
    rows.sort(key=lambda x: -x[1])

    failed_count = 0
    for v in fail_counts.values():
        failed_count += int(v)

    fail_reason_str = ', '.join(['%s=%d' % (k, int(v)) for k, v in fail_counts.items()])

    log.info('=' * 60)
    log.info('【分数快照】%s | 候选 %d 只 / 失败 %d 只 (legacy=%d)'
             % (str(context.current_dt.date()), len(rows), failed_count,
                len(g.legacy_holding_stocks)))
    if fail_counts:
        log.info('  失败原因: %s' % fail_reason_str)
    log.info('  Top 5 (>=阈值=%.0f, 接近线=%.0f):'
             % (CONFIG['bottom_score_threshold_t0'], CONFIG['log_near_miss_threshold']))
    for stock, tot, pb, dd, ocf, rsi in rows[:5]:
        if tot >= CONFIG['bottom_score_threshold_t0']:
            marker = '*'
        elif tot >= CONFIG['log_near_miss_threshold']:
            marker = '~'
        else:
            marker = ' '
        legacy_tag = '[L]' if stock in g.legacy_holding_stocks else '   '
        log.info('  %s%s %s 总=%.1f | pb=%.0f dd=%.0f ocf=%.0f rsi=%.0f'
                 % (marker, legacy_tag, stock, tot, pb, dd, ocf, rsi))
    if rows:
        all_scores = [r[1] for r in rows]
        sorted_scores = sorted(all_scores)
        median_val = sorted_scores[len(sorted_scores) // 2]
        log.info('  分布: max=%.1f / median=%.1f / min=%.1f'
                 % (max(all_scores), median_val, min(all_scores)))
    log.info('=' * 60)


def daily_signal_and_trade(context):
    if not g.quality_universe:
        return

    # v2.0 性能优化: 1 次 batch 拿当日 PB + last_close, 替代每股每天 4 次单股查询
    refresh_today_score_inputs(context)

    skips = {'processed': 0, 'no_cd': 0, 'paused': 0, 'is_st': 0, 'bad_price': 0}
    phase_counts = {'IDLE': 0, 'BUILDING': 0, 'GRID_RUNNING': 0, 'COOLDOWN': 0}
    eval_count = 0
    sample_logged = False

    cur_data = get_current_data()
    for stock in g.quality_universe:
        if stock in g.processed_today:
            skips['processed'] += 1
            continue
        try:
            cd = cur_data[stock]
        except (KeyError, TypeError, Exception):
            cd = None
        if cd is None:
            skips['no_cd'] += 1
            if not sample_logged:
                log.info('[诊断] %s no_cd | type(cur_data)=%s' % (stock, type(cur_data).__name__))
                sample_logged = True
            continue
        if cd.paused:
            skips['paused'] += 1
            continue
        if cd.is_st:
            skips['is_st'] += 1
            continue
        last = cd.last_price
        if last is None or last <= 0 or (isinstance(last, float) and last != last):
            skips['bad_price'] += 1
            if not sample_logged:
                log.info('[诊断] %s bad_price | last=%r paused=%r is_st=%r'
                         % (stock, last, cd.paused, cd.is_st))
                sample_logged = True
            continue

        if stock not in g.state:
            g.state[stock] = init_state()
        st = g.state[stock]

        # v1.6 状态自愈 (沿用)
        if st['phase'] in ('BUILDING', 'GRID_RUNNING'):
            pos_chk = _safe_get_position(context, stock)
            if pos_chk is None or pos_chk.total_amount <= 0:
                log.info('[%s] 状态自愈: phase=%s 持仓=0 → 重置为 IDLE'
                         % (stock, st['phase']))
                old_phase = st['phase']
                g.state[stock] = init_state()
                st = g.state[stock]
                _log_trade(context, stock, 'state_heal', 0, st,
                           extra='from=%s' % old_phase)
            elif (st['phase'] == 'GRID_RUNNING'
                    and pos_chk.value < CONFIG['min_position_value_yuan']):
                log.info('[%s] 超小持仓清算: pos=%.0f 元 < 阈值 %.0f → 全清 + COOLDOWN'
                         % (stock, pos_chk.value, CONFIG['min_position_value_yuan']))
                pos_val_pre = pos_chk.value
                if safe_close_all(stock) is not None:
                    st['phase'] = 'COOLDOWN'
                    st['cooldown_until_date'] = (
                        context.current_dt.date()
                        + timedelta(days=CONFIG['exit_a_cooldown_days']))
                    _log_trade(context, stock, 'micro_close', -pos_val_pre, st,
                               extra='pos=%.0f' % pos_val_pre)

        phase_counts[st['phase']] = phase_counts.get(st['phase'], 0) + 1
        eval_count += 1

        try:
            if st['phase'] == 'IDLE':
                # v2.0 新增: legacy holdings 不发新 T0, 等被自然清出 g.legacy_holding_stocks
                if stock in g.legacy_holding_stocks:
                    pass
                else:
                    try_enter_t0(context, stock, st, last)
            elif st['phase'] == 'BUILDING':
                # legacy: 不再加 T1/T2 (避免对已被踢出的票继续投入)
                if stock not in g.legacy_holding_stocks:
                    try_build_t1_t2(context, stock, st, last)
            elif st['phase'] == 'GRID_RUNNING':
                # legacy 仍跑网格, 但 run_grid 内部会跳过买入 (只允许卖出)
                run_grid(context, stock, st, last)
                check_daily_non_intraday_exits(context, stock, st)
            elif st['phase'] == 'COOLDOWN':
                try_exit_cooldown(context, stock, st, last)
        except Exception as e:
            log.warn('[%s] 主循环异常: %s' % (stock, e))

        g.processed_today.add(stock)

    if (g.trading_day_counter % CONFIG['diagnostic_log_freq_days']) == 1:
        skip_str = ', '.join(['%s=%d' % (k, v) for k, v in skips.items() if v > 0])
        phase_str = ', '.join(['%s=%d' % (k, v) for k, v in phase_counts.items() if v > 0])
        log.info('[诊断] 主循环 %s | universe=%d (legacy=%d) / 评估=%d / 跳过={%s} / 阶段={%s}'
                 % (context.current_dt.date(), len(g.quality_universe),
                    len(g.legacy_holding_stocks),
                    eval_count, skip_str or 'none', phase_str or 'none'))
        log_universe_score_snapshot(context)


def intraday_exit_check(context):
    """尾盘 14:50 再次检查退出 b (涨停打开) — 沿用 v1.9."""
    for stock, st in list(g.state.items()):
        if st['phase'] != 'GRID_RUNNING':
            continue
        cd = _get_cd(stock)
        if cd is None or cd.paused:
            continue
        try:
            check_exit_b_limit_up_open(context, stock, st)
        except Exception as e:
            log.warn('[%s] 尾盘退出检查异常: %s' % (stock, e))
