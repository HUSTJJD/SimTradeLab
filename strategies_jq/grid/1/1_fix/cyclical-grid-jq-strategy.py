# coding: utf-8
"""
周期股 · 多因子底部 + 动态网格 策略 (聚宽 JoinQuant 版)
=====================================================================

本策略实现 SimTradeLab/my_docs/cyclical-grid-strategy-analysis.md 中的核心思想，
但因聚宽无法获取大宗商品现货价格，已剔除「主营产品价格 5 年逆向分位」因子，
并相应将其权重重新分配到剩下 4 个因子上。

模块组成：
  1. 候选池          : 13 个细分行业、~36 只优质周期股 (硬编码)
                       v1.5 扩补: 钢铁 / 煤炭 / 水泥 / 工程机械 / 海运
  2. 质量过滤        : 资产负债率 / 商誉占比 / 流动性 / 上市时长
                       + 1 年内被硬止损 ≥2 次 → 隔离 1 年 (v1.5)
  3. 底部多因子打分  : PB 5 年逆向分位(.35) + 距前高回撤(.25)
                       + OCF/市值(.20) + 14 日 RSI 逆向(.20)
  4. 分批建仓        : 30% / 30% / 40% 三档 (T0 / T1 / T2)
                       T0 单窗口最多 1 次, 失败后等分数自然回落
  5. 动态网格        : 步长 = clamp(k*ATR/价格, [2.5%, 5%])，
                       不对称 (跌买 ×0.83 / 涨卖 ×1.17)，
                       金字塔加仓 (-3% / -6% / -9% / -12% / -15%+)
  6. 退出规则        : (a) 网格清空 + 60 日冷却 + 底仓保留
                       (b) 盈利 ≥30% + 涨停打开 → 卖 50%/全清(周期顶)
                       (c) 高水位回撤 ≥15% 减半 / ≥25% 全清,
                           减仓后 15 天或新高 +10% 后才再次触发
                       (d) 持仓满 3 年评估
  7. 风控            : 单股 ≤15% / 单行业 ≤30%
                       硬止损 35% + "连续 3 交易日 ≥阈值" 持续过滤 (v1.5)
  8. 科创板兼容      : 688/8/4/92 开头自动加 MarketOrderStyle 保护价

使用方法：
  1. 登录 https://www.joinquant.com/algorithm
  2. 新建策略，框架选 Python
  3. 把本文件全部内容粘贴到代码框
  4. 回测区间建议: 2018-01-01 ~ 2025-12-31 (覆盖完整周期)
  5. 初始资金建议 ≥ 100 万 (低于此值，单股 15% 上限会限制建仓档位)
  6. 频率: 日线
  7. 基准: 沪深 300 (代码中已设置)

主要参数都集中在文件顶部的 CONFIG 字典，可直接调整。
版本: v1.9  (2026-04-28)
v1.9 关键修复 (基于 v1.8 回测 log8.txt 的四次复盘 + log9 验证):
  * 修 Bug H: v1.8 退出 c 减仓 50% 后强制 30 日 COOLDOWN, 但
    try_exit_cooldown 的解除条件是 "cooldown 到期 AND score >= 65". 牛市中
    底部多因子 (PB / 回撤 / RSI 逆向) 都很低分, 整段牛市分数永远 < 65 →
    002714 (牧原) 在 2018-12-20 减仓后被冻结 916 天 (≈ 2.5 年), 直到 2021-06
    熊市来临 score 才回到 65. 这段时间正好是非洲猪瘟大牛, 价格从 25 涨到 92,
    log7 同票拿到了 +184.6% 退出 b, log8 完全错过. 损失约 5-7 万 (单股 15%
    上限计).
    修复: try_exit_cooldown 加快速路径, 当 has_position 且 last_high_after_grid
    为 None (退出 c/d 减半特征) → 到期无条件解除, 立即重启网格. 退出 a 网格
    清空 (会设 last_high_after_grid) 和全清后等 IDLE 的两条路径不变.
    设计哲理: 退出 c 减半 30 天的目的是让 high_water 自然消退打破 002311 类
    切碎循环, 而非 "等再次落入底部信号"; 而退出 a 才是真正的 "高位获利离场,
    等回落再入"——两类 COOLDOWN 不应共用解除条件.
    log9 验证: ★ 路径 B 触发 31 次, 002714 在 2019-01-21 解除冻结后,
    2019-03-06 拿回 +184.6% 退出 b (与 log7 完全一致). 网格买入 +69%,
    末日 GRID_RUNNING 1 → 6.

  * 修 Bug I (log9 暴露): v1.9 修 Bug H 时在 try_exit_cooldown 入口主动调用
    context.portfolio.positions.get(stock) 来判断 has_position. 但聚宽的 .get()
    对不存在的 stock 不返回 None, 而是返回一个空 Position + 打 WARNING. 全清后
    长期 cooldown 但 score < 65 的票 (e.g. 002714 全清后 829 天, 600216 类似)
    每天都触发一次 → log9 累计 2022 条 WARNING (v1.8 仅 71 条).
    修复: 抽 _safe_get_position(context, stock) helper, 用 ``stock in
    context.portfolio.positions`` 先检查再访问. 同步用到主循环状态自愈守卫.

v1.8 关键修复 (基于 v1.7 回测 log7.txt 的三次复盘):
  * 修 Bug E (新): 002311 (海大集团, 价格 ~60 元) 在 2025-07~2026-01 期间触发
    30 次"退出 c 减仓 50%"日志 → 实际 0 成交. 根因: safe_order_value 内
    min_value = max(2000, last × 200) = 12000 元 > 减仓金额 5715 元 → 静默
    return None; 而 v1.7 的兜底判定只看 sell_value < min_order_value_yuan(2000)
    → 5715 > 2000 不走全清分支 → high_water_pnl_pct 永不重置 → 第二天又触发.
    修复: 抽公共函数 calc_min_order_value(stock), 让 exit c/d/b 三处兑底判定
    精确匹配 safe_order_value 内部门槛, 高价股能正确走"全清 + COOLDOWN"分支.
  * 修 Bug F (新): 2022-04-26 同日 6 笔买入并发, 前 4 笔吃光可用现金, 后 2 笔
    (603379, 688106) 被聚宽撮合层折成 < 100/200 股而拒单 → log7 残留 2 条 ERROR.
    修复: safe_order_value 入口加 context 参数, 买入前预扣 portfolio.available_cash;
    超额自动 ×0.95 缩减或 skip, 确保聚宽层不会再因为现金不足拒单.
  * 强化 #1: 退出 c 减仓 50% 成功后强制 COOLDOWN 30 天 (替代当前"15 天 / +10% gap"
    软防抖). 002311 这种"持续创新高 + 高频震荡"的票, 减仓不改 avg_cost 导致
    pnl_pct 立即恢复 high_water → 软防抖锁不住. 改为强制休眠 30 天, 让
    high_water 自然消退, 彻底打破"切碎"循环.
  * 日志修正 (log8 后): 兑底分支原先 log 写 pos.value 易误解为「全仓 < 门槛」;
    实际判定为 sell_value=pos*0.5; 改为打印「减仓金额 … (持仓 …) 少于门槛」.

v1.7 关键修复 (基于 v1.6 回测 log6.txt 的二次复盘):
  * 修死循环 #3: 退出 d 减仓 50% 时, 若剩余持仓金额已低于 min_order_value
    (经过多次 c/d 减半后只剩 1k+ 元), safe_order_value 跳过下单但 first_buy_date
    等状态没重置 → 死循环. 修复: small order skip → 改用 safe_close_all 全清.
  * 修死循环 #4: 退出 c/b half-cut 同源问题, 三处统一用"safe_order_value None
    → close_all fallback → 状态重置"的范式.
  * 修订单泥石流 #2: log6 残余 47 条 "开仓数量不能小于 100/200" → min_order_shares
    100 → 200, min_order_value_yuan 1500 → 2000.
  * 新增"超小持仓清算": pos.value < min_position_value_yuan (默认 5000) 时
    主动清仓 + COOLDOWN, 释放策略 cycle 给真正能跑网格的票.

v1.6 关键修复 (基于 v1.5 回测 log5.txt 的事故复盘):
  * 修死循环 #1: 退出 d/b 减仓 50% 后未重置 high_water_pnl_pct 等状态 → 600801
    death loop 81 天. 修复: 减仓时统一重置 + 入口守卫 + 主循环自愈三层防御.
  * 修订单泥石流 #1: 1000+ "平仓数量不能小于 100" ERROR → safe_order_value
    加最小订单金额/一手占用金额 guard.

关联文档: SimTradeLab/my_docs/cyclical-grid-strategy-analysis.md
         SimTradeLab/my_docs/cyclical-grid-jq-strategy-handbook.md
"""

from jqdata import *
import pandas as pd
import numpy as np
from datetime import timedelta

# =============================================================================
# 1. 全局参数
# =============================================================================

CONFIG = {
    # ---- 仓位与资金 ----
    'single_stock_max_pct': 0.15,
    'single_sector_max_pct': 0.30,
    'min_cash_reserve_pct': 0.10,
    'base_position_pct_of_stock': 0.50,

    # ---- 质量过滤 ----
    'min_listed_days': 365,
    'max_debt_ratio': 0.60,
    'max_goodwill_to_net_asset': 0.30,
    'min_avg_amount_20d_yuan': 5e7,
    'rebalance_quality_freq_days': 20,

    # ---- 底部多因子打分 ----
    'bottom_score_threshold_t0': 70.0,
    'bottom_score_threshold_keep': 65.0,
    'signal_persistence_days': 5,
    'pb_history_years': 5,
    'rsi_period': 14,                          # 经典 RSI 周期, 反映短期超卖
    'drawdown_lookback_days': 750,
    'factor_weights': {
        'pb_low_pct': 0.35,
        'drawdown_high_pct': 0.25,
        'ocf_to_marketcap': 0.20,
        'rsi_low_pct': 0.20,
    },

    # ---- 分批建仓 ----
    'tier_pcts': [0.30, 0.30, 0.40],
    'tier_drop_pct': 0.08,
    'tier_t2_no_touch_days': 30,

    # ---- 动态网格 ----
    'grid_step_min_pct': 0.025,
    'grid_step_max_pct': 0.05,
    'grid_atr_k': 1.5,
    'grid_atr_period': 20,
    'grid_buy_step_factor': 0.83,
    'grid_sell_step_factor': 1.17,
    'grid_pyramid_thresholds': [-0.03, -0.06, -0.09, -0.12, -0.15],
    'grid_pyramid_multipliers': [1.0, 1.5, 2.0, 2.5, 3.0],

    # ---- 退出规则 ----
    'exit_a_cooldown_days': 60,
    'exit_a_release_drawdown_pct': 0.25,
    'exit_b_min_profit_pct': 0.30,
    'exit_b_limit_open_drop_pct': 0.015,
    'exit_b_pb_top_pct': 0.70,
    'exit_c_high_water_min_profit': 0.50,
    'exit_c_drawdown_half_cut': 0.15,
    'exit_c_drawdown_full_cut': 0.25,
    'exit_c_after_half_cut_arm_gap': 0.10,     # (v1.6 软防抖, v1.8 起被 cooldown 接管, 保留兼容)
    'exit_c_after_half_cut_min_days': 15,      # (v1.6 软防抖, v1.8 起被 cooldown 接管, 保留兼容)
    # v1.8 新增: 减仓 50% 成功后强制休眠天数 (彻底打破 002311 类"切碎"循环).
    # 002311 (~60 元) 减仓不改 avg_cost → next bar pnl_pct 立即恢复 high_water,
    # 软防抖 (15 天/+10% gap) 锁不住. 强制 phase=COOLDOWN 30 天后, 由
    # try_exit_cooldown 用 last_high_after_grid + 分数 重新评估再启动.
    'exit_c_half_cut_cooldown_days': 30,
    'exit_d_max_holding_days': 750,
    # ---- 硬止损 (v1.5 优化) ----
    # 周期股底部 V 反转特征明显, 25% 阈值常在熊市低点把人砸出局.
    # 改为 35% + "连续 N 个交易日浮亏均 ≥35%" 持续要求 → 让单日下影线/急跌不再误杀.
    'hard_stop_loss_pct': 0.35,
    'hard_stop_persist_days': 3,               # 必须连续 3 个交易日浮亏 ≥ 阈值才扣扳机
    # ---- 硬止损"惯犯"隔离 (v1.5 优化) ----
    # 同一只股 1 年内被硬止损 ≥ 2 次, 视为"反复抄到刀口", 自动从候选池剔除 1 年.
    'hard_stop_recent_window_days': 365,       # 滚动统计窗口
    'hard_stop_max_count_in_window': 2,        # 窗口内 ≥ 此次数 → 触发隔离
    'hard_stop_blacklist_days': 365,           # 隔离时长 (日历日)
    't0_max_attempts_per_window': 1,           # 同一信号窗口内最多发起 1 次 T0 (防止反复打 log/下单)

    # ---- 科创板 / 北交所 (688/8/4 开头) 必须传"保护价"才不被聚宽拒单 ----
    # 买入: 成交价 ≤ last_price * (1 + slippage); 卖出: 成交价 ≥ last_price * (1 - slippage)
    # 同时夹在当日涨跌停板内, 避免给出无效保护价
    'star_market_protect_slippage_pct': 0.02,

    # ---- 最小订单 guard (v1.6 引入, v1.7 调严) ----
    # log5 中出现 ~1000 条 "平仓数量不能小于 100/200" 错误, 主因是网格 base_grid_chunk
    # = grid_budget / 10, 对网格预算偏小的股 (e.g. 37510 元) 单格 ~3751, 当股价
    # 涨到 30+ 元时四舍五入到 0~100 股 → 触发聚宽拒单. v1.6 加了 guard 后降到 47 条,
    # v1.7 进一步把 min_order_shares 100 → 200 (覆盖科创板 200 股门槛),
    # min_order_value_yuan 1500 → 2000 (留出 ~10% 价差 buffer 应对开盘漂移).
    'min_order_value_yuan': 2000.0,           # 单笔金额下限 (~2 手 + buffer)
    'min_order_shares': 200,                  # 取主板 100 / 科创板 200 的较大值, 全板鲁棒

    # ---- 超小持仓自动清算 (v1.7 新增) ----
    # 一只股经过多次退出 c / d 减半后, 持仓金额可能缩到几百~一两千元的"僵尸状态",
    # 既无法继续跑网格 (单格金额不足), 又每天占用主循环 evaluate. 直接清掉换别的.
    'min_position_value_yuan': 5000.0,        # 持仓 < 5000 元 → 直接 close_all + COOLDOWN

    # ---- 调试 ----
    'verbose': True,
    'diagnostic_log_freq_days': 5,         # 每 N 个交易日打印一次"全候选池分数快照"
    'log_near_miss_threshold': 50.0,       # 分数 >= 此值时记录"接近触发"日志
}

# =============================================================================
# 2. 候选池 (按行业分组)
# =============================================================================

# v1.5: 扩到 13 行业 36 只, 增加钢铁/煤炭/水泥/工程机械/海运 5 个新行业,
# 缓解 v1.3 中 "化工 + 维生素" 占主导带来的相关性集中问题.
CANDIDATE_POOL = {
    '磷化工':   ['600141.XSHG', '600096.XSHG', '002312.XSHE', '000422.XSHE', '002895.XSHE'],
    '工业气体': ['002430.XSHE', '688268.XSHG', '688106.XSHG'],
    '维生素':   ['002001.XSHE', '600216.XSHG', '600299.XSHG', '300401.XSHE'],
    '钛白粉':   ['002601.XSHE', '002145.XSHE'],
    '制冷剂':   ['600160.XSHG', '603379.XSHG'],
    '有色':     ['601899.XSHG', '000933.XSHE', '603993.XSHG', '603799.XSHG'],
    '农药':     ['600486.XSHG', '002258.XSHE', '603599.XSHG'],
    '养殖':     ['002714.XSHE', '300498.XSHE', '002311.XSHE'],
    # ---- v1.5 新增 5 个行业 ----
    '钢铁':     ['600019.XSHG', '000932.XSHE'],   # 宝钢股份 / 华菱钢铁
    '煤炭':     ['601088.XSHG', '601225.XSHG'],   # 中国神华 / 陕西煤业
    '水泥':     ['600585.XSHG', '600801.XSHG'],   # 海螺水泥 / 华新水泥
    '工程机械': ['600031.XSHG', '000425.XSHE'],   # 三一重工 / 徐工机械
    '海运':     ['601919.XSHG', '601872.XSHG'],   # 中远海控 / 招商轮船
}


def all_candidates():
    return [s for ss in CANDIDATE_POOL.values() for s in ss]


def sector_of(stock):
    for sector, stocks in CANDIDATE_POOL.items():
        if stock in stocks:
            return sector
    return 'OTHER'


# =============================================================================
# 3. 持仓状态管理
# =============================================================================

def init_state():
    """单只股票的状态字典."""
    return {
        'phase': 'IDLE',                     # IDLE / BUILDING / GRID_RUNNING / COOLDOWN
        'first_buy_date': None,
        'first_buy_price': None,
        'tier_filled': 0,                    # 已完成的建仓档位 0..3
        'target_total_value': 0.0,
        'base_target_value': 0.0,
        'grid_target_value': 0.0,
        'last_grid_price': None,
        'grid_total_buy_value': 0.0,
        'grid_total_sell_value': 0.0,
        'grid_buy_count': 0,
        'grid_sell_count': 0,
        'pyramid_levels_done': [],           # 已加仓的档位 (e.g. [-0.03, -0.06])
        'high_water_pnl_pct': 0.0,
        'cooldown_until_date': None,
        'consecutive_signal_days': 0,
        'last_score': 0.0,
        'last_high_after_grid': None,        # 网格清空时记录的高点 (用于 exit a 解除条件)
        't0_attempts_in_window': 0,          # 当前信号窗口内已发起的 T0 次数 (失败重试上限)
        't0_disabled_until_score_below': None,  # 信号需先回落到此分数以下才能重新累计 (释放幽灵T0)
        'exit_c_last_action_date': None,     # 退出 c 上次触发日, 用于防抖
        'exit_c_arm_high': 0.0,              # 退出 c 上次触发时的高水位 PnL, 配合 arm_gap 防反复
        'hard_stop_persist_count': 0,        # v1.5: 连续触发硬止损阈值的交易日计数
        'hard_stop_last_check_date': None,   # v1.5: 上次评估硬止损的交易日 (避免日内重入)
    }


# =============================================================================
# 4. 聚宽框架接口
# =============================================================================

def initialize(context):
    log.info('=' * 70)
    log.info('周期股·网格策略 v1.9 启动 | 候选 %d 只 / %d 行业'
             % (len(all_candidates()), len(CANDIDATE_POOL)))
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

    g.state = {}
    g.quality_universe = []
    g.last_quality_check_date = None
    g.processed_today = set()
    g.trading_day_counter = 0
    g.last_score_snapshot = {}  # {stock: {pb,dd,ocf,rsi,total}} 最近一次完整分数

    # v1.5 优 3: 硬止损惯犯名单
    # hard_stop_history: {stock: [date1, date2, ...]} 完整历史触发日 (滚动 1 年内的有效)
    # hard_stop_blocked_until: {stock: date} 黑名单到期日, 在此之前 refresh_quality_universe 直接过滤
    g.hard_stop_history = {}
    g.hard_stop_blocked_until = {}

    run_daily(refresh_quality_universe, time='before_open')
    run_daily(daily_signal_and_trade,    time='10:00')
    run_daily(intraday_exit_check,       time='14:50')


def before_trading_start(context):
    g.processed_today = set()
    g.trading_day_counter = getattr(g, 'trading_day_counter', 0) + 1


# =============================================================================
# 5. 质量过滤 (~月度)
# =============================================================================

def refresh_quality_universe(context):
    today = context.current_dt.date()
    # 聚宽 avoid_future_data=True 下, 盘前/盘中查询 get_fundamentals(date=今天) 会被拒绝
    # (当天市值表 15:00 后才生效). 统一用 previous_date 查询, 既能在 09:00 跑也避免前视.
    query_date = context.previous_date

    if g.last_quality_check_date is not None:
        if (today - g.last_quality_check_date).days < CONFIG['rebalance_quality_freq_days']:
            return

    # v1.5 优 3: 先把"硬止损惯犯黑名单"剔掉, 1 年内 ≥ 2 次硬止损 → 隔离 1 年
    blocked_today = []
    for stock, until in list(getattr(g, 'hard_stop_blocked_until', {}).items()):
        if until is None or today >= until:
            # 隔离期满 → 移除黑名单, 但保留 history (可继续累计未来触发)
            g.hard_stop_blocked_until.pop(stock, None)
        else:
            blocked_today.append(stock)
    candidates = [s for s in all_candidates() if s not in blocked_today]
    if blocked_today:
        log.info('【硬止损隔离】%s | 排除 %d 只 (1年内≥%d次硬止损): %s'
                 % (today, len(blocked_today),
                    CONFIG['hard_stop_max_count_in_window'],
                    ','.join(blocked_today)))
    # 聚宽 BalanceSheet 字段: 商誉 = good_will (带下划线), 总负债 = total_liability
    try:
        df = get_fundamentals(query(
            valuation.code,
            valuation.market_cap,                # 亿元
            balance.total_liability,
            balance.total_assets,
            balance.good_will,
        ).filter(valuation.code.in_(candidates)), date=query_date)
    except Exception as e:
        log.warn('【质量过滤】查询失败: %s' % e)
        return

    passed = []
    for _, row in df.iterrows():
        stock = row['code']
        try:
            sec_info = get_security_info(stock)
            if sec_info is None:
                continue
            listed_days = (today - sec_info.start_date).days
            if listed_days < CONFIG['min_listed_days']:
                continue

            ta = row['total_assets'] or 0
            tl = row['total_liability'] or 0
            if ta <= 0:
                continue
            debt_ratio = tl / ta
            if debt_ratio > CONFIG['max_debt_ratio']:
                continue

            net_assets = ta - tl
            gw = row.get('good_will', 0) or 0
            if net_assets > 0 and gw / net_assets > CONFIG['max_goodwill_to_net_asset']:
                continue

            try:
                amt = attribute_history(stock, 20, '1d', ['money'], df=False)['money']
                if np.nanmean(amt) < CONFIG['min_avg_amount_20d_yuan']:
                    continue
            except Exception:
                continue

            passed.append(stock)
        except Exception:
            continue

    g.quality_universe = passed
    g.last_quality_check_date = today
    log.info('【质量过滤】%s | 通过 %d / %d : %s'
             % (today, len(passed), len(candidates), ','.join(passed[:10]) + ('...' if len(passed) > 10 else '')))


# =============================================================================
# 6. 底部多因子打分
# =============================================================================

def calc_pb_low_pct_score(stock, query_date):
    """PB 5 年逆向分位: 越低分位 → 越高分.
    query_date: 应为 context.previous_date, 避免 avoid_future_data 警告."""
    try:
        start = query_date - timedelta(days=int(CONFIG['pb_history_years'] * 365))
        df = get_valuation(stock, start_date=start, end_date=query_date, fields=['pb_ratio'])
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
    """距前高回撤评分: 50% 回撤 = 满分."""
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
    query_date: 应为 context.previous_date, 避免 avoid_future_data 警告."""
    try:
        df = get_fundamentals(query(
            valuation.market_cap,
            cash_flow.net_operate_cash_flow,
        ).filter(valuation.code == stock), date=query_date)
        if len(df) == 0:
            return None
        mv = df['market_cap'].iloc[0]      # 亿元
        ocf = df['net_operate_cash_flow'].iloc[0]  # 元
        if mv is None or mv <= 0 or ocf is None:
            return 0.0
        ratio = ocf / (mv * 1e8)
        return max(0.0, min(ratio / 0.10 * 100.0, 100.0))
    except Exception:
        return None


def calc_rsi_score(stock):
    """RSI 逆向分: RSI 30 → 70 分, RSI 50 → 50 分.

    经典周期 14 日; 用 ``CONFIG['rsi_period']`` 控制.
    取价时若窗口内有停牌/上市不足, 聚宽会回填 NaN, 这里先 dropna 再计算,
    避免 ``np.where(diff > 0, ...)`` 触发 RuntimeWarning.
    """
    try:
        period = CONFIG['rsi_period']
        # 多取 30 个交易日做缓冲, 防止 dropna 后样本不足
        raw = attribute_history(stock, period + 30, '1d', ['close'], df=False)['close']
        prices = np.asarray(raw, dtype=float)
        prices = prices[~np.isnan(prices)]
        if len(prices) < period + 1:
            return None
        prices = prices[-(period + 1):]
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
    """返回 dict {pb, dd, ocf, rsi, total, pb_pct, fail_reason}.
    fail_reason 非空表示数据不足/计算失败, 此时 total=None."""
    query_date = context.previous_date  # 避免 avoid_future_data

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
    """兼容旧接口: 返回 (composite_score, pb_pct). 同时把 detail 写入快照缓存."""
    detail = calc_bottom_score_detail(context, stock)
    g.last_score_snapshot[stock] = detail
    return detail['total'], detail['pb_pct']


# =============================================================================
# 7. 仓位/资金规模工具
# =============================================================================

def compute_target_value_for_stock(context, stock):
    """根据风控上限给出"理想目标总市值"(单股最大占比 + 单行业最大占比)."""
    portfolio_total = context.portfolio.total_value
    single_max = portfolio_total * CONFIG['single_stock_max_pct']

    sector = sector_of(stock)
    sector_used = 0.0
    for s, st in g.state.items():
        if sector_of(s) == sector and s != stock:
            # v1.9 Bug I 修复: 用 _safe_get_position 避免对已全清的同行业 stock
            # 调用 .get() 触发聚宽 "在 positions 中不存在" WARNING.
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
    """聚宽的 get_current_data() 返回 _CurrentDic, 不支持 .get(); 必须用 []."""
    try:
        return get_current_data()[stock]
    except (KeyError, TypeError, Exception):
        return None


def _safe_get_position(context, stock):
    """安全获取持仓 (v1.9 修复 Bug I).

    聚宽的 ``portfolio.positions.get(stock)`` / ``[stock]`` 在 stock 不在持仓字典中
    时, 不会返回 None, 而是会:
      - 返回一个 amount=price=avg_cost=0 的空 Position 对象
      - 同时打出一条 WARNING:
        ``Security(code=...) 在 positions 中不存在, 为了保持兼容, 我们返回空的
        Position 对象, amount/price/avg_cost/acc_avg_cost 都是 0``

    log9 实测共 2022 条此类 WARNING (v1.8 仅 71 条), 主要源于 v1.9 在
    ``try_exit_cooldown`` 入口主动调用 .get() 检查 has_position. 全清后处于
    COOLDOWN 但 score 一直 < 65 的票 (e.g. 002714 / 600216 / 603799 等), 每天都
    会触发一次 → 累计 2022 条噪音. 用 ``in`` 检查避开聚宽这个特殊行为.

    返回 ``Position`` 对象或 ``None``. 调用方需自行判断 ``pos.total_amount > 0``.
    """
    try:
        if stock in context.portfolio.positions:
            return context.portfolio.positions[stock]
    except Exception:
        pass
    return None


def _is_star_market(stock):
    """科创板 (688xxx) / 北交所 (8xxxxx, 4xxxxx, 920xxx) 必须传保护价."""
    code = stock.split('.')[0]
    return code.startswith('688') or code.startswith('8') or code.startswith('4') or code.startswith('92')


def _build_order_style(stock, side, cd):
    """构造下单 OrderStyle.

    主板/创业板 → 返回 None (聚宽默认 MarketOrderStyle 即可);
    科创板/北交所 → 必须传 ``MarketOrderStyle(保护价)``, 否则订单会被聚宽拒绝.

    side: 'buy' / 'sell'.
    保护价规则:
        买: 接受成交价 ≤ last × (1 + slip), 但不超过当日涨停;
        卖: 接受成交价 ≥ last × (1 - slip), 但不低于当日跌停.
    """
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
    # 价格按照 0.01 元取整, 避免某些标的对最小价位敏感
    protect = round(protect, 2)
    try:
        return MarketOrderStyle(protect)
    except Exception:
        return None


def calc_min_order_value(stock):
    """与 ``safe_order_value`` 内部一致的最小订单金额门槛 (v1.8 引入).

    用于 exit c/d/b 三处的 "金额过小 → 全清 + COOLDOWN" 兜底判定, 让兜底门槛
    精确匹配 ``safe_order_value`` 内部的真实拒单门槛, 避免高价股 (e.g. 002311
    ~60 元 → last × 200 = 12000 元) 被 ``safe_order_value`` 静默 return None
    但兜底分支认为 sell_value > min_order_value_yuan(2000) 而走"减仓 50%"分支,
    导致 high_water_pnl_pct 永不重置 → 反复触发空跑日志 (log7 002311 切碎 30 次).

    返回 ``float('inf')`` 时表示拿不到价格, 调用方应当跳过本次下单.
    """
    cd = _get_cd(stock)
    if cd is None or cd.last_price is None or cd.last_price <= 0:
        return float('inf')
    return max(CONFIG['min_order_value_yuan'],
               cd.last_price * CONFIG['min_order_shares'])


def safe_order_value(stock, value, context=None):
    """带保护的下单: 跳过停牌/涨跌停一字 + 太小订单, 自动按 100 股取整, 科创板自动加保护价.

    v1.6 增补: 单笔金额低于 ``min_order_value_yuan`` 或不足 ``min_order_shares`` 一手
    成本时直接放弃, 让金额累积到下一格再下. 避免聚宽日志被 "平仓/开仓数量不能小于 100"
    刷屏 (log5 实测 1000+ 条该 ERROR 几乎都源自网格单格金额过小).

    v1.8 新增: 当 ``context`` 非空且为买入 (value > 0) 时, 在送单前预扣
    ``portfolio.available_cash``. 防止同日多笔买单并发抢资金导致后到的订单被
    聚宽撮合层"按剩余现金折股 → 折成 < 100/200 股"而拒单 (log7 中 2022-04-26
    同日 6 笔订单连击, 后 2 笔被拒). 现金不够 ``calc_min_order_value`` 直接 skip,
    现金不够下单金额则按 95% 缩减 (留 5% 安全垫).
    """
    if abs(value) < 1.0:
        return None
    cd = _get_cd(stock)
    if cd is None or cd.paused:
        return None
    last = cd.last_price
    if last is None or last <= 0:
        return None
    # v1.6: 太小订单直接放弃 (绝对金额 + 一手占用金额 双门槛取大值)
    min_value = max(CONFIG['min_order_value_yuan'], last * CONFIG['min_order_shares'])
    if abs(value) < min_value:
        return None
    if value > 0 and cd.high_limit and last >= cd.high_limit - 1e-4:
        return None  # 涨停一字, 不追
    if value < 0 and cd.low_limit and last <= cd.low_limit + 1e-4:
        return None  # 跌停一字, 不砸

    # v1.8: 买入前预扣 available_cash, 防止同日资金竞争
    if value > 0 and context is not None:
        cash = context.portfolio.available_cash
        reserve = context.portfolio.total_value * CONFIG['min_cash_reserve_pct']
        usable = cash - reserve
        if usable < min_value:
            log.info('[%s] 资金不足: 可用 %.0f - 储备 %.0f = %.0f < 门槛 %.0f, 跳过买入'
                     % (stock, cash, reserve, usable, min_value))
            return None
        if value > usable:
            scaled = usable * 0.95   # 留 5% 安全垫给手续费 + 价格漂移
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
    """精确清空持仓 (避免 order_value 因价格漂移留下零碎股); 科创板自动加保护价."""
    cd = _get_cd(stock)
    if cd is None or cd.paused:
        return None
    if cd.low_limit and cd.last_price is not None and cd.last_price <= cd.low_limit + 1e-4:
        return None
    style = _build_order_style(stock, 'sell', cd)  # 清仓即卖出
    try:
        if style is None:
            return order_target_value(stock, 0)
        return order_target_value(stock, 0, style=style)
    except Exception as e:
        log.warn('[%s] order_target_value(0) 失败: %s' % (stock, e))
        return None


# =============================================================================
# 8. T0/T1/T2 分批建仓
# =============================================================================

def try_enter_t0(context, stock, st, current_price):
    """评估底部信号; 持续达标后买入 T0 (30%).

    防"幽灵 T0": 一次信号窗口内最多发起 ``t0_max_attempts_per_window`` 次下单;
    若聚宽因可用资金/科创板委托/涨停一字等返回 None, 主动重置 consecutive_signal_days
    并设 ``t0_disabled_until_score_below``, 等分数自然回落后才允许重新累计, 避免
    天天打 "T0 建仓" 日志。
    """
    # ---- 1) 解锁: 分数低于 disable 阈值后释放 ----
    if st.get('t0_disabled_until_score_below') is not None:
        # 先取分数判断
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

    # ---- 2) 信号累计 ----
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

    # ---- 3) 防幽灵: 当前窗口已达上限就不再下单 ----
    if st['t0_attempts_in_window'] >= CONFIG['t0_max_attempts_per_window']:
        # 锁住直到分数自然回落 5 分以下
        st['t0_disabled_until_score_below'] = max(
            CONFIG['bottom_score_threshold_t0'] - 5.0,
            CONFIG['log_near_miss_threshold'])
        log.info('[%s] T0 已达单窗口最大尝试次数 %d, 暂停至分数<%.1f'
                 % (stock, CONFIG['t0_max_attempts_per_window'],
                    st['t0_disabled_until_score_below']))
        return

    target_total = compute_target_value_for_stock(context, stock)
    if target_total <= 1000:
        # 资金/行业额度不足以建一档底仓: 同样进入"分数回落前禁用", 杜绝信号天天累加
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

    # 无论下单成功与否, 这一次窗口内的尝试都已消耗
    st['t0_attempts_in_window'] += 1

    res = safe_order_value(stock, t0_value, context=context)
    if res is None:
        # 下单被聚宽拒绝(资金/涨停/科创板等), 进入禁用直到分数回落, 杜绝幽灵 T0
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
    # 进入 BUILDING 后, 信号计数清零, 下次再积累需要新一轮 5 天
    st['consecutive_signal_days'] = 0
    st['t0_attempts_in_window'] = 0


def try_build_t1_t2(context, stock, st, current_price):
    """T1/T2 加仓: 价格再下跌 ≥8% 或 30 天未触及 T0 价(右侧确认)."""
    if st['first_buy_price'] is None:
        return

    drop_from_t0 = (st['first_buy_price'] - current_price) / st['first_buy_price']
    days_since_t0 = (context.current_dt.date() - st['first_buy_date']).days
    base = st['base_target_value']

    if st['tier_filled'] == 1:
        if drop_from_t0 >= CONFIG['tier_drop_pct']:
            t1_value = base * CONFIG['tier_pcts'][1]
            log.info('[%s] T1 加仓 (左侧) | 价格=%.2f / 跌幅=%.2f%% / 加仓=%.0f'
                     % (stock, current_price, drop_from_t0 * 100, t1_value))
            if safe_order_value(stock, t1_value, context=context) is not None:
                st['tier_filled'] = 2
        return

    if st['tier_filled'] == 2:
        cond_left = drop_from_t0 >= CONFIG['tier_drop_pct'] * 2
        cond_right = (days_since_t0 >= CONFIG['tier_t2_no_touch_days']
                      and current_price >= st['first_buy_price'] * 1.05)
        if cond_left or cond_right:
            t2_value = base * CONFIG['tier_pcts'][2]
            log.info('[%s] T2 加仓 (%s) | 价格=%.2f / 加仓=%.0f'
                     % (stock, '左侧' if cond_left else '右侧确认', current_price, t2_value))
            if safe_order_value(stock, t2_value, context=context) is not None:
                st['tier_filled'] = 3
                st['phase'] = 'GRID_RUNNING'
                st['last_grid_price'] = current_price
                log.info('[%s] 建仓完成, 网格启动 | 基准价=%.2f / 网格预算=%.0f'
                         % (stock, current_price, st['grid_target_value']))


# =============================================================================
# 9. 动态 + 不对称 + 金字塔 网格
# =============================================================================

def calc_dynamic_grid_step(stock):
    """步长 = clamp(k * ATR(20)/价格, [min, max])."""
    try:
        period = CONFIG['grid_atr_period']
        df = attribute_history(stock, period + 1, '1d',
                               ['high', 'low', 'close'], df=False)
        h = np.asarray(df['high'], dtype=float)
        l = np.asarray(df['low'], dtype=float)
        c = np.asarray(df['close'], dtype=float)
        # 任一序列含 NaN, 退化为最小步长 (避免 RuntimeWarning)
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
    """根据距 T0 价的跌幅, 决定本次网格买入倍数; 已加过的档位不再加倍."""
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
    """运行网格: 跌买更密 / 涨卖更稀 / 越跌越多 / 网格仓位有上限."""
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

    # 单格基准金额 = 网格预算 / 10  (即一轮完整网格约 10 个买卖动作)
    base_grid_chunk = grid_budget / 10.0

    if pct <= -buy_step:
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


# =============================================================================
# 10. 退出规则 a / b / c / d  +  硬止损
# =============================================================================

def get_position_pnl_pct(context, stock):
    pos = context.portfolio.positions.get(stock)
    if pos is None or pos.total_amount <= 0 or pos.avg_cost <= 0:
        return 0.0
    return (pos.price - pos.avg_cost) / pos.avg_cost


def get_today_high_close_limit(stock):
    """日内最高价 / 收盘价 / 涨停价 (用于涨停打开判定)."""
    try:
        df = attribute_history(stock, 1, '1d',
                               ['high', 'close', 'high_limit'], df=False)
        return float(df['high'][0]), float(df['close'][0]), float(df['high_limit'][0])
    except Exception:
        return None, None, None


def check_exit_a_grid_cleared(context, stock, st):
    """规则 a: 网格部分一轮买卖已闭合, 净仓位归零 → 进入冷却 (底仓保留)."""
    # 至少要有过 1 次买和 1 次卖, 否则不算"卖完"
    if st['grid_buy_count'] < 1 or st['grid_sell_count'] < 1:
        return False
    net = st['grid_total_buy_value'] - st['grid_total_sell_value']
    # 净持仓 ≤ 网格预算的 5% 视为"清空"
    if net > st['grid_target_value'] * 0.05:
        return False

    log.info('[%s] 退出 a: 网格清空 (买=%d次 卖=%d次 净额=%.0f), 进入 %d 日冷却 (底仓保留)'
             % (stock, st['grid_buy_count'], st['grid_sell_count'], net,
                CONFIG['exit_a_cooldown_days']))
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
    return True


def check_exit_b_limit_up_open(context, stock, st):
    """规则 b: 浮盈 ≥30% + 触及涨停 + 涨停打开 → 卖出."""
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
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                context.current_dt.date() + timedelta(days=CONFIG['exit_a_cooldown_days']))
            return True
    else:
        sell_value = pos.value * 0.5
        today = context.current_dt.date()
        # v1.8: 兑底门槛改用 calc_min_order_value, 与 safe_order_value 内部保持一致.
        # 高价股 (e.g. 002311 ~60 元 → last × 200 = 12000) 时, 5715 元的 50% 减仓也会
        # 被 safe_order_value 静默拒, 必须一并走全清分支.
        min_value = calc_min_order_value(stock)
        if sell_value < min_value:
            # 条件为 sell_value(50%% 仓金额) < min_value; 日志须打印 sell_value 避免误判
            log.info('[%s] 退出 b: 浮盈%.1f%% + 涨停打开, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                     % (stock, pnl_pct * 100, sell_value, pos.value, min_value))
            if safe_close_all(stock) is not None:
                st['phase'] = 'COOLDOWN'
                st['cooldown_until_date'] = (
                    today + timedelta(days=CONFIG['exit_a_cooldown_days']))
                return True
            return False
        log.info('[%s] 退出 b (常规): 浮盈%.1f%% + 涨停打开 → 卖出50%%'
                 % (stock, pnl_pct * 100))
        if safe_order_value(stock, -sell_value, context=context) is not None:
            _reset_state_after_partial_exit(st, today)
        return False
    return False


def check_exit_c_high_water(context, stock, st):
    """规则 c: 浮盈 ≥50% 后, 自高水位回撤 ≥15% 减半, ≥25% 清仓.

    防抖: 减仓后必须满足以下两个条件之一才允许下次触发, 避免对同一股
    短期内"切碎"持仓 (实测会出现 30 天内 4 次减仓 50% 的现象):
      - 持仓再创新高 (pnl 超过减仓时高水位 + ``exit_c_after_half_cut_arm_gap``)
      - 距上次减仓 ≥ ``exit_c_after_half_cut_min_days`` 天

    v1.6 入口守卫: 持仓 ≤ 0 直接返回; 否则 high_water 残留可能让 drawdown
    永远满足触发条件, 配合 safe_close_all 持仓为 0 时返回 None, 形成"每天
    全清但啥也没卖"的死循环 (log5 中 600801 复现 81 次).
    """
    pos = context.portfolio.positions.get(stock)
    if pos is None or pos.total_amount <= 0:
        return False
    pnl_pct = get_position_pnl_pct(context, stock)
    if pnl_pct > st['high_water_pnl_pct']:
        st['high_water_pnl_pct'] = pnl_pct
        return False
    if st['high_water_pnl_pct'] < CONFIG['exit_c_high_water_min_profit']:
        return False

    # 减仓后的"重新武装"条件
    if st.get('exit_c_last_action_date') is not None:
        days_since = (context.current_dt.date() - st['exit_c_last_action_date']).days
        rearm_gap = CONFIG['exit_c_after_half_cut_arm_gap']
        rearm_days = CONFIG['exit_c_after_half_cut_min_days']
        # 既没等够天数, 也没刷新出新的"高水位 + gap", 则继续静默
        new_high_required = st.get('exit_c_arm_high', 0.0) + rearm_gap
        if days_since < rearm_days and st['high_water_pnl_pct'] < new_high_required:
            return False

    drop_from_peak = st['high_water_pnl_pct'] - pnl_pct

    if drop_from_peak >= CONFIG['exit_c_drawdown_full_cut']:
        log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%% → 全部清仓'
                 % (stock, drop_from_peak * 100, CONFIG['exit_c_drawdown_full_cut'] * 100))
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                context.current_dt.date() + timedelta(days=CONFIG['exit_a_cooldown_days']))
            return True
    elif drop_from_peak >= CONFIG['exit_c_drawdown_half_cut']:
        sell_value = pos.value * 0.5
        today = context.current_dt.date()
        # v1.8: 兑底门槛改用 calc_min_order_value, 与 safe_order_value 内部保持一致.
        # bug E 修复: 002311 (~60 元) sell_value=5715 既 > min_order_value_yuan(2000)
        # 又 < last × 200 (=12000), 旧 v1.7 兜底 (sell_value < 2000) 不生效, 走"减仓"
        # 分支 → safe_order_value 静默 None → 状态不重置 → 第二天又触发, 30 次空跑.
        min_value = calc_min_order_value(stock)
        if sell_value < min_value:
            log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%%, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                     % (stock, drop_from_peak * 100,
                        CONFIG['exit_c_drawdown_half_cut'] * 100, sell_value, pos.value, min_value))
            if safe_close_all(stock) is not None:
                st['phase'] = 'COOLDOWN'
                st['cooldown_until_date'] = (
                    today + timedelta(days=CONFIG['exit_a_cooldown_days']))
                return True
            return False
        log.info('[%s] 退出 c: 高水位回撤%.1f%% ≥%.0f%% → 减仓50%% + COOLDOWN %d 天'
                 % (stock, drop_from_peak * 100,
                    CONFIG['exit_c_drawdown_half_cut'] * 100,
                    CONFIG['exit_c_half_cut_cooldown_days']))
        if safe_order_value(stock, -sell_value, context=context) is not None:
            # v1.7: 仅在订单实际下出去后才更新防抖, 避免静默跳过却写状态
            st['exit_c_last_action_date'] = today
            st['exit_c_arm_high'] = st['high_water_pnl_pct']
            st['high_water_pnl_pct'] = pnl_pct  # 等待下一轮新高
            # v1.8: 减仓后强制 COOLDOWN 30 天. 002311 类高频震荡票的 high_water 在
            # 减仓不改 avg_cost 的特性下会立即恢复, 软防抖 (15 天/+10% gap) 锁不住,
            # 必须用 phase 切换让股票完全休眠, 期间网格也不跑, 等高水位自然消退.
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = (
                today + timedelta(days=CONFIG['exit_c_half_cut_cooldown_days']))
            return True
    return False


def _reset_state_after_partial_exit(st, today):
    """退出 b/c/d 减仓 50% 成功后的状态重置块 (v1.6 引入, v1.7 抽公共).

    必须重置:
      - high_water_pnl_pct  : 否则 high_water 残留 → 退出 c 反复触发
      - grid_total_*        : 否则网格基于旧基准继续发疯卖
      - pyramid_levels_done : 让金字塔档位回到首档, 后续可以重新加仓
      - exit_c 防抖字段     : 避免新一轮 high_water 起来后被旧防抖锁死
      - last_grid_price     : 让 run_grid 用当前价重建基准
    """
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
    """规则 d: 持仓满 3 年强制评估; 若浮亏则清仓, 浮盈则减仓50%.

    v1.7 防死循环: 若剩余持仓金额已不足 ``min_order_value_yuan`` (经过多次
    c/d 减半后常出现), safe_order_value 会静默跳过, 而 first_buy_date 不重置
    会让 holding_days 永远 ≥ 750 → 第二天又触发. 改为 close_all 兑底.
    """
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
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = today + timedelta(days=cooldown_days)
            return True
        return False

    sell_value = pos.value * 0.5
    # v1.8: 兑底门槛改用 calc_min_order_value, 与 safe_order_value 内部保持一致.
    min_value = calc_min_order_value(stock)
    if sell_value < min_value:
        log.info('[%s] 退出 d: 持仓 %d 天浮盈%.1f%%, 减仓金额 %.0f 元(持仓 %.0f) 少于门槛 %.0f → 直接全清'
                 % (stock, holding_days, pnl_pct * 100, sell_value, pos.value, min_value))
        if safe_close_all(stock) is not None:
            st['phase'] = 'COOLDOWN'
            st['cooldown_until_date'] = today + timedelta(days=cooldown_days)
            return True
        # close_all 也没成 (停牌/跌停一字), 推迟到下一交易日再评估
        return False

    log.info('[%s] 退出 d: 持仓 %d 天浮盈%.1f%% → 减仓50%%, 续持评估'
             % (stock, holding_days, pnl_pct * 100))
    if safe_order_value(stock, -sell_value, context=context) is not None:
        st['first_buy_date'] = today  # 重置 3 年计时
        _reset_state_after_partial_exit(st, today)
    else:
        # v1.7: 减半下单失败 (停牌/一字 等), 至少把 first_buy_date 推后, 防止明天死循环
        st['first_buy_date'] = today
    return False


def _record_hard_stop(stock, today):
    """v1.5 优 3: 记录一次硬止损触发, 必要时更新黑名单到期日.

    历史只保留滚动窗口 (默认 365 日) 内的触发日; 窗口内累积达上限 → 设置黑名单.
    黑名单到期日 = 第 N 次触发日 + ``hard_stop_blacklist_days``.
    """
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
    """硬止损 (v1.5 强化版).

    旧版本: 单日浮亏 ≥25% 立即清仓. 周期股 V 反前的急跌容易把人砸出局,
    回测中 5 次硬止损平均 -31%, 其中 4 次发生在 2018 熊市底部.

    新规则:
      1. 阈值放宽到 35% (`hard_stop_loss_pct`);
      2. 必须连续 ``hard_stop_persist_days`` 个交易日浮亏 ≥ 35% 才触发清仓
         (用 ``hard_stop_persist_count`` 计数, 同一交易日内只增 1);
      3. 触发后调用 ``_record_hard_stop`` 记录历史, 1 年内累计 ≥ 2 次将
         自动进入黑名单, 由 ``refresh_quality_universe`` 在月度刷新时剔除.
    """
    pnl_pct = get_position_pnl_pct(context, stock)
    today = context.current_dt.date()
    threshold = -CONFIG['hard_stop_loss_pct']

    # 同一交易日内已计过数则不再重复 +1 (intraday + 主循环都可能调到这里)
    last_check = st.get('hard_stop_last_check_date')
    if last_check != today:
        if pnl_pct <= threshold:
            st['hard_stop_persist_count'] = st.get('hard_stop_persist_count', 0) + 1
        else:
            # 任何一日浮亏低于阈值就重置计数, 持续要求必须"连续 N 日"
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
    if safe_close_all(stock) is not None:
        _record_hard_stop(stock, today)
        st['phase'] = 'COOLDOWN'
        st['cooldown_until_date'] = (
            today + timedelta(days=CONFIG['exit_a_cooldown_days'] * 2))
        st['hard_stop_persist_count'] = 0
        return True
    return False


def check_daily_non_intraday_exits(context, stock, st):
    """非日内的退出 (日初/日中均可调用): 硬止损 / c / d / a.

    退出 b 依赖日内涨停打开判定, 单独在 14:50 由 intraday_exit_check 调用.
    """
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
    """COOLDOWN 解除 (v1.9 修复 Bug H):
       - 路径 A (退出 a 网格闭合): 设有 ``last_high_after_grid`` → 底仓保留, 到期还需
         「价格回撤 ≥25%」+「分数 ≥65」双确认才能重启网格 (避免追涨).
       - 路径 B (退出 c/d 减半保留底仓, v1.9 新): 没有 ``last_high_after_grid`` 但
         持仓 > 0 → **到期无条件解除**, 立即重启网格. 修复 v1.8 Bug H:
         002714 在 2018-12-20 退出 c 减仓 50% 后, 因 try_exit_cooldown 解除条件
         要求 score >= 65, 但牛市中 PB / RSI 都极低分 → 卡死 916 天 (≈ 2.5 年),
         直到 2021-06-22 才解除, **错过整段非洲猪瘟大牛**. log7 同票在 2019-03
         拿到了退出 b +184.6%; 把退出 c 减半的 cooldown 解除条件放宽到"到期即解",
         可让底仓继续吃后续上涨, 而 30 天内 high_water 已自然消退故不会立即再触发.
       - 路径 C (退出 b 周期顶 / 退出 c/d 全清 / 硬止损 / 超小持仓清算 = 持仓为零):
         保持原条件, 必须 score >= 65 才解除并整体重置为 IDLE 等新 T0 信号.
    """
    today = context.current_dt.date()
    if st['cooldown_until_date'] is None or today < st['cooldown_until_date']:
        return

    # v1.9 Bug I 修复: 用 _safe_get_position 避免对全清后 stock 调用 .get()
    # 触发聚宽 "Security(...) 在 positions 中不存在" WARNING (log9 共 2022 条).
    pos = _safe_get_position(context, stock)
    has_position = pos is not None and pos.total_amount > 0

    # ---- 路径 B (v1.9 新): 退出 c/d 减半 → 保留底仓 → 到期无条件解除 ----
    # 判定条件: has_position 且 last_high_after_grid 为 None (没经过退出 a).
    # 退出 a 必设 last_high_after_grid; 退出 c 减半 / 退出 d 减半都不设 →
    # 用这个差异区分两类 cooldown 来源, 不需要新增字段.
    if has_position and st.get('last_high_after_grid') is None:
        # score 仅用于日志; 不参与解除判定
        score, _ = calc_bottom_score(context, stock)
        score_str = ('%.1f' % score) if score is not None else 'N/A'
        log.info('[%s] 冷却解除 (减半后强制休眠到期 → 重启网格) | 分数=%s / 新基准=%.2f'
                 % (stock, score_str, current_price))
        st['phase'] = 'GRID_RUNNING'
        st['last_grid_price'] = current_price
        st['high_water_pnl_pct'] = 0.0
        st['cooldown_until_date'] = None
        # 清理退出 c 的防抖状态, 让新一轮 high_water 从零开始累积
        st['exit_c_last_action_date'] = None
        st['exit_c_arm_high'] = 0.0
        return

    # ---- 路径 A / C: 维持原 v1.8 行为 (双条件确认) ----
    drawdown_ok = True
    if st['last_high_after_grid'] is not None and st['last_high_after_grid'] > 0:
        drop = (st['last_high_after_grid'] - current_price) / st['last_high_after_grid']
        drawdown_ok = drop >= CONFIG['exit_a_release_drawdown_pct']

    score, _ = calc_bottom_score(context, stock)
    score_ok = score is not None and score >= CONFIG['bottom_score_threshold_keep']

    if not (drawdown_ok and score_ok):
        return

    if has_position:
        # 路径 A: 退出 a 网格闭合后保留底仓
        log.info('[%s] 冷却解除 → 重启网格 (保留底仓) | 分数=%.1f / 新基准=%.2f'
                 % (stock, score, current_price))
        st['phase'] = 'GRID_RUNNING'
        st['last_grid_price'] = current_price
        st['high_water_pnl_pct'] = 0.0
        st['cooldown_until_date'] = None
        st['last_high_after_grid'] = None
        st['exit_c_last_action_date'] = None
        st['exit_c_arm_high'] = 0.0
    else:
        # 路径 C: 持仓为零 → 整体重置为 IDLE
        log.info('[%s] 冷却解除 → 重置为 IDLE (持仓为零) | 分数=%.1f' % (stock, score))
        g.state[stock] = init_state()


# =============================================================================
# 11. 主交易循环
# =============================================================================

def log_universe_score_snapshot(context):
    """周期性打印全候选池分数排行, 直观看到分数分布与失败原因.
    注: 这里所有汇总用显式循环, 避免聚宽沙箱中 numpy.sum 隐式 shadow Python 内置 sum."""
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
    log.info('【分数快照】%s | 候选 %d 只 / 失败 %d 只'
             % (str(context.current_dt.date()), len(rows), failed_count))
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
        log.info('  %s %s 总=%.1f | pb=%.0f dd=%.0f ocf=%.0f rsi=%.0f'
                 % (marker, stock, tot, pb, dd, ocf, rsi))
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

    # 诊断: 统计每条 continue 路径被命中多少次
    skips = {'processed': 0, 'no_cd': 0, 'paused': 0, 'is_st': 0, 'bad_price': 0}
    phase_counts = {'IDLE': 0, 'BUILDING': 0, 'GRID_RUNNING': 0, 'COOLDOWN': 0}
    eval_count = 0
    sample_logged = False

    cur_data = get_current_data()
    for stock in g.quality_universe:
        if stock in g.processed_today:
            skips['processed'] += 1
            continue
        # 注: _CurrentDic 必须用 [] 访问, 不支持 .get
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

        # v1.6 状态自愈: BUILDING / GRID_RUNNING 持仓 0 → 直接重置为 IDLE,
        # 防止退出 d/b/网格 卖空后 exit c 等在残留 high_water 基础上每日重复触发.
        # 不主动转 COOLDOWN, 因为 COOLDOWN 路径需要分数回升才能解除, 反而会卡死;
        # 直接 IDLE 等下一次自然 T0 信号即可.
        # v1.9: 用 _safe_get_position 避免聚宽对不在 positions 中的 stock 打 WARNING.
        if st['phase'] in ('BUILDING', 'GRID_RUNNING'):
            pos_chk = _safe_get_position(context, stock)
            if pos_chk is None or pos_chk.total_amount <= 0:
                log.info('[%s] 状态自愈: phase=%s 持仓=0 → 重置为 IDLE'
                         % (stock, st['phase']))
                g.state[stock] = init_state()
                st = g.state[stock]
            elif (st['phase'] == 'GRID_RUNNING'
                    and pos_chk.value < CONFIG['min_position_value_yuan']):
                # v1.7: 超小持仓自动清算. 经过多次 c/d 减半后只剩几百~几千元的"僵尸",
                # 既无法继续跑网格 (单格金额不足 → 47 条 ERROR 来源), 又每天占用
                # 主循环 evaluate, 不如直接清掉换其他票.
                log.info('[%s] 超小持仓清算: pos=%.0f 元 < 阈值 %.0f → 全清 + COOLDOWN'
                         % (stock, pos_chk.value, CONFIG['min_position_value_yuan']))
                if safe_close_all(stock) is not None:
                    st['phase'] = 'COOLDOWN'
                    st['cooldown_until_date'] = (
                        context.current_dt.date()
                        + timedelta(days=CONFIG['exit_a_cooldown_days']))

        phase_counts[st['phase']] = phase_counts.get(st['phase'], 0) + 1
        eval_count += 1

        try:
            if st['phase'] == 'IDLE':
                try_enter_t0(context, stock, st, last)
            elif st['phase'] == 'BUILDING':
                try_build_t1_t2(context, stock, st, last)
            elif st['phase'] == 'GRID_RUNNING':
                run_grid(context, stock, st, last)
                check_daily_non_intraday_exits(context, stock, st)
            elif st['phase'] == 'COOLDOWN':
                try_exit_cooldown(context, stock, st, last)
        except Exception as e:
            log.warn('[%s] 主循环异常: %s' % (stock, e))

        g.processed_today.add(stock)

    # 周期性快照: 帮助诊断"为什么没人触发"
    if (g.trading_day_counter % CONFIG['diagnostic_log_freq_days']) == 1:
        skip_str = ', '.join(['%s=%d' % (k, v) for k, v in skips.items() if v > 0])
        phase_str = ', '.join(['%s=%d' % (k, v) for k, v in phase_counts.items() if v > 0])
        log.info('[诊断] 主循环 %s | universe=%d / 评估=%d / 跳过={%s} / 阶段={%s}'
                 % (context.current_dt.date(), len(g.quality_universe),
                    eval_count, skip_str or 'none', phase_str or 'none'))
        log_universe_score_snapshot(context)


def intraday_exit_check(context):
    """尾盘 14:50 再次检查退出 b (涨停打开) — 此时日内行情更接近最终状态."""
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
