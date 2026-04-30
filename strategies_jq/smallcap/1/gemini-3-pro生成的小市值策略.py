# -*- coding: utf-8 -*-
"""
策略名称：【2025终极打磨版】国九条小市值（低价+防热+止损）
策略作者：聚宽用户 / 优化版
更新日期：2025-12-14

策略核心逻辑：
1. 【精选标的】：
   - 范围：全A股（剔除科创、北交、ST、次新）
   - 安全：营收 > 3亿 (国九条防退市红线) & 净利 > 0 & 审计无雷
   - 风格：市值最小 + 股价 < 25元 (低价弹性) + 换手 < 30% (拒绝过热)
2. 【交易择时】：
   - 避险：1月、4月强制空仓（躲避年报/预告雷）
   - 熔断：大盘单日暴跌 > 4% 清仓
   - 轮动：每周三固定调仓
3. 【风控体系】：
   - 止损：个股亏损 > 10% 坚决斩仓
   - 止盈：持仓股封板不动，炸板立即止盈
"""

from jqdata import *
from jqfactor import *
import numpy as np
import pandas as pd
import prettytable
from prettytable import PrettyTable
from datetime import time, timedelta
from jqdata import finance
from redistrade_john import *

# ==============================================================================
# 1. 初始化与设置
# ==============================================================================
def initialize(context):
    # 开启防未来函数与真实价格回测
    set_option('avoid_future_data', True)
    set_option('use_real_price', True)
    set_benchmark('399303.XSHE') # 对标国证2000
    
    # 设置实盘级别的交易成本
    # 印花税：卖出千1；佣金：万3；最低佣金：5元
    set_order_cost(OrderCost(
        open_tax=0, 
        close_tax=0.001, 
        open_commission=0.0003, 
        close_commission=0.0003, 
        close_today_commission=0, 
        min_commission=5
    ), type='stock')
    
    # 设置滑点：双边千分之二，模拟小市值股票的冲击成本
    set_slippage(FixedSlippage(0.002))
    
    g.strategy = 'multi_strategy_john'  # 策略名
    
    # === 核心策略参数 ===
    g.stock_num = 5         # 目标持仓数量
    g.trade_day_weekly = 3  # 调仓日：每周三
    g.empty_months = [1, 4] # 空仓月份：1月(业绩预告)、4月(年报)
    g.pass_audit = True     # 是否开启审计意见筛选
    
    # === 选股因子参数 ===
    g.max_price = 20.0      # 股价上限：20元 (ID 63 配置 - 高盈亏比优选)
    g.max_turnover = 30.0   # 换手率上限：30% (防止高位接盘)
    g.min_turnover_amount = 15000000 # 最低成交额：1500万 (保证实盘流动性)
    
    # === 风控参数 ===
    g.stop_loss_pct = 0.10  # 个股止损线：10%
    
    # === 全局变量容器 ===
    g.target_list = []      # 目标持仓列表
    g.hold_list = []        # 当前持仓列表
    g.high_limit_list = []  # 昨日涨停列表

# ==============================================================================
# 执行入口, 定时任务下发
# ==============================================================================
def after_code_changed(context):
    unschedule_all()
    # === 定时任务注册 ===
    # 盘前准备
    run_daily(before_market_open, time='09:00')
    # 选股与调仓
    run_weekly(weekly_trade, weekday=g.trade_day_weekly, time='10:00')
    # 盘中风控
    run_daily(check_limit_break, time='14:00')          # 涨停炸板检查
    run_daily(check_individual_stop_loss, time='14:40') # 个股止损检查
    run_daily(market_stop_loss_check, time='09:35')     # 大盘熔断检查
    # 盘后统计
    run_daily(after_market_close, time='15:30')         # 每日收益统计
    
# ==============================================================================
# 2. 盘前数据准备
# ==============================================================================
def before_market_open(context):
    """
    每日开盘前更新持仓信息和昨日涨停状态
    """
    log.info(f"===> [盘前准备] 开始执行 (日期: {context.current_dt.date()})")
    g.hold_list = list(context.portfolio.positions.keys())
    g.high_limit_list = []
    
    if g.hold_list:
        # 获取昨日收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', 
                      fields=['close','high_limit'], count=1, panel=False)
        # 筛选出昨日收盘封死涨停的股票
        g.high_limit_list = df[df['close'] == df['high_limit']]['code'].tolist()
        
        if g.high_limit_list:
            log.info(f"🔒 [涨停监控] 昨日涨停持仓: {len(g.high_limit_list)}只 {g.high_limit_list}")
    
    log.info(f"===> [盘前准备] 执行完成")

# ==============================================================================
# 3. 核心选股逻辑
# ==============================================================================
def get_compliant_stock_list(context):
    """
    选股漏斗：基础过滤 -> 财务安全 -> 因子筛选 -> 市值排序
    """
    log.info(f"🔍 [选股逻辑] 开始执行选股漏斗...")
    
    # 1. 获取全市场股票并进行基础过滤
    initial_list = get_all_securities(['stock'], date=context.previous_date).index.tolist()
    log.info(f"  > [Step 1] 全市场股票: {len(initial_list)} 只")
    
    initial_list = filter_basic(context, initial_list)
    log.info(f"  > [Step 2] 基础过滤后(停牌/ST/科创/次新): {len(initial_list)} 只")
    
    # 2. 财务与因子筛选 (使用Query加速)
    q = query(
        valuation.code,
        valuation.market_cap
    ).filter(
        valuation.code.in_(initial_list),
        income.operating_revenue > 3e8,     # 核心：营收>3亿，规避新规退市风险
        income.net_profit > 0,              # 核心：拒绝亏损股
        valuation.pb_ratio > 0,             # 拒绝资不抵债
        valuation.turnover_ratio < g.max_turnover, # 拒绝过度炒作
        indicator.roe>0,
        indicator.roa>0
    ).order_by(
        valuation.market_cap.asc()          # 核心：小市值因子
    ).limit(g.stock_num * 8)                # 适度冗余，供后续二次筛选
    
    df = get_fundamentals(q, date=context.previous_date)
    if df.empty: 
        log.warn(f"⚠️ [选股警告] 财务筛选后无结果")
        # --- 诊断代码 Start ---
        log.info("🔍 [诊断模式] 开始排查为何选股结果为空...")
        
        # 1. 检查 valuation 表是否有数据
        q1 = query(valuation.code).filter(valuation.code.in_(initial_list[:10])).limit(10)
        df1 = get_fundamentals(q1, date=context.previous_date)
        log.info(f"  > [诊断] 基础 valuation 表测试 (前10只): {'有数据' if not df1.empty else '无数据 (可能日期问题)'}")
        
        # 2. 检查 income 表条件
        q2 = query(income.code).filter(
            income.code.in_(initial_list),
            income.operating_revenue > 3e8
        ).limit(10)
        df2 = get_fundamentals(q2, date=context.previous_date)
        log.info(f"  > [诊断] 营收 > 3亿 测试: {'有匹配' if not df2.empty else '全军覆没'}")
        
        # 3. 检查 net_profit 表条件
        q3 = query(income.code).filter(
            income.code.in_(initial_list),
            income.net_profit > 0
        ).limit(10)
        df3 = get_fundamentals(q3, date=context.previous_date)
        log.info(f"  > [诊断] 净利 > 0 测试: {'有匹配' if not df3.empty else '全军覆没'}")
        
        # 4. 检查 PB 条件
        q4 = query(valuation.code).filter(
            valuation.code.in_(initial_list),
            valuation.pb_ratio > 0
        ).limit(10)
        df4 = get_fundamentals(q4, date=context.previous_date)
        log.info(f"  > [诊断] PB > 0 测试: {'有匹配' if not df4.empty else '全军覆没'}")
        
        log.info("🔍 [诊断模式] 结束")
        # --- 诊断代码 End ---
        return []
    candidate_list = df['code'].tolist()
    log.info(f"  > [Step 3] 财务与市值筛选后(取前{g.stock_num*8}): {len(candidate_list)} 只")
    
    # 3. 价格与流动性二次过滤
    valid_candidates = []
    current_data = get_current_data()
    
    dropped_price = 0
    dropped_amt = 0
    
    for stock in candidate_list:
        # 过滤高价股
        if current_data[stock].last_price > g.max_price:
            dropped_price += 1
            continue
            
        # 过滤流动性枯竭的僵尸股
        amt = get_price(stock, end_date=context.previous_date, count=1, fields=['money'])['money'][0]
        if amt < g.min_turnover_amount: 
            dropped_amt += 1
            continue
            
        valid_candidates.append(stock)
            
    log.info(f"  > [Step 4] 价格与流动性过滤: 剔除高价股{dropped_price}只, 剔除低流动性{dropped_amt}只 -> 剩余 {len(valid_candidates)} 只")
    
    # 4. 审计意见排雷
    if g.pass_audit:
        before_audit = len(valid_candidates)
        valid_candidates = filter_audit_opinion(context, valid_candidates)
        after_audit = len(valid_candidates)
        if before_audit != after_audit:
             log.info(f"  > [Step 5] 审计排雷: 剔除 {before_audit - after_audit} 只 -> 剩余 {after_audit} 只")
        else:
             log.info(f"  > [Step 5] 审计排雷: 无风险股剔除")
    
    log.info(f"📋 [选股完成] 最终候选: {len(valid_candidates)} 只，截取前 {g.stock_num} 只")
    return valid_candidates[:g.stock_num]

# ==============================================================================
# 4. 交易逻辑
# ==============================================================================
def weekly_trade(context):
    """
    周度调仓主函数
    """
    log.info(f"===> [周度调仓] 开始执行 (日期: {context.current_dt.date()})")
    # --- 1. 月份避险逻辑 ---
    if context.current_dt.month in g.empty_months:
        log.info(f"📅 [月度避险] 当前月份 {context.current_dt.month} 在避险名单中 {g.empty_months}")
        if len(context.portfolio.positions) > 0:
            log.info(f"🛑 [月度避险] 执行清仓操作...")
            for stock in list(context.portfolio.positions.keys()):
                order_target_value_(context, stock, 0)
        else:
            log.info(f"🛡️ [月度避险] 当前为空仓状态，继续保持")
        log.info(f"===> [周度调仓] 避险期执行完成")
        return

    # --- 2. 获取本周目标 ---
    target_list = get_compliant_stock_list(context)
    g.target_list = target_list
    
    # 打印目标列表详情
    target_names = [f"{get_security_info(s).display_name}({s})" for s in target_list]
    log.info(f"🎯 [本周目标] 选出 {len(target_list)} 只标的: {target_names}")
    
    # --- 3. 卖出逻辑 ---
    log.info(f"📉 [卖出逻辑] 开始检查当前持仓({len(g.hold_list)}只)...")
    current_data = get_current_data()
    for stock in g.hold_list:
        stock_name = get_security_info(stock).display_name
        # 卖出不在目标池且昨日未涨停的股票
        if stock not in target_list and stock not in g.high_limit_list:
            # 跌停板无法卖出检查
            if current_data[stock].last_price <= current_data[stock].low_limit:
                log.info(f"⏭️ [跳过卖出] {stock_name}({stock}) - 原因: 跌停无法卖出")
                continue
            
            log.info(f"📤 [调仓卖出] {stock_name}({stock}) - 原因: 不在目标池且非昨日涨停")
            order_target_value_(context, stock, 0)
        else:
            reason = "昨日涨停" if stock in g.high_limit_list else "仍在目标池"
            log.info(f"🆗 [持仓不动] {stock_name}({stock}) - 原因: {reason}")
            
    # --- 4. 买入逻辑 ---
    current_holdings = len(context.portfolio.positions)
    log.info(f"📈 [买入逻辑] 当前持仓: {current_holdings}/{g.stock_num}")
    
    if current_holdings < g.stock_num:
        available_cash = context.portfolio.available_cash
        to_buy_count = g.stock_num - current_holdings
        per_stock_value = available_cash / to_buy_count if to_buy_count > 0 else 0
        
        log.info(f"💰 [资金分配] 可用资金: {available_cash:.2f}, 计划买入: {to_buy_count}只, 单只预算: {per_stock_value:.2f}")
        
        for stock in target_list:
            if stock in context.portfolio.positions:
                stock_name = get_security_info(stock).display_name
                log.info(f"⏭️ [跳过买入] {stock_name}({stock}) - 原因: 已在持仓中")
                continue

            current_data = get_current_data()
            stock_name = get_security_info(stock).display_name
            
            # 跳过涨停或停牌
            if current_data[stock].last_price >= current_data[stock].high_limit: 
                log.info(f"⏭️ [跳过买入] {stock_name}({stock}) - 原因: 一字涨停无法买入")
                continue
            if current_data[stock].paused: 
                log.info(f"⏭️ [跳过买入] {stock_name}({stock}) - 原因: 停牌")
                continue
            
            # 手动计算股数，消除API警告
            price = current_data[stock].last_price
            if price <= 0: 
                log.info(f"⏭️ [跳过买入] {stock_name}({stock}) - 原因: 价格异常({price})")
                continue
            
            # 计算可用资金购买的股数（向下取整100股）
            buy_cash = min(per_stock_value, context.portfolio.available_cash)
            amount = int(buy_cash / price / 100) * 100
            
            if amount >= 100:
                log.info(f"🛒 [调仓买入] {stock_name}({stock}) 价格:{price:.2f} 数量:{amount} 金额:{amount*price:.2f}")
                order_target_(context, stock, amount)
            else:
                log.info(f"⚠️ [资金不足] {stock_name}({stock}) 预算{buy_cash:.2f} 不足买入1手(股价{price:.2f})")
            
            # 达到持仓上限停止买入
            if len(context.portfolio.positions) >= g.stock_num: 
                log.info(f"✅ [买入结束] 已达到持仓上限 {g.stock_num}只")
                break
    else:
        log.info(f"✅ [买入跳过] 持仓已满 ({current_holdings}/{g.stock_num})")
        
    log.info(f"===> [周度调仓] 执行完成")

# ==============================================================================
# 5. 盘中风控逻辑
# ==============================================================================
def check_limit_break(context):
    """
    涨停炸板检查：如果持仓股昨日涨停，今日开板，立即卖出
    """
    if not g.high_limit_list: return
    log.info(f"===> [涨停炸板检查] 开始执行")
    current_data = get_current_data()
    
    for stock in g.high_limit_list:
        if stock in context.portfolio.positions:
            price = current_data[stock].last_price
            high_limit = current_data[stock].high_limit
            
            if price < high_limit:
                stock_name = get_security_info(stock).display_name
                # 跌停板无法卖出检查
                if price <= current_data[stock].low_limit:
                    log.info(f"⏭️ [跳过炸板卖出] {stock_name}({stock}) - 原因: 跌停无法卖出")
                    continue
                
                log.info(f"🌊 [涨停炸板] {stock_name}({stock}) 打开涨停，执行止盈卖出")
                order_target_value_(context, stock, 0)
    log.info(f"===> [涨停炸板检查] 执行完成")

def check_individual_stop_loss(context):
    """
    个股止损检查：亏损超过设定比例坚决卖出
    """
    positions = context.portfolio.positions
    if not positions: return
    log.info(f"===> [个股止损检查] 开始执行")
    current_data = get_current_data()
    for stock in list(positions.keys()):
        if stock in g.high_limit_list: continue # 涨停股不止损
        
        pos = positions[stock]
        if pos.avg_cost == 0: continue
        
        loss_pct = 1 - (pos.price / pos.avg_cost)
        if loss_pct > g.stop_loss_pct:
            stock_name = get_security_info(stock).display_name
            # 跌停板无法止损检查
            if current_data[stock].last_price <= current_data[stock].low_limit:
                log.info(f"⏭️ [跳过止损] {stock_name}({stock}) - 原因: 跌停无法卖出")
                continue
                
            log.info(f"✂️ [个股止损] {stock_name}({stock}) 亏损 {loss_pct:.2%} (> {g.stop_loss_pct:.0%})，坚决卖出")
            order_target_value_(context, stock, 0)
    log.info(f"===> [个股止损检查] 执行完成")

def market_stop_loss_check(context):
    """
    大盘熔断检查：国证2000单日跌幅超过4%清仓
    """
    log.info(f"===> [大盘熔断检查] 开始执行")
    index_code = '399303.XSHE' 
    hist = get_price(index_code, end_date=context.previous_date, frequency='daily', fields=['close'], count=2)
    pct_change = (hist['close'][-1] - hist['close'][-2]) / hist['close'][-2]
    
    if pct_change < -0.04: 
        log.info(f"⚠️ [系统熔断] 大盘昨日暴跌 {pct_change:.2%}，执行风控清仓")
        current_data = get_current_data()
        for stock in list(context.portfolio.positions.keys()):
            # 即使熔断，如果是个股涨停，通常可以稍微观察一下
            if stock in g.high_limit_list:
                continue
                
            # 跌停板无法卖出检查
            if current_data[stock].last_price <= current_data[stock].low_limit:
                stock_name = get_security_info(stock).display_name
                log.info(f"⏭️ [跳过熔断卖出] {stock_name}({stock}) - 原因: 跌停无法卖出")
                continue
                
            order_target_value_(context, stock, 0)
    log.info(f"===> [大盘熔断检查] 执行完成")

# ==============================================================================
# 6. 辅助筛选函数
# ==============================================================================
def filter_basic(context, stock_list):
    """
    基础过滤：剔除不符合交易规则的股票
    """
    curr_data = get_current_data()
    filtered = []
    for stock in stock_list:
        # 剔除停牌
        if curr_data[stock].paused: continue
        # 剔除ST/退市
        if curr_data[stock].is_st or '退' in curr_data[stock].name: continue
        # 剔除科创(688)、北交(4/8)
        if stock.startswith(('688', '4', '8')): continue
        # 剔除次新股 (上市不满1年)
        start_date = get_security_info(stock).start_date
        if (context.previous_date - start_date).days < 365: continue
        
        filtered.append(stock)
    return filtered

def filter_audit_opinion(context, stock_list):
    """
    审计意见过滤：剔除有严重财务风险的公司
    """
    curr_date = context.current_dt.date()
    # 追溯过去两年的审计报告
    two_years_ago = curr_date - datetime.timedelta(days=730)
    try:
        # 筛选意见类型为 4(否定) 或 5(无法表示意见) 的记录
        # 审计意见类型编码	审计意见类型
        # 1	无保留
        # 2	无保留带解释性说明
        # 3	保留意见
        # 4	拒绝/无法表示意见
        # 5	否定意见
        # 6	未经审计
        # 7	保留带解释性说明
        # 10	经审计（不确定具体意见类型）
        # 11	无保留带持续经营重大不确定性
        q = query(finance.STK_AUDIT_OPINION.code).filter(
            finance.STK_AUDIT_OPINION.code.in_(stock_list),
            finance.STK_AUDIT_OPINION.report_type == 0, 
            finance.STK_AUDIT_OPINION.opinion_type_id > 2,
            finance.STK_AUDIT_OPINION.opinion_type_id != 6,#6:未经审计，季报
            finance.STK_AUDIT_OPINION.pub_date >= two_years_ago
        )
            
        df = finance.run_query(q)
        if not df.empty:
            bad_stocks = set(df['code'].tolist())
            filtered = [s for s in stock_list if s not in bad_stocks]
            if len(bad_stocks) > 0:
                log.info(f"☢️ [审计排雷] 剔除 {len(bad_stocks)} 只风险股")
            return filtered
        else:
            return stock_list
    except Exception as e:
        log.warn(f"审计数据查询失败: {e}")
        return stock_list

# ==============================================================================
# 7. 盘后统计与日志
# ==============================================================================
def after_market_close(context):
    """
    盘后统计函数
    """
    log.info(f"===> [盘后统计] 开始执行")
    print_summary(context)
    log.info(f"===> [盘后统计] 执行完成")

def print_summary(context):
    """
    制表展示每日收益
    """
    # 获取总资产
    total_value = round(context.portfolio.total_value, 2)
    # 获取可用资金
    available_cash = round(context.portfolio.available_cash, 2)
    # 获取当前持仓
    current_stocks = context.portfolio.positions
    
    if not current_stocks:
        log.info(f"🚤 [空仓提示] 当前总资产: {total_value}，可用资金: {available_cash}，休息中...")
        return

    # 创建表格
    table = PrettyTable([
        "股票代码",
        "股票名称",
        "持仓数量",
        "持仓均价",
        "当前价格",
        "盈亏金额",
        "盈亏比例",
        "股票市值",
        "仓位占比"
    ])
    table.hrules = prettytable.ALL  # 显示所有水平线
    
    total_market_value = 0
    for stock in current_stocks:
        pos = current_stocks[stock]
        current_shares = pos.total_amount
        current_price = round(pos.price, 3)
        avg_cost = round(pos.avg_cost, 3)
        
        # 计算盈亏
        profit_amount = round((current_price - avg_cost) * current_shares, 2)
        profit_ratio = (current_price - avg_cost) / avg_cost if avg_cost != 0 else 0
        profit_ratio_str = f"{profit_ratio * 100:.2f}% {'↑' if profit_ratio > 0 else '↓' if profit_ratio < 0 else ''}"
        
        # 计算市值
        market_value = round(current_shares * current_price, 2)
        total_market_value += market_value
        
        # 股票名称
        stock_name = get_security_info(stock).display_name
        
        # 添加到表格
        table.add_row([
            stock,
            stock_name,
            current_shares,
            avg_cost,
            current_price,
            profit_amount,
            profit_ratio_str,
            market_value,
            f"{market_value / total_value * 100:.2f}%"
        ])
    
    # 添加汇总行
    table.add_row(["总计", "-", "-", "-", "-", "-", "-", f"{total_market_value:.2f}", f"{total_market_value / total_value * 100:.2f}%"])
    table.add_row(["可用资金", "-", "-", "-", "-", "-", "-", f"{available_cash:.2f}", f"{available_cash / total_value * 100:.2f}%"])
    table.add_row(["总资产", "-", "-", "-", "-", "-", "-", f"{total_value:.2f}", "100.00%"])
    
    print(f"\n📊 [每日持仓汇总] 日期: {context.current_dt.date()}\n{table}")