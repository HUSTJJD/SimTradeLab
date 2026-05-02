# -*- coding: utf-8 -*-
"""
布林均值回归策略 (Bollinger Mean Reversion Strategy)

策略概述:
本策略基于布林带（Bollinger Bands）进行均值回归交易，主要针对沪深300成分股。
策略捕捉股价超跌反弹的机会（触及布林下轨），并在股价回归高位（触及布林上轨）或达到预设盈亏条件时退出。

核心参数:
- 股票范围: 沪深300成分股
- 止损阈值: 4%
- 止盈阈值: 300%
- 最长持有: 25天
"""

import talib
import numpy as np

def initialize(context):
    """初始化策略"""
    # 设置基准为沪深300
    set_benchmark('000300.SS')
    
    # 策略参数
    context.timeperiod = 20       # 布林带周期
    context.nbdev = 2             # 布林带标准差倍数
    context.filter_days = 60      # 趋势过滤周期
    context.filter_pct = 0.94     # 趋势过滤比例 (当前价 < 60日最高价 * 0.94)
    context.profit_filter_pct = 0.11 # 卖出盈利过滤 (当前价 > 买入价 * 1.11)
    
    context.stop_loss = 0.04      # 止损 4%
    context.take_profit = 3.0     # 止盈 300%
    context.max_hold_days = 25    # 最长持有 25天
    
    # 记录持仓天数 {stock: days}
    context.hold_days = {}
    
    # 每次买入的资金比例 (假设最大持仓20只，避免资金分散过细或过于集中)
    context.max_pos_count = 20
    
    log.info("策略初始化完成")

def before_trading_start(context, data):
    """盘前处理"""
    # 获取沪深300成分股
    context.stocks = get_index_stocks('000300.SS')
    if not context.stocks:
        log.warning("未获取到 000300.SS 成分股，尝试使用 get_Ashares()")
        context.stocks = get_Ashares()[:300] # 兜底:取前300只

    log.info("初始化股票池: 数量: {}".format(len(context.stocks)))
    if len(context.stocks) > 0:
        log.info("示例股票: {}".format(context.stocks[:5]))

def handle_data(context, data):
    """主策略逻辑"""
    
    # 1. 更新持仓天数并处理持仓风控 (止损/止盈/超时)
    positions = context.portfolio.positions
    current_positions = [s for s, p in positions.items() if p.amount > 0]
    
    # 清理不在持仓中的股票记录
    for s in list(context.hold_days.keys()):
        if s not in current_positions:
            del context.hold_days[s]
            
    # 批量获取历史数据 (尝试一次性获取所有关注股票的数据以优化性能)
    # 我们需要足够的历史数据来计算 60日High 和 20日Bollinger
    # 需要: max(60, 20 + ?). 取 70天安全
    history_window = 70
    
    # 注意: get_history 可能会比较慢，如果股票池很大。
    # 这里为了演示逻辑，我们分批处理或者逐个处理。
    # 考虑到 simple 策略是逐个获取，我们先尝试批量获取。
    # 如果批量获取不支持，回退到逐个。
    # 为了稳健性，这里使用逐个遍历，或者分批。
    # 由于 300 只股票串行获取可能较慢，但对于回测引擎通常可以接受。
    
    # 获取所有股票的收盘价、最高价、最低价
    # 这里的 data 对象在 simple 策略中未被充分利用，主要用 get_history
    
    # 遍历持仓股票进行卖出检查
    for stock in current_positions:
        # 更新持仓天数
        context.hold_days[stock] = context.hold_days.get(stock, 0) + 1
        days_held = context.hold_days[stock]
        
        position = positions[stock]
        avg_cost = position.cost_basis
        
        # 获取该股票历史数据
        h = get_history(history_window, '1d', ['close', 'high', 'low'], [stock], is_dict=True)
        if stock not in h:
            continue
            
        # 提取数据
        try:
            closes = h[stock]['close']
            highs = h[stock]['high']
            lows = h[stock]['low']
        except KeyError:
            # 兼容可能的格式差异
            continue
            
        if len(closes) < context.timeperiod + 2:
            continue
            
        current_price = closes[-1]
        
        # 1. 止损平仓: 亏损幅度达到 4%
        pnl_pct = (current_price - avg_cost) / avg_cost
        if pnl_pct <= -context.stop_loss:
            order_target(stock, 0)
            log.info("[止损平仓] {} 亏损 {:.2%}".format(stock, pnl_pct))
            continue
            
        # 2. 止盈平仓: 盈利幅度达到 300%
        if pnl_pct >= context.take_profit:
            order_target(stock, 0)
            log.info("[止盈平仓] {} 盈利 {:.2%}".format(stock, pnl_pct))
            continue
            
        # 3. 超时平仓: 持仓天数达到 25 天
        if days_held >= context.max_hold_days:
            order_target(stock, 0)
            log.info("[超时平仓] {} 持有 {} 天".format(stock, days_held))
            continue
            
        # 4. 技术面卖出规则
        # 计算布林带
        upper, middle, lower = talib.BBANDS(
            closes, 
            timeperiod=context.timeperiod, 
            nbdevup=context.nbdev, 
            nbdevdn=context.nbdev
        )
        
        # 条件1: 前一交易日最高价下穿布林带上轨
        # High[T-1] < Upper[T-1] AND High[T-2] > Upper[T-2]
        prev_high = highs[-2]
        prev_upper = upper[-2]
        prev2_high = highs[-3]
        prev2_upper = upper[-3]
        
        tech_signal = (prev_high < prev_upper) and (prev2_high > prev2_upper)
        
        # 条件2: 盈利过滤 (当前价较买入价涨幅 >= 11%)
        profit_signal = pnl_pct >= context.profit_filter_pct
        
        if tech_signal and profit_signal:
            order_target(stock, 0)
            log.info("[技术卖出] {} 下穿上轨且盈利达标 ({:.2%})".format(stock, pnl_pct))

    # -----------------------------------------------------------
    # 买入逻辑
    # -----------------------------------------------------------
    # 检查是否还有仓位空间
    current_pos_count = len([s for s, p in positions.items() if p.amount > 0])
    if current_pos_count >= context.max_pos_count:
        return

    # 遍历候选股票 (为了性能，可以随机抽取或者只遍历部分，但这里遍历全部)
    # 实际生产中可能需要优化
    for stock in context.stocks:
        if stock in current_positions:
            continue
            
        # 获取历史数据
        h = get_history(history_window, '1d', ['close', 'high', 'low'], [stock], is_dict=True)
        if stock not in h:
            continue
            
        try:
            closes = h[stock]['close']
            highs = h[stock]['high']
            lows = h[stock]['low']
        except:
            continue
            
        if len(closes) < context.filter_days:
            continue
            
        # 计算指标
        upper, middle, lower = talib.BBANDS(
            closes, 
            timeperiod=context.timeperiod, 
            nbdevup=context.nbdev, 
            nbdevdn=context.nbdev
        )
        
        # 条件1: 技术形态 - 前一交易日最低价上穿布林带下轨
        # Low[T-1] > Lower[T-1] AND Low[T-2] < Lower[T-2]
        prev_low = lows[-2]
        prev_lower = lower[-2]
        prev2_low = lows[-3]
        prev2_lower = lower[-3]
        
        tech_entry = (prev_low > prev_lower) and (prev2_low < prev2_lower)
        
        if not tech_entry:
            # log.debug("{} 未触发技术信号".format(stock))
            continue
        
        # 条件2: 趋势过滤 - 当前股价 < 60日最高价 * 0.94
        # 60日最高价: 过去60天 (不包括今天? 还是包括? 通常指历史窗口)
        # 假设为包含昨天的过去60天
        max_60_high = np.max(highs[-60:]) 
        current_price = closes[-1]
        
        trend_filter = current_price < (max_60_high * context.filter_pct)
        
        if tech_entry and trend_filter:
            # 执行买入
            # 等权重买入剩余资金的一部分，或者按固定比例
            # 这里简单起见，按 (总资产 / max_pos_count) 分配
            target_value = context.portfolio.portfolio_value / context.max_pos_count
            
            # 检查现金是否足够
            if context.portfolio.cash >= target_value * 0.9: # 留点缓冲
                order_value(stock, target_value)
                context.hold_days[stock] = 0 # 初始化持有天数
                log.info("[买入信号] {} 上穿下轨且处于低位 (P={:.2f}, High60={:.2f})".format(stock, current_price, max_60_high))
                
                # 更新持仓计数，防止单日买入过多超限
                current_pos_count += 1
                if current_pos_count >= context.max_pos_count:
                    break

def after_trading_end(context, data):
    """盘后处理"""
    positions = context.portfolio.positions
    pos_count = sum(1 for p in positions.values() if p.amount > 0)
    log.info("日终持仓: {} 只, 总资产: {:.2f}".format(pos_count, context.portfolio.portfolio_value))
