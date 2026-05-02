
# -*- coding: utf-8 -*-
# from simtradelab.research.api import *
import pandas as pd
import numpy as np

def initialize(context):
    """初始化策略"""
    # 设置基准: 沪深300
    set_benchmark('000300.SS')
    
    # 策略参数
    context.max_stocks = 5
    context.stop_loss_pct = 0.10
    context.market_stop_loss_pct = 0.04
    
    # 记录上次调仓日期，避免重复执行
    context.last_rebalance_date = None
    
    log.info("国九条合规小市值策略初始化完成")

def before_trading_start(context, data):
    """盘前处理"""
    pass

def handle_data(context, data):
    """每日/每分钟交易逻辑"""
    current_date = context.current_dt.date()
    
    # 1. 风控检查 (每日/每Bar执行)
    market_risk = check_market_risk(context, data)
    
    if market_risk:
        # 如果触发市场熔断，不进行买入操作，仅执行清仓
        return

    # 2. 个股止损 (每日/每Bar执行)
    check_individual_stop_loss(context, data)
    
    # 3. 定期调仓 (周三)
    # 确保每天只执行一次调仓逻辑
    if context.last_rebalance_date != current_date:
        if context.current_dt.weekday() == 2:  # 周三 (0=周一, 2=周三)
            log.info("周三调仓日: {}".format(current_date))
            rebalance(context)
            context.last_rebalance_date = current_date

def check_market_risk(context, data):
    """大盘熔断风控"""
    # 获取基准指数最近2日收盘价
    bm = '000300.SS'
    hist = get_price(bm, count=2, fields=['close'])
    
    if len(hist) < 2:
        return False
        
    # 计算涨跌幅
    pct_change = (hist['close'].iloc[-1] - hist['close'].iloc[-2]) / hist['close'].iloc[-2]
    
    if pct_change < -context.market_stop_loss_pct:
        log.info("触发大盘熔断! 基准跌幅: {:.2%}".format(pct_change))
        # 全仓清仓
        for stock in list(context.portfolio.positions.keys()):
            order_target(stock, 0)
        return True
    
    return False

def check_individual_stop_loss(context, data):
    """个股止损"""
    positions = context.portfolio.positions
    for stock in list(positions.keys()):
        pos = positions[stock]
        if pos.amount > 0:
            # 获取当前价格
            if stock in data:
                current_price = data[stock].close
                cost = pos.cost_basis
                if cost > 0:
                    pnl_pct = (current_price - cost) / cost
                    
                    if pnl_pct < -context.stop_loss_pct:
                        log.info("触发个股止损: {}, 亏损: {:.2%}".format(stock, pnl_pct))
                        order_target(stock, 0)

def rebalance(context):
    """调仓逻辑"""
    current_month = context.current_dt.month
    
    # 月度避险: 1月和4月空仓
    if current_month in [1, 4]:
        log.info("当前为{}月避险期，清空持仓".format(current_month))
        for stock in list(context.portfolio.positions.keys()):
            order_target(stock, 0)
        return

    # 1. 股票池筛选
    # 获取所有A股
    all_stocks = get_Ashares()
    
    # 剔除 科创板(688), 北交所(8/4), ST, 停牌
    target_stocks = []
    for stock in all_stocks:
        if stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
            continue
        target_stocks.append(stock)
        
    log.info("初筛后股票数量: {}".format(len(target_stocks)))
    
    # 剔除ST (需获取ST状态)
    # 注意: get_stock_status 可能比较耗时，或者不可用，这里尝试使用
    try:
        # 假设返回 {stock: True/False} 或 Series
        st_status = get_stock_status(target_stocks, 'ST')
        if isinstance(st_status, dict):
            target_stocks = [s for s in target_stocks if not st_status.get(s, False)]
        elif isinstance(st_status, pd.Series):
            target_stocks = [s for s in target_stocks if not st_status.get(s, False)]
        log.info("剔除ST后数量: {}".format(len(target_stocks)))
    except Exception as e:
        log.warn("获取ST状态失败: {}, 跳过ST过滤".format(e))
    
    if not target_stocks:
        return

    # 2. 基本面过滤
    # 营收 > 3亿, 净利润 > 0
    try:
        # 获取 income 表数据
        q_inc = get_fundamentals(target_stocks, 'income', ['operating_revenue', 'net_profit'])
        
        # 获取 profit_ability 表数据 (用于补全营收)
        q_prof = get_fundamentals(target_stocks, 'profit_ability', ['net_profit_ratio'])
        
        # 合并
        q_fund = pd.concat([q_inc, q_prof], axis=1)
        
        log.info("获取到基本面数据: {}条".format(len(q_fund)))
        
        # 转换为数值类型
        q_fund['operating_revenue'] = pd.to_numeric(q_fund['operating_revenue'], errors='coerce')
        q_fund['net_profit'] = pd.to_numeric(q_fund['net_profit'], errors='coerce')
        q_fund['net_profit_ratio'] = pd.to_numeric(q_fund['net_profit_ratio'], errors='coerce')
        
        # 补全缺失的 operating_revenue
        # Revenue = NetProfit / NetProfitRatio
        # 注意: net_profit_ratio 可能是小数(0.1)或百分比(10)
        # 根据 debug_fundamentals.py 观察, 是小数 (0.52 = 52%)
        
        # 仅对 operating_revenue 为 NaN 的行进行计算
        mask_missing_rev = q_fund['operating_revenue'].isnull()
        
        if mask_missing_rev.any():
            # 避免除以0
            mask_valid_ratio = (q_fund['net_profit_ratio'].abs() > 0.0001)
            mask_calc = mask_missing_rev & mask_valid_ratio
            
            q_fund.loc[mask_calc, 'operating_revenue'] = q_fund.loc[mask_calc, 'net_profit'] / q_fund.loc[mask_calc, 'net_profit_ratio']
            
            log.info("补全营收数据: {}条".format(mask_calc.sum()))

        # 过滤
        q_fund = q_fund[ (q_fund['operating_revenue'] > 300000000) & (q_fund['net_profit'] > 0) ]
        target_stocks = q_fund.index.tolist()
        log.info("基本面筛选后数量: {}".format(len(target_stocks)))

        # 增加行情活跃度过滤 (剔除停牌/无行情股票)
        # 这一步是为了解决“基本面有数据但行情无数据”的问题 (如停牌、退市整理期)
        # 此时 DataServer 已优化为 preload 模式，且 price 数据常驻内存，批量检查速度很快
        log.info("开始行情活跃度过滤 ({}只)...".format(len(target_stocks)))
        valid_stocks = []
        
        # 批量获取行情（检查昨天是否有数据，或者最近5天是否有数据）
        # 这里为了简单，检查最近 5 天是否有交易
        valid_stocks = []
        
        # 这里的 get_price 在内存中查询，速度很快
        # 我们只需要检查是否有数据，不需要具体价格
        # 使用 count=1 检查最近一天
        for stock in target_stocks:
            try:
                # 简单检查: 获取最近1天数据
                hist = get_price(stock, count=1, fields=['close'])
                if not hist.empty:
                    # 额外检查：确保数据时间在当前时间之前（避免未来数据）
                    if hist.index[-1] <= context.current_dt:
                        valid_stocks.append(stock)
            except Exception as e:
                # 单个股票检查失败不应影响整体
                # log.warn("检查股票 {} 活跃度失败: {}".format(stock, e)) 
                continue
        
        log.info("行情过滤后数量: {} (剔除 {} 只)".format(len(valid_stocks), len(target_stocks) - len(valid_stocks)))
        target_stocks = valid_stocks
        
    except Exception as e:
        log.error("获取基本面数据失败: {}".format(e))
        return

    if not target_stocks:
        log.info("基本面筛选后无股票")
        return

    # 3. 排序与初选
    # 按总市值从小到大排序
    try:
        q_val = get_fundamentals(target_stocks, 'valuation', ['total_value', 'turnover_rate'])
        q_val = q_val.sort_values('total_value', ascending=True)
        log.info("获取到估值数据: {}条".format(len(q_val)))
        
        # 取前40只
        candidates = q_val.head(40).index.tolist()
    except Exception as e:
        log.error("获取估值数据失败: {}".format(e))
        return

    # 4. 二次过滤
    # 股价 < 25, 换手率 < 30%, 成交额 > 1500万
    final_list = []
    
    log.info("开始二次过滤，候选股票数: {}".format(len(candidates)))
    for i, stock in enumerate(candidates):
        try:
            # 获取最新一天行情
            hist = get_price(stock, count=1, fields=['close', 'money'])
            if hist.empty:
                log.info("[{}] {}: 无行情数据".format(i, stock))
                continue
                
            price = hist['close'].iloc[-1]
            money = hist['money'].iloc[-1] # 成交额
            
            # 换手率
            turnover_rate = 0
            if stock in q_val.index:
                turnover_rate = q_val.loc[stock, 'turnover_rate']
            
            # 检查条件
            # 换手率单位通常是 %，如 3.5 代表 3.5%
            cond_price = price < 25
            cond_turnover = turnover_rate < 30
            cond_money = money > 15000000
            
            if cond_price and cond_turnover and cond_money:
                final_list.append(stock)
                log.info("[{}] {}: 入选 (Price={:.2f}, TO={:.2f}, Money={:.0f}万)".format(i, stock, price, turnover_rate, money/10000))
            else:
                # Debug logging for rejection
                if len(final_list) == 0: # Log first few rejections
                     log.info("[{}] {}: 剔除 (Price={:.2f}, TO={:.2f}, Money={:.0f}万) Cond: P{} T{} M{}".format(i, stock, price, turnover_rate, money/10000, cond_price, cond_turnover, cond_money))
                
            if len(final_list) >= context.max_stocks:
                break
        except Exception as e:
            log.warn("选股循环异常 {}: {}".format(stock, e))
            continue
            
    log.info("选股完成: {}".format(final_list))

    # 5. 执行交易
    current_positions = list(context.portfolio.positions.keys())
    
    # 卖出不在名单中的
    for stock in current_positions:
        if stock not in final_list:
            order_target(stock, 0)
            
    # 买入名单中的
    if final_list:
        # 等权重买入
        target_value = context.portfolio.total_value / len(final_list)
        for stock in final_list:
            order_target_value(stock, target_value)

def after_trading_end(context, data):
    """盘后处理"""
    pass
