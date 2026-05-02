# -*- coding: utf-8 -*-
import numpy as np

def initialize(context):
    """初始化策略"""
    # 设置基准: 沪深300
    set_benchmark('000300.SS')
    
    # 策略参数
    context.stock = '002626.SZ'  # 金达威
    context.initial_qty = 3000   # 底仓
    context.grid_qty = 500       # 网格交易数量
    context.grid_pct = 0.03      # 网格幅度 3%
    
    # 状态变量
    context.last_grid_price = None
    context.is_initialized = False
    
    log.info("网格交易策略(金达威)初始化完成")

def before_trading_start(context, data):
    """盘前处理"""
    pass

def handle_data(context, data):
    """每日交易逻辑"""
    stock = context.stock
    
    # 确保有数据
    # 注意: SimTradeLab的data对象是惰性加载的，不能用 'if stock not in data' 判断
    current_price = data[stock].close
    
    if np.isnan(current_price):
        return
    
    # 1. 建仓逻辑 (首日)
    if not context.is_initialized:
        # 检查是否有资金 (这里假设资金充足，或者框架会自动拒绝)
        log.info("策略启动，建立底仓: {}, 数量: {}, 价格: {:.2f}".format(stock, context.initial_qty, current_price))
        order(stock, context.initial_qty)
        
        # 记录初始基准价格
        context.last_grid_price = current_price
        context.is_initialized = True
        return

    # 2. 网格交易逻辑
    if context.last_grid_price is None:
        return

    # 计算相对于上次网格价格的涨跌幅
    pct_change = (current_price - context.last_grid_price) / context.last_grid_price
    
    # 涨 3% -> 卖出
    if pct_change >= context.grid_pct:
        log.info("触发网格卖出: 涨幅 {:.2%}, 当前价格 {:.2f}, 上次价格 {:.2f}".format(
            pct_change, current_price, context.last_grid_price))
        
        # 卖出500股
        order(stock, -context.grid_qty)
        
        # 更新基准价格
        context.last_grid_price = current_price
        
    # 跌 3% -> 买入
    elif pct_change <= -context.grid_pct:
        log.info("触发网格买入: 跌幅 {:.2%}, 当前价格 {:.2f}, 上次价格 {:.2f}".format(
            pct_change, current_price, context.last_grid_price))
        
        # 买入500股
        order(stock, context.grid_qty)
        
        # 更新基准价格
        context.last_grid_price = current_price
