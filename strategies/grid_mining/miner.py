# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from simtradelab.ptrade.api import PtradeAPI
from simtradelab.ptrade.context import Context
from simtradelab.ptrade.object import Portfolio
from simtradelab.service.data_server import DataServer
import logging

# 全局 API 实例
_global_api = None
_global_data_server = None

def init_api(data_path=None, required_data=None):
    """初始化研究API（延迟加载模式）

    Args:
        data_path: 数据路径，默认None自动查找项目data目录
        required_data: 需要加载的数据类型，默认None（按需加载）
                      可显式指定: {'price', 'exrights', 'valuation', 'fundamentals'}

    Returns:
        PtradeAPI实例

    Note:
        如果required_data=None，数据将在首次访问时按需加载
    """
    global _global_api, _global_data_server

    # 默认不预加载，按需加载（传递空集合，不是None）
    if required_data is None:
        required_data = set()

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    log = logging.getLogger('research_api')

    if required_data:
        print(f"正在加载数据: {', '.join(required_data)}...")
    else:
        print(f"初始化API（按需加载模式）...")

    _global_data_server = DataServer(required_data=required_data)

    # 创建portfolio和context
    portfolio = Portfolio(initial_capital=100000)
    context = Context(portfolio=portfolio)

    # 创建API
    _global_api = PtradeAPI(
        data_context=_global_data_server,
        context=context,
        log=log
    )

    print(f"✓ API初始化完成")
    keys_list = list(_global_data_server.benchmark_data.keys()) # type: ignore
    print(f"✓ 可用基准(共 {len(keys_list)} 个): {', '.join(keys_list[:10])} ...")

    return _global_api

def get_api():
    """获取已初始化的API实例"""
    global _global_api
    if _global_api is None:
        print("API未初始化，自动初始化中...")
        return init_api()
    return _global_api

def get_Ashares(date=None):
    return get_api().get_Ashares(date)

def get_etf_list(date=None):
    """获取ETF列表"""
    # 从A股列表中筛选ETF
    all_stocks = get_api().get_Ashares(date)
    # 简单筛选ETF：通常ETF代码以51、50、15、16、18开头
    etfs = [stock for stock in all_stocks if stock.startswith(('51', '50', '15', '16', '18'))]
    return etfs

def get_stock_name(stocks):
    """获取股票名称"""
    return get_api().get_stock_name(stocks)

def get_price(stock, start_date=None, end_date=None, frequency='1d', fields=None, fq='pre', count=None):
    """获取价格数据"""
    return get_api().get_price(stock, start_date, end_date, frequency, fields, fq, count)

def get_index_stocks(index_code, date=None):
    """获取指数成分股"""
    return get_api().get_index_stocks(index_code, date)

def calculate_grid_score(prices, grid_pct=0.03):
    """
    模拟网格交易，计算网格收益率
    
    假设：
    1. 初始全仓买入（为了简化计算，或者假设持有底仓）
    2. 价格每波动 grid_pct，进行一次反向操作
    3. 忽略手续费，仅计算理论网格捕捉的波动收益
    
    更精确的指标：
    - 波动率 (Volatility)
    - 均值回归特性 (ADF Test / Hurst Exponent) - 计算复杂，这里用网格模拟代替
    - 穿越网格次数 (Grid Crossings)
    """
    if len(prices) < 2:
        return 0, 0
    
    # 简单的网格模拟
    initial_price = prices[0]
    last_grid_price = initial_price
    
    # 记录网格收益
    grid_profit = 0.0
    
    # 假设持有1手
    position = 100 
    cash = 0
    
    # 记录穿越次数
    crossings = 0
    
    for price in prices[1:]:
        change_pct = (price - last_grid_price) / last_grid_price
        
        if change_pct >= grid_pct:
            # 涨了，卖出
            # 假设卖出部分仓位，这里简化为记录一次套利收益
            # 收益 = 卖出价 - 上次网格价
            profit = (price - last_grid_price) 
            grid_profit += profit
            last_grid_price = price
            crossings += 1
            
        elif change_pct <= -grid_pct:
            # 跌了，买入
            # 假设买入，等待反弹
            # 这里不实际扣钱，只更新基准价格
            last_grid_price = price
            crossings += 1
            
    # 计算总收益率 (网格收益 / 初始价格)
    # 注意：这只是捕捉到的波动收益，不包含持仓本身的涨跌盈亏
    # 如果要找"震荡向上"的，可以加上 (prices[-1] - prices[0])
    # 如果纯粹找"震荡"的，只看 grid_profit
    
    # 归一化收益率
    yield_rate = grid_profit / initial_price
    
    return yield_rate, crossings

def calculate_volatility(prices):
    """计算年化波动率"""
    if len(prices) < 2:
        return 0
    returns = pd.Series(prices).pct_change().dropna()
    return returns.std() * np.sqrt(252)

def run_mining(index_code='000300.SS', start_date='2024-01-01', end_date='2024-12-31', top_n=20):
    print(f"开始挖掘 {index_code} 在 {start_date} 至 {end_date} 期间的网格交易标的...")
    
    # 初始化API
    init_api(required_data={'price'})
    
    # # 获取成分股
    # stocks = get_index_stocks(index_code, end_date)
    # print(f"获取到 {len(stocks)} 只股票 (指数: {index_code})")
    
    # if len(stocks) == 0:
    #     print("尝试获取所有A股...")
    #     stocks = get_Ashares(end_date)
    #     print(f"获取到 {len(stocks)} 只A股")
    #     # 调试模式：只取前100只测试 (如需全量跑请注释掉下行)
    #     # stocks = stocks[:100]
    
    # 获取全市场标的 (A股 + ETF)
    stocks = get_Ashares(end_date)
    etfs = get_etf_list(end_date)
    
    # 合并并去重
    all_targets = sorted(list(set(stocks + etfs)))
    print(f"获取到 {len(all_targets)} 只标的 (其中 ETF: {len(etfs)})")
    stocks = all_targets
        
    if len(stocks) == 0:
        print("未获取到任何股票，请检查数据源。")
        return
    
    results = []
    
    print(f"开始遍历 {len(stocks)} 只股票...", flush=True)
    for i, stock in enumerate(stocks):
        try:
            # 获取价格数据
            df = get_price(stock, start_date=start_date, end_date=end_date, frequency='1d', fields=['close'], fq='pre')
            
            if df is None or df.empty:
                continue
            
            if len(df) < 50:
                continue
                
            closes = df['close'].values
            
            # 计算指标
            volatility = calculate_volatility(closes)
            grid_yield, crossings = calculate_grid_score(closes, grid_pct=0.03)
            
            # 价格变动幅度
            price_change = (closes[-1] - closes[0]) / closes[0]
            
            # 评分公式优化：
            # 原始逻辑：score = grid_yield (倾向于单边上涨)
            # 新逻辑：score = grid_yield / (abs(price_change) + 0.1)
            # 含义：单位趋势下的网格收益率。
            # 如果股价翻倍(price_change=1.0)，分母为1.1，grid_yield需要很高才能得分高
            # 如果股价震荡回归(price_change=0.0)，分母为0.1，grid_yield被放大10倍
            # 加上 0.1 是为了避免分母过小，同时不过分惩罚小幅涨跌
            score = grid_yield / (abs(price_change) + 0.1)
            
            results.append({
                'code': stock,
                'volatility': volatility,
                'grid_yield': grid_yield,
                'crossings': crossings,
                'price_change': price_change,
                'score': score
            })
            
            if i % 100 == 0:
                print(f"已处理 {i}/{len(stocks)}...", flush=True)
                
        except Exception as e:
            # 忽略个别错误
            pass
            
    print(f"遍历完成，收集到 {len(results)} 个结果", flush=True)
    # 转为DataFrame
    res_df = pd.DataFrame(results)
    
    if res_df.empty:
        print("未收集到任何结果")
        return pd.DataFrame()

    # 排序
    res_df = res_df.sort_values(by='score', ascending=False)
    
    # 保存结果到CSV
    output_file = f'grid_mining_results_{start_date}_{end_date}.csv'
    res_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"结果已保存到 {output_file}")
    
    # 获取股票名称
    top_stocks = res_df.head(top_n)['code'].tolist()
    names = get_stock_name(top_stocks)
    names = get_stock_name(top_stocks)
    
    print("\n====== 挖掘结果 (Top {}) ======".format(top_n))
    print(f"{'代码':<10} {'名称':<10} {'评分':<10} {'网格收益':<10} {'穿越次数':<10} {'波动率':<10} {'区间涨跌':<10}")
    
    for idx, row in res_df.head(top_n).iterrows():
        code = row['code']
        name = names.get(code, 'Unknown')
        print(f"{code:<10} {name:<10} {row['score']:.2f}       {row['grid_yield']:.2%}     {row['crossings']:<10} {row['volatility']:.2%}     {row['price_change']:.2%}")
        
    return res_df

if __name__ == '__main__':
    # 示例：挖掘沪深300
    run_mining(index_code='000300.SS', start_date='2025-01-01', end_date='2025-12-31')
