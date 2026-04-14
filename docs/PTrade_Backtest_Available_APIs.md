# PTrade 回测可用 API（更新于 2026-04-13）

## 汇总
- 回测相关函数总数：`65`（按当前页面可见且可检索口径）
- 其中“仅回测模块可用”：`11`
- 其中“回测/交易可用（含研究交集）”：`53`
- 其中“两融回测专用可用”：`1`

## 仅在回测模块可用（11）
- `convert_position_from_csv` - 获取设置底仓的参数列表(股票)
- `get_margin_rate` - 获取用户设置的保证金比例
- `get_trades_file` - 获取对账数据文件
- `set_commission` - 设置佣金费率
- `set_fixed_slippage` - 设置固定滑点
- `set_future_commission` - 设置期货手续费
- `set_limit_mode` - 设置成交数量限制模式
- `set_margin_rate` - 设置期货保证金比例
- `set_slippage` - 设置滑点
- `set_volume_ratio` - 设置成交比例
- `set_yesterday_position` - 设置底仓

## 回测/交易可用（28）
- `buy_close` - 空平
- `buy_open` - 多开
- `cancel_order` - 撤单
- `get_CCI` - 顺势指标
- `get_KDJ` - 随机指标
- `get_MACD` - 异同移动平均线
- `get_RSI` - 相对强弱指标
- `get_instruments` - 获取合约信息
- `get_open_orders` - 获取未完成订单
- `get_order` - 获取指定订单
- `get_orders` - 获取全部订单
- `get_position` - 获取单只标的持仓信息
- `get_positions` - 获取多只标的持仓信息
- `get_trades` - 获取当日成交订单
- `get_user_name` - 获取登录终端的资金账号
- `order` - 按数量买卖
- `order_target` - 指定目标数量买卖
- `order_target_value` - 指定持仓市值买卖
- `order_value` - 指定目标价值买卖
- `run_daily` - 按日周期处理
- `sell_close` - 多平
- `sell_open` - 空开
- `set_benchmark` - 设置基准
- `set_universe` - 设置股票池

## 回测/交易可用（其他表述）
- `is_trade` - 业务代码场景判断
- `log` - 日志记录
- `get_business_type` - 获取当前策略的业务类型
- `get_frequency` - 获取当前业务代码的周期
- `get_research_path` - 获取研究路径

## 研究/回测/交易可用（23）
- `create_dir` - 创建文件路径
- `get_Ashares` - 获取指定日期A股代码列表
- `get_all_trades_days` - 获取全部交易日期
- `get_dominant_contract` - 获取主力合约代码
- `get_index_stocks` - 获取指数成分股
- `get_industry_stocks` - 获取行业成份股
- `get_market_detail` - 获取市场详细信息
- `get_market_list` - 获取市场列表
- `get_price` - 获取历史数据
- `get_reits_list` - 获取基础设施公募REITs基金代码列表
- `get_stock_blocks` - 获取证券所属板块信息
- `get_stock_exrights` - 获取证券除权除息信息
- `get_stock_info` - 获取证券基础信息
- `get_stock_name` - 获取证券名称
- `get_stock_status` - 获取证券状态信息
- `get_trade_days` - 获取指定范围交易日期
- `get_trading_day` - 获取交易日期
- `get_trading_day_by_date` - 按日期获取指定交易日
- `get_trend_data` - 获取集中竞价期间代码数据

## 其他包含回测场景
- `get_fundamentals` - 获取财务数据（研究/回测/交易）
- `get_history` - 获取历史行情（回测/交易/研究）
- `filter_stock_by_status` - 过滤指定状态的股票代码（回测/交易/研究）
- `get_current_kline_count` - 获取股票业务当前时间的分钟bar数量（回测/交易/研究）
- `check_limit` - 代码涨跌停状态判断（研究/回测/交易）
- `margin_trade` - 担保品买卖（仅支持 PTrade 客户端；两融回测/两融交易）

## 待确认接口（当前页面不可见）
以下接口在历史抓取源码中出现过，但在你当前官方页面可见内容中不可检索，暂不纳入本清单统计：
- `option_buy_open`
- `option_sell_close`
- `option_sell_open`
- `option_buy_close`
- `get_opt_objects`
- `get_opt_last_dates`
- `get_opt_contracts`
- `get_contract_info`
