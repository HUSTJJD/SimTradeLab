# PTrade API 完整接口总结

## 相比当前总结文档，新增的回测可用 API（9个）：
  - `get_trading_day_by_date`
  - `get_trend_data`
  - `get_reits_list`
  - `get_dominant_contract`
  - `log`
  - `get_frequency`
  - `get_business_type`
  - `get_current_kline_count`
  - `filter_stock_by_status`
- 说明：`option_*` 与 `get_opt_*` 相关接口目前在你当前官方页面可见内容中不可检索，已从“当前基线清单”移至“待确认接口”。

## 策略生命周期函数使用限制说明 📋

本文档为每个API函数标注了它们只能在特定的策略生命周期函数中使用，标注格式为 `{函数名}`：

### 策略生命周期函数说明
- **`{initialize}`** - 只能在 `initialize(context)` 中调用
- **`{handle_data}`** - 只能在 `handle_data(context, data)` 中调用
- **`{before_trading_start}`** - 只能在 `before_trading_start(context, data)` 中调用
- **`{after_trading_end}`** - 只能在 `after_trading_end(context, data)` 中调用
- **`{tick_data}`** - 只能在 `tick_data(context, data)` 中调用
- **`{on_order_response}`** - 只能在 `on_order_response(context, order)` 中调用
- **`{on_trade_response}`** - 只能在 `on_trade_response(context, trade)` 中调用
- **`{all}`** - 可以在所有策略生命周期函数中调用
- **`{函数A|函数B}`** - 可以在函数A或函数B中调用

### 使用限制原因
- **初始化专用**：配置类函数只能在策略启动时设置
- **交易时段专用**：交易类函数只能在交易时段调用
- **事件响应专用**：回调函数只能在对应事件触发时调用
- **数据获取通用**：查询类函数通常可以在任何时候调用

## 策略生命周期函数 (7个)

### 核心生命周期函数
- **initialize(context)** - 策略初始化（必选）*[回测/交易]*
- **handle_data(context, data)** - 主策略逻辑（必选）*[回测/交易]*
- **before_trading_start(context, data)** - 盘前处理（可选）*[回测/交易]*
- **after_trading_end(context, data)** - 盘后处理（可选）*[回测/交易]*
- **tick_data(context, data)** - tick级别处理（可选）*[仅交易]*

### 事件回调函数
- **on_order_response(context, order)** - 委托回报（可选）*[仅交易]*
- **on_trade_response(context, trade)** - 成交回报（可选）*[仅交易]*

## 设置函数 (12个)

### 基础设置
- **set_universe(securities)** - 设置股票池 *[回测/交易]* `{initialize}`
- **set_benchmark(benchmark)** - 设置基准 *[回测/交易]* `{initialize}`
- **set_commission(commission)** - 设置佣金费率 *[仅回测]* `{initialize}`
- **set_fixed_slippage(slippage)** - 设置固定滑点 *[仅回测]* `{initialize}`
- **set_slippage(slippage)** - 设置滑点 *[仅回测]* `{initialize}`
- **set_volume_ratio(ratio)** - 设置成交比例 *[仅回测]* `{initialize}`
- **set_limit_mode(mode)** - 设置回测成交数量限制模式 *[仅回测]* `{initialize}`
- **set_yesterday_position(positions)** - 设置底仓 *[仅回测]* `{initialize}`
- **set_parameters(params)** - 设置策略配置参数 *[仅交易]* `{initialize}`

### 定时函数
- **run_daily(func, time)** - 按日周期处理 *[回测/交易]* `{initialize}`
- **run_interval(func, interval)** - 按设定周期处理 *[仅交易]* `{initialize}`

### 期货设置
- **set_future_commission(commission)** - 设置期货手续费 *[仅回测]* `{initialize}`
- **set_margin_rate(security, rate)** - 设置期货保证金比例 *[仅回测]* `{initialize}`

## 获取信息函数 (50+个)

### 基础信息 (3个)
- **get_trading_day(date, offset=0)** - 获取交易日期 *[研究/回测/交易]* `{all}`
- **get_all_trades_days()** - 获取全部交易日期 *[研究/回测/交易]* `{all}`
- **get_trade_days(start_date, end_date)** - 获取指定范围交易日期 *[研究/回测/交易]* `{all}`

### 市场信息 (3个)
- **get_market_list()** - 获取市场列表 *[研究/回测/交易]* `{all}`
- **get_market_detail(market)** - 获取市场详细信息 *[研究/回测/交易]* `{all}`
- **get_cb_list()** - 获取可转债市场代码表 *[仅交易]* `{all}`

### 行情信息 (10个)
- **get_history(count, frequency, field, security_list, fq, include, fill, is_dict, start_date, end_date)** - 获取历史行情 *[研究/回测/交易]* `{all}`
- **get_price(security, start_date, end_date, frequency, fields, count)** - 获取历史数据 *[研究/回测/交易]* `{all}`
- **get_individual_entrust(security_list)** - 获取逐笔委托行情 *[仅交易]* `{tick_data}`
- **get_individual_transaction(security_list)** - 获取逐笔成交行情 *[仅交易]* `{tick_data}`
- **get_tick_direction(security_list)** - 获取分时成交行情 *[仅交易]* `{tick_data}`
- **get_sort_msg(market, category, sort_type)** - 获取板块、行业的涨幅排名 *[仅交易]* `{handle_data|before_trading_start|after_trading_end}`
- **get_etf_info(etf_code)** - 获取ETF信息 *[仅交易]* `{all}`
- **get_etf_stock_info(etf_code)** - 获取ETF成分券信息 *[仅交易]* `{all}`
- **get_gear_price(security_list)** - 获取指定代码的档位行情价格 *[仅交易]* `{handle_data|tick_data}`
- **get_snapshot(security_list)** - 获取行情快照 *[仅交易]* `{handle_data|tick_data}`
- **get_cb_info(cb_code)** - 获取可转债基础信息 *[仅交易]* `{all}`

### 股票信息 (12个)
- **get_stock_name(security_list)** - 获取股票名称 *[研究/回测/交易]* `{all}`
- **get_stock_info(security_list)** - 获取股票基础信息 *[研究/回测/交易]* `{all}`
- **get_stock_status(security_list)** - 获取股票状态信息 *[研究/回测/交易]* `{all}`
- **get_stock_exrights(security_list)** - 获取股票除权除息信息 *[研究/回测/交易]* `{all}`
- **get_stock_blocks(security_list)** - 获取股票所属板块信息 *[研究/回测/交易]* `{all}`
- **get_index_stocks(index_code)** - 获取指数成份股 *[研究/回测/交易]* `{all}`
- **get_etf_stock_list(etf_code)** - 获取ETF成分券列表 *[仅交易]* `{all}`
- **get_industry_stocks(industry_code)** - 获取行业成份股 *[研究/回测/交易]* `{all}`
- **get_fundamentals(stocks, table, fields, date)** - 获取财务数据信息 *[研究/回测/交易]* `{all}`
- **get_Ashares(date)** - 获取指定日期A股代码列表 *[研究/回测/交易]* `{all}`
- **get_etf_list()** - 获取ETF代码 *[仅交易]* `{all}`
- **get_ipo_stocks()** - 获取当日IPO申购标的 *[仅交易]* `{before_trading_start|handle_data}`

### 其他信息 (8个)
- **get_trades_file()** - 获取对账数据文件 *[仅回测]* `{after_trading_end}`
- **convert_position_from_csv(file_path)** - 获取设置底仓的参数列表 *[仅回测]* `{initialize}`
- **get_user_name()** - 获取登录终端的资金账号 *[回测/交易]* `{all}`
- **get_deliver(start_date, end_date)** - 获取历史交割单信息 *[仅交易]* `{after_trading_end}`
- **get_fundjour(start_date, end_date)** - 获取历史资金流水信息 *[仅交易]* `{after_trading_end}`
- **get_research_path()** - 获取研究路径 *[回测/交易]* `{initialize}`
- **get_trade_name()** - 获取交易名称 *[仅交易]* `{all}`

## 交易相关函数 (30+个)

### 股票交易函数 (11个)
- **order(security, amount, limit_price=None)** - 按数量买卖 *[回测/交易]* `{handle_data|tick_data}`
- **order_target(security, target_amount)** - 指定目标数量买卖 *[回测/交易]* `{handle_data|tick_data}`
- **order_value(security, value)** - 指定目标价值买卖 *[回测/交易]* `{handle_data|tick_data}`
- **order_target_value(security, target_value)** - 指定持仓市值买卖 *[回测/交易]* `{handle_data|tick_data}`
- **order_market(security, amount)** - 按市价进行委托 *[仅交易]* `{handle_data|tick_data}`
- **ipo_stocks_order(amount_per_stock=10000)** - 新股一键申购 *[仅交易]* `{before_trading_start}`
- **after_trading_order(security, amount, limit_price)** - 盘后固定价委托 *[仅交易]* `{after_trading_end}`
- **after_trading_cancel_order(order_id)** - 盘后固定价委托撤单 *[仅交易]* `{after_trading_end}`
- **etf_basket_order(etf_code, amount, side)** - ETF成分券篮子下单 *[仅交易]* `{handle_data|tick_data}`
- **etf_purchase_redemption(etf_code, amount, side)** - ETF基金申赎接口 *[仅交易]* `{handle_data|tick_data}`
- **get_positions(security_list)** - 获取多支股票持仓信息 *[回测/交易]* `{all}`

### 公共交易函数 (11个)
- **order_tick(security, amount, limit_price, tick_type)** - tick行情触发买卖 *[仅交易]* `{tick_data}`
- **cancel_order(order_id)** - 撤单 *[回测/交易]* `{handle_data|tick_data|on_order_response}`
- **cancel_order_ex(order_id)** - 撤单扩展 *[仅交易]* `{handle_data|tick_data|on_order_response}`
- **debt_to_stock_order(cb_code, amount)** - 债转股委托 *[仅交易]* `{handle_data|tick_data}`
- **get_open_orders(security=None)** - 获取未完成订单 *[回测/交易]* `{all}`
- **get_order(order_id)** - 获取指定订单 *[回测/交易]* `{all}`
- **get_orders(security=None)** - 获取全部订单 *[回测/交易]* `{all}`
- **get_all_orders()** - 获取账户当日全部订单 *[仅交易]* `{all}`
- **get_trades(security=None)** - 获取当日成交订单 *[回测/交易]* `{all}`
- **get_position(security)** - 获取持仓信息 *[回测/交易]* `{all}`

## 融资融券专用函数 (19个)

### 融资融券交易类函数 (7个)
- **margin_trade(security, amount, limit_price=None)** - 担保品买卖 *[两融回测/两融交易]* `{handle_data|tick_data}`
- **margincash_open(security, amount, limit_price=None)** - 融资买入 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **margincash_close(security, amount, limit_price=None)** - 卖券还款 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **margincash_direct_refund(amount)** - 直接还款 *[仅交易（两融账户）]* `{handle_data|after_trading_end}`
- **marginsec_open(security, amount, limit_price=None)** - 融券卖出 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **marginsec_close(security, amount, limit_price=None)** - 买券还券 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **marginsec_direct_refund(security, amount)** - 直接还券 *[仅交易（两融账户）]* `{handle_data|after_trading_end}`

### 融资融券查询类函数 (12个)
- **get_margincash_stocks()** - 获取融资标的列表 *[仅交易（两融账户）]* `{all}`
- **get_marginsec_stocks()** - 获取融券标的列表 *[仅交易（两融账户）]* `{all}`
- **get_margin_contract()** - 合约查询 *[仅交易（两融账户）]* `{all}`
- **get_margin_contractreal()** - 实时合约查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_margin_assert()** - 信用资产查询 *[仅交易（两融账户）]* `{all}`
- **get_assure_security_list()** - 担保券查询 *[仅交易（两融账户）]* `{all}`
- **get_margincash_open_amount(security)** - 融资标的最大可买数量查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_margincash_close_amount(security)** - 卖券还款标的最大可卖数量查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_marginsec_open_amount(security)** - 融券标的最大可卖数量查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_marginsec_close_amount(security)** - 买券还券标的最大可买数量查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_margin_entrans_amount(security)** - 现券还券数量查询 *[仅交易（两融账户）]* `{handle_data|tick_data}`
- **get_enslo_security_info(security)** - 融券头寸信息查询 *[仅交易（两融账户）]* `{all}`

## 期货专用函数 (7个)

### 期货交易类函数 (4个)
- **buy_open(security, amount, limit_price=None)** - 开多 *[回测/交易]* `{handle_data|tick_data}`
- **sell_close(security, amount, limit_price=None)** - 多平 *[回测/交易]* `{handle_data|tick_data}`
- **sell_open(security, amount, limit_price=None)** - 空开 *[回测/交易]* `{handle_data|tick_data}`
- **buy_close(security, amount, limit_price=None)** - 空平 *[回测/交易]* `{handle_data|tick_data}`

### 期货查询类函数 (2个)
- **get_margin_rate(security)** - 获取用户设置的保证金比例 *[仅回测]* `{all}`
- **get_instruments()** - 获取合约信息 *[回测/交易]* `{all}`

### 期货设置类函数 (1个)
- **set_future_commission(commission)** - 设置期货手续费 *[仅回测]* `{initialize}`

## 期权专用函数（待确认，当前页面不可见） (15个)

### 期权查询类函数 (6个)
- **get_opt_objects()** - 获取期权标的列表 *[研究/回测/交易]* `{all}`
- **get_opt_last_dates(underlying)** - 获取期权标的到期日列表 *[研究/回测/交易]* `{all}`
- **get_opt_contracts(underlying, last_date)** - 获取期权标的对应合约列表 *[研究/回测/交易]* `{all}`
- **get_contract_info(contract)** - 获取期权合约信息 *[研究/回测/交易]* `{all}`
- **get_covered_lock_amount(underlying)** - 获取期权标的可备兑锁定数量 *[仅交易]* `{handle_data|tick_data}`
- **get_covered_unlock_amount(underlying)** - 获取期权标的允许备兑解锁数量 *[仅交易]* `{handle_data|tick_data}`

### 期权交易类函数 (7个)
- **option_buy_open(security, amount, limit_price=None)** - 权利仓开仓 *[仅交易]* `{handle_data|tick_data}`
- **option_sell_close(security, amount, limit_price=None)** - 权利仓平仓 *[仅交易]* `{handle_data|tick_data}`
- **option_sell_open(security, amount, limit_price=None)** - 义务仓开仓 *[仅交易]* `{handle_data|tick_data}`
- **option_buy_close(security, amount, limit_price=None)** - 义务仓平仓 *[仅交易]* `{handle_data|tick_data}`
- **open_prepared(security, amount, limit_price=None)** - 备兑开仓 *[仅交易]* `{handle_data|tick_data}`
- **close_prepared(security, amount, limit_price=None)** - 备兑平仓 *[仅交易]* `{handle_data|tick_data}`
- **option_exercise(security, amount)** - 行权 *[仅交易]* `{handle_data|after_trading_end}`

### 期权其他函数 (2个)
- **option_covered_lock(security, amount)** - 期权标的备兑锁定 *[仅交易]* `{handle_data|tick_data}`
- **option_covered_unlock(security, amount)** - 期权标的备兑解锁 *[仅交易]* `{handle_data|tick_data}`

## 计算函数 (4个)

### 技术指标计算函数
- **get_MACD(close, short=12, long=26, m=9)** - 异同移动平均线 *[回测/交易]* `{all}`
- **get_KDJ(high, low, close, n=9, m1=3, m2=3)** - 随机指标 *[回测/交易]* `{all}`
- **get_RSI(close, n=6)** - 相对强弱指标 *[回测/交易]* `{all}`
- **get_CCI(high, low, close, n=14)** - 顺势指标 *[回测/交易]* `{all}`

## 其他函数 (7个)

### 工具函数
- **log** - 日志记录 (支持 debug, info, warning, error, critical 级别) *[回测/交易]* `{all}`
- **is_trade()** - 业务代码场景判断 *[回测/交易]* `{all}`
- **check_limit(security, query_date=None)** - 代码涨跌停状态判断 *[研究/回测/交易]* `{all}`
- **send_email(send_email_info, get_email_info, smtp_code, info, path, subject)** - 发送邮箱信息 *[仅交易]* `{after_trading_end|on_order_response|on_trade_response}`
- **send_qywx(corp_id, secret, agent_id, info, path, toparty, touser, totag)** - 发送企业微信信息 *[仅交易]* `{after_trading_end|on_order_response|on_trade_response}`
- **permission_test(account=None, end_date=None)** - 权限校验 *[仅交易]* `{initialize}`
- **create_dir(user_path=None)** - 创建文件路径 *[研究/回测/交易]* `{initialize}`

## 对象定义 (11个核心对象)

### 1. g - 全局对象
**功能**: 全局变量容器，用于存储用户的各类可被不同函数调用的全局数据
**使用场景**: 回测/交易
**属性**: 用户自定义属性，常见用法：
```python
g.security = "600570.SS"  # 股票池
g.count = 1               # 计数器
g.flag = 0               # 标志位
```

### 2. Context - 上下文对象
**功能**: 业务上下文对象，包含策略运行的完整环境信息
**使用场景**: 回测/交易
**主要属性**:
- `capital_base` - 起始资金
- `previous_date` - 前一个交易日
- `sim_params` - SimulationParameters对象
- `portfolio` - 账户信息（Portfolio对象）
- `initialized` - 是否执行初始化
- `slippage` - 滑点（VolumeShareSlippage对象）
- `commission` - 佣金费用（Commission对象）
- `blotter` - Blotter对象（记录）
- `recorded_vars` - 收益曲线值

### 3. SecurityUnitData对象
**功能**: 一个单位时间内的股票数据，是一个字典，根据sid获取BarData对象数据
**使用场景**: 回测/交易
**基本属性**:
- `dt` - 时间
- `open` - 时间段开始时价格
- `close` - 时间段结束时价格
- `price` - 结束时价格
- `low` - 最低价
- `high` - 最高价
- `volume` - 成交的股票数量
- `money` - 成交的金额

### 4. Portfolio对象
**功能**: 账户当前的资金、标的信息，即所有标的操作仓位的信息汇总
**使用场景**: 回测/交易

#### 股票账户属性 (8个):
- `cash` - 当前可用资金（不包含冻结资金）
- `positions` - 当前持有的标的（包含不可卖出的标的），dict类型，key是标的代码，value是Position对象
- `portfolio_value` - 当前持有的标的和现金的总价值
- `positions_value` - 持仓价值
- `capital_used` - 已使用的现金
- `returns` - 当前的收益比例，相对于初始资金
- `pnl` - 当前账户总资产-初始账户总资产
- `start_date` - 开始时间

#### 期货账户属性 (8个):
- `cash` - 当前可用资金（不包含冻结资金）
- `positions` - 当前持有的标的（包含不可卖出的标的），dict类型，key是标的代码，value是Position对象
- `portfolio_value` - 当前持有的标的和现金的总价值
- `positions_value` - 持仓价值
- `capital_used` - 已使用的现金
- `returns` - 当前的收益比例，相对于初始资金
- `pnl` - 当前账户总资产-初始账户总资产
- `start_date` - 开始时间

#### 期权账户属性 (9个):
- `cash` - 当前可用资金（不包含冻结资金）
- `positions` - 当前持有的标的（包含不可卖出的标的），dict类型，key是标的代码，value是Position对象
- `portfolio_value` - 当前持有的标的和现金的总价值
- `positions_value` - 持仓价值
- `returns` - 当前的收益比例，相对于初始资金
- `pnl` - 当前账户总资产-初始账户总资产
- `margin` - 保证金
- `risk_degree` - 风险度
- `start_date` - 开始时间

### 5. Position对象
**功能**: 持有的某个标的的信息
**使用场景**: 回测/交易
**注意**: 期货业务持仓分为多头仓(long)、空头仓(short)；期权业务持仓分为权利仓(long)、义务仓(short)、备兑仓(covered)

#### 股票账户属性 (7个):
- `sid` - 标的代码
- `enable_amount` - 可用数量
- `amount` - 总持仓数量
- `last_sale_price` - 最新价格
- `cost_basis` - 持仓成本价格
- `today_amount` - 今日开仓数量（且仅回测有效）
- `business_type` - 持仓类型

#### 期货账户属性 (18个):
- `sid` - 标的代码
- `short_enable_amount` - 空头仓可用数量
- `long_enable_amount` - 多头仓可用数量
- `today_short_amount` - 空头仓今仓数量
- `today_long_amount` - 多头仓今仓数量
- `long_cost_basis` - 多头仓持仓成本
- `short_cost_basis` - 空头仓持仓成本
- `long_amount` - 多头仓总持仓量
- `short_amount` - 空头仓总持仓量
- `long_pnl` - 多头仓浮动盈亏
- `short_pnl` - 空头仓浮动盈亏
- `amount` - 总持仓数量
- `enable_amount` - 可用数量
- `last_sale_price` - 最新价格
- `delivery_date` - 交割日
- `margin_rate` - 保证金比例
- `contract_multiplier` - 合约乘数
- `business_type` - 持仓类型

#### 期权账户属性 (17个):
- `sid` - 标的代码
- `short_enable_amount` - 义务仓可用数量
- `long_enable_amount` - 权利仓可用数量
- `covered_enable_amount` - 备兑仓可用数量
- `short_cost_basis` - 义务仓持仓成本
- `long_cost_basis` - 权利仓持仓成本
- `covered_cost_basis` - 备兑仓持仓成本
- `short_amount` - 义务仓总持仓量
- `long_amount` - 权利仓总持仓量
- `covered_amount` - 备兑仓总持仓量
- `short_pnl` - 义务仓浮动盈亏
- `long_pnl` - 权利仓浮动盈亏
- `covered_pnl` - 备兑仓浮动盈亏
- `last_sale_price` - 最新价格
- `margin` - 保证金
- `exercise_date` - 行权日
- `business_type` - 持仓类型

### 6. Order对象
**功能**: 买卖订单信息
**使用场景**: 回测/交易
**主要属性**:
- `id` - 订单号
- `dt` - 订单产生时间（datetime.datetime类型）
- `limit` - 指定价格
- `symbol` - 标的代码（注意：标的代码尾缀为四位，上证为XSHG，深圳为XSHE）
- `amount` - 下单数量，买入是正数，卖出是负数

### 7. SimulationParameters对象
**功能**: 模拟参数配置
**主要属性**:
- `capital_base` - 起始资金
- `data_frequency` - 数据频率

### 8. VolumeShareSlippage对象
**功能**: 滑点配置
**主要属性**:
- `volume_limit` - 成交限量
- `price_impact` - 价格影响力

### 9. Commission对象
**功能**: 佣金费用配置
**主要属性**:
- `tax` - 印花税费率
- `cost` - 佣金费率
- `min_trade_cost` - 最小佣金

### 10. Blotter对象
**功能**: 订单记录管理
**主要属性**:
- `current_dt` - 当前单位时间的开始时间（datetime.datetime对象，北京时间）

### 11. FutureParams对象
**功能**: 期货参数配置
**主要属性**:
- `margin_rate` - 保证金比例
- `contract_multiplier` - 合约乘数

## API 总数统计

| 分类 | 数量 | 说明 |
|------|------|------|
| 策略生命周期函数 | 7 | 核心框架函数 |
| 设置函数 | 12 | 策略配置和参数设置 |
| 获取信息函数 | 50+ | 数据获取和查询 |
| 交易相关函数 | 30+ | 交易执行和管理 |
| 融资融券函数 | 19 | 两融业务专用 |
| 期货专用函数 | 7 | 期货交易专用 |
| 期权专用函数 | 15 | 期权交易专用 |
| 计算函数 | 4 | 技术指标计算 |
| 其他函数 | 7 | 工具和辅助函数 |
| **总计** | **150+** | **完整API接口** |

## 调用场景分类

| 场景 | 描述 | 支持的API类型 |
|------|------|---------------|
| **研究** | 数据研究和分析环境 | 基础信息获取、股票信息获取、财务数据、期权查询、工具函数 |
| **回测** | 历史数据回测环境 | 除实时交易外的所有API：生命周期函数、设置函数、获取信息函数、交易相关函数、计算函数 |
| **交易** | 实盘交易环境 | 所有API，包括实时数据获取、委托交易、主推回调等 |
| **两融交易** | 融资融券交易环境 | 基础交易API + 融资融券专用函数 |

## 使用场景详细说明

### 研究模式 (Research)
- **支持场景**: 数据分析、策略研究、历史回测数据获取
- **可用API**: 信息获取类函数、计算函数、工具函数
- **限制**: 无法执行实际交易操作

### 回测模式 (Backtest)
- **支持场景**: 策略历史数据回测、性能评估
- **可用API**: 策略生命周期函数、设置函数、获取信息函数、交易相关函数、计算函数
- **限制**: 无法获取实时数据、无法执行实际交易

### 交易模式 (Trading)
- **支持场景**: 实盘交易、实时数据获取、委托下单
- **可用API**: 所有API函数
- **特有功能**: tick级别处理、实时行情获取、委托主推、成交主推

## 使用频率最高的核心API (Top 20)

1. **initialize(context)** - 策略初始化
2. **handle_data(context, data)** - 主策略逻辑
3. **set_universe(securities)** - 设置股票池
4. **get_history()** - 获取历史行情
5. **get_price()** - 获取历史数据
6. **order()** - 下单交易
7. **order_target()** - 目标数量交易
8. **order_value()** - 目标金额交易
9. **get_position()** - 获取持仓
10. **get_orders()** - 获取订单
11. **cancel_order()** - 撤销订单
12. **log.info()** - 日志记录
13. **get_snapshot()** - 获取行情快照
14. **get_stock_info()** - 获取股票信息
15. **get_fundamentals()** - 获取财务数据
16. **set_commission()** - 设置手续费
17. **set_benchmark()** - 设置基准
18. **before_trading_start()** - 盘前处理
19. **after_trading_end()** - 盘后处理
20. **get_MACD()** - 技术指标MACD

本总结涵盖了PTrade量化交易平台的完整API体系，为插件系统的PTrade兼容层提供了完整的参考规范。

---

## 按策略生命周期函数分类的API统计 📊

### initialize(context) 专用API (15个)
**功能**：策略初始化时的配置设置
| API类型 | 数量 | 主要功能 |
|---------|------|----------|
| 基础设置 | 9 | 股票池、基准、佣金、滑点等配置 |
| 定时函数 | 2 | 定时任务调度设置 |
| 期货设置 | 2 | 期货手续费、保证金设置 |
| 其他工具 | 2 | 权限校验、目录创建、底仓设置 |

**核心API**：`set_universe()`, `set_benchmark()`, `set_commission()`, `run_daily()`, `run_interval()`

### handle_data(context, data) 专用API (35个)
**功能**：主策略逻辑执行，包含大部分交易操作
| API类型 | 数量 | 主要功能 |
|---------|------|----------|
| 股票交易 | 6 | 基础买卖、目标交易 |
| 融资融券交易 | 6 | 两融买卖、还款还券 |
| 期货交易 | 4 | 期货开平仓 |
| 期权交易 | 9 | 期权开平仓、备兑操作 |
| 高级交易 | 3 | ETF申赎、债转股 |
| 交易管理 | 3 | 撤单、查询 |
| 实时行情 | 4 | 排名、档位价格、快照 |

**核心API**：`order()`, `order_target()`, `cancel_order()`, `get_snapshot()`

### before_trading_start(context, data) 专用API (2个)
**功能**：盘前准备工作
- **get_ipo_stocks()** - 获取当日IPO申购标的
- **ipo_stocks_order()** - 新股一键申购

### after_trading_end(context, data) 专用API (8个)
**功能**：盘后处理和数据整理
| API类型 | 数量 | 主要功能 |
|---------|------|----------|
| 盘后交易 | 2 | 盘后固定价委托和撤单 |
| 数据文件 | 3 | 对账文件、交割单、资金流水 |
| 通知推送 | 2 | 邮件、企业微信通知 |
| 期权操作 | 1 | 期权行权 |

**核心API**：`after_trading_order()`, `get_trades_file()`, `send_email()`

### tick_data(context, data) 专用API (18个)
**功能**：tick级别的实时数据处理和高频交易
| API类型 | 数量 | 主要功能 |
|---------|------|----------|
| 实时行情 | 5 | 逐笔委托、成交、分时数据 |
| 高频交易 | 13 | tick触发交易、所有交易类API |

**核心API**：`order_tick()`, `get_individual_entrust()`, `get_tick_direction()`

### on_order_response(context, order) 专用API (4个)
**功能**：委托回报事件处理
- **cancel_order()** - 撤单操作
- **cancel_order_ex()** - 撤单扩展
- **send_email()** - 邮件通知
- **send_qywx()** - 企业微信通知

### on_trade_response(context, trade) 专用API (2个)
**功能**：成交回报事件处理
- **send_email()** - 邮件通知
- **send_qywx()** - 企业微信通知

### 通用API (可在所有函数中调用) (95个)
**功能**：数据查询、信息获取、技术指标计算等
| API类型 | 数量 | 主要功能 |
|---------|------|----------|
| 基础信息 | 3 | 交易日期查询 |
| 市场信息 | 3 | 市场列表、详情 |
| 股票信息 | 11 | 股票基础信息、财务数据 |
| 行情数据 | 3 | 历史行情、可转债信息 |
| 交易查询 | 6 | 持仓、订单、成交查询 |
| 融资融券查询 | 8 | 两融标的、合约、资产查询 |
| 期货期权查询 | 6 | 合约信息、保证金查询 |
| 技术指标 | 4 | MACD、KDJ、RSI、CCI |
| 工具函数 | 3 | 日志、场景判断、涨跌停 |
| 其他信息 | 48 | 用户信息、路径等 |

### 策略生命周期函数使用频率统计 🎯

| 生命周期函数 | 专用API数 | 可用总API数 | 使用频率 | 主要用途 |
|-------------|-----------|-------------|----------|----------|
| **initialize** | 15 | 110 | ⭐⭐⭐ | 策略配置和初始化 |
| **handle_data** | 35 | 130 | ⭐⭐⭐⭐⭐ | 主策略逻辑和交易执行 |
| **before_trading_start** | 2 | 97 | ⭐⭐ | 盘前准备 |
| **after_trading_end** | 8 | 103 | ⭐⭐ | 盘后处理 |
| **tick_data** | 18 | 113 | ⭐⭐⭐⭐ | 高频交易和实时数据 |
| **on_order_response** | 4 | 99 | ⭐ | 委托事件处理 |
| **on_trade_response** | 2 | 97 | ⭐ | 成交事件处理 |

**关键发现**：
- **handle_data** 是最核心的函数，可使用130个API，承担主要的策略逻辑
- **tick_data** 专用于高频交易，有18个专用API用于实时数据处理
- **initialize** 负责策略配置，有15个专用的设置类API
- **事件回调函数** 主要用于异常处理和通知，API数量较少但很重要
