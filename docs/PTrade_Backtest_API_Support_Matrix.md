# PTrade 本地回测 API 支持矩阵（2026-04-13）

## 适用范围
- 当前仓库的本地回测引擎（`simtradelab/ptrade/api.py`）
- 业务边界：**股票回测为主**
- 说明：官方“含回测场景”API共73个；其中 `log` 为注入对象而非 `PtradeAPI` 成员函数

## 总览
- 官方含回测场景API：`73`
- 已支持（可调用）：`62`
- 显式不支持（调用即抛错）：`10`
- 特殊项（非成员函数）：`1`（`log`）

## 显式不支持清单（避免误用）
以下接口在本地回测中会抛 `NotImplementedError`：
- `margin_trade`（两融）
- `buy_open`（期货）
- `sell_close`（期货）
- `sell_open`（期货）
- `buy_close`（期货）
- `set_future_commission`（期货）
- `set_margin_rate`（期货）
- `get_margin_rate`（期货）
- `get_instruments`（期货）
- `get_dominant_contract`（期货）

补充说明（不计入“官方含回测场景API=73”的统计口径）：
- `get_individual_transaction`（交易逐笔）
- `get_margin_assert`（两融信用资产查询）

## 已支持（股票回测主链）
以下是当前可用且已实现的核心回测接口：
- 设置与调度：`set_universe`、`set_benchmark`、`set_commission`、`set_slippage`、`set_fixed_slippage`、`set_volume_ratio`、`set_limit_mode`、`set_yesterday_position`、`run_daily`
- 行情与数据：`get_history`、`get_price`、`get_market_list`、`get_market_detail`、`get_stock_info`、`get_stock_name`、`get_stock_status`、`get_stock_exrights`、`get_stock_blocks`、`get_index_stocks`、`get_industry_stocks`、`get_fundamentals`、`get_Ashares`、`get_reits_list`、`get_trend_data`
- 交易日/辅助：`get_trading_day`、`get_trade_days`、`get_all_trades_days`、`get_trading_day_by_date`、`check_limit`、`filter_stock_by_status`、`get_current_kline_count`、`get_frequency`、`get_business_type`、`is_trade`
- 交易与订单：`order`、`order_target`、`order_value`、`order_target_value`、`cancel_order`、`get_open_orders`、`get_order`、`get_orders`、`get_trades`、`get_position`、`get_positions`
- 其它：`convert_position_from_csv`、`get_user_name`、`get_research_path`、`get_trades_file`、`create_dir`
- 技术指标：`get_MACD`、`get_KDJ`、`get_RSI`、`get_CCI`

## 待确认接口（不纳入当前可见文档基线）
以下接口在历史抓取源码中出现过，但在你当前官方页面可见内容中不可检索，暂不纳入“已支持主清单”：
- `option_buy_open`、`option_sell_close`、`option_sell_open`、`option_buy_close`
- `get_opt_objects`、`get_opt_last_dates`、`get_opt_contracts`、`get_contract_info`

## 特殊项说明
- `log`：由策略执行器注入到策略命名空间，不是 `PtradeAPI` 成员函数，故不计入“成员函数实现数”。

## 代码依据
- 回测API实现：[api.py](simtradelab/ptrade/api.py)
- 生命周期限制配置：[lifecycle_config.py](simtradelab/ptrade/lifecycle_config.py)
