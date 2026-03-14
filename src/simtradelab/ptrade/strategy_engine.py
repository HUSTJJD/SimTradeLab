# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
PTrade 策略执行框架

提供完整的策略执行环境，整合生命周期控制、API验证和Context管理
"""


from __future__ import annotations

import builtins
import logging
import traceback
from typing import Any, Callable, Optional

from .context import Context

# 策略代码禁止导入的模块（与Ptrade平台一致）
_current_backtest_date: Optional[str] = None

_BLOCKED_MODULES = frozenset({
    'os', 'sys', 'io', 'subprocess', 'shutil', 'socket', 'http', 'urllib',
    'ctypes', 'signal', 'importlib', 'runpy', 'code', 'codeop',
})

_REAL_IMPORT = builtins.__import__


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    top = name.split('.')[0]
    if top in _BLOCKED_MODULES:
        raise ImportError(f"Module '{name}' is not allowed in strategy code")
    return _REAL_IMPORT(name, globals, locals, fromlist, level)


def _build_safe_builtins() -> dict:
    """构建受限 builtins，移除 exec/eval/compile/breakpoint"""
    unsafe = {'exec', 'eval', 'compile', 'breakpoint'}
    safe = {k: v for k, v in builtins.__dict__.items() if k not in unsafe}
    safe['__import__'] = _safe_import
    return safe


_SAFE_BUILTINS = _build_safe_builtins()


class StrategyExecutionError(Exception):
    """策略执行错误"""
    pass


class StrategyExecutionEngine:
    """PTrade策略执行引擎

    功能：
    1. 管理策略的完整生命周期
    2. 提供PTrade API接口
    3. 集成生命周期控制和API验证
    4. 支持多种运行模式（研究/回测/交易）
    """

    def __init__(
        self,
        context: Context,
        api: Any,
        stats_collector: Any,
        log: logging.Logger,
        frequency: str = '1d',
        sandbox: bool = True,
        cancel_event=None,
    ):
        """
        初始化策略执行引擎

        Args:
            context: PTrade Context对象
            api: PtradeAPI对象
            stats_collector: 统计收集器
            log: 日志对象
            frequency: 回测频率 '1d'日线 '1m'分钟线
            sandbox: 是否启用沙箱（限制import和builtins）
        """
        # 核心组件（外部注入）
        self.context = context
        self.api = api
        self.stats_collector = stats_collector
        self.log = log
        self.frequency = frequency
        self.sandbox = sandbox
        self._cancel_event = cancel_event

        # 获取生命周期控制器
        if self.context._lifecycle_controller is None:
            raise ValueError("Context lifecycle controller is not initialized")
        self.lifecycle_controller = self.context._lifecycle_controller

        # 策略相关
        self._strategy_functions: dict[str, Callable[..., Any]] = {}
        self._strategy_name: Optional[str] = None
        self._is_running = False
    # ==========================================
    # 策略注册接口
    # ==========================================

    def load_strategy_from_file(self, strategy_path: str) -> None:
        """从文件加载策略并自动注册所有生命周期函数

        Args:
            strategy_path: 策略文件路径
        """
        # 读取策略代码
        with open(strategy_path, 'r', encoding='utf-8') as f:
            strategy_code = f.read()

        # 构建命名空间
        strategy_namespace = {
            '__builtins__': _SAFE_BUILTINS if self.sandbox else builtins.__dict__.copy(),
            '__name__': '__main__',
            '__file__': strategy_path,
            'g': self.context.g,
            'log': self.log,
            'context': self.context,
        }

        # 注入API方法
        for attr_name in dir(self.api):
            if not attr_name.startswith('_'):
                attr = getattr(self.api, attr_name)
                if callable(attr) or attr_name == 'FUNDAMENTAL_TABLES':
                    strategy_namespace[attr_name] = attr

        # 执行策略代码
        exec(strategy_code, strategy_namespace)

        # 自动注册所有生命周期函数
        if 'initialize' in strategy_namespace:
            self.register_initialize(strategy_namespace['initialize'])
        if 'handle_data' in strategy_namespace:
            self.register_handle_data(strategy_namespace['handle_data'])
        if 'before_trading_start' in strategy_namespace:
            self.register_before_trading_start(strategy_namespace['before_trading_start'])
        if 'after_trading_end' in strategy_namespace:
            self.register_after_trading_end(strategy_namespace['after_trading_end'])
        if 'tick_data' in strategy_namespace:
            self.register_tick_data(strategy_namespace['tick_data'])
        if 'on_order_response' in strategy_namespace:
            self.register_on_order_response(strategy_namespace['on_order_response'])
        if 'on_trade_response' in strategy_namespace:
            self.register_on_trade_response(strategy_namespace['on_trade_response'])

    def set_strategy_name(self, strategy_name: str) -> None:
        """设置策略名称

        Args:
            strategy_name: 策略名称
        """
        self._strategy_name = strategy_name

    def register_initialize(self, func: Callable[[Context], None]) -> None:
        """注册initialize函数"""
        self._strategy_functions["initialize"] = func

    def register_handle_data(self, func: Callable[[Context, Any], None]) -> None:
        """注册handle_data函数"""
        self._strategy_functions["handle_data"] = func

    def register_before_trading_start(
        self, func: Callable[[Context, Any], None]
    ) -> None:
        """注册before_trading_start函数"""
        self._strategy_functions["before_trading_start"] = func

    def register_after_trading_end(
        self, func: Callable[[Context, Any], None]
    ) -> None:
        """注册after_trading_end函数"""
        self._strategy_functions["after_trading_end"] = func

    def register_tick_data(self, func: Callable[[Context, Any], None]) -> None:
        """注册tick_data函数"""
        self._strategy_functions["tick_data"] = func

    def register_on_order_response(
        self, func: Callable[[Context, Any], None]
    ) -> None:
        """注册on_order_response函数"""
        self._strategy_functions["on_order_response"] = func

    def register_on_trade_response(
        self, func: Callable[[Context, Any], None]
    ) -> None:
        """注册on_trade_response函数"""
        self._strategy_functions["on_trade_response"] = func

    # ==========================================
    # PTrade API 代理接口
    # ==========================================

    def __getattr__(self, name: str) -> Any:
        """代理PTrade API调用"""
        if hasattr(self.api, name):
            return getattr(self.api, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    # ==========================================
    # 策略执行接口
    # ==========================================

    def run_backtest(self, date_range) -> bool:
        """运行回测策略

        Args:
            date_range: 交易日序列

        Returns:
            bool: 是否成功完成
        """
        # 验证必选函数
        if not self._strategy_functions.get("initialize"):
            raise StrategyExecutionError("Strategy must have an initialize function")
        if not self._strategy_functions.get("handle_data"):
            raise StrategyExecutionError("Strategy must have a handle_data function")

        self._is_running = True

        try:
            self.log.info(f"Starting strategy execution: {self._strategy_name}")

            # 1. 执行初始化
            self._execute_initialize()

            # 2. 根据frequency选择循环模式
            if self.frequency == '1m':
                success = self._run_minute_loop(date_range)
            else:
                success = self._run_daily_loop(date_range)

            if success:
                self.log.info("Strategy execution completed successfully")

            return success

        except Exception as e:
            self.log.error(f"Strategy execution failed: {e}")
            traceback.print_exc()
            return False

        finally:
            self._is_running = False

    def _execute_initialize(self) -> None:
        """执行初始化阶段"""
        from simtradelab.ptrade.lifecycle_controller import LifecyclePhase

        self.log.info("Executing initialize phase")
        self.lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        self._strategy_functions["initialize"](self.context)
        self.context.initialized = True

    def _run_daily_loop(self, date_range) -> bool:
        """执行每日回测循环

        Args:
            date_range: 交易日序列

        Returns:
            是否成功完成所有交易日
        """
        from datetime import timedelta
        from simtradelab.ptrade.object import Data
        from simtradelab.ptrade.cache_manager import cache_manager

        # 跨日追踪：上一交易日收盘后的组合市值（用于计算真实日盈亏）
        prev_day_end_value = None

        for current_date in date_range:
            if self._cancel_event and self._cancel_event.is_set():
                self.log.info("回测已取消")
                return False
            # 更新日期上下文
            self.context.current_dt = current_date
            self.context.blotter.current_dt = current_date
            global _current_backtest_date
            _current_backtest_date = str(current_date.date())
            prev_trade_day = self.api.get_trading_day(-1)
            if prev_trade_day:
                self.context.previous_date = prev_trade_day
            else:
                # 回退方案：简单减1天
                self.context.previous_date = (current_date - timedelta(days=1)).date()

            # 清理全局缓存
            cache_manager.clear_daily_cache(current_date)

            # 收集交易日前置统计（日期轴）
            self.stats_collector.collect_pre_trading(self.context, current_date)

            # T+1日切：前日持仓全部可卖（在除权事件前重置，送股后由除权处理自行更新）
            for position in self.context.portfolio.positions.values():
                position.enable_amount = position.amount

            # 处理除权除息事件（在策略执行前）
            self._process_dividend_events(current_date)

            # 若上一交易日已跌破强平线，则在当日开盘执行强平
            self._process_pending_margin_liquidation(current_date)

            # 构造data对象
            data = Data(current_date, self.context.portfolio._bt_ctx)

            # 执行策略生命周期
            if not self._execute_lifecycle(data):
                return False

            # 执行 run_daily 注册的任务（日频固定在15:00执行）
            self._execute_daily_tasks()

            # 日终计提融资融券利息
            self.context.portfolio.accrue_margin_interest(current_date)
            self.context.portfolio.refresh_margin_risk_flags()

            # 收集交易金额（从OrderProcessor累计的gross金额）
            self.stats_collector.collect_trading_amounts(self.context)

            # 收集交易后统计（用上一交易日收盘后的组合市值计算真实日盈亏）
            current_end_value = self.context.portfolio.portfolio_value
            if prev_day_end_value is None:
                prev_day_end_value = self.context.portfolio.starting_cash
            self.stats_collector.collect_post_trading(self.context, prev_day_end_value)
            prev_day_end_value = current_end_value

        return True

    def _run_minute_loop(self, date_range) -> bool:
        """执行分钟级回测循环

        Args:
            date_range: 交易日序列

        Returns:
            是否成功完成所有交易日
        """
        import pandas as pd
        from datetime import timedelta
        from simtradelab.ptrade.object import Data
        from simtradelab.ptrade.cache_manager import cache_manager
        from simtradelab.ptrade.lifecycle_controller import LifecyclePhase

        # 跨日追踪：上一交易日收盘后的组合市值
        prev_day_end_value = None

        for current_date in date_range:
            if self._cancel_event and self._cancel_event.is_set():
                self.log.info("回测已取消")
                return False
            # 确保是 pd.Timestamp（防止 datetime.date 无法 replace 时间分量）
            current_date = pd.Timestamp(current_date)

            # 更新日期上下文（设为开盘时间）
            self.context.current_dt = current_date
            self.context.blotter.current_dt = current_date
            global _current_backtest_date
            _current_backtest_date = str(current_date.date())

            # 使用API获取真正的前一交易日
            prev_trade_day = self.api.get_trading_day(-1)
            if prev_trade_day:
                self.context.previous_date = prev_trade_day
            else:
                self.context.previous_date = (current_date - timedelta(days=1)).date()

            # 清理全局缓存
            cache_manager.clear_daily_cache(current_date)

            # 收集交易日前置统计（日期轴）
            self.stats_collector.collect_pre_trading(self.context, current_date)

            # T+1日切：前日持仓全部可卖
            for position in self.context.portfolio.positions.values():
                position.enable_amount = position.amount

            # 处理除权除息事件（在策略执行前）
            self._process_dividend_events(current_date)

            # 若上一交易日已跌破强平线，则在当日开盘执行强平
            self._process_pending_margin_liquidation(current_date.replace(hour=9, minute=30, second=0))

            # 构造data对象
            data = Data(current_date, self.context.portfolio._bt_ctx)

            # 1. before_trading_start（每日一次，开盘前）
            if not self._safe_call('before_trading_start', LifecyclePhase.BEFORE_TRADING_START, data):
                return False

            # 2. handle_data + run_daily任务（分钟级调用）
            minute_bars = self._get_minute_bars(current_date)
            daily_task_times = self._get_daily_task_time_set()
            for minute_dt in minute_bars:
                self.context.current_dt = minute_dt
                _current_backtest_date = minute_dt.strftime('%Y-%m-%d %H:%M')
                data = Data(minute_dt, self.context.portfolio._bt_ctx)
                if not self._safe_call('handle_data', LifecyclePhase.HANDLE_DATA, data):
                    return False
                # 在匹配的分钟bar执行run_daily任务
                hhmm = f'{minute_dt.hour:02d}:{minute_dt.minute:02d}'
                if hhmm in daily_task_times:
                    self._execute_daily_tasks_for_time(hhmm)

            # 3. after_trading_end（每日一次，收盘后）
            self.context.current_dt = current_date.replace(hour=15, minute=0, second=0)
            data = Data(self.context.current_dt, self.context.portfolio._bt_ctx)
            self._safe_call('after_trading_end', LifecyclePhase.AFTER_TRADING_END, data, allow_fail=True)

            # 日终计提融资融券利息
            self.context.portfolio.accrue_margin_interest(self.context.current_dt)
            self.context.portfolio.refresh_margin_risk_flags()

            # 收集交易金额（从OrderProcessor累计的gross金额）
            self.stats_collector.collect_trading_amounts(self.context)

            # 收集交易后统计
            current_end_value = self.context.portfolio.portfolio_value
            if prev_day_end_value is None:
                prev_day_end_value = self.context.portfolio.starting_cash
            self.stats_collector.collect_post_trading(self.context, prev_day_end_value)
            prev_day_end_value = current_end_value

        return True

    # 预生成分钟时间偏移模板（类级别，只算一次）
    _MINUTE_OFFSETS = None

    @classmethod
    def _get_minute_offsets(cls):
        """生成分钟时间偏移模板（惰性初始化，仅一次）"""
        if cls._MINUTE_OFFSETS is None:
            from datetime import timedelta
            # A股交易时间: 9:30-11:30 (121分钟), 13:00-15:00 (121分钟)
            morning = [timedelta(hours=9, minutes=30) + timedelta(minutes=i) for i in range(121)]
            afternoon = [timedelta(hours=13) + timedelta(minutes=i) for i in range(121)]
            cls._MINUTE_OFFSETS = morning + afternoon
        return cls._MINUTE_OFFSETS

    def _get_minute_bars(self, trade_date):
        """生成交易日分钟时间序列

        Args:
            trade_date: 交易日

        Returns:
            分钟时间戳列表
        """
        base = trade_date.normalize()
        return [base + offset for offset in self._get_minute_offsets()]

    def _execute_daily_tasks(self) -> None:
        """执行所有 run_daily 注册的任务（日频模式：忽略time参数，全部执行）"""
        for func, _ in self.api._daily_tasks:
            try:
                func(self.context)
            except Exception as e:
                self.log.error(f"run_daily任务执行失败: {e}")
                self.log.error(traceback.format_exc())

    def _get_daily_task_time_set(self) -> set[str]:
        """获取所有 run_daily 注册的时间集合（分钟模式用）"""
        return {time_str for _, time_str in self.api._daily_tasks}

    def _execute_daily_tasks_for_time(self, hhmm: str) -> None:
        """执行指定时间的 run_daily 任务（分钟模式）"""
        for func, time_str in self.api._daily_tasks:
            if time_str == hhmm:
                try:
                    func(self.context)
                except Exception as e:
                    self.log.error(f"run_daily任务({hhmm})执行失败: {e}")
                    self.log.error(traceback.format_exc())

    def _process_pending_margin_liquidation(self, trade_dt) -> None:
        """在下一交易日开盘执行待处理的强平。"""
        portfolio = self.context.portfolio
        if not portfolio.margin_enabled or not portfolio.margin_liquidation_pending:
            return

        original_dt = self.context.current_dt
        original_blotter_dt = self.context.blotter.current_dt
        try:
            self.context.current_dt = trade_dt
            self.context.blotter.current_dt = trade_dt
            self.log.warning('维持担保比例跌破强平线，开始执行开盘强平: %s', trade_dt)
            self._force_liquidate_margin_account()
        finally:
            self.context.current_dt = original_dt
            self.context.blotter.current_dt = original_blotter_dt

    def _force_liquidate_margin_account(self) -> None:
        """执行全账户强平，优先卖出多头并归还融资，再回补融券。"""
        portfolio = self.context.portfolio

        # 1) 先卖出全部多头持仓，优先偿还对应的融资负债
        for stock in list(portfolio.positions.keys()):
            position = portfolio.positions.get(stock)
            if position is None or position.amount <= 0:
                continue
            amount = int(position.amount)
            price = self.api.order_processor.get_execution_price(stock, None, False)
            if price is None:
                self.log.warning('强平卖出跳过 %s | 原因: 无法获取价格', stock)
                continue

            revenue = amount * price
            commission = self.api.order_processor.calculate_commission(amount, price, is_sell=True)
            tax_adjustment = portfolio.remove_position(stock, amount, self.context.current_dt)
            interest_repaid = 0.0
            principal_repaid = 0.0
            if stock in portfolio.margin_cash_positions:
                interest_repaid = portfolio.repay_margin_interest(revenue)
                principal_repaid = portfolio.repay_margin_cash(stock, amount, revenue - interest_repaid)
            cash_delta = revenue - interest_repaid - principal_repaid - commission - tax_adjustment
            portfolio._cash += cash_delta
            self.context._daily_sell_total += revenue
            self.context.total_commission = getattr(self.context, 'total_commission', 0.0) + commission
            self.log.warning(
                '强平卖出 %s | %s 股 | 价格 %.3f | 偿还利息 %.2f | 偿还本金 %.2f',
                stock, amount, price, interest_repaid, principal_repaid
            )

        # 2) 再按现金能力回补融券负债
        for stock in list(portfolio.margin_short_positions.keys()):
            short_item = portfolio.margin_short_positions.get(stock)
            if short_item is None:
                continue
            amount = int(short_item.get('amount', 0) or 0)
            if amount <= 0:
                continue

            price = self.api.order_processor.get_execution_price(stock, None, True)
            if price is None:
                self.log.warning('强平买券还券跳过 %s | 原因: 无法获取价格', stock)
                continue

            commission = self.api.order_processor.calculate_commission(amount, price, is_sell=False)
            total_cost = amount * price + commission
            if total_cost > portfolio.cash + 1e-8:
                affordable = int(portfolio.cash / price / 100) * 100 if price > 0 else 0
                while affordable >= 100:
                    test_commission = self.api.order_processor.calculate_commission(affordable, price, is_sell=False)
                    if affordable * price + test_commission <= portfolio.cash + 1e-8:
                        break
                    affordable -= 100
                amount = affordable
                if amount <= 0:
                    self.log.warning('强平买券还券跳过 %s | 原因: 现金不足', stock)
                    continue
                commission = self.api.order_processor.calculate_commission(amount, price, is_sell=False)
                total_cost = amount * price + commission

            portfolio._cash -= total_cost
            portfolio.reduce_margin_short(stock, amount)
            self.context._daily_buy_total += amount * price
            self.context._daily_buy_commission = getattr(self.context, '_daily_buy_commission', 0.0) + commission
            self.context.total_commission = getattr(self.context, 'total_commission', 0.0) + commission
            self.log.warning('强平买券还券 %s | %s 股 | 价格 %.3f', stock, amount, price)

        # 3) 使用剩余现金继续归还融资利息和本金
        if portfolio.cash > 0:
            interest_actual = portfolio.repay_margin_interest(portfolio.cash)
            principal_actual = portfolio.repay_margin_cash_with_value(portfolio.cash - interest_actual)
            actual = interest_actual + principal_actual
            portfolio._cash -= actual
            if actual > 0:
                self.log.warning('强平后继续归还融资负债 | 利息 %.2f | 本金 %.2f', interest_actual, principal_actual)

        portfolio.clear_margin_liquidation_flag()
        portfolio.refresh_margin_risk_flags()

    def _execute_lifecycle(self, data) -> bool:
        """执行策略生命周期方法

        Args:
            data: Data对象

        Returns:
            是否成功执行
        """
        from simtradelab.ptrade.lifecycle_controller import LifecyclePhase

        # before_trading_start
        if not self._safe_call('before_trading_start', LifecyclePhase.BEFORE_TRADING_START, data):
            return False

        # handle_data
        if not self._safe_call('handle_data', LifecyclePhase.HANDLE_DATA, data):
            return False

        # after_trading_end（允许失败）
        self._safe_call('after_trading_end', LifecyclePhase.AFTER_TRADING_END, data, allow_fail=True)

        return True

    def _safe_call(
        self,
        func_name: str,
        phase,
        data,
        allow_fail: bool = False
    ) -> bool:
        """安全调用策略方法

        Args:
            func_name: 函数名
            phase: 生命周期阶段
            data: Data对象
            allow_fail: 是否允许失败

        Returns:
            是否成功执行
        """
        # 始终设置生命周期阶段，即使函数不存在
        try:
            self.lifecycle_controller.set_phase(phase)
        except Exception as e:
            self.log.error(f"设置生命周期阶段 {phase} 失败: {e}")
            return False

        # 如果函数不存在，阶段已设置，直接返回成功
        if func_name not in self._strategy_functions:
            return True

        # 执行策略函数
        try:
            self._strategy_functions[func_name](self.context, data)
            return True
        except ValueError as e:
            self.log.error(f"{func_name}执行失败: {e}")
            return allow_fail
        except Exception as e:
            self.log.error(f"{func_name}执行失败: {e}")
            traceback.print_exc()
            return allow_fail

    def _process_dividend_events(self, current_date):
        """处理除权除息事件。

        处理逻辑：
        1. 多头：现金分红、送股/转增、配股摊薄与现金扣款
        2. 融券空头：红利补偿、送转后需归还证券数量调整
        """
        try:
            portfolio = self.context.portfolio
            date_str = current_date.strftime('%Y%m%d')
            date_int = int(date_str)
            long_positions = list(portfolio.positions.items())
            short_positions = list(portfolio.margin_short_positions.items())

            for stock_code, position in long_positions:
                if position.amount <= 0:
                    continue

                original_amount = int(position.amount)
                exrights_df = self.api.data_context.exrights_dict.get(stock_code)
                event = None
                if exrights_df is not None and not exrights_df.empty and date_int in exrights_df.index:
                    event = exrights_df.loc[date_int]

                allotted = float(event.get('allotted_ps', 0) or 0) if event is not None else 0.0
                rationed = float(event.get('rationed_ps', 0) or 0) if event is not None else 0.0
                rationed_px = float(event.get('rationed_px', 0) or 0) if event is not None else 0.0
                dividend_per_share_before_tax = float(event.get('bonus_ps', 0) or 0) if event is not None else 0.0

                if dividend_per_share_before_tax <= 0 and stock_code in self.api.data_context.dividend_cache:
                    stock_dividends = self.api.data_context.dividend_cache[stock_code]
                    if date_str in stock_dividends:
                        dividend_per_share_before_tax = float(stock_dividends[date_str] or 0.0)

                share_multiplier = 1.0 + allotted + rationed
                if share_multiplier > 1.0:
                    new_amount = int(round(original_amount * share_multiplier))
                    adjusted_cost = position.cost_basis
                    if share_multiplier > 0:
                        adjusted_cost = (position.cost_basis - dividend_per_share_before_tax + rationed * rationed_px) / share_multiplier
                    position.amount = new_amount
                    position.enable_amount = new_amount
                    position.cost_basis = max(adjusted_cost, 0.0)
                    position.market_value = new_amount * position.cost_basis
                    if stock_code in portfolio._position_lots:
                        for lot in portfolio._position_lots[stock_code]:
                            lot['amount'] = int(round(lot['amount'] * share_multiplier))
                    if rationed > 0 and rationed_px > 0:
                        portfolio._cash -= original_amount * rationed * rationed_px
                    portfolio._invalidate_cache()

                if dividend_per_share_before_tax > 0:
                    pre_tax_rate = 0.20
                    total_dividend_after_tax = dividend_per_share_before_tax * (1 - pre_tax_rate) * original_amount
                    if total_dividend_after_tax > 0:
                        portfolio._cash += total_dividend_after_tax
                        portfolio._invalidate_cache()
                        portfolio.add_dividend(stock_code, dividend_per_share_before_tax)

            for stock_code, item in short_positions:
                original_amount = int(item.get('amount', 0) or 0)
                if original_amount <= 0:
                    continue

                exrights_df = self.api.data_context.exrights_dict.get(stock_code)
                if exrights_df is None or exrights_df.empty or date_int not in exrights_df.index:
                    continue

                event = exrights_df.loc[date_int]
                allotted = float(event.get('allotted_ps', 0) or 0)
                rationed = float(event.get('rationed_ps', 0) or 0)
                rationed_px = float(event.get('rationed_px', 0) or 0)
                dividend_per_share_before_tax = float(event.get('bonus_ps', 0) or 0)
                share_multiplier = 1.0 + allotted + rationed

                if share_multiplier > 1.0:
                    new_amount = int(round(original_amount * share_multiplier))
                    adjusted_open_price = float(item.get('open_price', 0.0) or 0.0)
                    adjusted_open_price = max((adjusted_open_price - dividend_per_share_before_tax + rationed * rationed_px) / share_multiplier, 0.0)
                    item['amount'] = new_amount
                    item['open_price'] = adjusted_open_price

                if dividend_per_share_before_tax > 0:
                    compensation = dividend_per_share_before_tax * original_amount
                    item['dividend_compensation_paid'] = round(float(item.get('dividend_compensation_paid', 0.0) or 0.0) + compensation, 2)
                    portfolio._cash -= compensation

                portfolio._invalidate_cache()

        except Exception as e:
            self.log.warning(f"除权除息处理失败: {e}")
            traceback.print_exc()

    # ==========================================
    # 重置和清理接口
    # ==========================================

    def reset_strategy(self) -> None:
        """重置策略状态"""
        self.log.info("Resetting strategy state")

        self._strategy_functions.clear()
        self._strategy_name = None
        self._is_running = False

        # 重置Context
        self.context.reset_for_new_strategy()
