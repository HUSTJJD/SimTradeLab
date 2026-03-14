# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
统一订单处理器

整合订单创建、验证、执行的完整流程
"""


from __future__ import annotations

from typing import Optional
import uuid
import pandas as pd

from .config_manager import config
from .object import Order


class OrderProcessor:
    """订单处理器

    统一管理订单的完整生命周期：
    1. 价格获取
    2. 涨跌停检查
    3. 订单创建
    4. 买卖执行
    """

    def __init__(self, context, data_context, get_stock_date_index_func, log):
        """初始化订单处理器

        Args:
            context: 上下文对象
            data_context: 数据上下文对象
            get_stock_date_index_func: 获取股票日期索引的函数
            log: 日志对象
        """
        self.context = context
        self.data_context = data_context
        self.get_stock_date_index = get_stock_date_index_func
        self.log = log

    def _get_bar_context(self, stock: str) -> tuple[Optional[pd.DataFrame], Optional[int], str]:
        """返回当前交易上下文对应的数据表与索引。"""
        frequency = getattr(self.context, 'frequency', '1d')
        if frequency == '1m' and self.data_context.stock_data_dict_1m is not None:
            data_source = self.data_context.stock_data_dict_1m
        else:
            data_source = self.data_context.stock_data_dict

        if stock not in data_source:
            return None, None, frequency

        stock_df = data_source[stock]
        if not isinstance(stock_df, pd.DataFrame):
            return None, None, frequency

        try:
            current_dt = self.context.current_dt
            if frequency == '1m':
                idx = stock_df.index.searchsorted(current_dt, side='right') - 1
                if idx < 0:
                    return stock_df, None, frequency
            else:
                query_dt = pd.Timestamp(current_dt).normalize()
                date_dict, _ = self.get_stock_date_index(stock)
                idx = date_dict.get(query_dt.value)
                if idx is None:
                    idx = stock_df.index.get_loc(query_dt)
            return stock_df, idx, frequency
        except Exception:
            return stock_df, None, frequency

    def get_bar_volume_limit(self, stock: str) -> int:
        """返回当前 bar 可成交的最大数量。"""
        stock_df, idx, frequency = self._get_bar_context(stock)
        if stock_df is None or idx is None:
            return 0

        if config.trading.limit_mode == 'UNLIMITED':
            return int(1e18)

        if frequency == '1m':
            reference_volume = float(stock_df['volume'].values[idx])
        else:
            ref_idx = idx - 1 if idx > 0 else idx
            reference_volume = float(stock_df['volume'].values[ref_idx])

        if reference_volume <= 0:
            return 0
        return int(reference_volume * config.trading.volume_ratio)

    def apply_volume_limit(self, stock: str, amount: int) -> int:
        """按配置限制单笔成交量。"""
        if amount == 0 or config.trading.limit_mode == 'UNLIMITED':
            return amount

        limit_amount = self.get_bar_volume_limit(stock)
        if limit_amount <= 0:
            return 0

        sign = 1 if amount > 0 else -1
        adjusted = min(abs(int(amount)), limit_amount)
        if adjusted >= 100:
            adjusted = (adjusted // 100) * 100
        if adjusted <= 0:
            return 0
        if adjusted < abs(int(amount)):
            self.log.info("成交量限制生效 %s | 数量 %s -> %s", stock, abs(int(amount)), adjusted)
        return sign * adjusted

    def get_execution_price(self, stock: str, limit_price: Optional[float] = None, is_buy: bool = True) -> Optional[float]:
        """获取交易执行价格（含滑点）。"""
        if limit_price is not None:
            base_price = float(limit_price)
        else:
            stock_df, idx, frequency = self._get_bar_context(stock)
            if stock_df is None or idx is None:
                self.log.warning("get_execution_price 失败 | %s 无法定位当前bar", stock)
                return None

            try:
                volume = stock_df['volume'].values[idx]
                if volume == 0:
                    self.log.warning("订单撤销: 当前bar交易量不足 %s", stock)
                    return None

                price_col = 'close' if frequency == '1m' else 'open'
                price = stock_df[price_col].values[idx]
                base_price = float(price)
                if pd.isna(base_price) or base_price <= 0:
                    self.log.warning("get_execution_price 失败 | %s 价格异常: %s", stock, base_price)
                    return None
            except Exception as e:
                self.log.warning("get_execution_price 异常 | %s: %s", stock, e)
                return None

        slippage = config.trading.slippage
        fixed_slippage = config.trading.fixed_slippage
        if slippage > 0:
            slippage_amount = base_price * slippage / 2
        elif fixed_slippage > 0:
            slippage_amount = fixed_slippage / 2
        else:
            slippage_amount = 0

        final_price = base_price + slippage_amount if is_buy else base_price - slippage_amount
        return final_price

    def check_limit_status(self, stock: str, delta: int, limit_status: int) -> bool:
        """检查涨跌停限制。"""
        if config.trading.limit_mode == 'UNLIMITED':
            return True
        if limit_status > 0 and delta > 0:
            self.log.warning("订单失败 %s | 原因: 一字涨停无法买入", stock)
            return False
        if limit_status < 0 and delta < 0:
            self.log.warning("订单失败 %s | 原因: 一字跌停无法卖出", stock)
            return False
        return True

    def create_order(self, stock: str, amount: int, price: float) -> tuple[str, object]:
        """创建订单对象

        Args:
            stock: 股票代码
            amount: 交易数量
            price: 交易价格

        Returns:
            (order_id, order对象)
        """
        order_id = str(uuid.uuid4()).replace('-', '')
        order = Order(
            id=order_id,
            symbol=stock,
            amount=amount,
            dt=self.context.current_dt,
            limit=price
        )
        return order_id, order

    def calculate_commission(self, amount: int, price: float, is_sell: bool = False) -> float:
        """计算手续费

        Args:
            amount: 交易数量
            price: 交易价格
            is_sell: 是否卖出

        Returns:
            手续费总额
        """
        commission_ratio = config.trading.commission_ratio
        min_commission = config.trading.min_commission

        value = amount * price
        # 佣金费
        broker_fee = max(value * commission_ratio, min_commission)
        # 经手费
        transfer_fee = value * config.trading.transfer_fee_rate

        commission = broker_fee + transfer_fee

        # 印花税(仅卖出时收取)
        if is_sell:
            commission += value * config.trading.stamp_tax_rate

        return commission

    def execute_buy(self, stock: str, amount: int, price: float) -> bool:
        """执行买入操作

        Args:
            stock: 股票代码
            amount: 买入数量
            price: 买入价格

        Returns:
            是否成功
        """
        cost = amount * price
        commission = self.calculate_commission(amount, price, is_sell=False)
        total_cost = cost + commission

        available_cash = self.context.portfolio.available_cash
        if total_cost > available_cash:
            daily_commission = getattr(self.context, '_daily_buy_commission', 0.0)
            if cost > available_cash + daily_commission:
                self.log.warning(f"【买入失败】{stock} | 原因: 可用资金不足 (需要{total_cost:.2f}, 可用{available_cash:.2f})")
                return False

        self.context.portfolio._cash -= total_cost

        # 记录手续费
        if not hasattr(self.context, 'total_commission'):
            self.context.total_commission = 0
        self.context.total_commission += commission
        self.context._daily_buy_commission = getattr(self.context, '_daily_buy_commission', 0.0) + commission

        # 建仓/加仓（含批次追踪），cost_basis含佣金（与Ptrade一致）
        cost_basis = total_cost / amount
        self.context.portfolio.add_position(stock, amount, cost_basis, self.context.current_dt)

        # 累计当日买入金额（gross，不含手续费）
        self.context._daily_buy_total += amount * price

        return True

    def execute_sell(self, stock: str, amount: int, price: float) -> bool:
        """执行卖出操作（FIFO：先进先出）

        Args:
            stock: 股票代码
            amount: 卖出数量（正数）
            price: 卖出价格

        Returns:
            是否成功
        """
        if stock not in self.context.portfolio.positions:
            self.log.warning(f"【卖出失败】{stock} | 原因: 无持仓")
            return False

        position = self.context.portfolio.positions[stock]

        # T+1限制：只能卖出 enable_amount（前日持仓）
        if self.context.t_plus_1:
            if position.enable_amount <= 0:
                self.log.warning(f"【卖出失败】{stock} | 原因: T+1限制，当日买入不可卖出")
                return False

            if amount > position.enable_amount:
                # 截断到可卖数量（整手）
                available = (position.enable_amount // 100) * 100
                if available <= 0:
                    available = position.enable_amount  # 零股全出
                self.log.info(f"T+1截断: {stock} 卖出 {amount} → {available} 股")
                amount = available

        if position.amount < amount:
            self.log.warning(f"【卖出失败】{stock} | 原因: 持仓不足 (持有{position.amount}, 尝试卖出{amount})")
            return False

        # 计算手续费
        revenue = amount * price
        commission = self.calculate_commission(amount, price, is_sell=True)

        # 减仓/清仓（含FIFO分红税调整）
        tax_adjustment = self.context.portfolio.remove_position(stock, amount, self.context.current_dt)

        # 净收入
        net_revenue = revenue - commission - tax_adjustment

        # 记录手续费
        if not hasattr(self.context, 'total_commission'):
            self.context.total_commission = 0
        self.context.total_commission += commission

        # 更新价格（仅当position仍存在时）
        if stock in self.context.portfolio.positions:
            position = self.context.portfolio.positions[stock]
            position.last_sale_price = price
            if position.amount > 0:
                position.market_value = position.amount * price

        # 入账
        self.context.portfolio._cash += net_revenue

        # 累计当日卖出金额（gross，不含手续费）
        self.context._daily_sell_total += amount * price

        # 日志
        if tax_adjustment > 0:
            self.log.info(f"📊分红税 | {stock} | 补税{tax_adjustment:.2f}元")
        elif tax_adjustment < 0:
            self.log.info(f"📊分红税 | {stock} | 退税{-tax_adjustment:.2f}元")

        return True

    def process_order(self, stock: str, target_amount: int, limit_price: Optional[float] = None,
                     limit_status: int = 0) -> bool:
        """处理订单的完整流程

        Args:
            stock: 股票代码
            target_amount: 目标数量
            limit_price: 限价
            limit_status: 涨跌停状态

        Returns:
            是否成功
        """
        # 1. 获取执行价格
        price = self.get_execution_price(stock, limit_price)
        if price is None:
            self.log.warning(f"【订单失败】{stock} | 原因: 无法获取价格")
            return False

        # 2. 计算交易数量
        current_amount = 0
        if stock in self.context.portfolio.positions:
            current_amount = self.context.portfolio.positions[stock].amount

        delta = target_amount - current_amount

        if delta == 0:
            return True  # 无需交易

        # 3. 检查涨跌停
        if not self.check_limit_status(stock, delta, limit_status):
            return False

        # 4. 执行交易
        if delta > 0:
            return self.execute_buy(stock, delta, price)
        else:
            return self.execute_sell(stock, abs(delta), price)
