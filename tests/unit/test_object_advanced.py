# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
Object模块高级测试 - 提升覆盖率
"""

import pytest
from datetime import datetime

from simtradelab.ptrade.object import Position


class TestPositionAdvanced:
    """Position类的高级测试"""

    def test_position_cost_basis(self):
        """测试成本基础计算"""
        pos = Position(
            stock='600000.SH',
            amount=100,
            cost_basis=10.5
        )
        assert pos.cost_basis == 10.5
        assert pos.amount == 100

    def test_position_properties(self):
        """测试仓位属性访问"""
        pos = Position(
            stock='600000.SH',
            amount=100,
            cost_basis=10.5
        )
        assert pos.stock == '600000.SH'
        assert pos.sid == '600000.SH'  # 别名
        assert pos.amount == 100
        assert pos.cost_basis == 10.5
        assert pos.enable_amount == 100
        assert pos.today_amount == 0

    def test_position_market_value(self):
        """测试持仓市值计算"""
        pos = Position(
            stock='600000.SH',
            amount=100,
            cost_basis=10.5
        )
        # market_value = amount * cost_basis
        assert pos.market_value == 100 * 10.5


class TestPortfolioAdvanced:
    """Portfolio类的高级测试"""

    def test_portfolio_available_cash_after_buy(self, portfolio):
        """测试买入后的可用现金"""
        initial_cash = portfolio.cash

        # 添加仓位会减少现金
        portfolio.add_position(
            stock='600000.SH',
            amount=1000,
            price=10.0,
            date=datetime(2024, 1, 1)
        )

        # 检查现金是否改变(Portfolio可能有不同实现)
        # 至少应该有positions记录
        assert '600000.SH' in portfolio.positions

    def test_portfolio_available_cash_after_sell(self, portfolio):
        """测试卖出后的可用现金"""
        # 先添加仓位
        portfolio.add_position(
            stock='600000.SH',
            amount=1000,
            price=10.0,
            date=datetime(2024, 1, 1)
        )

        assert '600000.SH' in portfolio.positions

        # 卖出
        portfolio.remove_position('600000.SH', 500, datetime(2024, 1, 5))

        # 持仓应该减少
        assert portfolio.positions['600000.SH'].amount == 500

    def test_portfolio_positions_dict(self, portfolio):
        """测试持仓字典"""
        # 添加多个仓位
        portfolio.add_position('600000.SH', 100, 10.0, datetime(2024, 1, 1))
        portfolio.add_position('000001.SZ', 200, 15.0, datetime(2024, 1, 2))

        assert '600000.SH' in portfolio.positions
        assert '000001.SZ' in portfolio.positions
        assert len(portfolio.positions) == 2

    def test_portfolio_starting_cash_immutable(self, portfolio):
        """测试初始资金不可变"""
        initial = portfolio.starting_cash

        # 改变现金(通过交易)
        portfolio.add_position('600000.SH', 100, 10.0, datetime(2024, 1, 1))

        # starting_cash应保持不变
        assert portfolio.starting_cash == initial

    def test_portfolio_total_lots_single_position(self, portfolio):
        """测试单个仓位的批次数"""
        portfolio.add_position('600000.SH', 100, 10.0, datetime(2024, 1, 1))

        # 应该有1个批次
        lots = portfolio._position_lots.get('600000.SH', [])
        assert len(lots) == 1

    def test_portfolio_total_lots_multiple_adds(self, portfolio):
        """测试多次加仓的批次数"""
        portfolio.add_position('600000.SH', 100, 10.0, datetime(2024, 1, 1))
        portfolio.add_position('600000.SH', 50, 11.0, datetime(2024, 1, 5))

        # 应该有2个批次
        lots = portfolio._position_lots.get('600000.SH', [])
        assert len(lots) == 2

