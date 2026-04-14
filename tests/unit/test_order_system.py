# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
测试订单系统 - Blotter和Order
"""

from datetime import datetime

from simtradelab.ptrade.object import Blotter, Order


class TestOrder:
    """测试Order订单对象"""

    def test_order_creation(self):
        """测试订单创建"""
        order = Order(
            id=1,
            symbol='600000.SH',
            amount=100,
            dt=datetime(2024, 1, 2),
            limit=10.5
        )

        assert order.id == 1
        assert order.symbol == '600000.SH'
        assert order.amount == 100
        assert order.dt == datetime(2024, 1, 2)
        assert order.limit == 10.5
        assert order.filled == 0
        assert order.status == '0'

    def test_order_created_property(self):
        """测试created属性（dt的别名）"""
        dt = datetime(2024, 1, 2)
        order = Order(
            id=1,
            symbol='600000.SH',
            amount=100,
            dt=dt
        )

        assert order.created == dt
        assert order.created == order.dt

    def test_order_buy_direction(self):
        """测试买入方向（正数）"""
        order = Order(
            id=1,
            symbol='600000.SH',
            amount=100
        )

        assert order.amount > 0  # 买入

    def test_order_sell_direction(self):
        """测试卖出方向（负数）"""
        order = Order(
            id=2,
            symbol='600000.SH',
            amount=-100
        )

        assert order.amount < 0  # 卖出


class TestBlotter:
    """测试Blotter订单簿"""

    def test_blotter_initialization(self):
        """测试Blotter初始化"""
        current_dt = datetime(2024, 1, 2)
        blotter = Blotter(current_dt)

        assert blotter.current_dt == current_dt
        assert blotter.open_orders == []
        assert blotter._order_id_counter == 0

    def test_create_order(self):
        """测试创建订单"""
        current_dt = datetime(2024, 1, 2)
        blotter = Blotter(current_dt)

        order = blotter.create_order('600000.SH', 100)

        assert isinstance(order, Order)
        assert order.id == 1
        assert order.symbol == '600000.SH'
        assert order.amount == 100
        assert order.dt == current_dt
        assert order in blotter.open_orders
        assert len(blotter.open_orders) == 1

    def test_order_id_auto_increment(self):
        """测试订单ID自动递增"""
        blotter = Blotter(datetime(2024, 1, 2))

        order1 = blotter.create_order('600000.SH', 100)
        order2 = blotter.create_order('000001.SZ', 200)
        order3 = blotter.create_order('600519.SH', 50)

        assert order1.id == 1
        assert order2.id == 2
        assert order3.id == 3
        assert len(blotter.open_orders) == 3

    def test_cancel_order(self):
        """测试取消订单"""
        blotter = Blotter(datetime(2024, 1, 2))

        order1 = blotter.create_order('600000.SH', 100)
        order2 = blotter.create_order('000001.SZ', 200)

        # 取消订单
        result = blotter.cancel_order(order1)

        assert result is True
        assert order1.status == 'cancelled'
        assert order1 not in blotter.open_orders
        assert order2 in blotter.open_orders
        assert len(blotter.open_orders) == 1

    def test_cancel_nonexistent_order(self):
        """测试取消不存在的订单"""
        blotter = Blotter(datetime(2024, 1, 2))

        # 创建一个订单但不通过blotter
        fake_order = Order(id=999, symbol='600000.SH', amount=100)

        # 取消不存在的订单应该返回False
        result = blotter.cancel_order(fake_order)

        assert result is False
        assert fake_order.status != 'cancelled'  # 状态不应改变


class TestOrderStatus:
    """测试订单状态转换"""

    def test_order_status_cancelled(self):
        """测试订单cancelled状态"""
        blotter = Blotter(datetime(2024, 1, 2))

        order = blotter.create_order('600000.SH', 100)

        # 取消前
        assert order.status == '0'

        # 取消订单
        blotter.cancel_order(order)

        # 取消后
        assert order.status == 'cancelled'
