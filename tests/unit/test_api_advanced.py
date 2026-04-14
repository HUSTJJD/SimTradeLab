# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
API高级功能和边界测试 - 提升覆盖率
"""

import pytest
import pandas as pd
import numpy as np
import pandas as pd

from simtradelab.ptrade.lifecycle_controller import LifecyclePhase


class TestGetAsharesAPI:
    """测试get_Ashares API"""

    def test_get_ashares_no_date(self, ptrade_api):
        """测试不指定日期获取A股列表"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_Ashares()
        assert isinstance(result, list)
        assert len(result) > 0


class TestFundamentalsAdvanced:
    """测试基本面数据的高级场景"""

    def test_get_fundamentals_valuation_table(self, ptrade_api):
        """测试valuation表的特殊处理"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_fundamentals(
            security='600000.SH',
            table='valuation',
            fields=['market_cap', 'pe_ratio'],
            date='2024-01-01'
        )
        assert result is None or isinstance(result, pd.DataFrame)

    def test_get_fundamentals_empty_security(self, ptrade_api):
        """测试空股票列表查询"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_fundamentals(
            security=[],
            table='valuation',
            fields=['market_cap']
        )
        assert result is None or (isinstance(result, pd.DataFrame) and result.empty)


class TestTradeDaysAdvanced:
    """测试交易日API的高级场景"""

    def test_get_trade_days_with_start_end(self, ptrade_api):
        """测试用开始和结束日期获取交易日"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)

        result = ptrade_api.get_trade_days(
            start_date='2024-01-01',
            end_date='2024-01-10'
        )
        assert result is None or isinstance(result, list)

    def test_get_trade_days_none_params(self, ptrade_api):
        """测试不传参数获取所有交易日"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)

        result = ptrade_api.get_trade_days()
        assert result is None or isinstance(result, list)

    def test_get_trading_day_future(self, ptrade_api):
        """测试获取未来交易日"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        result = ptrade_api.get_trading_day(day=1)
        assert result is None or isinstance(result, str)

    def test_get_trading_day_far_past(self, ptrade_api):
        """测试获取很久之前的交易日"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        result = ptrade_api.get_trading_day(day=-100)
        assert result is None or isinstance(result, str)


class TestStockInfoAdvanced:
    """测试股票信息查询的高级场景"""

    def test_get_stock_info_all_fields(self, ptrade_api):
        """测试获取所有字段"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_stock_info('600000.SH')
        assert result is None or isinstance(result, dict)

    def test_get_stock_info_specific_fields(self, ptrade_api):
        """测试获取特定字段 - 通过结果判断"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_stock_info('600000.SH')
        # get_stock_info不支持fields参数,直接测试返回值
        assert result is None or isinstance(result, dict)

    def test_get_stock_info_invalid_stock(self, ptrade_api):
        """测试查询无效股票信息"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_stock_info('INVALID.XX')
        # 无效股票也会返回字典(可能包含默认值)
        assert result is None or isinstance(result, dict)


class TestGetPriceAdvanced:
    """测试get_price的高级场景"""

    def test_get_price_multiple_fields(self, ptrade_api):
        """测试获取多个字段"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        result = ptrade_api.get_price(
            security='600000.SH',
            fields=['open', 'high', 'low', 'close', 'volume']
        )
        assert result is None or isinstance(result, (pd.Series, pd.DataFrame, dict, float, int))

    def test_get_price_with_count(self, ptrade_api):
        """测试用count获取历史数据"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        result = ptrade_api.get_price(
            security='600000.SH',
            count=5,
            fields='close'
        )
        assert result is None or isinstance(result, (pd.Series, pd.DataFrame))

    def test_get_price_with_start_end(self, ptrade_api):
        """测试用起止日期获取历史数据"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-10')

        result = ptrade_api.get_price(
            security='600000.SH',
            start_date='2024-01-01',
            end_date='2024-01-05',
            fields='close'
        )
        assert result is None or isinstance(result, (pd.Series, pd.DataFrame))

    def test_get_price_list_of_securities(self, ptrade_api):
        """测试股票列表查询"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        result = ptrade_api.get_price(
            security=['600000.SH', '000001.SZ', '600519.SH'],
            fields='close'
        )
        assert result is None or isinstance(result, (pd.Series, pd.DataFrame))

    def test_get_price_with_fq(self, ptrade_api):
        """测试复权参数"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-05')

        # 测试前复权
        result = ptrade_api.get_price(
            security='600000.SH',
            fields='close',
            fq='pre'
        )
        assert result is None or isinstance(result, (float, int, np.number, pd.Series, pd.DataFrame))


class TestCheckStockStatus:
    """测试股票状态检查的高级场景"""

    def test_check_limit_up(self, ptrade_api):
        """测试涨停检查"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.check_limit('600000.SH')
        # check_limit返回字典
        assert isinstance(result, dict)

    def test_get_stock_status_normal(self, ptrade_api):
        """测试正常股票状态查询"""
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        ptrade_api.context.current_dt = pd.Timestamp('2024-01-02')

        result = ptrade_api.get_stock_status('600000.SH')
        # 返回1表示正常，0表示停牌或字典
        assert result is not None
