# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
测试DataContext数据上下文
"""


class TestDataContext:
    """测试DataContext数据容器"""

    def test_data_context_dividend_cache_default(self):
        """测试DataContext的dividend_cache默认值处理"""
        from simtradelab.ptrade.data_context import DataContext

        # 测试None转为空字典
        dc_none = DataContext(
            stock_data_dict={},
            valuation_dict={},
            fundamentals_dict={},
            exrights_dict={},
            benchmark_data={},
            stock_metadata=None,
            index_constituents={},
            stock_status_history={},
            adj_pre_cache={},
            dividend_cache=None,
        )
        assert dc_none.dividend_cache == {}
        assert isinstance(dc_none.dividend_cache, dict)

        # 测试传入自定义dividend_cache
        custom_cache = {"000001.XSHE": [{"date": "2024-01-01", "amount": 0.5}]}
        dc_custom = DataContext(
            stock_data_dict={},
            valuation_dict={},
            fundamentals_dict={},
            exrights_dict={},
            benchmark_data={},
            stock_metadata=None,
            index_constituents={},
            stock_status_history={},
            adj_pre_cache={},
            dividend_cache=custom_cache,
        )
        assert dc_custom.dividend_cache == custom_cache
        assert dc_custom.dividend_cache is custom_cache  # 验证引用传递

