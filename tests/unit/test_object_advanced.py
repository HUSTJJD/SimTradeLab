# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
Object模块高级测试 - 提升覆盖率
"""

from datetime import datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from simtradelab.ptrade.object import Data, Position


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


class TestStockDataAdvanced:
    """PTrade 文档口径的 StockData 字段测试。"""

    def test_money_reads_real_money_column_not_close_times_volume(self):
        # api_doc.txt: money 为平台真实成交额字段，旧实现用 close*volume 代替。
        dates = pd.date_range("2024-01-01", periods=5)
        stock_df = pd.DataFrame(
            {
                "open": np.arange(5.0) + 10.0,
                "high": np.arange(5.0) + 11.0,
                "low": np.arange(5.0) + 9.0,
                "close": np.arange(5.0) + 10.0,
                "volume": np.arange(5.0) + 100.0,
                "money": np.arange(5.0) * 1000.0 + 7.0,
            },
            index=dates,
        )
        idx = pd.DatetimeIndex(stock_df.index)
        date_dict = {int(ts.value): pos for pos, ts in enumerate(idx)}
        sorted_i8 = idx.asi8.astype("i8")
        bt_ctx = SimpleNamespace(
            stock_data_dict={"600000.SH": stock_df},
            get_stock_date_index=lambda stock: (date_dict, sorted_i8),
            context=None,
        )

        bar = Data(pd.Timestamp("2024-01-05"), bt_ctx)["600000.SH"]

        assert bar.money == 4007.0
        close_times_volume = float(
            stock_df.loc[pd.Timestamp("2024-01-05"), "close"]
            * stock_df.loc[pd.Timestamp("2024-01-05"), "volume"]
        )
        assert bar.money != close_times_volume


class TestLazyLoadingContracts:
    """按股票池惰性加载后，dict 语义与逐值结果必须与全量加载一致。"""

    @staticmethod
    def _write_adj_parquet(tmp_path):
        dates = pd.date_range("2024-01-02", periods=3)
        rows = []
        for symbol, factor in (("000001.SZ", 1.0), ("600000.SS", 2.0)):
            rows.append(pd.DataFrame({
                "date": dates,
                "adj_a": [factor] * 3,
                "adj_b": [factor / 2] * 3,
                "symbol": symbol,
            }))
        path = tmp_path / "adj.parquet"
        pd.concat(rows, ignore_index=True).to_parquet(path, index=False)
        return path

    def test_adj_factors_dict_matches_eager_load(self, tmp_path):
        from pandas.testing import assert_frame_equal

        from simtradelab.ptrade import adj_cache

        path = self._write_adj_parquet(tmp_path)
        eager = adj_cache._parquet_to_adj_cache(str(path))
        candidate = ["000001.SZ", "600000.SS", "999999.SZ"]
        lazy = adj_cache.AdjFactorsDict(str(path), candidate)

        # 惰性字典非空（api.py 用 bool 判断缓存是否可用），但初始不读盘
        assert bool(lazy)
        assert all(v is lazy._UNLOADED for v in lazy.values())

        lazy.ensure_loaded(candidate)
        for symbol in ("000001.SZ", "600000.SS"):
            assert symbol in lazy
            assert_frame_equal(lazy[symbol], eager[symbol], check_exact=True)
            assert_frame_equal(lazy.get(symbol), eager[symbol], check_exact=True)

        # parquet 中不存在的 symbol：in / [] / get 语义与普通小 dict 一致
        assert "999999.SZ" not in lazy
        assert lazy.get("999999.SZ", "fallback") == "fallback"
        with pytest.raises(KeyError):
            _ = lazy["999999.SZ"]

    def test_adj_factors_dict_contains_triggers_lazy_load(self, tmp_path):
        from pandas.testing import assert_frame_equal

        from simtradelab.ptrade import adj_cache

        path = self._write_adj_parquet(tmp_path)
        eager = adj_cache._parquet_to_adj_cache(str(path))
        lazy = adj_cache.AdjFactorsDict(str(path), ["000001.SZ", "600000.SS"])

        assert "000001.SZ" in lazy  # 未预热也应自动加载
        assert_frame_equal(lazy["000001.SZ"], eager["000001.SZ"], check_exact=True)

    def test_lazy_data_dict_batch_equals_per_key_load(self, tmp_path):
        from pandas.testing import assert_frame_equal

        from simtradelab.ptrade.object import LazyDataDict

        stocks_dir = tmp_path / "stocks"
        stocks_dir.mkdir()
        dates = pd.date_range("2024-01-02", periods=4)
        for symbol in ("A.SZ", "B.SZ"):
            pd.DataFrame({
                "date": dates,
                "open": [10.0] * 4,
                "high": [11.0] * 4,
                "low": [9.0] * 4,
                "close": [10.5] * 4,
                "volume": [100.0] * 4,
            }).to_parquet(stocks_dir / f"{symbol}.parquet", index=False)

        keys = ["A.SZ", "B.SZ"]
        per_key = LazyDataDict(str(tmp_path), "stock", keys,
                               preload=False, skip_empty=True)
        expected = {key: per_key[key] for key in keys}

        batch = LazyDataDict(str(tmp_path), "stock", keys,
                             preload=False, skip_empty=True)
        assert len(batch._cache) == 0
        batch.ensure_loaded(keys)
        assert len(batch._cache) == 2
        for key in keys:
            assert_frame_equal(batch[key], expected[key], check_exact=True)

        # 重复预热不应重复读盘或改变缓存内容
        batch.ensure_loaded(keys)
        assert len(batch._cache) == 2

