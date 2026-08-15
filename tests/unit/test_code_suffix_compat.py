# -*- coding: utf-8 -*-
"""PTrade 官方证券代码尾缀兼容测试。

官方依据：PTrade/api_doc.txt L243-248「策略中支持的代码尾缀」——
上海 XSHG/SS、深圳 XSHE/SZ、指数 XBHS、中金所期货 CCFX；
L2516 指数代码可为 000300.SS 或 000300.XBHS；
L3084 四位/两位尾缀代码皆可作为键取到返回字典值；
L4241/L6342 返回对象的 symbol 用四位尾缀，要求「做代码尾缀兼容」。

这些用例刻意使用与真实数据一致的 `.SS`/`.SZ` 内部键（既有 conftest 夹具用
`.SH` 键，会掩盖日线 date-index 路径上的兼容缺陷）。
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from simtradelab.ptrade.api import PtradeAPI, _normalize_code
from simtradelab.ptrade.code_compat import DualKeyDict, canonical_code, code_candidates
from simtradelab.ptrade.lifecycle_controller import LifecyclePhase

SH_STOCK = "600000.SS"
SZ_STOCK = "000001.SZ"
SH_LONG = "600000.XSHG"
SZ_LONG = "000001.XSHE"


def _frame(dates, close=10.0):
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": 100_000.0,
            "amount": close * 100_000.0,
        },
        index=dates,
    )


@pytest.fixture
def real_shape_api(simple_log, context):
    """内部行情/元数据全部使用 .SS/.SZ 键，模拟真实数据包。"""
    dates = pd.date_range("2024-01-02", periods=10, freq="D")
    stock_data = {SH_STOCK: _frame(dates), SZ_STOCK: _frame(dates, close=20.0)}
    metadata = pd.DataFrame(
        {
            "stock_name": ["浦发银行", "平安银行"],
            "security_type": ["stock", "stock"],
            "blocks": ["", ""],
            "listed_date": ["1999-11-10", "1991-04-03"],
        },
        index=[SH_STOCK, SZ_STOCK],
    )
    dc = SimpleNamespace(
        stock_data_dict=stock_data,
        stock_data_dict_1m={},
        benchmark_data={"000300.SS": _frame(dates, close=100.0)},
        valuation_dict={},
        fundamentals_dict={},
        exrights_dict={},
        stock_status={},
        stock_status_history={},
        stock_metadata=metadata,
        index_constituents={"20240105": {"000300.SS": [SH_STOCK, SZ_STOCK]}},
        adj_pre_cache={},
        adj_post_cache=None,
        dividend_cache={},
        trade_days=dates,
        listed_date_ts=None,
        de_listed_date_ts=None,
        _industry_index=None,
    )
    dc.get_industry_index = lambda: {}

    api = PtradeAPI(data_context=dc, context=context, log=simple_log)
    context.current_dt = pd.Timestamp("2024-01-08")
    # 停在 INITIALIZE：set_universe 只允许 INITIALIZE/BEFORE_TRADING_START/TICK_DATA，
    # 而生命周期状态机不允许从 HANDLE_DATA 回退。
    context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
    return api


class TestOfficialSuffixRules:
    def test_full_and_short_suffixes_are_both_official_inputs(self):
        assert canonical_code("600570.XSHG") == "600570.SS"
        assert canonical_code("000001.XSHE") == "000001.SZ"
        assert canonical_code("600570.SS") == "600570.SS"
        assert canonical_code("000001.SZ") == "000001.SZ"
        assert _normalize_code("000300.XSHG") == "000300.SS"

    def test_xbhs_is_index_suffix_not_shenzhen_stock_suffix(self):
        # 旧实现把 .XBHS 一律映射成 .SZ，导致 get_index_stocks('000300.XBHS') 静默返回空
        assert canonical_code("000300.XBHS") == "000300.SS"
        assert canonical_code("399101.XBHS") == "399101.SZ"
        assert code_candidates("000300.XBHS")[:3] == (
            "000300.XBHS",
            "000300.SS",
            "000300.SZ",
        )

    def test_board_codes_are_not_rewritten(self):
        # A01000 / DY1172 是行业、地域板块代码，不是行情标的
        for board in ("A01000.XBHS", "DY1172.XBHS", "710200.XBHS", "IF2312.CCFX"):
            assert canonical_code(board) == board
            assert code_candidates(board) == (board,)

    def test_sh_suffix_keeps_historical_contract_but_gets_candidates(self):
        assert canonical_code("000300.SH") == "000300.SH"
        assert "000300.SS" in code_candidates("000300.SH")

    def test_dual_key_dict_supports_both_official_writings(self):
        positions = DualKeyDict({"600570.SS": 100})
        assert positions["600570.XSHG"] == 100
        assert "600570.XSHG" in positions
        assert positions.get("600570.SH") == 100
        assert list(positions.keys()) == ["600570.SS"]


class TestAPIAcceptsOfficialLongSuffixes:
    def test_get_history_1d_long_suffix_equals_short_suffix(self, real_shape_api):
        api = real_shape_api
        short = api.get_history(3, "1d", "close", [SH_STOCK, SZ_STOCK])
        long = api.get_history(3, "1d", "close", [SH_LONG, SZ_LONG])
        assert not short.empty, "短后缀基线必须有数据，否则用例无效"
        pd.testing.assert_frame_equal(short, long)

    def test_get_price_count_long_suffix_equals_short_suffix(self, real_shape_api):
        api = real_shape_api
        short = api.get_price(SH_STOCK, end_date="2024-01-08", frequency="1d", count=3, fields=["close"])
        long = api.get_price(SH_LONG, end_date="2024-01-08", frequency="1d", count=3, fields=["close"])
        assert short is not None and not short.empty
        pd.testing.assert_frame_equal(short, long)

    def test_stock_info_and_name_are_not_silently_wrong(self, real_shape_api):
        api = real_shape_api
        assert api.get_stock_name([SH_LONG])[SH_LONG] == "浦发银行"
        info_long = api.get_stock_info([SH_LONG])
        assert info_long[SH_LONG]
        assert info_long[SH_LONG] == api.get_stock_info([SH_STOCK])[SH_STOCK]

    def test_check_limit_does_not_silently_report_not_limited(self, real_shape_api):
        api = real_shape_api
        assert api.check_limit([SH_LONG])[SH_LONG] == api.check_limit([SH_STOCK])[SH_STOCK]

    def test_get_index_stocks_accepts_official_xbhs_writing(self, real_shape_api):
        api = real_shape_api
        assert api.get_index_stocks("000300.SS", "20240108") == [SH_STOCK, SZ_STOCK]
        assert api.get_index_stocks("000300.XBHS", "20240108") == [SH_STOCK, SZ_STOCK]
        assert api.get_index_stocks("000300.XSHG", "20240108") == [SH_STOCK, SZ_STOCK]

    def test_set_universe_alias_normalizes_long_suffixes(self, real_shape_api):
        api = real_shape_api
        api.set_universe(stocks=[SH_LONG, SZ_LONG])
        assert SH_STOCK in api.active_universe
        assert SZ_STOCK in api.active_universe

    def test_position_and_data_container_accept_long_suffix_keys(self, real_shape_api):
        from simtradelab.ptrade.object import Data

        api = real_shape_api
        api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        context = api.context
        context.portfolio.add_position(SH_STOCK, 100, 10.0, context.current_dt)
        assert context.portfolio.positions[SH_LONG].amount == 100
        assert api.get_position(SH_LONG).amount == 100
        assert SH_LONG in api.get_positions([SH_LONG])

        # data['600000.XSHG'] 不走 API 方法，必须自己解析到内部 .SS 键
        bar_data = Data(context.current_dt, SimpleNamespace(stock_data_dict=api.data_context.stock_data_dict))
        assert bar_data[SH_LONG].stock == SH_STOCK
        assert bar_data[SH_LONG]._stock_df is api.data_context.stock_data_dict[SH_STOCK]


def test_strategy_file_with_official_long_suffixes_runs_end_to_end(simple_log, tmp_path):
    """完整模拟线上 PTrade 策略：代码里全用 XSHG，本地引擎应直接跑通并真实成交。

    修复前 order('600000.XSHG') 会静默返回 None（订单完全不下达），
    get_history 日线静默返回空，且不会抛任何异常。
    """
    from simtradelab.backtest.backtest_stats import StatsCollector
    from simtradelab.backtest.config import BacktestConfig
    from simtradelab.backtest.runner import BacktestRunner
    from simtradelab.ptrade.market_profile import get_market_profile
    from simtradelab.ptrade.strategy_engine import StrategyExecutionEngine

    dates = pd.DatetimeIndex(
        [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")]
    )
    benchmark_dates = dates.insert(0, dates[0] - pd.Timedelta(days=1))
    runner = BacktestRunner()
    runner._profile = get_market_profile("CN")
    runner.stock_data_dict = {SH_STOCK: _frame(dates)}
    runner.stock_data_dict_1m = None
    runner.valuation_dict = {}
    runner.fundamentals_dict = {}
    runner.exrights_dict = {}
    runner.benchmark_data = {"000300.SS": _frame(benchmark_dates, close=100.0)}
    runner.stock_metadata = pd.DataFrame()
    runner.index_constituents = {}
    runner.stock_status_history = {}
    runner.adj_pre_cache = {}
    runner.adj_post_cache = {}
    runner.dividend_cache = {}
    runner.trade_days = dates

    backtest_config = BacktestConfig(
        strategy_name="code_suffix_compat",
        start_date=dates[0],
        end_date=dates[-1] + pd.Timedelta(days=1),
        initial_capital=100_000.0,
        frequency="1d",
        market="CN",
        t_plus_1=True,
    )
    context, api = runner._initialize_context(backtest_config, dates[0], simple_log)
    collector = StatsCollector()
    api.stats_collector = collector
    engine = StrategyExecutionEngine(
        context=context, api=api, stats_collector=collector, log=simple_log, frequency="1d"
    )

    strategy_file = tmp_path / "long_suffix_strategy.py"
    strategy_file.write_text(
        """
def initialize(context):
    g.security = '600000.XSHG'
    g.benchmark = '000300.XSHG'
    g.flag = False
    g.rows = []
    g.bar_close = None
    g.order_result = None
    set_universe(g.security)
    set_benchmark(g.benchmark)

def handle_data(context, data):
    long_hist = get_history(2, '1d', 'close', ['600000.XSHG'])
    short_hist = get_history(2, '1d', 'close', ['600000.SS'])
    g.rows.append((len(long_hist), len(short_hist)))
    g.bar_close = data[g.security].close
    if not g.flag:
        g.order_result = order(g.security, 100, limit_price=10.0)
        g.flag = True
""",
        encoding="utf-8",
    )
    engine.load_strategy_from_file(str(strategy_file))
    assert engine.run_backtest(dates) is True

    assert context.g.rows, "handle_data 必须被执行"
    assert all(long_rows == short_rows for long_rows, short_rows in context.g.rows), (
        f"长短后缀取数结果必须逐日一致: {context.g.rows}"
    )
    assert context.g.rows[-1][0] > 0, "长后缀 get_history(1d) 不应静默返回空"
    assert context.g.bar_close == pytest.approx(10.0), "data['600000.XSHG'] 应解析到内部行情"
    assert context.g.order_result is not None, "长后缀 order 不应静默不下单"
    assert context.portfolio.positions[SH_LONG].amount == 100
    assert SH_STOCK in context.portfolio.positions
