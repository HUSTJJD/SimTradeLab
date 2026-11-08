# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
ptrade API 模拟层

模拟ptrade平台的所有API函数，用于本地回测

"""

from __future__ import annotations

import bisect
import calendar
import json
from pathlib import Path
import traceback
import weakref
from collections import OrderedDict
from collections.abc import Callable
from datetime import date as datetime_date
from functools import wraps
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from cachetools import LRUCache

from simtradelab.i18n import t
from simtradelab.ptrade.object import Position
from simtradelab.utils.perf import timer

from ..utils.paths import get_strategies_path
from . import storage
from .cache_manager import cache_manager
from .code_compat import (
    canonical_code,
    code_candidates,
    install_code_compat,
)
from .broker_profile import (
    is_api_supported_for_broker,
    needs_broker_support_guard,
    normalize_broker_profile,
)
from .config_manager import config
from .lifecycle_config import _ALL_PHASES_FROZENSET, API_ALLOWED_PHASES_LOOKUP
from .lifecycle_controller import PTradeLifecycleError
from .order_processor import OrderProcessor

# PTrade 官方尾缀规则集中在 code_compat 模块（见该模块顶部官方文档依据）。
# 旧实现把 .XBHS 一律映射成 .SZ 是错的：指数/板块尾缀不是交易所尾缀，
# 且本地 000300.SS / 000905.SZ / 399370.SZ 并存，静态映射必然猜错。
# 现在统一改为「按候选解析到数据里真实存在的键」。

# 兼容各券商文档中的参数名差异（alias -> canonical）
_API_KWARG_ALIASES = {
    "buy_open": {"security": "contract"},
    "sell_open": {"security": "contract"},
    "sell_close": {"security": "contract"},
    "buy_close": {"security": "contract"},
    "option_buy_open": {"security": "contract"},
    "option_sell_open": {"security": "contract"},
    "option_sell_close": {"security": "contract"},
    "option_buy_close": {"security": "contract"},
    "set_benchmark": {"benchmark": "sids"},
    "set_universe": {"stocks": "security_list", "securities": "security_list"},
    "convert_position_from_csv": {"file_path": "path"},
    "get_stock_blocks": {"stock": "stock_code", "security_list": "stock_code"},
    "set_margin_rate": {"security": "transaction_code", "rate": "margin_rate"},
    "get_margin_rate": {"security": "transaction_code"},
}

# 兼容文档中可传但本地回测不使用的扩展参数
_API_OPTIONAL_DROP_KWARGS = {
    "run_interval": frozenset(["interval_timer_ranges"]),
}

_FREQ_ALIASES = {
    "daily": "1d",
    "weekly": "1w",
    "monthly": "mo",
    "quarter": "1q",
    "yearly": "1y",
}

_MINUTE_FREQ_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
    "120m": 120,
}

_PERIOD_FREQ_RULE = {
    "1w": "W-FRI",
    "mo": "M",
    "1q": "Q",
    "1y": "Y",
}

_VALID_PRICE_FREQUENCIES = frozenset(["1d", *_MINUTE_FREQ_MINUTES, *_PERIOD_FREQ_RULE])


def _ptrade_datetime_int64(index) -> np.ndarray:
    """PTrade is_dict structured array 首列：YYYYMMDDHHMM 的 int64。

    依据 api_doc.txt get_price/get_history 返回示例：
    ``array([(201706010931, 37.1, ...), ...])`` / ``(202309220931, ...)``。
    """
    dt = pd.DatetimeIndex(index)
    return (
        dt.year.to_numpy(dtype="int64") * 10**8
        + dt.month.to_numpy(dtype="int64") * 10**6
        + dt.day.to_numpy(dtype="int64") * 10**4
        + dt.hour.to_numpy(dtype="int64") * 10**2
        + dt.minute.to_numpy(dtype="int64")
    )


class _TradeDaysArray(np.ndarray):
    """numpy.ndarray with list-like truthiness for legacy strategy compatibility."""

    def __new__(cls, values: list[str]):
        return np.asarray(values, dtype=object).view(cls)

    def __bool__(self) -> bool:
        return len(self) > 0


class _PTradeSeries(pd.Series):
    """Series returned by market-data APIs with PTrade-style positional int access."""

    @property
    def _constructor(self):
        return _PTradeSeries

    @property
    def _constructor_expanddim(self):
        return _PTradeDataFrame

    def __getitem__(self, key):
        if isinstance(key, (int, np.integer)) and not isinstance(key, bool):
            fallback = getattr(self.index, "_should_fallback_to_positional", None)
            if fallback is True:
                return self.iloc[int(key)]
        return super().__getitem__(key)


class _PTradeDataFrame(pd.DataFrame):
    """DataFrame returned by market-data APIs."""

    @property
    def _constructor(self):
        return _PTradeDataFrame

    @property
    def _constructor_sliced(self):
        return _PTradeSeries


def _normalize_code(code: str) -> str:
    """把官方全称尾缀规范化为引擎内部简称尾缀（见 code_compat 的官方依据）。"""
    return canonical_code(code)


_REPORT_MONTH = {3: "1", 6: "2", 9: "3", 12: "4"}


def select_report_rows(stocks, data_dict, fields, query_ts, start_year, end_year, report_types):
    """从已经加载的财务表里按年、按报告类型取多行。不访问网络。"""
    if report_types is None:
        types = None
    elif isinstance(report_types, (list, tuple, set)):
        types = {str(x) for x in report_types}
    else:
        types = {part.strip() for part in str(report_types).split(",") if part.strip()}
    y0 = int(start_year) if start_year is not None else 1900
    y1 = int(end_year) if end_year is not None else 2999
    columns = list(dict.fromkeys(["publ_date", "end_date", *list(fields or [])]))
    rows = []
    query_ts = pd.Timestamp(query_ts)
    for stock in stocks:
        df = data_dict.get(stock) if data_dict is not None else None
        if df is None or len(df) == 0:
            continue
        work = df
        if "publ_date" in work.columns:
            pub = pd.to_datetime(work["publ_date"], errors="coerce")
            work = work.loc[pub.isna() | (pub <= query_ts)]
        period_col = "end_date" if "end_date" in work.columns else ("date" if "date" in work.columns else None)
        if period_col is None:
            continue
        periods = pd.to_datetime(work[period_col], errors="coerce")
        mask = periods.dt.year.between(y0, y1)
        if types:
            mask = mask & periods.dt.month.map(_REPORT_MONTH).isin(types)
        flag = mask.fillna(False).to_numpy()
        picked = work.iloc[flag]
        period_values = periods.to_numpy()[flag]
        for idx in range(len(picked)):
            src = picked.iloc[idx]
            rec = {"secu_code": stock, "end_date": period_values[idx]}
            for field in columns:
                if field == "end_date":
                    continue
                rec[field] = src[field] if field in picked.columns else None
            rows.append(rec)
    out = pd.DataFrame(rows, columns=["secu_code", *columns])
    if out.empty:
        return out
    return out.set_index("secu_code")


def _to_ptrade_long_suffix(code: str) -> str:
    suffix_map = {
        ".XSHG": ".XSHG",
        ".XSHE": ".XSHE",
        ".SS": ".XSHG",
        ".SH": ".XSHG",
        ".SZ": ".XSHE",
    }
    for suffix, ptrade_suffix in suffix_map.items():
        if code.endswith(suffix):
            return code[: -len(suffix)] + ptrade_suffix
    return code


def _round2_scalar(fv: float) -> float:
    """Round single float to 2dp matching Ptrade's behavior.

    Default: round(fv, 2) — Python banker's rounding.
    TypeA (nearest float64 to .XX5 is from below, str shows '.XX5'):
      non-dyadic .XX5: float is BELOW exact midpoint, Python rounds DOWN (banker's to even),
      but Ptrade rounds UP (round half up) → force UP to .XX+1.
    Anti-TypeA (+1 ULP above nearest, str shows '.XX5000...1'):
      non-dyadic .XX5: float is ABOVE exact midpoint by 1 ULP, Python rounds UP,
      but Ptrade rounds DOWN (treats as exactly .XX5 → banker's even) → force DOWN to .XX.
    Upper bound < 0.005: at magnitudes ~35-60, float64 subtraction fv-rd overshoots 0.005
      due to ULP error. Ptrade skips TypeA when abs(diff)>=0.005 (treats as standard rounding).
    """
    rd = round(fv, 2)
    diff = fv - rd
    if 0.00499 < abs(diff) < 0.005:
        s = str(fv)
        dot = s.find(".")
        if dot >= 0:
            frac = s[dot + 1 :]
            if len(frac) >= 3 and frac[2] == "5" and int(frac[:3]) not in {125, 375, 625, 875}:
                if diff > 0:  # TypeA: below → UP
                    return round(round(rd + 0.01, 2), 2)
                # anti-TypeA: +1ULP above .XX5, EVEN → DOWN
                # 排除 frac[1]='0'（X.X05+ε）：该情况 Ptrade 给标准 UP（如 300382.SZ 20.205+ε）
                if len(frac) > 3 and int(frac[1]) % 2 == 0 and frac[1] != "0":
                    return round(rd - 0.01, 2)
    return rd


def _round2(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    result = np.round(values, 2)
    diff = np.abs(values - result)
    boundary_indices = np.flatnonzero((diff > 0.00499) & (diff < 0.0050000000001))
    for i in boundary_indices:
        result[i] = _round2_scalar(float(values[i]))
    return result


def _has_typeab(values: np.ndarray) -> bool:
    """Return True if _round2 differs from round() for any value (TypeA/anti-TypeA fires)."""
    values = np.asarray(values, dtype=np.float64)
    rounded = np.round(values, 2)
    diff = np.abs(values - rounded)
    boundary_indices = np.flatnonzero((diff > 0.00499) & (diff < 0.0050000000001))
    for i in boundary_indices:
        fv = float(values[i])
        if _round2_scalar(fv) != round(fv, 2):
            return True
    return False


def _compute_hl_adj(adj_b: np.ndarray, h_raw: np.ndarray, l_raw: np.ndarray):
    """计算前复权 high/low，自动决定是否使用 float64 bypass。

    bypass 条件（4个同时满足）：
      1. float64 adj >= rounded adj（系统性正偏置）
      2. 窗口内无 TypeA/anti-TypeA 触发
      3. float64 range ≠ rounded range（存在实际 ULP 污染）
      4. adj_b_bias > 0.003（.XX4/.XX6，排除 .XX2/.XX8 如 300441.SZ）
    满足时返回 float64 adj，否则返回 _round2 结果。
    """
    adj_h = h_raw + adj_b
    adj_l = l_raw + adj_b
    adj_h_r = _round2(adj_h)
    adj_l_r = _round2(adj_l)
    if np.all(adj_h >= adj_h_r) and np.all(adj_l >= adj_l_r) and not _has_typeab(adj_h) and not _has_typeab(adj_l):
        range_f = float(adj_h.max() - adj_l.min())
        range_r = float(adj_h_r.max() - adj_l_r.min())
        if range_f != range_r:
            adj_b_bias = abs(float(adj_b[-1]) - round(float(adj_b[-1]), 2))
            if adj_b_bias > 0.003:
                return adj_h, adj_l  # float64 bypass
    return adj_h_r, adj_l_r


def _datetime_index_ns(index: pd.DatetimeIndex) -> np.ndarray:
    """Return int64 nanoseconds, independent of pandas' stored datetime resolution."""
    return index.to_numpy(dtype="datetime64[ns]").view("i8")


def _build_date_index(index: pd.DatetimeIndex) -> tuple[dict[int, int], np.ndarray]:
    """Build the shared fast date lookup contract used by all daily data paths."""
    idx_i8 = _datetime_index_ns(index)
    return {int(date_value): idx for idx, date_value in enumerate(idx_i8)}, idx_i8


def validate_lifecycle(func: Callable) -> Callable:
    """生命周期验证装饰器

    优化策略:
    - all-phase API (get_history, get_price 等) 直接返回原函数，零包装开销
    - 受限 API 只做 frozenset in 检查，无 RLock / Pydantic 对象
    """
    api_name = func.__name__
    allowed_phases = API_ALLOWED_PHASES_LOOKUP.get(api_name, _ALL_PHASES_FROZENSET)
    need_broker_guard = needs_broker_support_guard(api_name)
    has_kwarg_compat = api_name in _API_KWARG_ALIASES or api_name in _API_OPTIONAL_DROP_KWARGS

    # all-phase 且无券商差异：直接返回原函数，无任何包装
    if allowed_phases is _ALL_PHASES_FROZENSET and not need_broker_guard and not has_kwarg_compat:
        return func

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if kwargs:
            kwargs = dict(kwargs)
            alias_map = _API_KWARG_ALIASES.get(api_name, {})
            for old_name, new_name in alias_map.items():
                if old_name in kwargs and new_name not in kwargs:
                    kwargs[new_name] = kwargs.pop(old_name)
            for k in _API_OPTIONAL_DROP_KWARGS.get(api_name, frozenset()):
                kwargs.pop(k, None)

        if need_broker_guard:
            profile = normalize_broker_profile(getattr(self, "broker_profile", getattr(self.context, "broker_profile", "auto")))
            if not is_api_supported_for_broker(api_name, profile):
                raise NotImplementedError(t("api.broker_api_unsupported", profile=profile, api_name=api_name))

        controller = self.context._lifecycle_controller if self.context else None
        if controller and controller._current_phase:
            if controller._current_phase.value not in allowed_phases:
                raise PTradeLifecycleError(
                    f"API '{api_name}' cannot be called in phase "
                    f"'{controller._current_phase.value}'. Allowed: {allowed_phases}"
                )
        return func(self, *args, **kwargs)

    return wrapper


class PtradeAPI:
    """ptrade API模拟器（面向对象封装）"""

    def __init__(self, data_context: Any, context: Any, log: Any) -> None:
        """初始化API模拟器

        Args:
            data_context: DataContext数据上下文对象
            context: Context上下文对象
            log: 日志对象
        """
        self.data_context = data_context
        self.context = context
        self.stats_collector = None
        self.log = log
        self.lot_size = 100
        self.has_price_limit = True
        self.broker_profile = normalize_broker_profile(getattr(context, "broker_profile", "auto"))
        self.context.broker_profile = self.broker_profile
        self.context.g.broker_profile = self.broker_profile

        # 股票池管理
        # 保序股票池（api_doc.txt: set_universe 决定 get_history 默认
        # security_list，返回顺序应与传入顺序一致）；用 list 而非 set。
        self.active_universe: list = []

        # 订单处理器（延迟初始化）
        self._order_processor: Optional[OrderProcessor] = None

        # 缓存 - 使用统一缓存管理器
        self._stock_status_cache = LRUCache(maxsize=50000)
        self._stock_date_index: dict[str, tuple[dict[int, int], np.ndarray]] = {}
        self._prebuilt_index: bool = False
        self._sorted_status_dates: Optional[list[str]] = None
        self._daily_tasks: list[tuple[Callable, str]] = []  # (func, time_str)
        self._history_cache = LRUCache(maxsize=config.cache.history_cache_size)
        self._history_cache_date: Optional[pd.Timestamp] = None
        self._fundamentals_cache = LRUCache(maxsize=500)
        self._sorted_index_dates: Optional[list[str]] = None
        self._adj_alignment_cache: dict[tuple[object, ...], bool] = {}
        # DataFrame 列的 numpy 视图缓存：pandas 每次 df[col] 都要构造 Series，
        # 多股票循环里这是主要开销之一。每个 DataFrame 一个条目，附弱引用防止 id 复用读错数据。
        self._values_cache: dict[tuple[int, object], tuple[weakref.ReferenceType, dict[str, np.ndarray]]] = {}
        getattr(self.data_context, "register_api", lambda _: None)(self)
        # 实盘模拟: 订单/成交回调队列
        self._pending_order_callbacks: list[dict] = []
        self._pending_trade_callbacks: list[dict] = []
        self._future_margin_rates: dict[str, float] = {}
        self._future_commission_ratio: Optional[float] = None

    @property
    def order_processor(self) -> OrderProcessor:
        """获取订单处理器（延迟初始化）"""
        if self._order_processor is None:
            self._order_processor = OrderProcessor(
                self.context,
                self.data_context,
                self.get_stock_date_index,
                self.log,
                stats_collector=self.stats_collector,
            )
            self._order_processor.lot_size = self.lot_size
        return self._order_processor

    def _unsupported_api(self, api_name: str, category: str = "接口") -> None:
        """Raise a consistent error for PTrade APIs not supported by local backtesting."""
        _ = category
        raise NotImplementedError(t("api.local_backtest_api_unsupported", api_name=api_name))

    def prebuild_date_index(self, stocks: Optional[list[str]] = None) -> None:
        """预构建股票日期索引（显著提升性能）"""
        if self._prebuilt_index and stocks is None:
            return

        source = self.data_context.stock_data_dict
        # 测试夹具/最小化上下文可能用哨兵对象或普通 dict，缺少 keys() 时
        # 直接退化为空列表，避免为了一个可选的性能预热把初始化打断。
        target_stocks = (
            stocks
            if stocks
            else list(source.keys()) if hasattr(source, "keys") else []
        )
        target_stocks = [stock for stock in target_stocks if stock not in self._stock_date_index]
        print(t("api.prebuilding", count=len(target_stocks)))

        for i, stock in enumerate(target_stocks):
            if (i + 1) % 1000 == 0:
                print(t("api.prebuild_progress", done=i + 1, total=len(target_stocks)))
            try:
                stock_df = self.data_context.stock_data_dict[stock]
                if isinstance(stock_df, pd.DataFrame) and isinstance(stock_df.index, pd.DatetimeIndex):
                    self._stock_date_index[stock] = _build_date_index(stock_df.index)
            except Exception:
                pass

        if stocks is None:
            self._prebuilt_index = True
        print(t("api.prebuild_done", count=len(self._stock_date_index)))

    def get_stock_date_index(self, stock: str) -> tuple[dict[int, int], np.ndarray]:
        """获取股票日期索引，返回 (date_dict, sorted_i8) 元组

        date_dict: {int64_nanoseconds: row_index} — 用 ts.value 查找
        sorted_i8: numpy int64 数组 — 用 .searchsorted(ts.value) 做二分查找
        """
        if stock not in self._stock_date_index:
            stock_df = None
            if stock in self.data_context.stock_data_dict:
                stock_df = self.data_context.stock_data_dict[stock]
            elif stock in self.data_context.benchmark_data:
                stock_df = self.data_context.benchmark_data[stock]

            if (
                stock_df is not None
                and isinstance(stock_df, pd.DataFrame)
                and isinstance(stock_df.index, pd.DatetimeIndex)
            ):
                # int64 nanoseconds 作为 dict key，避免 pd.Timestamp 构造开销
                self._stock_date_index[stock] = _build_date_index(stock_df.index)
            else:
                self._stock_date_index[stock] = ({}, np.array([], dtype="i8"))
        return self._stock_date_index[stock]

    def _resolve_daily_index(self, stock: str, stock_df: pd.DataFrame, query_dt: pd.Timestamp) -> Optional[int]:
        """解析日线索引，支持分钟时间戳对齐到交易日。

        规则：
        - 优先精确匹配（兼容原有行为）
        - 失败时对齐到 query_dt.normalize()，并向前取最近可用交易日
        """
        date_dict, sorted_i8 = self.get_stock_date_index(stock)
        if not len(sorted_i8):
            return None

        exact_idx = date_dict.get(query_dt.value)
        if exact_idx is not None:
            return exact_idx

        day_i8 = query_dt.normalize().value
        day_idx = date_dict.get(day_i8)
        if day_idx is not None:
            return day_idx

        pos = sorted_i8.searchsorted(day_i8, side="right") - 1
        if pos < 0:
            return None
        return int(pos)

    def _apply_adj_factors(self, stock_df: pd.DataFrame, stock: str, fq: str) -> pd.DataFrame:
        """对DataFrame应用复权因子（向量化）

        Args:
            stock_df: 股票数据DataFrame
            stock: 股票代码
            fq: 复权类型 ('pre'前复权, 'post'后复权)

        Returns:
            复权后的DataFrame（copy），无复权因子时返回原DataFrame
        """
        if fq == "pre":
            adj_cache = self.data_context.adj_pre_cache
        elif fq == "post":
            adj_cache = self.data_context.adj_post_cache
        else:
            return stock_df

        if not adj_cache or stock not in adj_cache:
            return stock_df

        adj_factors = adj_cache[stock]
        idx = stock_df.index
        # api_doc.txt: fq (pre/post/dypre) 对 1m~1y 全部频率均有效，
        # 无「分钟数据不支持复权」的限制。分钟 bar 按所属交易日取因子。
        intraday = isinstance(idx, pd.DatetimeIndex) and not idx.equals(idx.normalize())
        if intraday:
            aligned = adj_factors.reindex(idx.normalize()).ffill()
            adj_a = pd.Series(aligned["adj_a"].to_numpy(), index=idx)
            adj_b = pd.Series(aligned["adj_b"].to_numpy(), index=idx)
            valid = adj_a.notna().to_numpy()
            if not valid.any():
                return stock_df
            common_idx = idx[valid]
            adj_a = adj_a.loc[common_idx]
            adj_b = adj_b.loc[common_idx]
        else:
            common_idx = idx.intersection(adj_factors.index)
            if len(common_idx) == 0:
                return stock_df
            adj_a = adj_factors.loc[common_idx, "adj_a"]
            adj_b = adj_factors.loc[common_idx, "adj_b"]

        adjusted_df = stock_df.copy()
        price_cols = ["open", "high", "low", "close"]
        high_low_adjusted = {}
        if (
            fq == "pre"
            and np.all(adj_a.to_numpy() == 1.0)
            and "high" in adjusted_df.columns
            and "low" in adjusted_df.columns
        ):
            high_low_adjusted["high"], high_low_adjusted["low"] = _compute_hl_adj(
                adj_b.to_numpy(),
                adjusted_df.loc[common_idx, "high"].to_numpy(),
                adjusted_df.loc[common_idx, "low"].to_numpy(),
            )

        # 前/后复权公式相同：adj_a * 未复权价 + adj_b
        for col in price_cols:
            if col in high_low_adjusted:
                adjusted_df.loc[common_idx, col] = high_low_adjusted[col]
                continue
            adjusted = adj_a * adjusted_df.loc[common_idx, col] + adj_b
            if fq == "pre":
                adjusted = _round2(adjusted.to_numpy(dtype=float))
            adjusted_df.loc[common_idx, col] = adjusted

        return adjusted_df

    # ==================== 基础API ====================

    def get_research_path(self) -> str:
        """返回研究目录路径（基于 strategies 同级目录）"""
        p = get_strategies_path().parent / "research"
        p.mkdir(parents=True, exist_ok=True)
        return str(p) + "/"

    def create_dir(self, path: str) -> None:
        """创建目录（如果不存在）"""
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)

    def get_Ashares(self, date: str | None = None) -> list[str]:
        """返回A股代码列表，支持历史查询"""
        if date is None:
            target_date = self.context.current_dt
        else:
            target_date = pd.Timestamp(date)

        if self.data_context.stock_metadata.empty:
            return list(self.data_context.stock_data_dict.keys())

        # 使用预解析的 Timestamp 列（避免每次调用都解析日期字符串）
        frame = self.data_context.stock_metadata
        if "security_type" in frame.columns:
            kind = frame["security_type"].fillna("stock").astype(str)
            frame = frame[kind.isin(["", "stock", "nan"])]
        if frame is self.data_context.stock_metadata and self.data_context.listed_date_ts is not None:
            listed = self.data_context.listed_date_ts <= target_date
        else:
            listed = pd.to_datetime(frame["listed_date"], errors="coerce") <= target_date
        if frame is self.data_context.stock_metadata and self.data_context.de_listed_date_ts is not None:
            not_delisted = (frame["de_listed_date"] == "2900-01-01") | (
                self.data_context.de_listed_date_ts > target_date
            )
        else:
            not_delisted = (frame["de_listed_date"] == "2900-01-01") | (
                pd.to_datetime(frame["de_listed_date"], errors="coerce") > target_date
            )
        return frame[listed & not_delisted].index.tolist()

    def _instrument_frame(self) -> pd.DataFrame | None:
        frame = getattr(self.data_context, "stock_metadata", None)
        if frame is None or frame.empty:
            return None
        return frame

    def _active_symbols(self, mask: pd.Series, date: str | None) -> list[str]:
        """按上市日、退市日筛出仍在市的代码。"""
        frame = self._instrument_frame()
        if frame is None:
            return []
        target = self.context.current_dt if date is None else pd.Timestamp(date)
        chosen = frame.loc[mask.reindex(frame.index, fill_value=False)]
        if chosen.empty:
            return []
        if "listed_date" in chosen.columns:
            listed = pd.to_datetime(chosen["listed_date"], errors="coerce") <= target
        else:
            listed = pd.Series(True, index=chosen.index)
        if "de_listed_date" in chosen.columns:
            alive = (chosen["de_listed_date"].astype(str) == "2900-01-01") | (
                pd.to_datetime(chosen["de_listed_date"], errors="coerce") > target
            )
        else:
            alive = pd.Series(True, index=chosen.index)
        return sorted(chosen.index[listed.fillna(False) & alive.fillna(False)].astype(str).tolist())

    def _names_and_codes(self, frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        if "stock_name" in frame.columns:
            names = frame["stock_name"].fillna("").astype(str)
        else:
            names = pd.Series("", index=frame.index)
        codes = frame.index.to_series().astype(str).str.split(".").str[0]
        return names, codes

    @validate_lifecycle
    def get_reits_list(self, date: str | None = None) -> list[str]:
        """获取基础设施公募REITs基金代码列表。"""
        frame = self._instrument_frame()
        if frame is None:
            return []
        names, codes = self._names_and_codes(frame)
        mask = names.str.upper().str.contains("REIT") | codes.str.startswith(("180", "508"))
        return self._active_symbols(mask, date)

    def get_trade_days(
        self, start_date: str | None = None, end_date: str | None = None, count: int | None = None
    ) -> np.ndarray:
        """获取指定范围交易日列表

        Args:
            start_date: 开始日期（与count二选一）
            end_date: 结束日期（默认当前回测日期）
            count: 往前count个交易日（与start_date二选一）
        """
        if self.data_context.trade_days is None:
            raise RuntimeError("交易日历数据未加载")
        all_trade_days = self.data_context.trade_days

        if end_date is None:
            end_dt = self.context.current_dt
        else:
            end_dt = pd.Timestamp(end_date)

        # 交易日历为已排序 DatetimeIndex：用 searchsorted 定位 <= end_dt 的
        # 右边界，位置切片与全量布尔掩码逐位相同，避免构造 O(n) 掩码数组。
        end_pos = all_trade_days.searchsorted(end_dt, side='right')
        valid_days = all_trade_days[:end_pos]

        if count is not None:
            if start_date is not None:
                raise ValueError("start_date和count不能同时使用")
            # 找到end_date的位置
            if len(valid_days) == 0:
                return _TradeDaysArray([])
            # 往前取count个交易日（包含end_date）
            trade_days = valid_days[-count:]
        else:
            # 使用start_date和end_date范围
            trade_days = valid_days
            if start_date is not None:
                start_dt = pd.Timestamp(start_date)
                start_pos = trade_days.searchsorted(start_dt, side='left')
                trade_days = trade_days[start_pos:]

        return _TradeDaysArray([d.strftime("%Y-%m-%d") for d in trade_days])

    def get_all_trades_days(self, date: str | None = None) -> np.ndarray:
        """获取某日期之前的所有交易日列表

        Args:
            date: 截止日期（默认当前回测日期）
        """
        return self.get_trade_days(end_date=date)

    def get_trading_day(self, day: int = 0) -> Optional[datetime_date]:
        """获取当前时间数天前或数天后的交易日期

        Args:
            day: 偏移天数（正数向后，负数向前，0表示当天或上一交易日，默认0）

        Returns:
            datetime.date类型交易日对象
        """
        base_date = self.context.current_dt

        # 优先使用独立的交易日历数据
        if hasattr(self.data_context, "trade_days") and self.data_context.trade_days is not None:
            all_trade_days = self.data_context.trade_days
        elif "000300.SS" in self.data_context.benchmark_data:
            # 回退：从 benchmark_data 获取
            all_trade_days = self.data_context.benchmark_data["000300.SS"].index
        else:
            raise RuntimeError("交易日历数据未加载")

        if base_date in all_trade_days:
            base_idx = all_trade_days.get_loc(base_date)
        else:
            if self.broker_profile == "guosheng" and day == 0:
                # >= base_date 的第一个位置（side='left'），与布尔掩码逐位相同
                pos = all_trade_days.searchsorted(base_date, side='left')
                base_idx = pos if pos < len(all_trade_days) else len(all_trade_days)
            else:
                # <= base_date 的右边界；最后一个可用交易日即 pos-1
                pos = all_trade_days.searchsorted(base_date, side='right')
                base_idx = pos - 1 if pos > 0 else 0

        target_idx = base_idx + day

        if target_idx < 0 or target_idx >= len(all_trade_days):
            return None

        return pd.Timestamp(all_trade_days[target_idx]).date()

    @validate_lifecycle
    def get_trading_day_by_date(self, query_date: str, day: int = 0) -> Optional[datetime_date]:
        """按指定日期获取交易日

        Returns:
            datetime.date类型交易日对象
        """
        if self.data_context.trade_days is None:
            raise RuntimeError("交易日历数据未加载")

        all_trade_days = self.data_context.trade_days
        base_date = pd.Timestamp(query_date)

        if base_date in all_trade_days:
            base_idx = all_trade_days.get_loc(base_date)
        else:
            pos = all_trade_days.searchsorted(base_date, side='right')
            if pos == 0:
                return None
            base_idx = pos - 1

        target_idx = base_idx + day
        if target_idx < 0 or target_idx >= len(all_trade_days):
            return None
        return pd.Timestamp(all_trade_days[target_idx]).date()

    @validate_lifecycle
    def get_market_list(self) -> pd.DataFrame:
        """获取市场列表（回测简化版）"""
        # api_doc.txt get_market_list() 官方返回示例（QTV202401.05.021：
        # 仅返回系统支持的四个市场）：SS / SZ / CSI / XBHS。
        # XSHG/XSHE/CCFX/XSHO 是旧版别的口径，不是国泰海通客户端返回值。
        return pd.DataFrame(
            [
                {"finance_mic": "SS", "finance_name": "上海证券交易所"},
                {"finance_mic": "SZ", "finance_name": "深圳证券交易所"},
                {"finance_mic": "CSI", "finance_name": "中证指数"},
                {"finance_mic": "XBHS", "finance_name": "沪深板块"},
            ]
        )

    @validate_lifecycle
    def get_market_detail(self, finance_mic: str) -> pd.DataFrame:
        """获取市场详情（回测简化版）"""
        finance_mic = (finance_mic or "").upper()
        if finance_mic in ("XSHG", "SS"):
            suffix = ".SS"
        elif finance_mic in ("XSHE", "SZ"):
            suffix = ".SZ"
        else:
            suffix = None

        rows = []
        stocks = list(self.data_context.stock_data_dict.keys())
        for code in stocks[:500]:
            if suffix and not code.endswith(suffix):
                continue
            rows.append(
                {
                    "hq_type_code": "EQUITY",
                    "prod_code": code,
                    "prod_name": self.get_stock_name(code).get(code),
                    "trade_time_rule": 0,
                }
            )
            if len(rows) >= 200:
                break
        return pd.DataFrame(rows)

    # ==================== 基本面API ====================

    # 定义字段所属表的映射
    FUNDAMENTAL_TABLES = {
        "valuation": [
            "trading_day",
            "total_value",
            "float_value",
            "naps",
            "pcf",
            "secu_abbr",
            "secu_code",
            "ps",
            "ps_ttm",
            "pe_ttm",
            "a_shares",
            "a_floats",
            "pe_dynamic",
            "pe_static",
            "b_floats",
            "b_shares",
            "h_shares",
            "total_shares",
            "turnover_rate",
            "dividend_ratio",
            "pb",
            "roe",
        ],
        "profit_ability": [
            "secu_code",
            "secu_abbr",
            "publ_date",
            "end_date",
            "roe",
            "roa",
            "gross_income_ratio",
            "net_profit_ratio",
            "roe_ttm",
            "roa_ttm",
            "gross_income_ratio_ttm",
            "net_profit_ratio_ttm",
        ],
        "growth_ability": [
            "secu_code",
            "secu_abbr",
            "publ_date",
            "end_date",
            "operating_revenue_grow_rate",
            "net_profit_grow_rate",
            "total_asset_grow_rate",
            "basic_eps_yoy",
            "np_parent_company_yoy",
            "np_parent_company_cut_yoy",
            "oper_cash_ps_grow_rate",
        ],
        "operating_ability": [
            "secu_code",
            "secu_abbr",
            "publ_date",
            "end_date",
            "total_asset_turnover_rate",
            "current_assets_turnover_rate",
            "accounts_receivables_turnover_rate",
            "inventory_turnover_rate",
            "turnover_rate",
        ],
        "debt_paying_ability": [
            "secu_code",
            "secu_abbr",
            "publ_date",
            "end_date",
            "current_ratio",
            "quick_ratio",
            "debt_equity_ratio",
            "interest_cover",
            "roic",
            "roa_ebit_ttm",
        ],
    }
    FUNDAMENTAL_TABLE_ALIASES = {
        "balance_statement",
        "income_statement",
        "cashflow_statement",
        "eps",
    }
    FUNDAMENTAL_DEFAULT_FIELDS = ["secu_code", "secu_abbr", "publ_date", "end_date"]

    @staticmethod
    def _latest_row_idx_by_date(df: pd.DataFrame, date_column: str, query_ts: pd.Timestamp) -> int | None:
        """返回date_column不晚于query_ts的最后一行位置。"""
        dates = pd.to_datetime(df[date_column], errors="coerce")
        valid_positions = np.flatnonzero((dates <= query_ts).to_numpy())
        if len(valid_positions) == 0:
            return None
        return int(valid_positions[-1])

    @staticmethod
    def _date_value_from_row(df: pd.DataFrame, idx: int, *columns: str):
        """从字段列或日期索引中取行日期，用于对齐 PTrade 对外字段名。"""
        for column in columns:
            if column in df.columns:
                return df[column].values[idx]
        if isinstance(df.index, pd.DatetimeIndex):
            return df.index[idx]
        return None

    @timer()
    def get_fundamentals(
        self,
        security: str | list[str],
        table: str,
        fields: str | list[str] | None = None,
        date: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
        report_types: str | list[str] | None = None,
        date_type: str | None = None,
        merge_type: str | None = None,
        is_dataframe: bool | None = None,
    ) -> pd.DataFrame | dict:
        """获取基本面数据（优化版：增量缓存）

        重要：对于fundamentals表，使用publ_date（公告日期）进行过滤，而非end_date（报告期）
        这样可以避免未来函数（look-ahead bias）

        Args:
            security: 股票代码或股票代码列表
            table: 表名
            fields: 字段列表
            date: 查询日期（默认为回测当前日期）
        """
        profile = self.broker_profile
        _ = merge_type
        # 券商参数差异
        if profile == "guosheng":
            if is_dataframe is not None:
                raise ValueError(t("api.gf_unsupported_param_by_broker", profile=profile, param_name="is_dataframe"))
        elif profile == "dongguan":
            if date_type is not None:
                raise ValueError(t("api.gf_unsupported_param_by_broker", profile=profile, param_name="date_type"))
            if is_dataframe is not None:
                raise ValueError(t("api.gf_unsupported_param_by_broker", profile=profile, param_name="is_dataframe"))
        elif profile == "shanxi":
            if date_type is not None:
                raise ValueError(t("api.gf_unsupported_param_by_broker", profile=profile, param_name="date_type"))
            if is_dataframe is None:
                is_dataframe = False
        else:
            if is_dataframe is None:
                is_dataframe = True
        # 统一处理：将单个股票代码转换为列表
        if isinstance(security, str):
            stocks = [security]
        else:
            stocks = security

        if table == "valuation":
            if fields is None:
                fields = ["trading_day", "total_value", "secu_code"]
            elif isinstance(fields, str):
                fields = [fields]
            data_dict = self.data_context.valuation_dict
        else:
            valid_tables = set(self.FUNDAMENTAL_TABLES) | self.FUNDAMENTAL_TABLE_ALIASES
            if table not in valid_tables:
                raise ValueError(
                    t(
                        "api.gf_invalid_table",
                        table=table,
                        valid_tables=sorted(valid_tables),
                    )
                )

            if fields is None:
                if date is None and start_year is None and end_year is None:
                    raise ValueError(t("api.gf_fields_omitted_requires_named_date"))
                fields = self.FUNDAMENTAL_TABLES.get(table, self.FUNDAMENTAL_DEFAULT_FIELDS)
            elif isinstance(fields, str):
                fields = [fields]

            data_dict = self.data_context.fundamentals_dict

        # api_doc.txt get_fundamentals：「不传入date时默认取回测日期的
        # 上一个交易日数据」；fundamentals_doc.txt 同（前一交易日估值）。
        # 旧实现取当天，会引入未来函数。
        if date is None:
            query_ts = pd.Timestamp(self.context.current_dt)
            trade_days = self.data_context.trade_days
            if trade_days is not None and len(trade_days):
                pos = int(pd.DatetimeIndex(trade_days).searchsorted(query_ts.normalize(), side="left"))
                if pos > 0:
                    query_ts = pd.DatetimeIndex(trade_days)[pos - 1]
        else:
            query_ts = pd.Timestamp(date)
            # fundamentals_doc.txt 场景二：回测/交易中 date 为非交易日时，
            # 返回字段全为 NAN 的数据（研究环境才回退到最近交易日）。
            trade_days = pd.DatetimeIndex(self.data_context.trade_days) if self.data_context.trade_days is not None else None
            if trade_days is not None and len(trade_days):
                day = query_ts.normalize()
                # 仅在交易日历确实覆盖该日期时才判定「非交易日」；
                # 日历不完整（如测试夹具只覆盖局部区间）或查询未来日期时
                # 不误判成非交易日，避免把合法查询静默变成全 NaN。
                in_range = trade_days[0] <= day <= trade_days[-1]
                if in_range and trade_days.get_indexer([day])[0] < 0:
                    return _PTradeDataFrame(
                        np.full((len(stocks), len(fields)), np.nan),
                        index=pd.Index(stocks, name="secu_code"),
                        columns=list(fields),
                    )
        if start_year is not None or end_year is not None:
            if table == "valuation":
                raise ValueError("valuation 只支持按日查询，不接受 start_year/end_year")
            return select_report_rows(
                stocks, data_dict, list(fields), query_ts,
                start_year, end_year, report_types,
            )
        cache_key = (table, query_ts)

        # 获取或创建日期索引缓存（LRUCache 自动淘汰）
        if cache_key not in self._fundamentals_cache:
            self._fundamentals_cache[cache_key] = {}

        date_indices = self._fundamentals_cache[cache_key]

        # 只为缓存中不存在的股票计算索引（增量更新）
        stocks_to_index = [s for s in stocks if s not in date_indices and s in data_dict]

        if stocks_to_index:
            for stock in stocks_to_index:
                try:
                    df = data_dict[stock]
                    if df is None or len(df) == 0:
                        continue

                    # 对于fundamentals表，使用publ_date过滤
                    if table != "valuation" and "publ_date" in df.columns:
                        # 过滤出查询日期前已公告的财报（不修改原DataFrame）
                        publ_dates = pd.to_datetime(df["publ_date"], errors="coerce")
                        valid_mask = (publ_dates <= query_ts).to_numpy()
                        valid_positions = np.flatnonzero(valid_mask)

                        if len(valid_positions) > 0:
                            # 选择已公告记录中报告期最新的一行；本地数据通常以date列保存报告期。
                            period_column = "end_date" if "end_date" in df.columns else "date" if "date" in df.columns else None
                            if period_column:
                                periods = pd.to_datetime(df.iloc[valid_positions][period_column], errors="coerce")
                                latest_pos = periods.to_numpy().argmax()
                                date_indices[stock] = int(valid_positions[latest_pos])
                            else:
                                date_indices[stock] = int(valid_positions[-1])
                        else:
                            # 没有已公告的财报，跳过
                            continue
                    else:
                        # 对于valuation表，按交易日期列返回查询日前最近数据；兼容日期索引数据。
                        if "date" in df.columns:
                            idx = self._latest_row_idx_by_date(df, "date", query_ts)
                            if idx is not None:
                                date_indices[stock] = idx
                        elif isinstance(df.index, pd.DatetimeIndex):
                            idx = _datetime_index_ns(df.index).searchsorted(query_ts.value, side="right")
                            if idx > 0:
                                date_indices[stock] = idx - 1
                except Exception:
                    # 静默忽略错误，继续处理其他股票
                    continue

        result_data = {}

        # 对于valuation表的total_value/float_value，需要用查询日期的收盘价实时计算
        need_realtime_total_value = table == "valuation" and "total_value" in fields
        need_realtime_float_value = table == "valuation" and "float_value" in fields
        need_realtime_market_cap = need_realtime_total_value or need_realtime_float_value

        # 预先获取并缓存当天所查股票的收盘价
        close_prices = {}
        if need_realtime_market_cap:
            price_cache_key = ("_close_prices", query_ts)
            if price_cache_key in self._fundamentals_cache:
                close_prices = self._fundamentals_cache[price_cache_key]
            else:
                close_prices = {}
                self._fundamentals_cache[price_cache_key] = close_prices
            # 增量补充：只获取尚未缓存的股票收盘价
            for stock in stocks:
                if stock in close_prices:
                    continue
                stock_df = self.data_context.stock_data_dict.get(stock)
                if stock_df is not None and isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
                    idx = _datetime_index_ns(stock_df.index).searchsorted(query_ts.value, side="right")
                    if idx > 0 and stock_df["volume"].values[idx - 1] > 0:
                        close_prices[stock] = stock_df["close"].values[idx - 1]

        for stock in stocks:
            if stock not in date_indices:
                continue

            try:
                df = data_dict[stock]
                if df is None or len(df) == 0:
                    continue

                idx = date_indices[stock]

                stock_data = {}
                for field in fields:
                    if field == "total_value" and need_realtime_total_value:
                        col_idx = df.columns.get_loc("total_shares") if "total_shares" in df.columns else -1
                        total_shares = df.iat[idx, col_idx] if col_idx >= 0 else None
                        if field in df.columns and not pd.isna(df[field].values[idx]):
                            # 文件存储值优先（平台口径）；仅缺失时实时计算兜底
                            stock_data[field] = df[field].values[idx]
                        elif total_shares is not None and not pd.isna(total_shares) and stock in close_prices:
                            stock_data[field] = close_prices[stock] * total_shares
                    elif field == "float_value" and need_realtime_float_value:
                        col_idx = df.columns.get_loc("a_floats") if "a_floats" in df.columns else -1
                        a_floats = df.iat[idx, col_idx] if col_idx >= 0 else None
                        if field in df.columns and not pd.isna(df[field].values[idx]):
                            # 文件存储值优先（平台口径）；仅缺失时实时计算兜底
                            stock_data[field] = df[field].values[idx]
                        elif a_floats is not None and not pd.isna(a_floats) and stock in close_prices:
                            stock_data[field] = close_prices[stock] * a_floats
                    elif field == "secu_code":
                        stock_data[field] = stock
                    elif field == "secu_abbr":
                        if (
                            not self.data_context.stock_metadata.empty
                            and stock in self.data_context.stock_metadata.index
                        ):
                            stock_data[field] = self.data_context.stock_metadata.loc[stock, "stock_name"]
                        else:
                            stock_data[field] = stock
                    elif field == "trading_day":
                        value = self._date_value_from_row(df, idx, "date", "trading_day")
                        if value is not None:
                            stock_data[field] = value
                    elif field == "end_date":
                        value = self._date_value_from_row(df, idx, "end_date", "date")
                        if value is not None:
                            stock_data[field] = value
                    elif field in df.columns:
                        stock_data[field] = df[field].values[idx]

                if stock_data:
                    result_data[stock] = stock_data

            except Exception as e:
                print(t("api.read_failed", table=table, stock=stock, fields=fields, error=e))
                traceback.print_exc()
                raise

        df = (
            pd.DataFrame.from_dict(result_data, orient="index", columns=fields)
            if result_data
            else pd.DataFrame(columns=fields)
        )
        if profile == "shanxi" and is_dataframe is False:
            return df.to_dict(orient="index")
        return df

    # ==================== 行情API ====================

    class PanelLike(dict):
        """模拟pandas.Panel"""

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._stock_list: Optional[list[str]] = None

        def __getitem__(self, key: str) -> pd.DataFrame | Any:
            if key in self:
                return super().__getitem__(key)

            if self._stock_list is None and self:
                first_df = next(iter(self.values()))
                self._stock_list = list(first_df.columns)

            if self._stock_list and key in self._stock_list:
                stock_data = {}
                for field, df in self.items():
                    if key in df.columns:
                        stock_data[field] = df[key]
                return pd.DataFrame(stock_data)

            raise KeyError(key)

        @property
        def empty(self) -> bool:
            if not self:
                return True
            return all(df.empty for df in self.values())

        @property
        def columns(self) -> pd.Index:
            if not self:
                return pd.Index([])
            first_df = next(iter(self.values()))
            return first_df.columns

    @staticmethod
    def _normalize_frequency(frequency: str) -> str:
        f = (frequency or "1d").strip().lower()
        return _FREQ_ALIASES.get(f, f)

    @staticmethod
    def _aggregate_kline(df: pd.DataFrame, rule: str) -> pd.DataFrame:
        agg = {}
        for col in df.columns:
            if col == "open":
                agg[col] = "first"
            elif col == "high":
                agg[col] = "max"
            elif col == "low":
                agg[col] = "min"
            elif col in ("close", "price"):
                agg[col] = "last"
            elif col in ("volume", "money"):
                agg[col] = "sum"
            elif col == "preclose":
                agg[col] = "first"
            elif col == "high_limit":
                agg[col] = "max"
            elif col == "low_limit":
                agg[col] = "min"
            elif col in ("unlimited", "is_open"):
                agg[col] = "max"
            else:
                agg[col] = "last"

        out = df.resample(rule, label="right", closed="right").agg(agg)
        if "close" in out.columns:
            out = out.dropna(subset=["close"])
        if "volume" in out.columns:
            out["volume"] = out["volume"].fillna(0.0)
        if "money" in out.columns:
            out["money"] = out["money"].fillna(0.0)
        return out

    @staticmethod
    def _aggregate_intraday_kline(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
        """Aggregate minute bars independently inside each CN trading session."""
        agg = {}
        for col in df.columns:
            if col == "open":
                agg[col] = "first"
            elif col == "high":
                agg[col] = "max"
            elif col == "low":
                agg[col] = "min"
            elif col in ("close", "price"):
                agg[col] = "last"
            elif col in ("volume", "money"):
                agg[col] = "sum"
            elif col == "preclose":
                agg[col] = "first"
            elif col in ("high_limit", "unlimited", "is_open"):
                agg[col] = "max"
            elif col == "low_limit":
                agg[col] = "min"
            else:
                agg[col] = "last"

        pieces = []
        for day, day_df in df.groupby(df.index.normalize(), sort=True):
            sessions = (
                (
                    day_df.between_time("09:31", "11:30"),
                    day + pd.Timedelta(hours=9, minutes=31),
                ),
                (
                    day_df.between_time("13:01", "15:00"),
                    day + pd.Timedelta(hours=13, minutes=1),
                ),
            )
            for session_df, session_start in sessions:
                if session_df.empty:
                    continue
                elapsed_minutes = np.asarray(
                    (session_df.index - session_start) / pd.Timedelta(minutes=1),
                    dtype=np.int64,
                )
                bucket_ids = elapsed_minutes // minutes
                grouped = session_df.groupby(bucket_ids).agg(agg)
                grouped.index = pd.DatetimeIndex(
                    [
                        session_start
                        + pd.Timedelta(minutes=(int(bucket_id) + 1) * minutes - 1)
                        for bucket_id in grouped.index
                    ]
                )
                pieces.append(grouped)
        if not pieces:
            return df.iloc[0:0].copy()
        return pd.concat(pieces).sort_index()

    def _cached_values(self, df: pd.DataFrame, column: str, data_version: object) -> np.ndarray:
        """返回列的 numpy 视图，按 DataFrame 对象缓存（视图不复制数据）。

        每个 DataFrame 一个条目，命中前校验弱引用仍指向同一个对象：临时对象
        回收后 id 可能复用，仅凭 id 取缓存会读到上一个对象的数据。
        """
        key = (id(df), data_version)
        entry = self._values_cache.get(key)
        if entry is None or entry[0]() is not df:
            cache = self._values_cache
            entry = (
                weakref.ref(df, lambda _ref, _key=key, _cache=cache: _cache.pop(_key, None)),
                {},
            )
            cache[key] = entry
        values = entry[1]
        array = values.get(column)
        if array is None:
            array = df[column].to_numpy(copy=False)
            values[column] = array
        return array

    def _apply_dypre_to_daily(
        self, stock_df: pd.DataFrame, stock: str, base_dt: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        adj_cache = self.data_context.adj_pre_cache
        if not adj_cache or stock not in adj_cache:
            return stock_df
        adj_factors = adj_cache[stock]
        common_idx = stock_df.index.intersection(adj_factors.index)
        if len(common_idx) == 0:
            return stock_df

        if base_dt is None:
            base_idx = common_idx[-1]
        else:
            pos = common_idx.searchsorted(base_dt, side="right") - 1
            if pos < 0:
                return stock_df
            base_idx = common_idx[pos]

        adj_a = adj_factors.loc[common_idx, "adj_a"]
        adj_b = adj_factors.loc[common_idx, "adj_b"]
        adj_a_base = float(adj_factors.loc[base_idx, "adj_a"])
        adj_b_base = float(adj_factors.loc[base_idx, "adj_b"])

        adjusted_df = stock_df.copy()
        for col in ("open", "high", "low", "close"):
            if col in adjusted_df.columns:
                raw = adjusted_df.loc[common_idx, col].astype(float).values
                adj_val = _round2(adj_a.values * raw + adj_b.values)
                adjusted_df.loc[common_idx, col] = (adj_val - adj_b_base) / adj_a_base
        return adjusted_df

    def _get_stock_df_by_frequency(
        self, stock: str, frequency: str, fq: str | None = None, base_dt: pd.Timestamp | None = None
    ) -> Optional[pd.DataFrame]:
        """按频率获取单只标的数据，统一处理别名、聚合和money字段兼容。"""
        if frequency in _MINUTE_FREQ_MINUTES:
            base = self.data_context.stock_data_dict_1m
            df = self._frame_for_code(base, stock)
            if df is None:
                return None
            if frequency != "1m":
                df = self._aggregate_intraday_kline(
                    df, _MINUTE_FREQ_MINUTES[frequency]
                )
            # api_doc.txt: fq 对 1m~120m 同样有效，旧实现在分钟频静默忽略复权。
            if fq in ("pre", "post"):
                df = self._apply_adj_factors(df, stock, fq)
            elif fq == "dypre":
                df = self._apply_adj_factors(df, stock, "pre")
                cache = self.data_context.adj_pre_cache
                if cache and stock in cache and isinstance(df.index, pd.DatetimeIndex) and len(df):
                    aligned = cache[stock].reindex(df.index.normalize()).ffill()
                    a_base = float(aligned["adj_a"].iloc[-1])
                    b_base = float(aligned["adj_b"].iloc[-1])
                    if a_base and np.isfinite(a_base) and np.isfinite(b_base):
                        for col in ("open", "high", "low", "close"):
                            if col in df.columns:
                                df[col] = (df[col].to_numpy(dtype=float) - b_base) / a_base
            return storage.ensure_alias_columns(df)

        if frequency in _PERIOD_FREQ_RULE:
            daily_df = self._frame_for_code(self.data_context.stock_data_dict, stock)
            if daily_df is None:
                daily_df = self._frame_for_code(self.data_context.benchmark_data, stock)
            if daily_df is None:
                return None
            if fq in ("pre", "post"):
                daily_df = self._apply_adj_factors(daily_df, stock, fq)
            elif fq == "dypre":
                daily_df = self._apply_dypre_to_daily(daily_df, stock, base_dt)
            df = self._aggregate_kline(daily_df, _PERIOD_FREQ_RULE[frequency])
            return storage.ensure_alias_columns(df)

        df = self._frame_for_code(self.data_context.stock_data_dict, stock)
        if df is None:
            df = self._frame_for_code(self.data_context.benchmark_data, stock)
        if df is None:
            return None
        return storage.ensure_alias_columns(df)

    @staticmethod
    def _frame_for_code(mapping, stock: str):
        """行情字典内部用 .SS/.SZ；策略可写任意官方尾缀，按等价候选逐个查。"""
        if mapping is None:
            return None
        for candidate in code_candidates(stock):
            df = mapping.get(candidate) if hasattr(mapping, "get") else None
            if df is None:
                try:
                    if candidate in mapping:
                        df = mapping[candidate]
                except (KeyError, TypeError):
                    df = None
            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                return df
        return None

    @staticmethod
    def _fill_minute_gaps(df: pd.DataFrame, minutes: int, fill: str) -> pd.DataFrame:
        if df.empty:
            return df
        freq = "%dmin" % minutes
        pieces = []
        for _, day_df in df.groupby(df.index.normalize(), sort=True):
            if day_df.empty:
                continue
            full_idx = pd.date_range(day_df.index.min(), day_df.index.max(), freq=freq)
            # 午休 11:31-12:59 不补点
            lunch_mask = (full_idx.time > pd.Timestamp("11:30").time()) & (full_idx.time < pd.Timestamp("13:00").time())
            full_idx = full_idx[~lunch_mask]
            expanded = day_df.reindex(full_idx)
            if fill == "pre":
                expanded = expanded.ffill()
            pieces.append(expanded)
        if not pieces:
            return df
        return pd.concat(pieces).sort_index()

    def _minute_target_index(
        self, current_dt: pd.Timestamp, count: int, minutes: int, include: bool
    ) -> pd.DatetimeIndex:
        """Build the logical intraday bar index ending at the effective cutoff."""
        if self.data_context.trade_days is not None:
            days = pd.DatetimeIndex(self.data_context.trade_days)
            days = days[:days.searchsorted(current_dt.normalize(), side='right')]
        else:
            days = pd.DatetimeIndex([current_dt.normalize()])
        sample_day = current_dt.normalize()
        sample_morning_start = sample_day + pd.Timedelta(hours=9, minutes=31)
        sample_afternoon_start = sample_day + pd.Timedelta(hours=13, minutes=1)
        if minutes > 1:
            sample_morning_start += pd.Timedelta(minutes=minutes - 1)
            sample_afternoon_start += pd.Timedelta(minutes=minutes - 1)
        bars_per_day = len(
            pd.date_range(
                sample_morning_start,
                sample_day + pd.Timedelta(hours=11, minutes=30),
                freq=f"{minutes}min",
            )
        ) + len(
            pd.date_range(
                sample_afternoon_start,
                sample_day + pd.Timedelta(hours=15),
                freq=f"{minutes}min",
            )
        )
        required_days = (count + bars_per_day - 1) // bars_per_day + 2
        session_parts = []
        for day in days[-required_days:]:
            morning_start = day + pd.Timedelta(hours=9, minutes=31)
            afternoon_start = day + pd.Timedelta(hours=13, minutes=1)
            if minutes > 1:
                morning_start += pd.Timedelta(minutes=minutes - 1)
                afternoon_start += pd.Timedelta(minutes=minutes - 1)
            session_parts.extend(
                [
                    pd.date_range(morning_start, day + pd.Timedelta(hours=11, minutes=30), freq=f"{minutes}min"),
                    pd.date_range(afternoon_start, day + pd.Timedelta(hours=15), freq=f"{minutes}min"),
                ]
            )
        if not session_parts:
            return pd.DatetimeIndex([])
        # append 循环是 O(n²)：一次性拼接各段时间点，序列逐元素保持不变。
        target = pd.DatetimeIndex(np.concatenate([part.values for part in session_parts]))
        cutoff_pos = target.searchsorted(current_dt, side='right' if include else 'left')
        return target[:cutoff_pos][-count:]

    def _ensure_market_data(self, stocks) -> None:
        """一次性并行预热本次请求涉及的行情与复权因子。

        stock_data_dict / adj_*_cache 已改为按需加载：若不在入口批量预热，
        主循环里逐只 ``__getitem__`` 会退化成单线程逐个读 parquet。这里对
        尚未预热过的股票调用一次 ``ensure_loaded``（线程池 + parquet
        filters 下推），后续所有访问都命中内存缓存。
        """
        if not stocks:
            return
        warmed = getattr(self, "_warmed_stocks", None)
        if warmed is None:
            warmed = self._warmed_stocks = set()
        new = [s for s in dict.fromkeys(stocks) if s not in warmed]
        if not new:
            return
        # 策略可写 XSHG/XSHE 等官方全称尾缀，而数据包内部只用 .SS/.SZ；
        # 展开等价写法才能让预热命中真实键，避免退化成逐只惰性加载。
        expanded = list(dict.fromkeys(
            candidate for s in new for candidate in code_candidates(s)
        ))
        source = getattr(self.data_context, "stock_data_dict", None)
        if source is not None and hasattr(source, "ensure_loaded"):
            source.ensure_loaded(expanded)
        for cache in (getattr(self.data_context, "adj_pre_cache", None),
                      getattr(self.data_context, "adj_post_cache", None)):
            if cache is not None and hasattr(cache, "ensure_loaded"):
                cache.ensure_loaded(expanded)
        warmed.update(new)

    def get_price(
        self,
        security: str | list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "1d",
        fields: Optional[str | list[str]] = None,
        fq: Optional[str] = None,
        count: Optional[int] = None,
        is_dict: bool = False,
    ) -> pd.DataFrame | PtradeAPI.PanelLike | OrderedDict | None:
        """获取历史行情数据"""
        frequency = self._normalize_frequency(frequency)
        if frequency not in _VALID_PRICE_FREQUENCIES:
            raise ValueError("function get_price: invalid frequency argument, got %s" % frequency)
        profile = self.broker_profile
        if profile in ("guosheng", "dongguan") and is_dict:
            raise ValueError(t("api.get_price_unsupported_is_dict", profile=profile))

        has_start_date = start_date is not None
        has_count = count is not None

        # 参数互斥：start_date 与 count 不能同时存在
        if has_start_date and has_count:
            raise ValueError(t("api.get_price_start_count_exclusive"))
        # 文档要求 start_date 与 count 必须且只能选择一个
        if (not has_start_date) and (not has_count):
            raise ValueError(t("api.get_price_start_count_exclusive"))

        # 券商差异：山西支持 dypre，国盛/东莞不支持
        valid_fq = ["pre", "post", None]
        if profile in ("shanxi", "auto"):
            valid_fq = ["pre", "post", "dypre", None]
        if fq not in valid_fq:
            raise ValueError(t("api.get_price_invalid_fq", valid_fq=valid_fq, fq=fq, fq_type=type(fq)))

        if isinstance(fields, str):
            fields_list = [fields]
        elif fields is None:
            # 国泰海通 api_doc.txt get_price: fields=None 返回 12 列（含
            # is_open / preclose / high_limit / low_limit / unlimited）。
            # 旧实现只给 7 列，按文档编写的策略会取不到扩展字段。
            fields_list = [
                "open", "high", "low", "close", "volume", "money", "price",
                "is_open", "preclose", "high_limit", "low_limit", "unlimited",
            ]
        else:
            fields_list = list(fields)

        if profile in ("guosheng", "dongguan") and "is_open" in fields_list:
            raise ValueError(t("api.get_price_unsupported_field", profile=profile, field_name="is_open"))

        is_single_stock = isinstance(security, str)
        stocks = [security] if is_single_stock else security
        self._ensure_market_data(stocks)

        if count is not None:
            end_dt = pd.Timestamp(end_date) if end_date else pd.Timestamp(self.context.current_dt)
            # api_doc.txt get_price: "数据返回内容不包括当天数据"。未显式传
            # end_date 时以回测当日为界，日线需回退到上一交易日末。
            if end_date is None and frequency == "1d":
                end_dt = end_dt.normalize() - pd.Timedelta(nanoseconds=1)
            result = {}
            for stock in stocks:
                stock_df = self._get_stock_df_by_frequency(stock, frequency, fq=fq, base_dt=end_dt)
                if not isinstance(stock_df, pd.DataFrame):
                    continue

                try:
                    if frequency in _MINUTE_FREQ_MINUTES or frequency in _PERIOD_FREQ_RULE:
                        # 分钟/周期数据：使用searchsorted查找
                        # 用 DatetimeIndex.searchsorted 避免 datetime64[us] vs ns 单位不匹配
                        idx = stock_df.index.searchsorted(end_dt, side="right") - 1
                        if (
                            idx < 0
                            and self.context.frequency == "1d"
                            and end_dt == end_dt.normalize()
                        ):
                            end_of_day = end_dt.normalize() + pd.Timedelta(hours=15)
                            idx = stock_df.index.searchsorted(end_of_day, side="right") - 1
                            if idx < 0:
                                continue
                        current_idx = idx
                    else:
                        current_idx = self._resolve_daily_index(stock, stock_df, end_dt)
                        if current_idx is None:
                            continue
                except (KeyError, IndexError):
                    continue

                # Ptrade API语义: count=N 返回截止到end_date的N条数据（包含end_date）
                slice_df = stock_df.iloc[max(0, current_idx - count + 1) : current_idx + 1]
                result[stock] = slice_df
        else:
            start_dt = pd.Timestamp(start_date) if start_date else None
            end_dt = pd.Timestamp(end_date) if end_date else pd.Timestamp(self.context.current_dt)
            if end_date is None and frequency == "1d":
                end_dt = end_dt.normalize() - pd.Timedelta(nanoseconds=1)

            result = {}
            for stock in stocks:
                stock_df = self._get_stock_df_by_frequency(stock, frequency, fq=fq, base_dt=end_dt)
                if not isinstance(stock_df, pd.DataFrame):
                    continue

                if start_dt:
                    mask = (stock_df.index >= start_dt) & (stock_df.index <= end_dt)
                else:
                    mask = stock_df.index <= end_dt

                slice_df = stock_df[mask]
                result[stock] = slice_df

        # 复权处理（仅日线/周月季年支持，分钟频不支持）
        if frequency == "1d" and fq in ("pre", "post", "dypre"):
            for stock in list(result.keys()):
                stock_df = result[stock]
                if isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
                    if fq in ("pre", "post"):
                        result[stock] = self._apply_adj_factors(stock_df, stock, fq)
                    else:
                        # dypre: 以区间末端为基准做动态前复权
                        adj_cache = self.data_context.adj_pre_cache
                        if not adj_cache or stock not in adj_cache:
                            continue
                        adj_factors = adj_cache[stock]
                        common_idx = stock_df.index.intersection(adj_factors.index)
                        if len(common_idx) == 0:
                            continue

                        adj_a = adj_factors.loc[common_idx, "adj_a"]
                        adj_b = adj_factors.loc[common_idx, "adj_b"]
                        base_idx = common_idx[-1]
                        adj_a_base = float(adj_factors.loc[base_idx, "adj_a"])
                        adj_b_base = float(adj_factors.loc[base_idx, "adj_b"])

                        adjusted_df = stock_df.copy()
                        for col in ("open", "high", "low", "close"):
                            if col in adjusted_df.columns:
                                raw = adjusted_df.loc[common_idx, col].astype(float).values
                                adj_val = _round2(adj_a.values * raw + adj_b.values)
                                adjusted_df.loc[common_idx, col] = (adj_val - adj_b_base) / adj_a_base
                        result[stock] = adjusted_df

        if not result:
            self.log.warning(t("api.get_price_empty", stocks=security, frequency=frequency, fq=fq))
            # api_doc.txt: 「正常返回dict类型数据，异常时返回None(NoneType)」；
            # QTV202401.05.013 明确把空字典改为 None。旧实现只在 shanxi 档
            # 返回 None，默认 auto 档返回空 OrderedDict，与平台语义相反。
            return None

        if is_dict:
            out = OrderedDict()
            for stock in stocks:
                stock_df = result.get(stock)
                if stock_df is None or stock_df.empty:
                    continue
                selected_fields = [f for f in fields_list if f in stock_df.columns]
                selected = stock_df[selected_fields] if selected_fields else stock_df
                # 文档要求首列为 datetime int64 (YYYYMMDDHHMM)，其后为行情字段。
                dtype_items = [("datetime", "<i8")]
                for f in selected.columns:
                    dtype_items.append((f, "<i8" if f in ("is_open", "unlimited") else "<f8"))
                arr = np.empty(len(selected), dtype=dtype_items)
                arr["datetime"] = _ptrade_datetime_int64(selected.index)
                for f in selected.columns:
                    if f in ("is_open", "unlimited"):
                        arr[f] = np.nan_to_num(
                            selected[f].to_numpy(dtype="float64", na_value=np.nan), nan=0.0
                        ).astype("int64")
                    else:
                        arr[f] = selected[f].to_numpy(dtype="float64", na_value=np.nan)
                out[stock] = arr
            return out if out else None

        if is_single_stock:
            stock_df = result.get(security)
            if stock_df is None:
                self.log.warning(t("api.get_price_no_data", stock=security, frequency=frequency, fq=fq))
                return None
            selected_fields = [f for f in fields_list if f in stock_df.columns]
            ret = stock_df[selected_fields] if selected_fields else stock_df
            if profile == "shanxi" and "unlimited" in ret.columns:
                ret = ret.copy()
                ret["unlimited"] = ret["unlimited"].astype("int64")
            return _PTradeDataFrame(ret)

        # Python 3.11 口径（api_doc.txt「仅python3.11版本支持 第二种返回数据」）：
        # 多股票无论单/多字段，均返回 DataFrame，列索引为 code + 行情字段。
        # 「列=股票代码」与 pandas.Panel 是文档标注「仅 python3.5 支持」的历史形态。
        frames = []
        for stock in stocks:
            stock_df = result.get(stock)
            if stock_df is None or stock_df.empty:
                continue
            selected_fields = [f for f in fields_list if f in stock_df.columns]
            if not selected_fields:
                continue
            part = stock_df[selected_fields].copy()
            part.insert(0, "code", stock)
            frames.append(part)
        if not frames:
            return None
        return _PTradeDataFrame(pd.concat(frames))

    @validate_lifecycle
    def get_trend_data(
        self, date: str | None = None, stocks: str | list[str] | None = None, market: str | None = None
    ) -> OrderedDict:
        """获取集合竞价期间代码数据（回测简化版）"""
        if stocks is None:
            if market:
                mk = market.upper()
                if mk in ("XSHG", "SS"):
                    stocks = [s for s in self.get_Ashares(date) if s.endswith(".SS")]
                elif mk in ("XSHE", "SZ"):
                    stocks = [s for s in self.get_Ashares(date) if s.endswith(".SZ")]
                else:
                    stocks = self.get_Ashares(date)
            else:
                stocks = list(self.active_universe) if self.active_universe else self.get_Ashares(date)[:50]
        elif isinstance(stocks, str):
            stocks = [stocks]

        query_dt = pd.Timestamp(date) if date else self.context.current_dt
        result = OrderedDict()

        if self.data_context.stock_data_dict_1m is not None:
            for stock in stocks:
                stock_df = self.data_context.stock_data_dict_1m.get(stock)
                if stock_df is None or not isinstance(stock_df, pd.DataFrame) or stock_df.empty:
                    continue
                day_mask = stock_df.index.normalize() == query_dt.normalize()
                day_df = stock_df.loc[day_mask]
                if not day_df.empty:
                    result[stock] = day_df.copy()
        else:
            for stock in stocks:
                stock_df = self.data_context.stock_data_dict.get(stock)
                if stock_df is None or not isinstance(stock_df, pd.DataFrame) or stock_df.empty:
                    continue
                idx = stock_df.index.searchsorted(query_dt, side="right") - 1
                if idx >= 0:
                    result[stock] = stock_df.iloc[[idx]].copy()

        return result

    @validate_lifecycle
    def get_individual_transaction(
        self,
        stocks: str | list[str] | None = None,
        data_count: int = 50,
        start_pos: int = 0,
        search_direction: int = 1,
    ) -> OrderedDict:
        """获取逐笔成交行情（本地股票回测不支持逐笔流）"""
        return self._get_individual_transaction_impl(stocks, data_count, start_pos, search_direction)

    @validate_lifecycle
    def get_individual_entrust(
        self,
        stocks: str | list[str] | None = None,
        data_count: int = 50,
        start_pos: int = 0,
        search_direction: int = 1,
    ) -> None:
        """获取逐笔委托行情（当前本地回测不支持交易/L2行情接口）"""
        _ = (stocks, data_count, start_pos, search_direction)
        self._unsupported_api("get_individual_entrust", "交易行情接口")

    def _get_individual_transaction_impl(
        self,
        stocks: str | list[str] | None = None,
        data_count: int = 50,
        start_pos: int = 0,
        search_direction: int = 1,
    ) -> OrderedDict:
        """逐笔成交内部实现（兼容别名共用）"""
        _ = (stocks, data_count, start_pos, search_direction)
        self._unsupported_api("get_individual_transaction", "交易逐笔接口")

    @validate_lifecycle
    def get_individual_transcation(
        self,
        stocks: str | list[str] | None = None,
        data_count: int = 50,
        start_pos: int = 0,
        search_direction: int = 1,
    ) -> OrderedDict:
        """国盛历史拼写兼容：get_individual_transcation -> get_individual_transaction"""
        return self._get_individual_transaction_impl(stocks, data_count, start_pos, search_direction)

    @validate_lifecycle
    def get_tick_direction(
        self,
        symbols: str | list[str] | None = None,
        query_date: int | str = 0,
        start_pos: int = 0,
        search_direction: int = 1,
        data_count: int = 50,
    ) -> None:
        """获取分时成交行情（当前本地回测不支持交易/L2行情接口）"""
        _ = (symbols, query_date, start_pos, search_direction, data_count)
        self._unsupported_api("get_tick_direction", "交易行情接口")

    @validate_lifecycle
    def get_sort_msg(
        self,
        sort_type_grp: str | list[str] | None = None,
        sort_field_name: str | None = None,
        sort_type: int = 1,
        data_count: int = 100,
    ) -> None:
        """获取板块/行业涨幅排名（当前本地回测不支持交易行情接口）"""
        _ = (sort_type_grp, sort_field_name, sort_type, data_count)
        self._unsupported_api("get_sort_msg", "交易行情接口")

    @validate_lifecycle
    def get_etf_info(self, etf_code: str | list[str]) -> None:
        """获取ETF信息（当前本地回测不支持ETF交易接口）"""
        _ = etf_code
        self._unsupported_api("get_etf_info", "ETF交易接口")

    @validate_lifecycle
    def get_etf_stock_info(self, etf_code: str, security: str | list[str]) -> None:
        """获取ETF成分券信息（当前本地回测不支持ETF交易接口）"""
        _ = (etf_code, security)
        self._unsupported_api("get_etf_stock_info", "ETF交易接口")

    @validate_lifecycle
    def get_gear_price(self, sids: str | list[str]) -> None:
        """获取档位行情价格（当前本地回测不支持实时行情接口）"""
        _ = sids
        self._unsupported_api("get_gear_price", "实时行情接口")

    @validate_lifecycle
    def get_snapshot(self, security: str | list[str]) -> None:
        """获取实时行情快照（当前本地回测不支持交易实时行情接口）"""
        _ = security
        self._unsupported_api("get_snapshot", "实时行情接口")

    @validate_lifecycle
    def get_cb_info(self) -> pd.DataFrame:
        """可转债代码和名称。转股价、溢价率没有本地来源，这些列留空。"""
        columns = [
            "bond_code",
            "bond_name",
            "stock_code",
            "stock_name",
            "list_date",
            "premium_rate",
            "convert_date",
            "maturity_date",
            "convert_rate",
            "convert_price",
            "convert_value",
        ]
        codes = self.get_cb_list()
        if not codes:
            return pd.DataFrame(columns=columns)
        frame = self._instrument_frame()
        rows = []
        for code in codes:
            name = ""
            listed = None
            if frame is not None and code in frame.index:
                if "stock_name" in frame.columns and pd.notna(frame.at[code, "stock_name"]):
                    name = str(frame.at[code, "stock_name"])
                if "listed_date" in frame.columns and pd.notna(frame.at[code, "listed_date"]):
                    listed = str(frame.at[code, "listed_date"])[:10]
            rows.append(
                {
                    "bond_code": code,
                    "bond_name": name,
                    "stock_code": None,
                    "stock_name": None,
                    "list_date": listed,
                    "premium_rate": np.nan,
                    "convert_date": None,
                    "maturity_date": None,
                    "convert_rate": np.nan,
                    "convert_price": np.nan,
                    "convert_value": np.nan,
                }
            )
        return pd.DataFrame(rows, columns=columns)

    @timer()
    def get_history(
        self,
        count: int,
        frequency: str = "1d",
        field: str | list[str] | None = None,
        security_list: str | list[str] | None = None,
        fq: str | None = None,
        include: bool = False,
        fill: str = "nan",
        is_dict: bool = False,
    ) -> pd.DataFrame | dict | PtradeAPI.PanelLike:
        """模拟通用ptrade的get_history（优化批量处理+缓存）"""
        if not isinstance(count, (int, np.integer)) or isinstance(count, bool) or count <= 0:
            raise ValueError("function get_history: count must be greater than 0")
        frequency = self._normalize_frequency(frequency)
        if frequency not in _VALID_PRICE_FREQUENCIES:
            raise ValueError("function get_history: invalid frequency argument, got %s" % frequency)
        profile = self.broker_profile
        if fill not in ("nan", "pre"):
            raise ValueError(t("api.get_history_invalid_fill", fill=fill))

        # 券商差异：国盛签名不支持 is_dict
        if profile == "guosheng" and is_dict:
            raise ValueError(t("api.get_history_unsupported_is_dict", profile=profile))

        # 验证fq参数
        valid_fq = ["pre", "post", "dypre", None]
        if fq not in valid_fq:
            raise ValueError(t("api.get_history_invalid_fq", valid_fq=valid_fq, fq=fq, fq_type=type(fq)))

        if field is None:
            fields = ["open", "high", "low", "close", "volume", "money", "price"]
        elif isinstance(field, str):
            fields = [field]
        else:
            fields = field if field else ["open", "high", "low", "close", "volume", "money", "price"]

        # 券商差异：is_open 字段仅在山西口径文档中出现
        if profile in ("guosheng", "dongguan") and "is_open" in fields:
            raise ValueError(t("api.get_history_unsupported_field", profile=profile, field_name="is_open"))

        # 文档语义：security_list=None 时，使用当前 universe
        stocks = security_list if security_list is not None else list(self.active_universe)
        if isinstance(stocks, str):
            stocks = [stocks]

        # 批量快速返回空结果（api_doc.txt: 异常时返回 None）
        if not stocks:
            return None
        self._ensure_market_data(stocks)

        current_dt = pd.Timestamp(self.context.current_dt)
        query_date = current_dt.normalize()
        if self._history_cache_date is None or self._history_cache_date != query_date:
            self._history_cache.clear()
            self._history_cache_date = query_date

        # 缓存键保留股票请求顺序，避免不同列顺序共享结果。
        is_single_stock = isinstance(security_list, str)
        field_key = tuple(fields) if len(fields) > 1 else fields[0]
        data_version = getattr(self.data_context, "data_version", None)
        cache_key = (
            id(self.data_context),
            data_version,
            profile,
            is_single_stock,
            tuple(stocks),
            count,
            field_key,
            fq,
            current_dt,
            include,
            fill,
            is_dict,
            frequency,
        )

        # 检查缓存
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]

        # 根据frequency选择数据源
        # 优化1: 批量预加载股票数据（减少LazyDataDict的重复加载）
        stock_dfs = {}
        for stock in stocks:
            data_source = self._get_stock_df_by_frequency(stock, frequency, fq=fq, base_dt=current_dt)
            if data_source is not None:
                stock_dfs[stock] = data_source

        # 优化2: 批量获取索引位置
        stock_info = {}
        for stock, data_source in stock_dfs.items():
            if not isinstance(data_source, pd.DataFrame):
                continue
            try:
                if frequency in _MINUTE_FREQ_MINUTES or frequency in _PERIOD_FREQ_RULE:
                    # 分钟/周期数据：使用searchsorted查找
                    # 用 DatetimeIndex.searchsorted 避免 datetime64[us] vs ns 单位不匹配
                    idx = data_source.index.searchsorted(current_dt, side="right") - 1
                    if (
                        idx < 0
                        and self.context.frequency == "1d"
                        and current_dt == current_dt.normalize()
                    ):
                        # 跨频率场景：日线回测取分钟/周期数据时，
                        # current_dt 是当天 00:00:00，searchsorted 返回 0 导致 idx=-1。
                        # 对齐到当天收盘时间后重试，向前取最近的 bar。
                        end_of_day = current_dt.normalize() + pd.Timedelta(hours=15)
                        idx = data_source.index.searchsorted(end_of_day, side="right") - 1
                        if idx < 0:
                            continue
                    current_idx = idx
                else:
                    current_idx = self._resolve_daily_index(stock, data_source, current_dt)
                    if current_idx is None:
                        continue
                stock_info[stock] = (data_source, current_idx)
            except (KeyError, IndexError):
                continue

        # 优化3+4: 批量切片+复权（减少循环开销）
        result = {}
        result_dates = {}  # {stock: DatetimeIndex} 对齐 ptrade 返回的 datetime 行索引
        # 分钟数据不支持复权
        needs_adj_pre = frequency == "1d" and fq == "pre" and self.data_context.adj_pre_cache
        needs_adj_dypre = frequency == "1d" and fq == "dypre" and self.data_context.adj_pre_cache
        needs_adj_post = frequency == "1d" and fq == "post" and self.data_context.adj_post_cache
        price_fields = {"open", "high", "low", "close"}
        daily_target_dates = None
        target_ns = None
        calendar = None
        if frequency == "1d" and self.data_context.trade_days is not None:
            calendar = pd.DatetimeIndex(self.data_context.trade_days)
            cutoff = current_dt.normalize()
            cutoff_pos = calendar.searchsorted(cutoff, side='right' if include else 'left')
            valid_dates = calendar[:cutoff_pos]
            daily_target_dates = valid_dates[-count:]
            target_ns = _datetime_index_ns(daily_target_dates)

        for stock, (data_source, current_idx) in stock_info.items():
            if include:
                start_idx = max(0, current_idx - count + 1)
                end_idx = current_idx + 1
            else:
                start_idx = max(0, current_idx - count)
                end_idx = current_idx

                if end_idx == 0 and current_idx == 0:
                    end_idx = 1

            adj_factors = None
            adj_a = adj_b = None
            adj_a_full = adj_b_full = None

            if (needs_adj_pre or needs_adj_dypre) and stock in self.data_context.adj_pre_cache:
                adj_factors = self.data_context.adj_pre_cache[stock]
            elif needs_adj_post and stock in self.data_context.adj_post_cache:
                adj_factors = self.data_context.adj_post_cache[stock]

            if adj_factors is not None:
                alignment_key = (stock, fq, data_version)
                indexes_aligned = self._adj_alignment_cache.get(alignment_key)
                if indexes_aligned is None:
                    indexes_aligned = adj_factors.index.equals(data_source.index)
                    self._adj_alignment_cache[alignment_key] = indexes_aligned
                if indexes_aligned:
                    adj_a_full = self._cached_values(adj_factors, "adj_a", data_version)
                    adj_b_full = self._cached_values(adj_factors, "adj_b", data_version)
                else:
                    # reindex 分支的 DataFrame 是一次性临时对象，缓存没有收益，直接取值。
                    aligned_factors = adj_factors.reindex(data_source.index).ffill()
                    adj_a_full = aligned_factors["adj_a"].values
                    adj_b_full = aligned_factors["adj_b"].values
                adj_a = adj_a_full[start_idx:end_idx]
                adj_b = adj_b_full[start_idx:end_idx]

            if needs_adj_dypre and adj_a is not None:
                adj_a_base = adj_a_full[current_idx]
                adj_b_base = adj_b_full[current_idx]

            stock_result = {}
            hl_adj = {}
            if (
                needs_adj_pre
                and adj_a is not None
                and np.all(adj_a == 1.0)
                and "high" in fields
                and "low" in fields
                and "high" in data_source.columns
                and "low" in data_source.columns
            ):
                hl_adj["high"], hl_adj["low"] = _compute_hl_adj(
                    adj_b,
                    self._cached_values(data_source, "high", data_version)[start_idx:end_idx],
                    self._cached_values(data_source, "low", data_version)[start_idx:end_idx],
                )

            for field_name in fields:
                if field_name not in data_source.columns:
                    continue
                raw = self._cached_values(data_source, field_name, data_version)[start_idx:end_idx]

                if adj_a is not None and field_name in price_fields:
                    if needs_adj_post:
                        stock_result[field_name] = adj_a * raw + adj_b
                    elif needs_adj_dypre:
                        stock_result[field_name] = (_round2(adj_a * raw + adj_b) - adj_b_base) / adj_a_base
                    elif field_name in hl_adj:
                        stock_result[field_name] = hl_adj[field_name]
                    else:
                        stock_result[field_name] = _round2(adj_a * raw + adj_b)
                else:
                    stock_result[field_name] = raw

            if stock_result and daily_target_dates is not None:
                source_dates = data_source.index[start_idx:end_idx]
                target_dates = daily_target_dates
                # 日期索引在 _resolve_daily_index 里已按 int64 ns 建好，直接复用。
                index_ns = self.get_stock_date_index(stock)[1]
                window_is_dense = (
                    len(source_dates) == len(target_dates)
                    and np.array_equal(index_ns[start_idx:end_idx], target_ns)
                )
                if window_is_dense:
                    result_dates[stock] = source_dates
                else:
                    daily_df = pd.DataFrame(stock_result, index=source_dates)
                    seed_dates = source_dates[source_dates < target_dates[0]] if len(target_dates) else []
                    if len(seed_dates):
                        reindex_dates = pd.DatetimeIndex([seed_dates[-1]]).append(target_dates)
                    else:
                        reindex_dates = target_dates
                    daily_df = daily_df.reindex(reindex_dates)
                    price_fill_fields = {
                        "open",
                        "high",
                        "low",
                        "close",
                        "price",
                        "preclose",
                        "high_limit",
                        "low_limit",
                    }
                    for field_name in daily_df.columns:
                        if field_name in price_fill_fields:
                            daily_df[field_name] = daily_df[field_name].ffill()
                        elif field_name in {"volume", "money", "amount"}:
                            daily_df[field_name] = daily_df[field_name].fillna(0.0)
                    daily_df = daily_df.reindex(target_dates)
                    stock_result = {c: daily_df[c].values for c in daily_df.columns}
                    result_dates[stock] = daily_df.index
            elif stock_result and frequency in _MINUTE_FREQ_MINUTES:
                source_dates = data_source.index[start_idx:end_idx]
                logical_current_dt = current_dt
                if self.context.frequency == "1d" and current_dt == current_dt.normalize():
                    logical_current_dt = current_dt + pd.Timedelta(hours=15)
                target_dates = self._minute_target_index(
                    logical_current_dt, count, _MINUTE_FREQ_MINUTES[frequency], include
                )
                tmp_df = pd.DataFrame(stock_result, index=source_dates)
                seed_dates = source_dates[source_dates < target_dates[0]] if len(target_dates) else []
                if len(seed_dates):
                    reindex_dates = pd.DatetimeIndex([seed_dates[-1]]).append(target_dates)
                else:
                    reindex_dates = target_dates
                tmp_df = tmp_df.reindex(reindex_dates)
                if fill == "pre":
                    tmp_df = tmp_df.ffill()
                tmp_df = tmp_df.reindex(target_dates)
                if tmp_df.empty or tmp_df.isna().all(axis=None):
                    stock_result = {}
                else:
                    stock_result = {c: tmp_df[c].values for c in tmp_df.columns}
                    result_dates[stock] = tmp_df.index
            elif stock_result:
                result_dates[stock] = data_source.index[start_idx:end_idx]

            if stock_result:
                result[stock] = stock_result

        # ── 转换为返回格式（国泰海通 api_doc.txt / Python 3.11 口径）──
        if not result:
            self.log.warning(t("api.get_history_empty", stocks=security_list, count=count, frequency=frequency, fq=fq))
            # api_doc.txt: 「正常返回dict类型数据，异常时返回None(NoneType)」
            final_result = None
        elif is_dict:
            # 文档: OrderedDict[str, numpy structured array]，
            # 首列 datetime(int64, YYYYMMDDHHMM)，其后依次为请求的行情字段。
            final_result = OrderedDict()
            for stock in stocks:
                if stock not in result:
                    continue
                cols = [f for f in fields if f in result[stock]]
                if not cols:
                    continue
                n = len(result[stock][cols[0]])
                dtype_items = [("datetime", "<i8")] + [
                    (f, "<i8" if f in ("is_open", "unlimited") else "<f8") for f in cols
                ]
                arr = np.empty(n, dtype=dtype_items)
                dates = result_dates.get(stock)
                arr["datetime"] = (
                    _ptrade_datetime_int64(dates) if dates is not None and len(dates) == n else 0
                )
                for f in cols:
                    vals = np.asarray(result[stock][f])
                    if f in ("is_open", "unlimited"):
                        arr[f] = np.nan_to_num(vals.astype("float64"), nan=0.0).astype("int64")
                    else:
                        arr[f] = vals.astype("float64", copy=False)
                final_result[stock] = arr
            if not final_result:
                final_result = None
        else:
            stocks_list = [security_list] if is_single_stock else stocks

            if is_single_stock:
                if stocks_list[0] not in result:
                    final_result = None
                else:
                    stock_dates = result_dates.get(stocks_list[0])
                    df_data = {
                        field_name: result[stocks_list[0]][field_name]
                        for field_name in fields
                        if field_name in result[stocks_list[0]]
                    }
                    final_result = _PTradeDataFrame(
                        df_data,
                        index=stock_dates.to_pydatetime() if stock_dates is not None else None,
                    )

            elif profile == "shanxi":
                # 山西证券：MultiIndex(field, stock) + datetime 行索引
                all_dates = {}
                for stock in stocks_list:
                    if stock in result_dates:
                        all_dates[stock] = result_dates[stock]
                if not all_dates:
                    final_result = _PTradeDataFrame()
                else:
                    ref_stock = max(all_dates, key=lambda s: len(all_dates[s]))
                    ref_dates = all_dates[ref_stock]

                    col_data = {}
                    for field_name in fields:
                        for stock in stocks_list:
                            if stock not in result or field_name not in result[stock]:
                                continue
                            stock_dates = all_dates.get(stock)
                            vals = result[stock][field_name]
                            if stock_dates is not None and len(stock_dates) == len(ref_dates) and stock_dates.equals(ref_dates):
                                col_data[(field_name, stock)] = vals
                            else:
                                s = pd.Series(vals, index=stock_dates.to_pydatetime() if stock_dates is not None else None)
                                s = s.reindex(ref_dates.to_pydatetime())
                                col_data[(field_name, stock)] = s.values

                    final_result = _PTradeDataFrame(col_data, index=ref_dates.to_pydatetime())
                    final_result.columns = pd.MultiIndex.from_tuples(
                        final_result.columns, names=["field", "code"]
                    )

            else:
                # Python 3.11 口径（api_doc.txt「仅python3.11版本支持 第二种返回数据」）：
                # 多股票无论单/多字段，均返回 DataFrame，列索引 = code + 行情字段，
                # 行按股票分组排列。列=股票代码 / pandas.Panel 是文档标注
                # 「仅 python3.5 版本支持」的历史形态，不再作为默认输出。
                frames = []
                for stock in stocks_list:
                    if stock not in result:
                        continue
                    df_data = {f: result[stock][f] for f in fields if f in result[stock]}
                    if not df_data:
                        continue
                    stock_dates = result_dates.get(stock)
                    idx = stock_dates.to_pydatetime() if stock_dates is not None else None
                    part = _PTradeDataFrame(df_data, index=idx)
                    part.insert(0, "code", stock)
                    frames.append(part)
                final_result = _PTradeDataFrame(pd.concat(frames)) if frames else None

        # 券商差异：unlimited 返回类型（shanxi=int64, 其他=float64）
        if isinstance(final_result, pd.DataFrame) and not final_result.empty:
            ul_cols = [c for c in final_result.columns if c == "unlimited" or (isinstance(c, tuple) and c[0] == "unlimited")]
            for col in ul_cols:
                if profile == "shanxi":
                    final_result[col] = final_result[col].astype("int64")
                else:
                    final_result[col] = final_result[col].astype("float64", copy=False)

        # 缓存结果 (LRUCache自动管理大小)
        self._history_cache[cache_key] = final_result

        return final_result

    # ==================== 股票信息API ====================

    @validate_lifecycle
    def get_stock_blocks(self, stock_code: str) -> dict | None:
        """获取股票所属板块"""
        stock = stock_code
        if not self.data_context.stock_metadata.empty and stock in self.data_context.stock_metadata.index:
            try:
                blocks_str = self.data_context.stock_metadata.loc[stock, "blocks"]
                if pd.notna(blocks_str) and blocks_str:
                    return json.loads(blocks_str)
            except (KeyError, json.JSONDecodeError):
                pass
        return None

    def get_stock_info(self, stocks: str | list[str], field: str | list[str] | None = None) -> dict[str, dict]:
        """获取股票基础信息"""
        if isinstance(stocks, str):
            stocks = [stocks]

        if field is None:
            field = ["stock_name"]
        elif isinstance(field, str):
            field = [field]

        result = {}
        for stock in stocks:
            stock_info = {}

            if not self.data_context.stock_metadata.empty and stock in self.data_context.stock_metadata.index:
                meta_row = self.data_context.stock_metadata.loc[stock]
                for f in field:
                    if f in meta_row.index:
                        stock_info[f] = meta_row[f]

            if "stock_name" in field and "stock_name" not in stock_info:
                stock_info["stock_name"] = stock
            if "de_listed_date" in field and "de_listed_date" not in stock_info:
                stock_info["de_listed_date"] = "2900-01-01"

            result[stock] = stock_info

        return result

    def get_stock_name(self, stocks: str | list[str]) -> dict[str, str | None] | None:
        """获取股票名称"""
        if not isinstance(stocks, (str, list)):
            return None

        if isinstance(stocks, str):
            stocks = [stocks]

        result = {}
        for stock in stocks:
            if not self.data_context.stock_metadata.empty and stock in self.data_context.stock_metadata.index:
                result[stock] = self.data_context.stock_metadata.loc[stock, "stock_name"]
            else:
                result[stock] = None

        return result

    def get_stock_status(
        self, stocks: str | list[str], query_type: str = "ST", query_date: str | None = None
    ) -> dict[str, bool | None] | None:
        """获取股票状态（ST/HALT/DELISTING）

        基于日频 stock_status_history 数据，用 bisect 查找当日快照。
        """
        if not isinstance(stocks, (str, list)):
            return None

        if isinstance(stocks, str):
            stocks = [stocks]

        if query_date is None:
            query_dt = self.context.current_dt if self.context else pd.Timestamp.now()
        else:
            query_dt = pd.Timestamp(query_date)

        dt_value = query_dt.value

        # 查找当日快照
        status_dict = None
        if self.data_context.stock_status_history:
            if self._sorted_status_dates is None:
                self._sorted_status_dates = sorted(self.data_context.stock_status_history.keys())
            query_date_str = query_dt.strftime("%Y%m%d")
            pos = bisect.bisect_right(self._sorted_status_dates, query_date_str)
            if pos > 0:
                history = self.data_context.stock_status_history[self._sorted_status_dates[pos - 1]]
                status_dict = history.get(query_type)

        result = {}
        for stock in stocks:
            cache_key = (dt_value, stock, query_type)
            if cache_key in self._stock_status_cache:
                result[stock] = self._stock_status_cache[cache_key]
                continue

            has_stock_data = (
                stock in self.data_context.stock_data_dict
                or (
                    not self.data_context.stock_metadata.empty
                    and stock in self.data_context.stock_metadata.index
                )
            )
            is_match = None

            # 快照路径
            if status_dict is not None:
                if stock in status_dict:
                    is_match = status_dict.get(stock) is True
                elif has_stock_data:
                    is_match = False
            # fallback: 无快照数据时用 volume=0 判停牌
            elif query_type == "HALT":
                stock_df = self.data_context.stock_data_dict.get(stock)
                if stock_df is not None:
                    is_match = query_dt in stock_df.index and stock_df.loc[query_dt, "volume"] == 0
            elif has_stock_data:
                is_match = False

            self._stock_status_cache[cache_key] = is_match
            result[stock] = is_match

        return result

    def get_stock_exrights(self, stock_code: str, date: str | int | datetime_date | None = None) -> Optional[pd.DataFrame]:
        """获取股票除权除息信息"""
        exrights_df = self.data_context.exrights_dict.get(stock_code)
        if exrights_df is None or exrights_df.empty:
            return None

        if date is not None:
            if isinstance(date, int):
                date_int = int(date)
                query_date = pd.to_datetime(str(date_int), format="%Y%m%d", errors="coerce")
            else:
                query_date = pd.Timestamp(date)
                date_int = int(query_date.strftime("%Y%m%d"))

            if date_int in exrights_df.index:
                return exrights_df.loc[[date_int]]

            if query_date in exrights_df.index:
                return exrights_df.loc[[query_date]]

            index_as_dates = pd.to_datetime(exrights_df.index.astype(str), format="%Y%m%d", errors="coerce")
            match_positions = np.flatnonzero(np.asarray(index_as_dates == query_date.normalize()))
            if len(match_positions) > 0:
                return exrights_df.iloc[[int(match_positions[0])]]
            return None

        return exrights_df

    # ==================== 指数/行业API ====================

    @validate_lifecycle
    def get_etf_stock_list(self, etf_code: str) -> None:
        """获取ETF成分券列表（当前本地回测不支持ETF交易接口）"""
        _ = etf_code
        self._unsupported_api("get_etf_stock_list", "ETF交易接口")

    @staticmethod
    def _csi_rebalance_day(year: int, month: int) -> int:
        """CSI半年度调仓生效日 = 第二个周五后的下一个周一的day"""
        cal = calendar.monthcalendar(year, month)
        friday_count = 0
        for week in cal:
            if week[4] != 0:  # Friday = index 4
                friday_count += 1
                if friday_count == 2:
                    return week[4] + 3  # 下周一
        return 32

    def get_index_stocks(self, index_code: str, date: str | None = None) -> list[str]:
        """获取指数成份股（支持向前回溯查找）"""
        original_code = index_code
        if not self.data_context.index_constituents:
            return []

        # 缓存排序后的日期列表（避免每次调用重排序）
        if self._sorted_index_dates is None:
            self._sorted_index_dates = sorted(self.data_context.index_constituents.keys())
        available_dates = self._sorted_index_dates

        # 如果未指定日期，使用回测当前日期
        if date is None:
            query_date = self.context.current_dt.strftime("%Y%m%d")
        else:
            # 统一日期格式为YYYYMMDD
            query_date = date.replace("-", "")

        # CSI半年度调仓: 6/12月第二个周五后生效，使用当月月末快照
        month = int(query_date[4:6])
        if month in (6, 12):
            year = int(query_date[:4])
            day = int(query_date[6:8])
            if day >= self._csi_rebalance_day(year, month):
                last_day = calendar.monthrange(year, month)[1]
                query_date = "%04d%02d%02d" % (year, month, last_day)

        # 使用 bisect 找到小于等于 date 的最近日期
        idx = bisect.bisect_right(available_dates, query_date)

        # 官方 index_code 可为 000300.SS / 000300.XSHG / 000300.XBHS（api_doc L2516）。
        # 本地成份股数据只用 .SS/.SZ，故按全部等价写法依次尝试。
        index_candidates = code_candidates(original_code)

        if idx > 0:
            # 向前查找包含该指数数据的最近日期
            for i in range(idx - 1, -1, -1):
                nearest_date = available_dates[i]
                constituents = self.data_context.index_constituents[nearest_date]
                for code in index_candidates:
                    if code in constituents:
                        result = constituents[code]
                        return list(result) if hasattr(result, "__iter__") else []

        return []

    @validate_lifecycle
    def get_instruments(self, contract: str | None = None) -> pd.DataFrame:
        """获取合约信息（当前本地回测不支持期货）"""
        _ = contract
        self._unsupported_api("get_instruments", "期货接口")

    @validate_lifecycle
    def get_dominant_contract(self, contract: str, date: str | None = None) -> Optional[str]:
        """获取主力合约代码（当前本地回测不支持期货）"""
        _ = (contract, date)
        self._unsupported_api("get_dominant_contract", "期货接口")

    def get_industry_stocks(self, industry_code: str | None = None) -> dict | list[str]:
        """推导行业成份股（带缓存）"""
        if self.data_context.stock_metadata.empty:
            return {} if industry_code is None else []

        # 使用 DataContext._industry_index 缓存
        if self.data_context._industry_index is None:
            industries = {}
            for stock_code, row in self.data_context.stock_metadata.iterrows():
                try:
                    blocks = json.loads(row["blocks"])
                    if blocks.get("HY"):
                        ind_code = blocks["HY"][0][0]
                        ind_name = blocks["HY"][0][1]

                        if ind_code not in industries:
                            industries[ind_code] = {"name": ind_name, "stocks": []}
                        industries[ind_code]["stocks"].append(stock_code)
                except (KeyError, json.JSONDecodeError, IndexError, TypeError):
                    pass
            self.data_context._industry_index = industries

        if industry_code is None:
            return self.data_context._industry_index
        else:
            return self.data_context._industry_index.get(industry_code, {}).get("stocks", [])

    # ==================== 涨跌停API ====================

    def _get_price_limit_ratio(self, stock: str) -> float:
        """获取股票涨跌停幅度"""
        if not self.has_price_limit:
            return None
        if (stock.startswith("688") and stock.endswith(".SS")) or (stock.startswith("30") and stock.endswith(".SZ")):
            return 0.20
        elif stock.endswith(".BJ"):
            return 0.30
        else:
            return 0.10

    def check_limit(self, security: str | list[str], query_date: str | None = None) -> dict[str, int]:
        """检查涨跌停状态"""
        if self.broker_profile == "guosheng":
            return {}
        if not isinstance(security, (str, list)):
            return {}
        if not self.has_price_limit:
            return {s: 0 for s in (security if isinstance(security, list) else [security])}

        if isinstance(security, str):
            securities = [security]
        else:
            securities = security

        if query_date is None:
            query_dt = self.context.current_dt if self.context else pd.Timestamp.now()
        else:
            query_dt = pd.Timestamp(query_date)

        result = {}
        for stock in securities:
            status = 0

            if stock not in self.data_context.stock_data_dict:
                result[stock] = status
                continue

            stock_df = self.data_context.stock_data_dict[stock]
            if not isinstance(stock_df, pd.DataFrame):
                result[stock] = status
                continue

            try:
                idx = self._resolve_daily_index(stock, stock_df, query_dt)
                if idx is None:
                    result[stock] = status
                    continue

                if idx == 0:
                    result[stock] = status
                    continue

                current_high = stock_df["high"].values[idx]
                current_low = stock_df["low"].values[idx]
                prev_close = stock_df["close"].values[idx - 1]

                # api_doc.txt: high_limit / low_limit 是交易所下发的真实
                # 涨跌停价（含 ST ±5%、新股无限制、板块/日期规则）。优先读
                # 数据快照，避免引擎侧与导出侧各按固定比例算一套而互相矛盾；
                # 仅当快照缺失时才回退到按 preclose 推算。
                limit_up_price = None
                limit_down_price = None
                if "high_limit" in stock_df.columns and "low_limit" in stock_df.columns:
                    hl = stock_df["high_limit"].values[idx]
                    ll = stock_df["low_limit"].values[idx]
                    if not np.isnan(hl) and not np.isnan(ll) and hl > 0 and ll > 0:
                        limit_up_price, limit_down_price = float(hl), float(ll)

                if limit_up_price is None:
                    if np.isnan(prev_close) or prev_close <= 0:  # type: ignore
                        result[stock] = status
                        continue
                    limit_ratio = self._get_price_limit_ratio(stock)
                    limit_up_price = prev_close * (1 + limit_ratio)
                    limit_down_price = prev_close * (1 - limit_ratio)

                # 回测中不能使用当天收盘价判断涨停（会产生未来数据泄露）
                # 只检查一字涨停（开盘=最高=最低=涨停价）
                current_open = stock_df["open"].values[idx]

                # 涨停判断：一字涨停（无法买入）
                is_one_word_up_limit = (
                    abs(current_open - limit_up_price) < 0.01
                    and abs(current_high - limit_up_price) < 0.01
                    and abs(current_low - limit_up_price) < 0.01
                )

                # 跌停判断：一字跌停（无法卖出）
                is_one_word_down_limit = (
                    abs(current_open - limit_down_price) < 0.01
                    and abs(current_high - limit_down_price) < 0.01
                    and abs(current_low - limit_down_price) < 0.01
                )

                if is_one_word_up_limit:  # type: ignore
                    status = 1
                elif is_one_word_down_limit:  # type: ignore
                    status = -1

                result[stock] = status
            except (KeyError, IndexError, ValueError):
                result[stock] = 0

        return result

    @validate_lifecycle
    def filter_stock_by_status(
        self, stocks: str | list[str], filter_type: list[str] | None = None, query_date: str | None = None
    ) -> list[str]:
        """过滤指定状态股票代码"""
        if isinstance(stocks, str):
            stocks = [stocks]
        if filter_type is None:
            filter_type = ["ST", "HALT", "DELISTING"]

        filtered = list(stocks)
        for status_type in filter_type:
            status_map = self.get_stock_status(filtered, query_type=status_type, query_date=query_date)
            filtered = [s for s in filtered if not status_map.get(s, False)]
            if not filtered:
                break
        return filtered

    @validate_lifecycle
    def get_current_kline_count(self) -> int:
        """获取当前时间的分钟bar数量（股票业务）"""
        dt = self.context.current_dt
        if dt is None:
            return 0

        hhmm = dt.hour * 100 + dt.minute
        # A股分钟bar从09:31开始
        if hhmm < 931:
            return 0
        # 上午 09:31-11:30 => 120根
        if hhmm <= 1130:
            return (dt.hour - 9) * 60 + dt.minute - 30
        # 午休
        if hhmm < 1301:
            return 120
        # 下午 13:01-15:00 => 120根
        if hhmm <= 1500:
            return 120 + (dt.hour - 13) * 60 + dt.minute
        return 240

    # ==================== 交易API ====================

    def _get_execution_price(self, security: str, limit_price: Optional[float], amount: int) -> Optional[float]:
        """获取执行价格，失败时记录警告并返回 None。"""
        price = self.order_processor.get_execution_price(security, limit_price, amount > 0)
        if price is None:
            self.log.warning(t("api.order_no_price", stock=security))
        return price

    def _adjust_buy_amount(self, security: str, amount: int, price: float) -> Optional[int]:
        """买入时资金不足自动调整数量（Ptrade行为）。返回调整后的 amount 或 None。

        Ptrade 行为：同一 handle_data 内前一笔订单已付的手续费不计入后续订单的可用现金检查。
        即 available = portfolio._cash + _daily_buy_commission（当日已付手续费）。
        调整量以 test_cost <= available（不含手续费）为通过条件。
        """
        daily_commission = getattr(self.context, "_daily_buy_commission", 0.0)
        available_cash = self.context.portfolio._cash + daily_commission
        min_lot = self.lot_size

        cost = amount * price
        commission = self.order_processor.calculate_commission(amount, price, is_sell=False)
        if cost + commission <= self.context.portfolio._cash:
            return amount

        adjusted = int(available_cash / price / min_lot) * min_lot

        while adjusted >= min_lot:
            test_cost = adjusted * price
            if test_cost <= available_cash:  # Ptrade: 手续费不计入调整量检查
                break
            adjusted -= min_lot

        if adjusted < min_lot:
            self.log.warning(t("api.buy_no_cash", stock=security))
            return None

        self.log.warning(t("api.buy_adjusted", stock=security, amount=adjusted))
        return adjusted

    def _adjust_sell_amount(self, security: str, amount: int) -> int:
        """卖出时整手约束（Ptrade行为）。amount为负数，返回调整后的负数或0。"""
        abs_amount = abs(amount)
        min_lot = self.lot_size
        rounded = (abs_amount // min_lot) * min_lot

        # 剩余不足一手时卖出全部（清零股）
        current = self.context.portfolio.positions.get(security)
        if current and current.amount > 0:
            remaining = current.amount - rounded
            if 0 < remaining < min_lot:
                rounded = current.amount

        # 科创板单笔申报数量不足200股约束（非清仓时）
        if self.lot_size == 100 and security.startswith("688") and rounded < 200:
            current_amount = current.amount if current else 0
            if rounded < current_amount:  # 非清仓
                self.log.warning(t("api.star_min_200"))
                return 0

        return -rounded

    def _submit_order(self, security: str, amount: int, price: float) -> Optional[str]:
        """创建订单→注册blotter→执行→更新状态→收集回调。返回 order_id 或 None。"""
        bar_volume = self.order_processor._get_current_bar_volume(security)
        if bar_volume is None:
            self.log.warning(t("order.no_price", stock=security))
            return None
        if bar_volume == 0:
            self.log.warning(t("order.volume_zero", stock=security))
            return None

        fill_amount = self.order_processor.limit_fill_amount(
            security, abs(amount), bar_volume
        )
        if fill_amount == 0:
            return None
        if amount < 0:
            fill_amount = self.order_processor.adjust_sell_amount_for_t1(
                security, fill_amount
            )
        amount = fill_amount if amount > 0 else -fill_amount

        order_id, order = self.order_processor.create_order(security, amount, price)
        if self.context and self.context.blotter:
            self.context.blotter.all_orders.append(order)

        if amount > 0:
            # self.log.info(t("api.order_buy", order_id=order_id, stock=security, amount=amount))
            success = self.order_processor.execute_buy(security, amount, price)
        else:
            # self.log.info(t("api.order_sell", order_id=order_id, stock=security, amount=abs(amount)))
            success = self.order_processor.execute_sell(security, abs(amount), price)

        if success:
            order.status = "8"
            order.filled = amount
            if self.context and self.context.blotter:
                self.context.blotter.filled_orders.append(order)

        # 收集 on_order_response 回调数据（ptrade 实盘格式）
        self._pending_order_callbacks.append(
            {
                "entrust_no": order.entrust_no or order_id[:6],
                "error_info": "" if success else "委托失败",
                "order_time": str(self.context.current_dt),
                "stock_code": security,
                "amount": abs(amount),
                "price": float(price),
                "business_amount": float(abs(amount)) if success else 0.0,
                "status": "8" if success else "9",
                "entrust_type": "0",
                "entrust_prop": "0",
                "order_id": order_id,
            }
        )

        # 收集 on_trade_response 回调数据（仅成功时）
        if success:
            self._pending_trade_callbacks.append(
                {
                    "entrust_no": order.entrust_no or order_id[:6],
                    "business_time": str(self.context.current_dt),
                    "stock_code": security,
                    "entrust_bs": "1" if amount > 0 else "2",
                    "business_amount": float(abs(amount)),
                    "business_price": float(price),
                    "business_balance": float(abs(amount) * price),
                    "business_id": order_id[:8],
                    "status": "0",
                    "order_id": order_id,
                }
            )

        return order_id if success else None


    @validate_lifecycle
    def order(self, security: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """买卖指定数量的股票

        Args:
            security: 股票代码
            amount: 交易数量，正数表示买入，负数表示卖出
            limit_price: 买卖限价

        Returns:
            订单id或None
        """
        if amount == 0:
            return None
        if amount > 0:
            amount = (amount // self.lot_size) * self.lot_size
            if amount == 0:
                return None
        price = self._get_execution_price(security, limit_price, amount)
        if price is None:
            return None
        if amount > 0:
            amount = self._adjust_buy_amount(security, amount, price)
            if amount is None:
                return None
        else:
            # A股卖出必须整手(100股)，向下取整；清仓时可卖零股
            amount = self._adjust_sell_amount(security, amount)
            if amount == 0:
                return None
        return self._submit_order(security, amount, price)

    @validate_lifecycle
    def order_market(
        self, security: str, amount: int, market_type: int, limit_price: float | None = None
    ) -> None:
        """市价委托（当前本地回测不支持交易市价接口）"""
        _ = (security, amount, market_type, limit_price)
        self._unsupported_api("order_market", "交易接口")

    @validate_lifecycle
    def ipo_stocks_order(self, market_type: int | None = None, black_stocks: str | list[str] | None = None) -> None:
        """新股一键申购（当前本地回测不支持交易申购接口）"""
        _ = (market_type, black_stocks)
        self._unsupported_api("ipo_stocks_order", "交易接口")

    @validate_lifecycle
    def after_trading_order(self, security: str, amount: int, entrust_price: float) -> None:
        """盘后固定价委托（当前本地回测不支持交易盘后委托接口）"""
        _ = (security, amount, entrust_price)
        self._unsupported_api("after_trading_order", "交易接口")

    @validate_lifecycle
    def after_trading_cancel_order(self, order_param: Any) -> None:
        """盘后固定价委托撤单（当前本地回测不支持交易盘后委托接口）"""
        _ = order_param
        self._unsupported_api("after_trading_cancel_order", "交易接口")

    @validate_lifecycle
    def etf_basket_order(
        self,
        etf_code: str,
        amount: int,
        price_style: str | None = None,
        position: bool = True,
        info: dict[str, Any] | None = None,
    ) -> None:
        """ETF成分券篮子下单（当前本地回测不支持ETF交易接口）"""
        _ = (etf_code, amount, price_style, position, info)
        self._unsupported_api("etf_basket_order", "ETF交易接口")

    @validate_lifecycle
    def etf_purchase_redemption(
        self, etf_code: str, amount: int, limit_price: float | None = None
    ) -> None:
        """ETF基金申赎（当前本地回测不支持ETF交易接口）"""
        _ = (etf_code, amount, limit_price)
        self._unsupported_api("etf_purchase_redemption", "ETF交易接口")

    @validate_lifecycle
    def order_tick(
        self, sid: str, amount: int, priceGear: str = "1", limit_price: float | None = None
    ) -> None:
        """tick行情触发买卖（当前本地回测不支持tick交易接口）"""
        _ = (sid, amount, priceGear, limit_price)
        self._unsupported_api("order_tick", "tick交易接口")

    @validate_lifecycle
    def cancel_order_ex(self, order_param: dict[str, Any]) -> None:
        """交易账户撤单（当前本地回测不支持账户级订单接口）"""
        _ = order_param
        self._unsupported_api("cancel_order_ex", "交易接口")

    @validate_lifecycle
    def debt_to_stock_order(self, security: str, amount: int) -> None:
        """债转股委托（当前本地回测不支持可转债交易接口）"""
        _ = (security, amount)
        self._unsupported_api("debt_to_stock_order", "可转债交易接口")

    @validate_lifecycle
    def margin_trade(
        self, security: str, amount: int, limit_price: float | None = None, market_type: str | None = None
    ) -> Optional[str]:
        """担保品买卖（当前本地回测不支持两融）"""
        _ = (security, amount, limit_price, market_type)
        self._unsupported_api("margin_trade", "两融接口")

    @validate_lifecycle
    def margincash_open(
        self, security: str, amount: int, limit_price: float | None = None, market_type: int | None = None
    ) -> Optional[str]:
        """融资买入（当前本地回测不支持两融交易）"""
        _ = (security, amount, limit_price, market_type)
        self._unsupported_api("margincash_open", "两融接口")

    @validate_lifecycle
    def margincash_close(self, security: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """卖券还款（当前本地回测不支持两融交易）"""
        _ = (security, amount, limit_price)
        self._unsupported_api("margincash_close", "两融接口")

    @validate_lifecycle
    def margincash_direct_refund(self, value: float) -> None:
        """直接还款（当前本地回测不支持两融交易）"""
        _ = value
        self._unsupported_api("margincash_direct_refund", "两融接口")

    @validate_lifecycle
    def marginsec_open(self, security: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """融券卖出（当前本地回测不支持两融交易）"""
        _ = (security, amount, limit_price)
        self._unsupported_api("marginsec_open", "两融接口")

    @validate_lifecycle
    def marginsec_close(self, security: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """买券还券（当前本地回测不支持两融交易）"""
        _ = (security, amount, limit_price)
        self._unsupported_api("marginsec_close", "两融接口")

    @validate_lifecycle
    def marginsec_direct_refund(self, security: str, amount: int) -> None:
        """直接还券（当前本地回测不支持两融交易）"""
        _ = (security, amount)
        self._unsupported_api("marginsec_direct_refund", "两融接口")

    @validate_lifecycle
    def get_margincash_stocks(self) -> None:
        """获取融资标的列表（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_margincash_stocks", "两融接口")

    @validate_lifecycle
    def get_marginsec_stocks(self) -> None:
        """获取融券标的列表（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_marginsec_stocks", "两融接口")

    @validate_lifecycle
    def get_margin_contract(self) -> None:
        """合约查询（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_margin_contract", "两融接口")

    @validate_lifecycle
    def get_margin_contractreal(self) -> None:
        """实时合约流水查询（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_margin_contractreal", "两融接口")

    @validate_lifecycle
    def get_margin_assert(self) -> dict[str, Any]:
        """信用资产查询（当前本地回测不支持两融）"""
        return self._get_margin_asset_impl("get_margin_assert")

    def _get_margin_asset_impl(self, api_name: str) -> dict[str, Any]:
        """信用资产查询内部实现（兼容别名共用）"""
        _ = api_name
        self._unsupported_api(api_name, "两融接口")

    @validate_lifecycle
    def get_margin_asset(self) -> dict[str, Any]:
        """信用资产查询（山西命名）"""
        return self._get_margin_asset_impl("get_margin_asset")

    @validate_lifecycle
    def get_assure_security_list(self) -> None:
        """担保券查询（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_assure_security_list", "两融接口")

    @validate_lifecycle
    def get_margincash_open_amount(self, security: str, price: float | None = None) -> None:
        """融资标的最大可买数量查询（当前本地回测不支持两融查询）"""
        _ = (security, price)
        self._unsupported_api("get_margincash_open_amount", "两融接口")

    @validate_lifecycle
    def get_margincash_close_amount(self, security: str, price: float | None = None) -> None:
        """卖券还款标的最大可卖数量查询（当前本地回测不支持两融查询）"""
        _ = (security, price)
        self._unsupported_api("get_margincash_close_amount", "两融接口")

    @validate_lifecycle
    def get_marginsec_open_amount(self, security: str, price: float | None = None) -> None:
        """融券标的最大可卖数量查询（当前本地回测不支持两融查询）"""
        _ = (security, price)
        self._unsupported_api("get_marginsec_open_amount", "两融接口")

    @validate_lifecycle
    def get_marginsec_close_amount(self, security: str, price: float | None = None) -> None:
        """买券还券标的最大可买数量查询（当前本地回测不支持两融查询）"""
        _ = (security, price)
        self._unsupported_api("get_marginsec_close_amount", "两融接口")

    @validate_lifecycle
    def get_margin_entrans_amount(self, security: str) -> None:
        """现券还券数量查询（当前本地回测不支持两融查询）"""
        _ = security
        self._unsupported_api("get_margin_entrans_amount", "两融接口")

    @validate_lifecycle
    def get_enslo_security_info(self) -> None:
        """融券头寸信息查询（当前本地回测不支持两融查询）"""
        self._unsupported_api("get_enslo_security_info", "两融接口")

    @validate_lifecycle
    def buy_open(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期货多开（当前本地回测不支持期货）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("buy_open", "期货接口")

    @validate_lifecycle
    def sell_close(
        self, contract: str, amount: int, limit_price: float | None = None, close_today: bool = False
    ) -> Optional[str]:
        """期货多平（当前本地回测不支持期货）"""
        _ = (contract, amount, limit_price, close_today)
        self._unsupported_api("sell_close", "期货接口")

    @validate_lifecycle
    def sell_open(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期货空开（当前本地回测不支持期货）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("sell_open", "期货接口")

    @validate_lifecycle
    def buy_close(
        self, contract: str, amount: int, limit_price: float | None = None, close_today: bool = False
    ) -> Optional[str]:
        """期货空平（当前本地回测不支持期货）"""
        _ = (contract, amount, limit_price, close_today)
        self._unsupported_api("buy_close", "期货接口")

    @validate_lifecycle
    def option_buy_open(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期权权利仓开仓（当前本地回测不支持期权交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("option_buy_open", "期权接口")

    @validate_lifecycle
    def option_sell_close(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期权权利仓平仓（当前本地回测不支持期权交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("option_sell_close", "期权接口")

    @validate_lifecycle
    def option_sell_open(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期权义务仓开仓（当前本地回测不支持期权交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("option_sell_open", "期权接口")

    @validate_lifecycle
    def option_buy_close(self, contract: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """期权义务仓平仓（当前本地回测不支持期权交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("option_buy_close", "期权接口")

    @validate_lifecycle
    def open_prepared(self, contract: str, amount: int, limit_price: float | None = None) -> None:
        """备兑开仓（当前本地回测不支持期权备兑交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("open_prepared", "期权接口")

    @validate_lifecycle
    def close_prepared(self, contract: str, amount: int, limit_price: float | None = None) -> None:
        """备兑平仓（当前本地回测不支持期权备兑交易）"""
        _ = (contract, amount, limit_price)
        self._unsupported_api("close_prepared", "期权接口")

    @validate_lifecycle
    def option_exercise(self, contract: str, amount: int) -> None:
        """期权行权（当前本地回测不支持期权行权）"""
        _ = (contract, amount)
        self._unsupported_api("option_exercise", "期权接口")

    @validate_lifecycle
    def option_covered_lock(self, code: str, amount: int) -> None:
        """期权标的备兑锁定（当前本地回测不支持期权备兑交易）"""
        _ = (code, amount)
        self._unsupported_api("option_covered_lock", "期权接口")

    @validate_lifecycle
    def option_covered_unlock(self, code: str, amount: int) -> None:
        """期权标的备兑解锁（当前本地回测不支持期权备兑交易）"""
        _ = (code, amount)
        self._unsupported_api("option_covered_unlock", "期权接口")

    @validate_lifecycle
    def get_covered_lock_amount(self, code: str) -> None:
        """获取期权标的允许备兑锁定数量（当前本地回测不支持期权备兑查询）"""
        _ = code
        self._unsupported_api("get_covered_lock_amount", "期权接口")

    @validate_lifecycle
    def get_covered_unlock_amount(self, code: str) -> None:
        """获取期权标的允许备兑解锁数量（当前本地回测不支持期权备兑查询）"""
        _ = code
        self._unsupported_api("get_covered_unlock_amount", "期权接口")

    @validate_lifecycle
    def order_target(self, security: str, amount: int, limit_price: float | None = None) -> Optional[str]:
        """下单到目标数量

        Args:
            security: 股票代码
            amount: 期望的最终数量
            limit_price: 买卖限价

        Returns:
            订单id或None
        """
        current_amount = 0
        if security in self.context.portfolio.positions:
            current_amount = self.context.portfolio.positions[security].amount
        delta = amount - current_amount
        if delta == 0:
            return None

        if delta > 0:
            delta = (delta // self.lot_size) * self.lot_size
            if delta == 0:
                return None
        else:
            sell_amount = abs(delta)
            current_remainder = current_amount % self.lot_size
            removes_existing_odd_lot = (
                current_remainder > 0 and amount % self.lot_size == 0
            )
            if amount != 0 and not removes_existing_odd_lot:
                sell_amount = (sell_amount // self.lot_size) * self.lot_size
                if sell_amount == 0:
                    return None
                delta = -sell_amount

        price = self._get_execution_price(security, limit_price, delta)
        if price is None:
            return None
        if delta < 0:
            sell_amount = abs(delta)
            # 科创板非清仓卖出必须≥200股
            if self.lot_size == 100 and security.startswith("688"):
                if sell_amount < 200 and sell_amount < current_amount:
                    self.log.warning(t("api.star_min_200"))
                    return None
        return self._submit_order(security, delta, price)

    @validate_lifecycle
    def order_value(self, security: str, value: float, limit_price: float | None = None) -> Optional[str]:
        """按金额下单

        Args:
            security: 股票代码
            value: 股票价值，正数买入，负数卖出
            limit_price: 买卖限价

        Returns:
            订单id或None
        """
        if abs(value) < 1:
            return None
        is_buy = value > 0
        price = self._get_execution_price(security, limit_price, 1 if is_buy else -1)
        if price is None:
            return None

        min_lot = self.lot_size

        if is_buy:
            target_amount = int(value / price / min_lot) * min_lot
            if target_amount < min_lot:
                self.log.warning(
                    t(
                        "api.order_value_insufficient",
                        stock=security,
                        min_lot=min_lot,
                        value="{:.2f}".format(value),
                        price="{:.2f}".format(price),
                        cash="{:.2f}".format(self.context.portfolio._cash),
                    )
                )
                return None
            # 科创板买入最小申报数量 200 股
            if self.lot_size == 100 and security.startswith("688") and target_amount < 200:
                self.log.warning(t("api.star_min_200"))
                return None
            amount = self._adjust_buy_amount(security, target_amount, price)
            if amount is None:
                return None
            return self._submit_order(security, amount, price)
        else:
            sell_value = abs(value)
            target_amount = int(sell_value / price / min_lot) * min_lot

            if security not in self.context.portfolio.positions:
                self.log.warning(t("api.sell_no_position", stock=security))
                return None

            position = self.context.portfolio.positions[security]
            if target_amount >= position.amount:
                target_amount = position.amount
            elif target_amount < min_lot:
                self.log.warning(
                    t(
                        "api.sell_value_insufficient",
                        stock=security,
                        min_lot=min_lot,
                        value="{:.2f}".format(sell_value),
                        price="{:.2f}".format(price),
                    )
                )
                return None

            return self._submit_order(security, -target_amount, price)

    @validate_lifecycle
    def order_target_value(self, security: str, value: float, limit_price: float | None = None) -> Optional[str]:
        """调整股票持仓市值到目标价值

        Args:
            security: 股票代码
            value: 期望的股票最终价值
            limit_price: 买卖限价

        Returns:
            订单id或None
        """
        current_amount = 0
        if security in self.context.portfolio.positions:
            current_amount = self.context.portfolio.positions[security].amount

        is_buy = value > current_amount * (
            self.context.portfolio.positions[security].last_sale_price if current_amount > 0 else 0
        )
        execution_price = self.order_processor.get_execution_price(security, limit_price, is_buy)
        if execution_price is None:
            self.log.warning(t("api.order_no_price", stock=security))
            return None

        min_lot = self.lot_size
        if value <= 0:
            target_amount = 0
        else:
            target_amount = int(value / execution_price / min_lot) * min_lot

        delta = target_amount - current_amount
        if delta == 0:
            return None

        # 委托给 order_target 按数量交易
        return self.order_target(security, target_amount, limit_price)

    @validate_lifecycle
    def get_open_orders(self, security: str | None = None) -> list:
        """获取未成交订单"""
        if self.context and self.context.blotter:
            open_orders = self.context.blotter.open_orders
            if security is None:
                return open_orders
            return [o for o in open_orders if o.symbol == security]
        return []

    @validate_lifecycle
    def get_orders(self, security: str | None = None) -> list:
        """获取当日全部订单

        Args:
            security: 股票代码，None表示获取所有订单

        Returns:
            订单列表
        """
        if not self.context or not self.context.blotter:
            return []

        current_date = pd.Timestamp(self.context.current_dt).date()
        all_orders = [
            order
            for order in self.context.blotter.all_orders
            if order.dt is not None and pd.Timestamp(order.dt).date() == current_date
        ]
        if security is None:
            return all_orders
        else:
            return [o for o in all_orders if o.symbol == security]

    @validate_lifecycle
    def get_all_orders(self, security: str | None = None) -> None:
        """获取账户当日全部订单（当前本地回测不支持账户级订单接口）"""
        _ = security
        self._unsupported_api("get_all_orders", "交易接口")

    @validate_lifecycle
    def get_order(self, order_id: str) -> list[Any]:
        """获取指定订单

        Args:
            order_id: 订单id

        Returns:
            只包含指定Order对象的列表；未找到返回空列表
        """
        if not self.context or not self.context.blotter:
            return []

        for order in self.context.blotter.all_orders:
            if order.id == order_id:
                return [order]
        return []

    def _iter_filled_orders(self) -> list[Any]:
        """Return filled Order objects for the current trading day."""
        if not self.context or not self.context.blotter:
            return []
        orders = list(getattr(self.context.blotter, "filled_orders", []))
        if self.context.current_dt is None:
            return orders
        current_date = pd.Timestamp(self.context.current_dt).date()
        return [
            order
            for order in orders
            if order.dt is not None and pd.Timestamp(order.dt).date() == current_date
        ]

    @validate_lifecycle
    def get_trades(self) -> dict[str, list[list[Any]]]:
        """获取当日成交订单

        Returns:
            按订单编号分组的成交明细字典
        """
        trades = {}
        for order in self._iter_filled_orders():
            volume = float(abs(order.filled if order.filled else order.amount))
            price = float(order.limit) if order.limit is not None else 0.0
            side = "买" if order.amount > 0 else "卖"
            order_id = str(order.id)
            trades.setdefault(order_id, []).append(
                [
                    order_id[:8],
                    order.entrust_no or order_id[:6],
                    _to_ptrade_long_suffix(order.symbol),
                    side,
                    volume,
                    price,
                    volume * price,
                    str(order.dt) if order.dt else "",
                ]
            )
        return trades

    @validate_lifecycle
    def get_position(self, security: str) -> Position:
        """获取持仓信息

        Args:
            security: 股票代码

        Returns:
            Position对象；未持仓时返回amount为0的空持仓对象
        """
        if self.context and self.context.portfolio:
            position = self.context.portfolio.positions.get(security)
            if position is not None:
                return position
            return Position(security, 0, 0.0, t_plus_1=self.context.t_plus_1)
        return Position(security, 0, 0.0)

    @validate_lifecycle
    def get_positions(self, security: str | list[str] | None = None) -> dict[str, Position]:
        """获取多支股票持仓信息

        Args:
            security: 股票代码或代码列表，None表示获取所有持仓

        Returns:
            dict: {stock: Position对象}
        """
        if not self.context or not self.context.portfolio:
            return {}

        positions = self.context.portfolio.positions
        if security is None:
            return positions.copy()
        security_list = [security] if isinstance(security, str) else security
        return {s: positions[s] for s in security_list if s in positions}

    @validate_lifecycle
    def cancel_order(self, order_param: Any) -> None:
        """取消订单"""
        if self.context and self.context.blotter:
            self.context.blotter.cancel_order(order_param)
        return None

    def flush_order_callbacks(self) -> list[dict]:
        """取出并清空待处理的订单回调数据"""
        callbacks = self._pending_order_callbacks
        self._pending_order_callbacks = []
        return callbacks

    def flush_trade_callbacks(self) -> list[dict]:
        """取出并清空待处理的成交回调数据"""
        callbacks = self._pending_trade_callbacks
        self._pending_trade_callbacks = []
        return callbacks

    # ==================== 配置API ====================

    @validate_lifecycle
    def set_benchmark(self, sids: str) -> None:
        """设置基准（支持指数和普通股票）,会自动添加到benchmark_data"""
        benchmark = sids
        benchmark = _normalize_code(benchmark)
        # 优先从benchmark_data中查找（指数）
        if benchmark in self.data_context.benchmark_data:
            self.context.benchmark = benchmark
            self.log.info(t("api.benchmark_index", benchmark=benchmark))
            return

        # 如果不在benchmark_data中，检查stock_data_dict（普通股票/指数）
        if benchmark in self.data_context.stock_data_dict:
            self.context.benchmark = benchmark
            # 动态添加到benchmark_data供后续使用
            self.data_context.benchmark_data[benchmark] = self.data_context.stock_data_dict[benchmark]
            self.log.info(t("api.benchmark_stock", benchmark=benchmark))
            return

        # 都不存在，警告
        self.log.warning(t("api.benchmark_not_found", benchmark=benchmark))

    @validate_lifecycle
    def set_universe(self, security_list: str | list[str]) -> None:
        """设置股票池并预加载数据"""
        stocks = security_list
        # api_doc.txt: set_universe 用于设定 get_history 的默认 security_list，
        # 返回顺序与传入股票池一致。旧实现用 set 存储，hash 随机化会让
        # 多股票结果的列/行顺序不可复现。
        ordered = list(dict.fromkeys(stocks if isinstance(stocks, list) else [stocks]))
        previous = set(self.active_universe)
        to_preload = [s for s in ordered if s not in previous]
        if to_preload:
            self.log.debug(f"预加载 {len(to_preload)} 只新股票数据")
            self._ensure_market_data(to_preload)
        self.active_universe = ordered
        self.log.debug(f"股票池更新: {len(self.active_universe)} 只")

    def is_trade(self) -> bool:
        """是否实盘"""
        return False

    @validate_lifecycle
    def set_commission(
        self, commission_ratio: float = 0.0003, min_commission: float = 5.0, type: str = "STOCK"
    ) -> None:
        """设置交易佣金"""
        # 验证ptrade平台限制：佣金费率和最低交易佣金不能小于或者等于0
        if commission_ratio is not None and commission_ratio <= 0:
            raise ValueError("IQInvalidArgument: 佣金费率和最低交易佣金不能小于或者等于0,请核对后重新输入")
        if min_commission is not None and min_commission <= 0:
            raise ValueError("IQInvalidArgument: 佣金费率和最低交易佣金不能小于或者等于0,请核对后重新输入")

        if commission_ratio is not None:
            kwargs = {"commission_ratio": commission_ratio}
        else:
            kwargs = {}
        if min_commission is not None:
            kwargs["min_commission"] = min_commission
        if type is not None:
            kwargs["commission_type"] = type
        if kwargs:
            config.update_trading_config(**kwargs)

    @validate_lifecycle
    def set_slippage(self, slippage: float = 0.001) -> None:
        """设置滑点"""
        if slippage is not None:
            config.update_trading_config(
                slippage=slippage,
                fixed_slippage=0.0,
            )

    @validate_lifecycle
    def set_fixed_slippage(self, fixedslippage: float = 0.0) -> None:
        """设置固定滑点"""
        if fixedslippage is not None:
            config.update_trading_config(
                fixed_slippage=fixedslippage,
                slippage=0.0,
            )

    @validate_lifecycle
    def set_limit_mode(self, limit_mode: str = "LIMIT") -> None:
        """设置下单限制模式"""
        if limit_mode not in {"LIMIT", "UNLIMITED"}:
            raise ValueError("limit_mode must be 'LIMIT' or 'UNLIMITED'")
        config.update_trading_config(limit_mode=limit_mode)

    @validate_lifecycle
    def set_volume_ratio(self, volume_ratio: float = 0.25) -> None:
        """设置成交比例

        Args:
            volume_ratio: 成交比例，默认0.25，即单笔最大成交量为当日成交量的1/4
        """
        config.update_trading_config(volume_ratio=volume_ratio)

    @validate_lifecycle
    def set_yesterday_position(self, poslist: list[dict]) -> None:
        """设置底仓（回测用）

        Args:
            poslist: 持仓列表，每个元素为字典 {'sid': 股票代码, 'amount': 数量, 'enable_amount': 可用数量, 'cost_basis': 成本价}
        """
        if not isinstance(poslist, list):
            self.log.warning(t("api.pos_not_list"))
            return

        for pos_info in poslist:
            security = pos_info.get("sid")
            amount = int(pos_info.get("amount", 0))
            cost_basis = float(pos_info.get("cost_basis", 0))
            enable_amount = int(pos_info.get("enable_amount", amount))

            if security and amount > 0 and cost_basis > 0:
                position = Position(security, amount, cost_basis, t_plus_1=self.context.t_plus_1)
                position.enable_amount = enable_amount
                portfolio = self.context.portfolio
                portfolio.positions[security] = position
                lot_date = self.context.previous_date or self.context.current_dt
                portfolio._position_lots[security] = [
                    {'date': lot_date, 'amount': amount, 'dividends': [], 'dividends_total': 0.0}
                ]
                portfolio._invalidate_cache()
                self.log.info(t("api.set_position", stock=security, amount=amount, cost=cost_basis))

    @validate_lifecycle
    def set_future_commission(self, transaction_code: str, commission: float) -> None:
        """设置期货手续费（当前本地回测不支持期货）"""
        _ = (transaction_code, commission)
        self._unsupported_api("set_future_commission", "期货接口")

    @validate_lifecycle
    def set_margin_rate(self, transaction_code: str, margin_rate: float) -> None:
        """设置期货保证金比例（当前本地回测不支持期货）"""
        _ = (transaction_code, margin_rate)
        self._unsupported_api("set_margin_rate", "期货接口")

    @validate_lifecycle
    def get_margin_rate(self, transaction_code: str) -> float | dict[str, float] | None:
        """获取期货保证金比例（当前本地回测不支持期货）"""
        _ = transaction_code
        self._unsupported_api("get_margin_rate", "期货接口")

    @validate_lifecycle
    def run_interval(self, context: Any, func: Callable, seconds: int = 10) -> None:
        """定时运行函数（秒级，仅实盘）

        Args:
            context: Context对象
            func: 自定义函数，接受context参数
            seconds: 时间间隔（秒），最小3秒
        """
        _ = (context, func, seconds)  # 回测中不执行
        pass

    @validate_lifecycle
    def run_daily(self, context: Any, func: Callable, time: str = "9:31") -> None:
        """注册每日定时任务

        Args:
            context: Context对象
            func: 自定义函数，接受context参数
            time: 触发时间，格式HH:MM
        """
        hour, minute = time.split(':')
        normalized_time = f'{hour.zfill(2)}:{minute.zfill(2)}'
        self._daily_tasks.append((func, normalized_time))

    @validate_lifecycle
    def set_parameters(self, **kwargs) -> None:
        """设置策略配置参数

        Args:
            kwargs: a=b形式的策略参数
        """
        if "broker_profile" in kwargs:
            raise ValueError(t("api.set_parameters_broker_profile_readonly"))
        if not hasattr(self.context, "params"):
            self.context.params = {}
        self.context.params.update(kwargs)

    @validate_lifecycle
    def convert_position_from_csv(self, path: str) -> list[dict]:
        """从CSV文件获取设置底仓的参数列表

        Args:
            path: CSV文件路径

        Returns:
            list: 持仓列表，格式为 [{'sid': 股票代码, 'enable_amount': 可用数量, 'amount': 数量, 'cost_basis': 成本价}, ...]
        """
        file_path = path
        df = pd.read_csv(file_path)
        positions = []
        for _, row in df.iterrows():
            amount = int(row.get("amount", 0))
            positions.append(
                {
                    "sid": row.get("sid"),
                    "enable_amount": int(row.get("enable_amount", amount)),
                    "amount": amount,
                    "cost_basis": float(row.get("cost_basis", 0)),
                }
            )
        return positions

    def get_user_name(self) -> str:
        """获取登录终端的资金账号

        Returns:
            str: 资金账号（回测返回模拟账号）
        """
        return "backtest_user"

    @validate_lifecycle
    def create_dir(self, user_path: str | None = None) -> str | bool | None:
        """创建文件目录路径"""
        base = Path(self.get_research_path())
        profile = self.broker_profile
        if profile == "shanxi":
            if not user_path:
                raise ValueError(t("api.create_dir_user_path_required", profile=profile))
            target = base / user_path
            target.mkdir(parents=True, exist_ok=True)
            return True

        target = base if not user_path else (base / user_path)
        target.mkdir(parents=True, exist_ok=True)
        if profile in ("guosheng", "dongguan"):
            return None
        return str(target)

    @validate_lifecycle
    def get_trades_file(self, save_path: str = "") -> Optional[str]:
        """导出当日成交为CSV文件（回测简化版）"""
        base = Path(self.get_research_path())
        target_dir = base / save_path if save_path else base
        target_dir.mkdir(parents=True, exist_ok=True)
        date_str = self.context.current_dt.strftime("%Y%m%d") if self.context and self.context.current_dt else "unknown"
        file_path = target_dir / ("trades_%s.csv" % date_str)

        rows = []
        for order in self._iter_filled_orders():
            side = "buy" if order.amount > 0 else "sell"
            rows.append(
                {
                    "order_id": order.id,
                    "trading_id": order.id,
                    "entrust_id": order.entrust_no or "",
                    "security_code": order.symbol,
                    "order_type": side,
                    "volume": abs(order.filled if order.filled else order.amount),
                    "price": order.limit if order.limit is not None else 0.0,
                    "total_money": abs((order.filled if order.filled else order.amount) * (order.limit or 0.0)),
                    "trading_fee": 0.0,
                    "trade_time": str(order.dt) if order.dt else "",
                }
            )

        pd.DataFrame(rows).to_csv(file_path, index=False, encoding="utf-8")
        return str(file_path)

    @validate_lifecycle
    def get_frequency(self) -> str:
        """获取当前业务周期"""
        return getattr(self.context, "frequency", "1d")

    @validate_lifecycle
    def get_business_type(self) -> str:
        """获取当前策略业务类型"""
        return str(getattr(self.context.g, "business_type", "stock")).upper()

    @validate_lifecycle
    def get_cb_list(self) -> list[str]:
        """当前仍在市的可转债代码。沪市 11、深市 12，不含转债指数。"""
        frame = self._instrument_frame()
        if frame is None:
            return []
        names, codes = self._names_and_codes(frame)
        mask = codes.str.startswith(("11", "12")) & ~names.str.contains("指数")
        return self._active_symbols(mask, None)

    @validate_lifecycle
    def get_etf_list(self) -> list[str]:
        """当前仍在市的 ETF 代码。LOF 和 REITs 不在这个列表里。"""
        frame = self._instrument_frame()
        if frame is None:
            return []
        names, codes = self._names_and_codes(frame)
        upper = names.str.upper()
        mask = (upper.str.contains("ETF") | codes.str.startswith(("51", "56", "58", "159"))) & ~upper.str.contains("REIT")
        return self._active_symbols(mask, None)

    @validate_lifecycle
    def get_ipo_stocks(self) -> None:
        """获取当日IPO申购标的（当前本地回测不支持交易申购接口）"""
        self._unsupported_api("get_ipo_stocks", "交易接口")

    @validate_lifecycle
    def get_deliver(self, start_date: str, end_date: str) -> None:
        """获取历史交割单信息（当前本地回测不支持交易账户查询）"""
        _ = (start_date, end_date)
        self._unsupported_api("get_deliver", "交易接口")

    @validate_lifecycle
    def get_fundjour(self, start_date: str, end_date: str) -> None:
        """获取历史资金流水信息（当前本地回测不支持交易账户查询）"""
        _ = (start_date, end_date)
        self._unsupported_api("get_fundjour", "交易接口")

    @validate_lifecycle
    def get_trade_name(self) -> None:
        """获取交易名称（当前本地回测不支持交易账户查询）"""
        self._unsupported_api("get_trade_name", "交易接口")

    @validate_lifecycle
    def get_opt_objects(self, date: str | None = None) -> list[str]:
        """获取期权标的列表（回测简化版）"""
        _ = date
        return []

    @validate_lifecycle
    def get_opt_last_dates(
        self, security: str | None = None, date: str | None = None, underlying: str | None = None
    ) -> list[str]:
        """获取期权到期日列表（回测简化版）"""
        _ = (security, date, underlying)
        return []

    @validate_lifecycle
    def get_opt_contracts(
        self,
        security: str | None = None,
        date: str | None = None,
        underlying: str | None = None,
        last_date: str | None = None,
    ) -> list[str]:
        """获取期权合约列表（回测简化版）"""
        _ = (security, date, underlying, last_date)
        return []

    @validate_lifecycle
    def get_contract_info(self, contract: str) -> dict[str, Any]:
        """获取合约信息（回测简化版）"""
        return {"contract": contract}

    @validate_lifecycle
    def send_email(self, *args, **kwargs) -> None:
        """发送邮件（当前本地回测不支持交易通知接口）"""
        _ = (args, kwargs)
        self._unsupported_api("send_email", "交易通知接口")

    @validate_lifecycle
    def send_qywx(self, *args, **kwargs) -> None:
        """发送企业微信消息（当前本地回测不支持交易通知接口）"""
        _ = (args, kwargs)
        self._unsupported_api("send_qywx", "交易通知接口")

    @validate_lifecycle
    def permission_test(self, account: str | None = None, end_date: str | None = None) -> None:
        """权限校验（当前本地回测不支持交易授权接口）"""
        _ = (account, end_date)
        self._unsupported_api("permission_test", "交易授权接口")

    # ==================== 技术指标API ====================

    def get_MACD(
        self, close: np.ndarray, short: int = 12, long: int = 26, m: int = 9
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """计算MACD指标（异同移动平均线）

        Args:
            close: 收盘价时间序列，numpy.ndarray类型
            short: 短周期，默认12
            long: 长周期，默认26
            m: 移动平均线周期，默认9

        Returns:
            tuple: (dif, dea, macd) 三个numpy.ndarray
                - dif: MACD指标DIF值的时间序列
                - dea: MACD指标DEA值的时间序列
                - macd: MACD指标MACD值的时间序列（柱状图）
        """
        try:
            import talib
        except ImportError:
            raise ImportError("get_MACD需要安装ta-lib库: pip install ta-lib")

        if not isinstance(close, np.ndarray):
            close = np.array(close, dtype=float)

        # 使用talib计算MACD
        dif, dea, macd = talib.MACD(close, fastperiod=short, slowperiod=long, signalperiod=m)

        return dif, dea, macd

    def get_KDJ(
        self, high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 9, m1: int = 3, m2: int = 3
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """计算KDJ指标（随机指标）

        Args:
            high: 最高价时间序列，numpy.ndarray类型
            low: 最低价时间序列，numpy.ndarray类型
            close: 收盘价时间序列，numpy.ndarray类型
            n: 周期，默认9
            m1: K值平滑周期，默认3
            m2: D值平滑周期，默认3

        Returns:
            tuple: (k, d, j) 三个numpy.ndarray
                - k: KDJ指标K值的时间序列
                - d: KDJ指标D值的时间序列
                - j: KDJ指标J值的时间序列
        """
        try:
            import talib
        except ImportError:
            raise ImportError("get_KDJ需要安装ta-lib库: pip install ta-lib")

        if not isinstance(high, np.ndarray):
            high = np.array(high, dtype=float)
        if not isinstance(low, np.ndarray):
            low = np.array(low, dtype=float)
        if not isinstance(close, np.ndarray):
            close = np.array(close, dtype=float)

        # 使用talib的STOCH (Stochastic) 计算KD
        # talib.STOCH返回的是slowk和slowd
        k, d = talib.STOCH(
            high,
            low,
            close,
            fastk_period=n,
            slowk_period=m1,
            slowk_matype=0,  # SMA
            slowd_period=m2,
            slowd_matype=0,
        )  # SMA

        # 计算J值：J = 3K - 2D
        j = 3 * k - 2 * d

        return k, d, j

    def get_RSI(self, close: np.ndarray, n: int = 6) -> np.ndarray:
        """计算RSI指标（相对强弱指标）

        Args:
            close: 收盘价时间序列，numpy.ndarray类型
            n: 周期，默认6

        Returns:
            np.ndarray: RSI指标值的时间序列
        """
        try:
            import talib
        except ImportError:
            raise ImportError("get_RSI需要安装ta-lib库: pip install ta-lib")

        if not isinstance(close, np.ndarray):
            close = np.array(close, dtype=float)

        # 使用talib计算RSI
        rsi = talib.RSI(close, timeperiod=n)

        return rsi

    def get_CCI(
        self, high: np.ndarray, low: np.ndarray | None = None, close: np.ndarray | None = None, n: int = 14
    ) -> np.ndarray:
        """计算CCI指标（顺势指标）

        Args:
            high: 最高价时间序列，numpy.ndarray类型
            low: 最低价时间序列，numpy.ndarray类型
            close: 收盘价时间序列，numpy.ndarray类型
            n: 周期，默认14

        Returns:
            np.ndarray: CCI指标值的时间序列
        """
        try:
            import talib
        except ImportError:
            raise ImportError("get_CCI需要安装ta-lib库: pip install ta-lib")

        # 兼容文档中的 get_CCI(close, n) 形式
        if close is None:
            if low is None:
                close = high
                low = high
                high = high
            elif np.isscalar(low):
                n = int(low)
                close = high
                low = high
                high = high
            else:
                close = high

        if not isinstance(high, np.ndarray):
            high = np.array(high, dtype=float)
        if not isinstance(low, np.ndarray):
            low = np.array(low, dtype=float)
        if not isinstance(close, np.ndarray):
            close = np.array(close, dtype=float)

        # 使用talib计算CCI
        cci = talib.CCI(high, low, close, timeperiod=n)

        return cci

    def _clear_data_caches(self) -> None:
        """Clear instance caches derived from the active data source."""
        self._stock_status_cache.clear()
        self._stock_date_index.clear()
        self._prebuilt_index = False
        self._sorted_status_dates = None
        self._history_cache.clear()
        self._history_cache_date = None
        self._fundamentals_cache.clear()
        self._sorted_index_dates = None
        self._adj_alignment_cache.clear()
        self._values_cache.clear()


# PTrade 官方同时支持全称尾缀（XSHG/XSHE/XBHS）与简称尾缀（SS/SZ），
# 且返回对象的 symbol 用四位尾缀并要求「做代码尾缀兼容」（api_doc L243-248、
# L3084、L4241、L6342）。这里在类级别统一安装，使策略可直接沿用线上写法，
# 不必为了本地回测把 XSHG 改成 SS。
install_code_compat(PtradeAPI, _API_KWARG_ALIASES)
