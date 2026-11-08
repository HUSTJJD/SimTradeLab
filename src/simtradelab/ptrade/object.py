# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
回测核心类和数据结构

包含Portfolio, Position, Order, Context等核心对象
"""


from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from functools import wraps
from typing import Any, Optional
import os
import weakref

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from .cache_manager import cache_manager
from .code_compat import DualKeyDict, code_candidates
from .lifecycle_controller import LifecyclePhase


def _get_load_map():
    """获取数据类型到加载函数的映射（延迟导入避免循环依赖）"""
    from . import storage
    return {
        'stock': storage.load_stock,
        'stock_1m': storage.load_stock_1m,
        'valuation': storage.load_valuation,
        'fundamentals': storage.load_fundamentals,
        'exrights': lambda data_dir, k: storage.load_exrights(data_dir, k).get('exrights_events', pd.DataFrame())
    }


# DataFrame 列的 numpy 视图缓存（与 PtradeAPI._cached_values 同款思路）：
# pandas 每次 df[col] 都要构造 Series，多股票循环里按位置取标量是主要开销之一。
# 按 (id(df), column) 缓存，并用弱引用防止 id 复用后读到过期对象。
_COLUMN_VALUES_CACHE: dict[tuple[int, str], tuple[Any, np.ndarray]] = {}


def _column_values(df: pd.DataFrame, column: str) -> np.ndarray:
    """返回列的 numpy 视图（不复制数据），按 DataFrame 对象缓存。"""
    key = (id(df), column)
    entry = _COLUMN_VALUES_CACHE.get(key)
    if entry is None or entry[0]() is not df:
        entry = (
            weakref.ref(df, lambda _r, _k=key: _COLUMN_VALUES_CACHE.pop(_k, None)),
            df[column].to_numpy(copy=False),
        )
        _COLUMN_VALUES_CACHE[key] = entry
    return entry[1]


# df.iloc[i] 会按所有列的公共 dtype 构造 Series（与 i 无关）。缓存这个 dtype，
# 让按列视图取出的标量保持与旧 iloc 行完全一致的 dtype（例如混合 float64
# 价格列 + int64 volume 时，旧 row['volume'] 是 float64 而非 int64）。
_ROW_DTYPE_CACHE: dict[int, tuple[Any, Any]] = {}


def _row_scalar_dtype(df: pd.DataFrame):
    key = id(df)
    entry = _ROW_DTYPE_CACHE.get(key)
    if entry is None or entry[0]() is not df:
        dtype = df.iloc[0].dtype if len(df) else np.dtype('float64')
        entry = (
            weakref.ref(df, lambda _r, _k=key: _ROW_DTYPE_CACHE.pop(_k, None)),
            dtype,
        )
        _ROW_DTYPE_CACHE[key] = entry
    return entry[1]


# ==================== 多进程worker函数 ====================
def _load_data_chunk(data_dir, data_type, keys_chunk) -> dict[str, Any]:
    """多进程worker：加载一批数据

    Args:
        data_dir: 数据目录路径
        data_type: 数据类型（'stock', 'valuation', 'fundamentals', 'exrights'）
        keys_chunk: 要加载的key列表

    Returns:
        dict: {key: dataframe}
    """
    load_func = _get_load_map()[data_type]
    result: dict[str, Any] = {}

    for key in keys_chunk:
        try:
            df = load_func(data_dir, key)
            if not df.empty:
                result[key] = df
        except Exception:
            pass

    return result


def ensure_data_loaded(func):
    """装饰器：确保数据已加载后再访问"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._ensure_data_loaded()
        return func(self, *args, **kwargs)
    return wrapper


class BacktestContext:
    """回测上下文配置（封装共享依赖）"""
    def __init__(self, stock_data_dict=None, get_stock_date_index_func=None,
                 check_limit_func=None, log_obj=None, context_obj=None, data_context=None):
        self.stock_data_dict = stock_data_dict
        self.get_stock_date_index = get_stock_date_index_func
        self.check_limit = check_limit_func
        self.log = log_obj
        self.context = context_obj
        self.data_context = data_context

class LazyDataDict:
    """延迟加载数据字典（可选全量加载，支持多进程加速）"""
    def __init__(self, data_dir, data_type, all_keys_list, max_cache_size=6000,
                 preload=False, use_multiprocessing=True, skip_empty=False):
        """初始化延迟加载数据字典

        Args:
            data_dir: 数据根目录路径
            data_type: 数据类型（'stock', 'valuation', 'fundamentals', 'exrights'）
            all_keys_list: 所有可用的key列表
            max_cache_size: 最大缓存数量
            preload: 是否一次性加载全部数据（默认 False，按股票池按需加载）
            use_multiprocessing: 保留参数兼容；实际并行改用线程池
            skip_empty: 惰性加载时把空 DataFrame 视为不存在（复现旧 preload 语义）
        """
        self.data_dir = data_dir
        self.data_type = data_type

        # 使用公共加载映射
        self._load_map = _get_load_map()
        self._cache = OrderedDict()  # 使用OrderedDict实现LRU
        self._all_keys = all_keys_list
        self._all_keys_set = set(all_keys_list)  # O(1) 查找
        self._max_cache_size = max_cache_size  # 最大缓存数量
        self._preload = preload
        self._skip_empty = skip_empty
        self._access_count = 0  # 访问计数器
        self._lru_update_interval = 100  # 每N次访问才重新排序
        self._lazy_misses = 0  # 惰性未命中次数（用于自适应全量兜底）
        self._auto_warm_done = False

        if preload:
            # 线程池并行：pyarrow 在 C++ 层读 parquet 会释放 GIL，线程并行
            # 无需把上万个 DataFrame 跨进程 pickle 回传主进程。实测 600 只
            # 股票：串行 15.0s / 15 线程 7.4s / loky 15 进程 25.0s。
            import time
            from simtradelab.i18n import t
            workers = min(16, max(2, (os.cpu_count() or 4)))
            print(t("data.parallel_loading", workers=workers, count=len(all_keys_list)))
            start_time = time.perf_counter()
            self.ensure_loaded(all_keys_list, max_workers=workers)
            print(t("data.parallel_done", time="{:.1f}".format(time.perf_counter() - start_time)))

    def ensure_loaded(self, keys, max_workers=None) -> None:
        """批量并行加载 keys 中尚未缓存的数据。

        结果在主线程按请求顺序写入 ``_cache``，因此 OrderedDict 无需加锁，
        且加载顺序与原实现一致（不改变任何后续遍历/切片结果）。
        """
        missing = [
            key for key in dict.fromkeys(keys)
            if key in self._all_keys_set and key not in self._cache
        ]
        if not missing:
            return

        # 大批量预热时放宽 LRU，避免先加载的股票被淘汰后重复读盘。
        needed = len(self._cache) + len(missing)
        if needed > self._max_cache_size:
            self._max_cache_size = needed

        load_func = self._load_map[self.data_type]

        def _one(key):
            try:
                return key, load_func(self.data_dir, key)
            except Exception:
                return key, None

        if len(missing) == 1:
            results = [_one(missing[0])]
        else:
            from concurrent.futures import ThreadPoolExecutor
            workers = max_workers or min(16, max(2, (os.cpu_count() or 4)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(_one, missing))

        for key, value in results:
            if value is None:
                continue
            if self._skip_empty and isinstance(value, pd.DataFrame) and value.empty:
                continue
            self._cache[key] = value

    def __contains__(self, key):
        return key in self._all_keys_set

    def __getitem__(self, key):
        if key in self._cache:
            # LRU优化：每N次访问才重新排序（减少move_to_end开销）
            if not self._preload:
                self._access_count += 1
                if self._access_count % self._lru_update_interval == 0:
                    self._cache.move_to_end(key)
            return self._cache[key]

        # 预加载模式下，缓存中没有说明数据不存在
        if self._preload:
            raise KeyError(f"Stock {key} not found")

        # 自适应兜底：动态全市场扫描策略若逐只惰性加载，会退化成单线程
        # 读 23719 个文件。累计足够多的新 key 后一次性并行预热剩余数据，
        # 保证最坏情况仍不慢于原「全量预加载」基线。
        if not self._auto_warm_done:
            self._lazy_misses += 1
            if (self._lazy_misses >= 512
                    and len(self._cache) < len(self._all_keys) // 4):
                self._auto_warm_done = True
                self.ensure_loaded(self._all_keys)
                if key in self._cache:
                    return self._cache[key]

        # 延迟加载模式：缓存未命中，从存储加载
        try:
            load_func = self._load_map[self.data_type]
            value = load_func(self.data_dir, key)
            if self._skip_empty and (
                value is None or (isinstance(value, pd.DataFrame) and value.empty)
            ):
                raise KeyError(key)

            # 添加到缓存
            self._cache[key] = value

            # LRU淘汰：如果超过最大缓存，删除最旧的
            if len(self._cache) > self._max_cache_size:
                self._cache.popitem(last=False)  # 删除最早的项

            return value
        except KeyError:
            raise KeyError(f'Stock {key} not found')

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self._all_keys

    def items(self):
        for key in self._all_keys:
            yield key, self[key]

    def clear_cache(self):
        """手动清空缓存"""
        self._cache.clear()



class StockData:
    """单个股票的数据对象，支持mavg和vwap方法"""
    def __init__(self, stock, current_date, bt_ctx):
        """
        Args:
            stock: 股票代码
            current_date: 当前日期
            bt_ctx: BacktestContext实例
        """
        self.stock = stock
        self.current_date = current_date
        self._stock_df = None
        self._current_idx = None
        self._bt_ctx = bt_ctx
        self._data: Optional[dict[str, Any]] = None  # 延迟加载标记
        self._cached_phase = None  # 缓存的phase,用于判断是否需要重新加载
        self._cached_idx = None  # 缓存的idx,用于判断是否需要重新加载
        # 一次性缓存当前时间键与日期索引，避免 _ensure_data_loaded 每次重算
        self._dt_value = None
        self._date_dict = None
        self._sorted_i8 = None

        if bt_ctx and bt_ctx.stock_data_dict:
            # 策略可写 XSHG/XSHE 等官方全称尾缀，行情字典内部只用 .SS/.SZ
            for candidate in code_candidates(stock):
                if candidate in bt_ctx.stock_data_dict:
                    self.stock = candidate
                    self._stock_df = bt_ctx.stock_data_dict[candidate]
                    break

            if (
                self._stock_df is not None
                and isinstance(self._stock_df, pd.DataFrame)
                and bt_ctx.get_stock_date_index
            ):
                self._date_dict, self._sorted_i8 = bt_ctx.get_stock_date_index(self.stock)
                ctx = bt_ctx.context
                if ctx is not None and getattr(ctx, 'frequency', '1d') == '1m':
                    self._dt_value = current_date.value
                else:
                    self._dt_value = current_date.normalize().value

    def _ensure_data_loaded(self):
        """确保数据已加载（延迟加载）"""
        # 时间键与日期索引已在 __init__ 一次性缓存；这里只保留 phase 判定、
        # idx 判定和「变化才 reload」。
        if (
            self._stock_df is not None
            and isinstance(self._stock_df, pd.DataFrame)
            and self._date_dict is not None
        ):
            # 通过LifecycleController判断当前阶段
            controller = self._bt_ctx.context._lifecycle_controller if self._bt_ctx.context else None
            current_phase = controller.current_phase if controller else None

            if current_phase == LifecyclePhase.BEFORE_TRADING_START:
                # before_trading_start阶段：返回前一交易日数据
                # searchsorted(side='left') 等价于 bisect_left: 当 dt_value 在数组中时
                # 返回该元素位置，pos-1 得到前一交易日（必须保留，不能合并掉）
                pos = self._sorted_i8.searchsorted(self._dt_value, side='left')
                if pos > 0:
                    self._current_idx = self._date_dict[self._sorted_i8[pos - 1]]
            else:
                # handle_data阶段：返回当日数据
                if self._dt_value in self._date_dict:
                    self._current_idx = self._date_dict[self._dt_value]

            # 优化:只有phase或idx变化时才重新加载
            if (self._cached_phase != current_phase or
                self._cached_idx != self._current_idx or
                self._data is None):
                self._data = self._load_data()
                self._cached_phase = current_phase
                self._cached_idx = self._current_idx

        if self._data is None:
            raise ValueError("股票 %s 在 %s 无可用数据" % (self.stock, self.current_date))

    def _load_data(self):
        """加载股票当日数据（不复权；q&a.txt: data[stock].price 为不复权数据）"""
        if self._current_idx is None or self._stock_df is None:
            raise ValueError(f"股票 {self.stock} 在 {self.current_date} 数据加载失败")

        # 按列的 numpy 视图按位置取标量，避免 iloc 构造整行 Series；
        # 行 dtype 归一化保证与旧 row[col] 的 dtype 一致。
        df = self._stock_df
        idx = self._current_idx
        row_dtype = _row_scalar_dtype(df)

        def scalar(column):
            arr = _column_values(df, column)
            value = arr[idx]
            if arr.dtype != row_dtype:
                try:
                    return row_dtype.type(value)
                except (TypeError, ValueError, AttributeError):
                    return value
            return value

        data = {
            'close': scalar('close'),
            'open': scalar('open'),
            'high': scalar('high'),
            'low': scalar('low'),
            'volume': scalar('volume'),
            # api_doc.txt / PTRADE_PARQUET_FORMAT.md: money 是独立的真实
            # 成交金额字段，不能用 close*volume 推算（两者一般不相等）。
            # 缺失列回退 nan，与 row.get('money', np.nan) 一致。
            'money': (scalar('money') if 'money' in df.columns else np.nan),
        }

        return data

    @ensure_data_loaded
    def __getitem__(self, key):
        if key not in self._data:
            raise KeyError(f"股票 {self.stock} 数据中没有字段 {key}")
        return self._data[key]

    @property
    def dt(self):
        """时间"""
        return self.current_date

    @property
    @ensure_data_loaded
    def open(self):
        """开盘价"""
        return self._data.get('open', np.nan)

    @property
    @ensure_data_loaded
    def close(self):
        """收盘价"""
        return self._data.get('close', np.nan)

    @property
    @ensure_data_loaded
    def price(self):
        """结束时价格（同close）"""
        return self._data.get('close', np.nan)

    @property
    @ensure_data_loaded
    def low(self):
        """最低价"""
        return self._data.get('low', np.nan)

    @property
    @ensure_data_loaded
    def high(self):
        """最高价"""
        return self._data.get('high', np.nan)

    @property
    @ensure_data_loaded
    def volume(self):
        """成交量"""
        return self._data.get('volume', 0)

    @property
    @ensure_data_loaded
    def money(self):
        """成交金额（平台真实 money 字段）"""
        return self._data.get('money', np.nan)

    @ensure_data_loaded
    def mavg(self, window):
        """计算移动平均线（带全局缓存）"""
        cache_key = (self.stock, self.current_date, window)

        # 检查全局缓存
        cached_value = cache_manager.get('ma_cache', cache_key)
        if cached_value is not None:
            return cached_value

        if self._current_idx is None or self._stock_df is None:
            raise ValueError(f"股票 {self.stock} 无法计算mavg({window})")

        start_idx = max(0, self._current_idx - window + 1)
        close_arr = _column_values(self._stock_df, 'close')
        result = np.nanmean(close_arr[start_idx:self._current_idx + 1])

        # 更新全局缓存
        cache_manager.put('ma_cache', cache_key, result)

        return result

    @ensure_data_loaded
    def vwap(self, window):
        """计算成交量加权平均价（带全局缓存）"""
        cache_key = (self.stock, self.current_date, window)

        # 检查全局缓存
        cached_value = cache_manager.get('vwap_cache', cache_key)
        if cached_value is not None:
            return cached_value

        if self._current_idx is None or self._stock_df is None:
            raise ValueError(f"股票 {self.stock} 无法计算vwap({window})")

        start_idx = max(0, self._current_idx - window + 1)
        end_idx = self._current_idx + 1
        volumes = _column_values(self._stock_df, 'volume')[start_idx:end_idx]
        closes = _column_values(self._stock_df, 'close')[start_idx:end_idx]
        total_volume = np.sum(volumes)

        if total_volume == 0:
            raise ValueError(f"股票 {self.stock} 计算vwap({window})时成交量为0")

        result = np.sum(closes * volumes) / total_volume

        # 更新全局缓存
        cache_manager.put('vwap_cache', cache_key, result)

        return result


class Data(dict):
    """模拟data对象，支持动态获取股票数据（带LRU缓存限制）"""
    MAX_CACHE_SIZE = 200  # 减小最大缓存股票数，降低内存占用

    def __init__(self, current_date, bt_ctx=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_date = current_date
        self._bt_ctx = bt_ctx
        self._access_order = OrderedDict()  # 使用OrderedDict实现O(1) LRU

    def __getitem__(self, stock):
        """动态获取股票数据，返回StockData对象"""
        # 如果已经缓存，直接返回并更新LRU
        if stock in self:
            if stock in self._access_order:
                self._access_order.move_to_end(stock)  # O(1)操作
            return super().__getitem__(stock)

        # 创建StockData对象并缓存。缓存键保留策略原写法；StockData 内部会自动
        # 解析到行情字典里真实存在的 .SS/.SZ 键（见 StockData.__init__）。
        stock_data = StockData(stock, self.current_date, self._bt_ctx)
        super().__setitem__(stock, stock_data)
        self._access_order[stock] = None

        # LRU淘汰：如果超过上限，删除最旧的
        if len(self) > self.MAX_CACHE_SIZE:
            oldest, _ = self._access_order.popitem(last=False)
            if oldest in self:
                super().__delitem__(oldest)

        return stock_data

class Blotter:
    """模拟blotter对象"""
    def __init__(self, current_dt, bt_ctx=None):
        self.current_dt = current_dt
        self.open_orders = []
        self.all_orders = []
        self.filled_orders = []
        self._order_id_counter = 0
        self._bt_ctx = bt_ctx

    def create_order(self, stock, amount):
        """创建订单"""
        self._order_id_counter += 1
        order = Order(
            id=self._order_id_counter,
            symbol=stock,
            amount=amount,
            dt=self.current_dt,
            limit=None
        )
        self.open_orders.append(order)
        self.all_orders.append(order)
        return order

    def cancel_order(self, order):
        """取消订单"""
        target = order
        if order not in self.open_orders:
            target = next(
                (
                    open_order
                    for open_order in self.open_orders
                    if str(open_order.id) == str(order)
                ),
                None,
            )
        if target in self.open_orders:
            self.open_orders.remove(target)
            target.status = 'cancelled'
            return True
        return False

class Order(BaseModel):
    """订单对象"""
    id: int | str = Field(..., description="订单号（支持整数或UUID字符串）")
    dt: Optional[datetime] = Field(None, description="订单产生时间")
    symbol: str = Field(..., description="标的代码")
    amount: int = Field(..., description="下单数量（正数=买入，负数=卖出）")
    limit: Optional[float] = Field(None, description="指定价格")
    filled: int = Field(default=0, description="成交数量")
    entrust_no: str = Field(default='', description="委托编号")
    priceGear: Optional[int] = Field(default=None, description="盘口档位")
    status: str = Field(default='0', description="订单状态：'0'未报, '1'待报, '2'已报")

    class Config:
        arbitrary_types_allowed = True

    @property
    def created(self) -> Optional[datetime]:
        """订单生成时间（dt的别名，保持API兼容性）"""
        return self.dt


class Portfolio:
    """模拟portfolio对象"""
    def __init__(self, initial_capital=100000.0, bt_ctx=None, context_obj=None):
        self._cash = initial_capital
        self.starting_cash = initial_capital
        # 官方 api_doc L3084：四位尾缀或两位尾缀代码皆可作为键取到持仓值
        self.positions = DualKeyDict()
        self.positions_value = 0.0
        self._bt_ctx = bt_ctx
        self._context = context_obj
        # 日内缓存
        self._cached_portfolio_value = None
        self._cache_date = None
        # 每日收盘价缓存（避免重复 DataFrame 查找）
        self._close_price_cache = {}
        self._close_price_cache_date = None
        # 持股批次追踪（用于分红税FIFO计算）
        self._position_lots = DualKeyDict()

    def _invalidate_cache(self):
        """清空 portfolio_value 缓存（持仓变化时调用）

        注意：不清空 _close_price_cache，因为同一天收盘价不变
        """
        self._cached_portfolio_value = None

    def add_position(self, stock, amount, price, date):
        """买入建仓/加仓"""
        if stock not in self.positions:
            t_plus_1 = self._context.t_plus_1 if self._context else True
            self.positions[stock] = Position(stock, amount, price, t_plus_1=t_plus_1)
            self._position_lots[stock] = [{'date': date, 'amount': amount, 'dividends': [], 'dividends_total': 0.0}]
        else:
            # 可变模式：直接修改现有position
            position = self.positions[stock]
            new_amount = position.amount + amount
            new_cost = (position.amount * position.cost_basis + amount * price) / new_amount
            position.amount = new_amount
            position.cost_basis = new_cost
            t_plus_1 = self._context.t_plus_1 if self._context else True
            if not t_plus_1:
                position.enable_amount = new_amount
            position.market_value = new_amount * new_cost
            self._position_lots[stock].append({'date': date, 'amount': amount, 'dividends': [], 'dividends_total': 0.0})
        self._invalidate_cache()

    def remove_position(self, stock, amount, sell_date):
        """卖出减仓/清仓（FIFO扣减批次）"""
        if stock not in self.positions:
            return 0.0

        position = self.positions[stock]

        # 边界检查：卖出数量不能超过持仓
        if amount > position.amount:
            raise ValueError(
                f'卖出数量 {amount} 超过持仓 {position.amount}: {stock}'
            )

        # FIFO计算税务调整
        tax_adjustment = self._calculate_dividend_tax(stock, amount, sell_date)

        # 更新持仓
        if position.amount == amount:
            del self.positions[stock]
            if stock in self._position_lots:
                del self._position_lots[stock]
        else:
            position.amount -= amount
            position.enable_amount -= amount
            position.market_value = position.amount * position.cost_basis

        self._invalidate_cache()
        return tax_adjustment

    def add_dividend(self, stock, dividend_per_share):
        """记录分红到各批次"""
        if stock in self._position_lots:
            for lot in self._position_lots[stock]:
                lot_div = dividend_per_share * lot['amount']
                lot['dividends'].append(lot_div)
                lot['dividends_total'] = lot.get('dividends_total', 0.0) + lot_div

    def _calculate_dividend_tax(self, stock, amount, sell_date):
        """计算分红税调整（FIFO）

        Ptrade行为：分红时预扣20%，卖出时不做税务调整
        """
        return 0.0

    @property
    def cash(self):
        """当前可用资金"""
        return self._cash

    @property
    def available_cash(self):
        """当前可用资金（别名）"""
        return self._cash

    @property
    def capital_used(self):
        """已使用的现金"""
        return self.starting_cash - self._cash

    @property
    def returns(self):
        """当前收益比例"""
        if self.starting_cash > 0:
            return (self.portfolio_value - self.starting_cash) / self.starting_cash
        return 0.0

    @property
    def pnl(self):
        """浮动盈亏"""
        return self.portfolio_value - self.starting_cash

    @property
    def start_date(self):
        """开始时间"""
        return self._context.current_dt if self._context else None

    @property
    def portfolio_value(self):
        """计算总资产（现金+持仓市值）带日内缓存

        优化：收盘价按日缓存，交易后重算只做算术，不重复 DataFrame 查找
        """
        current_date = self._context.current_dt if self._context else None
        if current_date is not None and current_date == self._cache_date and self._cached_portfolio_value is not None:
            return self._cached_portfolio_value

        total = self._cash

        # 日切时清空收盘价缓存
        if current_date != self._close_price_cache_date:
            self._close_price_cache = {}
            self._close_price_cache_date = current_date

        positions_value = 0.0
        for stock, position in self.positions.items():
            if position.amount <= 0:
                continue

            # 从日缓存获取收盘价
            if stock in self._close_price_cache:
                current_price = self._close_price_cache[stock]
            else:
                current_price = position.cost_basis
                if self._bt_ctx and self._bt_ctx.get_stock_date_index:
                    stock_df = self._bt_ctx.stock_data_dict.get(stock)
                    if stock_df is not None and isinstance(stock_df, pd.DataFrame) and self._context:
                        date_dict, _ = self._bt_ctx.get_stock_date_index(stock)
                        lookup_dt = pd.Timestamp(self._context.current_dt)
                        if getattr(self._context, 'frequency', '1d') != '1m':
                            lookup_dt = lookup_dt.normalize()
                        idx = date_dict.get(lookup_dt.value)
                        if idx is None and getattr(self._context, 'frequency', '1d') == '1m':
                            candidate = stock_df.index.searchsorted(lookup_dt, side='right') - 1
                            if candidate >= 0:
                                idx = candidate
                        if idx is not None:
                            price = stock_df['close'].values[idx]
                            if not np.isnan(price) and price > 0:
                                current_price = price
                self._close_price_cache[stock] = current_price

            position.last_sale_price = current_price
            position.market_value = position.amount * current_price
            positions_value += position.amount * current_price

        self.positions_value = positions_value
        result = total + positions_value

        if current_date is not None:
            self._cache_date = current_date
            self._cached_portfolio_value = result

        return result

    @property
    def total_value(self):
        """总资产（portfolio_value 的别名）"""
        return self.portfolio_value

class Position:
    """模拟持仓对象"""
    def __init__(self, stock: str, amount: float, cost_basis: float, t_plus_1: bool = False):
        self.stock = stock
        self.sid = stock  # 别名，保持兼容
        self.amount = amount
        self.cost_basis = cost_basis
        self.enable_amount = 0 if t_plus_1 else amount
        self.last_sale_price = cost_basis
        self.today_amount = 0
        self.business_type = 'STOCK'
        self.market_value = amount * cost_basis
