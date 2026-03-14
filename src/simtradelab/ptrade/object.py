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
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from pydantic import BaseModel, Field
from tqdm import tqdm

from ..utils.performance_config import get_performance_config
from .cache_manager import cache_manager
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
    def __init__(self, data_dir, data_type, all_keys_list, max_cache_size=6000, preload=False, use_multiprocessing=True):
        """初始化延迟加载数据字典

        Args:
            data_dir: 数据根目录路径
            data_type: 数据类型（'stock', 'valuation', 'fundamentals', 'exrights'）
            all_keys_list: 所有可用的key列表
            max_cache_size: 最大缓存数量
            preload: 是否预加载所有数据
            use_multiprocessing: 是否使用多进程加载
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
        self._access_count = 0  # 访问计数器
        self._lru_update_interval = 100  # 每N次访问才重新排序

        # 如果启用预加载，一次性加载所有数据到内存
        if preload:
            config = get_performance_config()

            # 判断是否使用多进程
            enable_mp = (use_multiprocessing and
                        config.enable_multiprocessing and
                        len(all_keys_list) >= config.min_batch_size)

            if enable_mp:
                # 多进程并行加载
                num_workers = config.num_workers
                chunk_size = max(50, len(all_keys_list) // (num_workers * 2))
                chunks = [all_keys_list[i:i+chunk_size]
                         for i in range(0, len(all_keys_list), chunk_size)]

                print(f"  使用{num_workers}进程并行加载 {len(all_keys_list)} 只...")
                import time
                start_time = time.perf_counter()

                # 多进程加载
                results = Parallel(n_jobs=num_workers, backend='loky', verbose=0)(
                    delayed(_load_data_chunk)(self.data_dir, self.data_type, chunk)
                    for chunk in chunks
                )

                # 合并结果
                for chunk_result in results:
                    self._cache.update(chunk_result)

                elapsed = time.perf_counter() - start_time
                print(f"  ✓ 加载完成，耗时 {elapsed:.1f}秒")
            else:
                # 串行加载（带进度条）
                load_func = self._load_map[self.data_type]
                for key in tqdm(all_keys_list, desc='  加载', ncols=80, ascii=True,
                              bar_format='{desc}: {percentage:3.0f}%|{bar}| {n:4d}/{total:4d} [{elapsed}<{remaining}]'):
                    try:
                        self._cache[key] = load_func(self.data_dir, key)
                    except KeyError:
                        pass

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

        # 延迟加载模式：缓存未命中，从存储加载
        try:
            load_func = self._load_map[self.data_type]
            value = load_func(self.data_dir, key)

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

        if bt_ctx and bt_ctx.stock_data_dict and stock in bt_ctx.stock_data_dict:
            self._stock_df = bt_ctx.stock_data_dict[stock]

    def _ensure_data_loaded(self):
        """确保数据已加载（延迟加载）"""
        # 优化:只在phase或idx变化时重新加载
        # 因为同一个Data对象在before_trading_start和handle_data之间共享
        # 需要判断当前phase,如果phase变化则重新加载

        # 首次访问时才计算_current_idx（此时phase已正确设置）
        if self._stock_df is not None and isinstance(self._stock_df, pd.DataFrame):
            if self._bt_ctx and self._bt_ctx.get_stock_date_index:
                date_dict, sorted_i8 = self._bt_ctx.get_stock_date_index(self.stock)
                current_date_norm = self.current_date.normalize()
                dt_value = current_date_norm.value

                # 通过LifecycleController判断当前阶段
                controller = self._bt_ctx.context._lifecycle_controller if self._bt_ctx.context else None
                current_phase = controller.current_phase if controller else None

                frequency = getattr(self._bt_ctx.context, 'frequency', '1d') if self._bt_ctx and self._bt_ctx.context else '1d'
                use_previous_daily_bar = (
                    frequency != '1m' and
                    current_phase in (LifecyclePhase.BEFORE_TRADING_START, LifecyclePhase.HANDLE_DATA)
                )

                if use_previous_daily_bar:
                    # 日线盘前与 handle_data 阶段：统一返回前一交易日数据
                    pos = sorted_i8.searchsorted(dt_value, side='left')
                    if pos > 0:
                        self._current_idx = date_dict[sorted_i8[pos - 1]]
                else:
                    # 分钟回测或日线盘后阶段：返回当前bar数据
                    if dt_value in date_dict:
                        self._current_idx = date_dict[dt_value]

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
        """加载股票当日数据并应用前复权"""
        if self._current_idx is None or self._stock_df is None:
            raise ValueError(f"股票 {self.stock} 在 {self.current_date} 数据加载失败")

        row = self._stock_df.iloc[self._current_idx]
        data = {
            'close': row['close'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'volume': row['volume']
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
        """成交金额"""
        return self._data['close'] * self._data['volume']

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
        close_prices = self._stock_df.iloc[start_idx:self._current_idx + 1]['close'].values
        result = np.nanmean(close_prices)

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
        slice_df = self._stock_df.iloc[start_idx:self._current_idx + 1]
        volumes = slice_df['volume'].values
        closes = slice_df['close'].values
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

        # 创建StockData对象并缓存
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
        if order in self.open_orders:
            self.open_orders.remove(order)
            order.status = 'cancelled'
            return True
        return False

class Order(BaseModel):
    """订单对象"""
    id: Union[int, str] = Field(..., description="订单号（支持整数或UUID字符串）")
    dt: Optional[datetime] = Field(None, description="订单产生时间")
    symbol: str = Field(..., description="标的代码")
    amount: int = Field(..., description="下单数量（正数=买入，负数=卖出）")
    limit: Optional[float] = Field(None, description="指定价格")
    filled: int = Field(default=0, description="成交数量")
    entrust_no: str = Field(default='', description="委托编号")
    priceGear: Optional[int] = Field(default=None, description="盘口档位")
    status: str = Field(default='0', description="订单状态：'0'未报, '1'待报, '2'已报")
    business_type: str = Field(default='STOCK', description="业务类型：普通股票、融资买入、融券卖出等")

    model_config = {"arbitrary_types_allowed": True}

    @property
    def created(self) -> Optional[datetime]:
        """订单生成时间（dt的别名，保持API兼容性）"""
        return self.dt


class Portfolio:
    """模拟portfolio对象"""
    def __init__(self, initial_capital=100000.0, bt_ctx=None, context_obj=None):
        self._cash = initial_capital
        self.starting_cash = initial_capital
        self.positions = {}
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
        self._position_lots = {}

        # 融资融券账户状态
        self.margin_enabled = False
        self.margin_config = {
            'margincash_interest_rate': 0.0,
            'margincash_margin_rate': 1.0,
            'marginsec_interest_rate': 0.0,
            'marginsec_margin_rate': 1.0,
            'margin_call_rate': 1.3,
            'liquidation_rate': 1.1,
            'margincash_stocks': None,
            'marginsec_stocks': None,
            'assure_stocks': None,
        }
        self.margin_cash_positions = {}
        self.margin_short_positions = {}
        self.margin_interest = 0.0
        self.margin_last_accrual_date = None
        self.short_positions_value = 0.0
        self.margin_call_active = False
        self.margin_liquidation_pending = False

    def enable_margin_account(self, margincash_interest_rate=0.08,
                              margincash_margin_rate=1.5,
                              marginsec_interest_rate=0.10,
                              marginsec_margin_rate=1.5,
                              margin_call_rate=1.3,
                              liquidation_rate=1.1,
                              margincash_stocks=None,
                              marginsec_stocks=None,
                              assure_stocks=None):
        """启用融资融券账户模拟。"""
        self.margin_enabled = True
        self.margin_config = {
            'margincash_interest_rate': float(margincash_interest_rate),
            'margincash_margin_rate': float(margincash_margin_rate),
            'marginsec_interest_rate': float(marginsec_interest_rate),
            'marginsec_margin_rate': float(marginsec_margin_rate),
            'margin_call_rate': float(margin_call_rate),
            'liquidation_rate': float(liquidation_rate),
            'margincash_stocks': sorted(set(margincash_stocks)) if margincash_stocks else None,
            'marginsec_stocks': sorted(set(marginsec_stocks)) if marginsec_stocks else None,
            'assure_stocks': sorted(set(assure_stocks)) if assure_stocks else None,
        }
        if self._context is not None and self.margin_last_accrual_date is None and self._context.current_dt is not None:
            self.margin_last_accrual_date = self._context.current_dt.date()
        self._invalidate_cache()

    def ensure_margin_enabled(self):
        """确保当前账户已启用融资融券。"""
        if not self.margin_enabled:
            raise RuntimeError('当前账户未启用融资融券，请在回测配置中设置 enable_margin_account=true')

    @property
    def cash_liability(self):
        """当前融资负债。"""
        return sum(item.get('debt_balance', 0.0) for item in self.margin_cash_positions.values())

    def _get_market_price(self, stock, fallback=None):
        """获取当前回测时间对应的市场价格。"""
        current_price = fallback
        if current_price is None and stock in self.positions:
            current_price = self.positions[stock].cost_basis

        if stock in self._close_price_cache:
            return self._close_price_cache[stock]

        if self._bt_ctx and self._bt_ctx.get_stock_date_index:
            stock_df = self._bt_ctx.stock_data_dict.get(stock)
            if stock_df is not None and isinstance(stock_df, pd.DataFrame) and self._context:
                date_dict, _ = self._bt_ctx.get_stock_date_index(stock)
                idx = date_dict.get(self._context.current_dt.value)
                if idx is not None:
                    price = stock_df['close'].values[idx]
                    if not np.isnan(price) and price > 0:
                        current_price = float(price)

        if current_price is None:
            current_price = 0.0

        self._close_price_cache[stock] = current_price
        return current_price

    def get_marginsec_liability_value(self):
        """按市值计算当前融券负债。"""
        total = 0.0
        for stock, info in self.margin_short_positions.items():
            amount = int(info.get('amount', 0) or 0)
            if amount <= 0:
                continue
            price = self._get_market_price(stock, info.get('open_price', 0.0))
            info['last_price'] = price
            info['market_value'] = round(amount * price, 2)
            total += info['market_value']
        self.short_positions_value = total
        return total

    @property
    def sec_liability(self):
        """当前融券负债。"""
        return self.get_marginsec_liability_value()

    def get_margin_required_collateral(self):
        """计算当前两融占用的保证金需求。"""
        if not self.margin_enabled:
            return 0.0
        return (
            self.cash_liability * self.margin_config['margincash_margin_rate'] +
            self.get_marginsec_liability_value() * self.margin_config['marginsec_margin_rate']
        )

    def get_margin_account_summary(self):
        """汇总信用账户资产负债。"""
        portfolio_value = self.portfolio_value
        assure_asset = self._cash + self.positions_value
        cash_liability = self.cash_liability
        sec_liability = self.short_positions_value
        total_debit = cash_liability + sec_liability + self.margin_interest
        net_asset = portfolio_value
        required_collateral = self.get_margin_required_collateral()
        enable_bail_balance = max(net_asset - required_collateral, 0.0)
        maintenance_margin_rate = assure_asset / total_debit if total_debit > 0 else float('inf')

        if total_debit <= 0:
            risk_status = 'normal'
        elif maintenance_margin_rate < self.margin_config['liquidation_rate']:
            risk_status = 'liquidation'
        elif maintenance_margin_rate < self.margin_config['margin_call_rate']:
            risk_status = 'margin_call'
        else:
            risk_status = 'normal'

        return {
            'assure_asset': round(assure_asset, 2),
            'portfolio_value': round(portfolio_value, 2),
            'net_asset': round(net_asset, 2),
            'total_debit': round(total_debit, 2),
            'cash_liability': round(cash_liability, 2),
            'sec_liability': round(sec_liability, 2),
            'interest': round(self.margin_interest, 2),
            'required_collateral': round(required_collateral, 2),
            'enable_bail_balance': round(enable_bail_balance, 2),
            'raw_cash': round(self._cash, 2),
            'available_cash': round(self.available_cash, 2),
            'margin_call_rate': round(self.margin_config['margin_call_rate'], 6),
            'liquidation_rate': round(self.margin_config['liquidation_rate'], 6),
            'maintenance_margin_rate': round(maintenance_margin_rate, 6) if np.isfinite(maintenance_margin_rate) else float('inf'),
            'risk_status': risk_status,
            'liquidation_pending': self.margin_liquidation_pending,
        }

    def get_margin_risk_state(self):
        """返回当前两融风险状态。"""
        summary = self.get_margin_account_summary()
        return {
            'status': summary['risk_status'],
            'maintenance_margin_rate': summary['maintenance_margin_rate'],
            'margin_call_rate': summary['margin_call_rate'],
            'liquidation_rate': summary['liquidation_rate'],
            'liquidation_pending': self.margin_liquidation_pending,
            'total_debit': summary['total_debit'],
        }

    def refresh_margin_risk_flags(self):
        """根据当前维持担保比例刷新追保/强平标记。"""
        if not self.margin_enabled:
            self.margin_call_active = False
            self.margin_liquidation_pending = False
            return {'status': 'disabled'}

        state = self.get_margin_risk_state()
        status = state['status']
        self.margin_call_active = status in ('margin_call', 'liquidation')
        if status == 'liquidation':
            self.margin_liquidation_pending = True
        elif state['total_debit'] <= 0:
            self.margin_liquidation_pending = False
        return state

    def clear_margin_liquidation_flag(self):
        """清除待强平标记。"""
        self.margin_liquidation_pending = False

    def get_margin_capacity(self, side):
        """按保证金比例估算新增两融可用额度。"""
        if not self.margin_enabled:
            return 0.0

        state = self.refresh_margin_risk_flags()
        if state.get('status') != 'normal':
            return 0.0

        spare_equity = max(self.portfolio_value - self.get_margin_required_collateral(), 0.0)
        if side == 'cash':
            ratio = self.margin_config['margincash_margin_rate']
        else:
            ratio = self.margin_config['marginsec_margin_rate']
        if ratio <= 0:
            return 0.0
        return spare_equity / ratio

    def accrue_margin_interest(self, current_dt=None):
        """按自然日计提两融利息。"""
        if not self.margin_enabled:
            return 0.0

        if current_dt is None and self._context is not None:
            current_dt = self._context.current_dt
        if current_dt is None:
            return 0.0

        current_date = current_dt.date()
        if self.margin_last_accrual_date is None:
            self.margin_last_accrual_date = current_date
            return 0.0

        days = (current_date - self.margin_last_accrual_date).days
        if days <= 0:
            return 0.0

        interest = (
            self.cash_liability * self.margin_config['margincash_interest_rate'] / 365.0 +
            self.get_marginsec_liability_value() * self.margin_config['marginsec_interest_rate'] / 365.0
        ) * days
        self.margin_interest += interest
        self.margin_last_accrual_date = current_date
        self._invalidate_cache()
        return interest

    def record_margin_cash_open(self, stock, amount, price, dt):
        """记录融资买入负债。"""
        self.ensure_margin_enabled()
        if stock not in self.margin_cash_positions:
            self.margin_cash_positions[stock] = {
                'stock_code': stock,
                'amount': 0,
                'open_price': 0.0,
                'debt_balance': 0.0,
                'open_dt': dt,
                'business_type': 'MARGIN_CASH',
            }

        item = self.margin_cash_positions[stock]
        old_amount = int(item.get('amount', 0) or 0)
        new_amount = old_amount + int(amount)
        debt = float(amount) * float(price)
        if new_amount > 0:
            item['open_price'] = (
                old_amount * float(item.get('open_price', 0.0)) + float(amount) * float(price)
            ) / new_amount
        item['amount'] = new_amount
        item['debt_balance'] = float(item.get('debt_balance', 0.0)) + debt
        item['open_dt'] = item.get('open_dt') or dt
        self._invalidate_cache()

    def repay_margin_interest(self, value):
        """优先偿还已计提的两融利息。"""
        repay_value = min(float(value), max(self.margin_interest, 0.0))
        if repay_value <= 0:
            return 0.0
        self.margin_interest = max(self.margin_interest - repay_value, 0.0)
        self._invalidate_cache()
        return repay_value

    def repay_margin_cash(self, stock, amount, repayment_value):
        """归还融资本金负债。"""
        item = self.margin_cash_positions.get(stock)
        if item is None:
            return 0.0

        current_amount = int(item.get('amount', 0) or 0)
        current_debt = float(item.get('debt_balance', 0.0) or 0.0)
        repaid_amount = min(max(int(amount), 0), current_amount)
        repaid_value = min(max(float(repayment_value), 0.0), current_debt)

        remaining_amount = max(current_amount - repaid_amount, 0)
        remaining_debt = max(current_debt - repaid_value, 0.0)

        if remaining_amount <= 0 and remaining_debt <= 1e-8:
            del self.margin_cash_positions[stock]
        else:
            item['amount'] = remaining_amount
            item['debt_balance'] = remaining_debt

        self._invalidate_cache()
        return repaid_value

    def repay_margin_cash_with_value(self, value):
        """按金额直接还款，按融资负债占比分摊。"""
        remaining = float(value)
        if remaining <= 0:
            return 0.0

        for stock in list(self.margin_cash_positions.keys()):
            if remaining <= 1e-8:
                break
            debt = float(self.margin_cash_positions[stock].get('debt_balance', 0.0) or 0.0)
            if debt <= 0:
                continue
            current_amount = int(self.margin_cash_positions[stock].get('amount', 0) or 0)
            repay_value = min(remaining, debt)
            repay_amount = int(round(current_amount * (repay_value / debt))) if debt > 0 and current_amount > 0 else 0
            if repay_value >= debt:
                repay_amount = current_amount
            actual = self.repay_margin_cash(stock, repay_amount, repay_value)
            remaining -= actual

        return float(value) - remaining

    def record_margin_short_open(self, stock, amount, price, dt):
        """记录融券卖出负债。"""
        self.ensure_margin_enabled()
        if stock not in self.margin_short_positions:
            self.margin_short_positions[stock] = {
                'stock_code': stock,
                'amount': 0,
                'open_price': 0.0,
                'open_dt': dt,
                'business_type': 'MARGIN_SHORT',
                'last_price': float(price),
                'market_value': 0.0,
            }

        item = self.margin_short_positions[stock]
        old_amount = int(item.get('amount', 0) or 0)
        new_amount = old_amount + int(amount)
        if new_amount > 0:
            item['open_price'] = (
                old_amount * float(item.get('open_price', 0.0)) + float(amount) * float(price)
            ) / new_amount
        item['amount'] = new_amount
        item['open_dt'] = item.get('open_dt') or dt
        item['last_price'] = float(price)
        item['market_value'] = round(new_amount * float(price), 2)
        self._invalidate_cache()

    def reduce_margin_short(self, stock, amount):
        """减少融券负债。"""
        item = self.margin_short_positions.get(stock)
        if item is None:
            return 0

        current_amount = int(item.get('amount', 0) or 0)
        reduced = min(int(amount), current_amount)
        remaining = current_amount - reduced
        if remaining <= 0:
            del self.margin_short_positions[stock]
        else:
            item['amount'] = remaining
        self._invalidate_cache()
        return reduced

    def get_margin_contracts_frame(self):
        """返回当前未结两融合约。"""
        rows = []
        for stock, item in self.margin_cash_positions.items():
            last_price = self._get_market_price(stock, item.get('open_price', 0.0))
            rows.append({
                'compact_id': 'MC_{}'.format(stock.replace('.', '_')),
                'stock_code': stock,
                'compact_type': 'margincash',
                'amount': int(item.get('amount', 0) or 0),
                'open_price': round(float(item.get('open_price', 0.0) or 0.0), 4),
                'last_price': round(float(last_price), 4),
                'market_value': round(int(item.get('amount', 0) or 0) * last_price, 2),
                'debt_balance': round(float(item.get('debt_balance', 0.0) or 0.0), 2),
                'open_dt': item.get('open_dt'),
            })
        for stock, item in self.margin_short_positions.items():
            price = self._get_market_price(stock, item.get('open_price', 0.0))
            rows.append({
                'compact_id': 'MS_{}'.format(stock.replace('.', '_')),
                'stock_code': stock,
                'compact_type': 'marginsec',
                'amount': int(item.get('amount', 0) or 0),
                'open_price': round(float(item.get('open_price', 0.0) or 0.0), 4),
                'last_price': round(float(price), 4),
                'market_value': round(int(item.get('amount', 0) or 0) * price, 2),
                'debt_balance': round(int(item.get('amount', 0) or 0) * price, 2),
                'open_dt': item.get('open_dt'),
            })

        if not rows:
            return pd.DataFrame(columns=['compact_id', 'stock_code', 'compact_type', 'amount', 'open_price', 'last_price', 'market_value', 'debt_balance', 'open_dt'])
        return pd.DataFrame(rows)

    def get_margin_snapshot_rows(self, name_map=None):
        """导出当前两融头寸快照。"""
        name_map = name_map or {}
        rows = []
        for stock, item in self.margin_cash_positions.items():
            amount = int(item.get('amount', 0) or 0)
            debt_balance = float(item.get('debt_balance', 0.0) or 0.0)
            if amount <= 0 and debt_balance <= 1e-8:
                continue
            price = self._get_market_price(stock, item.get('open_price', 0.0))
            rows.append({
                'c': stock,
                'nm': name_map.get(stock, stock),
                'n': amount,
                'v': round(amount * price, 2),
                'b': round(float(item.get('open_price', 0.0) or 0.0), 2),
                'bt': 'MARGIN_CASH',
                'debt_balance': round(debt_balance, 2),
                'open_dt': item.get('open_dt'),
            })
        for stock, item in self.margin_short_positions.items():
            amount = int(item.get('amount', 0) or 0)
            if amount <= 0:
                continue
            price = self._get_market_price(stock, item.get('open_price', 0.0))
            rows.append({
                'c': stock,
                'nm': name_map.get(stock, stock),
                'n': -amount,
                'v': round(amount * price, 2),
                'b': round(float(item.get('open_price', 0.0) or 0.0), 2),
                'bt': 'MARGIN_SHORT',
                'debt_balance': round(amount * price, 2),
                'open_dt': item.get('open_dt'),
            })
        return rows

    def _invalidate_cache(self):
        """清空 portfolio_value 缓存（持仓变化时调用）

        注意：不清空 _close_price_cache，因为同一天收盘价不变
        """
        self._cached_portfolio_value = None

    def add_position(self, stock, amount, price, date):
        """买入建仓/加仓"""
        if stock not in self.positions:
            self.positions[stock] = Position(stock, amount, price)
            self._position_lots[stock] = [{'date': date, 'amount': amount, 'dividends': [], 'dividends_total': 0.0}]
        else:
            # 可变模式：直接修改现有position
            position = self.positions[stock]
            new_amount = position.amount + amount
            new_cost = (position.amount * position.cost_basis + amount * price) / new_amount
            position.amount = new_amount
            position.cost_basis = new_cost
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
            position.enable_amount = max(position.enable_amount - amount, 0)
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
        """当前现金余额（未扣除两融保证金占用）。"""
        return self._cash

    @property
    def available_cash(self):
        """当前可自由用于新增交易的资金。"""
        if not self.margin_enabled:
            return self._cash
        spare_equity = max(self.portfolio_value - self.get_margin_required_collateral(), 0.0)
        return max(min(self._cash, spare_equity), 0.0)

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

            current_price = self._get_market_price(stock, position.cost_basis)
            position.last_sale_price = current_price
            position.market_value = position.amount * current_price
            positions_value += position.amount * current_price

        self.positions_value = positions_value
        short_liability = self.get_marginsec_liability_value() if self.margin_enabled else 0.0
        result = total + positions_value - self.cash_liability - self.margin_interest - short_liability

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
    def __init__(self, stock: str, amount: float, cost_basis: float):
        self.stock = stock
        self.sid = stock  # 别名，保持兼容
        self.amount = amount
        self.cost_basis = cost_basis
        self.enable_amount = 0  # T+1：当日买入不可卖，日切时由引擎重置
        self.last_sale_price = cost_basis
        self.today_amount = 0
        self.business_type = 'STOCK'
        self.market_value = amount * cost_basis


