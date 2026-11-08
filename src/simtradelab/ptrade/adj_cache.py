# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
复权因子缓存模块 - 使用Parquet格式

前复权公式：前复权价 = adj_a * 未复权价 + adj_b  (adj_a=ef_a, adj_b=ef_b)
后复权公式：后复权价 = adj_a * 未复权价 + adj_b  (adj_a/adj_b 从除权事件累积)
"""

import pandas as pd
import numpy as np
import os
from ..utils.perf import timer
from joblib import Parallel, delayed


def _adj_cache_path(data_dir: str, kind: str) -> str:
    """返回市场特定的复权因子缓存路径"""
    return os.path.join(data_dir, f"ptrade_adj_{kind}.parquet")


def _calculate_adj_factors_from_events(stock, stock_df, exrights_events):
    """从平台预计算因子构建前复权因子

    平台公式: P_adj = ef_a * P + ef_b
    直接存储 ef_a/ef_b 作为 adj_a/adj_b，避免中间除法的精度损失
    """
    if stock_df is None or stock_df.empty:
        return None

    if exrights_events is None or exrights_events.empty:
        adj_factors = pd.DataFrame(
            index=stock_df.index, columns=["adj_a", "adj_b"], dtype="float64"
        )
        adj_factors["adj_a"] = 1.0
        adj_factors["adj_b"] = 0.0
        return adj_factors

    try:
        ex_dates_dt = pd.to_datetime(exrights_events.index.tolist(), format="%Y%m%d")
        n_events = len(ex_dates_dt)

        ef_a = exrights_events['exer_forward_a'].values
        ef_b = exrights_events['exer_forward_b'].values

        # 向量化转换：[n_events] 个除权区间 + 最新区间(1.0, 0.0)
        forward_a = np.ones(n_events + 1, dtype="float64")
        forward_b = np.zeros(n_events + 1, dtype="float64")
        forward_a[:n_events] = ef_a
        forward_b[:n_events] = ef_b

        # searchsorted 将每个交易日映射到对应的除权区间
        factor_idx = np.searchsorted(ex_dates_dt.values, stock_df.index.values, side="right")

        return pd.DataFrame(
            index=stock_df.index,
            data={"adj_a": forward_a[factor_idx], "adj_b": forward_b[factor_idx]},
        )

    except (ValueError, KeyError, IndexError, pd.errors.EmptyDataError) as e:
        import logging
        logging.getLogger(__name__).error(f"计算 {stock} 前复权因子失败: {e}")
        return None


def _adj_cache_to_parquet(adj_factors_cache, cache_path):
    """将复权因子缓存保存为Parquet格式

    将 dict[str, DataFrame] 转为单个长表格式存储
    """
    if not adj_factors_cache:
        return

    rows = []
    for stock, df in adj_factors_cache.items():
        if df is not None and not df.empty:
            df_copy = df.reset_index()
            df_copy['symbol'] = stock
            rows.append(df_copy)

    if rows:
        combined = pd.concat(rows, ignore_index=True)
        combined.to_parquet(cache_path, index=False)


def _parquet_to_adj_cache(cache_path):
    """从Parquet格式加载复权因子缓存

    Returns:
        dict[str, DataFrame]: {stock: adj_factors_df}
    """
    if not os.path.exists(cache_path):
        return None

    combined = pd.read_parquet(cache_path)
    adj_factors_cache = {}

    # 一次性去掉 symbol 列并设置日期索引，再按 groupby 的位置分组切片，
    # 避免逐组 drop(columns).copy() 的重复开销（结果逐值相同）。
    date_col = 'date' if 'date' in combined.columns else (
        'index' if 'index' in combined.columns else None
    )
    data = combined.drop(columns=['symbol'])
    if date_col is not None:
        data = data.set_index(date_col)
    for symbol, positions in combined.groupby('symbol').indices.items():
        adj_factors_cache[symbol] = data.iloc[positions]

    return adj_factors_cache


class AdjFactorsDict(dict):
    """复权因子惰性字典 —— 用 parquet filters 下推替代全量 read_parquet。

    原实现每次 DataServer 初始化都把 pre/post 两张长表全量读入内存
    （合计约 5073 万行 / 4.6 GB 常驻），而一次回测通常只访问几十只股票。
    这里把 dict 的值换成「未加载」哨兵，按 symbol 惰性加载：

    * ``bool`` / ``keys`` / ``in`` / ``[]`` / ``get`` 全部保持 dict 语义；
    * ``ensure_loaded(keys)`` 用一次 ``filters=[('symbol','in', keys)]``
      批量读入（row-group statistics 下推），避免逐只查询退化；
    * 已验证 filters 切片与全量 ``groupby().indices`` 切片 ``assert_frame_equal``
      （check_exact=True）逐值一致，DataFrame 的 index/dtype/列顺序不变。

    ``candidate_keys`` 应当是 parquet symbol 集合的超集；对 parquet 中确实
    不存在的 symbol 会标记为 _MISSING，``in`` 返回 False、``[]`` 抛 KeyError，
    与全量加载得到的小 dict 行为一致。
    """

    _UNLOADED = object()
    _MISSING = object()

    def __init__(self, cache_path, candidate_keys):
        super().__init__()
        self._cache_path = cache_path
        self._candidate = set(candidate_keys)
        dict.update(self, dict.fromkeys(candidate_keys, self._UNLOADED))

    def ensure_loaded(self, keys) -> None:
        """一次性批量加载 keys 中尚未加载的 symbol。"""
        missing = [
            k for k in dict.fromkeys(keys)
            if k in self._candidate and dict.__getitem__(self, k) is self._UNLOADED
        ]
        if not missing:
            return

        sub = pd.read_parquet(
            self._cache_path, filters=[("symbol", "in", missing)]
        )
        found = set()
        if not sub.empty:
            date_col = "date" if "date" in sub.columns else (
                "index" if "index" in sub.columns else None
            )
            data = sub.drop(columns=["symbol"])
            if date_col is not None:
                data = data.set_index(date_col)
            for symbol, positions in sub.groupby("symbol").indices.items():
                dict.__setitem__(self, symbol, data.iloc[positions])
                found.add(symbol)
        for k in missing:
            if k not in found:
                dict.__setitem__(self, k, self._MISSING)

    def __contains__(self, key):
        if key not in self._candidate:
            return False
        value = dict.__getitem__(self, key)
        if value is self._UNLOADED:
            self.ensure_loaded([key])
            value = dict.__getitem__(self, key)
        return value is not self._MISSING

    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if value is self._UNLOADED:
            self.ensure_loaded([key])
            value = dict.__getitem__(self, key)
        if value is self._MISSING:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


def _validate_adj_cache_file(cache_path) -> None:
    """轻量元数据校验：保留原「缓存损坏立即重建」的时机，但不读全量数据。"""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(cache_path)
    names = set(pf.schema_arrow.names)
    required = {"symbol", "adj_a", "adj_b"}
    if not required.issubset(names):
        raise ValueError(f"复权缓存缺少必要列: {required - names}")
    if pf.metadata.num_rows <= 0:
        raise ValueError("复权缓存为空")


def _load_lazy_adj_cache(data_context, kind):
    """返回按 symbol 惰性加载的复权因子字典。"""
    import logging
    logger = logging.getLogger(__name__)

    cache_path = _adj_cache_path(data_context.stock_data_dict.data_dir, kind)
    create = create_adj_pre_cache if kind == "pre" else create_adj_post_cache

    if not os.path.exists(cache_path):
        create(data_context)

    logger.info("正在校验%s复权因子缓存..." % ("前" if kind == "pre" else "后"))
    try:
        _validate_adj_cache_file(cache_path)
    except Exception as e:
        logger.error(f"缓存文件损坏或格式错误: {e}")
        try:
            os.remove(cache_path)
            logger.info("已删除损坏的缓存文件，重新创建...")
        except OSError as remove_error:
            logger.error(f"删除损坏的缓存文件失败: {remove_error}")
            raise
        create(data_context)
        _validate_adj_cache_file(cache_path)

    candidate = list(data_context.stock_data_dict.keys())
    cache = AdjFactorsDict(cache_path, candidate)
    logger.info(
        f"✓ {'前' if kind == 'pre' else '后'}复权因子缓存就绪 "
        f"（{len(candidate)} 只股票按需加载）"
    )
    return cache


def _load_exrights_for_adj(data_context, all_stocks, data_dir, num_workers, logger):
    """加载全市场除权事件；同一 data_context 内 pre/post 缓存共用一次结果。

    create_adj_pre_cache / create_adj_post_cache 原本各自全市场扫一遍
    storage.load_exrights，这里按 (data_dir, all_stocks) 缓存到 data_context
    实例上（同一 DataServer 初始化传入的是同一个 temp_context），避免重复
    的多进程加载；跨 data_context 不共享，不会污染其他回测。
    """
    cache_key = (data_dir, tuple(all_stocks))
    cached = getattr(data_context, '_adj_exrights_loaded', None)
    if cached is not None and cached[0] == cache_key:
        return cached[1]

    from . import storage
    exrights_results = Parallel(n_jobs=num_workers, backend="loky", verbose=0)(
        delayed(storage.load_exrights)(data_dir, stock) for stock in all_stocks
    )
    exrights_cache = {}
    for stock, exrights_full in zip(all_stocks, exrights_results):
        if exrights_full and "exrights_events" in exrights_full:
            ex_df = exrights_full["exrights_events"]
            if not ex_df.empty:
                exrights_cache[stock] = ex_df
    logger.info(f"    已加载 {len(exrights_cache)} 只股票的除权数据")
    data_context._adj_exrights_loaded = (cache_key, exrights_cache)
    return exrights_cache


@timer(threshold=0.1, name="perf.name.adj_pre_create")
def create_adj_pre_cache(data_context):
    """创建并保存所有股票的前复权因子缓存"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info("正在创建前复权因子缓存...")
    all_stocks = list(data_context.stock_data_dict.keys())
    total_stocks = len(all_stocks)

    logger.info("  预加载股票价格数据...")
    stock_data_cache = {s: data_context.stock_data_dict.get(s) for s in all_stocks}

    logger.info("  加载除权事件数据...")
    data_dir = data_context.stock_data_dict.data_dir
    num_workers = int(os.getenv("PTRADE_NUM_WORKERS", "-1"))

    try:
        exrights_cache = _load_exrights_for_adj(
            data_context, all_stocks, data_dir, num_workers, logger
        )

        logger.info(f"  并行计算前复权因子({num_workers if num_workers > 0 else 'auto'} 进程)...")

        results = Parallel(n_jobs=num_workers, backend="loky", verbose=0)(
            delayed(_calculate_adj_factors_from_events)(
                stock, stock_data_cache.get(stock), exrights_cache.get(stock)
            )
            for stock in all_stocks
        )

        logger.info("  正在保存到Parquet文件...")
        adj_factors_cache = {}
        saved_count = 0
        failed_stocks = []

        for stock, adj_factors in zip(all_stocks, results):
            if adj_factors is not None:
                adj_factors_cache[stock] = adj_factors
                saved_count += 1
            else:
                failed_stocks.append(stock)

        cache_path = _adj_cache_path(data_dir, "pre")
        _adj_cache_to_parquet(adj_factors_cache, cache_path)

        file_size = os.path.getsize(cache_path) / 1024 / 1024

        logger.info("✓ 前复权因子缓存创建完成！")
        logger.info(f"  处理: {total_stocks} 只股票")
        logger.info(f"  保存: {saved_count} 只（有除权数据或价格数据）")
        if failed_stocks:
            logger.warning(f"  失败股票: {len(failed_stocks)} 只")
        logger.info(f"  文件: {cache_path} ({file_size:.1f}MB)")

    except OSError as e:
        logger.error(f"创建前复权因子缓存失败: {e}")
        raise
    except Exception as e:
        logger.error(f"创建前复权因子缓存时发生未预期错误: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise


@timer(threshold=0.1, name="perf.name.adj_pre_load")
def load_adj_pre_cache(data_context):
    """加载前复权因子缓存（按 symbol 惰性加载）

    前复权价 = adj_a * 未复权价 + adj_b
    """
    return _load_lazy_adj_cache(data_context, "pre")


def _calculate_adj_post_factors_from_events(stock, stock_df, exrights_events):
    """从除权除息事件计算后复权因子

    后复权公式: P_adj = adj_a * P + adj_b
    从历史往最新累积，上市首日 adj_a=1, adj_b=0
    """
    if stock_df is None or stock_df.empty:
        return None

    if exrights_events is None or exrights_events.empty:
        adj_factors = pd.DataFrame(
            index=stock_df.index, columns=["adj_a", "adj_b"], dtype="float64"
        )
        adj_factors["adj_a"] = 1.0
        adj_factors["adj_b"] = 0.0
        return adj_factors

    try:
        ex_dates_dt = pd.to_datetime(exrights_events.index.tolist(), format="%Y%m%d")

        allotted_ps = exrights_events["allotted_ps"].values
        bonus_ps = exrights_events["bonus_ps"].values
        rationed_ps = exrights_events["rationed_ps"].values
        rationed_px = exrights_events["rationed_px"].values

        n_events = len(allotted_ps)
        backward_a = np.ones(n_events + 1, dtype="float64")
        backward_b = np.zeros(n_events + 1, dtype="float64")

        for i in range(n_events):
            # 官方口径（PTrade/api_doc.txt get_stock_exrights 示例 / PTRADE_PARQUET_FORMAT.md）:
            #   后复权价 = A * 原价 + B
            #   A[i+1] = A[i] * m,  m = 1 + allotted_ps + rationed_ps
            #   B[i+1] = B[i] + A[i] * (bonus_ps - rationed_ps * rationed_px)
            # 官方示例 600570.SS: (1.0,0.430) -> (1.5,0.630) -> (2.1,0.630)
            #                  -> (2.94,0.861)，可用于回归断言。
            # 旧实现 `B[i]*m + bonus` 会把因子本身再乘一次 m，导致后复权价系统性偏高。
            m = 1.0 + allotted_ps[i] + rationed_ps[i]
            backward_a[i + 1] = backward_a[i] * m
            backward_b[i + 1] = backward_b[i] + backward_a[i] * (
                bonus_ps[i] - rationed_ps[i] * rationed_px[i]
            )

        factor_idx = np.searchsorted(ex_dates_dt.values, stock_df.index.values, side="right")

        return pd.DataFrame(
            index=stock_df.index,
            data={"adj_a": backward_a[factor_idx], "adj_b": backward_b[factor_idx]},
        )

    except (ValueError, KeyError, IndexError, pd.errors.EmptyDataError) as e:
        import logging
        logging.getLogger(__name__).error(f"计算 {stock} 后复权因子失败: {e}")
        return None


@timer(threshold=0.1, name="perf.name.adj_post_create")
def create_adj_post_cache(data_context):
    """创建并保存所有股票的后复权因子缓存"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info("正在创建后复权因子缓存...")
    all_stocks = list(data_context.stock_data_dict.keys())
    total_stocks = len(all_stocks)

    logger.info("  预加载股票价格数据...")
    stock_data_cache = {s: data_context.stock_data_dict.get(s) for s in all_stocks}

    logger.info("  加载除权事件数据...")
    data_dir = data_context.stock_data_dict.data_dir
    num_workers = int(os.getenv("PTRADE_NUM_WORKERS", "-1"))

    try:
        exrights_cache = _load_exrights_for_adj(
            data_context, all_stocks, data_dir, num_workers, logger
        )

        logger.info(f"  并行计算后复权因子({num_workers if num_workers > 0 else 'auto'} 进程)...")

        results = Parallel(n_jobs=num_workers, backend="loky", verbose=0)(
            delayed(_calculate_adj_post_factors_from_events)(
                stock, stock_data_cache.get(stock), exrights_cache.get(stock)
            )
            for stock in all_stocks
        )

        logger.info("  正在保存到Parquet文件...")
        adj_factors_cache = {}
        saved_count = 0
        failed_stocks = []

        for stock, adj_factors in zip(all_stocks, results):
            if adj_factors is not None:
                adj_factors_cache[stock] = adj_factors
                saved_count += 1
            else:
                failed_stocks.append(stock)

        cache_path = _adj_cache_path(data_dir, "post")
        _adj_cache_to_parquet(adj_factors_cache, cache_path)

        file_size = os.path.getsize(cache_path) / 1024 / 1024

        logger.info("✓ 后复权因子缓存创建完成！")
        logger.info(f"  处理: {total_stocks} 只股票")
        logger.info(f"  保存: {saved_count} 只（有除权数据或价格数据）")
        if failed_stocks:
            logger.warning(f"  失败股票: {len(failed_stocks)} 只")
        logger.info(f"  文件: {cache_path} ({file_size:.1f}MB)")

    except OSError as e:
        logger.error(f"创建后复权因子缓存失败: {e}")
        raise
    except Exception as e:
        logger.error(f"创建后复权因子缓存时发生未预期错误: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        raise


@timer(threshold=0.1, name="perf.name.adj_post_load")
def load_adj_post_cache(data_context):
    """加载后复权因子缓存（按 symbol 惰性加载）

    后复权价 = adj_a * 未复权价 + adj_b
    """
    return _load_lazy_adj_cache(data_context, "post")


def create_dividend_cache(data_context):
    """按需加载分红数据

    返回: DividendLazyLoader对象，支持按需加载
    """
    return DividendLazyLoader(data_context.stock_data_dict.data_dir)


class DividendLazyLoader:
    """延迟加载分红数据 - 按股票代码按需加载"""

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self._cache = {}

    def get(self, stock_code, default=None):
        """获取指定股票的分红数据"""
        if stock_code in self._cache:
            return self._cache[stock_code]

        from . import storage
        exrights_data = storage.load_exrights(self.data_dir, stock_code)

        if not exrights_data or "dividends" not in exrights_data:
            self._cache[stock_code] = default
            return default

        dividends_list = exrights_data["dividends"]
        if not dividends_list:
            self._cache[stock_code] = default
            return default

        result = {}
        for event in dividends_list:
            date_str = event["date"].replace("-", "")
            dividend = event["dividend"]
            if dividend > 0:
                result[date_str] = dividend

        self._cache[stock_code] = result if result else default
        return self._cache[stock_code]

    def __contains__(self, stock_code):
        return self.get(stock_code) is not None

    def __getitem__(self, stock_code):
        result = self.get(stock_code)
        if result is None:
            raise KeyError(stock_code)
        return result
