# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
数据存储工具函数

仅支持 Parquet 格式
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path


def _date_to_int(dt_series: pd.Series) -> pd.Series:
    """向量化将datetime转为YYYYMMDD整数"""
    return (
        dt_series.dt.year * 10000 +
        dt_series.dt.month * 100 +
        dt_series.dt.day
    ).astype(int)


def _date_to_iso(dt_series: pd.Series) -> pd.Series:
    """向量化将datetime转为YYYY-MM-DD字符串"""
    return (
        dt_series.dt.year.astype(str) + '-' +
        dt_series.dt.month.astype(str).str.zfill(2) + '-' +
        dt_series.dt.day.astype(str).str.zfill(2)
    )


def load_stock(data_dir, symbol):
    """加载股票价格数据"""
    parquet_file = Path(data_dir) / 'stocks' / f'{symbol}.parquet'
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        if not df.empty and 'date' in df.columns:
            df.set_index('date', inplace=True)
        return df
    return pd.DataFrame()


def load_valuation(data_dir, symbol):
    """加载估值数据"""
    parquet_file = Path(data_dir) / 'valuation' / f'{symbol}.parquet'
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        if not df.empty and 'date' in df.columns:
            df.set_index('date', inplace=True)
        return df
    return pd.DataFrame()


def load_fundamentals(data_dir, symbol):
    """加载财务数据"""
    parquet_file = Path(data_dir) / 'fundamentals' / f'{symbol}.parquet'
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        if not df.empty and 'date' in df.columns:
            df.set_index('date', inplace=True)
        return df
    return pd.DataFrame()


def load_exrights(data_dir, symbol):
    """加载除权数据

    Returns:
        dict，包含除权事件、复权因子、分红信息
    """
    empty_result = {
        'exrights_events': pd.DataFrame(),
        'adj_factors': pd.DataFrame(),
        'dividends': []
    }

    parquet_file = Path(data_dir) / 'exrights' / f'{symbol}.parquet'
    if not parquet_file.exists():
        return empty_result

    df = pd.read_parquet(parquet_file)
    if df.empty:
        return empty_result

    # 构建exrights_events
    ex_df = df.copy()
    if 'date' in ex_df.columns:
        ex_df['date'] = _date_to_int(ex_df['date'])
        ex_df.set_index('date', inplace=True)

    # 构建dividends列表
    dividends = []
    if 'dividend' in df.columns:
        valid_mask = df['dividend'].notna()
        if valid_mask.any():
            valid_df = df.loc[valid_mask, ['date', 'dividend']]
            date_strs = _date_to_iso(valid_df['date'])
            dividends = [
                {'date': d, 'dividend': div}
                for d, div in zip(date_strs.values, valid_df['dividend'].values)
            ]

    return {
        'exrights_events': ex_df,
        'adj_factors': pd.DataFrame(),
        'dividends': dividends
    }


def load_metadata(data_dir, filename):
    """加载元数据文件

    Args:
        data_dir: 数据根目录
        filename: 元数据文件名（如 'metadata' 或 'trade_days'）

    Returns:
        解析后的数据（dict或DataFrame）
    """
    data_path = Path(data_dir) / 'metadata'

    # 兼容旧调用：去除.br后缀
    if filename.endswith('.br'):
        filename = filename[:-3]

    # metadata特殊处理：已拆分为index_constituents和stock_status
    if filename == 'metadata':
        ic_file = data_path / 'index_constituents.parquet'
        ss_file = data_path / 'stock_status.parquet'
        if ic_file.exists() or ss_file.exists():
            return _load_metadata_parquet(data_path, filename)

    # 其他元数据
    parquet_file = data_path / f'{filename}.parquet'
    if parquet_file.exists():
        return _load_metadata_parquet(data_path, filename)

    return None


def _load_metadata_parquet(metadata_dir, base_name):
    """加载Parquet格式的元数据"""
    # metadata特殊处理：已拆分为index_constituents和stock_status
    if base_name == 'metadata':
        result = {}

        # index_constituents (预聚合格式: date, index_code, symbols)
        ic_file = metadata_dir / 'index_constituents.parquet'
        if ic_file.exists():
            ic_df = pd.read_parquet(ic_file)
            index_constituents = {}
            for date, group in ic_df.groupby('date'):
                index_constituents[date] = dict(zip(group['index_code'], group['symbols']))
            result['index_constituents'] = index_constituents

        # stock_status_history (预聚合格式: date, status_type, symbols)
        ss_file = metadata_dir / 'stock_status.parquet'
        if ss_file.exists():
            ss_df = pd.read_parquet(ss_file)
            stock_status_history = {}
            for date, group in ss_df.groupby('date'):
                stock_status_history[date] = {'ST': {}, 'HALT': {}, 'DELISTING': {}}
                for st, syms in zip(group['status_type'], group['symbols']):
                    stock_status_history[date][st] = dict.fromkeys(syms, True)
            result['stock_status_history'] = stock_status_history

        return result if result else None

    file_path = metadata_dir / f'{base_name}.parquet'
    if not file_path.exists():
        return None

    df = pd.read_parquet(file_path)

    if base_name == 'trade_days':
        return {'trade_days': _date_to_iso(df['date']).tolist()}

    elif base_name == 'stock_metadata':
        return {'data': df.to_dict('records')}

    elif base_name == 'benchmark':
        if 'date' in df.columns:
            df = df.copy()
            df['date'] = _date_to_iso(df['date'])
        return {'data': df.to_dict('records')}

    elif base_name == 'version':
        return df.iloc[0].to_dict()

    # 默认返回DataFrame
    return df


def list_stocks(data_dir):
    """列出所有可用的股票代码"""
    stocks_dir = Path(data_dir) / 'stocks'
    if not stocks_dir.exists():
        return []

    parquet_files = list(stocks_dir.glob('*.parquet'))
    return [f.stem for f in parquet_files]


def _simulate_minute_data(daily_df):
    """基于日线数据模拟分钟数据（开盘价到收盘价的线性插值）

    模拟方法：
    1. 价格：从开盘价线性插值到收盘价
    2. 成交量：均匀分布到每个分钟
    3. 交易时间：9:30-11:30, 13:00-15:00（共240分钟）

    Args:
        daily_df: 日线数据，必须包含 open, close, high, low, volume 列

    Returns:
        分钟线DataFrame，datetime索引
    """
    if daily_df.empty or not all(col in daily_df.columns for col in ['open', 'close', 'volume']):
        return pd.DataFrame()

    minute_records = []

    for date, row in daily_df.iterrows():
        # 确定日期（如果索引是DatetimeIndex则直接使用，否则转为datetime）
        if hasattr(date, 'date'):
            base_date = pd.to_datetime(date).date()
        else:
            base_date = pd.to_datetime(date).date()

        # 定义交易时间段（每段2小时=120分钟，共240分钟）
        morning_sessions = [(9, 30), (10, 30), (11, 30)]
        afternoon_sessions = [(13, 0), (14, 0), (15, 0)]

        all_minutes = []
        for i in range(len(morning_sessions) - 1):
            start = morning_sessions[i]
            end = morning_sessions[i + 1]
            all_minutes.extend(_generate_minute_times(base_date, start[0], start[1], end[0], end[1]))
        for i in range(len(afternoon_sessions) - 1):
            start = afternoon_sessions[i]
            end = afternoon_sessions[i + 1]
            all_minutes.extend(_generate_minute_times(base_date, start[0], start[1], end[0], end[1]))

        if not all_minutes:
            continue

        total_minutes = len(all_minutes)

        # 获取日线数据
        open_price = row['open']
        close_price = row['close']
        high_price = row['high']
        low_price = row['low']
        daily_volume = row['volume']

        # 线性插值价格（从开盘到收盘）
        price_slope = (close_price - open_price) / (total_minutes - 1) if total_minutes > 1 else 0
        prices = [open_price + price_slope * i for i in range(total_minutes)]

        # 确保价格在high和low范围内
        min_high = max(prices) if max(prices) > low_price else high_price
        max_low = min(prices) if min(prices) < high_price else low_price

        # 成交量均匀分布
        minute_volume = daily_volume / total_minutes if daily_volume > 0 else 0

        # 生成分钟记录
        for i, dt in enumerate(all_minutes):
            minute_records.append({
                'datetime': dt,
                'open': prices[i],
                'close': prices[i],
                'high': min_high,
                'low': max_low,
                'volume': minute_volume,
                'money': prices[i] * minute_volume
            })

    if minute_records:
        df = pd.DataFrame(minute_records)
        df.set_index('datetime', inplace=True)
        return df
    return pd.DataFrame()


def _generate_minute_times(date, start_hour, start_minute, end_hour, end_minute):
    """生成指定时间段的每分钟时间戳（不包括结束时间点）"""
    times = []
    # 修复时间戳生成，使用pd.Timestamp的年月日构造方式
    start_time = pd.Timestamp(
        year=date.year,
        month=date.month,
        day=date.day,
        hour=start_hour,
        minute=start_minute
    )

    end_time = pd.Timestamp(
        year=date.year,
        month=date.month,
        day=date.day,
        hour=end_hour,
        minute=end_minute
    )

    current = start_time

    # 使用严格小于，不包括结束时间点
    while current < end_time:
        times.append(current)
        current += pd.Timedelta(minutes=1)

    return times


def load_stock_1m(data_dir, symbol, simulate_if_missing=True):
    """加载分钟线数据，如果不存在则基于日线数据模拟

    Args:
        data_dir: 数据目录
        symbol: 股票代码
        simulate_if_missing: 如果真实数据不存在，是否基于日线数据模拟

    Returns:
        分钟线DataFrame，datetime索引
    """
    parquet_file = Path(data_dir) / 'stocks_1m' / (symbol + '.parquet')
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        if not df.empty:
            if 'datetime' in df.columns:
                df.set_index('datetime', inplace=True)
            elif 'date' in df.columns:
                df.set_index('date', inplace=True)
        return df

    # 如果真实分钟数据不存在且允许模拟，则基于日线数据模拟
    if simulate_if_missing:
        daily_df = load_stock(data_dir, symbol)
        if not daily_df.empty:
            return _simulate_minute_data(daily_df)

    return pd.DataFrame()


def list_stocks_1m(data_dir):
    """列出所有可用的分钟数据股票代码"""
    stocks_dir = Path(data_dir) / 'stocks_1m'
    if not stocks_dir.exists():
        return []

    parquet_files = list(stocks_dir.glob('*.parquet'))
    return [f.stem for f in parquet_files]
