# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
#
"""
回测统计分析模块

包含收益率、风险指标、交易统计等计算函数，以及图表生成函数
"""


import os
import json
import numpy as np
from simtradelab.utils.plot import save_figure

from simtradelab.utils.perf import timer
from simtradelab.backtest.backtest_stats import BacktestStats


def _load_index_names():
    """加载指数名称映射

    Returns:
        dict: 指数代码到名称的映射字典
    """
    indices_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'indices.json')
    try:
        with open(indices_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _get_benchmark_name(benchmark_code):
    """获取基准名称

    Args:
        benchmark_code: 基准代码

    Returns:
        str: 基准名称，如果找不到则返回代码本身
    """
    index_names = _load_index_names()
    return index_names.get(benchmark_code, benchmark_code)


def calculate_returns(portfolio_values):
    """计算收益率指标

    Args:
        portfolio_values: 每日组合价值数组

    Returns:
        dict: 包含total_return, annual_return, daily_returns等
    """
    if len(portfolio_values) == 0:
        return {
            'total_return': 0,
            'annual_return': 0,
            'daily_returns': np.array([]),
            'initial_value': 0,
            'final_value': 0,
            'trading_days': 0
        }

    initial_value = portfolio_values[0]
    final_value = portfolio_values[-1]
    total_return = (final_value - initial_value) / initial_value if initial_value > 0 else 0

    # 每日收益率
    daily_returns = np.diff(portfolio_values) / portfolio_values[:-1]

    # 年化收益率（假设252个交易日）
    trading_days = len(portfolio_values)
    annual_return = (final_value / initial_value) ** (252 / trading_days) - 1 if trading_days > 0 and initial_value > 0 else 0

    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'daily_returns': daily_returns,
        'initial_value': initial_value,
        'final_value': final_value,
        'trading_days': trading_days
    }


def calculate_risk_metrics(daily_returns, portfolio_values):
    """计算风险指标

    Args:
        daily_returns: 每日收益率数组
        portfolio_values: 每日组合价值数组

    Returns:
        dict: 包含sharpe_ratio, max_drawdown, volatility等
    """
    # 夏普比率
    if len(daily_returns) > 0 and np.std(daily_returns) > 0:
        sharpe_ratio = np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252)
    else:
        sharpe_ratio = 0

    # 最大回撤
    cummax = np.maximum.accumulate(portfolio_values)
    drawdown = (portfolio_values - cummax) / cummax
    max_drawdown = np.min(drawdown)

    # 波动率（年化）
    volatility = np.std(daily_returns) * np.sqrt(252) if len(daily_returns) > 0 else 0

    # Sortino比率（仅用负收益标准差）
    downside = daily_returns[daily_returns < 0]
    downside_std = np.std(downside) * np.sqrt(252) if len(downside) > 0 else 0
    sortino_ratio = (np.mean(daily_returns) * 252) / downside_std if downside_std > 0 else 0

    return {
        'sharpe_ratio': sharpe_ratio,
        'sortino_ratio': sortino_ratio,
        'max_drawdown': max_drawdown,
        'volatility': volatility,
        'drawdown': drawdown
    }


def calculate_benchmark_metrics(daily_returns, benchmark_daily_returns, annual_return, benchmark_annual_return):
    """计算相对基准的指标

    Args:
        daily_returns: 策略每日收益率
        benchmark_daily_returns: 基准每日收益率
        annual_return: 策略年化收益
        benchmark_annual_return: 基准年化收益

    Returns:
        dict: 包含alpha, beta, information_ratio等
    """
    if len(daily_returns) == 0 or len(benchmark_daily_returns) == 0:
        return {
            'alpha': 0,
            'beta': 0,
            'information_ratio': 0,
            'tracking_error': 0
        }

    # 对齐长度
    min_len = min(len(daily_returns), len(benchmark_daily_returns))

    # 协方差计算至少需要2个样本
    if min_len < 2:
        return {
            'alpha': 0,
            'beta': 0,
            'information_ratio': 0,
            'tracking_error': 0
        }

    strategy_returns = daily_returns[:min_len]
    benchmark_returns = benchmark_daily_returns[:min_len]

    # 转换为numpy数组
    strategy_returns = np.array(strategy_returns)
    benchmark_returns = np.array(benchmark_returns)

    # 计算Beta
    covariance = np.cov(strategy_returns, benchmark_returns)[0][1]
    benchmark_variance = np.var(benchmark_returns)
    beta = covariance / benchmark_variance if benchmark_variance > 0 else 0

    # 计算Alpha
    alpha = annual_return - (benchmark_annual_return * beta)

    # 计算信息比率
    excess_returns = strategy_returns - benchmark_returns
    tracking_error = np.std(excess_returns) * np.sqrt(252)
    information_ratio = (annual_return - benchmark_annual_return) / tracking_error if tracking_error > 0 else 0

    return {
        'alpha': alpha,
        'beta': beta,
        'information_ratio': information_ratio,
        'tracking_error': tracking_error
    }


def calculate_trade_stats(daily_returns):
    """计算交易统计

    Args:
        daily_returns: 每日收益率数组

    Returns:
        dict: 包含win_rate, profit_loss_ratio, win_count, lose_count等
    """
    if len(daily_returns) == 0:
        return {
            'win_rate': 0,
            'profit_loss_ratio': 0,
            'win_count': 0,
            'lose_count': 0,
            'avg_win': 0,
            'avg_lose': 0
        }

    win_days = daily_returns[daily_returns > 0]
    lose_days = daily_returns[daily_returns < 0]

    win_count = len(win_days)
    lose_count = len(lose_days)
    win_rate = win_count / len(daily_returns)

    avg_win = np.mean(win_days) if len(win_days) > 0 else 0
    avg_lose = np.mean(lose_days) if len(lose_days) > 0 else 0
    profit_loss_ratio = abs(avg_win / avg_lose) if avg_lose != 0 else 0

    return {
        'win_rate': win_rate,
        'profit_loss_ratio': profit_loss_ratio,
        'win_count': win_count,
        'lose_count': lose_count,
        'avg_win': avg_win,
        'avg_lose': avg_lose
    }


def generate_backtest_report(backtest_stats: BacktestStats, start_date, end_date, benchmark_df, benchmark_code='000300.SS'):
    """生成完整的回测报告。"""
    portfolio_values = np.array(backtest_stats.portfolio_values, dtype=float)

    # 基本收益指标
    returns_metrics = calculate_returns(portfolio_values)

    # 风险指标
    risk_metrics = calculate_risk_metrics(returns_metrics['daily_returns'], portfolio_values)

    # 基准对比
    benchmark_slice = benchmark_df.loc[
        (benchmark_df.index >= start_date) &
        (benchmark_df.index <= end_date)
    ]

    if len(benchmark_slice) > 0:
        benchmark_initial = benchmark_slice['close'].iloc[0]
        benchmark_final = benchmark_slice['close'].iloc[-1]
        benchmark_return = (benchmark_final - benchmark_initial) / benchmark_initial
        benchmark_annual_return = (benchmark_final / benchmark_initial) ** (252 / len(benchmark_slice)) - 1
        benchmark_daily_returns = benchmark_slice['close'].pct_change().dropna().values

        excess_return = returns_metrics['total_return'] - benchmark_return

        benchmark_metrics = calculate_benchmark_metrics(
            returns_metrics['daily_returns'],
            benchmark_daily_returns,
            returns_metrics['annual_return'],
            benchmark_annual_return
        )
    else:
        benchmark_return = 0
        benchmark_annual_return = 0
        excess_return = 0
        benchmark_metrics = {'alpha': 0, 'beta': 0, 'information_ratio': 0, 'tracking_error': 0}

    # Calmar比率（年化收益 / 最大回撤绝对值）
    calmar_ratio = (
        returns_metrics['annual_return'] / abs(risk_metrics['max_drawdown'])
        if risk_metrics['max_drawdown'] != 0 else 0
    )

    # 交易统计
    trade_stats = calculate_trade_stats(returns_metrics['daily_returns'])

    # 获取基准名称
    benchmark_name = _get_benchmark_name(benchmark_code)

    cash_liability = np.asarray(getattr(backtest_stats, 'daily_cash_liability', []), dtype=float)
    sec_liability = np.asarray(getattr(backtest_stats, 'daily_sec_liability', []), dtype=float)
    margin_interest = np.asarray(getattr(backtest_stats, 'daily_margin_interest', []), dtype=float)
    total_debit = np.asarray(getattr(backtest_stats, 'daily_total_debit', []), dtype=float)
    maintenance = np.asarray(getattr(backtest_stats, 'daily_maintenance_margin_rate', []), dtype=float)
    bail_balance = np.asarray(getattr(backtest_stats, 'daily_enable_bail_balance', []), dtype=float)
    available_cash = np.asarray(getattr(backtest_stats, 'daily_available_cash', []), dtype=float)

    margin_enabled = bool(
        len(cash_liability) > 0 or len(sec_liability) > 0 or len(margin_interest) > 0
    ) and bool(
        np.any(np.abs(cash_liability) > 1e-8) or
        np.any(np.abs(sec_liability) > 1e-8) or
        np.any(np.abs(margin_interest) > 1e-8) or
        np.any(np.abs(total_debit) > 1e-8)
    )

    finite_maintenance = maintenance[np.isfinite(maintenance)] if maintenance.size else np.array([])
    margin_metrics = {
        'margin_enabled': bool(margin_enabled),
        'final_cash_liability': float(cash_liability[-1]) if cash_liability.size else 0.0,
        'max_cash_liability': float(np.max(cash_liability)) if cash_liability.size else 0.0,
        'final_sec_liability': float(sec_liability[-1]) if sec_liability.size else 0.0,
        'max_sec_liability': float(np.max(sec_liability)) if sec_liability.size else 0.0,
        'final_margin_interest': float(margin_interest[-1]) if margin_interest.size else 0.0,
        'max_margin_interest': float(np.max(margin_interest)) if margin_interest.size else 0.0,
        'final_total_debit': float(total_debit[-1]) if total_debit.size else 0.0,
        'max_total_debit': float(np.max(total_debit)) if total_debit.size else 0.0,
        'final_enable_bail_balance': float(bail_balance[-1]) if bail_balance.size else 0.0,
        'min_enable_bail_balance': float(np.min(bail_balance)) if bail_balance.size else 0.0,
        'final_available_cash': float(available_cash[-1]) if available_cash.size else 0.0,
        'min_maintenance_margin_rate': float(np.min(finite_maintenance)) if finite_maintenance.size else float('inf'),
        'final_maintenance_margin_rate': float(maintenance[-1]) if maintenance.size else float('inf'),
    }

    # 合并所有指标
    report = {
        **returns_metrics,
        **risk_metrics,
        'calmar_ratio': calmar_ratio,
        'benchmark_code': benchmark_code,
        'benchmark_name': benchmark_name,
        'benchmark_return': benchmark_return,
        'benchmark_annual_return': benchmark_annual_return,
        'excess_return': excess_return,
        **benchmark_metrics,
        **trade_stats,
        **margin_metrics,
    }

    return report


def _pad_series(values, expected_len):
    arr = np.asarray(values, dtype=float) if len(values) > 0 else np.array([], dtype=float)
    if expected_len == 0:
        return arr
    if arr.size == 0:
        return np.zeros(expected_len, dtype=float)
    if arr.size < expected_len:
        return np.pad(arr, (0, expected_len - arr.size), constant_values=0.0)
    return arr[:expected_len]


def _validate_chart_data(backtest_stats: BacktestStats):
    """验证并对齐图表数据。"""
    dates = np.array(backtest_stats.trade_dates)
    expected_len = len(dates)

    portfolio_values = _pad_series(backtest_stats.portfolio_values, expected_len)
    daily_pnl = _pad_series(backtest_stats.daily_pnl, expected_len)
    daily_buy = _pad_series(backtest_stats.daily_buy_amount, expected_len)
    daily_sell = _pad_series(backtest_stats.daily_sell_amount, expected_len)
    daily_positions_val = _pad_series(backtest_stats.daily_positions_value, expected_len)
    cash_liability = _pad_series(getattr(backtest_stats, 'daily_cash_liability', []), expected_len)
    sec_liability = _pad_series(getattr(backtest_stats, 'daily_sec_liability', []), expected_len)
    margin_interest = _pad_series(getattr(backtest_stats, 'daily_margin_interest', []), expected_len)
    total_debit = _pad_series(getattr(backtest_stats, 'daily_total_debit', []), expected_len)
    enable_bail_balance = _pad_series(getattr(backtest_stats, 'daily_enable_bail_balance', []), expected_len)
    maintenance_margin_rate = _pad_series(getattr(backtest_stats, 'daily_maintenance_margin_rate', []), expected_len)

    return (
        dates,
        portfolio_values,
        daily_pnl,
        daily_buy,
        daily_sell,
        daily_positions_val,
        cash_liability,
        sec_liability,
        margin_interest,
        total_debit,
        enable_bail_balance,
        maintenance_margin_rate,
    )


def _plot_nav_curve(ax, dates, portfolio_values, daily_buy, daily_sell, benchmark_data, start_date, end_date, benchmark_code='000300.SS'):
    """绘制净值曲线子图。"""
    if len(portfolio_values) == 0:
        return

    initial_value = portfolio_values[0] if portfolio_values[0] != 0 else 1.0
    strategy_nav = portfolio_values / initial_value
    ax.plot(dates, strategy_nav, linewidth=2, label='策略净值', color='#1f77b4')

    benchmark_name = _get_benchmark_name(benchmark_code)
    if benchmark_code in benchmark_data and not benchmark_data[benchmark_code].empty:
        benchmark_df_data = benchmark_data[benchmark_code]
        benchmark_slice = benchmark_df_data.loc[
            (benchmark_df_data.index >= start_date) &
            (benchmark_df_data.index <= end_date)
        ]
        if len(benchmark_slice) > 0:
            benchmark_nav = benchmark_slice['close'] / benchmark_slice['close'].iloc[0]
            ax.plot(benchmark_slice.index[:len(dates)], benchmark_nav[:len(dates)],
                    linewidth=2, label=benchmark_name, color='#ff7f0e', alpha=0.7)

    buy_dates = dates[daily_buy > 0]
    buy_navs = strategy_nav[daily_buy > 0]
    ax.scatter(buy_dates, buy_navs, marker='^', color='red', s=45, label='买入', zorder=5)

    sell_dates = dates[daily_sell > 0]
    sell_navs = strategy_nav[daily_sell > 0]
    ax.scatter(sell_dates, sell_navs, marker='v', color='green', s=45, label='卖出', zorder=5)

    ax.set_title('策略净值 vs 基准', fontsize=14, fontweight='bold')
    ax.set_ylabel('净值', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)


def _plot_daily_pnl(ax, dates, daily_pnl):
    """绘制每日盈亏子图。"""
    ax.fill_between(dates, daily_pnl, 0, where=daily_pnl >= 0, color='red', alpha=0.7)
    ax.fill_between(dates, daily_pnl, 0, where=daily_pnl < 0, color='green', alpha=0.7)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('每日盈亏', fontsize=14, fontweight='bold')
    ax.set_ylabel('盈亏（元）', fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')


def _plot_trade_amounts(ax, dates, daily_buy, daily_sell):
    """绘制交易金额子图。"""
    ax.fill_between(dates, daily_buy, 0, color='red', alpha=0.7, label='买入金额')
    ax.fill_between(dates, -daily_sell, 0, color='green', alpha=0.7, label='卖出金额')
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('每日买卖金额', fontsize=14, fontweight='bold')
    ax.set_ylabel('金额（元）', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')


def _plot_positions_value(ax, dates, daily_positions_val):
    """绘制持仓市值子图。"""
    ax.fill_between(dates, daily_positions_val, alpha=0.3, color='#9467bd')
    ax.plot(dates, daily_positions_val, linewidth=2, color='#9467bd', label='持仓市值')
    ax.set_title('每日持仓市值', fontsize=14, fontweight='bold')
    ax.set_ylabel('市值（元）', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)


def _plot_margin_liabilities(ax, dates, cash_liability, sec_liability, margin_interest, total_debit):
    """绘制融资融券负债与利息曲线。"""
    ax.plot(dates, cash_liability, linewidth=2, color='#d62728', label='融资负债')
    ax.plot(dates, sec_liability, linewidth=2, color='#1f77b4', label='融券负债')
    ax.plot(dates, total_debit, linewidth=2, color='#7f7f7f', linestyle='--', label='总负债')
    ax.fill_between(dates, margin_interest, alpha=0.18, color='#ff7f0e', label='累计利息')
    ax.set_title('融资融券负债', fontsize=14, fontweight='bold')
    ax.set_ylabel('金额（元）', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)


def _plot_margin_risk(ax, dates, enable_bail_balance, maintenance_margin_rate):
    """绘制保证金空间与维持担保比例。"""
    ax.fill_between(dates, enable_bail_balance, alpha=0.22, color='#2ca02c', label='可用保证金')
    ax.plot(dates, enable_bail_balance, linewidth=2, color='#2ca02c')
    ax.set_title('融资融券风险状态', fontsize=14, fontweight='bold')
    ax.set_ylabel('可用保证金（元）', fontsize=12)
    ax.grid(True, alpha=0.3)

    ax2 = ax.twinx()
    finite = np.isfinite(maintenance_margin_rate)
    mmr = maintenance_margin_rate.copy()
    if not np.any(finite):
        mmr = np.zeros_like(maintenance_margin_rate)
    else:
        fallback = np.max(mmr[finite])
        mmr[~finite] = fallback
    ax2.plot(dates, mmr, linewidth=1.8, color='#9467bd', label='维持担保比例')
    ax2.axhline(y=1.3, color='#ff7f0e', linestyle='--', linewidth=1.0, label='追保线(1.30)')
    ax2.axhline(y=1.1, color='#d62728', linestyle='--', linewidth=1.0, label='强平线(1.10)')
    ax2.set_ylabel('维持担保比例', fontsize=12)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='best', fontsize=10)


@timer(name="图表生成")
def generate_backtest_charts(backtest_stats: BacktestStats, start_date, end_date, benchmark_data, chart_filename, benchmark_code='000300.SS'):
    """生成回测图表。"""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'WenQuanYi Micro Hei', 'PingFang SC', 'Hiragino Sans GB', 'Ubuntu', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    (
        dates,
        portfolio_values,
        daily_pnl,
        daily_buy,
        daily_sell,
        daily_positions_val,
        cash_liability,
        sec_liability,
        margin_interest,
        total_debit,
        enable_bail_balance,
        maintenance_margin_rate,
    ) = _validate_chart_data(backtest_stats)

    if len(dates) == 0:
        raise ValueError('回测图表生成失败：没有可用的交易日数据')

    _, axes = plt.subplots(6, 1, figsize=(16, 28), sharex=True)

    _plot_nav_curve(axes[0], dates, portfolio_values, daily_buy, daily_sell, benchmark_data, start_date, end_date, benchmark_code)
    _plot_daily_pnl(axes[1], dates, daily_pnl)
    _plot_trade_amounts(axes[2], dates, daily_buy, daily_sell)
    _plot_positions_value(axes[3], dates, daily_positions_val)
    _plot_margin_liabilities(axes[4], dates, cash_liability, sec_liability, margin_interest, total_debit)
    _plot_margin_risk(axes[5], dates, enable_bail_balance, maintenance_margin_rate)
    axes[5].set_xlabel('日期', fontsize=12)

    total_days = (dates[-1] - dates[0]).days if len(dates) > 1 else 0
    if total_days > 365 * 4:
        major_locator = mdates.YearLocator()
        major_fmt = mdates.DateFormatter('%Y')
    elif total_days > 365:
        major_locator = mdates.MonthLocator(interval=3)
        major_fmt = mdates.DateFormatter('%Y-%m')
    else:
        major_locator = mdates.MonthLocator()
        major_fmt = mdates.DateFormatter('%Y-%m')

    for ax in axes:
        ax.xaxis.set_major_formatter(major_fmt)
        ax.xaxis.set_major_locator(major_locator)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    chart_dir = os.path.dirname(chart_filename)
    os.makedirs(chart_dir, exist_ok=True)

    fig = plt.gcf()
    fig.tight_layout()
    save_figure(fig, chart_filename, dpi=100)

    return chart_filename


def print_backtest_report(report, log, start_date, end_date, time_str, positions_count):
    """打印回测报告到日志。"""
    log.info("")
    log.info("=" * 70)
    log.info(f"回测报告 {start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')} | "
             f"周期: {report['trading_days']}天 | 耗时: {time_str}")
    log.info("=" * 70)

    log.info("")
    log.info(f"总收益率: {report['total_return']*100:+.2f}%  |  "
             f"年化收益: {report['annual_return']*100:+.2f}%  |  "
             f"最大回撤: {report['max_drawdown']*100:.2f}%")
    log.info(f"夏普比率: {report['sharpe_ratio']:.3f}  |  "
             f"信息比率: {report['information_ratio']:.3f}  |  "
             f"本金: {report['initial_value']/10000:.0f}万 → {report['final_value']/10000:.1f}万")
    log.info(f"索提诺比率: {report['sortino_ratio']:.3f}  |  "
             f"卡玛比率: {report['calmar_ratio']:.3f}")

    log.info("")
    benchmark_name = report.get('benchmark_name', 'Benchmark')
    log.info(f"vs {benchmark_name}: 超额收益 {report['excess_return']*100:+.2f}% | "
             f"Alpha {report['alpha']*100:+.2f}% | Beta {report['beta']:.3f}")

    avg_pos = np.mean(positions_count) if len(positions_count) > 0 else 0
    max_pos = np.max(positions_count) if len(positions_count) > 0 else 0
    log.info("")
    log.info(f"盈利天数: {report['win_count']}/{report['trading_days']}天 ({report['win_rate']*100:.1f}%) | "
             f"盈亏比: {report['profit_loss_ratio']:.2f} | "
             f"持仓: {avg_pos:.1f}只(最大{max_pos}只)")

    if report.get('margin_enabled'):
        min_mmr = report.get('min_maintenance_margin_rate', float('inf'))
        min_mmr_str = '∞' if not np.isfinite(min_mmr) else f'{min_mmr:.3f}'
        final_mmr = report.get('final_maintenance_margin_rate', float('inf'))
        final_mmr_str = '∞' if not np.isfinite(final_mmr) else f'{final_mmr:.3f}'
        log.info("")
        log.info(
            f"两融: 融资负债峰值 {report['max_cash_liability']/10000:.2f}万 | "
            f"融券负债峰值 {report['max_sec_liability']/10000:.2f}万 | "
            f"利息峰值 {report['max_margin_interest']:.2f}元"
        )
        log.info(
            f"两融期末: 总负债 {report['final_total_debit']/10000:.2f}万 | "
            f"可用保证金 {report['final_enable_bail_balance']/10000:.2f}万 | "
            f"维持担保比例 {final_mmr_str} (最低 {min_mmr_str})"
        )

    log.info("=" * 70)
