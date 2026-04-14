# -*- coding: utf-8 -*-

"""Broker profile helpers for multi-broker API compatibility."""

from __future__ import annotations

BROKER_PROFILES = frozenset(["auto", "guosheng", "dongguan", "shanxi"])

# 按券商口径严格模式下不支持的接口（仅针对当前本地已实现 API 的差异项）
BROKER_STRICT_UNSUPPORTED_APIS = {
    "guosheng": frozenset(
        [
            "filter_stock_by_status",
            "get_business_type",
            "get_current_kline_count",
            "get_dominant_contract",
            "get_frequency",
            "get_individual_transaction",
            "get_margin_asset",
            "get_reits_list",
            "get_trading_day_by_date",
            "get_trend_data",
        ]
    ),
    "dongguan": frozenset(
        [
            "filter_stock_by_status",
            "get_business_type",
            "get_current_kline_count",
            "get_dominant_contract",
            "get_frequency",
            "get_individual_transcation",
            "get_margin_asset",
            "get_reits_list",
            "get_trading_day_by_date",
            "get_trend_data",
        ]
    ),
    "shanxi": frozenset(["get_individual_transcation", "get_margin_assert"]),
}


def normalize_broker_profile(profile: str | None) -> str:
    """Normalize broker profile name.

    Accepted values: auto/guosheng/dongguan/shanxi (case-insensitive).
    """
    if profile is None:
        return "auto"
    p = str(profile).strip().lower()
    if p in BROKER_PROFILES:
        return p
    raise ValueError("invalid broker_profile: %s, valid: %s" % (profile, sorted(BROKER_PROFILES)))


def is_api_supported_for_broker(api_name: str, profile: str) -> bool:
    """Check API support under broker strict mode."""
    p = normalize_broker_profile(profile)
    if p == "auto":
        return True
    return api_name not in BROKER_STRICT_UNSUPPORTED_APIS.get(p, frozenset())


def needs_broker_support_guard(api_name: str) -> bool:
    """Whether this API has broker-specific availability differences."""
    for unsupported in BROKER_STRICT_UNSUPPORTED_APIS.values():
        if api_name in unsupported:
            return True
    return False
