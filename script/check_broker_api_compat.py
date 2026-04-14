# -*- coding: utf-8 -*-

"""三券商 API 快照与本地实现一致性检查。

用途：
1. 基于网页快照提取的基线（JSON），检查关键 API 在各券商口径下的“是否存在”与本地 strict guard 是否一致。
2. 检查 strict 清单中的 API 是否都在 PtradeAPI 中有实现入口（避免悬空配置）。

默认读取：
    docs/api_snapshots/core_api_presence_2026-04-13.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simtradelab.ptrade.broker_profile import BROKER_STRICT_UNSUPPORTED_APIS
from simtradelab.ptrade.lifecycle_config import API_ALLOWED_PHASES_LOOKUP
from simtradelab.ptrade.api import PtradeAPI


def load_baseline(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def collect_api_methods() -> set[str]:
    return {name for name in dir(PtradeAPI) if not name.startswith("_")}


def run_check(baseline: dict) -> tuple[bool, list[str]]:
    errors = []
    methods = collect_api_methods()

    # 1) strict 清单中的 API 必须存在实现入口
    for broker, apis in BROKER_STRICT_UNSUPPORTED_APIS.items():
        for api in apis:
            if api not in methods:
                errors.append("strict 清单包含未实现 API: broker=%s api=%s" % (broker, api))

    # 2) 对照网页基线：存在/不存在 与 strict 支持性要一致
    brokers = baseline.get("brokers", {})
    for broker, cfg in brokers.items():
        if broker not in BROKER_STRICT_UNSUPPORTED_APIS:
            errors.append("baseline 含未知 broker: %s" % broker)
            continue
        strict_set = BROKER_STRICT_UNSUPPORTED_APIS[broker]
        api_presence = cfg.get("api_presence", {})
        for api_name, present in api_presence.items():
            if api_name not in methods:
                errors.append("API 未实现入口: broker=%s api=%s" % (broker, api_name))
                continue
            if present and api_name in strict_set:
                errors.append("网页标记存在但 strict 禁止: broker=%s api=%s" % (broker, api_name))
            if (not present) and api_name not in strict_set:
                errors.append("网页标记不存在但 strict 未禁止: broker=%s api=%s" % (broker, api_name))

    # 3) 关键别名需有生命周期配置（防止调用阶段校验漏项）
    for api_name in ("get_margin_assert", "get_margin_asset", "get_individual_transcation", "get_individual_transaction"):
        if api_name not in API_ALLOWED_PHASES_LOOKUP:
            errors.append("生命周期配置缺失: %s" % api_name)

    return (len(errors) == 0, errors)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Check broker API compatibility against snapshot baseline.")
    parser.add_argument(
        "--baseline",
        default="docs/api_snapshots/core_api_presence_2026-04-13.json",
        help="Path to baseline json.",
    )
    args = parser.parse_args(argv)

    baseline_path = Path(args.baseline)
    if not baseline_path.exists():
        print("baseline not found: %s" % baseline_path)
        return 2

    baseline = load_baseline(baseline_path)
    ok, errors = run_check(baseline)
    if ok:
        print("broker api compat check passed")
        return 0

    print("broker api compat check failed:")
    for e in errors:
        print("- %s" % e)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
