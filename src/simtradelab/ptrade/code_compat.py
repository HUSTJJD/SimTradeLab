# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
#
# This file is part of SimTradeLab, dual-licensed under AGPL-3.0 and a
# commercial license. See LICENSE-COMMERCIAL.md or contact kayou@duck.com
"""PTrade 证券代码尾缀兼容层。

官方依据（PTrade/api_doc.txt）：

* 「策略中支持的代码尾缀」(L243-248)：上海 XSHG/SS、深圳 XSHE/SZ、
  指数 XBHS、中金所期货 CCFX —— **全称尾缀与简称尾缀都是官方合法输入**。
* L2516: ``index_code：指数代码，如沪深300：000300.SS或000300.XBHS``。
* L3084: 「四位尾缀或者两位尾缀代码皆可作为键取到返回的数据字典值」。
* L4241 / L6342: 「股票标的代码尾缀为四位，上证为XSHG，深圳为XSHE，
  **如需对应到代码请做代码尾缀兼容**」—— 返回对象的 symbol 用四位尾缀。
* L5383 等官方示例直接用 ``g.security = "600570.XSHG"`` 调 API。

本地 parquet / 所有内部字典使用简称尾缀 ``.SS`` / ``.SZ`` 作为键。因此策略
无论写哪种官方尾缀，都必须在进入引擎前解析成内部真实存在的键；返回给策略
的字典则要允许两种尾缀作为键取值。本模块在 PtradeAPI 类级别一次性完成，
避免几十处字典查找各自打补丁（也避免静默取空）。
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from functools import lru_cache, wraps
from typing import Any

# 官方全称尾缀 -> 引擎内部简称尾缀
_LONG_SUFFIX_TO_SHORT = {
    ".XSHG": ".SS",
    ".XSHE": ".SZ",
}

# 同一市场的全部等价写法（查询候选用）
_MARKET_SUFFIXES = {
    "SH": (".SS", ".XSHG", ".SH"),
    "SZ": (".SZ", ".XSHE"),
}

# 官方其它尾缀：引擎没有对应行情数据，保持原样透传，不做猜测式映射
_OTHER_OFFICIAL_SUFFIXES = frozenset(
    {".CCFX", ".XSHO", ".XSZO", ".XHKG-SS", ".XHKG-SZ", ".XBSE", ".BJ"}
)

_CODE_WITH_SUFFIX_RE = re.compile(r"^(?P<base>[A-Za-z0-9]+)(?P<suffix>\.[A-Za-z-]+)$")
_PLAIN_CODE_RE = re.compile(r"^\d{6}$")

# 公开 API 中承载证券代码的参数名。用白名单而非"所有字符串参数"，避免误伤
# fields / frequency / date / table 等形似代码的字符串。
_CODE_PARAM_NAMES = frozenset(
    {
        "security",
        "securities",
        "security_list",
        "stock",
        "stocks",
        "stock_code",
        "stock_list",
        "symbol",
        "symbols",
        "sid",
        "sids",
        "code",
        "codes",
        "index_code",
        "benchmark",
        "contract",
        "etf_code",
        "underlying",
        "order_book_id",
        "black_stocks",
        "poslist",
        "transaction_code",
    }
)


def _is_index_code(base: str) -> bool:
    """区分 .XBHS 里的「指数代码」与「行业/地域/概念板块代码」。

    api_doc L2489-2492 的板块代码既有字母开头（A01000、DY1172），也有纯数字
    （710200 计算机应用、003596 融资融券、031027 MSCI概念）；真正的指数是
    中证 000xxx 与深证/国证 399xxx。板块代码没有行情标的，绝不能映射到交易所。
    """
    return len(base) == 6 and base.isdigit() and (base.startswith("000") or base.startswith("399"))


def _infer_short_suffix(base: str) -> str:
    """按 A 股代码前缀推断交易所（仅用于不带尾缀的代码）。"""
    if base[:1] in ("6", "9"):
        return ".SS"
    if base[:1] in ("0", "1", "2", "3"):
        return ".SZ"
    return ".SS"


def code_candidates(code: str) -> tuple[str, ...]:
    """返回一个证券代码的所有等价写法，按"最可能是内部键"的顺序排列。

    第一个元素恒为原始写法；调用方应先按此列表在内部字典里查找，找不到时
    再退回 :func:`canonical_code` 的规范写法。
    """
    if not isinstance(code, str) or not code:
        return (code,)
    return _code_candidates_cached(code)


@lru_cache(maxsize=8192)
def _code_candidates_cached(code: str) -> tuple[str, ...]:
    """纯函数结果缓存：回测里同一代码会重复出现几十万次。"""
    match = _CODE_WITH_SUFFIX_RE.match(code)
    if match:
        base = match.group("base")
        suffix = match.group("suffix").upper()

        if suffix in (".XSHG", ".SS", ".SH"):
            variants = _MARKET_SUFFIXES["SH"]
        elif suffix in (".XSHE", ".SZ"):
            variants = _MARKET_SUFFIXES["SZ"]
        elif suffix == ".XBHS":
            # XBHS 是「指数/板块」尾缀，不是交易所尾缀：
            #   - 字母开头（A01000 / DY1172 / 710200）是行业、地域、概念板块代码，
            #     不是行情标的，必须原样保留，绝不能映射到 .SZ（旧实现的 bug）。
            #   - 399xxx 国证/深证指数归深市；000xxx 中证指数在上证行情体系。
            #     两者都给候选，由 resolver 按数据里真实存在的键决定。
            if not _is_index_code(base):
                return (code,)
            variants = (".SZ", ".SS") if base.startswith("399") else (".SS", ".SZ")
        elif suffix in _OTHER_OFFICIAL_SUFFIXES:
            return (code,)
        else:
            return (code,)

        out = [code]
        for item in variants:
            candidate = base + item
            if candidate not in out:
                out.append(candidate)
        return tuple(out)

    if _PLAIN_CODE_RE.match(code):
        preferred = _infer_short_suffix(code)
        other = ".SZ" if preferred == ".SS" else ".SS"
        return (code, code + preferred, code + other)

    return (code,)


def canonical_code(code: str) -> str:
    """把任意官方写法规范化为引擎内部写法（无数据字典可用时的兜底）。

    注意 ``.SH`` 不是 PTrade 官方尾缀，历史契约要求保持原样，这里也保持；
    但它仍出现在 :func:`code_candidates` 里，供真实数据查询时命中 ``.SS``。
    """
    match = _CODE_WITH_SUFFIX_RE.match(code) if isinstance(code, str) else None
    if not match:
        candidates = code_candidates(code)
        return candidates[1] if len(candidates) > 1 else code

    base, suffix = match.group("base"), match.group("suffix").upper()
    if suffix in (".SS", ".SZ", ".SH"):
        return code  # 已是内部写法，或非官方但需保持的 .SH
    if suffix == ".XSHG":
        return base + ".SS"
    if suffix == ".XSHE":
        return base + ".SZ"
    if suffix == ".XBHS":
        if not _is_index_code(base):
            return code  # 行业/地域/概念板块代码，不是行情标的
        return base + (".SZ" if base.startswith("399") else ".SS")
    return code  # CCFX / XSHO / XSZO / XHKG-* / BJ：引擎无对应数据，原样透传


class DualKeyDict(dict):
    """允许用全称或简称尾缀作为键取值的 dict。

    对应官方 L3084：「四位尾缀或者两位尾缀代码皆可作为键取到返回的数据字典值」。
    枚举（keys/items）仍只返回内部规范键，保持与既有策略/报告一致。
    """

    def _resolve_key(self, key: Any) -> Any:
        if dict.__contains__(self, key):
            return key
        if isinstance(key, str):
            for candidate in code_candidates(key):
                if dict.__contains__(self, candidate):
                    return candidate
        return None

    def __contains__(self, key: Any) -> bool:
        return self._resolve_key(key) is not None

    def __getitem__(self, key: Any) -> Any:
        resolved = self._resolve_key(key)
        if resolved is None:
            raise KeyError(key)
        return dict.__getitem__(self, resolved)

    def get(self, key: Any, default: Any = None) -> Any:
        resolved = self._resolve_key(key)
        return dict.__getitem__(self, resolved) if resolved is not None else default

    def __delitem__(self, key: Any) -> None:
        resolved = self._resolve_key(key)
        if resolved is None:
            raise KeyError(key)
        dict.__delitem__(self, resolved)

    def pop(self, key: Any, *args: Any) -> Any:
        resolved = self._resolve_key(key)
        if resolved is None:
            if args:
                return args[0]
            raise KeyError(key)
        return dict.pop(self, resolved)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        resolved = self._resolve_key(key)
        if resolved is not None:
            return dict.__getitem__(self, resolved)
        dict.__setitem__(self, key, default)
        return default


def _looks_like_security_code(value: Any) -> bool:
    return isinstance(value, str) and bool(_CODE_WITH_SUFFIX_RE.match(value))


def wrap_return(result: Any) -> Any:
    """对"以证券代码为键"的纯 dict 结果启用双键取值。

    只处理 ``type(result) is dict``：get_history 的 PanelLike / DataFrame 子类
    有自定义结构和约定，不能替换类型，否则会破坏返回形状契约。
    """
    if type(result) is dict and result and any(_looks_like_security_code(k) for k in result):
        return DualKeyDict(result)
    return result


def make_resolver(api: Any) -> Callable[[str], bool]:
    """构造 key -> bool 判定器：该代码是否是引擎内部真实存在的键。

    优先"按数据里实际存在的键解析"而不是硬编码映射，因为本地指数数据本身
    就不统一（例如 000300.SS、000905.SZ、399370.SZ 并存），静态映射必然猜错。
    """

    index_keys: list[set[str] | None] = [None]
    # 回测中同一代码会被反复解析；缓存判定结果，避免每根 bar 重扫多个数据字典。
    # False 结果过期也无害：找不到时会退回 canonical_code，结果仍正确。
    seen: dict[str, bool] = {}

    def _exact_contains(mapping: Any, key: str) -> bool:
        """必须用**精确键**判定，不能走 DualKeyDict 的兼容 __contains__。

        否则一旦持仓里存在 600000.SS，DualKeyDict 会让 '600000.XSHG' in positions
        返回 True，resolver 就会误判长后缀已是内部键而停止归一化。
        """
        if isinstance(mapping, dict):
            return dict.__contains__(mapping, key)
        return key in mapping

    def _resolve_uncached(key: str) -> bool:
        try:
            data_context = getattr(api, "data_context", None)
            if data_context is not None:
                for name in (
                    "stock_data_dict",
                    "stock_data_dict_1m",
                    "benchmark_data",
                    "valuation_dict",
                    "fundamentals_dict",
                    "exrights_dict",
                ):
                    mapping = getattr(data_context, name, None)
                    if mapping is None:
                        continue
                    try:
                        if _exact_contains(mapping, key):
                            return True
                    except (TypeError, KeyError):
                        pass

                metadata = getattr(data_context, "stock_metadata", None)
                if metadata is not None:
                    try:
                        if key in metadata.index:
                            return True
                    except (TypeError, AttributeError):
                        pass

                constituents = getattr(data_context, "index_constituents", None)
                if constituents:
                    if not index_keys[0]:
                        keys: set[str] = set()
                        for snapshot in constituents.values():
                            if isinstance(snapshot, dict):
                                keys.update(snapshot.keys())
                        index_keys[0] = keys
                    if key in index_keys[0]:
                        return True

            context = getattr(api, "context", None)
            portfolio = getattr(context, "portfolio", None) if context is not None else None
            positions = getattr(portfolio, "positions", None) if portfolio is not None else None
            if positions is not None and _exact_contains(positions, key):
                return True
        except Exception:
            return False
        return False

    def resolve(key: str) -> bool:
        cached = seen.get(key)
        if cached is not None:
            return cached
        result = _resolve_uncached(key)
        seen[key] = result
        return result

    return resolve


def normalize_code_value(value: Any, resolve: Callable[[str], bool] | None = None) -> Any:
    """递归归一化代码值；无变化时返回**原对象**，避免无谓拷贝。"""
    if isinstance(value, str):
        candidates = code_candidates(value)
        if len(candidates) == 1:
            return value
        if resolve is not None:
            for candidate in candidates:
                if resolve(candidate):
                    return value if candidate == value else candidate
        canonical = canonical_code(value)
        return value if canonical == value else canonical

    if isinstance(value, (list, tuple, set, frozenset)):
        changed = False
        items = []
        for item in value:
            new_item = normalize_code_value(item, resolve)
            if new_item is not item:
                changed = True
            items.append(new_item)
        if not changed:
            return value
        if isinstance(value, list):
            return items
        if isinstance(value, tuple):
            return tuple(items)
        return type(value)(items)

    if isinstance(value, dict):
        changed = False
        result = {}
        for key, item in value.items():
            new_key = normalize_code_value(key, resolve)
            new_item = normalize_code_value(item, resolve)
            if new_key is not key or new_item is not item:
                changed = True
            result[new_key] = new_item
        return result if changed else value

    return value


def _make_wrapper(
    func: Callable,
    positions: tuple[int, ...],
    keyword_names: frozenset[str],
) -> Callable:
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        resolver = self.__dict__.get("_code_resolver")
        if resolver is None:
            resolver = make_resolver(self)
            self.__dict__["_code_resolver"] = resolver

        new_args = None
        for index in positions:
            if index < len(args):
                value = args[index]
                normalized = normalize_code_value(value, resolver)
                if normalized is not value:
                    if new_args is None:
                        new_args = list(args)
                    new_args[index] = normalized

        new_kwargs = None
        for name in keyword_names:
            if name in kwargs:
                value = kwargs[name]
                normalized = normalize_code_value(value, resolver)
                if normalized is not value:
                    if new_kwargs is None:
                        new_kwargs = dict(kwargs)
                    new_kwargs[name] = normalized

        if new_args is None and new_kwargs is None:
            result = func(self, *args, **kwargs)
        else:
            result = func(self, *(new_args if new_args is not None else args), **(new_kwargs if new_kwargs is not None else kwargs))
        return wrap_return(result)

    wrapper._code_compat_wrapped = True  # 便于诊断/测试确认包装生效
    return wrapper


def install_code_compat(cls: type, alias_map: dict[str, dict[str, str]] | None = None) -> type:
    """给 PtradeAPI 的所有公开方法装上证券代码归一化包装。

    放在类级别是因为引擎没有统一分发点：``strategy_engine`` 只是把公开方法
    原样注入策略命名空间，``validate_lifecycle`` 又对多数行情 API 零包装。
    这样无论从策略、runner 还是测试直接调用，行为都一致。
    """
    alias_map = alias_map or {}
    for name, raw in list(cls.__dict__.items()):
        if name.startswith("_"):
            continue
        # 公开静态方法在本类里不存在；跳过可避免 __getattribute__ 之外的 self 语义问题
        if isinstance(raw, (staticmethod, classmethod, property)) or not callable(raw):
            continue
        func = raw

        try:
            parameters = list(inspect.signature(func).parameters)
        except (TypeError, ValueError):
            continue
        if parameters:
            parameters = parameters[1:]  # 去掉 self，位置索引与 *args 对齐

        positions = tuple(i for i, param in enumerate(parameters) if param in _CODE_PARAM_NAMES)
        keyword_names = {param for param in parameters if param in _CODE_PARAM_NAMES}
        # validate_lifecycle 在更内层做参数别名改写，外层包装必须一并识别别名，
        # 否则 set_universe(stocks=[...]) / set_benchmark(benchmark=...) 会漏掉归一化。
        for alias, canonical_name in alias_map.get(name, {}).items():
            if canonical_name in _CODE_PARAM_NAMES:
                keyword_names.add(alias)
            if alias in _CODE_PARAM_NAMES:
                keyword_names.add(canonical_name)

        if not positions and not keyword_names:
            continue

        setattr(cls, name, _make_wrapper(func, positions, frozenset(keyword_names)))
    return cls
