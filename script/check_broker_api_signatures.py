# -*- coding: utf-8 -*-

"""关键 API 参数签名与默认值校验。"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_matrix(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _default_to_jsonable(value):
    if value is inspect._empty:
        return "__EMPTY__"
    return value


class _AstParam:
    def __init__(self, name: str, default, has_default: bool, kind: str):
        self.name = name
        self.default = default
        self.has_default = has_default
        self.kind = kind  # pos, var_kw


def _literal(node):
    if node is None:
        return None
    try:
        return ast.literal_eval(node)
    except Exception:
        return "__NON_LITERAL__"


def _load_api_signature_map() -> dict[str, list[_AstParam]]:
    api_file = ROOT / "src" / "simtradelab" / "ptrade" / "api.py"
    mod = ast.parse(api_file.read_text(encoding="utf-8"))
    out = {}
    for n in mod.body:
        if isinstance(n, ast.ClassDef) and n.name == "PtradeAPI":
            for fn in n.body:
                if not isinstance(fn, ast.FunctionDef):
                    continue
                if fn.name.startswith("_"):
                    continue
                args = fn.args
                defaults = list(args.defaults)
                pos_args = list(args.args)
                # skip self
                if pos_args and pos_args[0].arg == "self":
                    pos_args = pos_args[1:]
                default_pad = len(pos_args) - len(defaults)
                params = []
                for i, a in enumerate(pos_args):
                    if i < default_pad:
                        params.append(_AstParam(a.arg, "__EMPTY__", False, "pos"))
                    else:
                        dv = defaults[i - default_pad]
                        params.append(_AstParam(a.arg, _literal(dv), True, "pos"))
                if args.kwarg is not None:
                    params.append(_AstParam(args.kwarg.arg, "__VAR_KEYWORD__", False, "var_kw"))
                out[fn.name] = params
    return out


def run_check(matrix: dict) -> tuple[bool, list[str]]:
    errors = []
    apis = matrix.get("apis", {})
    api_sig_map = _load_api_signature_map()

    for api_name, cfg in apis.items():
        if api_name not in api_sig_map:
            errors.append("PtradeAPI 缺少接口: %s" % api_name)
            continue

        params = api_sig_map[api_name]
        param_names = [p.name for p in params if p.kind == "pos"]

        required = cfg.get("required_params", [])
        optional = cfg.get("optional_params", [])
        expected_all = list(required) + list(optional)

        for name in required:
            if name not in param_names:
                errors.append("%s 缺少必选参数: %s" % (api_name, name))
                continue
            p = next(x for x in params if x.name == name and x.kind == "pos")
            if p.has_default:
                errors.append("%s 必选参数不应有默认值: %s" % (api_name, name))

        for name in optional:
            if name not in param_names:
                errors.append("%s 缺少可选参数: %s" % (api_name, name))
                continue
            p = next(x for x in params if x.name == name and x.kind == "pos")
            if not p.has_default:
                errors.append("%s 可选参数缺少默认值: %s" % (api_name, name))

        unknown = [n for n in param_names if n not in expected_all]
        if unknown:
            errors.append("%s 存在基线外参数: %s" % (api_name, unknown))

        defaults = cfg.get("default_values", {})
        for name, expected in defaults.items():
            if name not in param_names:
                errors.append("%s 默认值校验参数缺失: %s" % (api_name, name))
                continue
            p = next(x for x in params if x.name == name and x.kind == "pos")
            got = _default_to_jsonable(p.default)
            if got != expected:
                errors.append("%s 默认值不一致: %s expected=%r got=%r" % (api_name, name, expected, got))

        if cfg.get("must_have_var_keyword"):
            if not any(p.kind == "var_kw" for p in params):
                errors.append("%s 缺少 **kwargs 兼容入口" % api_name)

    return (len(errors) == 0, errors)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Check core API signatures against baseline matrix.")
    parser.add_argument(
        "--matrix",
        default="docs/api_snapshots/core_api_signature_matrix_2026-04-13.json",
        help="Path to signature matrix json.",
    )
    args = parser.parse_args(argv)

    matrix_path = Path(args.matrix)
    if not matrix_path.exists():
        print("matrix not found: %s" % matrix_path)
        return 2

    matrix = _load_matrix(matrix_path)
    ok, errors = run_check(matrix)
    if ok:
        print("broker api signature check passed")
        return 0

    print("broker api signature check failed:")
    for e in errors:
        print("- %s" % e)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
