# -*- coding: utf-8 -*-

from pathlib import Path

from script.check_broker_api_signatures import _load_matrix, run_check


def test_broker_api_signature_guard():
    matrix_path = Path("docs/api_snapshots/core_api_signature_matrix_2026-04-13.json")
    matrix = _load_matrix(matrix_path)
    ok, errors = run_check(matrix)
    assert ok, "signature guard failed: %s" % errors

