# -*- coding: utf-8 -*-

from pathlib import Path

from script.check_broker_api_compat import load_baseline, run_check


def test_broker_api_snapshot_guard():
    baseline_path = Path("docs/api_snapshots/core_api_presence_2026-04-13.json")
    baseline = load_baseline(baseline_path)
    ok, errors = run_check(baseline)
    assert ok, "snapshot guard failed: %s" % errors

