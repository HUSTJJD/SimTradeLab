# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
测试生命周期控制器
"""

from simtradelab.ptrade.lifecycle_controller import (
    LifecyclePhase,
    PTradeLifecycleError,
)
from simtradelab.ptrade.lifecycle_config import API_ALLOWED_PHASES_LOOKUP
import pytest


class TestLifecycleController:
    """测试生命周期控制器"""

    def test_init(self, lifecycle_controller):
        """测试初始化"""
        assert lifecycle_controller._current_phase is None
        assert lifecycle_controller._strategy_mode == "backtest"

    def test_set_phase(self, lifecycle_controller):
        """测试设置阶段"""
        lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        assert lifecycle_controller.current_phase == LifecyclePhase.INITIALIZE
        assert lifecycle_controller.current_phase_name == "initialize"

    def test_phase_transition(self, lifecycle_controller):
        """测试阶段转换"""
        lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        lifecycle_controller.set_phase(LifecyclePhase.BEFORE_TRADING_START)
        lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
        assert lifecycle_controller.current_phase == LifecyclePhase.HANDLE_DATA

    def test_invalid_phase_transition(self, lifecycle_controller):
        """测试非法阶段转换"""
        lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        with pytest.raises(PTradeLifecycleError):
            lifecycle_controller.set_phase(LifecyclePhase.AFTER_TRADING_END)

    def test_is_phase_executed(self, lifecycle_controller):
        """测试阶段执行记录"""
        lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        assert lifecycle_controller.is_phase_executed(LifecyclePhase.INITIALIZE)
        assert not lifecycle_controller.is_phase_executed(LifecyclePhase.HANDLE_DATA)

    def test_reset(self, lifecycle_controller):
        """测试重置"""
        lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
        lifecycle_controller.reset()
        assert lifecycle_controller.current_phase is None
        assert not lifecycle_controller.is_phase_executed(LifecyclePhase.INITIALIZE)


class TestLifecycleConfig:
    """测试生命周期配置（通过预计算 lookup 表验证）"""

    def test_initialize_only_api(self):
        """set_benchmark 仅限 initialize"""
        allowed = API_ALLOWED_PHASES_LOOKUP["set_benchmark"]
        assert "initialize" in allowed
        assert "handle_data" not in allowed

    def test_handle_data_api(self):
        """order 可在 handle_data/tick_data"""
        allowed = API_ALLOWED_PHASES_LOOKUP["order"]
        assert "handle_data" in allowed
        assert "initialize" not in allowed

    def test_all_phases_api(self):
        """get_price 在所有阶段都可用"""
        from simtradelab.ptrade.lifecycle_config import _ALL_PHASES_FROZENSET
        assert API_ALLOWED_PHASES_LOOKUP["get_price"] is _ALL_PHASES_FROZENSET

    def test_before_trading_start_api(self):
        """set_universe 可在 initialize 和 before_trading_start"""
        allowed = API_ALLOWED_PHASES_LOOKUP["set_universe"]
        assert "initialize" in allowed
        assert "before_trading_start" in allowed
        assert "handle_data" not in allowed
