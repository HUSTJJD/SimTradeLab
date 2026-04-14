# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kay
"""
测试缓存管理器
"""

import pytest
from datetime import datetime

from simtradelab.ptrade.cache_manager import (
    cache_manager,
    CacheNamespace,
    UnifiedCacheManager
)


class TestCacheNamespace:
    """测试缓存命名空间"""

    def test_init(self):
        """测试初始化"""
        ns = CacheNamespace('test', max_size=100)
        assert ns.name == 'test'
        assert ns.maxsize() == 100
        assert ns.size() == 0

    def test_put_and_get(self):
        """测试存取"""
        ns = CacheNamespace('test', max_size=100)

        # 存入数据
        ns.put('key1', 'value1')
        assert ns.size() == 1

        # 获取数据
        value = ns.get('key1')
        assert value == 'value1'

        # 获取不存在的key
        value = ns.get('not_exist')
        assert value is None

    def test_lru_eviction(self):
        """测试LRU淘汰"""
        ns = CacheNamespace('test', max_size=3)

        # 填满缓存
        ns.put('key1', 'value1')
        ns.put('key2', 'value2')
        ns.put('key3', 'value3')
        assert ns.size() == 3

        # 再添加一个，应该淘汰最旧的
        ns.put('key4', 'value4')
        assert ns.size() == 3

        # key1应该被淘汰
        assert ns.get('key1') is None
        assert ns.get('key4') == 'value4'

    def test_stats(self):
        """测试统计信息"""
        ns = CacheNamespace('test', max_size=100)

        ns.put('key1', 'value1')
        ns.get('key1')  # hit
        ns.get('not_exist')  # miss

        stats = ns.get_stats()
        assert stats['hits'] == 1
        assert stats['misses'] == 1
        assert stats['puts'] == 1
        assert stats['hit_rate'] == 0.5

    def test_clear(self):
        """测试清空"""
        ns = CacheNamespace('test', max_size=100)

        ns.put('key1', 'value1')
        ns.put('key2', 'value2')
        assert ns.size() == 2

        ns.clear()
        assert ns.size() == 0


class TestUnifiedCacheManager:
    """测试统一缓存管理器"""

    def test_singleton(self):
        """测试单例模式"""
        manager1 = UnifiedCacheManager()
        manager2 = UnifiedCacheManager()
        assert manager1 is manager2

    def test_get_namespace(self, reset_cache):
        """测试获取命名空间"""
        ns = cache_manager.get_namespace('history')
        assert isinstance(ns, CacheNamespace)
        assert ns.name == '历史数据'

    def test_get_unknown_namespace(self, reset_cache):
        """测试获取不存在的命名空间"""
        with pytest.raises(ValueError, match="未知的缓存命名空间"):
            cache_manager.get_namespace('unknown')

    def test_put_and_get(self, reset_cache):
        """测试通过管理器存取"""
        cache_manager.put('history', 'test_key', 'test_value')
        value = cache_manager.get('history', 'test_key')
        assert value == 'test_value'

    def test_clear_namespace(self, reset_cache):
        """测试清空命名空间"""
        cache_manager.put('history', 'key1', 'value1')
        cache_manager.put('ma_cache', 'key2', 'value2')

        cache_manager.clear_namespace('history')

        assert cache_manager.get('history', 'key1') is None
        assert cache_manager.get('ma_cache', 'key2') == 'value2'

    def test_clear_all(self, reset_cache):
        """测试清空所有缓存"""
        cache_manager.put('history', 'key1', 'value1')
        cache_manager.put('ma_cache', 'key2', 'value2')

        cache_manager.clear_all()

        assert cache_manager.get('history', 'key1') is None
        assert cache_manager.get('ma_cache', 'key2') is None

    def test_clear_daily_cache(self, reset_cache):
        """测试清理日级缓存"""
        # 添加数据到日级缓存
        cache_manager.put('ma_cache', 'key1', 'value1')
        cache_manager.put('vwap_cache', 'key2', 'value2')
        cache_manager.put('history', 'key3', 'value3')

        # 清理日级缓存
        cache_manager.clear_daily_cache(datetime(2024, 1, 2))

        # 日级缓存应该被清空
        assert cache_manager.get('ma_cache', 'key1') is None
        assert cache_manager.get('vwap_cache', 'key2') is None

        # 非日级缓存应该保留
        assert cache_manager.get('history', 'key3') == 'value3'
