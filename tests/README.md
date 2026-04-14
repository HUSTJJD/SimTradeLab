# SimTradeLab 测试指南

## 🎯 当前状态

✅ **247个测试全部通过**（精简后，质量优化）
📊 **代码覆盖率 38% (核心API 71%, context 71%, order_processor 64%)**
🛡️ **严格遵循PTrade API文档**
⭐ **仅测试文档公开API**
🎯 **聚焦核心兼容性保护**

---

## 📚 目录

- [快速开始](#快速开始)
- [测试文件结构](#测试文件结构)
- [测试范围](#测试范围)
- [代码覆盖率](#代码覆盖率)
- [测试设计原则](#测试设计原则)
- [关键保护](#关键保护)
- [添加新测试](#添加新测试)
- [持续集成](#持续集成)
- [下一步改进](#下一步改进)

---

## 🚀 快速开始

### 安装依赖

```bash
poetry install --with dev
```

### 运行测试

```bash
# 简洁输出
poetry run pytest tests/unit -q

# 详细输出
poetry run pytest tests/unit -v

# 带覆盖率
poetry run pytest tests/unit --cov=simtradelab --cov-report=term-missing

# HTML覆盖率报告
poetry run pytest tests/unit --cov=simtradelab --cov-report=html
# Linux: xdg-open htmlcov/index.html
# macOS: open htmlcov/index.html
# Windows: start htmlcov/index.html
```

### 运行特定测试

```bash
# 单个文件
poetry run pytest tests/unit/test_api.py -v

# 单个测试类
poetry run pytest tests/unit/test_api.py::TestOrderAPI -v

# 单个测试用例
poetry run pytest tests/unit/test_api.py::TestOrderAPI::test_order_in_handle_data -v
```

### 调试测试

```bash
# 显示print输出
poetry run pytest tests/unit -v -s

# 详细错误信息
poetry run pytest tests/unit -vv

# 只运行失败的测试
poetry run pytest tests/unit --lf

# 第一个失败时停止
poetry run pytest tests/unit -x
```

---

## 📦 测试文件结构

```
tests/
├── conftest.py              # 测试fixtures和配置
├── pytest.ini              # pytest配置
├── README.md               # 本文档
├── API_TEST_COVERAGE.md    # API测试覆盖分析
└── unit/                   # 单元测试 (247个)
    ├── test_api.py         # 核心API (9个)
    ├── test_api_boundaries.py # 边界测试 (19个)
    ├── test_api_complete.py # 完整API (59个)
    ├── test_api_extended.py # 扩展API (17个)
    ├── test_api_advanced.py # 高级场景 (17个)
    ├── test_api_formats.py # API格式测试 (18个) ← 优化
    ├── test_object.py      # Portfolio/Position (23个)
    ├── test_object_advanced.py # 对象高级功能 (9个)
    ├── test_order_system.py # 订单系统 (14个)
    ├── test_order_processor.py # 订单处理器 (12个)
    ├── test_order_processor_advanced.py # 订单处理器高级 (7个)
    ├── test_cache.py       # 缓存管理 (12个)
    ├── test_lifecycle.py   # 生命周期控制 (11个)
    ├── test_context_lifecycle.py # Context生命周期 (13个)
    └── test_data_context.py # 数据上下文 (1个) ← 精简
```

---

## 测试范围

**基于PTrade API官方文档，仅测试文档公开的API**

### API测试 (140个)
- test_api.py (9) - 核心API
- test_api_boundaries.py (19) - 边界测试
- test_api_complete.py (59) - 完整覆盖
- test_api_extended.py (17) - 扩展功能
- test_api_advanced.py (17) - 高级场景
- test_api_formats.py (18) - 多格式返回测试

### 对象和订单测试 (65个)
- test_object.py (23) - Portfolio/Position
- test_object_advanced.py (9) - 对象高级功能
- test_order_system.py (14) - 订单系统
- test_order_processor.py (12) - 订单处理器
- test_order_processor_advanced.py (7) - 订单处理器高级场景

### 基础设施测试 (42个)
- test_cache.py (12) - 缓存管理
- test_lifecycle.py (11) - 生命周期控制
- test_context_lifecycle.py (13) - Context生命周期管理
- test_data_context.py (1) - 数据上下文（精简）
- test_config.py (5) - 配置管理

### 已删除的非文档API测试
❌ `get_adjusted_price` - 内部辅助方法，非公开API
❌ `get_stock_blocks` 冗余测试（保留1个基础测试）
❌ `get_stock_exrights` 冗余测试（非必测API）
❌ `get_industry_stocks` 冗余测试（非必测API）
❌ `get_Ashares` 冗余测试（非必测API）

---

## 代码覆盖率

| 模块 | 覆盖率 | 说明 |
|------|--------|------|
| data_context.py | 100% | 数据上下文 |
| cache_manager.py | 97% | 缓存管理 |
| lifecycle_config.py | 83% | 生命周期配置 |
| config_manager.py | 77% | 配置管理 |
| **api.py** | **71%** | **核心API（+5%）** |
| context.py | 71% | 上下文 |
| lifecycle_controller.py | 71% | 生命周期控制 |
| order_processor.py | 64% | 订单处理（+20%） |
| object.py | 53% | 核心对象 |

---

## 测试设计原则

1. **真实对象** - 使用真实Portfolio/Context/API，零Mock
2. **生命周期严格** - 遵循ptrade生命周期，非法调用必须抛异常
3. **自动隔离** - autouse fixture自动重置缓存，独立测试数据
4. **简化数据** - 3个测试股票，20天数据，不依赖完整HDF5

---

## 关键保护

**交易API**: order, order_target, order_value, cancel_order, get_position
**数据API**: get_price, get_history, get_stock_info, check_limit, get_trade_days
**配置API**: set_benchmark, set_universe, set_commission, set_slippage

---

## 添加新测试

```python
def test_my_api(ptrade_api):
    # 设置生命周期
    ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.INITIALIZE)
    ptrade_api.context._lifecycle_controller.set_phase(LifecyclePhase.HANDLE_DATA)
    ptrade_api.context.current_dt = datetime(2024, 1, 2)

    # 调用测试
    result = ptrade_api.my_api()
    assert result is not None
```

---

## 持续集成

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Install dependencies
        run: poetry install --with dev
      - name: Run tests
        run: poetry run pytest tests/unit --cov=simtradelab --cov-fail-under=25
```

---

## 下一步改进

**提升覆盖率**
- backtest/runner.py - 回测流程测试
- backtest/stats.py - 统计指标测试
- strategy_engine.py - 策略执行测试

**集成测试** - tests/integration/
- 完整回测流程
- 多策略并发
- 大数据集性能

---

## 总结

✅ 247个测试全部通过（精简2个冗余测试）
✅ 核心模块覆盖率38%
✅ 严格遵循PTrade API官方文档
✅ 仅测试文档公开API，保证兼容性

**测试质量优化**:
- 精简test_data_context.py（3→1个测试）
- 删除冗余的属性赋值验证
- 加强test_api_formats.py断言
- 移除过于宽松的`or result is None`断言
- 添加数据内容和结构验证

**总体优化成果（两轮合计）**:
- Context生命周期测试（13个）
- DataContext数据上下文测试（1个，精简优化）
- OrderProcessor边界场景测试（7个）
- API格式返回测试（18个，加强断言）
- context.py: 55% → 71% (+16%)
- data_context.py: 0% → 100% (+100%)
- order_processor.py: 57% → 64% (+7%)
- **api.py: 66% → 71% (+5%)**
- 总覆盖率: 36% → 38%
- **测试质量**: 优化断言强度，删除冗余测试

**每次修改代码前后都要运行测试！**

```bash
poetry run pytest tests/unit -v
```
