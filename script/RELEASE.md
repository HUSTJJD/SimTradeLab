# SimTradeLab 发布指南

## 发布模型（当前仓库真实流程）

当前发布由两部分组成：

1. 本地执行 `script/release.py`：
- 更新 `pyproject.toml` 版本号
- 更新 `README.md` / `README.zh-CN.md` / `README.de.md` 中版本相关文本
- 提交版本 bump commit（`chore: bump version to X.Y.Z`）

2. 推送 `main` 后，GitHub Actions（`.github/workflows/publish.yml`）自动：
- 检查 `pyproject.toml` 版本是否已有 tag
- 自动创建并推送 tag（`vX.Y.Z`）
- 创建 GitHub Release（从 `CHANGELOG.md` 提取对应版本说明）
- 构建并发布到 PyPI（Trusted Publishing）
- 安装回归验证

## 发布前检查

```bash
# 1) 工作区必须干净（除了你准备发布的改动）
git status --short

# 2) 测试必须通过
poetry run pytest -q

# 3) CHANGELOG 需包含目标版本段落（例如 2.10.2）
rg -n "^## \\[2\\.10\\.2\\]" CHANGELOG.md
```

## 标准发布步骤

以发布 `2.10.2` 为例：

```bash
# 1) 执行版本准备（默认会先跑测试）
python script/release.py --version 2.10.2

# 2) 推送主分支
git push origin main
```

推送后无需手动打 tag，也无需手动上传 PyPI。CI 会自动完成。

## 进度与结果查看

- Actions 运行页：`https://github.com/kay-ou/SimTradeLab/actions`
- 发布流水线：`Auto Release`（`publish.yml`）
- PyPI 包页：`https://pypi.org/project/simtradelab/`
- GitHub Releases：`https://github.com/kay-ou/SimTradeLab/releases`

## 常用命令

```bash
# 跳过本地测试（仅在你已确认 CI 测试覆盖时使用）
python script/release.py --version 2.10.2 --skip-tests

# 查看当前版本
python - << 'PY'
import re, pathlib
txt = pathlib.Path("pyproject.toml").read_text(encoding="utf-8")
print(re.search(r'version\\s*=\\s*"([^"]+)"', txt).group(1))
PY

# 验证 tag 是否已存在
git tag --list "v2.10.2"
```

## 失败回滚策略

PyPI 不支持覆盖同版本重新发布。若发布失败或有问题：

1. 修复问题
2. 增加补丁版本（例如 `2.10.3`）
3. 重新执行发布流程

## 版本号建议（SemVer）

- PATCH（`x.y.Z`）：bugfix、文档修正、兼容性补丁
- MINOR（`x.Y.z`）：向后兼容的新功能
- MAJOR（`X.y.z`）：破坏性变更

当前已有 tag 到 `v2.10.1`，本次若为兼容修复，建议 `2.10.2`。
