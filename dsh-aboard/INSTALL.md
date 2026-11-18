# 安装 dsh-aboard（A股看板）

一个 DeepSeek Harness 插件包：在侧边栏新增「A股看板」，主面板打开 A 股市场宏观与情绪看板
（行情数据、宏观研判、情绪资金、个股分析、投资日历、消息面、股指期权七个页签，含综合评分、
市场宽度、恐贪指数、ETF 份额、期货基差与回测）。

## 1. 安装

任选一种：

**A. 插件页安装（推荐）**

1. 解压 `dsh-aboard-<版本>.zip`，得到 `dsh-aboard` 文件夹。
2. 打开 Harness 的 **插件** 页 → **添加插件 / Add plugin**。
3. 选择解压出来的 `dsh-aboard` 文件夹（含 `package.json` 的那一层）。
4. 等安装完成，侧边栏出现「A股看板」。

**B. 命令行 / 由 Agent 安装**

```
plugin_manager { action: "install_bundle", target: "<解压目录的绝对路径>\\dsh-aboard" }
```

## 2. 首次启动会做什么

插件启用后会启动自带的 Python 数据后端（`backend/server.py`），并且：

- **自动检测 Python**：`ABOARD_PYTHON` → `DSH_PYTHON` → `PYTHON` → `python` / `python3` / `py` →
  常见安装目录 → Harness 自带运行时。
- **自动安装后端依赖**：找不到能 `import flask, pandas, akshare` 的解释器时，会用第一个可用的
  Python 执行 `pip install -r backend/requirements.txt`（需要联网，首次约几分钟）。进度会显示在看板
  等待页上，日志写入 `<profile>/aboard-data/backend.log`。
- 之后每次启动只做快速探测，不会重复安装。

如果自动安装失败（例如系统 Python 没有 pip、无网络、或有代理限制），等待页会显示具体原因和命令，
手动执行即可：

```powershell
python -m pip install -r "<插件目录>\backend\requirements.txt"
```

也可以设置环境变量 `ABOARD_PYTHON` 指向一个已经装好依赖的 `python.exe`，或用
`ABOARD_NO_AUTO_PIP=1` 关闭自动安装。

## 3. 环境要求

- Python 3.10+（64 位），带 pip。
- 依赖见 `backend/requirements.txt`；其中 **akshare 必须是 1.20.4x**（已验证 1.20.37），
  1.20.45 起官方重命名了接口，本插件使用的旧接口会失效。
- 联网：行情/新闻使用腾讯、新浪、东方财富、金十等公开接口；断网时界面仍可渲染，但行情为空。
- 后端只在 `127.0.0.1` 的随机空闲端口监听，不对外暴露。

## 4. 数据与卸载

- 运行时数据（自选股、恐贪历史、ETF 份额、回测记录、日志）在
  `<profile>/aboard-data`，卸载插件不会删除；删掉该目录即可恢复初始状态。
- 卸载：插件页停用或移除 `dsh-aboard`，侧边栏入口与 `/aboard` 路由一起消失，后端子进程会结束。

## 5. 打包（给维护者）

```powershell
# 目录本身就是一个合法 bundle（package.json 的 dsh.bundle.patch 指向 cordis.patch.yml）
python -c "import tarfile; ..."   # 或 npm pack / pnpm pack
```

打包前确认包内不含 `aboard-data`、`__pycache__`、`*.pyc` 和编辑备份文件。
