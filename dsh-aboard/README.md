# dsh-aboard

在 DeepSeek Harness 里打开 **A股市场宏观与情绪看板**。

## 它做什么

- 侧边栏新增「A股看板」入口（排在「插件市场」之后），主面板整页显示原版看板。
- 看板是原项目的 `A股市场看板.html`，ECharts 5.5.0 改为离线内置，功能未做裁剪：
  行情数据、宏观研判、情绪资金、个股分析、投资日历、消息面、股指期权七个页签，
  以及综合评分、市场宽度、恐贪指数、ETF 份额、期货基差与回测。
- 数据由插件自带的 Python 后端 `backend/server.py`（Flask + akshare + pandas + yfinance）提供，
  由 Host 半在插件启用时启动、禁用时结束，随机绑定 `127.0.0.1` 上的空闲端口。

## 路由

| 路径                     | 说明                                       |
| ------------------------ | ------------------------------------------ |
| `/aboard`                | 看板页面（后端就绪前显示启动等待页）        |
| `/aboard/echarts.min.js` | 内置 ECharts 5.5.0                          |
| `/aboard/api/*`          | 反向代理到 Python 后端                      |
| `/aboard/__status`       | 就绪状态 JSON，供等待页轮询                 |

看板页面里的 `/api/...` 请求由 Host 半在生成页面时改写成 `/aboard/api/...`，
ECharts 的 CDN 地址也改写成内置文件，因此断网时除行情外的界面仍能正常渲染。

## 数据与日志位置

运行时数据（自选股、恐贪指数历史、ETF 份额、回测记录、`backend.log`）保存在：

```
%DSH_PROFILE_DIR%\aboard-data
```

默认为 `C:\Users\<你>\.dsh\profiles\desktop\aboard-data`。首次启用时从包内 `backend/*.json` 播种，
之后升级插件不会覆盖你的数据。删除该目录即可恢复初始状态。

## 环境要求

- Python 3.10+，带 pip。
- 解释器按顺序尝试：`ABOARD_PYTHON` → `DSH_PYTHON` → `PYTHON` 环境变量 →
  `python` / `python3` / `py` → `%LOCALAPPDATA%\Programs\Python\Python31x\python.exe`，
  取第一个能成功 `import flask, pandas` 的（探测是异步的，不会卡住 Harness 启动；
  结果缓存在 `aboard-data/python.json`，7 天内直接复用，`ABOARD_FORCE_PROBE=1` 可强制重探）。
- **首次启动会自动安装依赖**（`backend/requirements.txt`）：找不到可用解释器时，用第一个带 pip 的
  Python 执行 `pip install -r backend/requirements.txt`，进度显示在等待页、日志写入 `backend.log`。
  设置 `ABOARD_NO_AUTO_PIP=1` 可关闭自动安装；失败时手动执行：
- 行情与新闻需要联网（腾讯行情、东方财富、新浪等公开接口）。
- **akshare 需 <= 1.20.4x**。1.20.45 起官方重命名了大量接口
  （如 `stock_zh_a_daily` → `stock_zh_a_hist_sina`），本项目使用的旧名字会失效，
  `/api/bt_data` 等会报错。已验证可用版本：`akshare==1.20.37`。

手动安装依赖（自动安装失败时）：

```powershell
python -m pip install -r "<插件目录>\backend\requirements.txt"
```

完整安装说明见 [INSTALL.md](INSTALL.md)。

## 从 GitHub 安装

1. 在仓库的 **Releases** 里下载 `dsh-aboard-<版本>.zip` 并解压（得到 `dsh-aboard/` 目录）；
   也可以直接 `git clone` 本仓库。
2. 打开 DSH → **插件** → **添加插件**，选择解压出来的 `dsh-aboard` 目录。
3. 首次启用会自动探测 Python 并安装依赖，等待页会显示进度；就绪后侧边栏出现「A股看板」。

维护者发布新版本时：更新 `package.json` 的 `version` → 打 tag（如 `v1.1.1`）并 push →
在 GitHub Releases 里用该 tag 发版并上传 `_dist/*.zip`、`_dist/*.tgz` 与 `SHA256SUMS.txt`。

## 卸载

在「插件」页停用或移除 `dsh-aboard`，侧边栏入口与 `/aboard` 路由一起消失，
后端子进程同时结束；`aboard-data` 目录会保留，可自行删除。

## 说明

- 看板以同源 `iframe` 嵌入，因此它保留原项目固定的深色主题，不跟随 Harness 的浅色/深色切换。
  这是因为看板是 5000 行、20 多个 ECharts 图表的既有应用，改写为 React 会丢失图表与交互。
  面板外框与侧边栏图标使用 Harness 主题变量，与宿主保持一致。
- 插件的所有路由都在 `/aboard` 前缀下，不与 Harness 的 `/api/*` 冲突，也不占用
  `dsh-market` 插件的面板 id。
