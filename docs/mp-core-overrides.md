# MoviePilot 本体覆盖改动记录

更新时间：2026-09-20

> 2026-09-20 同步说明：本次把容器内实际运行的覆盖文件整体同步回仓库，对应 MoviePilot **v2.12.3**。
> 因此本文件记录的“保留点”应理解为 v2.12.3 基线上的当前状态，而不是 6 月快照。

本文只记录 MoviePilot 本体覆盖文件，不记录 `net115helper` 插件自身逻辑。目的：以后升级 MoviePilot、重建容器或同步代码时，如果本体文件被上游覆盖，可以快速知道哪些行为需要重新合并。

## 总览

当前仓库中涉及 MP 本体的覆盖文件：

| 仓库路径 | 容器路径 | 状态 | 主要目的 |
| --- | --- | --- | --- |
| `files/app/core/config.py` | `/app/app/core/config.py` | 已纳入 git 初始快照 | 增加网盘搜索/转存默认配置、DOH/GitHub/Telegram 域名、插件市场默认列表 |
| `files/app/chain/download.py` | `/app/app/chain/download.py` | 已纳入 git 初始快照 | 强化订阅下载时的缺失剧集判断、分集下载和 episode group 透传 |
| `files/app/modules/themoviedb/__init__.py` | `/app/app/modules/themoviedb/__init__.py` | 已纳入 git 初始快照 | 支持剧集组信息、按具体季播出日期做电视剧分类 |
| `files/app/modules/filemanager/__init__.py` | `/app/app/modules/filemanager/__init__.py` | 本地新增覆盖文件 | 降低 115 媒体库存在性检查的重复读取，支持快照限深/增量参数 |

说明：前三个文件来自当前仓库的初始快照，git 历史里没有更早的上游基线；这里记录的是“当前需要保留的覆盖行为”。如要做精确逐行 diff，需要拿容器对应 MoviePilot 版本的原始文件再比对。

## `app/core/config.py`

保留点：

- `DOH_DOMAINS` 包含 TMDB、GitHub、raw.githubusercontent、codeload、Telegram API 等域名，用于容器内 DNS/直连不稳定时的解析兜底。
- 增加网盘搜索与转存相关配置项：`PANSOU_URL`、`PANSOU_AUTH_USER`、`PANSOU_AUTH_PASS`、`U115_COOKIES`、`DEFAULT_115_CID`。
- `PLUGIN_MARKET` 默认包含多个第三方插件市场地址，便于插件市场页面直接拉取。
- `SECURITY_IMAGE_DOMAINS` 包含 GitHub/raw 等域名，避免图片代理/缓存安全域名限制导致展示失败。

升级风险：

- 上游升级 `config.py` 时容易覆盖这些默认配置项，表现为 PanSou/115 相关配置读不到，或插件市场列表变少。
- 不要在这个文件里写个人 cookie/token；个人配置应继续走环境变量、系统配置或插件设置。

## `app/chain/download.py`

保留点：

- `download_single()` 会发出 `ResourceDownload` 事件，并允许外部逻辑修改 `save_path`、下载器、来源等选项。
- 识别媒体时透传 `episode_group`，避免使用剧集组的订阅在下载判断中丢失集数结构。
- `batch_download()`/缺失剧集匹配逻辑支持三层补全：
  - 整季缺失时先匹配完整季资源。
  - 已知缺失集时直接匹配单集或多集资源。
  - 仍缺失时下载种子文件，解析种子内文件列表，只选择需要的集数下载。
- `get_no_exists_info()` 使用 `NotExistMediaInfo.total_episode` 和 `start_episode` 记录缺失范围；当外部传入每季总集数时，按总集数计算缺失集，而不是只按 TMDB 当前返回的 episode 列表。

对应症状：

- 如果该覆盖丢失，可能出现“库里缺 E01，但后续集已经下载后不再补 E01”、“整季包里只需要几集却全量下载”、“episode group 订阅集数判断不准”等问题。

升级风险：

- 上游下载链更新较频繁，合并时要重点检查 `download_single()`、`batch_download()`、`get_no_exists_info()` 三块。
- 回归验证时至少选一个“已入库部分剧集 + 搜索结果包含单集/整季包”的剧，确认只下载缺失集。

## `app/modules/themoviedb/__init__.py`

保留点：

- `_process_episode_groups()` 与异步版本会在指定剧集组时写入：
  - `mediainfo.seasons`
  - `mediainfo.number_of_seasons`
  - `mediainfo.season_info`
  - `mediainfo.season_years`
  - `mediainfo.episode_group`
  - `mediainfo.episode_groups`
- 未指定剧集组但 TMDB 返回 episode groups 时，会读取 type=6 的剧集组并补充 `season_years`。
- `_get_season_aware_info()` 会在识别具体季时，用该季 `air_date` 替换分类判断用的 `first_air_date`，让新旧剧/连载剧分类按“当前季年份”判断，而不是只看整部剧第一季年份。
- 同步和异步识别路径都调用上述逻辑，避免 API/后台任务两条链路结果不一致。

对应症状：

- 如果该覆盖丢失，跨年多季剧可能被按第一季年份分类到错误目录。
- 使用 TMDB episode group 的剧可能出现总集数、季集列表、缺失判断不一致。

升级风险：

- 上游如果改了 TMDB 模块的 `MediaInfo` 构建流程，需要把 season-aware 分类和 episode group 填充重新合并进去。

## `app/modules/filemanager/__init__.py`

新增原因：

- 115 风控期间，媒体库存在性检查会频繁读取 115 目标库目录。目标库里完结剧较多时，反复递归扫描会放大 115 读请求。
- 我们需要让“订阅缺失集判断”仍然有效，同时减少对 115 媒体库目录的重复读取。

保留点：

- 增加仅针对 `u115` 存储的 `media_files()` 内存缓存：
  - 有结果缓存 20 分钟。
  - 空结果缓存 3 分钟。
  - 本地盘和其他存储不缓存，避免影响本地下载/整理实时性。
- `media_exists(..., force_refresh=True/refresh=True)` 可以绕过缓存，用于插件确认入库后的强制刷新。
- 创建目录、删除、重命名、上传、整理成功后会清空 115 媒体文件缓存，避免旧缓存长期误判。
- `media_files()` 构造用于重命名路径的 `MetaInfo` 时补齐 type/year/season/episode 默认值，降低目标路径为空或扫到媒体库父目录的概率。
- `snapshot_storage()` 支持透传 `last_snapshot_time` 和 `max_depth`，给后续增量快照/限制递归深度留接口。

对应症状：

- 如果该覆盖丢失，115 目标媒体库存在性检查会更频繁地递归扫库，可能加重 115 风控。
- 如果缓存清理逻辑缺失，可能出现“刚入库但插件还认为缺失”或“刚删除/重命名后仍按旧结果判断”。

升级风险：

- 这是完整覆盖 `FileManagerModule` 的文件。MoviePilot 升级后，如果上游 `filemanager` 模块改动较多，不能直接盲目覆盖；需要以新上游文件为基底，把上面几类逻辑重新移植。
- 特别关注 `transfer()`、`media_files()`、`media_exists()`、`snapshot_storage()` 的签名和调用方是否变化。

## 不是本体改动但与本体行为相关

排查通知链路时确认：MP 原生 `TransferChain.manual_transfer(..., background=False)` 最终是 `manual=True` 的实时手动整理，MP 默认不会发送“整理入库”通知。对应修复没有改 MP 本体，而是在 `net115helper` 插件里补偿发送 `NotificationType.Organize` 通知。

如果以后这条逻辑失效，先检查插件 `post_message()` 是否仍然走 `_PluginBase.post_message`，以及通知类型是否仍是 `NotificationType.Organize`。

排查结论：

- 通知停发起点早于当时新增的 `filemanager` 本体覆盖文件部署时间，因此不是该本体覆盖直接导致。
- 个别剧集有成功 `transferhistory`，但没有对应 `message` 记录；这些记录走的是 115 插件触发的实时手动整理路径，才需要插件侧补偿发 `NotificationType.Organize`。
- 以前部分 115 订阅入库有 TG 通知，是因为 MP 的远程目录监控 `monitor.py` 先发现待刮削目录里的新文件并加入整理队列；这条 MP 队列整理会走 `message.py` 发“整理入库”。
- 同一时间插件的待入库整理也可能调用 `manual_transfer(..., background=False)`，但如果 MP 监控已经先入队/整理，通知来自 MP 监控队列；如果插件手动整理先完成并移动了文件，MP 监控就不再能发现原文件，因此原生通知不会出现。

## 2026-09-20：v2.12.3 升级后的实测偏移

同步时对比容器内文件与 6 月快照，发现以下变化，均已按容器实际状态写回仓库：

### `app/core/config.py`：4 个自定义配置项在升级中丢失

v2.12.3 的 `config.py` 里已不存在下列声明（6 月快照中有）：

- `PANSOU_URL`、`PANSOU_AUTH_USER`、`PANSOU_AUTH_PASS`
- `U115_COOKIES`、`DEFAULT_115_CID`

`DOH_DOMAINS`、`PLUGIN_MARKET`、`SECURITY_IMAGE_DOMAINS` 仍在。

影响评估：

- **`net115helper` 不受影响**。它的读取顺序是「插件配置 → `getattr(settings, ...)` → 环境变量」，实际值来自插件配置页，settings 只是兜底。
- 仍引用这些 settings 的是 `pansousubscribe`（已被 net115helper 取代的旧插件）与
  `app/agent/tools/impl/{search_pansou,save_115_share}.py`。如果以后要重新启用它们，
  需要在插件配置、环境变量或重新补回 `config.py` 声明中任选一种方式提供取值。

结论：当前不需要回补。若后续发现旧插件或 agent 工具取不到值，再按上面三种方式之一补。

### `app/modules/themoviedb/__init__.py`、`app/chain/download.py`、`app/modules/filemanager/__init__.py`

相对 6 月快照分别有 422 / 89 / 98 行差异，主要来自上游 v2.12.3 变更与我们补丁的重新合并结果。
原有保留点（剧集组、缺失集三层补全、115 存在性检查限深）在当前容器版本中仍然存在。

### 插件侧脱敏（非本体）

`net115helper` 原先把 CD2 的 115 本地挂载根目录写死为具体部署路径，本次改为
配置项 `cd2_local_mount_115`（留空则按 `/volume*/CloudDrive/...` 等通用约定自动探测）。
已在容器内实测：自动探测结果与原硬编码路径一致，行为不变。

## 升级/回退后核对清单

1. 对比容器内文件和仓库覆盖文件：
   - `/app/app/core/config.py`
   - `/app/app/chain/download.py`
   - `/app/app/modules/themoviedb/__init__.py`
   - `/app/app/modules/filemanager/__init__.py`
2. 如果上游升级改过同名文件，先以新版上游为基底合并，不要直接用旧文件覆盖。
3. 合并后在容器内跑语法检查：
   - `python -m py_compile /app/app/core/config.py`
   - `python -m py_compile /app/app/chain/download.py`
   - `python -m py_compile /app/app/modules/themoviedb/__init__.py`
   - `python -m py_compile /app/app/modules/filemanager/__init__.py`
4. 重启 MoviePilot 后看启动日志，确认没有 `ImportError`、`Traceback`、模块加载失败。
5. 做一次订阅巡检或指定剧补片，确认：
   - 已入库剧集不会重复下载。
   - 缺失单集能后补。
   - 115 风控日志没有因为扫目标媒体库明显放大。
   - 新入库后仍能触发媒体服务器刷新和 TG 通知。
