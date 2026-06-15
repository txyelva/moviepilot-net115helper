# MoviePilot 115 网盘助手

这是一个围绕 MoviePilot、115 网盘和 CloudDrive2 构建的云盘订阅自动化插件。核心目标不是替代 Plex，而是把传统的“PT 下载到本地再整理入库”改成“云盘转存 + CloudDrive2 挂载 + MoviePilot 整理入库”。

本仓库包含 `Net115Helper` 插件、本地补丁和相关说明文档。仓库内容按可分享版本维护，真实 NAS 地址、个人目录、cookie、token、扫码 session 等运行时私密信息不应进入版本库。

## 状态与风险

本项目是实验性自动化工具，面向个人媒体库工作流。它会调用网盘接口、保存分享、复制文件、清理临时目录并触发 MoviePilot 整理入库。使用前请先阅读 [DISCLAIMER.md](DISCLAIMER.md)。

注意：

- 本项目与 MoviePilot、115 网盘、CloudDrive2、阿里云盘、123 云盘、夸克网盘、百度网盘、Plex 等平台没有官方关联。
- 网盘接口、Cookie、扫码 session、分享转存、限流和风控策略都可能变化。
- 自动化转存、扫描和跨盘复制可能触发账号风控、登录失效或平台限制。
- 使用者需要自行遵守各平台服务条款，并自行承担账号、数据和兼容性风险。
- 仓库包含 MoviePilot 本体覆盖文件，不是完整 MoviePilot 发行版；升级 MoviePilot 后需要重新比对和合并补丁。

## 一句话说明

如果资源在 115 网盘上已经存在，插件会优先把分享资源转存/秒传到 115 待整理目录，再触发 MoviePilot 原生整理入库。Plex 扫描的是 CloudDrive2 挂载出来的 115 媒体库目录，所以很多剧集可以绕过本地下载流程，比 PT 下载入库更快。

如果 115 没有资源，插件可以降级去阿里云盘、123 云盘、夸克网盘、百度网盘等来源查找。找到后先保存到对应网盘自己的临时目录，再通过 CloudDrive2 从对应网盘挂载目录复制到 115 待整理目录，最后仍然交给 MoviePilot 统一识别、重命名、刮削和入库。

## 整体链路

```text
MoviePilot 订阅
  -> 115 网盘助手读取订阅和缺失集
  -> 优先搜索 115 资源
  -> 115 转存/秒传到待整理目录
  -> MoviePilot 整理、刮削、入库、通知
  -> Plex 扫描 CloudDrive2 挂载出来的 115 媒体库
```

降级链路：

```text
115 未命中
  -> 搜索阿里云盘 / 123 云盘 / 夸克网盘 / 百度网盘
  -> 插件登录对应网盘并保存分享到临时目录
  -> CloudDrive2 从该网盘挂载路径复制到 115 待整理目录
  -> MoviePilot 整理入库
  -> Plex 看到 115 媒体库里的新文件
```

最终媒体库仍然收束到 115。Plex 不需要分别添加阿里云盘、夸克网盘、百度网盘这些库目录，降级盘只是补资源的中转来源。

## NAS 上需要的服务

| 类型 | 服务 | 是否必需 | 作用 |
| --- | --- | --- | --- |
| NAS 套件 / 宿主机服务 | Plex Media Server | 已有即可 | 扫描媒体库并播放。媒体库目录指向 CloudDrive2 挂载出来的 115 媒体库目录。 |
| NAS 套件 / 宿主机服务 | Container Manager / Docker | 必需 | 运行 MoviePilot 等容器服务。 |
| NAS 套件 / 宿主机服务优先 | CloudDrive2 | 必需 | 挂载 115 给 Plex 使用，也负责把降级盘里的文件复制到 115 待整理目录。优先装在 NAS 宿主机或套件层；Docker 方式也可以，但通常需要 FUSE、特权权限和额外挂载配置。 |
| Docker | MoviePilot | 必需 | 负责订阅、识别、刮削、整理、入库、通知，以及承载本插件。 |
| Docker / 外部服务 | PanSou 或其它搜索服务 | 按需 | 给插件提供网盘资源搜索能力；也可以使用已有的内网或公网搜索 API。 |
| Docker / 套件 | Emby / Jellyfin | 可选 | 如果不用 Plex，可以换成其它媒体服务器。 |
| Docker / 套件 | Tautulli | 可选 | Plex 统计和日报，不是入库链路必需项。 |

CloudDrive2 不建议只理解成“播放器挂载工具”。在这套链路里它有两个角色：

- 挂载 115 媒体库，让 Plex 像读本地目录一样读 115。
- 挂载阿里云盘、123 云盘、夸克网盘、百度网盘等降级盘，让插件可以把这些盘里的资源复制回 115 待整理目录。

## 账号和会员成本口径

具体价格会随官方活动变化，本文不写死金额。判断成本时可以按“必选”和“可选”拆：

| 账号 / 会员 | 是否必需 | 用途 |
| --- | --- | --- |
| 115 网盘会员 | 必需 | 主媒体库、分享转存、CloudDrive2 挂载、Plex 播放。这个是整套方案的核心成本。 |
| CloudDrive2 会员 / 授权 | 必需 | 提供稳定挂载和跨网盘复制能力。没有 CloudDrive2，这套“挂载 + 降级搬运”的链路就不完整。 |
| MoviePilot | 必需 | 自建服务本身通常没有单独会员成本。 |
| Plex Pass | 非必需 | 已有 Plex 可直接用。Plex Pass 只影响 Plex 自身高级功能，不是本插件必需。 |
| 阿里云盘会员 | 可选 | 作为降级资源来源。没有也可以，只是少一个补资源渠道。 |
| 123 云盘会员 | 可选 | 作为降级资源来源。没有也可以。 |
| 夸克网盘会员 | 可选 | 作为降级资源来源。没有也可以。 |
| 百度网盘会员 | 可选 | 作为降级资源来源。没有也可以。 |

最小可用组合：

```text
已有 NAS + Plex
  + Docker / Container Manager
  + MoviePilot
  + CloudDrive2
  + 115 网盘会员
  + 本插件
```

增强补资源组合：

```text
最小组合
  + 阿里云盘 / 123 云盘 / 夸克网盘 / 百度网盘中的一个或多个账号
  + 对应网盘在插件里的登录信息
  + 对应网盘在 CloudDrive2 里的挂载
```

## 插件做了什么

- 从 MoviePilot 订阅里识别需要追更的剧集。
- 根据本地媒体库已有集数判断缺失集，优先只处理缺失集，避免整季重复转存。
- 优先搜索并转存 115 分享资源。
- 115 未命中时，按前端勾选的降级盘顺序去其它网盘找资源。
- 对阿里云盘、123 云盘、夸克网盘、百度网盘执行各自的保存分享逻辑。
- 通过 CloudDrive2 把降级盘临时目录里的文件复制到 115 待整理目录。
- 调用 MoviePilot 原生整理流程，让文件最终走 MoviePilot 的识别、重命名、刮削、入库、通知。
- 对 115 请求做分批、冷却和限流，尽量减少 115 风控。
- 对整理失败、pending、缺失集做后补，减少因为临时失败导致长期漏集。

## 配置原则

所有个人配置都应该放在 MoviePilot 插件设置里，不要写死在代码里。

应该由用户配置的内容包括：

- 115 Cookie 或扫码登录状态。
- 阿里云盘 refresh token / 扫码登录状态。
- 夸克网盘 Cookie。
- 百度网盘 Cookie。
- 123 云盘相关授权。
- CloudDrive2 地址、端口、账号或 token。
- 115 待整理目录、媒体库目录、电影/剧集分类目录。
- 各降级盘的临时转存目录。
- 是否启用某个降级盘，以及降级优先级。
- 搜索服务地址。
- 通知、巡检、自动检查登录态等开关。

公开发布或分享前，必须确认没有提交：

- 真实 cookie、token、refresh token、Authorization header。
- 真实 NAS 地址、SSH 用户名、密码。
- 真实家庭内网 IP、端口映射、反代域名。
- 个人媒体库路径、个人账号截图、扫码 session。
- Docker 部署包、NAS 配置备份、日志文件。

## 仓库结构

```text
files/app/plugins/net115helper/
  115 网盘助手主插件和各网盘客户端

files/public/plugins/net115helper/
  插件详情页和扫码登录相关前端页面

files/app/modules/
files/app/chain/
files/app/core/
  当前环境里为了配合插件做过的 MoviePilot 本体补丁

docs/mp-core-overrides.md
  MoviePilot 本体补丁说明。升级 MoviePilot 前后需要重点核对。
```

AI agent 或维护者理解代码时，建议先读：

1. `README.md`
2. `docs/mp-core-overrides.md`
3. `files/app/plugins/net115helper/__init__.py`
4. `files/app/plugins/net115helper/clients/cd2.py`
5. `files/app/plugins/net115helper/clients/aliyun.py`
6. `files/app/plugins/net115helper/clients/quark.py`
7. `files/app/plugins/net115helper/clients/baidu.py`

## 部署思路

当前仓库是补丁式结构，不是完整 MoviePilot 镜像。部署时的基本思路是把 `files/` 下的内容覆盖到 MoviePilot 容器内对应路径，然后重启 MoviePilot。

部署前建议：

- 备份 MoviePilot 配置目录。
- 备份容器内原始插件目录。
- 先在测试容器或低峰期部署。
- 部署后检查 MoviePilot 是否正常启动。
- 检查 115 插件详情页能否打开。
- 检查 115、阿里云盘、夸克网盘、百度网盘等登录态。
- 手动跑一次巡检或单个订阅归档，确认整理和通知链路正常。

## 简版解释

```text
这套方案的核心不是 Plex，而是 115 网盘 + CloudDrive2 + MoviePilot + 自写 115 助手插件。

Plex 的库目录指向 CloudDrive2 挂载出来的 115 媒体库。MoviePilot 负责订阅、识别、刮削和整理。本插件负责根据订阅自动搜索网盘资源，优先把 115 分享转存到 115 待整理目录，再触发 MoviePilot 入库。

如果 115 没资源，插件会按配置去阿里云盘、123 云盘、夸克网盘、百度网盘找，先保存到对应网盘临时目录，再通过 CloudDrive2 复制到 115 待整理目录，最后还是交给 MoviePilot 整理入库。

所以速度快的原因是优先走云盘内转存/秒传，不需要先 PT 下载到本地再搬运。必需成本主要是 115 会员和 CloudDrive2 授权；其它网盘会员是可选降级资源池。
```

## 维护注意

- 不要提交 `.claude/`、运行时 cookie、扫码 session、部署压缩包或 NAS 配置备份。
- 线上部署前先备份 MoviePilot 配置目录和容器内插件目录。
- MoviePilot 升级后要对照 `docs/mp-core-overrides.md` 检查本体补丁是否被覆盖。
- 如果插件前端或登录态异常，优先检查 MoviePilot 插件设置，不要把个人配置重新写回源码。

## License

本项目原创插件代码按 [MIT License](LICENSE) 发布。仓库中用于覆盖或补丁的 MoviePilot 上游派生文件，仍以其上游许可证为准。
