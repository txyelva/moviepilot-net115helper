"""
115网盘设置插件 [已废弃]
⚠️ 此插件已被「115网盘助手」(Net115Helper) 替代。
请启用新插件，旧配置会自动迁移。
"""

from typing import Any, Dict, List, Optional, Tuple

from app.log import logger
from app.plugins import _PluginBase


class Net115Config(_PluginBase):
    """
    115 网盘设置 [已废弃]
    ⚠️ 此插件已被「115网盘助手」(Net115Helper) 替代。
    """

    # 插件基本信息
    plugin_name = "115网盘设置 [已废弃]"
    plugin_desc = "⚠️ 此插件已废弃，请改用「115网盘助手」插件（集成 Cookie 配置 + 订阅管理 + 详情页），旧配置会自动迁移。"
    plugin_version = "1.1"
    plugin_order = 99
    plugin_icon = "https://115.com/favicon.ico"

    # 私有属性
    _enabled = False
    _cookies = ""
    _default_cid = "0"
    _movie_staging_cid = ""
    _ongoing_staging_cid = ""
    _archive_staging_cid = ""

    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        if config:
            self._enabled = config.get("enabled", False)
            self._cookies = config.get("cookies", "")
            self._default_cid = config.get("default_cid", "0")
            self._movie_staging_cid = config.get("movie_staging_cid", "")
            self._ongoing_staging_cid = config.get("ongoing_staging_cid", "")
            self._archive_staging_cid = config.get("archive_staging_cid", "")

        if self._enabled:
            logger.warning(
                "⚠️ [115网盘设置] 此插件已废弃！请改用「115网盘助手」(Net115Helper) 插件。"
                "\n   新插件集成了 Cookie 配置、分类目录、订阅管理和详情页，旧配置会自动迁移。"
                "\n   请到 设置 → 插件 中启用「115网盘助手」，然后禁用本插件。"
            )

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """无需定时服务"""
        return []

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """
        插件配置页面
        """
        return [
            {
                "component": "VForm",
                "content": [
                    # ======== 废弃提示 ========
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "title": "⚠️ 此插件已废弃",
                                            "text": "请改用「115网盘助手」插件，它集成了 Cookie 配置、分类目录、订阅管理和详情页。"
                                                    "\n旧配置会自动迁移到新插件，无需手动操作。"
                                                    "\n请到 设置 → 插件 中启用「115网盘助手」，然后禁用本插件。",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # ======== 基本设置 ========
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用 115 网盘转存功能",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # Cookie 输入框
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "cookies",
                                            "label": "115 网盘 Cookie",
                                            "placeholder": "UID=xxx; CID=xxx; SEID=xxx; ...",
                                            "rows": 4,
                                            "hint": "从浏览器获取: 登录 115.com → F12 → Network → 任意请求 → Request Headers → 复制 Cookie 值",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                        ]
                    },

                    # ======== 分类临时目录 ========
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "分类临时目录（推荐配置）",
                                            "text": "在 115 网盘中创建 3 个临时文件夹（如「待整理-电影」「待整理-连载剧」「待整理-老剧」），"
                                                    "将文件夹 ID 填在下方。"
                                                    "\n\nAI 转存时会按类型存到对应临时目录 → CloudDrive2 同步到 NAS → "
                                                    "MoviePilot 目录监控自动刮削整理 → 剪切到你的最终媒体库目录。"
                                                    "\n\n获取文件夹 ID: 在 115 网页版中打开文件夹，地址栏中 cid= 后面的数字就是 ID。"
                                                    "\n\n注意: 还需要在 MoviePilot「设置 → 存储 & 目录」中为每个临时目录配置目录监控，"
                                                    "指定对应的媒体库目标路径、整理方式选「移动」、开启刮削。",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # 电影临时目录
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "movie_staging_cid",
                                            "label": "电影临时目录 ID",
                                            "placeholder": "例: 2345678901",
                                            "hint": "115 中的「待整理-电影」文件夹 ID",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                            # 连载剧临时目录
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "ongoing_staging_cid",
                                            "label": "连载剧临时目录 ID",
                                            "placeholder": "例: 3456789012",
                                            "hint": "115 中的「待整理-连载剧」文件夹 ID",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                            # 老剧临时目录
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "archive_staging_cid",
                                            "label": "老剧/完结剧临时目录 ID",
                                            "placeholder": "例: 4567890123",
                                            "hint": "115 中的「待整理-老剧」文件夹 ID",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # 默认目录（兜底）
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "default_cid",
                                            "label": "默认目录 ID（兜底）",
                                            "placeholder": "0",
                                            "hint": "未指定类型时的默认目录，0 = 根目录",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                        ]
                    },

                    # ======== 使用说明 ========
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "success",
                                            "variant": "tonal",
                                            "title": "配置完成后的使用流程",
                                            "text": "1. 在 Telegram 对话中说「搜索xxx的115资源」"
                                                    "\n2. AI 找到资源后，电影会直接转存到电影临时目录"
                                                    "\n3. 电视剧会先问你「连载剧还是老剧？」，然后存到对应临时目录"
                                                    "\n4. CloudDrive2 自动同步，文件出现在 NAS 挂载路径"
                                                    "\n5. MoviePilot 目录监控自动刮削（海报/NFO）、重命名、移动到最终媒体库"
                                                    "\n6. Plex/Emby 自动发现新内容，即可播放",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "cookies": "",
            "default_cid": "0",
            "movie_staging_cid": "",
            "ongoing_staging_cid": "",
            "archive_staging_cid": "",
        }

    def get_page(self) -> List[dict]:
        """无自定义页面"""
        return []

    def stop_service(self):
        """停止插件"""
        pass
