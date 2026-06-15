"""管理网盘订阅工具 - 添加/删除/查看 PanSou 网盘资源订阅"""

import json
from typing import Optional, Type

from pydantic import BaseModel, Field

from app.agent.tools.base import MoviePilotTool
from app.core.plugin import PluginManager
from app.log import logger


class ManagePansouSubscribeInput(BaseModel):
    """管理网盘订阅工具的输入参数模型"""
    explanation: Optional[str] = Field("", description="Clear explanation of why this tool is being used in the current context")
    action: str = Field(
        ...,
        description="Action to perform: 'add' to create a new subscription, 'remove' to delete a subscription, "
                    "'list' to show all subscriptions, 'run' to manually trigger a check now."
    )
    title: Optional[str] = Field(
        None,
        description="Media title for add/remove (e.g., '三体'). Required for 'add' action."
    )
    tmdb_id: Optional[int] = Field(
        None,
        description="TMDB ID of the media. Required for 'add' action. Get this from search_media results."
    )
    media_type: Optional[str] = Field(
        None,
        description="Media type: '电影' or '电视剧'. Default: '电视剧'. Mainly for TV shows since movies don't need episode tracking."
    )
    season: Optional[int] = Field(
        None,
        description="Season number to subscribe to. E.g., season=2 will monitor for Season 2 episodes only."
    )
    search_keyword: Optional[str] = Field(
        None,
        description="Custom search keyword to use on PanSou (if different from title). "
                    "Do NOT include year in the keyword. "
                    "E.g., title='三体' but search_keyword='三体 第二季' for season-specific results."
    )
    year: Optional[int] = Field(
        None,
        description="Release year of the media (e.g., 2026). "
                    "IMPORTANT: Always provide this from search_media results. "
                    "Used to filter search results and avoid mixing same-named shows from different years."
    )


class ManagePansouSubscribeTool(MoviePilotTool):
    name: str = "manage_pansou_subscribe"
    description: str = (
        "Manage PanSou cloud storage subscriptions for automatic episode tracking and transfer. "
        "Actions: "
        "'add' - Subscribe to a TV show, the system will periodically search PanSou for new 115 episodes, "
        "compare with your library, and automatically save missing episodes. "
        "'remove' - Cancel a subscription. "
        "'list' - Show all active subscriptions. "
        "'run' - Manually trigger a subscription check now. "
        "This is like a PT subscription but for 115 cloud storage resources. "
        "Requires the '115网盘助手' (Net115Helper) plugin to be enabled, and PANSOU_URL configured."
    )
    args_schema: Type[BaseModel] = ManagePansouSubscribeInput

    def get_tool_message(self, **kwargs) -> Optional[str]:
        action = kwargs.get("action", "")
        title = kwargs.get("title", "")
        if action == "add":
            return f"正在添加网盘订阅: {title}"
        elif action == "remove":
            return f"正在取消网盘订阅: {title}"
        elif action == "list":
            return "正在查看网盘订阅列表"
        elif action == "run":
            return "正在手动触发网盘订阅检查"
        return None

    async def run(self, action: str,
                  title: Optional[str] = None,
                  tmdb_id: Optional[int] = None,
                  media_type: Optional[str] = None,
                  season: Optional[int] = None,
                  search_keyword: Optional[str] = None,
                  year: Optional[int] = None,
                  **kwargs) -> str:
        logger.info(
            f"执行工具: {self.name}, 参数: action={action}, title={title}, "
            f"tmdb_id={tmdb_id}, season={season}, year={year}"
        )

        # 获取插件实例
        plugin = self._get_plugin()
        if not plugin:
            return (
                "错误: 115网盘助手插件未安装或未启用。\n"
                "请在 MoviePilot 设置 → 插件 中启用「115网盘助手」插件。"
            )

        try:
            if action == "list":
                return self._list_subscriptions(plugin)
            elif action == "add":
                return self._add_subscription(plugin, title, tmdb_id, media_type, season, search_keyword, year)
            elif action == "remove":
                return self._remove_subscription(plugin, title, tmdb_id, season)
            elif action == "run":
                return self._run_check(plugin)
            else:
                return f"未知操作: {action}。支持的操作: add, remove, list, run"
        except Exception as e:
            logger.error(f"管理网盘订阅失败: {e}", exc_info=True)
            return f"操作失败: {str(e)}"

    def _get_plugin(self):
        """获取 Net115Helper 插件实例（向后兼容 PanSouSubscribe）"""
        try:
            plugin_manager = PluginManager()
            # 优先使用新插件
            plugin = plugin_manager.running_plugins.get("Net115Helper")
            if plugin and plugin.get_state():
                return plugin
            # 向后兼容旧插件
            plugin = plugin_manager.running_plugins.get("PanSouSubscribe")
            if plugin and plugin.get_state():
                return plugin
            return None
        except Exception:
            return None

    def _list_subscriptions(self, plugin) -> str:
        """列出所有订阅"""
        subs = plugin.list_subscriptions()
        if not subs:
            return "当前没有活跃的网盘订阅。\n使用 add 操作来添加新的订阅。"

        lines = [f"📋 网盘订阅列表 (共 {len(subs)} 个)\n"]
        for i, sub in enumerate(subs, 1):
            title = sub.get("title", "未知")
            tmdb_id = sub.get("tmdb_id", "")
            season = sub.get("season")
            media_type = sub.get("media_type", "电视剧")
            created = sub.get("created", "")
            last_found = sub.get("last_found", "从未")
            last_episodes = sub.get("last_found_episodes", [])

            year = sub.get("year", "")
            line = f"{i}. **{title}**"
            if year:
                line += f" ({year})"
            if season:
                line += f" 第{season}季"
            line += f" [{media_type}]"
            line += f" (TMDB: {tmdb_id})"
            lines.append(line)

            if last_found and last_found != "从未":
                ep_str = ", ".join(f"E{e:02d}" for e in last_episodes) if last_episodes else ""
                lines.append(f"   最近发现: {last_found[:10]} {ep_str}")

        return "\n".join(lines)

    def _add_subscription(self, plugin, title: Optional[str], tmdb_id: Optional[int],
                          media_type: Optional[str], season: Optional[int],
                          search_keyword: Optional[str],
                          year: Optional[int] = None) -> str:
        """添加订阅"""
        if not title:
            return "错误: 添加订阅需要提供 title 参数。"
        if not tmdb_id:
            return "错误: 添加订阅需要提供 tmdb_id 参数。请先使用 search_media 获取 TMDB ID。"

        resolved_media_type = media_type or "电视剧"

        # 通过插件方法自动判断分类（区分新/老电影）
        if hasattr(plugin, '_resolve_media_category'):
            media_category = plugin._resolve_media_category(resolved_media_type, season, year)
        else:
            # 兼容旧版插件
            if resolved_media_type == "电影":
                media_category = "movie"
            elif season and season > 1:
                media_category = "archive"
            else:
                media_category = "ongoing"

        sub = {
            "title": title,
            "tmdb_id": tmdb_id,
            "media_type": resolved_media_type,
            "media_category": media_category,
            "season": season,
            "search_keyword": search_keyword or title,
        }
        if year:
            sub["year"] = year

        success = plugin.add_subscription(sub)
        if success:
            result = f"✅ 网盘订阅已添加: {title}"
            if season:
                result += f" 第{season}季"

            # 立即触发一次检查，不用等 60 分钟
            try:
                plugin.check_subscriptions()
                result += "\n\n已立即执行首次检查，如有缺失集数会自动转存。"
            except Exception as e:
                logger.warning(f"首次检查失败: {e}")
                result += f"\n\n系统将每隔 {plugin._interval_minutes} 分钟自动检查。"

            result += "\n找到缺失的集数后会自动转存到你的 115 账号。"
            return result
        else:
            return f"该订阅已存在: {title} (TMDB: {tmdb_id}, 季: {season})"

    def _remove_subscription(self, plugin, title: Optional[str],
                             tmdb_id: Optional[int], season: Optional[int]) -> str:
        """删除订阅"""
        if not tmdb_id:
            return "错误: 删除订阅需要提供 tmdb_id 参数。使用 list 操作查看所有订阅。"

        success = plugin.remove_subscription(tmdb_id, season)
        if success:
            result = f"✅ 已取消订阅"
            if title:
                result += f": {title}"
            if season:
                result += f" 第{season}季"
            return result
        else:
            return "未找到匹配的订阅。使用 list 操作查看所有订阅。"

    def _run_check(self, plugin) -> str:
        """手动触发检查"""
        try:
            plugin.check_subscriptions()
            return "✅ 网盘订阅检查已执行完毕。如果有新发现的资源，会收到通知。"
        except Exception as e:
            return f"检查执行失败: {str(e)}"
