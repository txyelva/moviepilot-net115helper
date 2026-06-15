"""115网盘分享转存工具 - 将分享链接中的资源保存到自己的115账号，支持智能选集和分类存储

直接调用 115 Web API，无需独立微服务。
优先从"115网盘设置"插件读取 Cookie 和分类目录（插件页面可视化配置），
也支持通过环境变量 U115_COOKIES 配置作为备选。

分类存储:
  - movie: 电影 → 存到插件中配置的"电影目录"
  - ongoing: 连载剧 → 存到"连载剧目录"
  - archive: 老剧/完结剧 → 存到"老剧目录"
"""

import json
import re
from typing import Dict, List, Optional, Type

import httpx
from pydantic import BaseModel, Field

from app.agent.tools.base import MoviePilotTool
from app.core.config import settings
from app.core.plugin import PluginManager
from app.core.metainfo import MetaInfo as ParseMeta
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger


class Save115ShareInput(BaseModel):
    """115分享转存工具的输入参数模型"""
    explanation: str = Field(..., description="Clear explanation of why this tool is being used in the current context")
    share_url: str = Field(
        ...,
        description="The 115 share link URL (e.g., 'https://115.com/s/sw29bxxxxxx') or share code (e.g., 'sw29bxxxxxx'). "
                    "This is typically obtained from search_pansou results."
    )
    password: Optional[str] = Field(
        None,
        description="The extraction code (提取码/密码) for the share link. Some shares don't require a password."
    )
    media_category: Optional[str] = Field(
        None,
        description="IMPORTANT: Media category for choosing the right storage folder. Must be one of: "
                    "'movie' (新电影), 'movie_archive' (老电影), 'ongoing' (连载剧/正在更新的电视剧), 'archive' (老剧/已完结电视剧). "
                    "For movies: always use 'movie'. "
                    "For TV series: ASK the user whether it's 'ongoing' (连载剧) or 'archive' (老剧/完结剧) BEFORE saving. "
                    "If not specified, files go to the default folder."
    )
    target_cid: Optional[str] = Field(
        None,
        description="Override: manually specify a 115 folder ID. Only use this if the user explicitly provides a folder ID. "
                    "Normally use media_category instead to auto-select the right folder."
    )
    list_only: Optional[bool] = Field(
        False,
        description="If true, only list the contents of the share with parsed season/episode info, without saving. "
                    "Use this first to preview what's in the share and identify episodes."
    )
    season: Optional[int] = Field(
        None,
        description="Filter: only save files from this specific season number. "
                    "E.g., season=2 will only save S02 files. Very important when shares contain multiple seasons."
    )
    episodes: Optional[List[int]] = Field(
        None,
        description="Filter: only save these specific episode numbers. "
                    "E.g., episodes=[5,6,7,8] will only save E05-E08. "
                    "Use together with season parameter. "
                    "If not specified but season is set, saves all episodes of that season."
    )
    file_ids: Optional[List[str]] = Field(
        None,
        description="Manually specify file IDs to save (from list_only results). "
                    "Use this for precise control when automatic episode detection isn't sufficient."
    )


# 常见视频文件扩展名
_VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".wmv", ".flv", ".mov", ".ts", ".m2ts",
    ".rmvb", ".rm", ".mpg", ".mpeg", ".vob", ".iso", ".bdmv",
}

# 常见字幕文件扩展名
_SUBTITLE_EXTENSIONS = {
    ".srt", ".ass", ".ssa", ".sub", ".idx", ".sup",
}

# 115 Web API 请求头
_115_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://115.com/",
    "Origin": "https://115.com",
}

# 分类名映射
_CATEGORY_NAMES = {
    "movie": "电影",
    "movie_archive": "老电影",
    "ongoing": "连载剧",
    "archive": "老剧/完结剧",
}


class Save115ShareTool(MoviePilotTool):
    name: str = "save_115_share"
    description: str = (
        "Save shared 115 cloud storage (115网盘) resources to your own account with smart episode filtering "
        "and CATEGORIZED STORAGE. "
        "Takes a 115 share link (from search_pansou results) and transfers files to your 115 account. "
        "KEY FEATURES: "
        "1) Set list_only=true to preview share contents with parsed season/episode info. "
        "2) Use season and episodes parameters to save only specific episodes (e.g., season=2, episodes=[5,6,7]). "
        "3) Automatically identifies season/episode from filenames even when shares contain multiple seasons. "
        "4) CATEGORIZED STORAGE: Set media_category to auto-save to the right folder: "
        "   'movie' for movies, 'ongoing' for airing/ongoing TV series, 'archive' for completed/old TV series. "
        "   For movies, auto-use 'movie'. For TV series, ALWAYS ASK the user: '这是连载剧还是老剧/完结剧？' "
        "   before saving, then set media_category accordingly. "
        "5) After saving, tell the user to wait for MoviePilot to auto-organize and scrape metadata via CloudDrive2. "
        "Requires 115 Cookie - configure via the '115网盘助手' (Net115Helper) plugin in MoviePilot settings."
    )
    args_schema: Type[BaseModel] = Save115ShareInput

    @staticmethod
    def _get_115_config() -> dict:
        """
        获取 115 配置：
        优先从"115网盘助手"(Net115Helper) 插件读取，
        向后兼容旧 "115网盘设置"(Net115Config) 插件，
        环境变量作为最终备选。
        返回 {"cookies": str, "default_cid": str, "movie_cid": str, "ongoing_cid": str, "archive_cid": str}
        """
        # 优先：直接调用运行中的 Net115Helper，复用 cookie_source=auto/mp/p115client 的解析逻辑。
        try:
            plugin = PluginManager().running_plugins.get("Net115Helper")
            if plugin and plugin.get_state() and hasattr(plugin, "get_115_config"):
                config = plugin.get_115_config()
                if config and config.get("cookies"):
                    return {
                        "cookies": str(config.get("cookies") or "").strip(),
                        "default_cid": config.get("default_cid", "0") or "0",
                        "movie_cid": config.get("movie_cid", "") or "",
                        "old_movie_cid": config.get("old_movie_cid", "") or "",
                        "ongoing_cid": config.get("ongoing_cid", "") or "",
                        "archive_cid": config.get("archive_cid", "") or "",
                    }
        except Exception as e:
            logger.debug(f"读取运行中 115网盘助手 配置失败: {e}")

        # 备选：从新插件 Net115Helper 持久化配置读取
        try:
            plugin_config = SystemConfigOper().get("plugin.Net115Helper")
            if plugin_config and isinstance(plugin_config, dict):
                if plugin_config.get("enabled") and plugin_config.get("cookies"):
                    return {
                        "cookies": plugin_config["cookies"].strip(),
                        "default_cid": plugin_config.get("default_cid", "0") or "0",
                        "movie_cid": plugin_config.get("movie_staging_cid", "") or "",
                        "old_movie_cid": plugin_config.get("old_movie_staging_cid", "") or "",
                        "ongoing_cid": plugin_config.get("ongoing_staging_cid", "") or "",
                        "archive_cid": plugin_config.get("archive_staging_cid", "") or "",
                    }
        except Exception as e:
            logger.debug(f"读取 115网盘助手 插件配置失败: {e}")

        # 向后兼容：旧 Net115Config 插件
        try:
            plugin_config = SystemConfigOper().get("plugin.Net115Config")
            if plugin_config and isinstance(plugin_config, dict):
                if plugin_config.get("enabled") and plugin_config.get("cookies"):
                    return {
                        "cookies": plugin_config["cookies"].strip(),
                        "default_cid": plugin_config.get("default_cid", "0") or "0",
                        "movie_cid": plugin_config.get("movie_staging_cid", "") or "",
                        "old_movie_cid": "",
                        "ongoing_cid": plugin_config.get("ongoing_staging_cid", "") or "",
                        "archive_cid": plugin_config.get("archive_staging_cid", "") or "",
                    }
        except Exception as e:
            logger.debug(f"读取旧 115网盘设置 插件配置失败: {e}")

        # 备选：从环境变量读取
        if settings.U115_COOKIES:
            return {
                "cookies": settings.U115_COOKIES.strip(),
                "default_cid": getattr(settings, "DEFAULT_115_CID", "0") or "0",
                "movie_cid": "",
                "old_movie_cid": "",
                "ongoing_cid": "",
                "archive_cid": "",
            }

        return {"cookies": "", "default_cid": "0", "movie_cid": "", "old_movie_cid": "", "ongoing_cid": "", "archive_cid": ""}

    def _resolve_target_cid(self, config_115: dict, media_category: Optional[str],
                            target_cid: Optional[str]) -> tuple:
        """
        根据分类确定目标临时目录 CID。
        返回 (cid, category_label) - cid 是临时目录 ID，category_label 是分类中文名
        """
        # 手动指定的 CID 优先
        if target_cid:
            return target_cid, "手动指定"

        # 根据分类选择对应的临时目录
        if media_category:
            category = media_category.lower().strip()
            if category == "movie" and config_115.get("movie_cid"):
                return config_115["movie_cid"], "电影临时"
            elif category == "movie_archive" and config_115.get("old_movie_cid"):
                return config_115["old_movie_cid"], "老电影临时"
            elif category == "ongoing" and config_115.get("ongoing_cid"):
                return config_115["ongoing_cid"], "连载剧临时"
            elif category == "archive" and config_115.get("archive_cid"):
                return config_115["archive_cid"], "老剧临时"
            # 分类设置了但对应临时目录没配置，提示用户
            cat_name = _CATEGORY_NAMES.get(category, category)
            if category in ("movie", "movie_archive", "ongoing", "archive"):
                logger.warning(f"115 分类存储: {cat_name}临时目录未配置，使用默认目录")

        return config_115.get("default_cid", "0"), "默认"

    def get_tool_message(self, **kwargs) -> Optional[str]:
        share_url = kwargs.get("share_url", "")
        list_only = kwargs.get("list_only", False)
        season = kwargs.get("season")
        episodes = kwargs.get("episodes")
        media_category = kwargs.get("media_category")
        share_code = share_url
        match = re.search(r"115\.com/s/(\w+)", share_url)
        if match:
            share_code = match.group(1)

        if list_only:
            return f"正在查看 115 分享内容: {share_code}"

        msg = f"正在转存 115 分享资源: {share_code}"
        if media_category:
            cat_name = _CATEGORY_NAMES.get(media_category.lower(), media_category)
            msg += f" → {cat_name}目录"
        if season:
            msg += f" (第{season}季"
            if episodes:
                ep_str = ",".join(str(e) for e in sorted(episodes))
                msg += f" 第{ep_str}集"
            msg += ")"
        return msg

    def _build_headers(self, cookies: str) -> dict:
        """构建带 Cookie 的 115 API 请求头"""
        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies
        return headers

    async def run(self, share_url: str,
                  password: Optional[str] = None,
                  media_category: Optional[str] = None,
                  target_cid: Optional[str] = None,
                  list_only: Optional[bool] = False,
                  season: Optional[int] = None,
                  episodes: Optional[List[int]] = None,
                  file_ids: Optional[List[str]] = None,
                  **kwargs) -> str:
        logger.info(
            f"执行工具: {self.name}, 参数: share_url={share_url}, "
            f"password={'***' if password else 'None'}, media_category={media_category}, "
            f"target_cid={target_cid}, list_only={list_only}, "
            f"season={season}, episodes={episodes}, file_ids={file_ids}"
        )

        # 从插件或环境变量获取 115 配置
        config_115 = self._get_115_config()

        if not config_115["cookies"]:
            return (
                "错误: 未配置 115 网盘 Cookie。\n\n"
                "请在 MoviePilot 中配置：\n"
                "1. 进入 设置 → 插件 → 找到「115网盘助手」\n"
                "2. 启用插件，将 115 Cookie 粘贴到输入框中\n"
                "3. 保存即可\n\n"
                "获取 Cookie 方法：浏览器登录 115.com → F12 → Network → 任意请求 → 复制 Cookie 头内容。"
            )

        try:
            share_code = share_url
            receive_code = password or ""

            # 从完整链接中提取 share_code
            match = re.search(r"115\.com/s/(\w+)", share_url)
            if match:
                share_code = match.group(1)
                pwd_match = re.search(r"[?&]password=(\w+)", share_url)
                if pwd_match and not password:
                    receive_code = pwd_match.group(1)

            # 也支持 anxia.com 和 115cdn.com 的链接格式
            if not match:
                match = re.search(r"(?:anxia|115cdn)\.com/s/(\w+)", share_url)
                if match:
                    share_code = match.group(1)
                    pwd_match = re.search(r"[?&]password=(\w+)", share_url)
                    if pwd_match and not password:
                        receive_code = pwd_match.group(1)

            # 获取分享文件列表
            cookies = config_115["cookies"]
            share_result = await self._fetch_share_files(share_code, receive_code, cookies)
            if isinstance(share_result, str):
                return share_result  # 错误信息

            files = share_result.get("files", [])
            share_title = share_result.get("share_title", "")

            if not files:
                return "分享链接中没有找到文件，链接可能已失效或密码不正确。"

            # 解析每个文件的季集信息
            parsed_files = self._parse_episode_info(files)

            if list_only:
                return self._format_file_list(parsed_files, share_title)

            # 确定目标目录
            resolved_cid, category_label = self._resolve_target_cid(
                config_115, media_category, target_cid
            )

            # 确定要保存的文件
            if file_ids:
                # 手动指定文件 ID
                selected_ids = file_ids
                selected_files = [f for f in parsed_files if f["file_id"] in file_ids]
            elif season is not None or episodes is not None:
                # 按季/集过滤
                selected_files = self._filter_by_episode(parsed_files, season, episodes)
                selected_ids = [f["file_id"] for f in selected_files]
                if not selected_ids:
                    # 没有匹配的文件，返回完整列表帮助用户选择
                    result = "未找到匹配的文件。\n\n"
                    if season:
                        result += f"在分享中没有找到第 {season} 季"
                        if episodes:
                            result += f" 第 {','.join(str(e) for e in episodes)} 集"
                        result += " 的文件。\n\n"
                    result += "以下是分享中所有文件的季集解析结果：\n\n"
                    result += self._format_file_list(parsed_files, share_title)
                    return result
            else:
                # 全部保存
                selected_ids = [f["file_id"] for f in parsed_files]
                selected_files = parsed_files

            return await self._save_share(
                share_code, receive_code, cookies,
                resolved_cid, category_label,
                selected_ids, selected_files, season, episodes
            )

        except httpx.TimeoutException:
            return "操作超时，115 网盘 API 响应时间过长，请稍后重试。"
        except httpx.ConnectError:
            return "无法连接到 115 网盘 API，请检查网络连接。"
        except Exception as e:
            error_message = f"115 分享转存失败: {str(e)}"
            logger.error(f"115 分享转存失败: {e}", exc_info=True)
            return error_message

    async def _fetch_share_files(self, share_code: str, receive_code: str, cookies: str) -> dict | str:
        """直接调用 115 Web API 获取分享文件列表"""
        headers = self._build_headers(cookies)
        try:
            async with httpx.AsyncClient(
                headers=headers, follow_redirects=True, timeout=30.0
            ) as client:
                resp = await client.get(
                    "https://webapi.115.com/share/snap",
                    params={
                        "share_code": share_code,
                        "receive_code": receive_code,
                        "cid": "0",
                        "limit": 200,
                        "offset": 0,
                    },
                )
                data = resp.json()

            if not data.get("state"):
                error_msg = data.get("error", data.get("error_msg", "未知错误"))
                # 常见错误翻译
                if "密码" in str(error_msg) or "receive_code" in str(error_msg):
                    return f"分享提取码不正确或未提供。请确认提取码后重试。"
                if "已取消" in str(error_msg) or "不存在" in str(error_msg) or "已失效" in str(error_msg):
                    return f"分享链接已失效或不存在。"
                return f"获取分享内容失败: {error_msg}"

            share_data = data.get("data", {})
            file_list = share_data.get("list", [])
            share_info = share_data.get("shareinfo", {})

            files = []
            for f in file_list:
                is_dir = bool(f.get("fc", 0))  # fc > 0 表示文件夹（子文件数）
                file_id = str(f.get("cid", f.get("fid", "")))
                files.append({
                    "file_id": file_id,
                    "name": f.get("n", f.get("fn", "")),
                    "size": int(f.get("s", 0)),
                    "is_dir": is_dir,
                })

            return {
                "share_code": share_code,
                "share_title": share_info.get("snap_name", ""),
                "file_count": len(files),
                "files": files,
            }

        except httpx.TimeoutException:
            return "获取分享内容超时，请稍后重试。"
        except httpx.ConnectError:
            return "无法连接到 115 网盘服务器，请检查网络。"
        except Exception as e:
            logger.error(f"获取分享内容异常: {e}", exc_info=True)
            return f"获取分享内容失败: {str(e)}"

    def _parse_episode_info(self, files: List[dict]) -> List[dict]:
        """
        解析每个文件的季集信息
        使用 MoviePilot 的 MetaInfo 解析引擎
        """
        parsed = []
        for f in files:
            name = f.get("name", "")
            file_id = f.get("file_id", "")
            size = f.get("size", 0)
            is_dir = f.get("is_dir", False)

            entry = {
                "file_id": file_id,
                "name": name,
                "size": size,
                "is_dir": is_dir,
                "season": None,
                "episode": None,
                "episode_list": [],
                "media_name": "",
                "is_video": False,
                "is_subtitle": False,
            }

            # 检查文件类型
            name_lower = name.lower()
            ext = ""
            if "." in name:
                ext = "." + name.rsplit(".", 1)[-1].lower()

            entry["is_video"] = ext in _VIDEO_EXTENSIONS or is_dir
            entry["is_subtitle"] = ext in _SUBTITLE_EXTENSIONS

            # 使用 MetaInfo 解析文件名
            try:
                meta = ParseMeta(name)
                entry["season"] = meta.begin_season
                entry["episode"] = meta.begin_episode
                entry["episode_list"] = meta.episode_list if hasattr(meta, 'episode_list') and meta.episode_list else (
                    list(range(meta.begin_episode, (meta.end_episode or meta.begin_episode) + 1))
                    if meta.begin_episode else []
                )
                entry["media_name"] = meta.name or ""
                # 如果没有识别到季数，但有集数，默认为第1季
                if entry["episode"] and not entry["season"]:
                    entry["season"] = 1
            except Exception as e:
                logger.debug(f"解析文件名失败: {name}, 错误: {e}")

            parsed.append(entry)

        return parsed

    def _filter_by_episode(self, parsed_files: List[dict],
                           season: Optional[int],
                           episodes: Optional[List[int]]) -> List[dict]:
        """
        按季和集过滤文件
        """
        selected = []
        for f in parsed_files:
            # 文件夹总是跳过（由其中的文件决定）
            if f["is_dir"]:
                continue

            # 非视频/字幕文件跳过
            if not f["is_video"] and not f["is_subtitle"]:
                continue

            file_season = f.get("season")
            file_episodes = f.get("episode_list", [])

            # 按季过滤
            if season is not None:
                if file_season != season:
                    continue

            # 按集过滤
            if episodes is not None:
                if not file_episodes:
                    continue
                # 检查文件的集数是否与请求的集数有交集
                if not set(file_episodes) & set(episodes):
                    continue

            selected.append(f)

        return selected

    def _format_file_list(self, parsed_files: List[dict], share_title: str) -> str:
        """格式化文件列表，包含季集解析信息"""
        result_lines = []
        if share_title:
            result_lines.append(f"分享标题: {share_title}")
        result_lines.append(f"文件数量: {len(parsed_files)}")
        result_lines.append("")

        # 按季分组展示
        by_season: Dict[Optional[int], List[dict]] = {}
        no_episode = []
        total_size = 0

        for f in parsed_files:
            total_size += f.get("size", 0)
            s = f.get("season")
            if s is not None:
                by_season.setdefault(s, []).append(f)
            else:
                no_episode.append(f)

        # 先展示按季分组的文件
        for s in sorted(by_season.keys()):
            season_files = by_season[s]
            # 按集数排序
            season_files.sort(key=lambda x: (x.get("episode") or 0))
            result_lines.append(f"第 {s} 季 ({len(season_files)} 个文件)")
            for f in season_files:
                ep = f.get("episode")
                ep_list = f.get("episode_list", [])
                size_str = self._format_size(f["size"]) if f["size"] else ""

                if ep_list and len(ep_list) > 1:
                    ep_tag = f"E{ep_list[0]:02d}-E{ep_list[-1]:02d}"
                elif ep:
                    ep_tag = f"E{ep:02d}"
                else:
                    ep_tag = "未识别集数"

                icon = "[DIR]" if f["is_dir"] else " "
                parts = [f"  {icon} S{s:02d}{ep_tag}"]
                if size_str:
                    parts.append(size_str)
                parts.append(f"| {f['name']}")
                parts.append(f"(ID: {f['file_id']})")
                result_lines.append(" ".join(parts))
            result_lines.append("")

        # 展示无法识别季集的文件
        if no_episode:
            result_lines.append(f"其他文件 ({len(no_episode)} 个)")
            for f in no_episode:
                icon = "[DIR]" if f["is_dir"] else " "
                size_str = self._format_size(f["size"]) if f["size"] else ""
                parts = [f"  {icon} {f['name']}"]
                if size_str:
                    parts.append(f"({size_str})")
                parts.append(f"(ID: {f['file_id']})")
                result_lines.append(" ".join(parts))
            result_lines.append("")

        if total_size > 0:
            result_lines.append(f"总大小: {self._format_size(total_size)}")

        result_lines.append("")
        result_lines.append("使用方法:")
        result_lines.append("- 转存全部: save_115_share (不设 season/episodes)")
        result_lines.append("- 转存某季: save_115_share season=2")
        result_lines.append("- 转存某几集: save_115_share season=2 episodes=[5,6,7,8]")
        result_lines.append("- 手动选择: save_115_share file_ids=[\"xxx\",\"yyy\"]")
        result_lines.append("- 分类存储: save_115_share media_category='movie'|'ongoing'|'archive'")

        return "\n".join(result_lines)

    async def _save_share(self, share_code: str, receive_code: str,
                          cookies: str,
                          cid: str,
                          category_label: str,
                          selected_ids: List[str],
                          selected_files: List[dict],
                          season: Optional[int],
                          episodes: Optional[List[int]]) -> str:
        """直接调用 115 Web API 转存分享资源"""
        cid = cid or "0"
        headers = self._build_headers(cookies)

        try:
            async with httpx.AsyncClient(
                headers=headers, follow_redirects=True, timeout=60.0
            ) as client:
                resp = await client.post(
                    "https://webapi.115.com/share/receive",
                    data={
                        "share_code": share_code,
                        "receive_code": receive_code,
                        "file_id": ",".join(selected_ids),
                        "cid": cid,
                    },
                )
                result = resp.json()

        except httpx.TimeoutException:
            return "转存操作超时，请稍后重试。"
        except Exception as e:
            logger.error(f"转存请求异常: {e}", exc_info=True)
            return f"转存请求失败: {str(e)}"

        if result.get("state"):
            saved_count = len(selected_ids)

            # 构建结果摘要
            summary = f"转存成功！"
            if season and episodes:
                ep_str = ", ".join(f"E{e:02d}" for e in sorted(episodes))
                summary += f" 已保存第 {season} 季 {ep_str} 共 {saved_count} 个文件"
            elif season:
                summary += f" 已保存第 {season} 季全部 {saved_count} 个文件"
            else:
                summary += f" 已保存 {saved_count} 个文件"
            summary += f"\n存储位置: {category_label}目录（CID: {cid}）\n\n"

            # 列出保存的文件
            if selected_files and len(selected_files) <= 20:
                summary += "已保存的文件:\n"
                for f in selected_files:
                    s = f.get("season")
                    ep = f.get("episode")
                    tag = ""
                    if s and ep:
                        tag = f"S{s:02d}E{ep:02d} "
                    elif s:
                        tag = f"S{s:02d} "
                    summary += f"  - {tag}{f['name']}\n"
                summary += "\n"

            summary += "后续自动处理:\n"
            summary += "1. CloudDrive2 会自动同步，文件稍后出现在 NAS 挂载路径的临时目录中\n"
            summary += "2. MoviePilot 目录监控自动发现新文件 → 刮削海报/NFO → 重命名\n"
            summary += "3. 刮削完成后自动移动到最终媒体库目录，Plex/Emby 即可播放"
            return summary
        else:
            error_msg = result.get("error", result.get("error_msg", "转存失败"))
            # 常见错误处理
            if "已存在" in str(error_msg) or "已保存" in str(error_msg):
                return f"资源已存在于目标目录中，无需重复转存。"
            if "cookie" in str(error_msg).lower() or "登录" in str(error_msg):
                return f"115 Cookie 已过期或无效，请到 插件 → 115网盘助手 中重新粘贴新的 Cookie。"
            return f"转存失败: {error_msg}"

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        size = float(size_bytes)
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        return f"{size:.1f} {units[unit_index]}"
