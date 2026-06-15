"""
PanSou 网盘订阅插件 [已废弃]
⚠️ 此插件已被「115网盘助手」(Net115Helper) 替代。
请启用新插件，旧配置会自动迁移。
"""

import json
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Type

import httpx

from app.core.config import settings
from app.core.metainfo import MetaInfo as ParseMeta
from app.chain.media import MediaChain
from app.chain.transfer import TransferChain
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, MediaType


# 115 Web API 请求头
_115_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://115.com/",
    "Origin": "https://115.com",
}


class PanSouSubscribe(_PluginBase):
    """
    PanSou 网盘订阅插件 [已废弃]
    ⚠️ 此插件已被「115网盘助手」(Net115Helper) 替代。
    """

    # 插件基本信息
    plugin_name = "网盘资源订阅 [已废弃]"
    plugin_desc = "⚠️ 此插件已废弃，请改用「115网盘助手」插件（集成 Cookie 配置 + 订阅管理 + 详情页），旧配置会自动迁移。"
    plugin_version = "1.2"
    plugin_order = 99

    # 私有属性
    _enabled = False
    _interval_minutes = 60
    _notify = True
    _cloud_types = ["115"]
    _quality_keywords = []
    _exclude_keywords = ["预告", "花絮", "CAM", "枪版"]
    _subscriptions: List[dict] = []

    # 视频文件扩展名
    _video_extensions = {
        ".mkv", ".mp4", ".avi", ".wmv", ".flv", ".mov", ".ts", ".m2ts",
        ".rmvb", ".rm", ".mpg", ".mpeg", ".vob",
    }

    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        if config and config.get("enabled"):
            logger.warning(
                "⚠️ [网盘资源订阅] 此插件已废弃！请改用「115网盘助手」(Net115Helper) 插件。"
                "\n   新插件集成了 Cookie 配置、分类目录、订阅管理和详情页，旧配置会自动迁移。"
                "\n   请到 设置 → 插件 中启用「115网盘助手」，然后禁用本插件。"
            )
        if config:
            self._enabled = config.get("enabled", False)
            self._interval_minutes = config.get("interval_minutes", 60)
            self._notify = config.get("notify", True)
            cloud_types = config.get("cloud_types", "115")
            if isinstance(cloud_types, str):
                self._cloud_types = [t.strip() for t in cloud_types.split(",") if t.strip()]
            else:
                self._cloud_types = cloud_types or ["115"]
            quality_kw = config.get("quality_keywords", "")
            if isinstance(quality_kw, str):
                self._quality_keywords = [k.strip() for k in quality_kw.split(",") if k.strip()]
            else:
                self._quality_keywords = quality_kw or []
            exclude_kw = config.get("exclude_keywords", "预告,花絮,CAM,枪版")
            if isinstance(exclude_kw, str):
                self._exclude_keywords = [k.strip() for k in exclude_kw.split(",") if k.strip()]
            else:
                self._exclude_keywords = exclude_kw or []
            subs = config.get("subscriptions", "[]")
            if isinstance(subs, str):
                try:
                    self._subscriptions = json.loads(subs) if subs.strip() else []
                except json.JSONDecodeError:
                    self._subscriptions = []
            else:
                self._subscriptions = subs or []

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务"""
        if not self._enabled:
            return []
        return [{
            "id": "pansou_subscribe_check",
            "name": "网盘订阅检查",
            "trigger": "interval",
            "func": self.check_subscriptions,
            "kwargs": {
                "minutes": self._interval_minutes,
            }
        }]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """插件配置页面"""
        return [
            {
                "component": "VForm",
                "content": [
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
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval_minutes",
                                            "label": "检查间隔（分钟）",
                                            "type": "number",
                                            "placeholder": "60",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cloud_types",
                                            "label": "网盘类型（逗号分隔）",
                                            "placeholder": "115,aliyun,quark",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "quality_keywords",
                                            "label": "质量关键词（逗号分隔，为空不过滤）",
                                            "placeholder": "4K,2160p,HDR,蓝光",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exclude_keywords",
                                            "label": "排除关键词（逗号分隔）",
                                            "placeholder": "预告,花絮,CAM,枪版",
                                        }
                                    }
                                ]
                            },
                        ]
                    },
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
                                            "model": "subscriptions",
                                            "label": "订阅列表（JSON 格式）",
                                            "placeholder": '[{"title": "剑来", "tmdb_id": 93740, "media_type": "电视剧", "season": 2, "media_category": "ongoing"}]',
                                            "rows": 8,
                                            "hint": "media_category: ongoing=连载剧, archive=老剧。也可通过 TG 智能体添加订阅。",
                                            "persistent-hint": True,
                                        }
                                    }
                                ]
                            },
                        ]
                    },
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
                                            "title": "使用前提",
                                            "text": "需要先配置好以下内容："
                                                    "\n1. 「115网盘设置」插件中的 Cookie 和临时目录 CID"
                                                    "\n2. 环境变量 PANSOU_URL（PanSou 搜索服务地址）"
                                                    "\n\n插件会按设定间隔自动检查，发现缺失集数后自动转存到对应临时目录。",
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
            "notify": True,
            "interval_minutes": 60,
            "cloud_types": "115",
            "quality_keywords": "",
            "exclude_keywords": "预告,花絮,CAM,枪版",
            "subscriptions": "[]",
        }

    def get_page(self) -> Optional[List[dict]]:
        return None

    def stop_service(self):
        pass

    # ============ 115 配置读取 ============

    @staticmethod
    def _get_115_config() -> dict:
        """从 115网盘设置 插件或环境变量读取配置"""
        try:
            plugin_config = SystemConfigOper().get("plugin.Net115Config")
            if plugin_config and isinstance(plugin_config, dict):
                if plugin_config.get("enabled") and plugin_config.get("cookies"):
                    return {
                        "cookies": plugin_config["cookies"].strip(),
                        "default_cid": plugin_config.get("default_cid", "0") or "0",
                        "ongoing_cid": plugin_config.get("ongoing_staging_cid", "") or "",
                        "archive_cid": plugin_config.get("archive_staging_cid", "") or "",
                    }
        except Exception as e:
            logger.debug(f"读取 115 插件配置失败: {e}")

        if hasattr(settings, "U115_COOKIES") and settings.U115_COOKIES:
            return {
                "cookies": settings.U115_COOKIES.strip(),
                "default_cid": getattr(settings, "DEFAULT_115_CID", "0") or "0",
                "ongoing_cid": "",
                "archive_cid": "",
            }

        return {"cookies": "", "default_cid": "0", "ongoing_cid": "", "archive_cid": ""}

    # ============ 核心逻辑 ============

    def check_subscriptions(self):
        """定时任务：检查所有网盘订阅"""
        if not self._enabled:
            return

        pansou_url = getattr(settings, "PANSOU_URL", None)
        if not pansou_url:
            logger.warning("【网盘订阅】未配置 PANSOU_URL，跳过检查")
            return

        config_115 = self._get_115_config()
        if not config_115["cookies"]:
            logger.warning("【网盘订阅】未配置 115 Cookie（请在「115网盘设置」插件中配置），跳过检查")
            return

        subscriptions = self._load_subscriptions()
        if not subscriptions:
            logger.info("【网盘订阅】没有活跃的订阅")
            return

        logger.info(f"【网盘订阅】开始检查 {len(subscriptions)} 个订阅...")

        completed = []
        for i, sub in enumerate(subscriptions):
            try:
                # 订阅之间加延迟，避免 115 API 限速
                if i > 0:
                    time.sleep(3)
                self._process_subscription(sub, config_115)
            except Exception as e:
                logger.error(f"【网盘订阅】处理订阅 {sub.get('title')} 失败: {e}", exc_info=True)

            # 检查是否已全集完结，标记自动取消
            if sub.get("auto_completed"):
                completed.append(sub.get("title", ""))

        # 移除已完结的订阅
        if completed:
            subscriptions[:] = [s for s in subscriptions if not s.get("auto_completed")]
            logger.info(f"【网盘订阅】自动取消已完结的订阅: {', '.join(completed)}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.MediaServer,
                    title="网盘订阅 - 自动完结",
                    text=f"以下订阅已全集入库，自动取消:\n{', '.join(completed)}",
                )

        # 保存更新后的订阅状态
        self._save_subscriptions(subscriptions)
        logger.info("【网盘订阅】检查完成")

    def _process_subscription(self, sub: dict, config_115: dict):
        """处理单个订阅"""
        title = sub.get("title", "")
        tmdb_id = sub.get("tmdb_id")
        media_type_str = sub.get("media_type", "电视剧")
        season = sub.get("season")
        search_keyword = sub.get("search_keyword", title)
        media_category = sub.get("media_category", "ongoing")

        logger.info(f"【网盘订阅】检查: {title} (TMDB: {tmdb_id}, 季: {season})")

        # 1. 获取媒体库中已有的集数
        existing_episodes = self._get_existing_episodes(title, tmdb_id, media_type_str, season)
        logger.info(f"【网盘订阅】{title} 已有集数: {existing_episodes}")

        # 2. 搜索 PanSou
        search_results = self._search_pansou(search_keyword)
        if not search_results:
            logger.info(f"【网盘订阅】{title} 未搜索到网盘资源")
            return

        logger.info(f"【网盘订阅】{title} PanSou 搜到 {len(search_results)} 个结果")

        # 3. 确定目标 CID
        if media_category == "archive" and config_115.get("archive_cid"):
            target_cid = config_115["archive_cid"]
        elif media_category == "ongoing" and config_115.get("ongoing_cid"):
            target_cid = config_115["ongoing_cid"]
        else:
            target_cid = config_115.get("default_cid", "0")

        cookies = config_115["cookies"]
        logger.info(f"【网盘订阅】{title} 目标 CID: {target_cid}, 类别: {media_category}")

        # 4. 遍历搜索结果，找到包含缺失集数的 115 分享
        for idx, result in enumerate(search_results[:5]):
            # 每个分享之间加延迟，避免 115 API 限速
            if idx > 0:
                time.sleep(1)
            share_url = result.get("url", "")
            share_password = result.get("password", "")
            share_note = result.get("note", "")[:80]

            if not share_url:
                continue

            # 只处理 115 分享链接
            if "115.com" not in share_url and "anxia.com" not in share_url and "115cdn.com" not in share_url:
                logger.info(f"【网盘订阅】{title} 跳过非115链接: {share_url[:60]}")
                continue

            try:
                # 解析分享码
                share_code = share_url
                receive_code = share_password
                match = re.search(r"(?:115|anxia|115cdn)\.com/s/(\w+)", share_url)
                if match:
                    share_code = match.group(1)
                    pwd_match = re.search(r"[?&]password=(\w+)", share_url)
                    if pwd_match and not share_password:
                        receive_code = pwd_match.group(1)

                logger.info(f"【网盘订阅】{title} [{idx+1}/5] 尝试分享: {share_code} ({share_note})")

                # 5. 获取分享内容
                share_files = self._list_share_files_115(share_code, receive_code, cookies)
                if not share_files:
                    logger.info(f"【网盘订阅】{title} 分享 {share_code} 获取文件列表为空")
                    continue

                logger.info(f"【网盘订阅】{title} 分享 {share_code} 包含 {len(share_files)} 个文件/文件夹")

                # 6. 解析集数信息
                parsed_files = self._parse_files(share_files)
                logger.info(
                    f"【网盘订阅】{title} 解析后 {len(parsed_files)} 个有效文件, "
                    f"集数: {[f.get('episode') for f in parsed_files[:10]]}"
                )

                # 7. 筛选缺失的集数
                missing_files = self._find_missing_episodes(
                    parsed_files, season, existing_episodes
                )

                if not missing_files:
                    logger.info(f"【网盘订阅】{title} 分享 {share_code} 没有缺失集数")
                    continue

                logger.info(
                    f"【网盘订阅】{title} 发现 {len(missing_files)} 个缺失文件: "
                    f"{[f.get('name','')[:40] for f in missing_files[:5]]}"
                )

                # 8. 转存缺失的文件
                file_ids = [f["file_id"] for f in missing_files]
                success = self._save_share_115(share_code, receive_code, cookies, file_ids, target_cid)

                if not success:
                    logger.warning(f"【网盘订阅】{title} 转存失败 (115 API 返回 false)")
                    continue

                if success:
                    episode_list = []
                    for f in missing_files:
                        if f.get("episode"):
                            episode_list.append(f"E{f['episode']:02d}")
                    ep_desc = ", ".join(episode_list) if episode_list else f"{len(missing_files)} 个文件"

                    msg = f"【网盘订阅】{title}"
                    if season:
                        msg += f" 第{season}季"
                    msg += f" 发现并转存了新资源: {ep_desc}"
                    logger.info(msg)

                    # 更新最后检查时间
                    sub["last_found"] = datetime.now().isoformat()
                    sub["last_found_episodes"] = [f.get("episode") for f in missing_files if f.get("episode")]

                    # 发送通知
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.MediaServer,
                            title=f"网盘订阅 - {title}",
                            text=f"发现并转存了新资源: {ep_desc}\n来源: {result.get('note', share_url)}",
                        )

                    break  # 找到有效的分享就够了

            except Exception as e:
                logger.warning(f"【网盘订阅】处理分享 {share_url} 失败: {e}")
                continue

        # === 自动完结检测 ===
        # 如果搜到的分享描述里有"完结"/"全XX集"，且当前已有集数覆盖了全部，自动取消订阅
        if existing_episodes and media_type_str != "电影":
            try:
                total_episodes = self._detect_total_episodes(search_results, title)
                if total_episodes and len(existing_episodes) >= total_episodes:
                    logger.info(
                        f"【网盘订阅】{title} 已全集入库 ({len(existing_episodes)}/{total_episodes} 集)，"
                        f"标记自动取消"
                    )
                    sub["auto_completed"] = True
            except Exception:
                pass

    def _detect_total_episodes(self, search_results: List[dict], title: str) -> Optional[int]:
        """从搜索结果的描述中检测总集数"""
        for result in search_results[:10]:
            note = result.get("note", "")
            # 匹配 "全48集"、"共48集"、"完结 48集" 等
            m = re.search(r"(?:全|共)\s*(\d+)\s*集", note)
            if m:
                return int(m.group(1))
            # 匹配 "[完结]" + 集数
            if "完结" in note or "完结" in note:
                m2 = re.search(r"(\d+)\s*集", note)
                if m2:
                    return int(m2.group(1))
        return None

    def _get_existing_episodes(self, title: str, tmdb_id: Optional[int],
                               media_type_str: str, season: Optional[int]) -> List[int]:
        """获取媒体库中已有的集数"""
        try:
            media_chain = MediaChain()

            meta = ParseMeta(title)
            if season:
                meta.begin_season = season
            if media_type_str == "电影":
                meta.type = MediaType.MOVIE
            else:
                meta.type = MediaType.TV

            mediainfo = media_chain.recognize_by_meta(meta)
            if not mediainfo:
                return []

            if tmdb_id and mediainfo.tmdb_id != tmdb_id:
                mediainfo.tmdb_id = tmdb_id

            existsinfo = media_chain.media_exists(mediainfo)
            if not existsinfo or not existsinfo.seasons:
                return []

            target_season = season or 1
            return existsinfo.seasons.get(target_season, [])

        except Exception as e:
            logger.warning(f"【网盘订阅】获取已有集数失败: {e}")
            return []

    def _search_pansou(self, keyword: str) -> List[dict]:
        """搜索 PanSou"""
        pansou_url = getattr(settings, "PANSOU_URL", "")
        if not pansou_url:
            logger.warning("【网盘订阅】未配置 PANSOU_URL")
            return []
        pansou_url = pansou_url.rstrip("/")
        headers = {"Content-Type": "application/json"}

        # 认证
        pansou_auth_user = getattr(settings, "PANSOU_AUTH_USER", None)
        pansou_auth_pass = getattr(settings, "PANSOU_AUTH_PASS", None)
        if pansou_auth_user and pansou_auth_pass:
            try:
                with httpx.Client(timeout=10.0) as client:
                    auth_resp = client.post(
                        f"{pansou_url}/api/auth/login",
                        json={
                            "username": pansou_auth_user,
                            "password": pansou_auth_pass,
                        },
                    )
                    token = auth_resp.json().get("token")
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
            except Exception as e:
                logger.warning(f"【网盘订阅】PanSou 认证失败: {e}")

        search_body = {
            "kw": keyword,
            "res": "merge",
            "cloud_types": self._cloud_types,
        }

        filter_config = {}
        if self._quality_keywords:
            filter_config["include"] = self._quality_keywords
        if self._exclude_keywords:
            filter_config["exclude"] = self._exclude_keywords
        if filter_config:
            search_body["filter"] = filter_config

        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    f"{pansou_url}/api/search",
                    headers=headers,
                    json=search_body,
                )
                if resp.status_code != 200:
                    logger.warning(f"【网盘订阅】PanSou 搜索失败: HTTP {resp.status_code}")
                    return []

                result = resp.json()
                # PanSou 返回: {"code":0,"data":{"total":N,"merged_by_type":{...}}}
                data = result.get("data", {})
                merged = data.get("merged_by_type", {})

                results = []
                for cloud_type, links in merged.items():
                    for link in (links or []):
                        results.append(link)

                return results

        except Exception as e:
            logger.error(f"【网盘订阅】PanSou 搜索异常: {e}")
            return []

    def _list_share_files_115(self, share_code: str, receive_code: str,
                              cookies: str, cid: str = "0", depth: int = 0) -> List[dict]:
        """
        直接调用 115 Web API 获取分享文件列表。
        如果顶层只有文件夹，自动进入文件夹列出内部文件（最多递归 2 层）。
        """
        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies

        try:
            with httpx.Client(headers=headers, follow_redirects=True, timeout=30.0) as client:
                resp = client.get(
                    "https://webapi.115.com/share/snap",
                    params={
                        "share_code": share_code,
                        "receive_code": receive_code,
                        "cid": cid,
                        "limit": 200,
                        "offset": 0,
                    },
                )
                data = resp.json()

            if not data.get("state"):
                logger.info(f"【网盘订阅】115 share/snap 返回 state=false, cid={cid}")
                return []

            share_data = data.get("data", {})
            file_list = share_data.get("list", [])

            files = []
            for f in file_list:
                # 115 API: 文件有 fid，文件夹没有 fid 但有 cid
                has_fid = "fid" in f
                is_dir = not has_fid
                name = f.get("n", f.get("fn", ""))

                if is_dir:
                    folder_cid = str(f.get("cid", ""))
                    if folder_cid and depth < 2:
                        # 进入文件夹递归获取内部文件，加延迟避免限速
                        time.sleep(0.5)
                        logger.info(f"【网盘订阅】进入文件夹: {name} (cid={folder_cid})")
                        sub_files = self._list_share_files_115(
                            share_code, receive_code, cookies,
                            cid=folder_cid, depth=depth + 1
                        )
                        files.extend(sub_files)
                    else:
                        # 超过递归深度，把文件夹整体作为一个条目
                        files.append({
                            "file_id": folder_cid or str(f.get("cid", "")),
                            "name": name,
                            "size": 0,
                            "is_dir": True,
                        })
                else:
                    file_id = str(f.get("fid", ""))
                    files.append({
                        "file_id": file_id,
                        "name": name,
                        "size": int(f.get("s", 0)),
                        "is_dir": False,
                    })

            return files

        except Exception as e:
            logger.warning(f"【网盘订阅】获取 115 分享内容失败: {e}")
            return []

    def _save_share_115(self, share_code: str, receive_code: str,
                        cookies: str, file_ids: List[str], cid: str) -> bool:
        """直接调用 115 Web API 转存"""
        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies

        try:
            with httpx.Client(headers=headers, follow_redirects=True, timeout=60.0) as client:
                # 如果文件太多，分批转存（每批最多 5 个），避免 API 限制
                batch_size = 5
                all_success = True
                for i in range(0, len(file_ids), batch_size):
                    batch = file_ids[i:i + batch_size]
                    if i > 0:
                        time.sleep(1)  # 批次之间加延迟
                    resp = client.post(
                        "https://webapi.115.com/share/receive",
                        data={
                            "share_code": share_code,
                            "receive_code": receive_code,
                            "file_id": ",".join(batch),
                            "cid": cid,
                        },
                    )
                    result = resp.json()
                    if not result.get("state", False):
                        error_msg = result.get("error", result.get("message", result.get("msg", str(result))))
                        errno = result.get("errno", result.get("errNo", ""))
                        # errno 4200045 = "文件已接收" → 视为成功（文件已在账号中）
                        if str(errno) == "4200045":
                            logger.info(
                                f"【网盘订阅】115 转存批次: 文件已接收 (跳过), "
                                f"share={share_code}, 文件数={len(batch)}"
                            )
                            # 不标记为失败，视为成功
                        else:
                            logger.warning(
                                f"【网盘订阅】115 转存批次失败: errno={errno}, error={error_msg}, "
                                f"share={share_code}, 文件数={len(batch)}"
                            )
                            all_success = False
                    else:
                        logger.info(f"【网盘订阅】115 转存批次成功: {len(batch)} 个文件")

                return all_success

        except Exception as e:
            logger.error(f"【网盘订阅】115 转存失败: {e}")
            return False

    def _parse_files(self, files: List[dict]) -> List[dict]:
        """解析文件的季集信息"""
        parsed = []
        for f in files:
            name = f.get("name", "")
            file_id = f.get("file_id", "")
            size = f.get("size", 0)
            is_dir = f.get("is_dir", False)

            ext = ""
            if "." in name:
                ext = "." + name.rsplit(".", 1)[-1].lower()
            is_video = ext in self._video_extensions

            if not is_video and not is_dir:
                continue

            entry = {
                "file_id": file_id,
                "name": name,
                "size": size,
                "is_dir": is_dir,
                "season": None,
                "episode": None,
                "episode_list": [],
            }

            try:
                meta = ParseMeta(name)
                entry["season"] = meta.begin_season
                entry["episode"] = meta.begin_episode
                if hasattr(meta, 'episode_list') and meta.episode_list:
                    entry["episode_list"] = meta.episode_list
                elif meta.begin_episode:
                    end_ep = meta.end_episode or meta.begin_episode
                    entry["episode_list"] = list(range(meta.begin_episode, end_ep + 1))
                if entry["episode"] and not entry["season"]:
                    entry["season"] = 1
            except Exception:
                pass

            parsed.append(entry)

        return parsed

    def _find_missing_episodes(self, parsed_files: List[dict],
                               target_season: Optional[int],
                               existing_episodes: List[int]) -> List[dict]:
        """筛选出缺失的集数对应的文件，同一集只保留一个（优先 mp4）"""
        missing = []
        existing_set = set(existing_episodes)

        for f in parsed_files:
            file_season = f.get("season")
            file_episodes = f.get("episode_list", [])

            if target_season is not None and file_season != target_season:
                continue

            if file_episodes:
                if any(ep not in existing_set for ep in file_episodes):
                    missing.append(f)
            elif f.get("is_dir"):
                missing.append(f)

        # 同一集去重：优先 mp4 > mkv > 其他，保留文件最大的
        if missing:
            ep_best = {}
            _format_priority = {".mp4": 3, ".mkv": 2, ".ts": 1}
            for f in missing:
                ep = f.get("episode")
                if not ep:
                    # 没有集数信息的（如文件夹）直接保留
                    ep_best.setdefault(f"_dir_{f.get('file_id')}", f)
                    continue
                name = f.get("name", "").lower()
                ext = ""
                if "." in name:
                    ext = "." + name.rsplit(".", 1)[-1]
                priority = _format_priority.get(ext, 0)
                size = f.get("size", 0)

                key = (f.get("season"), ep)
                if key not in ep_best:
                    ep_best[key] = (priority, size, f)
                else:
                    old_pri, old_size, _ = ep_best[key]
                    # 优先级高的胜出；优先级相同取文件大的
                    if priority > old_pri or (priority == old_pri and size > old_size):
                        ep_best[key] = (priority, size, f)

            # 提取去重后的文件列表
            deduped = []
            for v in ep_best.values():
                if isinstance(v, tuple):
                    deduped.append(v[2])
                else:
                    deduped.append(v)
            if len(deduped) < len(missing):
                logger.info(f"【网盘订阅】去重: {len(missing)} → {len(deduped)} 个文件 (同集多格式只保留最优)")
            missing = deduped

        return missing

    # ============ 订阅数据管理 ============

    def _load_subscriptions(self) -> List[dict]:
        """加载订阅列表"""
        data = self.get_data("subscriptions")
        if isinstance(data, list):
            return data
        if self._subscriptions:
            return self._subscriptions
        return []

    def _save_subscriptions(self, subscriptions: List[dict]):
        """保存订阅列表"""
        self._subscriptions = subscriptions
        # 保存到 plugin data store
        self.save_data("subscriptions", subscriptions)
        # 同步更新 plugin config，让 UI 表单也能显示
        config = self.get_config() or {}
        config["subscriptions"] = json.dumps(subscriptions, ensure_ascii=False)
        self.update_config(config)

    def add_subscription(self, sub: dict) -> bool:
        """添加订阅"""
        subs = self._load_subscriptions()
        for s in subs:
            if s.get("tmdb_id") == sub.get("tmdb_id") and s.get("season") == sub.get("season"):
                return False
        sub["created"] = datetime.now().isoformat()
        subs.append(sub)
        self._save_subscriptions(subs)
        return True

    def remove_subscription(self, tmdb_id: int, season: Optional[int] = None) -> bool:
        """删除订阅"""
        subs = self._load_subscriptions()
        original_len = len(subs)
        subs = [s for s in subs if not (
            s.get("tmdb_id") == tmdb_id and
            (season is None or s.get("season") == season)
        )]
        if len(subs) < original_len:
            self._save_subscriptions(subs)
            return True
        return False

    def list_subscriptions(self) -> List[dict]:
        """列出所有订阅"""
        return self._load_subscriptions()
