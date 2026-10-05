"""
115网盘助手 — 合并版插件
将原 Net115Config（Cookie/CID 配置）和 PanSouSubscribe（订阅逻辑）合二为一。
提供：可视化配置表单、订阅管理详情页（含进度条）、自定义 API、定时任务、扫码登录。
"""

import glob
import json
import os
import re
import sqlite3
import time
import threading as _threading_mod
import unicodedata
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Body
from fastapi.responses import HTMLResponse
import httpx

from app.core.config import settings
from app.core.metainfo import MetaInfo as ParseMeta
from app.chain.media import MediaChain
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import Notification, NotificationType, MediaType, TransferInfo
from app.schemas.types import ContentType

# 115 Web API 请求头
_115_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://115.com/",
    "Origin": "https://115.com",
}

# ── 导入客户端模块 ──────────────────────────────────────
try:
    from .clients import P115ClientManager, QrcodeState, get_client_manager, COOKIES_PATH
    _CLIENT_AVAILABLE = True
except ImportError as e:
    P115ClientManager = None  # type: ignore
    QrcodeState = None  # type: ignore
    get_client_manager = None  # type: ignore
    COOKIES_PATH = "/config/.p115_cookies"
    _CLIENT_AVAILABLE = False
    logger.debug(f"【115助手】客户端模块导入失败（部分功能不可用）: {e}")


class Net115Helper(_PluginBase):
    """
    115 网盘助手（合并版）
    集成 Cookie/CID 配置 + 网盘资源订阅，一个插件搞定所有 115 相关功能。
    """

    # ───────── 插件元信息 ─────────
    plugin_name = "115网盘助手"
    plugin_desc = (
        "一站式管理 115 网盘：Cookie 配置、分类临时目录、"
        "网盘资源订阅（定期搜索 PanSou → 比对缺失集数 → 自动转存）。"
    )
    plugin_version = "2.2"
    plugin_order = 29
    plugin_icon = "https://115.com/favicon.ico"

    # ───────── 私有属性（来自 Net115Config）─────────
    _enabled: bool = False
    _cookies: str = ""
    _cookie_source: str = "auto"  # auto / plugin / mp / p115client
    _active_cookie_source: str = "未检测"
    _cookie_status: dict = {}
    _default_cid: str = "0"
    _movie_staging_cid: str = ""
    _old_movie_staging_cid: str = ""
    _ongoing_staging_cid: str = ""
    _archive_staging_cid: str = ""

    # ───────── 私有属性（来自 PanSouSubscribe）─────────
    _notify: bool = True
    _pansou_url: str = ""  # PanSou 搜索服务地址
    _pansou_auth_user: str = ""  # PanSou 认证用户名
    _pansou_auth_pass: str = ""  # PanSou 认证密码
    _tmdb_api_key: str = ""
    _tmdb_api_domain: str = "api.themoviedb.org"
    _movie_year_threshold: int = 2025
    _interval_minutes: int = 60
    _interval_unit: str = "minutes"  # minutes / hours / days
    _batch_check_enabled: bool = True
    _batch_size: int = 3
    _batch_interval_minutes: int = 25
    _batch_direct_115_limit: int = 0
    _batch_cd2_copy_limit: int = 3
    _batch_fallback_cloud_limit: int = 1
    _rate_limit_cooldown_minutes: int = 60
    # 追平已播出集数后跳过搜索（省风控/复制）
    _airing_skip_enabled: bool = True
    _airing_window_start_hour: int = 18
    _airing_idle_probe_hours: int = 6
    _rate_limit_escalation_enabled: bool = True
    _notice_same_title_window_minutes: int = 360
    _notice_same_title_max: int = 2
    _notice_global_window_minutes: int = 60
    _notice_global_max: int = 8
    _cloud_types: List[str] = ["115"]
    _quality_keywords: List[str] = []
    _exclude_keywords: List[str] = ["预告", "花絮", "CAM", "枪版"]
    _subscriptions: List[dict] = []
    _subscriptions_lock: _threading_mod.Lock = _threading_mod.Lock()

    # ───────── Token 过期提醒节流 ─────────
    # key: token 来源标识  value: 上次发送提醒的 Unix 时间戳
    _token_warn_times: dict = {}
    _TOKEN_WARN_INTERVAL = 4 * 3600  # 同一来源最多每 4 小时提醒一次
    _PENDING_COPY_TTL_SECONDS = 50 * 60  # 待入库 pending 最多保留 50 分钟
    _115_COOLDOWN_SECONDS = 3600
    _FALLBACK_COPY_GUARD_SECONDS = 90 * 60
    _FALLBACK_COPY_SUBMIT_LIMIT_PER_RUN = 3
    _PRECISION_DIRECT_115_LIMIT_PER_RUN = 1

    # ───────── CD2 降级配置 ─────────
    _fallback_enabled: bool = False
    _fallback_clouds: List[str] = []          # ["阿里云盘", "123云盘", "夸克", "百度网盘"]
    _fallback_cloud_priority: List[str] = []  # 执行顺序；只对已勾选的云盘生效
    _FALLBACK_CLOUD_DEFAULT_PRIORITY = ["阿里云盘", "百度网盘", "123云盘", "夸克"]
    _cd2_host: str = ""
    _cd2_port: int = 19798
    _cd2_token: str = ""
    _cd2_username: str = ""
    _cd2_password: str = ""
    _cd2_totp_code: str = ""
    _aliyun_staging_path: str = "/阿里云盘/MP临时转存"
    _123_staging_path: str = "/123云盘/MP临时转存"
    _quark_staging_path: str = "/夸克网盘/MP临时转存"
    _baidu_staging_path: str = "/百度网盘/MP临时转存"
    _aliyun_token: str = ""
    _quark_cookie: str = ""
    _baidu_cookie: str = ""
    _cloud_auth_status: dict = {}
    _cloud_auth_last_warn: dict = {}
    _runtime_state: dict = {}
    _existing_episodes_cache: dict = {}
    _EXISTING_EPISODES_CACHE_TTL = 60
    _cd2_target_movie_path: str = ""
    _cd2_target_old_movie_path: str = ""
    _cd2_target_ongoing_path: str = ""
    _cd2_target_archive_path: str = ""
    # 115 在本容器内的实际挂载根目录（留空则按常见 CloudDrive 挂载位置自动探测）
    _cd2_local_mount_115: str = ""
    # 自动探测用的通用候选位置，仅含通用约定路径，不写死具体部署环境
    _CD2_LOCAL_MOUNT_GLOBS = (
        "/volume*/CloudDrive/CloudDrive/115",
        "/volume*/CloudDrive/115",
        "/CloudDrive/CloudDrive/115",
        "/CloudDrive/115",
        "/mnt/CloudDrive/115",
    )

    # ───────── 插件闭环整理配置 ─────────
    _closed_loop_organize_enabled: bool = False
    _closed_loop_scrape_metadata: bool = True
    _closed_loop_movie_library_path: str = ""
    _closed_loop_old_movie_library_path: str = ""
    _closed_loop_ongoing_library_path: str = ""
    _closed_loop_archive_library_path: str = ""
    _CLOSED_LOOP_HISTORY_KEY = "closed_loop_organized_v1"

    # 视频文件扩展名
    _video_extensions = {
        ".mkv", ".mp4", ".avi", ".wmv", ".flv", ".mov", ".ts", ".m2ts",
        ".rmvb", ".rm", ".mpg", ".mpeg", ".vob", ".strm",
    }
    _ARCHIVE_EXTENSIONS = {
        ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso",
    }

    # ───────── CD2 巡查同步配置 ─────────
    _cd2_watch_enabled: bool = False
    _cd2_watch_interval_minutes: int = 120
    _cd2_watch_rules: List[dict] = []   # [{id, enabled, name, source_path, dest_115_path}]
    _CD2_WATCH_NOTICE_TTL_SECONDS = 24 * 3600
    _PLUGIN_NOTICE_SENT_KEY = "plugin_notice_sent_v1"
    _PLUGIN_NOTICE_SENT_TTL_SECONDS = 7 * 24 * 3600
    _PLUGIN_NOTICE_RATE_KEY = "plugin_notice_rate_v1"
    _CD2_WATCH_INDEX_DATA_KEY = "cd2_watch_index_v1"
    _CD2_WATCH_ARCHIVE_LIMIT = 200

    # ================================================================
    #  初始化 / 状态
    # ================================================================

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置。
        首次启动时自动迁移旧 Net115Config / PanSouSubscribe 数据。
        """
        if config is None:
            config = {}
        config_changed = False

        # ---- 旧插件数据自动迁移 ----
        if not config:
            config = self._migrate_old_plugins()

        # ---- 115 配置 ----
        self._enabled = config.get("enabled", False)
        self._cookies = config.get("cookies", "")
        self._cookie_source = str(config.get("cookie_source", "auto") or "auto").strip()
        if self._cookie_source not in ("auto", "plugin", "mp", "p115client"):
            self._cookie_source = "auto"
        self._default_cid = config.get("default_cid", "0")
        self._movie_staging_cid = config.get("movie_staging_cid", "")
        self._old_movie_staging_cid = config.get("old_movie_staging_cid", "")
        self._ongoing_staging_cid = config.get("ongoing_staging_cid", "")
        self._archive_staging_cid = config.get("archive_staging_cid", "")

        # ---- PanSou 配置 ----
        # 优先从插件配置读取，如果没有则从 settings 或环境变量读取
        self._pansou_url = config.get("pansou_url", "")
        if not self._pansou_url:
            self._pansou_url = getattr(settings, "PANSOU_URL", "") or os.environ.get("PANSOU_URL", "")
        self._pansou_auth_user = config.get("pansou_auth_user", "")
        if not self._pansou_auth_user:
            self._pansou_auth_user = getattr(settings, "PANSOU_AUTH_USER", "")
        self._pansou_auth_pass = config.get("pansou_auth_pass", "")
        if not self._pansou_auth_pass:
            self._pansou_auth_pass = getattr(settings, "PANSOU_AUTH_PASS", "")

        # ---- TMDB 配置 ----
        # 留空时使用 MoviePilot 全局 TMDB 配置；插件不再内置或写入共享 API Key。
        self._tmdb_api_key = str(config.get("tmdb_api_key") or "").strip()

        tmdb_api_domain = self._normalize_tmdb_domain(
            config.get("tmdb_api_domain")
            or getattr(settings, "TMDB_API_DOMAIN", "")
            or "api.themoviedb.org"
        )
        self._tmdb_api_domain = tmdb_api_domain
        if config.get("tmdb_api_domain") != tmdb_api_domain:
            config["tmdb_api_domain"] = tmdb_api_domain
            config_changed = True
        self._apply_tmdb_settings()

        # ---- 新/老电影分界年份 ----
        try:
            self._movie_year_threshold = int(config.get("movie_year_threshold", 2025))
        except (TypeError, ValueError):
            self._movie_year_threshold = 2025

        # ---- 通知 ----
        notify_value = config.get("notify", True)
        self._notify = True if notify_value is None else bool(notify_value)
        try:
            self._notice_same_title_window_minutes = max(
                1, int(config.get("notice_same_title_window_minutes", 360))
            )
        except (TypeError, ValueError):
            self._notice_same_title_window_minutes = 360
        try:
            self._notice_same_title_max = max(1, int(config.get("notice_same_title_max", 2)))
        except (TypeError, ValueError):
            self._notice_same_title_max = 2
        try:
            self._notice_global_window_minutes = max(
                1, int(config.get("notice_global_window_minutes", 60))
            )
        except (TypeError, ValueError):
            self._notice_global_window_minutes = 60
        try:
            self._notice_global_max = max(1, int(config.get("notice_global_max", 8)))
        except (TypeError, ValueError):
            self._notice_global_max = 8
        # ---- token 过期提醒节流（实例级，不跨实例共享）----
        if not hasattr(self, "_token_warn_times") or not isinstance(self._token_warn_times, dict):
            self._token_warn_times = {}

        # ---- 订阅检查间隔 ----
        self._interval_unit = config.get("interval_unit", "minutes")
        raw_interval = config.get("interval_value", config.get("interval_minutes", 60))
        try:
            raw_interval = int(raw_interval)
        except (TypeError, ValueError):
            raw_interval = 60
        self._interval_minutes = self._to_minutes(raw_interval, self._interval_unit)
        batch_check_value = config.get("batch_check_enabled", True)
        self._batch_check_enabled = True if batch_check_value is None else bool(batch_check_value)
        try:
            self._batch_size = max(1, int(config.get("batch_size", 3)))
        except (TypeError, ValueError):
            self._batch_size = 3
        try:
            self._batch_interval_minutes = max(5, int(config.get("batch_interval_minutes", 25)))
        except (TypeError, ValueError):
            self._batch_interval_minutes = 25
        try:
            self._batch_direct_115_limit = max(0, int(config.get("batch_direct_115_limit", 0)))
        except (TypeError, ValueError):
            self._batch_direct_115_limit = 0
        try:
            self._batch_cd2_copy_limit = max(0, int(config.get("batch_cd2_copy_limit", 3)))
        except (TypeError, ValueError):
            self._batch_cd2_copy_limit = 3
        try:
            self._batch_fallback_cloud_limit = max(1, int(config.get("batch_fallback_cloud_limit", 1)))
        except (TypeError, ValueError):
            self._batch_fallback_cloud_limit = 1
        try:
            self._rate_limit_cooldown_minutes = max(10, int(config.get("rate_limit_cooldown_minutes", 60)))
        except (TypeError, ValueError):
            self._rate_limit_cooldown_minutes = 60
        self._rate_limit_escalation_enabled = bool(config.get("rate_limit_escalation_enabled", True))
        self._airing_skip_enabled = bool(config.get("airing_skip_enabled", True))
        try:
            self._airing_window_start_hour = min(23, max(0, int(config.get("airing_window_start_hour", 18))))
        except (TypeError, ValueError):
            self._airing_window_start_hour = 18
        try:
            self._airing_idle_probe_hours = max(0, int(config.get("airing_idle_probe_hours", 6)))
        except (TypeError, ValueError):
            self._airing_idle_probe_hours = 6
        self._115_COOLDOWN_SECONDS = self._rate_limit_cooldown_minutes * 60
        self._FALLBACK_COPY_SUBMIT_LIMIT_PER_RUN = self._batch_cd2_copy_limit

        # ---- 网盘类型 ----
        cloud_types = config.get("cloud_types", "115")
        if isinstance(cloud_types, str):
            self._cloud_types = [t.strip() for t in cloud_types.split(",") if t.strip()]
        else:
            self._cloud_types = cloud_types or ["115"]

        # ---- 质量关键词 ----
        quality_kw = config.get("quality_keywords", "")
        if isinstance(quality_kw, str):
            self._quality_keywords = [k.strip() for k in quality_kw.split(",") if k.strip()]
        else:
            self._quality_keywords = quality_kw or []

        # ---- 排除关键词 ----
        exclude_kw = config.get("exclude_keywords", "预告,花絮,CAM,枪版")
        if isinstance(exclude_kw, str):
            self._exclude_keywords = [k.strip() for k in exclude_kw.split(",") if k.strip()]
        else:
            self._exclude_keywords = exclude_kw or []

        # ---- 订阅列表 ----
        subs = config.get("subscriptions", "[]")
        if isinstance(subs, str):
            try:
                self._subscriptions = json.loads(subs) if subs.strip() else []
            except json.JSONDecodeError:
                self._subscriptions = []
        else:
            self._subscriptions = subs or []

        # ---- CD2 降级配置 ----
        self._fallback_enabled = config.get("fallback_enabled", False)
        fb_clouds = config.get("fallback_clouds", [])
        self._fallback_clouds = self._normalize_fallback_cloud_list(fb_clouds)
        priority_value = config.get("fallback_cloud_priority", "")
        self._fallback_cloud_priority = (
            self._normalize_fallback_cloud_list(priority_value)
            or list(self._FALLBACK_CLOUD_DEFAULT_PRIORITY)
        )
        if not priority_value:
            config["fallback_cloud_priority"] = self._format_fallback_cloud_order(
                self._fallback_cloud_priority
            )
            config_changed = True
        self._cd2_host = config.get("cd2_host", "")
        try:
            self._cd2_port = int(config.get("cd2_port", 19798))
        except (TypeError, ValueError):
            self._cd2_port = 19798
        self._cd2_token = str(config.get("cd2_token", "") or "").strip()
        if self._cd2_token.lower().startswith("bearer "):
            self._cd2_token = self._cd2_token[7:].strip()
        self._cd2_username = str(config.get("cd2_username", "") or "").strip()
        self._cd2_password = str(config.get("cd2_password", "") or "")
        self._cd2_totp_code = str(config.get("cd2_totp_code", "") or "").strip()
        self._aliyun_staging_path = config.get("aliyun_staging_path", "/阿里云盘/MP临时转存")
        self._123_staging_path = config.get("123_staging_path", "/123云盘/MP临时转存")
        self._quark_staging_path = config.get("quark_staging_path", "/夸克网盘/MP临时转存")
        self._baidu_staging_path = config.get("baidu_staging_path", "/百度网盘/MP临时转存")
        self._aliyun_token = str(config.get("aliyun_token", "") or "").strip()
        self._quark_cookie = str(config.get("quark_cookie", "") or "").strip()
        self._baidu_cookie = str(config.get("baidu_cookie", "") or "").strip()
        try:
            from .clients import AliyunClient, QuarkClient, BaiduClient
            if AliyunClient and hasattr(AliyunClient, "set_refresh_token"):
                AliyunClient.set_refresh_token(self._aliyun_token)
            if QuarkClient:
                QuarkClient.set_cookie(self._quark_cookie)
            if BaiduClient:
                BaiduClient.set_cookie(self._baidu_cookie)
        except Exception as e:
            logger.debug(f"【115助手】初始化备用云盘 Cookie 失败: {e}")
        self._cd2_target_movie_path = str(config.get("cd2_target_movie_path", "") or "").strip()
        self._cd2_target_old_movie_path = str(config.get("cd2_target_old_movie_path", "") or "").strip()
        self._cd2_target_ongoing_path = str(config.get("cd2_target_ongoing_path", "") or "").strip()
        self._cd2_target_archive_path = str(config.get("cd2_target_archive_path", "") or "").strip()
        self._cd2_local_mount_115 = str(config.get("cd2_local_mount_115", "") or "").strip()

        # ---- 插件闭环整理配置 ----
        closed_loop_value = config.get("closed_loop_organize_enabled", False)
        self._closed_loop_organize_enabled = False if closed_loop_value is None else bool(closed_loop_value)
        scrape_metadata_value = config.get("closed_loop_scrape_metadata", True)
        self._closed_loop_scrape_metadata = True if scrape_metadata_value is None else bool(scrape_metadata_value)
        self._closed_loop_movie_library_path = str(config.get("closed_loop_movie_library_path", "") or "").strip()
        self._closed_loop_old_movie_library_path = str(config.get("closed_loop_old_movie_library_path", "") or "").strip()
        self._closed_loop_ongoing_library_path = str(config.get("closed_loop_ongoing_library_path", "") or "").strip()
        self._closed_loop_archive_library_path = str(config.get("closed_loop_archive_library_path", "") or "").strip()

        # ---- CD2 巡查同步配置 ----
        self._cd2_watch_enabled = config.get("cd2_watch_enabled", False)
        try:
            self._cd2_watch_interval_minutes = max(30, int(config.get("cd2_watch_interval_minutes", 120)))
        except (TypeError, ValueError):
            self._cd2_watch_interval_minutes = 120
        watch_rules = config.get("cd2_watch_rules", "[]")
        if isinstance(watch_rules, str):
            try:
                self._cd2_watch_rules = json.loads(watch_rules) if watch_rules.strip() else []
            except json.JSONDecodeError:
                self._cd2_watch_rules = []
        else:
            self._cd2_watch_rules = watch_rules or []

        # ---- 快捷操作处理（保存配置时执行）----
        self._process_config_actions(config)
        if config_changed:
            self.update_config(config)

        # ---- 日志 ----
        if self._enabled:
            logger.info("115网盘助手已启用")

            # ── 如果已有扫码登录的 cookie 文件，同步到配置 ──
            if _CLIENT_AVAILABLE and not self._cookies:
                self._try_sync_cookies_from_p115client()

            if self._cookies:
                logger.info("  Cookie 已配置")
            else:
                logger.warning("  Cookie 为空，转存功能将不可用（可使用扫码登录）")
            if self._movie_staging_cid:
                logger.info(f"  电影临时目录 CID: {self._movie_staging_cid}")
            if self._old_movie_staging_cid:
                logger.info(f"  老电影临时目录 CID: {self._old_movie_staging_cid}")
            if self._ongoing_staging_cid:
                logger.info(f"  连载剧临时目录 CID: {self._ongoing_staging_cid}")
            if self._archive_staging_cid:
                logger.info(f"  老剧临时目录 CID: {self._archive_staging_cid}")
            logger.info(f"  新/老电影分界: {self._movie_year_threshold}年起算新电影")
            logger.info(f"  TMDB API: {self._mask_secret(self._tmdb_api_key)} @ {self._tmdb_api_domain}")

            # ── 启动后立即执行一次订阅检查 ──
            import threading
            threading.Thread(target=self.check_subscriptions, daemon=True).start()

    # ── 旧插件迁移 ──────────────────────────────────────

    def _migrate_old_plugins(self) -> dict:
        """
        首次启动时，把旧 Net115Config + PanSouSubscribe 的配置合并写入自身。
        返回合并后的 config dict。
        """
        merged: Dict[str, Any] = {}
        migrated_from = []
        sysconfig = SystemConfigOper()

        # 1) Net115Config
        try:
            old_115 = sysconfig.get("plugin.Net115Config")
            if old_115 and isinstance(old_115, dict) and old_115.get("cookies"):
                merged["enabled"] = old_115.get("enabled", False)
                merged["cookies"] = old_115.get("cookies", "")
                merged["default_cid"] = old_115.get("default_cid", "0")
                merged["movie_staging_cid"] = old_115.get("movie_staging_cid", "")
                merged["ongoing_staging_cid"] = old_115.get("ongoing_staging_cid", "")
                merged["archive_staging_cid"] = old_115.get("archive_staging_cid", "")
                migrated_from.append("Net115Config")
        except Exception as e:
            logger.debug(f"迁移 Net115Config 失败: {e}")

        # 2) PanSouSubscribe
        try:
            old_sub = sysconfig.get("plugin.PanSouSubscribe")
            if old_sub and isinstance(old_sub, dict):
                if old_sub.get("enabled"):
                    merged["enabled"] = True
                merged["notify"] = old_sub.get("notify", True)
                merged["interval_minutes"] = old_sub.get("interval_minutes", 60)
                merged["cloud_types"] = old_sub.get("cloud_types", "115")
                merged["quality_keywords"] = old_sub.get("quality_keywords", "")
                merged["exclude_keywords"] = old_sub.get("exclude_keywords", "预告,花絮,CAM,枪版")
                merged["subscriptions"] = old_sub.get("subscriptions", "[]")
                migrated_from.append("PanSouSubscribe")
        except Exception as e:
            logger.debug(f"迁移 PanSouSubscribe 失败: {e}")

        # 3) 迁移订阅数据 (plugin data store)
        try:
            from app.db.plugindata_oper import PluginDataOper
            pdo = PluginDataOper()
            old_subs_data = pdo.get_data("PanSouSubscribe", "subscriptions")
            if old_subs_data and isinstance(old_subs_data, list):
                self.save_data("subscriptions", old_subs_data)
                logger.info(f"  迁移订阅数据: {len(old_subs_data)} 条")
        except Exception as e:
            logger.debug(f"迁移订阅 data store 失败: {e}")

        if migrated_from:
            logger.info(f"115网盘助手: 已从旧插件迁移配置 ({', '.join(migrated_from)})")
            # 保存合并后的配置
            self.update_config(merged)

        return merged

    # ── 订阅分类辅助 ──────────────────────────────────────

    def _resolve_media_category(self, media_type: str, season: Optional[int] = None,
                                year: Optional[int] = None) -> str:
        """
        根据类型、年份自动判断 media_category。
        电影: ≥ threshold → 'movie', < threshold → 'movie_archive'
        电视剧: ≥ threshold → 'ongoing', < threshold → 'archive'
        未提供年份时电视剧默认 'ongoing'
        """
        # 兼容英文类型（TMDB 格式）
        _mt = media_type or ""
        if _mt.lower() in ("movie", "电影"):
            if year and year < self._movie_year_threshold:
                return "movie_archive"
            return "movie"
        if year and year < self._movie_year_threshold:
            return "archive"
        return "ongoing"

    # ── 配置表单快捷操作处理 ──────────────────────────────

    def _process_config_actions(self, config: dict):
        """
        处理配置表单中的快捷操作字段（添加/取消订阅）。
        操作完成后自动清空对应字段。
        """
        changed = False

        # ── 取消订阅 ──
        cancel_tmdb_raw = config.get("action_cancel_tmdb_id", "")
        if cancel_tmdb_raw:
            try:
                cancel_tmdb_id = int(cancel_tmdb_raw)
                cancel_season_raw = config.get("action_cancel_season", "")
                cancel_season = int(cancel_season_raw) if cancel_season_raw else None
                removed = self.remove_subscription(cancel_tmdb_id, cancel_season)
                if removed:
                    logger.info(f"【115助手】配置快捷操作: 已取消订阅 TMDB={cancel_tmdb_id}, 季={cancel_season}")
                else:
                    logger.warning(f"【115助手】配置快捷操作: 未找到订阅 TMDB={cancel_tmdb_id}, 季={cancel_season}")
            except (ValueError, TypeError) as e:
                logger.warning(f"【115助手】取消订阅失败: TMDB ID 无效 '{cancel_tmdb_raw}': {e}")
            config["action_cancel_tmdb_id"] = ""
            config["action_cancel_season"] = ""
            changed = True

        # ── 添加订阅 ──
        add_title = config.get("action_add_title", "").strip()
        add_tmdb_raw = config.get("action_add_tmdb_id", "")
        if add_title and add_tmdb_raw:
            try:
                add_tmdb_id = int(add_tmdb_raw)
                _raw_add_type = (config.get("action_add_type", "电视剧") or "电视剧").lower()
                add_type = "电影" if _raw_add_type in ("movie", "电影") else "电视剧"
                add_season_raw = config.get("action_add_season", "")
                add_season = int(add_season_raw) if add_season_raw else None
                add_year_raw = config.get("action_add_year", "")
                add_year = int(add_year_raw) if add_year_raw else None

                media_category = self._resolve_media_category(add_type, add_season, add_year)

                sub = {
                    "title": add_title,
                    "tmdb_id": add_tmdb_id,
                    "media_type": add_type,
                    "media_category": media_category,
                    "season": add_season,
                    "search_keyword": add_title,
                }
                if add_year:
                    sub["year"] = add_year

                added = self.add_subscription(sub)
                if added:
                    logger.info(
                        f"【115助手】配置快捷操作: 已添加订阅 {add_title} "
                        f"(TMDB={add_tmdb_id}, 季={add_season}, 年={add_year})"
                    )
                    self._start_subscription_check_background(f"新增订阅首检: {add_title}")
                else:
                    logger.warning(f"【115助手】配置快捷操作: 订阅已存在 {add_title} TMDB={add_tmdb_id}")
            except (ValueError, TypeError) as e:
                logger.warning(f"【115助手】添加订阅失败: 参数无效: {e}")
            config["action_add_title"] = ""
            config["action_add_tmdb_id"] = ""
            config["action_add_type"] = "电视剧"
            config["action_add_season"] = ""
            config["action_add_year"] = ""
            changed = True

        if changed:
            self.update_config(config)

    # ================================================================
    #  工具方法
    # ================================================================

    # ── p115client cookie 同步 ────────────────────────────

    _COOKIE_SOURCE_LABELS = {
        "auto": "自动选择",
        "plugin": "插件Cookie",
        "mp": "MP的115Cookie",
        "p115client": "扫码Cookie",
    }

    def _try_sync_cookies_from_p115client(self):
        """
        启动时：如果存在 p115client 的 cookie 文件，
        且插件配置中没有手动粘贴的 cookie，自动同步到插件配置。
        cookie 文件格式：latin-1 编码的 cookie 字符串（非 JSON）。
        """
        try:
            cookies_path = COOKIES_PATH
            if not os.path.exists(cookies_path):
                return
            with open(cookies_path, "rb") as f:
                content = f.read()
            cookies_str = content.decode("latin-1").strip()
            if not cookies_str or len(cookies_str) < 10:
                return

            logger.info("【115助手】检测到 p115client cookie 文件，同步到插件配置")
            self._cookies = cookies_str
            # 不调用 update_config（避免覆盖用户其他配置），
            # 仅更新内存中的值，供本次运行使用
        except Exception as e:
            logger.debug(f"【115助手】同步 p115client cookies 失败: {e}")

    def _read_p115client_cookie(self) -> str:
        """读取扫码登录保存的 p115client Cookie 文件。"""
        try:
            cookies_path = COOKIES_PATH
            if not os.path.exists(cookies_path):
                return ""
            with open(cookies_path, "rb") as f:
                return f.read().decode("latin-1").strip()
        except Exception as e:
            logger.debug(f"【115助手】读取扫码 Cookie 失败: {e}")
            return ""

    def _get_mp_115_cookie(self) -> str:
        """读取 MoviePilot 主 115 配置插件中的 Cookie。"""
        try:
            from app.db.plugindata_oper import PluginDataOper as _PDO
            conf = _PDO().get_data("net115config", "config") or {}
            cookie = str(conf.get("cookies") or "").strip()
            if cookie:
                return cookie
        except Exception as e:
            logger.debug(f"【115助手】读取 net115config 插件 Cookie 失败: {e}")

        try:
            conf = SystemConfigOper().get("plugin.Net115Config") or {}
            if isinstance(conf, dict):
                return str(conf.get("cookies") or "").strip()
        except Exception as e:
            logger.debug(f"【115助手】读取旧 Net115Config Cookie 失败: {e}")
        return ""

    def _cookie_source_display(self, source: Optional[str] = None) -> str:
        source = source or self._cookie_source
        return self._COOKIE_SOURCE_LABELS.get(source, source or "未知")

    def _resolve_115_cookie(self, validate: bool = True) -> Tuple[str, str]:
        """
        根据配置选择实际用于 115 Web API 的 Cookie。
        auto 模式会按 插件Cookie → MP的115Cookie → 扫码Cookie 依次选择第一份可用 Cookie。
        """
        candidates = {
            "plugin": self._cookies.strip() if self._cookies else "",
            "mp": self._get_mp_115_cookie(),
            "p115client": self._read_p115client_cookie(),
        }
        order = ["plugin", "mp", "p115client"] if self._cookie_source == "auto" else [self._cookie_source]
        statuses: Dict[str, dict] = {}
        checker = None
        if validate and _CLIENT_AVAILABLE and P115ClientManager:
            try:
                checker = P115ClientManager()
            except Exception as e:
                logger.debug(f"【115助手】初始化 Cookie 检查器失败: {e}")

        fallback_cookie = ""
        fallback_source = ""
        for source in order:
            cookie = candidates.get(source, "")
            label = self._cookie_source_display(source)
            if not cookie:
                statuses[source] = {"label": label, "has_cookie": False, "valid": False, "message": "未配置"}
                continue
            if not fallback_cookie:
                fallback_cookie = cookie
                fallback_source = label
            if checker:
                ok, err = checker.check_cookie_valid(cookie)
                statuses[source] = {
                    "label": label,
                    "has_cookie": True,
                    "valid": bool(ok),
                    "message": err or ("可用" if ok else "不可用"),
                }
                if not ok:
                    self._notify_token_expired(label, err)
                    continue
            else:
                statuses[source] = {"label": label, "has_cookie": True, "valid": None, "message": "未校验"}

            self._active_cookie_source = label
            self._cookie_status = statuses
            if self._cookie_source == "auto":
                logger.info(f"【115助手】本次使用 115 Cookie 来源: {label}")
            return cookie, label

        self._cookie_status = statuses
        self._active_cookie_source = "无可用Cookie"
        if fallback_cookie and not checker:
            return fallback_cookie, fallback_source
        return "", ""

    # ── 工具方法 ──────────────────────────────────────────

    @staticmethod
    def _to_minutes(value: int, unit: str) -> int:
        """将 value + unit 转换为分钟"""
        if unit == "hours":
            return value * 60
        elif unit == "days":
            return value * 1440
        return value  # minutes

    @staticmethod
    def _normalize_tmdb_domain(value: Any) -> str:
        """归一化 TMDB API 域名，配置里允许填完整 URL。"""
        domain = str(value or "").strip()
        domain = re.sub(r"^https?://", "", domain).strip("/")
        return domain or "api.themoviedb.org"

    @classmethod
    def _normalize_fallback_cloud_name(cls, value: Any) -> str:
        name = str(value or "").strip()
        if not name:
            return ""
        compact = re.sub(r"\s+", "", name).lower()
        aliases = {
            "aliyun": "阿里云盘",
            "ali": "阿里云盘",
            "alipan": "阿里云盘",
            "阿里": "阿里云盘",
            "阿里云": "阿里云盘",
            "阿里云盘": "阿里云盘",
            "baidu": "百度网盘",
            "百度": "百度网盘",
            "百度云": "百度网盘",
            "百度网盘": "百度网盘",
            "quark": "夸克",
            "kuake": "夸克",
            "夸克": "夸克",
            "夸克网盘": "夸克",
            "123": "123云盘",
            "123云盘": "123云盘",
        }
        return aliases.get(compact, name if name in cls._FALLBACK_CLOUD_DEFAULT_PRIORITY else "")

    @classmethod
    def _normalize_fallback_cloud_list(cls, value: Any) -> List[str]:
        if isinstance(value, str):
            raw_items = re.split(r"[,，、\s]+", value)
        elif isinstance(value, (list, tuple, set)):
            raw_items = list(value)
        else:
            raw_items = []

        normalized = []
        seen = set()
        for item in raw_items:
            cloud = cls._normalize_fallback_cloud_name(item)
            if cloud and cloud not in seen:
                normalized.append(cloud)
                seen.add(cloud)
        return normalized

    @staticmethod
    def _format_fallback_cloud_order(clouds: List[str]) -> str:
        return ",".join([str(cloud).strip() for cloud in (clouds or []) if str(cloud).strip()])

    def _get_effective_fallback_clouds(self) -> List[str]:
        enabled = self._normalize_fallback_cloud_list(self._fallback_clouds)
        if not enabled:
            return []
        enabled_set = set(enabled)
        ordered_candidates = []
        for cloud in (
            self._normalize_fallback_cloud_list(self._fallback_cloud_priority)
            + list(self._FALLBACK_CLOUD_DEFAULT_PRIORITY)
            + enabled
        ):
            if cloud not in ordered_candidates:
                ordered_candidates.append(cloud)
        return [cloud for cloud in ordered_candidates if cloud in enabled_set]

    @staticmethod
    def _mask_secret(value: str) -> str:
        if not value:
            return "未配置"
        if len(value) <= 8:
            return "已配置"
        return f"{value[:4]}...{value[-4:]}"

    def _apply_tmdb_settings(self):
        """把插件里的 TMDB 配置写入 MoviePilot 运行时 settings。"""
        try:
            if self._tmdb_api_key:
                settings.TMDB_API_KEY = self._tmdb_api_key
                os.environ["TMDB_API_KEY"] = self._tmdb_api_key
            if self._tmdb_api_domain:
                settings.TMDB_API_DOMAIN = self._normalize_tmdb_domain(self._tmdb_api_domain)
                os.environ["TMDB_API_DOMAIN"] = settings.TMDB_API_DOMAIN
        except Exception as e:
            logger.warning(f"【115助手】应用 TMDB 配置失败: {e}")

    def _build_cd2_client(self):
        """创建 CD2Client；如配置了账号密码，则先换取 gRPC JWT。"""
        from .clients.cd2 import CD2Client

        cd2 = CD2Client(self._cd2_host, self._cd2_port, self._cd2_token)
        if self._cd2_username and self._cd2_password:
            jwt_token = cd2.get_token(
                self._cd2_username,
                self._cd2_password,
                self._cd2_totp_code,
            )
            if jwt_token:
                cd2 = CD2Client(self._cd2_host, self._cd2_port, jwt_token)
            else:
                logger.warning(f"【CD2巡查】CD2 登录换取 Token 失败: {cd2.last_error}")
        return cd2

    @staticmethod
    def _format_cd2_connection_error(cd2) -> str:
        err = getattr(cd2, "last_error", "") or "未知错误"
        if "UNAUTHENTICATED" in err or "Invalid auth token" in err:
            return (
                "CD2 连接失败：Token 无效或已过期。"
                "如果填的是 CD2「令牌管理」里的 36 位 token，请改填 CD2 用户名/密码，"
                "插件会自动换取 gRPC JWT。"
            )
        return f"CD2 连接失败：{err}"

    def get_state(self) -> bool:
        return self._enabled

    # ================================================================
    #  定时服务
    # ================================================================

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        subscribe_interval = (
            self._batch_interval_minutes
            if self._batch_check_enabled
            else self._interval_minutes
        )
        services = [{
            "id": "net115helper_subscribe_check",
            "name": "115网盘订阅检查",
            "trigger": "interval",
            "func": self.check_subscriptions,
            "kwargs": {
                "minutes": subscribe_interval,
            }
        }]
        if self._closed_loop_organize_enabled:
            services.append({
                "id": "net115helper_pending_organize",
                "name": "115待入库闭环整理",
                "trigger": "interval",
                "func": self._organize_pending_subscriptions,
                "kwargs": {
                    "minutes": 3,
                }
            })
        if self._cd2_watch_enabled and self._cd2_watch_rules:
            services.append({
                "id": "net115helper_cd2_watch",
                "name": "CD2云盘巡查同步",
                "trigger": "interval",
                "func": self._run_cd2_watch,
                "kwargs": {
                    "minutes": self._cd2_watch_interval_minutes,
                }
            })
            services.append({
                "id": "net115helper_cd2_watch_notice_confirm",
                "name": "CD2巡查入库通知补偿确认",
                "trigger": "interval",
                "func": self._run_cd2_watch_notice_compensation,
                "kwargs": {
                    "minutes": 10,
                }
            })
        services.append({
            "id": "net115helper_cloud_auth_health",
            "name": "网盘登录状态健康检查",
            "trigger": "interval",
            "func": self.check_cloud_auth_health,
            "kwargs": {
                "minutes": 8 * 60,
            }
        })
        return services

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    # ================================================================
    #  自定义 API
    # ================================================================

    def get_api(self) -> List[Dict[str, Any]]:
        apis = [
                {
                    "path": "/check_all",
                    "endpoint": self.api_check_all,
                    "methods": ["GET"],
                    "allow_anonymous": True,
                    "summary": "一键触发订阅检查",
                    "description": "手动触发所有网盘订阅的检查，立即搜索并转存缺失集数。",
                },
                {
                    "path": "/run_subscribe_now",
                    "endpoint": self.api_run_subscribe_now,
                    "methods": ["GET"],
                    "allow_anonymous": True,
                    "summary": "立即触发订阅巡检",
                    "description": "后台触发一次 115 订阅剧巡检，不等待下次定时任务。",
                },
                {
                    "path": "/organize_pending_now",
                    "endpoint": self.api_organize_pending_now,
                    "methods": ["GET"],
                    "allow_anonymous": True,
                    "summary": "立即整理待入库文件",
                    "description": "后台触发一次 MP 原生整理，处理订阅中已转存但仍处于待入库状态的文件。",
                },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "获取订阅状态",
                "description": "返回所有订阅的当前状态，包括已有集数、总集数、进度等。",
            },
            {
                "path": "/logs_page",
                "endpoint": self.api_logs_page,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "日志查看页面",
                "description": "返回 115 助手内置的 MoviePilot 日志查看页面。",
            },
            {
                "path": "/mp_logs",
                "endpoint": self.api_mp_logs,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "读取 MoviePilot 日志",
                "description": "读取最近的 MoviePilot 日志，支持关键词过滤和行数限制。",
            },
            {
                "path": "/cancel_sub",
                "endpoint": self.api_cancel_sub,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "取消订阅",
                "description": "取消指定 TMDB ID 的订阅。参数: tmdb_id (必填), season (可选)",
            },
            {
                "path": "/archive_sub",
                "endpoint": self.api_archive_sub,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "手动归档订阅",
                "description": "将指定 TMDB ID 的订阅移入历史记录。参数: tmdb_id (必填), season (可选), reason (可选)",
            },
            {
                "path": "/delete_history",
                "endpoint": self.api_delete_history,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "删除历史归档记录",
                "description": "删除指定 TMDB ID 的历史归档记录，删除后可重新订阅。参数: tmdb_id (必填), season (可选)",
            },
            {
                "path": "/add_sub",
                "endpoint": self.api_add_sub,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "添加订阅",
                "description": "添加新的网盘订阅。参数: title, tmdb_id (必填), media_type, season, year",
            },
        ]

        # ── CD2 巡查同步 API ──
        apis.extend([
            {
                "path": "/watch_rules",
                "endpoint": self.api_get_watch_rules,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "获取CD2巡查规则",
                "description": "返回所有CD2巡查同步规则列表。",
            },
            {
                "path": "/save_watch_rule",
                "endpoint": self.api_save_watch_rule,
                "methods": ["POST"],
                "allow_anonymous": True,
                "summary": "保存CD2巡查规则",
                "description": "新增或更新一条CD2巡查规则。",
            },
            {
                "path": "/delete_watch_rule",
                "endpoint": self.api_delete_watch_rule,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "删除CD2巡查规则",
                "description": "删除指定ID的巡查规则。参数: id",
            },
            {
                "path": "/run_watch_now",
                "endpoint": self.api_run_watch_now,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "立即触发CD2巡查",
                "description": "立即执行一次CD2巡查同步，不等待定时任务。",
            },
        ])

        # ── 阿里云盘扫码登录 API ──
        apis.extend([
            {
                "path": "/auth_status",
                "endpoint": self.api_auth_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "统一网盘登录状态",
                "description": "返回 115/阿里/夸克/百度的统一登录状态。",
            },
            {
                "path": "/auth_check",
                "endpoint": self.api_auth_check,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "检查指定网盘登录状态",
                "description": "参数 cloud=115|aliyun|quark|baidu，返回统一状态对象。",
            },
            {
                "path": "/auth_health_check",
                "endpoint": self.api_auth_health_check,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "立即执行网盘登录健康检查",
                "description": "检查已配置或已勾选降级的网盘登录状态。",
            },
            {
                "path": "/aliyun_page",
                "endpoint": self.api_aliyun_page,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "阿里云盘扫码登录页面",
                "description": "返回阿里云盘扫码登录 HTML 页面。",
            },
            {
                "path": "/aliyun_qrcode_login",
                "endpoint": self.api_aliyun_qrcode_login,
                "methods": ["POST"],
                "allow_anonymous": True,
                "summary": "阿里云盘扫码登录",
                "description": "启动阿里云盘扫码登录流程，返回 QR 码图片（base64）。",
            },
            {
                "path": "/aliyun_qrcode_status",
                "endpoint": self.api_aliyun_qrcode_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "阿里云盘扫码状态",
                "description": "轮询阿里云盘登录状态。",
            },
            {
                "path": "/aliyun_login_status",
                "endpoint": self.api_aliyun_login_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "阿里云盘登录状态",
                "description": "返回阿里云盘是否已登录。",
            },
            {
                "path": "/quark_login_status",
                "endpoint": self.api_quark_login_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "夸克网盘登录状态",
                "description": "返回夸克网盘 Cookie 是否可用。",
            },
            {
                "path": "/baidu_login_status",
                "endpoint": self.api_baidu_login_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "百度网盘登录状态",
                "description": "返回百度网盘 Cookie 是否可用。",
            },
        ])

        # ── 115 扫码登录 API ──
        apis.extend([
            {
                "path": "/qrcode_page",
                "endpoint": self.api_qrcode_page,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "115 扫码登录页面",
                "description": "返回插件内置的 115 扫码登录 HTML 页面。",
            },
            {
                "path": "/qrcode_login",
                "endpoint": self.api_qrcode_login,
                "methods": ["POST"],
                "allow_anonymous": True,
                "summary": "扫码登录 115",
                "description": "触发 115 扫码登录流程，生成二维码图片并返回访问路径。前端轮询 /qrcode_status 获取扫码状态。",
            },
            {
                "path": "/qrcode_status",
                "endpoint": self.api_qrcode_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "扫码状态查询",
                "description": "查询扫码登录状态。返回: idle | waiting | scanned | confirmed | expired | error。",
            },
            {
                "path": "/login_status",
                "endpoint": self.api_login_status,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "115 登录状态",
                "description": "返回 115 当前登录状态（是否已登录、UID 等）。",
            },
        ])

        return apis

    def api_check_all(self):
        """API: 一键触发全部订阅检查"""
        try:
            self.check_subscriptions()
            return {"success": True, "message": "订阅检查已完成"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_run_subscribe_now(self):
        """API: 后台立即触发一次订阅巡检"""
        if not self._load_subscriptions():
            return {"success": False, "message": "还没有添加 115 订阅"}
        started = self._start_subscription_check_background("手动立即执行")
        if started:
            return {"success": True, "message": "115订阅巡检已在后台启动"}
        return {"success": True, "message": "已有订阅巡检正在运行，已排队补跑"}

    def api_organize_pending_now(self, tmdb_id: int = None, season: int = None):
        """API: 后台立即触发一次待入库文件整理"""
        if not self._load_subscriptions():
            return {"success": False, "message": "还没有添加 115 订阅"}
        started = self._start_pending_organize_background(
            tmdb_id=int(tmdb_id) if tmdb_id else None,
            season=int(season) if season else None,
            reason="手动整理待入库",
        )
        if started:
            return {"success": True, "message": "待入库整理已在后台启动"}
        return {"success": False, "message": "已有待入库整理任务正在运行，请稍后再试"}

    def api_logs_page(self):
        """API: 返回日志查看器页面"""
        return HTMLResponse(self._build_logs_page_html())

    def api_mp_logs(self, lines: int = 400, keyword: str = "", source: str = ""):
        """API: 读取 MoviePilot 最近日志。"""
        try:
            try:
                lines = int(lines or 400)
            except (TypeError, ValueError):
                lines = 400
            lines = max(50, min(lines, 3000))
            keyword = str(keyword or "").strip()
            source = str(source or "").strip()

            log_files = self._get_mp_log_files()
            if not log_files:
                return {
                    "success": False,
                    "message": f"未找到日志文件，已检查目录: {settings.LOG_PATH}",
                    "data": [],
                    "sources": [],
                }

            selected = None
            if source:
                for item in log_files:
                    if item["path"] == source or item["name"] == source:
                        selected = item
                        break
            if not selected:
                selected = log_files[0]

            raw_lines = self._tail_text_file(Path(selected["path"]), max(lines * 5, lines))
            if keyword:
                words = [w.strip() for w in re.split(r"[\s,，]+", keyword) if w.strip()]
                filtered = []
                for line in raw_lines:
                    if all(w.lower() in line.lower() for w in words):
                        filtered.append(line)
                raw_lines = filtered[-lines:]
            else:
                raw_lines = raw_lines[-lines:]

            return {
                "success": True,
                "message": "OK",
                "data": raw_lines,
                "source": selected,
                "sources": log_files,
            }
        except Exception as e:
            logger.warning(f"【115助手】读取 MP 日志失败: {e}")
            return {"success": False, "message": str(e), "data": [], "sources": []}

    def api_get_watch_rules(self):
        """API: 获取CD2巡查规则列表"""
        return {"success": True, "data": self._cd2_watch_rules or []}

    def api_save_watch_rule(self, rule: dict = Body(default=None)):
        """API: 保存（新增或更新）一条CD2巡查规则"""
        import uuid as _uuid
        # 兼容两种提交格式：
        # 1) {"rule": {...}}
        # 2) {"name": "...", "source_path": "...", "dest_115_path": "..."}
        if not rule:
            return {"success": False, "message": "缺少规则数据"}
        if not isinstance(rule, dict):
            return {"success": False, "message": "规则数据格式错误"}
        if isinstance(rule.get("rule"), dict) and not any(
            key in rule for key in ("name", "source_path", "dest_115_path")
        ):
            rule = rule["rule"]
        rule = dict(rule)
        rule["name"] = str(rule.get("name", "")).strip()
        rule["source_path"] = str(rule.get("source_path", "")).strip()
        rule["dest_115_path"] = str(rule.get("dest_115_path", "")).strip()
        rule["enabled"] = bool(rule.get("enabled", True))
        if not rule["name"] or not rule["source_path"] or not rule["dest_115_path"]:
            return {"success": False, "message": "规则名称、源路径、115目标父目录都必须填写"}
        rules = list(self._cd2_watch_rules or [])
        rule_id = str(rule.get("id") or "").strip()
        if rule_id:
            rule["id"] = rule_id
            # 更新已有规则
            for i, r in enumerate(rules):
                if str(r.get("id") or "") == rule_id or self._cd2_watch_rule_key(r) == rule_id:
                    rules[i] = rule
                    break
            else:
                rules.append(rule)
        else:
            rule["id"] = str(_uuid.uuid4())
            rules.append(rule)
        self._cd2_watch_rules = rules
        config = self.get_config() or {}
        config["cd2_watch_rules"] = rules
        self.update_config(config)
        return {"success": True, "message": "规则已保存", "data": rule}

    def api_delete_watch_rule(self, id: str = None, key: str = None):
        """API: 删除CD2巡查规则"""
        targets = {
            str(value).strip()
            for value in (id, key)
            if str(value or "").strip()
        }
        if not targets:
            return {"success": False, "message": "缺少 id 参数"}

        removed_rules = []
        rules = []
        for rule in (self._cd2_watch_rules or []):
            candidates = {
                str(rule.get("id") or "").strip(),
                self._cd2_watch_rule_key(rule),
            }
            if candidates & targets:
                removed_rules.append(rule)
            else:
                rules.append(rule)

        if not removed_rules:
            return {"success": False, "message": "未找到要删除的巡查规则"}

        self._cd2_watch_rules = rules
        config = self.get_config() or {}
        config["cd2_watch_rules"] = rules
        self.update_config(config)

        cleanup_keys = set(targets)
        for rule in removed_rules:
            cleanup_keys.add(self._cd2_watch_rule_key(rule))
            if rule.get("id"):
                cleanup_keys.add(str(rule.get("id")))

        try:
            index = self._load_cd2_watch_index()
            rule_states = index.get("rules")
            if isinstance(rule_states, dict):
                for rule_key in cleanup_keys:
                    rule_states.pop(rule_key, None)
            completed = index.get("completed")
            if isinstance(completed, list):
                index["completed"] = [
                    item for item in completed
                    if not isinstance(item, dict)
                    or str(item.get("rule_id") or "") not in cleanup_keys
                ]
            self._save_cd2_watch_index(index)
        except Exception as e:
            logger.warning(f"【CD2巡查】删除规则后清理索引失败: {e}")

        try:
            notices = [
                item for item in self._load_cd2_watch_pending_notices()
                if not isinstance(item, dict)
                or str(item.get("rule_id") or "") not in cleanup_keys
            ]
            self._save_cd2_watch_pending_notices(notices)
        except Exception as e:
            logger.warning(f"【CD2巡查】删除规则后清理待通知失败: {e}")

        return {"success": True, "message": "规则已删除"}

    def api_run_watch_now(self):
        """API: 立即触发一次CD2巡查同步"""
        import threading
        if not self._cd2_watch_rules:
            return {"success": False, "message": "还没有配置CD2巡查规则"}
        if not self._cd2_host or not (self._cd2_token or (self._cd2_username and self._cd2_password)):
            return {"success": False, "message": "CD2 地址或认证信息未配置"}
        try:
            cd2 = self._build_cd2_client()
            if not cd2.test_connection():
                return {"success": False, "message": self._format_cd2_connection_error(cd2)}
        except Exception as e:
            return {"success": False, "message": f"CD2 连接测试异常: {e}"}
        threading.Thread(target=self._run_cd2_watch, kwargs={"force": True}, daemon=True).start()
        return {"success": True, "message": "CD2巡查已在后台启动"}

    def api_status(self):
        """API: 获取全部订阅状态"""
        try:
            subscriptions = self._load_subscriptions()
            status_list = []
            for sub in subscriptions:
                status_list.append({
                    "title": sub.get("title", ""),
                    "tmdb_id": sub.get("tmdb_id"),
                    "season": sub.get("season"),
                    "year": sub.get("year"),
                    "media_type": sub.get("media_type", "电视剧"),
                    "media_category": sub.get("media_category", "ongoing"),
                    "last_found": sub.get("last_found", ""),
                    "created": sub.get("created", ""),
                    "existing_count": sub.get("_cache_existing_count"),
                    "existing_episodes": sub.get("_cache_existing_episodes", []),
                    "pending_count": sub.get("_cache_pending_count", 0),
                    "pending_episodes": sub.get("_cache_pending_episodes", []),
                    "pending_since": sub.get("_cache_pending_since", ""),
                    "pending_status": sub.get("pending_copy_status", ""),
                    "last_found_via": sub.get("last_found_via", ""),
                    "total_episodes": sub.get("_cache_total_episodes"),
                    "aired_episodes": sub.get("_cache_aired_episodes"),
                    "next_air_date": sub.get("_cache_next_air_date"),
                    "last_search_at": sub.get("_last_search_at"),
                })
            runtime = dict(self._load_runtime_state() or {})
            runtime.update({
                "batch_check_enabled": self._batch_check_enabled,
                "batch_interval_minutes": self._batch_interval_minutes,
                "batch_size": self._batch_size,
                "batch_direct_115_limit": self._batch_direct_115_limit,
                "batch_cd2_copy_limit": self._batch_cd2_copy_limit,
                "batch_fallback_cloud_limit": self._batch_fallback_cloud_limit,
                "rate_limit_cooldown_minutes": self._rate_limit_cooldown_minutes,
                "rate_limit_escalation_enabled": self._rate_limit_escalation_enabled,
                "airing_skip_enabled": self._airing_skip_enabled,
                "airing_window_start_hour": self._airing_window_start_hour,
                "airing_idle_probe_hours": self._airing_idle_probe_hours,
                "notice_same_title_window_minutes": self._notice_same_title_window_minutes,
                "notice_same_title_max": self._notice_same_title_max,
                "notice_global_window_minutes": self._notice_global_window_minutes,
                "notice_global_max": self._notice_global_max,
                "fallback_cloud_priority": self._format_fallback_cloud_order(self._fallback_cloud_priority),
                "effective_fallback_clouds": self._get_effective_fallback_clouds(),
            })
            return {"success": True, "data": status_list, "runtime": runtime}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_cancel_sub(self, tmdb_id: int = None, season: int = None):
        """API: 取消订阅"""
        try:
            if not tmdb_id:
                return {"success": False, "message": "需要提供 tmdb_id 参数"}
            removed = self.remove_subscription(int(tmdb_id), int(season) if season else None)
            if removed:
                return {"success": True, "message": f"已取消订阅 TMDB={tmdb_id}"}
            return {"success": False, "message": f"未找到订阅 TMDB={tmdb_id}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_archive_sub(self, tmdb_id: int = None, season: int = None, reason: str = "手动归档"):
        """API: 手动归档订阅"""
        try:
            if not tmdb_id:
                return {"success": False, "message": "需要提供 tmdb_id 参数"}

            archived = self.archive_subscription(
                int(tmdb_id),
                int(season) if season else None,
                reason or "手动归档",
            )
            if archived:
                names = ", ".join([item.get("title", "") for item in archived if item.get("title")])
                return {
                    "success": True,
                    "message": f"已归档 {len(archived)} 个订阅" + (f": {names}" if names else ""),
                    "data": archived,
                }
            return {"success": False, "message": f"未找到订阅 TMDB={tmdb_id}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_delete_history(self, tmdb_id: int = None, season: int = None):
        """API: 删除历史归档记录（删除后可重新订阅）"""
        try:
            if not tmdb_id:
                return {"success": False, "message": "需要提供 tmdb_id 参数"}
            removed = self.delete_history(int(tmdb_id), int(season) if season else None)
            if removed:
                return {
                    "success": True,
                    "message": f"已删除 {removed} 条历史归档记录，现在可以重新订阅",
                }
            return {"success": False, "message": f"未找到历史归档 TMDB={tmdb_id}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_add_sub(self, title: str = "", tmdb_id: int = None,
                    media_type: str = "电视剧", season: int = None, year: int = None):
        """API: 添加订阅"""
        try:
            if not title or not tmdb_id:
                return {"success": False, "message": "需要提供 title 和 tmdb_id 参数"}

            _raw_type = (media_type or "电视剧").lower()
            add_type = "电影" if _raw_type in ("movie", "电影") else "电视剧"
            int_season = int(season) if season else None
            int_year = int(year) if year else None
            media_category = self._resolve_media_category(add_type, int_season, int_year)

            sub = {
                "title": title,
                "tmdb_id": int(tmdb_id),
                "media_type": add_type,
                "media_category": media_category,
                "season": int_season,
                "search_keyword": title,
            }
            if int_year:
                sub["year"] = int_year

            added = self.add_subscription(sub)
            if added:
                self._start_subscription_check_background(f"新增订阅首检: {title}")
                return {"success": True, "message": f"已添加订阅: {title}"}
            return {"success": False, "message": f"订阅已存在: {title}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── 扫码登录 API ────────────────────────────────────

    def api_qrcode_page(self):
        """API: 返回 115 扫码登录独立页面。"""
        try:
            html_path = os.path.join(os.path.dirname(__file__), "qrcode.html")
            with open(html_path, "r", encoding="utf-8") as f:
                html = f.read()
            # 页面本身可以匿名打开；真正发起扫码/状态轮询时仍使用 token 调插件 API。
        except Exception as e:
            logger.warning(f"【115助手】读取扫码页面失败: {e}")
            html = (
                "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"UTF-8\">"
                "<title>115 扫码登录</title></head><body>"
                "<p>扫码页面文件读取失败，请检查插件目录 qrcode.html。</p>"
                "</body></html>"
            )
        return HTMLResponse(content=html, media_type="text/html")

    def api_qrcode_login(self):
        """
        API: 触发扫码登录。
        生成二维码图片，返回可访问的 URL。
        前端应轮询 /qrcode_status 直到状态变为 confirmed。
        """
        if not _CLIENT_AVAILABLE:
            return {"success": False, "message": "p115client 库未安装，请先在容器中执行: pip install p115client"}

        try:
            manager = get_client_manager()
            if not manager:
                return {"success": False, "message": "客户端管理器初始化失败"}

            result = manager.qrcode_login()

            if result.get("success"):
                qrcode_image_path = result.get("qrcode_image_path", "")
                qrcode_base64 = result.get("qrcode_base64", "")

                return {
                    "success": True,
                    "message": result.get("message", ""),
                    "qrcode_image_path": qrcode_image_path,
                    "qrcode_base64": qrcode_base64,
                    "qrcode_uid": result.get("qrcode_uid", ""),
                }
            else:
                return {"success": False, "message": result.get("message", "扫码登录失败")}
        except Exception as e:
            logger.error(f"【115助手】api_qrcode_login 异常: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    def api_qrcode_status(self):
        """API: 查询扫码登录状态"""
        if not _CLIENT_AVAILABLE:
            return {"success": False, "status": "error", "message": "p115client 库未安装"}

        try:
            qr_state = QrcodeState.get_instance()
            state = qr_state.to_dict()

            # 登录成功后，自动将 cookies 同步到插件配置
            if state.get("status") == "confirmed":
                self._sync_cookies_from_file()

            return {
                "success": True,
                "status": state.get("status", "idle"),
                "message": state.get("message", ""),
                "qrcode_image_path": state.get("qrcode_image_path", ""),
            }
        except Exception as e:
            return {"success": False, "status": "error", "message": str(e)}

    def api_login_status(self):
        """API: 查询 115 登录状态"""
        if not _CLIENT_AVAILABLE:
            # 回退到直接检查 cookie 配置
            has_cookie = bool(self._cookies and self._cookies.strip())
            return {
                "success": True,
                "logged_in": has_cookie,
                "message": "已登录（手动 Cookie）" if has_cookie else "未登录",
                "login_method": "manual_cookie",
            }

        try:
            manager = get_client_manager()
            if not manager:
                return {"success": False, "message": "客户端管理器未初始化"}

            candidates = {
                "plugin": self._cookies.strip() if self._cookies else "",
                "mp": self._get_mp_115_cookie(),
                "p115client": self._read_p115client_cookie(),
            }
            order = ["plugin", "mp", "p115client"] if self._cookie_source == "auto" else [self._cookie_source]
            status_map: Dict[str, dict] = {}
            active_source = ""
            active_label = ""
            active_message = ""

            for source in order:
                cookie = candidates.get(source, "")
                label = self._cookie_source_display(source)
                if not cookie:
                    status_map[source] = {
                        "label": label,
                        "has_cookie": False,
                        "valid": False,
                        "message": "未配置",
                    }
                    continue

                ok, msg = manager.check_cookie_valid(cookie)
                status_map[source] = {
                    "label": label,
                    "has_cookie": True,
                    "valid": bool(ok),
                    "message": msg or ("可用" if ok else "不可用"),
                }
                if ok and not active_source:
                    active_source = source
                    active_label = label
                    active_message = status_map[source]["message"]

            cookie_file_exists = os.path.exists(COOKIES_PATH)
            if active_source:
                message = f"{active_label} 可用"
                if active_message and active_message != "可用":
                    message = f"{message}: {active_message}"
            else:
                errors = [
                    f"{item.get('label')}: {item.get('message')}"
                    for item in status_map.values()
                    if item.get("has_cookie")
                ]
                message = "；".join(errors) if errors else "未配置 115 Cookie"

            return {
                "success": True,
                "logged_in": bool(active_source),
                "valid": bool(active_source),
                "active_source": active_source,
                "active_source_label": active_label,
                "username": active_label,
                "cookie_file_exists": cookie_file_exists,
                "cookie_status": status_map,
                "message": message,
                "login_method": "validated_cookie",
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── 统一网盘登录状态 ────────────────────────────────

    def _cloud_auth_is_required(self, cloud: str) -> bool:
        """是否需要在健康检查中关注该网盘。"""
        if cloud == "115":
            return True
        cloud_names = {
            "aliyun": "阿里云盘",
            "quark": "夸克",
            "baidu": "百度网盘",
        }
        return cloud_names.get(cloud, "") in (self._fallback_clouds or [])

    def _cloud_auth_configured(self, cloud: str) -> bool:
        if cloud == "115":
            return bool(self._cookies or self._get_mp_115_cookie() or self._read_p115client_cookie())
        if cloud == "aliyun":
            try:
                from .clients import AliyunClient
                return bool(self._aliyun_token) or bool(AliyunClient and AliyunClient.is_logged_in())
            except Exception:
                return bool(self._aliyun_token)
        if cloud == "quark":
            return bool(self._quark_cookie)
        if cloud == "baidu":
            return bool(self._baidu_cookie)
        return False

    def _check_cloud_auth(self, cloud: str) -> dict:
        cloud = (cloud or "").strip().lower()
        labels = {
            "115": "115",
            "aliyun": "阿里云盘",
            "quark": "夸克",
            "baidu": "百度网盘",
        }
        if cloud not in labels:
            return {
                "cloud": cloud,
                "label": cloud or "未知网盘",
                "configured": False,
                "required": False,
                "logged_in": False,
                "level": "error",
                "message": "不支持的网盘",
                "checked_at": datetime.now().isoformat(),
            }

        configured = self._cloud_auth_configured(cloud)
        required = self._cloud_auth_is_required(cloud)
        message = ""
        logged_in = False
        success = True
        level = "ok"

        try:
            if cloud == "115":
                raw = self.api_login_status()
                success = bool(raw.get("success", True))
                logged_in = bool(raw.get("logged_in") or raw.get("valid"))
                message = raw.get("message") or ("已登录" if logged_in else "未登录")
            elif cloud == "aliyun":
                raw = self.api_aliyun_login_status()
                success = bool(raw.get("success", True))
                logged_in = bool(raw.get("logged_in"))
                message = raw.get("message") or ("已登录" if logged_in else "未登录，请扫码")
            elif cloud == "quark":
                raw = self.api_quark_login_status()
                success = bool(raw.get("success", True))
                logged_in = bool(raw.get("logged_in"))
                message = raw.get("message") or ("已登录" if logged_in else "Cookie 不可用")
            elif cloud == "baidu":
                raw = self.api_baidu_login_status()
                success = bool(raw.get("success", True))
                logged_in = bool(raw.get("logged_in"))
                message = raw.get("message") or ("已登录" if logged_in else "Cookie 不可用")
        except Exception as e:
            success = False
            logged_in = False
            message = f"检查失败: {e}"

        if not configured and required:
            level = "error"
            if not message or "未登录" in message:
                message = "已启用但未配置登录凭据"
        elif not configured:
            level = "muted"
            message = message or "未配置"
        elif not success or not logged_in:
            level = "error" if required else "warning"
        else:
            level = "ok"

        return {
            "cloud": cloud,
            "label": labels[cloud],
            "configured": configured,
            "required": required,
            "logged_in": logged_in,
            "level": level,
            "message": message,
            "checked_at": datetime.now().isoformat(),
        }

    def _check_all_cloud_auth(self, notify: bool = False) -> dict:
        statuses = {
            cloud: self._check_cloud_auth(cloud)
            for cloud in ("115", "aliyun", "quark", "baidu")
        }
        self._cloud_auth_status = statuses
        if notify and self._notify:
            now = time.time()
            for cloud, item in statuses.items():
                if item.get("level") != "error" or not item.get("required"):
                    continue
                last = self._cloud_auth_last_warn.get(cloud, 0)
                if now - last < self._TOKEN_WARN_INTERVAL:
                    continue
                self._cloud_auth_last_warn[cloud] = now
                try:
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title="115网盘助手 - 网盘登录失效",
                        text=f"{item.get('label')}: {item.get('message')}",
                    )
                except Exception as e:
                    logger.debug(f"【115助手】发送网盘登录失效通知失败: {e}")
        return statuses

    def check_cloud_auth_health(self):
        """定时健康检查：每天约 3 次检查登录态。"""
        try:
            statuses = self._check_all_cloud_auth(notify=True)
            bad = [
                f"{item.get('label')}: {item.get('message')}"
                for item in statuses.values()
                if item.get("required") and item.get("level") == "error"
            ]
            if bad:
                logger.warning("【115助手】网盘登录健康检查异常: " + "；".join(bad))
            else:
                logger.info("【115助手】网盘登录健康检查通过")
            return statuses
        except Exception as e:
            logger.warning(f"【115助手】网盘登录健康检查失败: {e}")
            return {}

    def api_auth_status(self):
        """API: 返回统一网盘登录状态。"""
        return {"success": True, "data": self._check_all_cloud_auth(notify=False)}

    def api_auth_check(self, cloud: str = ""):
        """API: 检查单个网盘登录状态。"""
        return {"success": True, "data": self._check_cloud_auth(cloud)}

    def api_auth_health_check(self):
        """API: 立即执行一次登录健康检查。"""
        return {"success": True, "data": self.check_cloud_auth_health()}

    # ── 阿里云盘扫码登录 API ─────────────────────────────

    def api_aliyun_page(self):
        """API: 返回阿里云盘扫码登录独立页面。"""
        html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>阿里云盘扫码登录</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #121212; color: #fff; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
  .card { background: #1e1e1e; border-radius: 12px; padding: 32px; max-width: 360px; width: calc(100% - 32px); text-align: center; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }
  h2 { margin: 0 0 8px 0; font-size: 20px; }
  .subtitle { color: #aaa; font-size: 13px; margin-bottom: 20px; }
  .qr-container { margin: 16px auto; min-height: 220px; display: flex; justify-content: center; align-items: center; }
  .qr-img { max-width: 220px; max-height: 220px; border-radius: 8px; display: none; background: #fff; padding: 8px; }
  .placeholder { color: #666; font-size: 14px; padding: 64px 0; }
  .status { font-size: 14px; margin: 16px 0; min-height: 24px; color: #aaa; }
  .status.confirmed { color: #4caf50; }
  .status.error { color: #f44336; }
  .status.expired { color: #ff9800; }
  .btn { background: #fb8c00; color: #fff; border: 0; border-radius: 8px; padding: 12px 24px; font-size: 15px; cursor: pointer; width: 100%; }
  .btn:disabled { background: #444; color: #888; cursor: not-allowed; }
  .debug { font-size: 11px; color: #666; text-align: left; word-break: break-all; margin-top: 8px; }
</style>
</head>
<body>
<div class="card">
  <h2>阿里云盘扫码登录</h2>
  <p class="subtitle">用于 CD2 降级转存读取阿里云盘分享资源</p>
  <div class="qr-container">
    <div class="placeholder" id="placeholder">点击下方按钮生成二维码</div>
    <img class="qr-img" id="qrcode" alt="二维码">
  </div>
  <div class="status" id="status"></div>
  <div class="debug" id="debug"></div>
  <button class="btn" id="generateBtn" onclick="generateQR()">生成二维码</button>
</div>
<script>
function getToken() {
  var params = new URLSearchParams(window.location.search);
  return params.get('token') || localStorage.getItem('token') || localStorage.getItem('access_token') || '';
}
function setStatus(msg, cls) {
  var el = document.getElementById('status');
  el.textContent = msg;
  el.className = 'status ' + (cls || '');
}
function setDebug(msg) { document.getElementById('debug').textContent = msg; }
function headers() {
  var h = { 'Content-Type': 'application/json' };
  var token = getToken();
  if (token) h.Authorization = 'Bearer ' + token;
  return h;
}
function generateQR() {
  var btn = document.getElementById('generateBtn');
  btn.disabled = true;
  btn.textContent = '生成中...';
  setStatus('正在生成二维码...');
  fetch('/api/v1/plugin/Net115Helper/aliyun_qrcode_login', { method: 'POST', headers: headers(), credentials: 'include' })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      btn.disabled = false;
      btn.textContent = '重新生成二维码';
      if (data.status === 'confirmed') {
        document.getElementById('placeholder').style.display = 'block';
        document.getElementById('placeholder').textContent = data.message || '阿里云盘已登录';
        document.getElementById('qrcode').style.display = 'none';
        setStatus(data.message || '阿里云盘已登录', 'confirmed');
        return;
      }
      if (!data.success) {
        setStatus('生成失败: ' + (data.message || '未知错误'), 'error');
        return;
      }
      document.getElementById('placeholder').style.display = 'none';
      var img = document.getElementById('qrcode');
      img.style.display = 'block';
      img.src = data.qrcode_base64;
      setStatus('请用阿里云盘 APP 扫码');
      startPolling();
    })
    .catch(function(err) {
      btn.disabled = false;
      btn.textContent = '生成二维码';
      setStatus('网络错误: ' + err.message, 'error');
    });
}
var pollTimer = null;
function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(function() {
    fetch('/api/v1/plugin/Net115Helper/aliyun_qrcode_status', { headers: headers(), credentials: 'include' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var status = data.status || '';
        var msg = data.message || '';
        setStatus(msg || status, status);
        if (status === 'confirmed' || status === 'success') {
          clearInterval(pollTimer);
          setStatus('登录成功，授权已保存', 'confirmed');
        } else if (status === 'expired' || status === 'error') {
          clearInterval(pollTimer);
          setStatus(msg || '登录失败，请重新生成二维码', status);
        }
      })
      .catch(function(err) { setDebug('轮询错误: ' + err.message); });
  }, 2000);
}
</script>
</body>
</html>"""
        return HTMLResponse(content=html, media_type="text/html")

    def api_aliyun_qrcode_login(self):
        """API: 启动阿里云盘扫码登录，返回 QR 码 base64 图片"""
        try:
            from .clients import AliyunClient
        except ImportError:
            return {"success": False, "message": "aligo 未安装"}
        if AliyunClient is None:
            return {"success": False, "message": "aligo 未安装"}

        if AliyunClient.is_logged_in():
            return {"success": True, "status": "confirmed", "message": "阿里云盘已登录"}

        result = AliyunClient.start_qrcode_login()
        if not result.get("success"):
            return result

        # 等待最多 10 秒拿到 QR 码
        for _ in range(20):
            time.sleep(0.5)
            status = AliyunClient.get_login_status()
            if status.get("status") == "error":
                return {"success": False, "message": status.get("error") or "阿里云盘登录初始化失败"}
            img_bytes = AliyunClient.get_qr_image_bytes()
            if img_bytes:
                import base64
                b64 = "data:image/png;base64," + base64.b64encode(img_bytes).decode()
                return {"success": True, "qrcode_base64": b64}
            if status.get("status") == "confirmed" and AliyunClient.is_logged_in():
                return {"success": True, "status": "confirmed", "message": "阿里云盘已登录"}

        status = AliyunClient.get_login_status()
        detail = status.get("error") or status.get("status") or "waiting"
        return {"success": False, "message": f"QR 码生成超时，请重试（当前状态: {detail}）"}

    def api_aliyun_qrcode_status(self):
        """API: 轮询阿里云盘登录状态"""
        try:
            from .clients import AliyunClient
        except ImportError:
            return {"status": "error", "message": "aligo 未安装"}
        if AliyunClient is None:
            return {"status": "error", "message": "aligo 未安装"}

        s = AliyunClient.get_login_status()
        status = s.get("status", "idle")  # idle / waiting / confirmed / error
        if status == "confirmed":
            return {"status": "confirmed", "message": "登录成功！"}
        elif status == "waiting":
            return {"status": "waiting", "message": "请用阿里云盘 APP 扫码"}
        elif status == "error":
            return {"status": "error", "message": s.get("error", "登录失败")}
        return {"status": status, "message": ""}

    def api_aliyun_login_status(self):
        """API: 查询阿里云盘是否已登录"""
        try:
            from .clients import AliyunClient
        except ImportError:
            return {"logged_in": False, "message": "aligo 未安装"}
        if AliyunClient is None:
            return {"logged_in": False, "message": "aligo 未安装"}

        logged_in = AliyunClient.is_logged_in()
        return {
            "logged_in": logged_in,
            "message": "已登录" if logged_in else "未登录，请扫码",
        }

    def api_quark_login_status(self):
        """API: 查询夸克网盘 Cookie 是否可用"""
        try:
            from .clients import QuarkClient
        except ImportError:
            return {"logged_in": False, "message": "夸克客户端不可用"}
        if QuarkClient is None:
            return {"logged_in": False, "message": "夸克客户端不可用"}
        QuarkClient.set_cookie(self._quark_cookie)
        ok, message = QuarkClient.check_login()
        return {"logged_in": ok, "message": message}

    def api_baidu_login_status(self):
        """API: 查询百度网盘 Cookie 是否可用"""
        try:
            from .clients import BaiduClient
        except ImportError:
            return {"logged_in": False, "message": "百度客户端不可用"}
        if BaiduClient is None:
            return {"logged_in": False, "message": "百度客户端不可用"}
        BaiduClient.set_cookie(self._baidu_cookie)
        ok, message = BaiduClient.check_login()
        return {"logged_in": ok, "message": message}

    def _sync_cookies_from_file(self):
        """
        扫码登录成功后，将 cookies 文件内容同步到插件配置。
        这样 save_115_share.py 等直接读取插件配置的代码也能正常使用。
        cookie 文件格式：latin-1 编码的 cookie 字符串（非 JSON）。
        """
        if not _CLIENT_AVAILABLE:
            return
        try:
            cookies_path = COOKIES_PATH
            if not os.path.exists(cookies_path):
                return
            with open(cookies_path, "rb") as f:
                content = f.read()
            cookies_str = content.decode("latin-1").strip()
            if not cookies_str or len(cookies_str) < 10:
                return

            if cookies_str != self._cookies:
                logger.info(f"【115助手】扫码登录成功，同步 cookies 到插件配置（长度: {len(cookies_str)})")
                self._cookies = cookies_str
                # 更新插件配置
                config = self.get_config() or {}
                config["cookies"] = cookies_str
                self.update_config(config)
        except Exception as e:
            logger.warning(f"【115助手】同步 cookies 失败: {e}")

    # ================================================================
    #  配置表单 (get_form)
    # ================================================================

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    # ── 基础设置 ──
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "enabled",
                                        "label": "启用插件",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "notify",
                                        "label": "发送通知",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "notice_same_title_window_minutes",
                                        "label": "同剧限频窗口（分钟）",
                                        "type": "number",
                                        "placeholder": "360",
                                        "hint": "CD2 入库通知按剧名/TMDB 合并并限频；默认 6 小时。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "notice_same_title_max",
                                        "label": "同剧最多通知（次）",
                                        "type": "number",
                                        "placeholder": "2",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "notice_global_window_minutes",
                                        "label": "全局限频窗口（分钟）",
                                        "type": "number",
                                        "placeholder": "60",
                                        "hint": "仅限制插件发出的入库通知，登录失效等告警不受影响。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "notice_global_max",
                                        "label": "全局最多通知（次）",
                                        "type": "number",
                                        "placeholder": "8",
                                    }
                                }]
                            },
                        ]
                    },
                    # ── Cookie ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12, "md": 6},
                            "content": [{
                                "component": "VSelect",
                                "props": {
                                    "model": "cookie_source",
                                    "label": "115 Cookie 来源",
                                    "items": [
                                        {"title": "自动选择可用 Cookie（推荐）", "value": "auto"},
                                        {"title": "只用插件 Cookie", "value": "plugin"},
                                        {"title": "只用 MoviePilot 主 115 Cookie", "value": "mp"},
                                        {"title": "只用扫码 Cookie 文件", "value": "p115client"},
                                    ],
                                    "hint": "想少维护一份 Cookie，可选 MoviePilot 主 115 Cookie；自动模式会按插件 → MP → 扫码文件兜底。",
                                    "persistent-hint": True,
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextField",
                                "props": {
                                    "model": "cookies",
                                    "label": "115 Cookie",
                                    "placeholder": "UID=xxx; CID=xxx; SEID=xxx; ...",
                                    "type": "password",
                                    "hint": "仅在 Cookie 来源为「自动」或「只用插件 Cookie」时使用。",
                                    "persistent-hint": True,
                                }
                            }]
                        }]
                    },
                    # ── 统一网盘账号中心 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "网盘账号与登录状态",
                                    "text": (
                                        "115、阿里、夸克、百度的凭据统一在这里维护。"
                                        "长 Cookie 默认隐藏显示，填入或修改后保存插件设置即可生效；"
                                        "下方按钮可直接检查登录态或打开扫码授权。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "aliyun_token",
                                        "label": "阿里云盘 Token / Session（可选）",
                                        "placeholder": "优先使用扫码登录；如需手动维护可粘贴备注或后续接入的 token",
                                        "type": "password",
                                        "hint": "阿里当前主要使用扫码生成的 aligo session；此字段先集中展示，避免账号入口分散。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "quark_cookie",
                                        "label": "夸克 Cookie",
                                        "placeholder": "从已登录的 pan.quark.cn 浏览器请求中复制 Cookie",
                                        "type": "password",
                                        "hint": "用于插件把夸克分享链接保存到夸克临时目录。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "订阅巡检分批规则",
                                    "text": (
                                        "订阅较多时按批次轮询，减少 115 同时访问量。"
                                        "运行进度会自动记录，下批从上次位置继续。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "batch_check_enabled",
                                        "label": "启用分批巡检",
                                        "hint": "开启后按批次检查订阅，而不是每次全量扫描",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "batch_size",
                                        "label": "每批订阅数",
                                        "type": "number",
                                        "placeholder": "3",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "batch_interval_minutes",
                                        "label": "批次间隔（分钟）",
                                        "type": "number",
                                        "placeholder": "25",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "batch_fallback_cloud_limit",
                                        "label": "每订阅备用盘尝试数",
                                        "type": "number",
                                        "placeholder": "3",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "rate_limit_cooldown_minutes",
                                        "label": "115 风控静默分钟",
                                        "type": "number",
                                        "placeholder": "60",
                                        "hint": "触发 115 访问上限后，插件暂停会触碰 115 的动作",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "rate_limit_escalation_enabled",
                                        "label": "连续风控自动加长静默",
                                        "hint": "两小时内连续触发时按 1倍/1.5倍/2倍退避",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "airing_skip_enabled",
                                        "label": "追平已播出集数后跳过搜索",
                                        "hint": "按 TMDB 已播出集数而非总集数判断缺失，避免空搜未播出的集",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "airing_window_start_hour",
                                        "label": "播出日积极抓取起点(时)",
                                        "type": "number",
                                        "placeholder": "18",
                                        "hint": "排播日当天到点后，把当天这集也算进目标主动抓",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "airing_idle_probe_hours",
                                        "label": "兜底探针间隔(小时)",
                                        "type": "number",
                                        "placeholder": "6",
                                        "hint": "已追平时每隔多久仍放行一轮，兼容 TMDB 录入滞后；0=不放行",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "batch_direct_115_limit",
                                        "label": "每批 115 直连重动作上限",
                                        "type": "number",
                                        "placeholder": "0",
                                        "hint": "打开 115 分享详情/直转存会消耗额度；建议 0，优先走备用云盘降级",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "batch_cd2_copy_limit",
                                        "label": "每批 CD2→115 复制上限",
                                        "type": "number",
                                        "placeholder": "1",
                                        "hint": "降级转存复制到 115 的提交上限；填 0 可只查不复制",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "baidu_cookie",
                                        "label": "百度网盘 Cookie",
                                        "placeholder": "从已登录的 pan.baidu.com 浏览器请求中复制 Cookie",
                                        "type": "password",
                                        "hint": "用于插件把百度分享链接保存到百度临时目录。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "color": "primary",
                                        "variant": "tonal",
                                        "block": True,
                                        "onclick": (
                                            "fetch('/api/v1/plugin/Net115Helper/auth_status',{credentials:'include'})"
                                            ".then(r=>r.json()).then(d=>{"
                                            "const s=d.data||{};"
                                            "alert(Object.values(s).map(x=>x.label+'：'+(x.message||'')).join('\\n'));"
                                            "}).catch(e=>alert('登录态检查失败：'+e))"
                                        ),
                                        "prepend-icon": "mdi-cloud-check-outline",
                                    },
                                    "text": "检查全部登录态",
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "color": "primary",
                                        "variant": "elevated",
                                        "block": True,
                                        "onclick": "window.open('/api/v1/plugin/Net115Helper/qrcode_page','_blank')",
                                        "prepend-icon": "mdi-qrcode-scan",
                                    },
                                    "text": "115 扫码登录",
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "color": "orange",
                                        "variant": "elevated",
                                        "block": True,
                                        "onclick": "window.open('/api/v1/plugin/Net115Helper/aliyun_page','_blank')",
                                        "prepend-icon": "mdi-qrcode-scan",
                                    },
                                    "text": "阿里扫码登录",
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "color": "green",
                                        "variant": "tonal",
                                        "block": True,
                                        "onclick": (
                                            "fetch('/api/v1/plugin/Net115Helper/auth_check?cloud=quark',{credentials:'include'})"
                                            ".then(r=>r.json()).then(d=>alert('夸克：'+((d.data||{}).message||'')))"
                                            ".catch(e=>alert('夸克检查失败：'+e))"
                                        ),
                                        "prepend-icon": "mdi-cloud-check-outline",
                                    },
                                    "text": "检查夸克",
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "color": "blue",
                                        "variant": "tonal",
                                        "block": True,
                                        "onclick": (
                                            "fetch('/api/v1/plugin/Net115Helper/auth_check?cloud=baidu',{credentials:'include'})"
                                            ".then(r=>r.json()).then(d=>alert('百度：'+((d.data||{}).message||'')))"
                                            ".catch(e=>alert('百度检查失败：'+e))"
                                        ),
                                        "prepend-icon": "mdi-cloud-check-outline",
                                    },
                                    "text": "检查百度",
                                }]
                            },
                        ]
                    },

                    # ── 分类临时目录 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "分类临时目录（推荐配置）",
                                    "text": (
                                        "在 115 网盘中创建 3 个临时文件夹（如「待整理-电影」「待整理-连载剧」「待整理-老剧」），"
                                        "将文件夹 ID 填在下方。"
                                        "\n\nAI 转存时会按类型存到对应临时目录 → CloudDrive2 同步到 NAS → "
                                        "MoviePilot 目录监控自动刮削整理 → 剪切到你的最终媒体库目录。"
                                        "\n\n获取文件夹 ID: 在 115 网页版中打开文件夹，地址栏中 cid= 后面的数字就是 ID。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "movie_staging_cid",
                                        "label": "新电影临时目录 ID",
                                        "placeholder": "例: 2345678901",
                                        "hint": "分界年份及之后的电影",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "old_movie_staging_cid",
                                        "label": "老电影临时目录 ID",
                                        "placeholder": "例: 5678901234",
                                        "hint": "分界年份之前的电影",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "ongoing_staging_cid",
                                        "label": "连载剧临时目录 ID",
                                        "placeholder": "例: 3456789012",
                                        "hint": "115 中的「待整理-连载剧」文件夹 ID",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "archive_staging_cid",
                                        "label": "老剧/完结剧临时目录 ID",
                                        "placeholder": "例: 4567890123",
                                        "hint": "115 中的「待整理-老剧」文件夹 ID",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "default_cid",
                                        "label": "默认目录 ID（兜底）",
                                        "placeholder": "0",
                                        "hint": "未指定类型时的默认目录，0 = 根目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "movie_year_threshold",
                                        "label": "新/老电影分界年份",
                                        "type": "number",
                                        "placeholder": "2025",
                                        "hint": "此年份及之后 → 新电影目录，之前 → 老电影目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },

                    # ── PanSou 搜索服务配置 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "PanSou 搜索服务配置（必需）",
                                    "text": (
                                        "PanSou 是网盘资源搜索服务，插件需要通过它来搜索 115/阿里云盘/夸克等网盘的分享链接。"
                                        "\n\n请填写你的 PanSou 服务地址（如: http://192.168.1.100:8888）。"
                                        "\n如果没有 PanSou 服务，请先部署：https://github.com/wushihan89/PanSou"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "pansou_url",
                                        "label": "PanSou 搜索服务地址",
                                        "placeholder": "http://192.168.1.100:8888 或 http://pansou:5000",
                                        "hint": "必填。你的 PanSou 服务访问地址",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "pansou_auth_user",
                                        "label": "PanSou 认证用户名（可选）",
                                        "placeholder": "如果 PanSou 开启了认证才需要填写",
                                        "hint": "留空表示不使用认证",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "pansou_auth_pass",
                                        "label": "PanSou 认证密码（可选）",
                                        "placeholder": "如果 PanSou 开启了认证才需要填写",
                                        "hint": "留空表示不使用认证",
                                        "persistent-hint": True,
                                        "type": "password",
                                    }
                                }]
                            },
                        ]
                    },

                    # ── TMDB 配置 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "TMDB 配置",
                                    "text": (
                                        "订阅进度、总集数和媒体识别会调用 TMDB。"
                                        "如果日志里出现 TMDB 连接或鉴权失败，可以在这里替换 API Key。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "tmdb_api_key",
                                        "label": "TMDB API Key",
                                        "type": "password",
                                        "placeholder": "留空使用 MoviePilot 全局 TMDB API Key",
                                        "hint": "仅在需要为本插件单独指定 TMDB Key 时填写；保存后会覆盖本次运行时 TMDB Key",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "tmdb_api_domain",
                                        "label": "TMDB API 域名",
                                        "placeholder": "api.themoviedb.org",
                                        "hint": "可填 api.themoviedb.org 或可访问的反代域名",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },

                    # ── 订阅设置 ──
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "interval_value",
                                        "label": "检查间隔",
                                        "type": "number",
                                        "placeholder": "60",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "interval_unit",
                                        "label": "间隔单位",
                                        "items": [
                                            {"title": "分钟", "value": "minutes"},
                                            {"title": "小时", "value": "hours"},
                                            {"title": "天", "value": "days"},
                                        ],
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cloud_types",
                                        "label": "网盘类型（逗号分隔）",
                                        "placeholder": "115,aliyun,quark",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "quality_keywords",
                                        "label": "质量关键词（逗号分隔，为空不过滤）",
                                        "placeholder": "4K,2160p,HDR,蓝光",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "exclude_keywords",
                                        "label": "排除关键词（逗号分隔）",
                                        "placeholder": "预告,花絮,CAM,枪版",
                                    }
                                }]
                            },
                        ]
                    },

                    # ── 快速订阅管理 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "title": "订阅管理（保存后生效）",
                                    "text": (
                                        "在下方填写信息后点击保存即可添加/取消订阅。"
                                        "操作完成后相关字段会自动清空。"
                                        "\nTMDB ID 可在详情页的订阅表格中查看。"
                                    ),
                                }
                            }]
                        }]
                    },
                    # 取消订阅
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_cancel_tmdb_id",
                                        "label": "取消订阅 — TMDB ID",
                                        "placeholder": "输入要取消的订阅 TMDB ID",
                                        "hint": "保存后该 TMDB ID 对应的订阅会被移除",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_cancel_season",
                                        "label": "季（可选）",
                                        "type": "number",
                                        "placeholder": "留空=全部",
                                    }
                                }]
                            },
                        ]
                    },
                    # 添加订阅
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_add_title",
                                        "label": "添加订阅 — 标题",
                                        "placeholder": "如：成何体统",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_add_tmdb_id",
                                        "label": "TMDB ID",
                                        "type": "number",
                                        "placeholder": "必填",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 2},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "action_add_type",
                                        "label": "类型",
                                        "items": [
                                            {"title": "电视剧", "value": "电视剧"},
                                            {"title": "电影", "value": "电影"},
                                        ],
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_add_season",
                                        "label": "季",
                                        "type": "number",
                                        "placeholder": "电视剧填",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "action_add_year",
                                        "label": "年份",
                                        "type": "number",
                                        "placeholder": "如 2026",
                                        "hint": "重要：防止同名剧混淆",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },

                    # ── 订阅列表 JSON ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextarea",
                                "props": {
                                    "model": "subscriptions",
                                    "label": "订阅列表（JSON 格式，高级用户直接编辑）",
                                    "placeholder": '[{"title": "剑来", "tmdb_id": 93740, "media_type": "电视剧", "season": 2, "year": 2025, "media_category": "ongoing"}]',
                                    "rows": 6,
                                    "hint": "推荐使用上方的快速管理或 TG 智能体，JSON 用于高级编辑。",
                                    "persistent-hint": True,
                                }
                            }]
                        }]
                    },

                    # ── 使用说明 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "success",
                                    "variant": "tonal",
                                    "title": "配置完成后的使用流程",
                                    "text": (
                                        "1. 在 Telegram 对话中说「搜索xxx的115资源」"
                                        "\n2. AI 找到资源后，电影会直接转存到电影临时目录"
                                        "\n3. 电视剧会先问你「连载剧还是老剧？」，然后存到对应临时目录"
                                        "\n4. CloudDrive2 自动同步，文件出现在 NAS 挂载路径"
                                        "\n5. MoviePilot 目录监控自动刮削（海报/NFO）、重命名、移动到最终媒体库"
                                        "\n6. 网盘订阅功能：添加订阅后自动定期搜索缺失集数并转存"
                                    ),
                                }
                            }]
                        }]
                    },

                    # ─────────────────────────────────────────
                    # 备用云盘降级转存（CD2）配置区
                    # ─────────────────────────────────────────
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "备用云盘降级转存（CloudDrive2）",
                                    "text": (
                                        "当 115 直接搜索失败时，自动在阿里云盘/123云盘/夸克/百度网盘搜索资源，"
                                        "先由插件登录对应网盘并把分享链接保存到临时目录，再通过 CloudDrive2 跨云复制到 115。\n"
                                        "需要插件内对应网盘登录可用、CloudDrive2 已挂载目标云盘，且 115 目标路径存在。\n"
                                        "目标路径格式为 CD2 挂载路径（如 /115/待整理电影），请在下方按你的挂载结构填写。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "fallback_enabled",
                                        "label": "启用降级转存",
                                        "hint": "115 搜不到时自动启用备用云盘",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "fallback_clouds",
                                        "label": "备用云盘（可多选）",
                                        "items": [
                                            {"title": "阿里云盘", "value": "阿里云盘"},
                                            {"title": "123云盘", "value": "123云盘"},
                                            {"title": "夸克", "value": "夸克"},
                                            {"title": "百度网盘", "value": "百度网盘"},
                                        ],
                                        "multiple": True,
                                        "chips": True,
                                        "hint": "这里只决定哪些云盘允许参与；实际先后顺序由下方优先级控制",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextField",
                                "props": {
                                    "model": "fallback_cloud_priority",
                                    "label": "降级优先级",
                                    "placeholder": "阿里云盘,百度网盘,123云盘,夸克",
                                    "hint": (
                                        "逗号分隔，只对上方已勾选的云盘生效；"
                                        "未写到但已勾选的云盘会按默认顺序排到后面。"
                                        "夸克经由 AList/WebDAV 较慢，建议放最后兜底。"
                                    ),
                                    "persistent-hint": True,
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 5},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_host",
                                        "label": "CD2 地址",
                                        "placeholder": "clouddrive.example.local 或 192.168.1.10",
                                        "hint": "CloudDrive2 服务器 IP 或域名",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_port",
                                        "label": "CD2 端口",
                                        "placeholder": "19798",
                                        "hint": "默认 19798",
                                        "persistent-hint": True,
                                        "type": "number",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 5},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_token",
                                        "label": "CD2 JWT Token（可选）",
                                        "placeholder": "可留空，优先使用下方账号密码自动获取",
                                        "hint": "如手动填写，建议使用 CD2 GetToken 返回的 JWT；36 位令牌管理 token 可能无法用于 gRPC",
                                        "persistent-hint": True,
                                        "type": "password",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_username",
                                        "label": "CD2 用户名（推荐）",
                                        "placeholder": "CloudDrive2 登录用户名",
                                        "hint": "填写后插件会自动换取 gRPC JWT",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_password",
                                        "label": "CD2 密码（推荐）",
                                        "placeholder": "CloudDrive2 登录密码",
                                        "hint": "仅用于向 CD2 本机服务换取 JWT",
                                        "persistent-hint": True,
                                        "type": "password",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_totp_code",
                                        "label": "CD2 2FA/TOTP（可选）",
                                        "placeholder": "开启二次验证时填写当前验证码",
                                        "hint": "未开启二次验证可留空",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "aliyun_staging_path",
                                        "label": "阿里云盘临时目录（CD2 路径）",
                                        "placeholder": "/阿里云盘/MP临时转存",
                                        "hint": "分享链接将保存到此目录，转存完成后自动清理",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "123_staging_path",
                                        "label": "123云盘临时目录（CD2 路径）",
                                        "placeholder": "/123云盘/MP临时转存",
                                        "hint": "分享链接将保存到此目录，转存完成后自动清理",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "quark_staging_path",
                                        "label": "夸克临时目录（CD2 路径）",
                                        "placeholder": "/WebDAV/kuake/MP临时转存",
                                        "hint": "CD2 复制使用此路径；若是 /WebDAV/kuake/MP临时转存，插件会自动保存到夸克根目录 /MP临时转存",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "baidu_staging_path",
                                        "label": "百度网盘临时目录（CD2 路径）",
                                        "placeholder": "/百度网盘/MP临时转存",
                                        "hint": "插件先把百度分享链接保存到此目录，转存完成后自动清理",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_target_movie_path",
                                        "label": "115 新电影目标目录（CD2 路径）",
                                        "placeholder": "/115/待整理电影",
                                        "hint": "降级转存复制到 115 时使用；留空则禁用对应分类的降级转存",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_target_old_movie_path",
                                        "label": "115 老电影目标目录（CD2 路径）",
                                        "placeholder": "/115/待整理老电影",
                                        "hint": "分界年份之前的电影降级转存目标目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_target_ongoing_path",
                                        "label": "115 连载剧目标目录（CD2 路径）",
                                        "placeholder": "/115/待整理连载剧",
                                        "hint": "连载剧降级转存目标目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_target_archive_path",
                                        "label": "115 老剧/完结剧目标目录（CD2 路径）",
                                        "placeholder": "/115/待整理老剧",
                                        "hint": "老剧或完结剧降级转存目标目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_local_mount_115",
                                        "label": "115 在本容器内的挂载根目录（可选）",
                                        "placeholder": "/CloudDrive/115",
                                        "hint": "留空则自动探测常见 CloudDrive 挂载位置；多个用英文逗号分隔",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "title": "插件闭环整理（试运行）",
                                    "text": (
                                        "打开后，115 助手会把待入库文件从上方 115 目标目录移动到正式媒体库，"
                                        "并由插件完成重命名、基础 NFO/海报写入、Plex/Emby 局部刷新和入库通知。\n"
                                        "跑通前不要删除 MoviePilot 本体的目录整理配置；未开启或未配置正式库目录时仍回退到 MP 原生整理。"
                                    ),
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "closed_loop_organize_enabled",
                                        "label": "启用插件闭环整理",
                                        "hint": "试运行开关；关闭时继续使用 MP 原生整理",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "closed_loop_scrape_metadata",
                                        "label": "写入基础 NFO/海报",
                                        "hint": "仅写剧集/季级 NFO 与海报，不写每集海报",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "closed_loop_movie_library_path",
                                        "label": "新电影正式库目录（CD2 路径）",
                                        "placeholder": "/115影视库/电影/新电影",
                                        "hint": "闭环整理移动到这里；留空则对应分类回退 MP 原生整理",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "closed_loop_old_movie_library_path",
                                        "label": "老电影正式库目录（CD2 路径）",
                                        "placeholder": "/115影视库/电影/老电影",
                                        "hint": "分界年份之前电影的正式库目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "closed_loop_ongoing_library_path",
                                        "label": "连载剧正式库目录（CD2 路径）",
                                        "placeholder": "/115影视库/电视剧/连载剧",
                                        "hint": "闭环整理会创建 剧名 (年份)/Season 1",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "closed_loop_archive_library_path",
                                        "label": "老剧/完结剧正式库目录（CD2 路径）",
                                        "placeholder": "/115影视库/电视剧/完结剧",
                                        "hint": "老剧或完结剧闭环整理目标目录",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    # ── CD2 巡查同步 ──
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VDivider",
                                "props": {"class": "my-2"}
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "cd2_watch_enabled",
                                        "label": "启用 CD2 云盘巡查同步",
                                        "hint": "定期扫描指定云盘目录，将缺失集数增量复制到115；默认依赖本地巡查索引，尽量少读115目标盘。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "cd2_watch_interval_minutes",
                                        "label": "CD2 巡查间隔（分钟）",
                                        "type": "number",
                                        "placeholder": "120",
                                        "hint": "建议 120 分钟；手动点击详情页里的“立即巡查”不受这里限制。",
                                        "persistent-hint": True,
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": "巡查规则在下方「详情页」中管理，点击插件右上角「详情」按钮进入，可逐条添加、删除规则。",
                                }
                            }]
                        }]
                    },
                ]
            }
        ], {
            "enabled": False,
            "cookie_source": "auto",
            "cookies": "",
            "notify": True,
            "notice_same_title_window_minutes": 360,
            "notice_same_title_max": 2,
            "notice_global_window_minutes": 60,
            "notice_global_max": 8,
            "default_cid": "0",
            "movie_staging_cid": "",
            "old_movie_staging_cid": "",
            "ongoing_staging_cid": "",
            "archive_staging_cid": "",
            "movie_year_threshold": 2025,
            "pansou_url": "",
            "pansou_auth_user": "",
            "pansou_auth_pass": "",
            "tmdb_api_key": "",
            "tmdb_api_domain": "api.themoviedb.org",
            "interval_value": 60,
            "interval_unit": "minutes",
            "batch_check_enabled": True,
            "batch_size": 3,
            "batch_interval_minutes": 25,
            "batch_direct_115_limit": 0,
            "batch_cd2_copy_limit": 3,
            "batch_fallback_cloud_limit": 1,
            "rate_limit_cooldown_minutes": 60,
            "airing_skip_enabled": True,
            "airing_window_start_hour": 18,
            "airing_idle_probe_hours": 6,
            "rate_limit_escalation_enabled": True,
            "cloud_types": "115",
            "quality_keywords": "",
            "exclude_keywords": "预告,花絮,CAM,枪版",
            "subscriptions": "[]",
            "action_cancel_tmdb_id": "",
            "action_cancel_season": "",
            "action_add_title": "",
            "action_add_tmdb_id": "",
            "action_add_type": "电视剧",
            "action_add_season": "",
            "action_add_year": "",
            # ── CD2 降级配置默认值 ──
            "fallback_enabled": False,
            "fallback_clouds": [],
            "fallback_cloud_priority": "阿里云盘,百度网盘,123云盘,夸克",
            "cd2_host": "",
            "cd2_port": 19798,
            "cd2_token": "",
            "cd2_username": "",
            "cd2_password": "",
            "cd2_totp_code": "",
            "aliyun_staging_path": "/阿里云盘/MP临时转存",
            "123_staging_path": "/123云盘/MP临时转存",
            "quark_staging_path": "/夸克网盘/MP临时转存",
            "baidu_staging_path": "/百度网盘/MP临时转存",
            "aliyun_token": "",
            "quark_cookie": "",
            "baidu_cookie": "",
            "cd2_target_movie_path": "",
            "cd2_target_old_movie_path": "",
            "cd2_target_ongoing_path": "",
            "cd2_target_archive_path": "",
            "cd2_local_mount_115": "",
            # ── 插件闭环整理默认值 ──
            "closed_loop_organize_enabled": False,
            "closed_loop_scrape_metadata": True,
            "closed_loop_movie_library_path": "",
            "closed_loop_old_movie_library_path": "",
            "closed_loop_ongoing_library_path": "",
            "closed_loop_archive_library_path": "",
            # ── CD2 巡查同步默认值 ──
            "cd2_watch_enabled": False,
            "cd2_watch_interval_minutes": 120,
            "cd2_watch_rules": "[]",
        }

    # ================================================================
    #  详情页 (get_page)
    # ================================================================

    def _get_mp_log_files(self) -> List[dict]:
        """返回可读取的 MP 日志文件，按修改时间倒序。"""
        candidates: List[Path] = []
        for base in [
            getattr(settings, "LOG_PATH", None),
            Path("/config/logs"),
            Path("/moviepilot/logs"),
            Path("/app/logs"),
        ]:
            if not base:
                continue
            try:
                base_path = Path(base)
            except Exception:
                continue
            if not base_path.exists() or not base_path.is_dir():
                continue
            for pattern in ("*.log", "*.log.*", "*.txt"):
                candidates.extend(base_path.glob(pattern))

        seen = set()
        files = []
        for path in candidates:
            try:
                if not path.is_file():
                    continue
                key = str(path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                stat = path.stat()
                files.append({
                    "name": path.name,
                    "path": str(path),
                    "size": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
            except Exception:
                continue
        files.sort(key=lambda item: item.get("mtime", ""), reverse=True)
        return files

    @staticmethod
    def _tail_text_file(path: Path, lines: int = 400) -> List[str]:
        """读取文本文件尾部，避免一次性加载大日志。"""
        lines = max(1, int(lines or 400))
        block_size = 8192
        data = b""
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            position = f.tell()
            while position > 0 and data.count(b"\n") <= lines:
                read_size = min(block_size, position)
                position -= read_size
                f.seek(position)
                data = f.read(read_size) + data
                if len(data) > 8 * 1024 * 1024:
                    break
        text = data.decode("utf-8", errors="replace")
        return text.splitlines()[-lines:]

    @staticmethod
    def _build_logs_page_html() -> str:
        """日志查看器 HTML。"""
        return r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>115网盘助手 - MP日志</title>
  <style>
    :root { color-scheme: dark; --bg:#101418; --panel:#171d23; --line:#2a333d; --text:#e8edf2; --muted:#8c98a5; --accent:#64b5f6; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--text); }
    header { padding: 18px 20px 12px; border-bottom: 1px solid var(--line); background: #12181e; position: sticky; top: 0; z-index: 2; }
    h1 { margin: 0 0 12px; font-size: 20px; font-weight: 650; }
    .toolbar { display: grid; grid-template-columns: minmax(160px, 1fr) minmax(180px, 1.4fr) 110px auto auto auto auto; gap: 10px; align-items: center; }
    select, input, button { height: 36px; border: 1px solid var(--line); border-radius: 6px; background: var(--panel); color: var(--text); padding: 0 10px; font-size: 14px; }
    button { cursor: pointer; background: #1f2a33; }
    button.primary { background: #1565c0; border-color: #1976d2; }
    button:hover { border-color: var(--accent); }
    main { padding: 14px 20px 22px; }
    .meta { color: var(--muted); font-size: 13px; margin-bottom: 10px; min-height: 20px; }
    pre { margin: 0; padding: 14px; min-height: calc(100vh - 145px); overflow: auto; border: 1px solid var(--line); border-radius: 8px; background: #070a0d; color: #d7dde4; line-height: 1.5; font-size: 13px; white-space: pre-wrap; word-break: break-word; }
    .error { color: #ff8a80; }
    @media (max-width: 900px) { .toolbar { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <header>
    <h1>MoviePilot 日志</h1>
    <div class="toolbar">
      <select id="source"></select>
      <input id="keyword" placeholder="关键词，空格分隔：入库 115助手 降级">
      <input id="lines" type="number" min="50" max="3000" value="500">
      <button onclick="preset('入库 整理 转移')">入库</button>
      <button onclick="preset('115助手 降级')">115</button>
      <button onclick="preset('ERROR Exception Traceback 失败 异常')">错误</button>
      <button class="primary" onclick="loadLogs()">刷新</button>
    </div>
  </header>
  <main>
    <div class="meta" id="meta">准备读取日志...</div>
    <pre id="log"></pre>
  </main>
  <script>
    function headers() {
      var token = localStorage.getItem('token') || localStorage.getItem('access_token') || '';
      var h = {'Content-Type': 'application/json'};
      if (token) h['Authorization'] = 'Bearer ' + token;
      return h;
    }
    function preset(value) {
      document.getElementById('keyword').value = value;
      loadLogs();
    }
    function setSources(sources, selected) {
      var sel = document.getElementById('source');
      var current = sel.value || selected;
      sel.innerHTML = '';
      (sources || []).forEach(function(item) {
        var opt = document.createElement('option');
        opt.value = item.path;
        opt.textContent = item.name + ' (' + item.mtime + ')';
        if (item.path === current) opt.selected = true;
        sel.appendChild(opt);
      });
    }
    function loadLogs() {
      var source = document.getElementById('source').value || '';
      var keyword = document.getElementById('keyword').value || '';
      var lines = document.getElementById('lines').value || '500';
      var url = '/api/v1/plugin/Net115Helper/mp_logs?lines=' + encodeURIComponent(lines)
        + '&keyword=' + encodeURIComponent(keyword)
        + '&source=' + encodeURIComponent(source);
      document.getElementById('meta').textContent = '读取中...';
      fetch(url, {headers: headers(), credentials: 'include'})
        .then(function(r) { return r.json(); })
        .then(function(d) {
          setSources(d.sources || [], d.source && d.source.path);
          if (!d.success) {
            document.getElementById('meta').innerHTML = '<span class="error">' + (d.message || '读取失败') + '</span>';
            document.getElementById('log').textContent = '';
            return;
          }
          var count = (d.data || []).length;
          var src = d.source ? d.source.name + ' / ' + d.source.mtime : '';
          document.getElementById('meta').textContent = '来源：' + src + '，显示 ' + count + ' 行';
          document.getElementById('log').textContent = (d.data || []).join('\n');
        })
        .catch(function(e) {
          document.getElementById('meta').innerHTML = '<span class="error">请求失败：' + e + '</span>';
        });
    }
    loadLogs();
    setInterval(loadLogs, 30000);
  </script>
</body>
</html>"""

    def get_page(self) -> Optional[List[dict]]:
        """
        构建详情页：
        - 顶部 3 张卡片显示临时目录 CID
        - 下方表格显示订阅列表（含进度条）
        - 包含扫码登录区块（JS 控制显示）
        """
        # ── 顶部：设置概览 + 临时目录卡片 ──
        settings_card = self._build_settings_summary()
        cid_cards = self._build_cid_cards()

        # ── 订阅数据 ──
        table_rows = self._build_subscription_table()

        # 表头
        headers = [
            {"title": "剧名", "key": "title", "sortable": True},
            {"title": "TMDB", "key": "tmdb_id", "sortable": False},
            {"title": "季", "key": "season", "sortable": True},
            {"title": "进度", "key": "progress", "sortable": False},
            {"title": "已有/总集数", "key": "episode_text", "sortable": False},
            {"title": "上次发现", "key": "last_found", "sortable": True},
            {"title": "状态", "key": "status", "sortable": False},
            {"title": "操作", "key": "actions", "sortable": False},
        ]

        page: List[dict] = []

        # ── 设置概览行 ──
        if settings_card:
            page.append(settings_card)

        # ── 网盘登录入口 ──
        page.append(self._build_qrcode_card())

        # ── CID 卡片行 ──
        if cid_cards:
            page.append({
                "component": "VRow",
                "props": {"class": "mb-4"},
                "content": cid_cards,
            })

        page.append(self._build_subscription_check_card(len(table_rows)))
        page.append(self._build_log_viewer_card())

        # ── 订阅表格 ──
        if table_rows:
            page.append({
                "component": "VCard",
                "props": {"title": f"网盘订阅列表（{len(table_rows)} 个）"},
                "content": [{
                    "component": "VCardText",
                    "content": [{
                        "component": "VTable",
                        "props": {"density": "comfortable"},
                        "content": [
                            # thead
                            {
                                "component": "thead",
                                "content": [{
                                    "component": "tr",
                                    "content": [
                                        {
                                            "component": "th",
                                            "props": {"class": "text-start"},
                                            "text": h["title"],
                                        }
                                        for h in headers
                                    ]
                                }]
                            },
                            # tbody
                            {
                                "component": "tbody",
                                "content": [
                                    self._build_table_row(row)
                                    for row in table_rows
                                ]
                            },
                        ]
                    }]
                }]
            })
        else:
            page.append({
                "component": "VCard",
                "props": {"title": "网盘订阅列表"},
                "content": [{
                    "component": "VCardText",
                    "content": [{
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": "暂无订阅。可通过 TG 智能体添加，或点击右上角设置按钮，在「订阅管理」区域快速添加。",
                        }
                    }]
                }]
            })

        # ── 历史记录表格 ──
        history_rows = self._build_history_table()
        if history_rows:
            history_total = len(history_rows)
            history_preview_limit = 5
            history_has_extra = history_total > history_preview_limit
            history_toggle_js = (
                "var rows=document.querySelectorAll('[data-net115-history-extra=\"1\"]');"
                "var expanded=false;"
                "rows.forEach(function(r){"
                "  if(r.style.display==='none'||!r.style.display){expanded=true;}"
                "});"
                "rows.forEach(function(r){r.style.display=expanded?'table-row':'none';});"
                "var label=document.getElementById('net115-history-toggle-label');"
                "if(label){label.textContent=expanded?'收起':'查看全部';}"
            )
            history_headers = [
                {"title": "名称", "key": "title"},
                {"title": "类型", "key": "media_type"},
                {"title": "完结原因", "key": "reason"},
                {"title": "完结时间", "key": "completed_time"},
                {"title": "操作", "key": "actions"},
            ]
            page.append({
                "component": "VCard",
                "props": {"title": f"已完结（{history_total} 个）", "class": "mt-4"},
                "content": [{
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex align-center justify-space-between mb-2",
                            },
                            "content": [
                                {
                                    "component": "span",
                                    "props": {"class": "text-caption text-medium-emphasis"},
                                    "text": (
                                        f"默认显示最近 {history_preview_limit} 个，"
                                        "展开后查看全部历史。"
                                        if history_has_extra
                                        else "按完结时间倒序显示。"
                                    ),
                                },
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "variant": "text",
                                        "size": "small",
                                        "prepend-icon": "mdi-chevron-down",
                                        "onclick": history_toggle_js,
                                        "style": "" if history_has_extra else "display:none",
                                    },
                                    "content": [{
                                        "component": "span",
                                        "props": {"id": "net115-history-toggle-label"},
                                        "text": "查看全部",
                                    }],
                                },
                            ],
                        },
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [{
                                        "component": "tr",
                                        "content": [
                                            {
                                                "component": "th",
                                                "props": {"class": "text-start"},
                                                "text": h["title"],
                                            }
                                            for h in history_headers
                                        ]
                                    }]
                                },
                                {
                                    "component": "tbody",
                                    "content": [
                                        self._build_history_table_row(
                                            row,
                                            extra_props={
                                                "data-net115-history-extra": "1",
                                                "style": "display:none",
                                            } if idx >= history_preview_limit else None,
                                        )
                                        for idx, row in enumerate(history_rows)
                                    ]
                                },
                            ]
                        }
                    ]
                }]
            })

        # ── CD2 巡查规则卡片 ──
        page.append(self._build_cd2_watch_card())

        return page

    def _build_subscription_check_card(self, subscription_count: int) -> dict:
        """构建 115 订阅剧巡检手动触发卡片。"""
        token_js = "(localStorage.getItem('token')||localStorage.getItem('access_token')||'')"
        auth_js = (
            f"var t={token_js};"
            "var h={'Content-Type':'application/json'};"
            "if(t){h['Authorization']='Bearer '+t;}"
        )
        parse_json_js = (
            ".then(function(r){"
            "return r.json().then(function(d){d._ok=r.ok;return d;})"
            ".catch(function(){return {_ok:r.ok,message:r.status+' '+r.statusText};});"
            "})"
        )
        error_msg_js = "(d.message||d.detail||d.error||JSON.stringify(d))"
        run_now_js = (
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/run_subscribe_now',"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'115订阅巡检已在后台启动，请稍后查看日志');}"
            f"else{{alert('启动失败: '+{error_msg_js});}}}})"
            ".catch(function(e){alert('请求失败: '+e);})"
        )
        organize_now_js = (
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/organize_pending_now',"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'待入库整理已在后台启动，请稍后查看日志');}"
            f"else{{alert('启动失败: '+{error_msg_js});}}}})"
            ".catch(function(e){alert('请求失败: '+e);})"
        )
        return {
            "component": "VCard",
            "props": {"class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center justify-space-between"},
                    "content": [
                        {"component": "span", "text": f"115 订阅剧巡检（{subscription_count} 个订阅）"},
                        {
                            "component": "div",
                            "props": {"class": "d-flex align-center ga-2"},
                            "content": [
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "color": "primary",
                                        "size": "small",
                                        "variant": "tonal",
                                        "prepend-icon": "mdi-magnify-scan",
                                        "disabled": subscription_count <= 0,
                                        "onclick": run_now_js,
                                    },
                                    "text": "立即巡检",
                                },
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "color": "success",
                                        "size": "small",
                                        "variant": "tonal",
                                        "prepend-icon": "mdi-file-sync-outline",
                                        "disabled": subscription_count <= 0,
                                        "onclick": organize_now_js,
                                    },
                                    "text": "整理待入库",
                                },
                            ],
                        },
                    ],
                },
            ],
        }

    def _build_log_viewer_card(self) -> dict:
        """构建 MoviePilot 日志查看入口。"""
        return {
            "component": "VCard",
            "props": {"class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center justify-space-between"},
                    "content": [
                        {"component": "span", "text": "MoviePilot 日志"},
                        {
                            "component": "VBtn",
                            "props": {
                                "color": "info",
                                "size": "small",
                                "variant": "tonal",
                                "prepend-icon": "mdi-text-box-search-outline",
                                "onclick": "window.open('/api/v1/plugin/Net115Helper/logs_page','_blank')",
                            },
                            "text": "打开日志查看器",
                        },
                    ],
                },
                {
                    "component": "VCardText",
                    "content": [{
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "density": "compact",
                            "text": "可直接查看 MP 最近日志，支持入库、115助手、降级转存等关键词筛选。",
                        },
                    }],
                },
            ],
        }

    def _build_cd2_watch_card(self) -> dict:
        """构建 CD2 巡查规则管理卡片（详情页）"""
        rules = self._cd2_watch_rules or []
        watch_index = self._load_cd2_watch_index()
        rule_states = watch_index.get("rules", {}) if isinstance(watch_index, dict) else {}
        tracked_shows = 0
        completed_shows = 0
        if isinstance(rule_states, dict):
            for item in rule_states.values():
                if not isinstance(item, dict):
                    continue
                shows = item.get("shows")
                if isinstance(shows, dict):
                    tracked_shows += len(shows)
                    completed_shows += len([
                        show for show in shows.values()
                        if isinstance(show, dict) and show.get("completed")
                    ])
        token_js = "(localStorage.getItem('token')||localStorage.getItem('access_token')||'')"
        auth_js = (
            f"var t={token_js};"
            "var h={'Content-Type':'application/json'};"
            "if(t){h['Authorization']='Bearer '+t;}"
        )
        parse_json_js = (
            ".then(function(r){"
            "return r.json().then(function(d){d._ok=r.ok;return d;})"
            ".catch(function(){return {_ok:r.ok,message:r.status+' '+r.statusText};});"
            "})"
        )
        error_msg_js = "(d.message||d.detail||d.error||JSON.stringify(d))"

        # ── 已有规则列表：使用响应式卡片，避免手机竖屏横向表格把操作按钮挤出屏幕 ──
        rule_cards = []
        path_text_style = "word-break:break-all;white-space:normal;line-height:1.45;"
        action_style = "display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end;"
        for rule in rules:
            rule_key = self._cd2_watch_rule_key(rule)
            rule_id = str(rule.get("id") or rule_key)
            enabled = rule.get("enabled", True)
            rule_name = rule.get("name", "")
            delete_confirm = json.dumps(f"确定删除规则「{rule_name}」？", ensure_ascii=False)
            toggle_payload = json.dumps({
                "id": rule_id,
                "enabled": not enabled,
                "name": rule_name,
                "source_path": rule.get("source_path", ""),
                "dest_115_path": rule.get("dest_115_path", ""),
            }, ensure_ascii=False)
            delete_js = (
                f"if(confirm({delete_confirm})){{"
                f"{auth_js}"
                f"fetch('/api/v1/plugin/Net115Helper/delete_watch_rule?id='+encodeURIComponent({json.dumps(rule_id)})"
                f"+'&key='+encodeURIComponent({json.dumps(rule_key)}),"
                "{headers:h,credentials:'include'})"
                f"{parse_json_js}"
                ".then(function(d){if(d._ok&&d.success){alert(d.message||'规则已删除');location.reload();}"
                f"else{{alert('删除失败: '+{error_msg_js});}}}})"
                ".catch(function(e){alert('请求失败: '+e);})}"
            )
            toggle_js = (
                f"{auth_js}"
                "fetch('/api/v1/plugin/Net115Helper/save_watch_rule',"
                "{method:'POST',headers:h,credentials:'include',body:JSON.stringify("
                f"{toggle_payload}"
                ")})"
                f"{parse_json_js}"
                ".then(function(d){if(d._ok&&d.success){alert(d.message||'规则已保存');location.reload();}"
                f"else{{alert('操作失败: '+{error_msg_js});}}}})"
                ".catch(function(e){alert('请求失败: '+e);})"
            )
            rule_cards.append({
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "mb-2",
                },
                "content": [{
                    "component": "VCardText",
                    "props": {"class": "py-3"},
                    "content": [{
                        "component": "VRow",
                        "props": {"class": "align-center"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-subtitle-2 font-weight-medium",
                                            "style": path_text_style,
                                        },
                                        "text": rule_name or "未命名规则",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "color": "success" if enabled else "default",
                                            "size": "x-small",
                                            "variant": "tonal",
                                            "class": "mt-1",
                                        },
                                        "text": "启用中" if enabled else "已停用",
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "源路径（CD2）"},
                                    {
                                        "component": "div",
                                        "props": {"class": "text-body-2", "style": path_text_style},
                                        "text": rule.get("source_path", ""),
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "115 目标父目录"},
                                    {
                                        "component": "div",
                                        "props": {"class": "text-body-2", "style": path_text_style},
                                        "text": rule.get("dest_115_path", ""),
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2, "style": action_style},
                                "content": [
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "color": "success" if not enabled else "warning",
                                            "size": "small",
                                            "variant": "tonal",
                                            "prepend-icon": "mdi-play-circle-outline" if not enabled else "mdi-pause-circle-outline",
                                            "onclick": toggle_js,
                                        },
                                        "text": "启用" if not enabled else "停用",
                                    },
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "color": "error",
                                            "size": "small",
                                            "variant": "tonal",
                                            "prepend-icon": "mdi-delete-outline",
                                            "onclick": delete_js,
                                        },
                                        "text": "删除",
                                    },
                                ],
                            },
                        ],
                    }],
                }],
            })

        # ── 已有规则 ──
        if rule_cards:
            rules_table = {
                "component": "div",
                "props": {"class": "mb-4"},
                "content": rule_cards,
            }
        else:
            rules_table = {
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal", "class": "mb-4"},
                "text": "暂无巡查规则，在上方填写并点击「保存新规则」。",
            }

        # ── 添加规则表单（纯 HTML input + JS fetch）──
        add_js = (
            f"{auth_js}"
            "var name=document.getElementById('cd2w_name').value.trim();"
            "var src=document.getElementById('cd2w_src').value.trim();"
            "var dst=document.getElementById('cd2w_dst').value.trim();"
            "if(!name||!src||!dst){alert('三个字段都必须填写');return;}"
            "fetch('/api/v1/plugin/Net115Helper/save_watch_rule',"
            "{method:'POST',headers:h,credentials:'include',"
            "body:JSON.stringify({enabled:true,name:name,source_path:src,dest_115_path:dst})})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){"
            "alert((d.message||'规则已保存')+'\\n规则：'+((d.data&&d.data.name)||name));"
            "document.getElementById('cd2w_name').value='';"
            "document.getElementById('cd2w_src').value='';"
            "document.getElementById('cd2w_dst').value='';"
            "location.reload();"
            "}"
            f"else{{alert('添加失败: '+{error_msg_js});}}}})"
            ".catch(function(e){alert('请求失败: '+e);})"
        )

        add_form = {
            "component": "VRow",
            "props": {"class": "mt-2"},
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 3},
                    "content": [
                        {"component": "div", "props": {"class": "text-caption mb-1"}, "text": "规则名称"},
                        {
                            "component": "input",
                            "props": {
                                "id": "cd2w_name",
                                "type": "text",
                                "placeholder": "夸克热播剧",
                                "style": "width:100%;padding:9px 12px;border:1px solid rgba(128,128,128,.45);border-radius:4px;",
                            }
                        },
                    ]
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 4},
                    "content": [
                        {"component": "div", "props": {"class": "text-caption mb-1"}, "text": "源路径（CD2 路径）"},
                        {
                            "component": "input",
                            "props": {
                                "id": "cd2w_src",
                                "type": "text",
                                "placeholder": "/夸克云盘/热播剧",
                                "style": "width:100%;padding:9px 12px;border:1px solid rgba(128,128,128,.45);border-radius:4px;",
                            }
                        },
                    ]
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 4},
                    "content": [
                        {"component": "div", "props": {"class": "text-caption mb-1"}, "text": "115 目标父目录（CD2 路径）"},
                        {
                            "component": "input",
                            "props": {
                                "id": "cd2w_dst",
                                "type": "text",
                                "placeholder": "/115/待整理连载剧",
                                "style": "width:100%;padding:9px 12px;border:1px solid rgba(128,128,128,.45);border-radius:4px;",
                            }
                        },
                    ]
                },
                {
                    "component": "VCol",
                    "props": {"cols": 12, "md": 1, "class": "d-flex align-center"},
                    "content": [{
                        "component": "VBtn",
                        "props": {
                            "color": "primary",
                            "variant": "elevated",
                            "prepend-icon": "mdi-plus",
                            "style": "min-width:120px;",
                            "onclick": add_js,
                        },
                        "text": "保存新规则",
                    }]
                },
            ]
        }
        new_rule_js = (
            "var el=document.getElementById('cd2w_name');"
            "if(el){el.scrollIntoView({behavior:'smooth',block:'center'});setTimeout(function(){el.focus();},200);}"
        )
        run_now_js = (
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/run_watch_now',"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'CD2巡查已在后台启动，请稍后查看日志');}"
            f"else{{alert('启动失败: '+{error_msg_js});}}}})"
            ".catch(function(e){alert('请求失败: '+e);})"
        )

        return {
            "component": "VCard",
            "props": {"class": "mt-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {
                        "class": "d-flex align-center justify-space-between",
                        "style": "flex-wrap:wrap;gap:8px;",
                    },
                    "content": [
                        {
                            "component": "span",
                            "props": {"style": "white-space:normal;line-height:1.35;"},
                            "text": f"CD2 云盘巡查同步（{len(rules)} 条规则）",
                        },
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex ga-2",
                                "style": "flex-wrap:wrap;",
                            },
                            "content": [
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "color": "primary",
                                        "size": "small",
                                        "variant": "text",
                                        "prepend-icon": "mdi-plus",
                                        "onclick": new_rule_js,
                                    },
                                    "text": "新增规则",
                                },
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "color": "primary",
                                        "size": "small",
                                        "variant": "tonal",
                                        "prepend-icon": "mdi-sync",
                                        "onclick": run_now_js,
                                    },
                                    "text": "立即同步",
                                },
                            ]
                        },
                    ]
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VAlert",
                            "props": {
                                "type": "info",
                                "variant": "tonal",
                                "density": "compact",
                                "class": "mb-3",
                                "text": (
                                    f"当前定时巡查间隔：{self._cd2_watch_interval_minutes} 分钟。"
                                    f"本地巡查索引已记录 {tracked_shows} 部，已标记完结 {completed_shows} 部。"
                                    "默认优先使用本地索引 + Plex/Emby/整理历史判断缺失，仅在首次同步或手动强刷时读取 115 目标目录。"
                                ),
                            },
                        },
                        {"component": "div", "props": {"class": "text-subtitle-2 mb-2"}, "text": "新增规则（填写后点击保存新规则）"},
                        add_form,
                        {"component": "VDivider", "props": {"class": "my-4"}},
                        rules_table,
                    ],
                },
            ]
        }

    def _build_history_table(self) -> List[dict]:
        """构建历史记录行数据"""
        history = self._load_history()
        if not history:
            return []

        rows = []
        for sub in reversed(history):  # 最新的在前
            completed_time = sub.get("completed_time", "")
            if completed_time:
                try:
                    dt = datetime.fromisoformat(completed_time)
                    completed_time = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    completed_time = completed_time[:16]

            rows.append({
                "title": sub.get("title", ""),
                "tmdb_id": sub.get("tmdb_id"),
                "season": sub.get("season"),
                "media_type": sub.get("media_type", "电视剧"),
                "reason": sub.get("completed_reason", "自动完结"),
                "completed_time": completed_time or "未知",
            })
        return rows

    # ── 扫码登录卡片 ──────────────────────────────────

    def _build_qrcode_card(self) -> dict:
        """构建统一的网盘登录入口。"""
        cd2_url = ""
        if self._cd2_host:
            host = self._cd2_host
            if not host.startswith(("http://", "https://")):
                host = f"http://{host}"
            cd2_url = f"{host}:{self._cd2_port}"

        cd2_open_js = (
            f"window.open({json.dumps(cd2_url)},'_blank')"
            if cd2_url
            else "alert('请先在插件设置中填写 CD2 地址和端口')"
        )

        return {
            "component": "VCard",
            "props": {
                "variant": "outlined",
                "class": "mb-4",
                "color": "primary",
            },
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center"},
                    "content": [
                        {"component": "VIcon", "props": {"icon": "mdi-cloud-key-outline", "class": "mr-2"}},
                        {"component": "span", "text": "网盘登录中心"},
                    ]
                },
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VAlert",
                            "props": {"type": "info", "variant": "tonal", "class": "mb-3"},
                            "text": (
                                "把所有账号入口集中在这里：115 用于主转存；阿里、夸克、百度在插件里登录并负责分享转存；"
                                "CloudDrive2 只负责把备用盘临时目录复制到 115。"
                            ),
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [{
                                        "component": "VBtn",
                                        "props": {
                                            "color": "primary",
                                            "variant": "elevated",
                                            "block": True,
                                            "onclick": "window.open('/api/v1/plugin/Net115Helper/qrcode_page','_blank')",
                                            "prepend-icon": "mdi-qrcode-scan",
                                        },
                                        "text": "115 扫码登录",
                                    }]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [{
                                        "component": "VBtn",
                                        "props": {
                                            "color": "orange",
                                            "variant": "elevated",
                                            "block": True,
                                            "onclick": "window.open('/api/v1/plugin/Net115Helper/aliyun_page','_blank')",
                                            "prepend-icon": "mdi-cloud-outline",
                                        },
                                        "text": "阿里云盘扫码登录",
                                    }]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [{
                                        "component": "VBtn",
                                        "props": {
                                            "color": "secondary",
                                            "variant": "tonal",
                                            "block": True,
                                            "onclick": cd2_open_js,
                                            "prepend-icon": "mdi-open-in-new",
                                        },
                                        "text": "打开 CloudDrive2",
                                    }]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [{
                                        "component": "VBtn",
                                        "props": {
                                            "color": "green",
                                            "variant": "tonal",
                                            "block": True,
                                            "onclick": (
                                                "fetch('/api/v1/plugin/Net115Helper/quark_login_status',{credentials:'include'})"
                                                ".then(r=>r.json()).then(d=>alert('夸克网盘：'+(d.message||'')))"
                                                ".catch(e=>alert('夸克网盘状态检查失败：'+e))"
                                            ),
                                            "prepend-icon": "mdi-cloud-check-outline",
                                        },
                                        "text": "检查夸克登录",
                                    }]
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [{
                                        "component": "VBtn",
                                        "props": {
                                            "color": "blue",
                                            "variant": "tonal",
                                            "block": True,
                                            "onclick": (
                                                "fetch('/api/v1/plugin/Net115Helper/baidu_login_status',{credentials:'include'})"
                                                ".then(r=>r.json()).then(d=>alert('百度网盘：'+(d.message||'')))"
                                                ".catch(e=>alert('百度网盘状态检查失败：'+e))"
                                            ),
                                            "prepend-icon": "mdi-cloud-check-outline",
                                        },
                                        "text": "检查百度登录",
                                    }]
                                },
                            ],
                        },
                        {
                            "component": "div",
                            "props": {"class": "text-caption text-medium-emphasis mt-2"},
                            "text": (
                                "夸克和百度 Cookie、CD2 账号、各网盘临时目录和 115 目标目录在插件设置中维护；"
                                "这里只放登录入口和状态检查，避免入口分散。"
                            ),
                        },
                    ]
                },
            ]
        }

    # ── 设置概览 ────────────────────────────────────────

    def _build_settings_summary(self) -> Optional[dict]:
        """构建设置概览卡片（检查间隔、网盘类型等）"""
        # 计算显示文本
        interval = self._interval_minutes
        if interval >= 1440 and interval % 1440 == 0:
            interval_text = f"每 {interval // 1440} 天"
        elif interval >= 60 and interval % 60 == 0:
            interval_text = f"每 {interval // 60} 小时"
        else:
            interval_text = f"每 {interval} 分钟"

        cloud_text = ", ".join(self._cloud_types) if self._cloud_types else "不限"
        quality_text = ", ".join(self._quality_keywords) if self._quality_keywords else "不限"
        exclude_text = ", ".join(self._exclude_keywords) if self._exclude_keywords else "无"
        notify_text = "开启" if self._notify else "关闭"
        cookie_text = self._cookie_source_display()
        active_cookie_text = getattr(self, "_active_cookie_source", "") or "未检测"
        tmdb_text = (
            f"{self._mask_secret(self._tmdb_api_key)} @ {self._tmdb_api_domain}"
            if self._tmdb_api_key
            else f"使用MP全局配置 @ {self._tmdb_api_domain}"
        )

        chips = [
            ("mdi-timer-outline", f"检查间隔: {interval_text}", "primary"),
            ("mdi-cloud-outline", f"网盘类型: {cloud_text}", "info"),
            ("mdi-bell-outline", f"通知: {notify_text}", "success" if self._notify else "grey"),
            ("mdi-cookie-outline", f"Cookie来源: {cookie_text} / 当前: {active_cookie_text}", "success" if active_cookie_text not in ("未检测", "无可用Cookie") else "warning"),
            ("mdi-movie-filter-outline", f"新/老电影分界: {self._movie_year_threshold}年", "secondary"),
            ("mdi-database-search-outline", f"TMDB: {tmdb_text}", "primary"),
        ]
        if self._quality_keywords:
            chips.append(("mdi-quality-high", f"质量: {quality_text}", "warning"))
        if self._exclude_keywords:
            chips.append(("mdi-filter-off-outline", f"排除: {exclude_text}", "error"))

        chip_components = []
        for icon, text, color in chips:
            chip_components.append({
                "component": "VChip",
                "props": {
                    "prepend-icon": icon,
                    "color": color,
                    "variant": "tonal",
                    "size": "small",
                    "class": "mr-2 mb-1",
                },
                "text": text,
            })

        return {
            "component": "VRow",
            "props": {"class": "mb-2"},
            "content": [{
                "component": "VCol",
                "props": {"cols": 12},
                "content": [{
                    "component": "div",
                    "props": {"class": "d-flex flex-wrap align-center"},
                    "content": chip_components,
                }]
            }]
        }

    def _build_cid_cards(self) -> List[dict]:
        """构建 CID 概览卡片"""
        cards_data = [
            ("新电影临时目录", self._movie_staging_cid, "mdi-movie-open-outline"),
            ("老电影临时目录", self._old_movie_staging_cid, "mdi-filmstrip-box"),
            ("连载剧临时目录", self._ongoing_staging_cid, "mdi-television-classic"),
            ("老剧临时目录", self._archive_staging_cid, "mdi-archive-outline"),
        ]
        cards = []
        for label, cid, icon in cards_data:
            display_cid = cid if cid else "未配置"
            color = "primary" if cid else "grey"
            cards.append({
                "component": "VCol",
                "props": {"cols": 12, "md": 3},
                "content": [{
                    "component": "VCard",
                    "props": {"variant": "outlined", "color": color},
                    "content": [
                        {
                            "component": "VCardTitle",
                            "props": {"class": "d-flex align-center"},
                            "content": [
                                {
                                    "component": "VIcon",
                                    "props": {"icon": icon, "class": "mr-2"},
                                },
                                {"component": "span", "text": label},
                            ]
                        },
                        {
                            "component": "VCardText",
                            "text": f"CID: {display_cid}",
                        }
                    ]
                }]
            })
        return cards

    def _build_subscription_table(self) -> List[dict]:
        """
        为每个订阅获取进度信息，返回用于渲染表格的行数据列表。
        使用 check_subscriptions 期间写入的缓存数据，避免详情页加载时实时查询 TMDB。
        """
        subscriptions = self._load_subscriptions()
        if not subscriptions:
            return []

        rows = []
        for sub in subscriptions:
            title = sub.get("title", "")
            tmdb_id = sub.get("tmdb_id")
            media_type_str = sub.get("media_type", "电视剧")
            season = sub.get("season")
            year = sub.get("year")
            last_found = sub.get("last_found", "")

            # 从缓存读取进度（check_subscriptions 期间写入）
            existing_count = sub.get("_cache_existing_count")
            total_episodes = sub.get("_cache_total_episodes")
            pending_count = sub.get("_cache_pending_count") or 0
            cache_updated = sub.get("_cache_updated")

            if existing_count is None:
                # 还没跑过 check_subscriptions，显示等待状态
                existing_count = 0

            progress = 0
            if total_episodes and total_episodes > 0:
                progress = round(existing_count / total_episodes * 100)
            episode_text = f"{existing_count}/{total_episodes or '?'}"
            if pending_count > 0:
                episode_text += f"（待入库 {pending_count}）"

            # 状态判断
            if total_episodes and existing_count >= total_episodes:
                status = "已完结"
                status_color = "success"
            elif pending_count > 0:
                status = "待入库"
                status_color = "info"
            elif existing_count > 0:
                status = "订阅中"
                status_color = "primary"
            elif cache_updated:
                status = "等待中"
                status_color = "warning"
            else:
                status = "待首检"
                status_color = "grey"

            # 上次发现时间 - 人性化
            last_found_display = ""
            if last_found:
                try:
                    dt = datetime.fromisoformat(last_found)
                    delta = datetime.now() - dt
                    if delta.days > 0:
                        last_found_display = f"{delta.days}天前"
                    elif delta.seconds >= 3600:
                        last_found_display = f"{delta.seconds // 3600}小时前"
                    else:
                        last_found_display = f"{delta.seconds // 60}分钟前"
                except Exception:
                    last_found_display = last_found[:10]

            # 标题显示（带年份）
            display_title = title
            if year:
                display_title += f" ({year})"

            rows.append({
                "title": display_title,
                "raw_title": title,
                "tmdb_id": tmdb_id,
                "season": f"S{season}" if season else "-",
                "raw_season": season,
                "progress": progress,
                "episode_text": episode_text,
                "pending_count": pending_count,
                "last_found": last_found_display or "从未",
                "status": status,
                "status_color": status_color,
            })
        return rows

    @staticmethod
    def _build_history_table_row(row: dict, extra_props: Optional[dict] = None) -> dict:
        """为单条历史归档构建一个 <tr>，附删除按钮（删除归档后可重新订阅）"""
        tmdb_id = str(row.get("tmdb_id") or "")
        raw_season = row.get("season")
        season_js = ""
        if raw_season not in (None, "", "-"):
            season_js = f"+'&season='+encodeURIComponent({json.dumps(str(raw_season), ensure_ascii=False)})"
        confirm_text = json.dumps(
            f"确定删除「{row.get('title') or tmdb_id}」的完结归档记录？删除后可重新订阅该剧。",
            ensure_ascii=False,
        )
        auth_js = (
            "var t=localStorage.getItem('token')||localStorage.getItem('access_token')||'';"
            "var h={};"
            "if(t){h['Authorization']='Bearer '+t;}"
        )
        parse_json_js = (
            ".then(function(r){"
            "return r.json().then(function(d){d._ok=r.ok;return d;})"
            ".catch(function(){return {_ok:r.ok,message:r.status+' '+r.statusText};});"
            "})"
        )
        delete_js = (
            f"if(confirm({confirm_text})){{"
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/delete_history?tmdb_id='+encodeURIComponent("
            f"{json.dumps(tmdb_id, ensure_ascii=False)})"
            f"{season_js},"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'已删除');location.reload();}"
            "else{alert('删除失败: '+(d.message||d.detail||d.error||JSON.stringify(d)));}})"
            ".catch(function(e){alert('请求失败: '+e);})}"
        )
        tr = {
            "component": "tr",
            "content": [
                {"component": "td", "text": row["title"]},
                {"component": "td", "text": row["media_type"]},
                {"component": "td", "text": row["reason"]},
                {"component": "td", "text": row["completed_time"]},
                {
                    "component": "td",
                    "content": [{
                        "component": "VBtn",
                        "props": {
                            "size": "x-small",
                            "color": "error",
                            "variant": "tonal",
                            "prepend-icon": "mdi-delete-outline",
                            "disabled": not bool(tmdb_id),
                            "onclick": delete_js,
                        },
                        "text": "删除",
                    }]
                },
            ]
        }
        if extra_props:
            tr["props"] = extra_props
        return tr

    @staticmethod
    def _build_table_row(row: dict) -> dict:
        """为单条订阅构建一个 <tr> Vuetify 组件"""
        tmdb_id = str(row.get("tmdb_id") or "")
        raw_season = row.get("raw_season")
        season_js = ""
        if raw_season not in (None, "", "-"):
            season_js = f"+'&season='+encodeURIComponent({json.dumps(str(raw_season), ensure_ascii=False)})"
        confirm_text = json.dumps(
            f"确定将「{row.get('raw_title') or row.get('title') or tmdb_id}」归档到历史记录？",
            ensure_ascii=False,
        )
        auth_js = (
            "var t=localStorage.getItem('token')||localStorage.getItem('access_token')||'';"
            "var h={};"
            "if(t){h['Authorization']='Bearer '+t;}"
        )
        parse_json_js = (
            ".then(function(r){"
            "return r.json().then(function(d){d._ok=r.ok;return d;})"
            ".catch(function(){return {_ok:r.ok,message:r.status+' '+r.statusText};});"
            "})"
        )
        archive_js = (
            f"if(confirm({confirm_text})){{"
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/archive_sub?tmdb_id='+encodeURIComponent("
            f"{json.dumps(tmdb_id, ensure_ascii=False)})"
            f"{season_js}"
            "+'&reason='+encodeURIComponent('手动归档'),"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'已归档');location.reload();}"
            "else{alert('归档失败: '+(d.message||d.detail||d.error||JSON.stringify(d)));}})"
            ".catch(function(e){alert('请求失败: '+e);})}"
        )
        organize_js = (
            f"{auth_js}"
            "fetch('/api/v1/plugin/Net115Helper/organize_pending_now?tmdb_id='+encodeURIComponent("
            f"{json.dumps(tmdb_id, ensure_ascii=False)})"
            f"{season_js},"
            "{headers:h,credentials:'include'})"
            f"{parse_json_js}"
            ".then(function(d){if(d._ok&&d.success){alert(d.message||'待入库整理已在后台启动');}"
            "else{alert('整理失败: '+(d.message||d.detail||d.error||JSON.stringify(d)));}})"
            ".catch(function(e){alert('请求失败: '+e);})"
        )
        return {
            "component": "tr",
            "content": [
                {"component": "td", "text": row["title"]},
                {
                    "component": "td",
                    "content": [{
                        "component": "VChip",
                        "props": {"size": "x-small", "variant": "text"},
                        "text": str(row.get("tmdb_id", "")),
                    }]
                },
                {"component": "td", "text": row["season"]},
                {
                    "component": "td",
                    "content": [{
                        "component": "VProgressLinear",
                        "props": {
                            "model-value": row["progress"],
                            "height": 20,
                            "color": row["status_color"],
                            "striped": row["status"] == "订阅中",
                        },
                        "content": [{
                            "component": "span",
                            "props": {"class": "text-caption"},
                            "text": f"{row['progress']}%",
                        }]
                    }]
                },
                {"component": "td", "text": row["episode_text"]},
                {"component": "td", "text": row["last_found"]},
                {
                    "component": "td",
                    "content": [{
                        "component": "VChip",
                        "props": {
                            "color": row["status_color"],
                            "size": "small",
                            "variant": "elevated",
                        },
                        "text": row["status"],
                    }]
                },
                {
                    "component": "td",
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "d-flex align-center ga-2"},
                            "content": [
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "size": "x-small",
                                        "color": "primary",
                                        "variant": "tonal",
                                        "prepend-icon": "mdi-file-sync-outline",
                                        "disabled": (not bool(tmdb_id)) or int(row.get("pending_count") or 0) <= 0,
                                        "onclick": organize_js,
                                    },
                                    "text": "整理",
                                },
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "size": "x-small",
                                        "color": "success",
                                        "variant": "tonal",
                                        "prepend-icon": "mdi-archive-check-outline",
                                        "disabled": not bool(tmdb_id),
                                        "onclick": archive_js,
                                    },
                                    "text": "归档",
                                },
                            ],
                        }
                    ]
                },
            ]
        }

    # ================================================================
    #  115 配置读取（供外部工具调用）
    # ================================================================

    def get_115_config(self) -> dict:
        """
        返回 115 配置，供 AI Agent 工具使用。
        """
        active_cookie, active_source = self._resolve_115_cookie(validate=False)
        return {
            "cookies": active_cookie,
            "cookie_source": active_source or self._cookie_source_display(),
            "default_cid": self._default_cid or "0",
            "movie_cid": self._movie_staging_cid or "",
            "old_movie_cid": self._old_movie_staging_cid or "",
            "ongoing_cid": self._ongoing_staging_cid or "",
            "archive_cid": self._archive_staging_cid or "",
            "movie_year_threshold": self._movie_year_threshold,
            "cd2_target_movie_path": self._cd2_target_movie_path,
            "cd2_target_old_movie_path": self._cd2_target_old_movie_path,
            "cd2_target_ongoing_path": self._cd2_target_ongoing_path,
            "cd2_target_archive_path": self._cd2_target_archive_path,
            "fallback_clouds": self._fallback_clouds,
            "fallback_cloud_priority": self._format_fallback_cloud_order(self._fallback_cloud_priority),
            "effective_fallback_clouds": self._get_effective_fallback_clouds(),
            "closed_loop_organize_enabled": self._closed_loop_organize_enabled,
            "closed_loop_movie_library_path": self._closed_loop_movie_library_path,
            "closed_loop_old_movie_library_path": self._closed_loop_old_movie_library_path,
            "closed_loop_ongoing_library_path": self._closed_loop_ongoing_library_path,
            "closed_loop_archive_library_path": self._closed_loop_archive_library_path,
        }

    # ================================================================
    #  TMDB 总集数查询
    # ================================================================

    def _get_total_episodes(self, title: str, tmdb_id: Optional[int],
                            media_type_str: str,
                            season: Optional[int],
                            meta_out: Optional[dict] = None) -> Optional[int]:
        """从 TMDB 查询某季的总集数

        meta_out: 可选 dict，成功识别媒体后回填播出状态
        (fetched/status/next_episode_to_air)，供在播保护判断使用。
        """
        if not tmdb_id or media_type_str == "电影":
            return None
        target_season = season or 1
        try:
            self._apply_tmdb_settings()
            media_chain = MediaChain()
            meta = ParseMeta(title)
            meta.type = MediaType.TV
            if season:
                meta.begin_season = season

            mediainfo = media_chain.recognize_by_meta(meta)
            if not mediainfo:
                logger.info(f"【115助手】TMDB 总集数: {title} 未识别到媒体信息")
                return None

            if tmdb_id and mediainfo.tmdb_id != tmdb_id:
                mediainfo.tmdb_id = tmdb_id

            if meta_out is not None:
                meta_out["fetched"] = True
                tmdb_info_for_status = getattr(mediainfo, "tmdb_info", None)
                status_val = None
                next_ep_val = None
                if isinstance(tmdb_info_for_status, dict):
                    status_val = tmdb_info_for_status.get("status")
                    next_ep_val = tmdb_info_for_status.get("next_episode_to_air")
                elif tmdb_info_for_status is not None:
                    status_val = getattr(tmdb_info_for_status, "status", None)
                    next_ep_val = getattr(tmdb_info_for_status, "next_episode_to_air", None)
                if not status_val:
                    status_val = getattr(mediainfo, "status", None)
                if next_ep_val in (None, ""):
                    next_ep_val = getattr(mediainfo, "next_episode_to_air", None)
                meta_out["status"] = str(status_val).strip() if status_val else ""
                meta_out["next_episode_to_air"] = next_ep_val

                # 已播出集数 / 下一集排播日：用于「追平已播出进度后跳过搜索」
                last_ep_val = None
                if isinstance(tmdb_info_for_status, dict):
                    last_ep_val = tmdb_info_for_status.get("last_episode_to_air")
                elif tmdb_info_for_status is not None:
                    last_ep_val = getattr(tmdb_info_for_status, "last_episode_to_air", None)
                if last_ep_val in (None, ""):
                    last_ep_val = getattr(mediainfo, "last_episode_to_air", None)

                def _pick(obj, key):
                    if isinstance(obj, dict):
                        return obj.get(key)
                    return getattr(obj, key, None)

                # 多季剧：last_episode_to_air 的集号是相对它自己那一季的，
                # 季号对不上时不能拿来当本季的已播出集数。
                aired_eps = None
                last_season = _pick(last_ep_val, "season_number") if last_ep_val else None
                if last_ep_val is not None:
                    try:
                        if last_season is None or int(last_season) == int(target_season):
                            aired_eps = int(_pick(last_ep_val, "episode_number"))
                    except (TypeError, ValueError):
                        aired_eps = None
                meta_out["aired_episodes"] = aired_eps

                next_air_date = None
                if next_ep_val is not None:
                    next_season = _pick(next_ep_val, "season_number")
                    try:
                        same_season = next_season is None or int(next_season) == int(target_season)
                    except (TypeError, ValueError):
                        same_season = False
                    if same_season:
                        nd = _pick(next_ep_val, "air_date")
                        next_air_date = str(nd).strip() if nd else None
                meta_out["next_air_date"] = next_air_date
                logger.info(
                    f"【115助手】TMDB {title} S{target_season}: 播出状态={meta_out['status'] or '未知'}, "
                    f"已播出={aired_eps}, 下一集排播日={next_air_date or '无'}"
                )

            # 打印可用的属性，便于调试
            attrs_debug = []
            for attr in ("season_info", "seasons", "tmdb_info", "detail"):
                val = getattr(mediainfo, attr, None)
                if val is not None:
                    if isinstance(val, (list, dict)):
                        attrs_debug.append(f"{attr}({type(val).__name__}, len={len(val)})")
                    else:
                        attrs_debug.append(f"{attr}({type(val).__name__})")
            logger.info(f"【115助手】TMDB {title}: 可用属性 [{', '.join(attrs_debug)}]")

            # 方法 1: season_info 属性（MoviePilot 标准）
            if hasattr(mediainfo, "season_info") and mediainfo.season_info:
                for s_info in mediainfo.season_info:
                    sn = getattr(s_info, "season_number", None)
                    if isinstance(s_info, dict):
                        sn = s_info.get("season_number")
                    if sn == target_season:
                        ec = getattr(s_info, "episode_count", None)
                        if isinstance(s_info, dict):
                            ec = s_info.get("episode_count")
                        if ec:
                            logger.info(f"【115助手】TMDB {title} S{target_season}: {ec} 集 (via season_info)")
                            return ec

            # 方法 2: seasons 属性（可能是 list 或 dict）
            if hasattr(mediainfo, "seasons") and mediainfo.seasons:
                seasons_data = mediainfo.seasons
                # 如果是 dict {season_num: episode_list} 格式（跟 existsinfo 一样）
                if isinstance(seasons_data, dict):
                    eps = seasons_data.get(target_season, [])
                    if eps and isinstance(eps, list) and len(eps) > 0:
                        # 这里是已有集数列表，不是总集数
                        pass
                # 如果是 list of season objects/dicts（TMDB 格式）
                elif isinstance(seasons_data, list):
                    for s in seasons_data:
                        sn = s.get("season_number") if isinstance(s, dict) else getattr(s, "season_number", None)
                        ec = s.get("episode_count") if isinstance(s, dict) else getattr(s, "episode_count", None)
                        if sn == target_season and ec:
                            logger.info(f"【115助手】TMDB {title} S{target_season}: {ec} 集 (via seasons list)")
                            return ec

            # 方法 3: tmdb_info 属性（原始 TMDB 数据）
            tmdb_info = getattr(mediainfo, "tmdb_info", None)
            if tmdb_info:
                tmdb_seasons = None
                if isinstance(tmdb_info, dict):
                    tmdb_seasons = tmdb_info.get("seasons", [])
                elif hasattr(tmdb_info, "seasons"):
                    tmdb_seasons = tmdb_info.seasons
                if tmdb_seasons:
                    for s in tmdb_seasons:
                        sn = s.get("season_number") if isinstance(s, dict) else getattr(s, "season_number", None)
                        ec = s.get("episode_count") if isinstance(s, dict) else getattr(s, "episode_count", None)
                        if sn == target_season and ec:
                            logger.info(f"【115助手】TMDB {title} S{target_season}: {ec} 集 (via tmdb_info)")
                            return ec

            # 方法 4: detail 属性
            detail = getattr(mediainfo, "detail", None)
            if detail:
                detail_seasons = None
                if isinstance(detail, dict):
                    detail_seasons = detail.get("seasons", [])
                elif hasattr(detail, "seasons"):
                    detail_seasons = detail.seasons
                if detail_seasons:
                    for s in detail_seasons:
                        sn = s.get("season_number") if isinstance(s, dict) else getattr(s, "season_number", None)
                        ec = s.get("episode_count") if isinstance(s, dict) else getattr(s, "episode_count", None)
                        if sn == target_season and ec:
                            logger.info(f"【115助手】TMDB {title} S{target_season}: {ec} 集 (via detail)")
                            return ec

            logger.info(f"【115助手】TMDB {title} S{target_season}: 未能获取总集数")

        except Exception as e:
            logger.warning(f"【115助手】查询 TMDB 总集数失败 (title={title}, tmdb_id={tmdb_id}): {e}")
        return None

    @staticmethod
    def _total_episodes_trustworthy(sub: dict) -> bool:
        """
        TMDB 的本季总集数是否可信（是否登记了尚未播出的集）。

        `总集数 > 已播出集数` 说明 TMDB 已经排好了完整排播表，总集数是真实季长度；
        `总集数 == 已播出集数` 时，「总集数」很可能只是「目前播了几集」——新剧开播
        当天就是这种情况（藏锋 1/1 被误判完结归档即源于此），此时不可当作季长度。
        """
        try:
            total = int(sub.get("_cache_total_episodes") or 0)
            aired = int(sub.get("_cache_aired_episodes") or 0)
        except (TypeError, ValueError):
            return False
        return total > aired > 0

    @classmethod
    def _should_allow_auto_complete(cls, sub: dict) -> bool:
        """
        在播保护：避免把 TMDB 集数滞后的在播新剧误判成全集完结。

        允许归档的两种情况：
          1) TMDB 明确标记 Ended/Canceled；
          2) 总集数可信（见 _total_episodes_trustworthy）且已入库覆盖了全部集数——
             此时资源已抢先于播出拿全，没有可再抓的内容，继续挂着只是空耗。
        状态取不到时维持原行为（允许归档），避免影响老剧/完结剧归档链路。
        """
        status = str(sub.get("_cache_tmdb_status") or "").strip().lower()
        if status in ("ended", "canceled", "cancelled"):
            return True

        if cls._total_episodes_trustworthy(sub):
            try:
                total = int(sub.get("_cache_total_episodes") or 0)
            except (TypeError, ValueError):
                total = 0
            existing = set()
            for ep in sub.get("_cache_existing_episodes") or []:
                try:
                    existing.add(int(ep))
                except (TypeError, ValueError):
                    continue
            if total > 0 and not (set(range(1, total + 1)) - existing):
                return True

        if sub.get("_cache_has_next_episode"):
            return False
        if status:
            # Returning Series / In Production / Planned / Pilot 等：仍在播出或制作
            return False
        return True

    def _airing_effective_target(self, sub: dict, total_eps: Optional[int],
                                 now: Optional[datetime] = None) -> Optional[int]:
        """
        本轮真正应该追到第几集。返回 None 表示 TMDB 数据不足，不启用跳过。

        基准是 TMDB 的「已播出集数」而不是「本季总集数」——总集数含未播出的集，
        拿它算缺失会让插件每轮都去搜根本还没播的集。
        国产剧当天新集一般 18:00 后放出，而 TMDB 的 last_episode_to_air 往往滞后，
        所以在「排播日当天且已过窗口起点」时把今天这集也算进目标（时间闸门内的 +1，
        不是无条件冗余；无条件冗余会让跳过条件永远不成立）。
        """
        aired = sub.get("_cache_aired_episodes")
        try:
            aired = int(aired) if aired not in (None, "") else None
        except (TypeError, ValueError):
            aired = None
        if not aired or aired <= 0:
            return None

        now = now or datetime.now()
        target = aired
        next_air_date = str(sub.get("_cache_next_air_date") or "").strip()
        if next_air_date == now.strftime("%Y-%m-%d") and now.hour >= self._airing_window_start_hour:
            target = aired + 1

        if total_eps:
            try:
                target = min(target, int(total_eps))
            except (TypeError, ValueError):
                pass
        return target

    def _should_skip_airing_search(self, sub: dict, existing_episodes: List[int],
                                   total_eps: Optional[int],
                                   now: Optional[datetime] = None) -> Tuple[bool, str]:
        """
        已追平当前已播出进度时跳过本轮搜索/降级，返回 (是否跳过, 说明)。

        为兼容 TMDB 录入滞后，仍保留一个低频兜底探针：距上次实际搜索超过
        配置小时数时放行一轮。probe 小时数设为 0 表示不放行（完全按 TMDB 走）。
        """
        if not self._airing_skip_enabled:
            return False, ""
        if sub.get("media_type", "电视剧") == "电影":
            return False, ""

        target = self._airing_effective_target(sub, total_eps, now=now)
        if target is None:
            return False, ""

        existing_set = set()
        for ep in existing_episodes or []:
            try:
                existing_set.add(int(ep))
            except (TypeError, ValueError):
                continue
        if set(range(1, target + 1)) - existing_set:
            # 还有已播出但没入库的集，正常搜索
            return False, ""

        now = now or datetime.now()
        if self._airing_idle_probe_hours > 0:
            last_search = self._parse_iso_datetime(sub.get("_last_search_at"))
            if not last_search:
                return False, ""
            idle_hours = (now - last_search).total_seconds() / 3600.0
            if idle_hours >= self._airing_idle_probe_hours:
                return False, ""
            return True, (
                f"已追平已播出进度（已入库覆盖 1-{target} 集），"
                f"距上次搜索 {idle_hours:.1f} 小时未到 {self._airing_idle_probe_hours} 小时探针间隔"
            )
        return True, f"已追平已播出进度（已入库覆盖 1-{target} 集）"

    # ================================================================
    #  核心订阅检查逻辑（来自 PanSouSubscribe）
    # ================================================================

    def check_subscriptions(self):
        """定时任务：检查所有网盘订阅（带互斥，避免多路触发并发覆盖）。"""
        lock = self._get_subscription_check_lock()
        if not lock.acquire(blocking=False):
            self._subscription_check_pending_reason = "并发触发补跑"
            logger.info("【115助手】已有订阅巡检正在运行，本次触发已排队补跑")
            return
        try:
            return self._check_subscriptions_impl()
        finally:
            lock.release()
            pending_reason = getattr(self, "_subscription_check_pending_reason", "")
            if pending_reason:
                self._subscription_check_pending_reason = ""
                logger.info(f"【115助手】订阅巡检补跑启动: {pending_reason}")
                _threading_mod.Thread(target=self.check_subscriptions, daemon=True).start()

    def _get_subscription_check_lock(self):
        """获取订阅巡检互斥锁。"""
        lock = getattr(self, "_subscription_check_run_lock", None)
        if lock is None:
            lock = _threading_mod.Lock()
            self._subscription_check_run_lock = lock
        return lock

    def _start_subscription_check_background(self, reason: str = "手动触发") -> bool:
        """后台启动一次订阅巡检。"""
        if self._get_subscription_check_lock().locked():
            self._subscription_check_pending_reason = reason
            logger.info(f"【115助手】订阅巡检已有任务在运行，已排队补跑: {reason}")
            return False
        logger.info(f"【115助手】订阅巡检后台启动: {reason}")
        _threading_mod.Thread(target=self.check_subscriptions, daemon=True).start()
        return True

    def _get_pending_organize_lock(self):
        """获取待入库整理互斥锁。"""
        lock = getattr(self, "_pending_organize_run_lock", None)
        if lock is None:
            lock = _threading_mod.Lock()
            self._pending_organize_run_lock = lock
        return lock

    def _start_pending_organize_background(
        self,
        tmdb_id: Optional[int] = None,
        season: Optional[int] = None,
        reason: str = "手动触发",
    ) -> bool:
        """后台启动一次待入库整理。"""
        lock = self._get_pending_organize_lock()
        if lock.locked():
            logger.info(f"【115助手】待入库整理已有任务在运行，跳过触发: {reason}")
            return False
        logger.info(
            f"【115助手】待入库整理后台启动: {reason}, "
            f"TMDB={tmdb_id or '全部'}, 季={season or '全部'}"
        )
        _threading_mod.Thread(
            target=self._organize_pending_subscriptions,
            kwargs={"tmdb_id": tmdb_id, "season": season},
            daemon=True,
        ).start()
        return True

    def _load_runtime_state(self) -> dict:
        """读取运行态节流数据，避免重启后立即重复撞 115。"""
        if isinstance(self._runtime_state, dict) and self._runtime_state:
            return self._runtime_state
        data = self.get_data("runtime_state")
        self._runtime_state = data if isinstance(data, dict) else {}
        return self._runtime_state

    def _save_runtime_state(self) -> None:
        self.save_data("runtime_state", self._runtime_state or {})

    # ── 失效分享链接黑名单 ─────────────────────────────
    _DEAD_SHARE_TTL_SECONDS = 7 * 24 * 3600
    _DEAD_SHARE_MAX = 500

    def _load_dead_shares(self) -> dict:
        state = self._load_runtime_state()
        data = state.get("dead_shares")
        return data if isinstance(data, dict) else {}

    def _is_dead_share_link(self, share_url: str) -> bool:
        """分享已被取消/失效的链接在 TTL 内直接跳过，不再重复请求。"""
        if not share_url:
            return False
        expire = self._load_dead_shares().get(str(share_url))
        try:
            return float(expire or 0) > time.time()
        except (TypeError, ValueError):
            return False

    def _mark_dead_share_link(self, share_url: str, cloud_name: str = "") -> None:
        if not share_url:
            return
        state = self._load_runtime_state()
        dead = state.get("dead_shares")
        if not isinstance(dead, dict):
            dead = {}
        now = time.time()
        # 顺手清掉过期项，并在超量时保留最近的一批
        dead = {k: v for k, v in dead.items() if float(v or 0) > now}
        dead[str(share_url)] = now + self._DEAD_SHARE_TTL_SECONDS
        if len(dead) > self._DEAD_SHARE_MAX:
            dead = dict(sorted(dead.items(), key=lambda kv: kv[1], reverse=True)[: self._DEAD_SHARE_MAX])
        state["dead_shares"] = dead
        self._runtime_state = state
        self._save_runtime_state()
        logger.info(
            f"【115助手】[降级] {cloud_name}分享已失效，加入黑名单 "
            f"{self._DEAD_SHARE_TTL_SECONDS // 86400} 天: {str(share_url)[:70]}"
        )

    def _plugin_notice_subject_key(self, subscription: dict) -> str:
        media_type_str = str(subscription.get("media_type") or "电视剧")
        season = int(subscription.get("season") or 1)
        tmdb_id = subscription.get("tmdb_id")
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        if tmdb_id:
            subject = f"tmdb:{tmdb_id}"
        else:
            title = str(subscription.get("title") or "").strip()
            year = str(subscription.get("year") or "").strip()
            subject = f"title:{title}|year:{year}"
        return f"{media_type_str}|{subject}|S{season:02d}"

    def _plugin_notice_rate_subject_key(self, subscription: dict) -> str:
        """通知限频按作品聚合，季号不同仍视为同一部剧。"""
        media_type_str = str(subscription.get("media_type") or "电视剧")
        tmdb_id = subscription.get("tmdb_id")
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        if tmdb_id:
            subject = f"tmdb:{tmdb_id}"
        else:
            title = str(subscription.get("title") or "").strip().casefold()
            year = str(subscription.get("year") or "").strip()
            subject = f"title:{title}|year:{year}"
        return f"{media_type_str}|{subject}"

    def _load_plugin_notice_rate_events(
        self,
        now: Optional[datetime] = None,
    ) -> List[dict]:
        state = self._load_runtime_state()
        raw = state.get(self._PLUGIN_NOTICE_RATE_KEY)
        if not isinstance(raw, list):
            return []

        now = now or datetime.now()
        retention_seconds = max(
            int(self._notice_same_title_window_minutes),
            int(self._notice_global_window_minutes),
        ) * 60
        cleaned = []
        changed = False
        for item in raw:
            if not isinstance(item, dict):
                changed = True
                continue
            sent_at_raw = item.get("sent_at")
            try:
                sent_at = datetime.fromisoformat(str(sent_at_raw))
            except (TypeError, ValueError):
                changed = True
                continue
            age_seconds = (now - sent_at).total_seconds()
            if age_seconds > retention_seconds:
                changed = True
                continue
            cleaned.append({
                "subject": str(item.get("subject") or ""),
                "sent_at": sent_at.isoformat(),
            })
        if changed:
            if cleaned:
                state[self._PLUGIN_NOTICE_RATE_KEY] = cleaned
            else:
                state.pop(self._PLUGIN_NOTICE_RATE_KEY, None)
            self._save_runtime_state()
        return cleaned

    def _check_plugin_notice_rate_limit(
        self,
        subscription: dict,
        now: Optional[datetime] = None,
    ) -> Tuple[bool, str, int]:
        """返回 (是否允许, 命中的限制, 建议重试秒数)。"""
        now = now or datetime.now()
        now_ts = now.timestamp()
        subject = self._plugin_notice_rate_subject_key(subscription)
        events = self._load_plugin_notice_rate_events(now=now)

        same_window_seconds = int(self._notice_same_title_window_minutes) * 60
        same_events = sorted(
            datetime.fromisoformat(item["sent_at"]).timestamp()
            for item in events
            if item.get("subject") == subject
            and now_ts - datetime.fromisoformat(item["sent_at"]).timestamp() < same_window_seconds
        )
        if len(same_events) >= int(self._notice_same_title_max):
            retry_after = max(1, int(same_events[0] + same_window_seconds - now_ts + 0.999))
            return False, "same_title", retry_after

        global_window_seconds = int(self._notice_global_window_minutes) * 60
        global_events = sorted(
            datetime.fromisoformat(item["sent_at"]).timestamp()
            for item in events
            if now_ts - datetime.fromisoformat(item["sent_at"]).timestamp() < global_window_seconds
        )
        if len(global_events) >= int(self._notice_global_max):
            retry_after = max(1, int(global_events[0] + global_window_seconds - now_ts + 0.999))
            return False, "global", retry_after
        return True, "", 0

    def _record_plugin_notice_rate_event(
        self,
        subscription: dict,
        now: Optional[datetime] = None,
    ) -> None:
        now = now or datetime.now()
        state = self._load_runtime_state()
        events = self._load_plugin_notice_rate_events(now=now)
        events.append({
            "subject": self._plugin_notice_rate_subject_key(subscription),
            "sent_at": now.isoformat(),
        })
        state[self._PLUGIN_NOTICE_RATE_KEY] = events
        self._save_runtime_state()

    def _group_cd2_watch_notice_records(self, records: List[dict]) -> List[List[dict]]:
        """将同一作品的多季记录合并为一个发送批次。"""
        grouped: Dict[str, List[dict]] = {}
        for record in records or []:
            subscription = self._build_cd2_watch_subscription(record)
            key = self._plugin_notice_rate_subject_key(subscription)
            grouped.setdefault(key, []).append(record)
        return list(grouped.values())

    def _format_multi_season_notice(self, records: List[dict]) -> str:
        season_episodes: Dict[int, set] = {}
        for record in records or []:
            try:
                season = int(record.get("season") or 1)
            except (TypeError, ValueError):
                season = 1
            season_episodes.setdefault(season, set()).update(
                int(ep) for ep in (record.get("episodes") or []) if ep is not None
            )
        seasons = sorted(season_episodes)
        if not seasons:
            return ""

        ranges = []
        start = previous = seasons[0]
        for season in seasons[1:] + [None]:
            if season is not None and season == previous + 1:
                previous = season
                continue
            if start == previous:
                ranges.append(f"S{start:02d}")
            else:
                ranges.append(f"S{start:02d}-S{previous:02d}")
            if season is not None:
                start = previous = season
        episode_count = sum(len(episodes) for episodes in season_episodes.values())
        return f"{','.join(ranges)} 共{episode_count}集"

    def _plugin_notice_episode_key(self, subscription: dict, episode: int) -> str:
        return f"{self._plugin_notice_subject_key(subscription)}|E{int(episode):02d}"

    def _load_plugin_notice_sent_map(self) -> dict:
        state = self._load_runtime_state()
        raw = state.get(self._PLUGIN_NOTICE_SENT_KEY)
        if not isinstance(raw, dict):
            return {}

        now = datetime.now()
        cleaned = {}
        changed = False
        for key, value in raw.items():
            sent_at = self._parse_iso_datetime(value)
            if sent_at and (now - sent_at).total_seconds() > self._PLUGIN_NOTICE_SENT_TTL_SECONDS:
                changed = True
                continue
            cleaned[str(key)] = str(value or "")
        if changed:
            if cleaned:
                state[self._PLUGIN_NOTICE_SENT_KEY] = cleaned
            else:
                state.pop(self._PLUGIN_NOTICE_SENT_KEY, None)
            self._save_runtime_state()
        return cleaned

    def _mark_plugin_notice_sent(self, subscription: dict, episodes: List[int]) -> None:
        cleaned = sorted({int(ep) for ep in episodes if ep is not None})
        if not cleaned or subscription.get("media_type") == "电影":
            return
        state = self._load_runtime_state()
        sent = self._load_plugin_notice_sent_map()
        now_iso = datetime.now().isoformat()
        for ep in cleaned:
            sent[self._plugin_notice_episode_key(subscription, ep)] = now_iso
        state[self._PLUGIN_NOTICE_SENT_KEY] = sent
        self._save_runtime_state()

    @staticmethod
    def _normalize_episode_map(value: Any) -> Dict[int, List[int]]:
        result: Dict[int, List[int]] = {}
        if not isinstance(value, dict):
            return result
        for season_key, episodes in value.items():
            try:
                season_num = int(season_key)
            except (TypeError, ValueError):
                continue
            cleaned = set()
            for ep in episodes or []:
                try:
                    cleaned.add(int(ep))
                except (TypeError, ValueError):
                    continue
            if cleaned:
                result[season_num] = sorted(cleaned)
        return result

    @staticmethod
    def _episode_map_union(*maps: Dict[int, List[int]]) -> Dict[int, List[int]]:
        merged: Dict[int, set] = {}
        for item in maps:
            for season, episodes in Net115Helper._normalize_episode_map(item).items():
                merged.setdefault(int(season), set()).update(int(ep) for ep in episodes)
        return {
            int(season): sorted(values)
            for season, values in merged.items()
            if values
        }

    @staticmethod
    def _episode_map_difference(source_map: Dict[int, List[int]],
                                covered_map: Dict[int, List[int]]) -> Dict[int, List[int]]:
        source = Net115Helper._normalize_episode_map(source_map)
        covered = Net115Helper._normalize_episode_map(covered_map)
        diff: Dict[int, List[int]] = {}
        for season, episodes in source.items():
            remain = sorted(set(episodes) - set(covered.get(season, [])))
            if remain:
                diff[int(season)] = remain
        return diff

    @staticmethod
    def _episode_map_count(value: Any) -> int:
        total = 0
        for episodes in Net115Helper._normalize_episode_map(value).values():
            total += len(episodes)
        return total

    @staticmethod
    def _normalize_title_token(text: str) -> str:
        cleaned = Net115Helper._strip_year(str(text or "")).strip().lower()
        cleaned = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", cleaned)
        return cleaned

    def _cd2_watch_rule_key(self, rule: dict) -> str:
        return str(
            rule.get("id")
            or f"{rule.get('source_path', '').strip()}->{rule.get('dest_115_path', '').strip()}"
        )

    def _cd2_watch_show_key(self, show_name: str, media_type_str: str) -> str:
        year = self._extract_year(show_name) or 0
        token = self._normalize_title_token(show_name) or str(show_name or "").strip().lower()
        return f"{media_type_str}|{token}|{year}"

    def _load_cd2_watch_index(self) -> dict:
        cached = getattr(self, "_cd2_watch_index_cache", None)
        if isinstance(cached, dict):
            return cached
        raw = self.get_data(self._CD2_WATCH_INDEX_DATA_KEY)
        if not isinstance(raw, dict):
            raw = {}
        index = {
            "rules": raw.get("rules") if isinstance(raw.get("rules"), dict) else {},
            "completed": raw.get("completed") if isinstance(raw.get("completed"), list) else [],
        }
        self._cd2_watch_index_cache = index
        return index

    def _save_cd2_watch_index(self, index: dict) -> None:
        completed = index.get("completed")
        if isinstance(completed, list) and len(completed) > self._CD2_WATCH_ARCHIVE_LIMIT:
            index["completed"] = completed[-self._CD2_WATCH_ARCHIVE_LIMIT:]
        self._cd2_watch_index_cache = index
        self.save_data(self._CD2_WATCH_INDEX_DATA_KEY, index)

    def _get_cd2_watch_rule_state(self, index: dict, rule: dict) -> dict:
        rules = index.setdefault("rules", {})
        rule_key = self._cd2_watch_rule_key(rule)
        rule_state = rules.setdefault(rule_key, {})
        rule_state["id"] = rule.get("id") or rule_key
        rule_state["name"] = rule.get("name") or rule.get("source_path") or rule_key
        rule_state["source_path"] = rule.get("source_path", "")
        rule_state["dest_path"] = rule.get("dest_115_path", "")
        rule_state["updated_at"] = datetime.now().isoformat()
        if not isinstance(rule_state.get("shows"), dict):
            rule_state["shows"] = {}
        return rule_state

    def _split_cd2_watch_pending_map(
        self,
        show_state: dict,
    ) -> Tuple[Dict[int, List[int]], Dict[int, List[int]], int]:
        pending_map = self._normalize_episode_map(show_state.get("pending_episodes_by_season"))
        if not pending_map:
            return {}, {}, 0
        pending_at_raw = show_state.get("pending_updated_at") or show_state.get("pending_since")
        pending_at = self._parse_iso_datetime(pending_at_raw)
        if not pending_at:
            return {}, pending_map, self._PENDING_COPY_TTL_SECONDS + 1
        age = int((datetime.now() - pending_at).total_seconds())
        if age < self._PENDING_COPY_TTL_SECONDS:
            return pending_map, {}, age
        return {}, pending_map, age

    def _set_cd2_watch_pending_map(
        self,
        show_state: dict,
        pending_map: Dict[int, List[int]],
        when: Optional[datetime] = None,
    ) -> None:
        cleaned = self._normalize_episode_map(pending_map)
        if cleaned:
            ts = (when or datetime.now()).isoformat()
            show_state["pending_episodes_by_season"] = cleaned
            show_state["pending_updated_at"] = ts
            show_state["pending_since"] = ts
            show_state["pending_count"] = self._episode_map_count(cleaned)
        else:
            show_state.pop("pending_episodes_by_season", None)
            show_state.pop("pending_updated_at", None)
            show_state.pop("pending_since", None)
            show_state["pending_count"] = 0

    def _append_cd2_watch_completed_show(
        self,
        index: dict,
        rule_state: dict,
        show_state: dict,
        reason: str,
    ) -> None:
        completed = index.setdefault("completed", [])
        completion_key = str(show_state.get("completion_key") or "")
        if completion_key and any(str(item.get("completion_key") or "") == completion_key for item in completed):
            return
        snapshot = {
            "completion_key": completion_key or (
                f"{rule_state.get('id')}|{show_state.get('show_key')}|{show_state.get('completed_at')}"
            ),
            "rule_id": rule_state.get("id"),
            "rule_name": rule_state.get("name"),
            "show_key": show_state.get("show_key"),
            "show_name": show_state.get("show_name"),
            "title": show_state.get("title"),
            "year": show_state.get("year"),
            "tmdb_id": show_state.get("tmdb_id"),
            "media_type": show_state.get("media_type"),
            "total_episodes": show_state.get("total_episodes"),
            "existing_count": show_state.get("existing_count"),
            "confirmed_episodes_by_season": self._normalize_episode_map(
                show_state.get("confirmed_episodes_by_season")
            ),
            "completed_at": show_state.get("completed_at"),
            "completed_reason": reason,
        }
        completed.append(snapshot)

    def _resolve_cd2_watch_subscription_match(
        self,
        show_name: str,
        media_type_str: str,
    ) -> Optional[dict]:
        show_title = self._strip_year(show_name) or show_name
        show_token = self._normalize_title_token(show_title)
        show_year = self._extract_year(show_name)
        if not show_token:
            return None

        candidates = []
        for source_name, collection in (
            ("active", self._load_subscriptions()),
            ("history", self._load_history()),
        ):
            for item in collection or []:
                item_type = str(item.get("media_type") or "电视剧")
                if item_type != media_type_str:
                    continue
                title = str(item.get("title") or "").strip()
                token = self._normalize_title_token(title)
                if not token:
                    continue
                score = 0
                if token == show_token:
                    score += 100
                elif token in show_token or show_token in token:
                    score += 60
                else:
                    continue
                item_year = item.get("year")
                try:
                    item_year = int(item_year) if item_year not in (None, "") else None
                except (TypeError, ValueError):
                    item_year = None
                if show_year and item_year and show_year == item_year:
                    score += 20
                if source_name == "active":
                    score += 10
                candidates.append((score, dict(item)))

        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _hydrate_cd2_watch_show_meta(
        self,
        show_state: dict,
        show_name: str,
        media_type_str: str,
    ) -> dict:
        show_state["show_name"] = show_name
        show_state["media_type"] = media_type_str
        show_state["title"] = str(show_state.get("title") or self._strip_year(show_name) or show_name).strip()
        show_state["year"] = show_state.get("year") or self._extract_year(show_name)

        match = self._resolve_cd2_watch_subscription_match(show_name, media_type_str)
        if match:
            show_state["title"] = str(match.get("title") or show_state.get("title") or "").strip()
            show_state["year"] = match.get("year") or show_state.get("year")
            show_state["tmdb_id"] = match.get("tmdb_id") or show_state.get("tmdb_id")
            if media_type_str != "电影":
                try:
                    show_state["season"] = int(match.get("season") or show_state.get("season") or 1)
                except (TypeError, ValueError):
                    show_state["season"] = 1
                total_eps = match.get("_cache_total_episodes") or match.get("total_episodes")
                try:
                    total_eps = int(total_eps) if total_eps not in (None, "") else None
                except (TypeError, ValueError):
                    total_eps = None
                if total_eps:
                    show_state["total_episodes"] = total_eps

        if not show_state.get("tmdb_id") or (media_type_str != "电影" and not show_state.get("total_episodes")):
            subscription = {
                "title": show_state.get("title") or show_name,
                "tmdb_id": show_state.get("tmdb_id"),
                "media_type": media_type_str,
                "season": show_state.get("season") or 1,
                "year": show_state.get("year"),
            }
            try:
                _, mediainfo = self._build_notification_media_context(subscription, [1], show_name)
            except Exception as e:
                logger.debug(f"【CD2巡查】识别媒体信息失败 {show_name}: {e}")
                mediainfo = None
            if mediainfo:
                tmdb_id = getattr(mediainfo, "tmdb_id", None)
                try:
                    tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
                except (TypeError, ValueError):
                    tmdb_id = None
                if tmdb_id:
                    show_state["tmdb_id"] = tmdb_id
                media_title = getattr(mediainfo, "title", None) or getattr(mediainfo, "name", None)
                if media_title:
                    show_state["title"] = str(media_title).strip()
                media_year = getattr(mediainfo, "year", None)
                if media_year:
                    show_state["year"] = media_year

        if media_type_str != "电影" and show_state.get("tmdb_id") and not show_state.get("total_episodes"):
            total_eps = self._get_total_episodes(
                title=str(show_state.get("title") or show_name),
                tmdb_id=show_state.get("tmdb_id"),
                media_type_str=media_type_str,
                season=show_state.get("season") or 1,
            )
            if total_eps:
                show_state["total_episodes"] = total_eps

        show_state["meta_updated_at"] = datetime.now().isoformat()
        return show_state

    def _refresh_cd2_watch_confirmed_map(
        self,
        show_state: dict,
        show_name: str,
        media_type_str: str,
        seasons: List[int],
    ) -> Dict[int, List[int]]:
        confirmed = self._normalize_episode_map(show_state.get("confirmed_episodes_by_season"))
        title = str(show_state.get("title") or self._strip_year(show_name) or show_name).strip()
        tmdb_id = show_state.get("tmdb_id")

        if media_type_str == "电影":
            existing = self._get_existing_episodes(
                title=title,
                tmdb_id=tmdb_id,
                media_type_str=media_type_str,
                season=None,
            )
            if -1 in existing:
                confirmed = {1: [-1]}
        else:
            for season in sorted({int(s) for s in seasons if s is not None} or {show_state.get("season") or 1}):
                existing = self._get_existing_episodes(
                    title=title,
                    tmdb_id=tmdb_id,
                    media_type_str=media_type_str,
                    season=season,
                    total_episodes=show_state.get("total_episodes"),
                )
                if existing:
                    confirmed[season] = sorted(set(confirmed.get(season, [])) | {int(ep) for ep in existing})

        confirmed = self._normalize_episode_map(confirmed)
        show_state["confirmed_episodes_by_season"] = confirmed
        show_state["existing_count"] = self._episode_map_count(confirmed)
        show_state["confirmed_updated_at"] = datetime.now().isoformat()
        return confirmed

    def _update_cd2_watch_completion_state(
        self,
        index: dict,
        rule_state: dict,
        show_state: dict,
    ) -> None:
        media_type_str = str(show_state.get("media_type") or "电视剧")
        confirmed = self._normalize_episode_map(show_state.get("confirmed_episodes_by_season"))
        total_eps = show_state.get("total_episodes")
        try:
            total_eps = int(total_eps) if total_eps not in (None, "") else None
        except (TypeError, ValueError):
            total_eps = None
        existing_count = self._episode_map_count(confirmed)
        show_state["existing_count"] = existing_count

        completed_now = False
        reason = ""
        if media_type_str == "电影":
            completed_now = -1 in set(confirmed.get(1, []))
            if completed_now:
                reason = "电影已入库"
        elif total_eps and existing_count >= total_eps:
            completed_now = True
            reason = f"已有/总集数满足 ({existing_count}/{total_eps})"

        if completed_now:
            if not show_state.get("completed"):
                completed_at = datetime.now().isoformat()
                show_state["completed"] = True
                show_state["completed_at"] = completed_at
                show_state["completed_reason"] = reason
                show_state["completion_key"] = (
                    f"{rule_state.get('id')}|{show_state.get('show_key')}|{completed_at}"
                )
                self._append_cd2_watch_completed_show(index, rule_state, show_state, reason)
            self._set_cd2_watch_pending_map(show_state, {})
        else:
            show_state["completed"] = False
            show_state.pop("completed_at", None)
            show_state.pop("completed_reason", None)
            show_state.pop("completion_key", None)

    def _get_cd2_watch_notice_lock(self):
        lock = getattr(self, "_cd2_watch_notice_lock", None)
        if lock is None:
            lock = _threading_mod.Lock()
            self._cd2_watch_notice_lock = lock
        return lock

    @staticmethod
    def _parse_iso_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    def _prune_cd2_watch_pending_notices(self, notices: Optional[List[dict]] = None) -> List[dict]:
        notices = notices if notices is not None else self._load_runtime_state().get("cd2_watch_pending_notices")
        if not isinstance(notices, list):
            return []
        now = datetime.now()
        pruned = []
        seen = set()
        for item in notices:
            if not isinstance(item, dict):
                continue
            created_at = self._parse_iso_datetime(
                item.get("copy_completed_at") or item.get("queued_at") or item.get("created_at")
            )
            if created_at and (now - created_at).total_seconds() > self._CD2_WATCH_NOTICE_TTL_SECONDS:
                continue
            key = self._cd2_watch_notice_stable_key(item) or str(item.get("key") or "")
            if key and key in seen:
                continue
            if key:
                item["key"] = key
                seen.add(key)
            pruned.append(item)
        return pruned

    def _load_cd2_watch_pending_notices(self) -> List[dict]:
        return self._prune_cd2_watch_pending_notices()

    def _save_cd2_watch_pending_notices(self, notices: List[dict]) -> None:
        state = self._load_runtime_state()
        cleaned = self._prune_cd2_watch_pending_notices(notices)
        if cleaned:
            state["cd2_watch_pending_notices"] = cleaned
        else:
            state.pop("cd2_watch_pending_notices", None)
        self._save_runtime_state()

    def _append_cd2_watch_pending_notices(self, notices: List[dict]) -> None:
        if not notices:
            return
        merged_by_key = {}
        passthrough = []
        for item in self._load_cd2_watch_pending_notices() + notices:
            if not isinstance(item, dict):
                continue
            key = self._cd2_watch_notice_stable_key(item) or str(item.get("key") or "")
            if key:
                item["key"] = key
                merged_by_key[key] = item
            else:
                passthrough.append(item)
        self._save_cd2_watch_pending_notices([*merged_by_key.values(), *passthrough])

    def _is_115_cooldown_active(self, reason: str = "") -> bool:
        state = self._load_runtime_state()
        until_raw = state.get("115_cooldown_until")
        if not until_raw:
            return False
        try:
            until_ts = float(until_raw)
        except (TypeError, ValueError):
            return False
        remaining = int(until_ts - time.time())
        if remaining <= 0:
            state.pop("115_cooldown_until", None)
            state.pop("115_cooldown_reason", None)
            self._save_runtime_state()
            return False
        if reason:
            logger.info(f"【115助手】115 风控冷却中，跳过{reason}，剩余 {remaining}s")
        return True

    def _mark_115_cooldown(self, detail: str = "") -> None:
        state = self._load_runtime_state()
        now = time.time()
        last_seen = float(state.get("115_cooldown_last_seen") or 0)
        if self._rate_limit_escalation_enabled and now - last_seen < 2 * 3600:
            consecutive = min(int(state.get("115_cooldown_consecutive") or 0) + 1, 3)
        else:
            consecutive = 1
        state["115_cooldown_consecutive"] = consecutive
        state["115_cooldown_last_seen"] = now
        cooldown_seconds = self._115_COOLDOWN_SECONDS
        if self._rate_limit_escalation_enabled:
            multipliers = {1: 1.0, 2: 1.5, 3: 2.0}
            cooldown_seconds = int(cooldown_seconds * multipliers.get(consecutive, 2.0))
        until_ts = now + cooldown_seconds
        old_until = float(state.get("115_cooldown_until") or 0)
        if until_ts > old_until:
            state["115_cooldown_until"] = until_ts
            state["115_cooldown_reason"] = detail or "rate_limit"
            self._save_runtime_state()
        logger.warning(
            f"【115助手】检测到 115 风控/访问上限，暂停会访问 115 的动作 "
            f"{cooldown_seconds}s (连续第 {consecutive} 次): {detail}"
        )

    @staticmethod
    def _looks_like_115_rate_limit(data_or_text: Any) -> bool:
        text = data_or_text
        if isinstance(data_or_text, dict):
            errno = (
                data_or_text.get("errno")
                or data_or_text.get("errNo")
                or data_or_text.get("code")
                or ""
            )
            msg = (
                data_or_text.get("error")
                or data_or_text.get("message")
                or data_or_text.get("msg")
                or ""
            )
            text = f"{errno} {msg}"
        text = str(text or "")
        return any(token in text for token in ("40140117", "访问上限", "风控", "rate limit"))

    def _fallback_copy_guard_active(self, sub: dict) -> bool:
        """已有降级/待入库任务时，保护期内不重复复制到 115。"""
        pending = self._get_pending_episode_numbers(sub)
        if not pending:
            return False
        status = str(sub.get("pending_copy_status") or "")
        guarded_statuses = {
            "copy_submitted",
            "copied_to_staging",
            "115_saved_to_staging",
            "organize_submitted",
            "copy_submit_timeout",
        }
        if status not in guarded_statuses:
            return False
        since_raw = sub.get("pending_copy_since")
        if not since_raw:
            return False
        try:
            age = (datetime.now() - datetime.fromisoformat(since_raw)).total_seconds()
        except Exception:
            return False
        if age >= self._FALLBACK_COPY_GUARD_SECONDS:
            return False
        logger.info(
            f"【115助手】{sub.get('title')} 已有降级/待入库任务在保护期内 "
            f"(status={status}, age={int(age)}s, pending={pending})，跳过重复降级复制"
        )
        return True

    @staticmethod
    def _has_active_fallback_work(sub: dict) -> bool:
        """判断降级链路是否已经接手，避免继续打开 115 分享详情。"""
        status = str(sub.get("pending_copy_status") or "")
        active_statuses = {
            "copy_submitted",
            "copied_to_staging",
            "copy_submit_timeout",
            "fallback_quota_deferred",
            "organize_submitted",
            "115_saved_to_staging",
        }
        return status in active_statuses

    def _select_subscription_batch(self, subscriptions: List[dict]) -> List[dict]:
        """按可见分批规则选择本轮要处理的订阅。"""
        if not self._batch_check_enabled:
            return subscriptions
        total = len(subscriptions)
        if total <= self._batch_size:
            state = self._load_runtime_state()
            state["batch_cursor"] = 0
            state["batch_total"] = total
            state["batch_count"] = total
            state["batch_titles"] = [s.get("title", "") for s in subscriptions]
            state["batch_updated_at"] = datetime.now().isoformat()
            self._save_runtime_state()
            return subscriptions

        state = self._load_runtime_state()
        try:
            cursor = int(state.get("batch_cursor") or 0) % total
        except (TypeError, ValueError):
            cursor = 0

        indexed = list(enumerate(subscriptions))
        selected = []
        for offset in range(min(self._batch_size, total)):
            selected.append(indexed[(cursor + offset) % total])

        next_cursor = (cursor + len(selected)) % total
        selected_subs = [sub for _, sub in selected]
        batch_no = (cursor // self._batch_size) + 1
        batch_total = (total + self._batch_size - 1) // self._batch_size
        state["batch_cursor"] = next_cursor
        state["batch_total"] = total
        state["batch_size"] = self._batch_size
        state["batch_count"] = len(selected_subs)
        state["batch_no"] = batch_no
        state["batch_total_batches"] = batch_total
        state["batch_titles"] = [s.get("title", "") for s in selected_subs]
        state["batch_updated_at"] = datetime.now().isoformat()
        self._save_runtime_state()
        logger.info(
            f"【115助手】分批巡检: 第 {batch_no}/{batch_total} 批，"
            f"本批 {len(selected_subs)}/{total} 个订阅: "
            f"{', '.join(state['batch_titles'])}"
        )
        return selected_subs

    def _check_subscriptions_impl(self):
        """定时任务：检查所有网盘订阅"""
        if not self._enabled:
            return

        if not self._pansou_url:
            # 尝试从环境变量动态读取（容器重载前的 fallback）
            self._pansou_url = os.environ.get("PANSOU_URL", "")
        if not self._pansou_url:
            logger.warning("【115助手】未配置 PanSou URL，跳过检查")
            return

        subscriptions = self._load_subscriptions()
        if not subscriptions:
            logger.info("【115助手】没有活跃的订阅")
            return

        logger.info(f"【115助手】开始检查 {len(subscriptions)} 个订阅...")
        self._fallback_copy_submits_this_run = 0
        self._direct_115_actions_this_run = 0
        batch_subscriptions = self._select_subscription_batch(subscriptions)

        active_cookie, active_cookie_source = self._resolve_115_cookie(validate=True)
        if not active_cookie:
            logger.warning(
                f"【115助手】未找到可用 115 Cookie，本次只刷新入库进度，跳过搜索转存。"
                f"当前 Cookie 来源策略: {self._cookie_source_display()}"
            )

        config_115 = {
            "cookies": active_cookie or "",
            "cookie_source": active_cookie_source or "插件Cookie",
            "default_cid": self._default_cid or "0",
            "movie_cid": self._movie_staging_cid or "",
            "old_movie_cid": self._old_movie_staging_cid or "",
            "ongoing_cid": self._ongoing_staging_cid or "",
            "archive_cid": self._archive_staging_cid or "",
            "movie_year_threshold": self._movie_year_threshold,
        }

        # ── 清理临时目录中的残留文件 ──
        if active_cookie and not self._is_115_cooldown_active("临时目录清理"):
            self._cleanup_staging_folders(config_115)

        completed = []
        for i, sub in enumerate(batch_subscriptions):
            try:
                if i > 0:
                    time.sleep(3)
                self._process_subscription(sub, config_115)
            except Exception as e:
                logger.error(f"【115助手】处理订阅 {sub.get('title')} 失败: {e}", exc_info=True)

            if sub.get("auto_completed"):
                completed.append(sub.get("title", ""))

        with self._subscriptions_lock:
            completed_subs = [s for s in subscriptions if s.get("auto_completed")]
            completed_map = {
                self._subscription_key(s): s
                for s in completed_subs
                if self._subscription_key(s)[0] is not None
            }
            completed_keys = set(completed_map.keys())
            latest_subscriptions = self._load_subscriptions()
            latest_by_key = {
                self._subscription_key(s): s
                for s in latest_subscriptions
                if self._subscription_key(s)[0] is not None
            }
            completed_names = []
            if completed_map:
                history = self._load_history()
                for key, sub in completed_map.items():
                    latest_sub = latest_by_key.get(key)
                    if not latest_sub or not self._subscription_identity_matches(latest_sub, sub):
                        logger.info(
                            f"【115助手】订阅巡检检测到 {sub.get('title', key[0])} 已被移除或重新添加，"
                            "跳过自动完结写入"
                        )
                        continue
                    item = dict(sub)
                    item["status"] = "completed"
                    history.append(item)
                    completed_names.append(item.get("title", ""))
                if completed_names:
                    self._save_history(history)
                    logger.info(f"【115助手】自动完结订阅已移入历史: {', '.join(completed_names)}")

            subscriptions = self._merge_processed_subscriptions(
                latest_subscriptions=latest_subscriptions,
                processed_subscriptions=subscriptions,
                completed_keys=completed_keys,
                context="订阅巡检",
            )
            self._save_subscriptions(subscriptions)

        if completed_names and self._notify:
            self.post_message(
                mtype=NotificationType.MediaServer,
                title="115网盘助手 - 自动完结",
                text=f"以下订阅已完结并归档:\n{', '.join(completed_names)}",
            )

        has_pending = any(self._get_pending_episode_numbers(sub) for sub in subscriptions)
        if has_pending:
            self._start_pending_organize_background(reason="订阅巡检后自动整理待入库")
        logger.info("【115助手】检查完成")

    # ── 年份清洗 ──

    _YEAR_PATTERN = re.compile(r'\s*[\(\（]?\b(19|20)\d{2}\b[\)\）]?\s*')

    @classmethod
    def _strip_year(cls, text: str) -> str:
        """去除字符串中的年份（如 '菜肉馄饨 2025' → '菜肉馄饨'）"""
        return cls._YEAR_PATTERN.sub(" ", text).strip()

    @staticmethod
    def _extract_year(text: str) -> Optional[int]:
        """从文本中提取年份"""
        m = re.search(r'\b(19|20)\d{2}\b', text)
        return int(m.group(0)) if m else None

    def _process_subscription(self, sub: dict, config_115: dict):
        """处理单个订阅"""
        title = sub.get("title", "")
        tmdb_id = sub.get("tmdb_id")
        media_type_str = sub.get("media_type", "电视剧")
        season = sub.get("season")
        search_keyword = sub.get("search_keyword", title)
        media_category = sub.get("media_category", "ongoing")

        logger.info(f"【115助手】检查: {title} (TMDB: {tmdb_id}, 季: {season})")
        try:
            self._apply_tmdb_settings()
            media_chain = MediaChain()
            meta = ParseMeta(title)
            if season:
                meta.begin_season = season
            meta.type = MediaType.MOVIE if media_type_str == "电影" else MediaType.TV
            recognized = media_chain.recognize_by_meta(meta)
            recognized_tmdb = getattr(recognized, "tmdb_id", None) if recognized else None
            if recognized_tmdb:
                try:
                    recognized_tmdb = int(recognized_tmdb)
                except (TypeError, ValueError):
                    recognized_tmdb = None
            current_tmdb = None
            try:
                current_tmdb = int(tmdb_id) if tmdb_id not in (None, "") else None
            except (TypeError, ValueError):
                current_tmdb = None
            if recognized_tmdb and recognized_tmdb != current_tmdb:
                logger.warning(
                    f"【115助手】{title} 订阅 TMDB 与识别结果不一致，自动纠正: "
                    f"{current_tmdb} -> {recognized_tmdb}"
                )
                sub["tmdb_id"] = recognized_tmdb
                tmdb_id = recognized_tmdb
        except Exception as e:
            logger.warning(f"【115助手】{title} 校验订阅 TMDB 失败: {e}")

        # 1. 获取真正已入库集数。CD2 待整理文件只参与防重复，不参与“已有/完结”判断。
        previous_cached_episodes = set()
        for ep in sub.get("_cache_existing_episodes") or []:
            try:
                previous_cached_episodes.add(int(ep))
            except (TypeError, ValueError):
                continue
        tmdb_meta_out: dict = {}
        total_eps = self._get_total_episodes(
            title, tmdb_id, media_type_str, season, meta_out=tmdb_meta_out
        )
        if tmdb_meta_out.get("fetched"):
            # 成功取到 TMDB 数据时刷新播出状态缓存（含「无下一集」这一状态），
            # 失败时沿用上次缓存，与总集数缓存的兜底策略一致。
            sub["_cache_tmdb_status"] = tmdb_meta_out.get("status") or ""
            sub["_cache_has_next_episode"] = bool(tmdb_meta_out.get("next_episode_to_air"))
            aired_now = tmdb_meta_out.get("aired_episodes")
            if aired_now:
                # 已播出集数只增不减，避免 TMDB 偶发返回空把进度冲掉
                try:
                    prev_aired = int(sub.get("_cache_aired_episodes") or 0)
                except (TypeError, ValueError):
                    prev_aired = 0
                sub["_cache_aired_episodes"] = max(prev_aired, int(aired_now))
            sub["_cache_next_air_date"] = tmdb_meta_out.get("next_air_date") or ""
        if not total_eps:
            previous_total = sub.get("_cache_total_episodes")
            if isinstance(previous_total, int) and previous_total > 0:
                logger.info(
                    f"【115助手】{title} 本次未取到总集数，沿用上次缓存: {previous_total}"
                )
                total_eps = previous_total
        real_existing_episodes = self._get_existing_episodes(
            title,
            tmdb_id,
            media_type_str,
            season,
            cached_episodes=sorted(previous_cached_episodes),
            total_episodes=total_eps,
        )
        real_existing_set = set(real_existing_episodes)
        logger.info(f"【115助手】{title} 已入库集数: {real_existing_episodes}")
        newly_existing = sorted(real_existing_set - previous_cached_episodes)
        if previous_cached_episodes and newly_existing and self._notify and media_type_str != "电影":
            try:
                notice_episodes, skipped_episodes = self._filter_plugin_notice_episodes(sub, newly_existing)
                if skipped_episodes:
                    logger.info(
                        f"【115助手】{title} 已有整理历史或插件通知账本，跳过插件补通知: "
                        f"{self._format_episode_preview(skipped_episodes)}"
                    )
                if notice_episodes:
                    self._post_organize_success_template_message(
                        subscription=sub,
                        episodes=notice_episodes,
                        reason="115 入库确认",
                    )
                    ep_text = self._format_episode_preview(notice_episodes)
                    logger.info(f"【115助手】{title} 发送新增入库补偿通知: {ep_text}")
                else:
                    logger.info(f"【115助手】{title} 新增集数均已有 MP 整理历史，无需插件补通知")
            except Exception as e:
                logger.warning(f"【115助手】{title} 发送新增入库补偿通知失败: {e}")

        pending_episodes = []
        _pending_eps = sub.get("pending_copy_episodes", [])
        _pending_since = sub.get("pending_copy_since")
        if _pending_eps and _pending_since:
            try:
                _pending_age = (datetime.now() - datetime.fromisoformat(_pending_since)).total_seconds()
                if _pending_age < self._PENDING_COPY_TTL_SECONDS:
                    pending_set = set()
                    for ep in _pending_eps:
                        try:
                            ep_num = int(ep)
                        except (TypeError, ValueError):
                            continue
                        if ep_num not in real_existing_set:
                            pending_set.add(ep_num)
                    pending_episodes = sorted(pending_set)
                    if pending_episodes:
                        sub["pending_copy_episodes"] = pending_episodes
                        self._prune_pending_source_records(sub, pending_episodes)
                        logger.info(
                            f"【115助手】{title} 待入库集数 (age={int(_pending_age)}s): {pending_episodes}"
                        )
                    else:
                        logger.info(f"【115助手】{title} 待入库集数已全部入库，清除 pending 标记")
                        sub.pop("pending_copy_episodes", None)
                        sub.pop("pending_copy_files", None)
                        sub.pop("pending_copy_since", None)
                        sub.pop("pending_copy_status", None)
                else:
                    pending_set = set()
                    for ep in _pending_eps:
                        try:
                            ep_num = int(ep)
                        except (TypeError, ValueError):
                            continue
                        if ep_num not in real_existing_set:
                            pending_set.add(ep_num)
                    pending_episodes = sorted(pending_set)
                    if pending_episodes:
                        sub["pending_copy_episodes"] = pending_episodes
                        self._prune_pending_source_records(sub, pending_episodes)
                        sub["pending_copy_status"] = "organize_retry"
                        sub["_cache_pending_retry"] = True
                        logger.info(
                            f"【115助手】{title} 待入库标记已超过50分钟，进入重试队列: {pending_episodes}"
                        )
                    else:
                        logger.info(f"【115助手】{title} 待入库集数已全部入库，清除 pending 标记")
                        sub.pop("pending_copy_episodes", None)
                        sub.pop("pending_copy_files", None)
                        sub.pop("pending_copy_since", None)
                        sub.pop("pending_copy_status", None)
            except Exception as e:
                logger.warning(f"【115助手】{title} 解析待入库集数失败: {e}")

        pending_status = str(sub.get("pending_copy_status") or "")
        pending_blocks_refetch = pending_status not in {
            "organize_retry",
            "organize_file_missing",
            "copy_failed",
        }
        effective_pending_set = set(pending_episodes) if pending_blocks_refetch else set()
        if pending_episodes and not pending_blocks_refetch:
            logger.info(
                f"【115助手】{title} pending 状态为 {pending_status}，"
                f"不再把 {pending_episodes} 当作已覆盖集数，允许后补"
            )
        effective_existing_episodes = sorted(real_existing_set | effective_pending_set)

        # 缓存进度到订阅字典（供详情页快速读取，不必再实时查 TMDB）
        sub["_cache_existing_count"] = len(real_existing_set)
        sub["_cache_existing_episodes"] = sorted(real_existing_set)
        sub["_cache_pending_count"] = len(pending_episodes)
        sub["_cache_pending_episodes"] = pending_episodes
        sub["_cache_pending_since"] = _pending_since if pending_episodes else ""
        sub["_cache_total_episodes"] = total_eps
        sub["_cache_updated"] = datetime.now().isoformat()
        target_missing_episodes = []
        if media_type_str != "电影" and total_eps:
            # 缺失集数按「TMDB 已播出集数」封顶，而不是按整季总集数。
            # 否则会去找根本没播的集，一旦命中错剧资源（聚合帖/裸数字文件），
            # 就会把别的剧当成本剧后续集入库。
            fetch_ceiling = self._airing_effective_target(sub, total_eps)
            if fetch_ceiling is None:
                fetch_ceiling = total_eps
            target_missing_episodes = sorted(
                set(range(1, int(fetch_ceiling) + 1)) - set(effective_existing_episodes)
            )
            if fetch_ceiling < total_eps:
                logger.info(
                    f"【115助手】{title} 当前缺失集数: {target_missing_episodes} "
                    f"(按已播出 {fetch_ceiling}/{total_eps} 集封顶)"
                )
            else:
                logger.info(
                    f"【115助手】{title} 当前缺失集数: {target_missing_episodes}"
                )
        # 已经确认全集入库时，必须在「追平已播出进度」判断前归档。
        # 否则全集的缺失集为空，会提前 return，既不归档也进不到兜底探针。
        if media_type_str == "电影":
            if -1 in real_existing_set:
                logger.info(f"【115助手】电影 {title} 已入库，标记自动取消订阅")
                sub["auto_completed"] = True
                sub["completed_time"] = datetime.now().isoformat()
                sub["completed_reason"] = "已入库"
                return
        elif total_eps and set(range(1, int(total_eps) + 1)).issubset(real_existing_set):
            if not self._should_allow_auto_complete(sub):
                logger.info(
                    f"【115助手】{title} 已覆盖 TMDB 当前已知集数 "
                    f"({len(real_existing_set)}/{total_eps})，但剧集仍在播出，"
                    "保持订阅等待后续更新，本轮不再搜索"
                )
                return
            logger.info(
                f"【115助手】{title} 已全集入库 ({len(real_existing_set)}/{total_eps} 集)，"
                "跳过搜索并标记自动取消"
            )
            sub["auto_completed"] = True
            sub["completed_time"] = datetime.now().isoformat()
            sub["completed_reason"] = f"全集入库 ({len(real_existing_set)}/{total_eps})"
            return

        if pending_episodes and not target_missing_episodes:
            logger.info(
                f"【115助手】{title} 已由入库+待入库覆盖当前追更目标，"
                "跳过搜索/降级，等待待入库整理确认"
            )
            return

        # 追平已播出进度就不再空搜：TMDB 总集数含未播出的集，按它算缺失会让插件
        # 每轮都去搜根本还没播的集（实测每部剧每天约 100 轮、近乎全部白搜）。
        _skip_airing, _skip_reason = self._should_skip_airing_search(
            sub, effective_existing_episodes, total_eps
        )
        if _skip_airing:
            logger.info(f"【115助手】{title} 已刷新入库进度；{_skip_reason}，本轮不搜索/降级")
            return

        if self._is_115_cooldown_active(f"{title} 的 115 搜索/转存"):
            logger.info(f"【115助手】{title} 已刷新入库进度；115 风控冷却期内不搜索/转存")
            return

        if not config_115.get("cookies"):
            logger.info(f"【115助手】{title} 已刷新入库进度；无可用 115 Cookie，跳过搜索和转存")
            return

        # 真正进入搜索/降级，记录时间供兜底探针计时
        sub["_last_search_at"] = datetime.now().isoformat()

        # 2. 搜索 PanSou — 只用纯标题（去掉年份），年份用于结果排序
        clean_keyword = self._strip_year(search_keyword)
        # 尝试从订阅信息中获取年份（优先用 TMDB 识别的年份）
        expected_year = sub.get("year") or self._extract_year(search_keyword)
        if not expected_year:
            expected_year = self._extract_year(title)

        logger.info(f"【115助手】{title} 搜索关键词: '{clean_keyword}', 期望年份: {expected_year}")
        search_results = self._search_pansou(clean_keyword, expected_year=expected_year)
        if not search_results:
            logger.info(f"【115助手】{title} 未搜索到 115 网盘资源")
            # ── 降级：尝试通过 CD2 从备用云盘转存 ──
            if self._fallback_enabled and self._fallback_clouds and self._cd2_host:
                self._try_fallback_cloud_transfer(sub, clean_keyword, expected_year, effective_existing_episodes)
            return

        logger.info(f"【115助手】{title} PanSou 搜到 {len(search_results)} 个结果")
        ranked_search_results = self._rank_episode_results_for_missing(
            search_results,
            sub,
            effective_existing_episodes,
            target_missing_episodes,
            log_context=f"{title} 115",
        )
        if ranked_search_results:
            search_results = [item["result"] for item in ranked_search_results]
        else:
            search_results = []
        precise_115_available = any(
            item.get("missing_hits") for item in ranked_search_results[:20]
        )

        # 115 当前风控敏感：即使搜到了 115 分享，也优先尝试已勾选的备用云盘。
        # 备用云盘一旦提交复制或进入 pending，本轮就不再打开 115 分享详情。
        if self._fallback_enabled and self._fallback_clouds and self._cd2_host:
            direct_used = int(getattr(self, "_direct_115_actions_this_run", 0) or 0)
            precision_direct_available = (
                self._batch_check_enabled
                and self._batch_direct_115_limit <= 0
                and precise_115_available
                and direct_used < self._PRECISION_DIRECT_115_LIMIT_PER_RUN
            )
            if precision_direct_available:
                logger.info(
                    f"【115助手】{title} 发现 115 标题精准覆盖缺失集，"
                    "本轮保留 1 次直连确认机会，备用云盘作为后置兜底"
                )
            else:
                before_fallback_submits = int(getattr(self, "_fallback_copy_submits_this_run", 0) or 0)
                before_fallback_status = str(sub.get("pending_copy_status") or "")
                self._try_fallback_cloud_transfer(
                    sub,
                    clean_keyword,
                    expected_year,
                    effective_existing_episodes,
                )
                after_fallback_submits = int(getattr(self, "_fallback_copy_submits_this_run", 0) or 0)
                after_fallback_status = str(sub.get("pending_copy_status") or "")
                if (
                    after_fallback_submits > before_fallback_submits
                    or after_fallback_status != before_fallback_status
                    or self._has_active_fallback_work(sub)
                ):
                    logger.info(
                        f"【115助手】{title} 已由备用云盘降级链路接手 "
                        f"(status={after_fallback_status or 'unknown'})，跳过 115 直连兜底"
                    )
                    return

        # 3. 确定目标 CID（区分新/老电影）
        if media_category in ("movie", "movie_archive") and media_type_str == "电影":
            threshold = config_115.get("movie_year_threshold", 2025)
            movie_year = sub.get("year") or expected_year
            if movie_year and movie_year < threshold and config_115.get("old_movie_cid"):
                target_cid = config_115["old_movie_cid"]
                logger.info(f"【115助手】{title} ({movie_year}) < {threshold} → 老电影目录")
            elif config_115.get("movie_cid"):
                target_cid = config_115["movie_cid"]
            else:
                target_cid = config_115.get("default_cid", "0")
        elif media_category == "archive" and config_115.get("archive_cid"):
            target_cid = config_115["archive_cid"]
        elif media_category == "ongoing" and config_115.get("ongoing_cid"):
            target_cid = config_115["ongoing_cid"]
        else:
            target_cid = config_115.get("default_cid", "0")

        cookies = config_115["cookies"]
        logger.info(f"【115助手】{title} 目标 CID: {target_cid}, 类别: {media_category}")

        # 4. 遍历搜索结果。电影保留较小上限；电视剧常见“一集一个分享”，需要多看一些结果。
        search_result_limit = 5 if media_type_str == "电影" else 20
        search_candidates = search_results[:search_result_limit]
        effective_direct_115_limit = self._batch_direct_115_limit
        found_episode_nums_this_run = set()
        if self._batch_check_enabled and self._batch_direct_115_limit <= 0:
            precise_candidates = [
                item["result"] for item in ranked_search_results[:search_result_limit]
                if item.get("missing_hits")
            ]
            if (
                precise_candidates
                and int(getattr(self, "_direct_115_actions_this_run", 0) or 0)
                < self._PRECISION_DIRECT_115_LIMIT_PER_RUN
            ):
                search_candidates = precise_candidates[:1]
                effective_direct_115_limit = self._PRECISION_DIRECT_115_LIMIT_PER_RUN
                logger.info(
                    f"【115助手】{title} 本批 115 常规直连上限为 0，"
                    "但允许打开 1 个精准命中缺集的分享"
                )
            else:
                logger.info(f"【115助手】{title} 本批 115 直连重动作上限为 0，跳过 115 分享详情")
                search_candidates = []
        for idx, result in enumerate(search_candidates):
            if idx > 0:
                time.sleep(1)
            share_url = result.get("url", "")
            share_password = result.get("password", "")
            share_note_full = str(result.get("note", "") or "")
            share_note = share_note_full[:80]

            if not share_url:
                continue

            if "115.com" not in share_url and "anxia.com" not in share_url and "115cdn.com" not in share_url:
                logger.info(f"【115助手】{title} 跳过非115链接: {share_url[:60]}")
                continue

            if media_type_str != "电影":
                result_text = " ".join(
                    str(result.get(key, "") or "")
                    for key in ("note", "name", "title")
                )
                hinted_episodes = self._extract_episode_hints_from_text(
                    result_text,
                    total_episodes=total_eps,
                    target_season=season,
                )
                if hinted_episodes:
                    compare_missing = (
                        set(target_missing_episodes)
                        if target_missing_episodes
                        else (set(hinted_episodes) - set(effective_existing_episodes))
                    )
                    if not (set(hinted_episodes) & compare_missing):
                        logger.info(
                            f"【115助手】{title} 跳过标题已明确为非缺失集数的分享: "
                            f"集数={self._format_episode_preview(hinted_episodes)}, "
                            f"note={share_note}"
                        )
                        continue

            try:
                share_code = share_url
                receive_code = share_password
                match = re.search(r"(?:115|anxia|115cdn)\.com/s/(\w+)", share_url)
                if match:
                    share_code = match.group(1)
                    pwd_match = re.search(r"[?&]password=(\w+)", share_url)
                    if pwd_match and not share_password:
                        receive_code = pwd_match.group(1)

                logger.info(
                    f"【115助手】{title} [{idx+1}/{len(search_candidates)}] 尝试分享: {share_code} ({share_note})"
                )

                # 5. 获取分享内容
                direct_used = int(getattr(self, "_direct_115_actions_this_run", 0) or 0)
                if self._batch_check_enabled and direct_used >= effective_direct_115_limit:
                    logger.info(
                        f"【115助手】{title} 本批 115 直连重动作额度已用完 "
                        f"({direct_used}/{effective_direct_115_limit})，停止打开更多 115 分享"
                    )
                    break
                self._direct_115_actions_this_run = direct_used + 1
                share_files = self._list_share_files_115(share_code, receive_code, cookies)
                if not share_files:
                    logger.info(f"【115助手】{title} 分享 {share_code} 获取文件列表为空")
                    continue

                logger.info(f"【115助手】{title} 分享 {share_code} 包含 {len(share_files)} 个文件/文件夹")

                # 6. 解析集数信息
                parsed_files = self._parse_files(share_files)
                logger.info(
                    f"【115助手】{title} 解析后 {len(parsed_files)} 个有效文件, "
                    f"集数: {[f.get('episode') for f in parsed_files[:10]]}"
                )

                # 7. 筛选缺失
                missing_files = self._find_missing_episodes(
                    parsed_files, season, effective_existing_episodes,
                    media_type_str=media_type_str,
                    total_episodes=total_eps,
                )
                if not missing_files:
                    logger.info(f"【115助手】{title} 分享 {share_code} 没有缺失集数")
                    continue

                logger.info(
                    f"【115助手】{title} 发现 {len(missing_files)} 个缺失文件: "
                    f"{[f.get('name', '')[:40] for f in missing_files[:5]]}"
                )

                # 8. 转存
                file_ids = [f["file_id"] for f in missing_files]
                success = self._save_share_115(
                    share_code,
                    receive_code,
                    cookies,
                    file_ids,
                    target_cid,
                    config_115.get("cookie_source", "插件Cookie"),
                )

                if not success:
                    logger.warning(f"【115助手】{title} 转存失败 (115 API 返回 false)")
                    continue

                episode_list = []
                for f in missing_files:
                    if f.get("episode"):
                        episode_list.append(f"E{f['episode']:02d}")
                ep_desc = ", ".join(episode_list) if episode_list else f"{len(missing_files)} 个文件"

                msg = f"【115助手】{title}"
                if season:
                    msg += f" 第{season}季"
                msg += f" 发现并转存了新资源: {ep_desc}"
                logger.info(msg)

                found_episode_nums = set()
                for f in missing_files:
                    for ep in (f.get("episode_list") or []):
                        try:
                            found_episode_nums.add(int(ep))
                        except (TypeError, ValueError):
                            continue
                    if f.get("episode"):
                        try:
                            found_episode_nums.add(int(f.get("episode")))
                        except (TypeError, ValueError):
                            pass

                sub["last_found"] = datetime.now().isoformat()
                sub["last_found_via"] = "115_saved"
                if found_episode_nums:
                    found_episode_nums_this_run.update(found_episode_nums)
                    sub["last_found_episodes"] = sorted(found_episode_nums_this_run)
                    self._merge_pending_source_records(sub, missing_files, season)

                if media_type_str != "电影" and found_episode_nums:
                    effective_existing_episodes = sorted(
                        set(effective_existing_episodes) | found_episode_nums
                    )
                    if total_eps:
                        target_missing_episodes = sorted(
                            set(range(1, total_eps + 1)) - set(effective_existing_episodes)
                        )
                    pending_episodes = sorted(
                        (set(pending_episodes) | found_episode_nums) - real_existing_set
                    )
                    if pending_episodes:
                        pending_since_now = datetime.now().isoformat()
                        sub["pending_copy_episodes"] = pending_episodes
                        sub["pending_copy_since"] = pending_since_now
                        sub["pending_copy_status"] = "115_saved_to_staging"
                        sub["_cache_pending_count"] = len(pending_episodes)
                        sub["_cache_pending_episodes"] = pending_episodes
                        sub["_cache_pending_since"] = pending_since_now
                        self._prune_pending_source_records(sub, pending_episodes)

                if self._notify:
                    self.post_message(
                        mtype=NotificationType.MediaServer,
                        title=f"115网盘助手 - {title}",
                        text=f"发现并转存了新资源: {ep_desc}\n来源: {result.get('note', share_url)}",
                    )

                # 电影转存成功后立刻标记完结（不用等入库检测）
                if media_type_str == "电影":
                    logger.info(f"【115助手】电影 {title} 转存成功，标记自动完结")
                    sub["auto_completed"] = True
                    sub["completed_time"] = datetime.now().isoformat()
                    sub["completed_reason"] = "转存成功"

                    break  # 电影找到有效分享即可

                if not found_episode_nums:
                    logger.info(
                        f"【115助手】{title} 本次转存无法解析具体集数，停止继续遍历以避免重复转存"
                    )
                    break

                if (
                    total_eps
                    and set(range(1, int(total_eps) + 1)).issubset(set(effective_existing_episodes))
                ):
                    logger.info(
                        f"【115助手】{title} 本轮已覆盖全集 ({len(set(effective_existing_episodes))}/{total_eps})，停止继续遍历"
                    )
                    break

                logger.info(
                    f"【115助手】{title} 本轮已处理集数: {sorted(found_episode_nums_this_run)}，继续检查后续分享"
                )

            except Exception as e:
                logger.warning(f"【115助手】处理分享 {share_url} 失败: {e}")
                continue

        # ── 电视剧：115 处理后也尝试备用云盘补全剩余集数 ──
        # 场景：115 只有部分剧集（如只有E04），阿里云盘可能有更多
        # 短路条件：总集数已知且 Plex 已有全部集数时，跳过降级（无需补全）
        _fallback_has_missing = (
            not total_eps  # 总集数未知，保守策略继续检查
            or not set(range(1, int(total_eps) + 1)).issubset(set(effective_existing_episodes))  # 确实有缺失
        )
        if (media_type_str != "电影"
                and self._fallback_enabled
                and self._fallback_clouds
                and self._cd2_host
                and _fallback_has_missing):
            self._try_fallback_cloud_transfer(sub, clean_keyword, expected_year, effective_existing_episodes)
        elif media_type_str != "电影" and not _fallback_has_missing:
            logger.info(f"【115助手】{title} 已全集入库，跳过降级转存")

        # === 自动完结检测 ===
        if media_type_str == "电影":
            # 电影：库里有了就自动完结
            if real_existing_episodes and -1 in real_existing_episodes:
                logger.info(f"【115助手】电影 {title} 已入库，标记自动取消订阅")
                sub["auto_completed"] = True
                sub["completed_time"] = datetime.now().isoformat()
                sub["completed_reason"] = "已入库"
        elif real_existing_episodes:
            # 优先用 TMDB 总集数，不可用时再从搜索描述中检测
            check_total = total_eps or self._detect_total_episodes(search_results, title)
            if (
                check_total
                and set(range(1, int(check_total) + 1)).issubset(set(real_existing_episodes))
            ):
                if not self._should_allow_auto_complete(sub):
                    logger.info(
                        f"【115助手】{title} 已覆盖当前已知 {check_total} 集，"
                        "但剧集仍在播出，暂不自动完结"
                    )
                else:
                    logger.info(
                        f"【115助手】{title} 已全集入库 ({len(real_existing_episodes)}/{check_total} 集)，"
                        f"标记自动取消"
                    )
                    sub["auto_completed"] = True
                    sub["completed_time"] = datetime.now().isoformat()
                    sub["completed_reason"] = f"全集入库 ({len(real_existing_episodes)}/{check_total})"

    def _organize_pending_subscriptions(
        self,
        tmdb_id: Optional[int] = None,
        season: Optional[int] = None,
    ) -> None:
        """用 MP 原生整理链路处理订阅中已转存但仍待入库的文件。"""
        lock = self._get_pending_organize_lock()
        if not lock.acquire(blocking=False):
            logger.info("【115助手】已有待入库整理正在运行，本次触发跳过")
            return
        try:
            subscriptions = self._load_subscriptions()
            if not subscriptions:
                logger.info("【115助手】没有活跃订阅，跳过待入库整理")
                return

            target_subs = []
            for sub in subscriptions:
                if tmdb_id and int(sub.get("tmdb_id") or 0) != int(tmdb_id):
                    continue
                if season and int(sub.get("season") or 0) != int(season):
                    continue
                if not self._get_pending_episode_numbers(sub):
                    if tmdb_id:
                        sub["_force_scan_pending_staging"] = True
                    else:
                        continue
                target_subs.append(sub)

            if not target_subs:
                logger.info("【115助手】没有待入库订阅，跳过待入库整理")
                return

            if self._is_115_cooldown_active("待入库整理"):
                return

            active_cookie, active_cookie_source = self._resolve_115_cookie(validate=True)
            if not active_cookie:
                logger.warning("【115助手】没有可用 115 Cookie，无法扫描待刮削目录")
                return

            total_files = 0
            total_subs = 0
            for sub in target_subs:
                try:
                    pending_eps = self._get_pending_episode_numbers(sub)
                    if not pending_eps and sub.pop("_force_scan_pending_staging", False):
                        pending_eps = self._discover_pending_staging_episodes(sub, active_cookie)
                        if pending_eps:
                            now_iso = datetime.now().isoformat()
                            sub["pending_copy_episodes"] = pending_eps
                            sub["pending_copy_since"] = now_iso
                            sub["pending_copy_status"] = "organize_retry"
                            sub["_cache_pending_count"] = len(pending_eps)
                            sub["_cache_pending_episodes"] = pending_eps
                            sub["_cache_pending_since"] = now_iso
                            logger.info(
                                f"【115助手】{sub.get('title')} 手动整理从待整理目录发现 pending: {pending_eps}"
                            )
                    if not pending_eps:
                        continue
                    total_subs += 1
                    total_files += self._organize_pending_subscription(
                        sub,
                        active_cookie,
                        active_cookie_source or "插件Cookie",
                    )
                except Exception as e:
                    logger.error(
                        f"【115助手】整理待入库订阅 {sub.get('title')} 失败: {e}",
                        exc_info=True,
                    )

            with self._subscriptions_lock:
                subscriptions = self._merge_processed_subscriptions(
                    latest_subscriptions=self._load_subscriptions(),
                    processed_subscriptions=subscriptions,
                    context="待入库整理",
                )
                self._save_subscriptions(subscriptions)

            logger.info(
                f"【115助手】待入库整理完成: 处理订阅 {total_subs} 个，提交整理文件 {total_files} 个"
            )
        finally:
            lock.release()

    @staticmethod
    def _get_pending_episode_numbers(sub: dict) -> List[int]:
        """读取订阅中的待入库集数。"""
        values = sub.get("pending_copy_episodes") or sub.get("_cache_pending_episodes") or []
        pending = set()
        for value in values:
            try:
                pending.add(int(value))
            except (TypeError, ValueError):
                continue
        return sorted(pending)

    def _discover_pending_staging_episodes(self, subscription: dict, cookies: str) -> List[int]:
        """手动整理兜底：从待整理目录按订阅标题发现待入库集数。"""
        cid = self._get_staging_cid_for_subscription(subscription)
        base_path = self._get_mp_staging_path(subscription)
        if not cid or cid == "0" or not base_path:
            return []
        raw_files = self._list_own_files_115_tree(cookies, cid, base_path)
        if not raw_files:
            return []
        parsed_files = self._parse_files(raw_files)
        target_season = int(subscription.get("season") or 1)
        try:
            total_episodes = int(subscription.get("_cache_total_episodes") or 0)
        except (TypeError, ValueError):
            total_episodes = 0
        episodes = set()
        for parsed in parsed_files:
            file_season = int(parsed.get("season") or 1)
            if file_season != target_season:
                continue
            name = str(parsed.get("name") or "")
            source = next((item for item in raw_files if item.get("file_id") == parsed.get("file_id")), {})
            path = str(source.get("path") or "")
            title_state = self._subscription_title_match_state(f"{path} {name}", subscription)
            if title_state is not True:
                continue
            for ep in parsed.get("episode_list") or []:
                try:
                    episodes.add(int(ep))
                except (TypeError, ValueError):
                    continue
            if parsed.get("episode"):
                try:
                    episodes.add(int(parsed["episode"]))
                except (TypeError, ValueError):
                    pass
        return sorted(
            ep for ep in episodes
            if ep > 0 and (not total_episodes or ep <= total_episodes)
        )

    @staticmethod
    def _normalize_pending_source_name(value: str) -> str:
        """Normalize a staging/source filename for matching 115 receive conflict copies."""
        name = Path(str(value or "")).name
        name = re.sub(r"\(\d+\)(?=\.[^.]+$)", "", name)
        return name.strip().lower()

    @staticmethod
    def _episode_set_from_record(record: dict) -> set:
        episodes = set()
        for ep in record.get("episodes") or record.get("episode_list") or []:
            try:
                episodes.add(int(ep))
            except (TypeError, ValueError):
                continue
        if record.get("episode"):
            try:
                episodes.add(int(record.get("episode")))
            except (TypeError, ValueError):
                pass
        return episodes

    def _merge_pending_source_records(self, subscription: dict, files: List[dict], season: Optional[int]) -> None:
        """Remember exact source filenames saved into 115 staging for later closed-loop matching."""
        records = subscription.get("pending_copy_files")
        if not isinstance(records, list):
            records = []

        merged = {}
        for record in records:
            if not isinstance(record, dict):
                continue
            name = str(record.get("name") or "").strip()
            key = self._normalize_pending_source_name(name)
            if not key:
                continue
            episodes = sorted(self._episode_set_from_record(record))
            if not episodes:
                continue
            merged[key] = {
                "name": name,
                "episodes": episodes,
                "season": record.get("season") or season or 1,
                "size": int(record.get("size", 0) or 0),
                "source_file_id": str(record.get("source_file_id") or record.get("file_id") or ""),
                "updated_at": record.get("updated_at") or datetime.now().isoformat(),
            }

        now_text = datetime.now().isoformat()
        for item in files or []:
            name = str(item.get("name") or "").strip()
            key = self._normalize_pending_source_name(name)
            episodes = sorted(self._episode_set_from_record(item))
            if not key or not episodes:
                continue
            merged[key] = {
                "name": name,
                "episodes": episodes,
                "season": item.get("season") or season or 1,
                "size": int(item.get("size", 0) or 0),
                "source_file_id": str(item.get("file_id") or ""),
                "updated_at": now_text,
            }

        if merged:
            subscription["pending_copy_files"] = list(merged.values())[-100:]

    def _prune_pending_source_records(self, subscription: dict, pending_episodes: List[int]) -> None:
        """Keep source filename records only for episodes still waiting to be organized."""
        pending_set = {int(ep) for ep in pending_episodes or [] if ep is not None}
        if not pending_set:
            subscription.pop("pending_copy_files", None)
            return
        records = subscription.get("pending_copy_files")
        if not isinstance(records, list):
            return
        kept = []
        for record in records:
            if not isinstance(record, dict):
                continue
            if self._episode_set_from_record(record) & pending_set:
                kept.append(record)
        if kept:
            subscription["pending_copy_files"] = kept[-100:]
        else:
            subscription.pop("pending_copy_files", None)

    def _pending_source_file_matches(
        self,
        subscription: dict,
        name: str,
        path: str,
        episodes: set,
        pending_set: set,
    ) -> bool:
        """Match staging files by the exact source filenames saved during 115 direct receive."""
        records = subscription.get("pending_copy_files")
        if not isinstance(records, list) or not records:
            return False

        candidate_names = {
            self._normalize_pending_source_name(name),
            self._normalize_pending_source_name(path),
        }
        candidate_names.discard("")
        if not candidate_names:
            return False

        for record in records:
            if not isinstance(record, dict):
                continue
            record_eps = self._episode_set_from_record(record)
            if record_eps and not (record_eps & episodes & pending_set):
                continue
            record_name = self._normalize_pending_source_name(str(record.get("name") or ""))
            if record_name and record_name in candidate_names:
                return True
        return False

    def _staging_file_matches_expected_tmdb(
        self,
        subscription: dict,
        name: str,
        media_cache: dict,
    ) -> bool:
        """Fallback for older pending tasks that were saved before source filename records existed."""
        try:
            expected_tmdb = int(subscription.get("tmdb_id") or 0)
        except (TypeError, ValueError):
            expected_tmdb = 0
        if not expected_tmdb:
            return False

        cache_key = self._normalize_pending_source_name(name)
        if not cache_key:
            return False
        if cache_key in media_cache:
            return bool(media_cache[cache_key])

        try:
            media_chain = media_cache.get("__media_chain")
            if media_chain is None:
                media_chain = MediaChain()
                media_cache["__media_chain"] = media_chain
            meta = ParseMeta(Path(name).stem)
            meta.type = MediaType.MOVIE if subscription.get("media_type") == "电影" else MediaType.TV
            if subscription.get("season"):
                meta.begin_season = int(subscription.get("season") or 1)
            mediainfo = media_chain.recognize_by_meta(meta)
            actual_tmdb = getattr(mediainfo, "tmdb_id", None) if mediainfo else None
            try:
                actual_tmdb = int(actual_tmdb) if actual_tmdb not in (None, "") else None
            except (TypeError, ValueError):
                actual_tmdb = None
            matched = actual_tmdb == expected_tmdb
            media_cache[cache_key] = matched
            if matched:
                logger.info(
                    f"【115助手】{subscription.get('title')} 待入库文件通过 TMDB 匹配: {name}"
                )
            return matched
        except Exception as e:
            logger.debug(f"【115助手】待入库文件 TMDB 匹配失败: {name} | {e}")
            media_cache[cache_key] = False
            return False

    def _get_staging_cid_for_subscription(self, subscription: dict) -> str:
        """根据订阅分类返回对应 115 临时目录 CID。"""
        media_type = subscription.get("media_type", "")
        media_category = subscription.get("media_category", "ongoing")
        if media_type == "电影":
            if media_category == "movie_archive" and self._old_movie_staging_cid:
                return self._old_movie_staging_cid
            return self._movie_staging_cid or self._default_cid or "0"
        if media_category == "archive":
            return self._archive_staging_cid or self._default_cid or "0"
        return self._ongoing_staging_cid or self._default_cid or "0"

    def _get_mp_staging_path(self, subscription: dict) -> str:
        """返回 MP u115 存储使用的待刮削路径。"""
        target_path = self._get_115_target_path(subscription)
        if not target_path:
            return ""
        if target_path.startswith("/115/"):
            return target_path[len("/115"):]
        if target_path == "/115":
            return "/"
        return target_path

    def _list_own_files_115_tree(
        self,
        cookies: str,
        cid: str,
        base_path: str,
        depth: int = 0,
        max_depth: int = 2,
    ) -> List[dict]:
        """列出 115 目录下的视频文件，最多递归两层，避免误扫整个盘。"""
        files = []
        for item in self._list_own_files_115(cookies, cid):
            name = item.get("name", "")
            if not name:
                continue
            item_path = f"{base_path.rstrip('/')}/{name}"
            if item.get("is_dir"):
                if depth < max_depth and item.get("fid"):
                    files.extend(
                        self._list_own_files_115_tree(
                            cookies,
                            str(item["fid"]),
                            item_path,
                            depth=depth + 1,
                            max_depth=max_depth,
                        )
                    )
                continue

            ext = ""
            if "." in name:
                ext = "." + name.rsplit(".", 1)[-1].lower()
            if ext not in self._video_extensions:
                continue
            files.append({
                "file_id": str(item.get("fid", "")),
                "name": name,
                "size": int(item.get("size", 0) or 0),
                "is_dir": False,
                "path": item_path,
            })
        return files

    def _find_pending_staging_files(
        self,
        subscription: dict,
        cookies: str,
        pending_episodes: List[int],
    ) -> List[dict]:
        """从待刮削目录中找出当前订阅 pending 集数对应的视频文件。"""
        cid = self._get_staging_cid_for_subscription(subscription)
        if not cid or cid == "0":
            logger.warning(f"【115助手】{subscription.get('title')} 未配置有效临时目录 CID")
            return []

        base_path = self._get_mp_staging_path(subscription)
        if not base_path:
            logger.warning(f"【115助手】{subscription.get('title')} 未配置对应 CD2 目标路径，无法整理待入库文件")
            return []
        raw_files = self._list_own_files_115_tree(cookies, cid, base_path)
        if not raw_files:
            logger.info(f"【115助手】{subscription.get('title')} 待刮削目录未找到视频文件")
            return []

        by_id = {item.get("file_id"): item for item in raw_files}
        parsed_files = self._parse_files(raw_files)
        try:
            total_episodes = int(subscription.get("_cache_total_episodes") or 0)
        except (TypeError, ValueError):
            total_episodes = 0
        pending_set = set()
        for ep in pending_episodes or []:
            try:
                ep_num = int(ep)
            except (TypeError, ValueError):
                continue
            if ep_num > 0 and (not total_episodes or ep_num <= total_episodes):
                pending_set.add(ep_num)
        if not pending_set:
            logger.info(
                f"【115助手】{subscription.get('title')} pending 集数均超过总集数上限，跳过待入库匹配"
            )
            return []
        title = subscription.get("title", "")
        target_season = subscription.get("season") or 1
        matched = []
        media_match_cache = {}

        for parsed in parsed_files:
            source = by_id.get(parsed.get("file_id"), {})
            name = parsed.get("name", "")
            path = source.get("path", "")

            file_season = parsed.get("season") or 1
            if target_season and file_season != int(target_season):
                continue

            episodes = set()
            for ep in parsed.get("episode_list") or []:
                try:
                    episodes.add(int(ep))
                except (TypeError, ValueError):
                    continue
            if parsed.get("episode"):
                try:
                    episodes.add(int(parsed["episode"]))
                except (TypeError, ValueError):
                    pass

            if not episodes or not (episodes & pending_set):
                continue
            if total_episodes and any(ep > total_episodes for ep in episodes):
                logger.info(
                    f"【115助手】{title} 待入库跳过超过总集数的文件: "
                    f"episodes={sorted(episodes)}, total={total_episodes}, name={name}"
                )
                continue

            # 优先靠标题收窄范围；文件名本身一旦明确是串剧，不能被
            # 待整理路径里的目标剧名放行。115 直转保留英文源名时，
            # 再改用转存时记录的源文件名或 TMDB 兜底识别。
            name_title_state = self._subscription_title_match_state(name, subscription)
            if name_title_state is False:
                logger.info(
                    f"【115助手】{title} 待入库跳过疑似串剧文件: {name}"
                )
                continue
            path_title_state = self._subscription_title_match_state(path, subscription)
            if name_title_state is None and path_title_state is False:
                logger.info(
                    f"【115助手】{title} 待入库跳过疑似串剧路径: {path}"
                )
                continue

            title_hit = (
                name_title_state is True
                or (name_title_state is None and path_title_state is True)
            )
            source_hit = self._pending_source_file_matches(
                subscription,
                name,
                path,
                episodes,
                pending_set,
            )
            tmdb_hit = False
            if title and not title_hit and not source_hit:
                tmdb_hit = self._staging_file_matches_expected_tmdb(
                    subscription,
                    name,
                    media_match_cache,
                )
            if title and not (title_hit or source_hit or tmdb_hit):
                continue

            matched.append({
                **source,
                "season": file_season,
                "episodes": sorted(episodes),
            })

        matched.sort(key=lambda item: item.get("path", ""))
        deduped_by_episode = {}
        for item in matched:
            episodes = [ep for ep in item.get("episodes") or [] if ep in pending_set]
            if not episodes:
                continue
            path = item.get("path", "")
            name = item.get("name", "")
            duplicate_rank = 1 if re.search(r"\(\d+\)(?=\.[^.]+$)", name) else 0
            for ep in episodes:
                current = deduped_by_episode.get(ep)
                current_rank = 1 if current and re.search(r"\(\d+\)(?=\.[^.]+$)", current.get("name", "")) else 0
                if not current or (duplicate_rank, len(path), path) < (current_rank, len(current.get("path", "")), current.get("path", "")):
                    deduped_by_episode[ep] = item

        if deduped_by_episode:
            deduped = []
            seen_paths = set()
            for ep in sorted(deduped_by_episode):
                item = deduped_by_episode[ep]
                path = item.get("path", "")
                if path in seen_paths:
                    continue
                seen_paths.add(path)
                deduped.append(item)
            if len(deduped) != len(matched):
                logger.info(
                    f"【115助手】{title} 待入库文件去重: {len(matched)} -> {len(deduped)}"
                )
            matched = deduped
        logger.info(
            f"【115助手】{title} 待入库匹配到 {len(matched)} 个文件: "
            f"{[item.get('name') for item in matched[:5]]}"
        )
        return matched

    def _get_closed_loop_library_path(self, subscription: dict) -> str:
        """根据订阅分类返回插件闭环整理的正式媒体库路径。"""
        media_type = subscription.get("media_type", "")
        media_category = subscription.get("media_category", "ongoing")
        if media_type == "电影":
            if media_category == "movie_archive":
                return self._to_cd2_115_path(self._closed_loop_old_movie_library_path).rstrip("/")
            return self._to_cd2_115_path(self._closed_loop_movie_library_path).rstrip("/")
        if media_category == "archive":
            return self._to_cd2_115_path(self._closed_loop_archive_library_path).rstrip("/")
        return self._to_cd2_115_path(self._closed_loop_ongoing_library_path).rstrip("/")

    def _closed_loop_available_for_subscription(self, subscription: dict) -> bool:
        """闭环整理是否可以接管当前订阅。"""
        if not self._closed_loop_organize_enabled:
            return False
        if not self._cd2_host or not (self._cd2_token or (self._cd2_username and self._cd2_password)):
            logger.warning("【115助手】插件闭环整理已开启，但 CD2 未配置，回退 MP 原生整理")
            return False
        if not self._get_closed_loop_library_path(subscription):
            logger.warning(
                f"【115助手】{subscription.get('title')} 未配置闭环正式库目录，回退 MP 原生整理"
            )
            return False
        return True

    @staticmethod
    def _to_cd2_115_path(path: str) -> str:
        """把 115 Web API 路径转换成 CD2 挂载路径。"""
        path = str(path or "").strip()
        if not path:
            return ""
        if path.startswith("/115/") or path == "/115":
            return path
        if path.startswith("/"):
            return f"/115{path}"
        return f"/115/{path}"

    def _pending_staging_path_to_cd2_path(self, subscription: dict, web_path: str) -> str:
        """
        将待入库扫描得到的 115 Web 路径映射回 CD2 路径。
        优先使用用户配置的 115 目标目录作为 CD2 根，避免硬编码 /115。
        """
        web_path = str(web_path or "").strip()
        if not web_path:
            return ""
        cd2_root = self._get_115_target_path(subscription).rstrip("/")
        mp_root = self._get_mp_staging_path(subscription).rstrip("/")
        if cd2_root and mp_root:
            normalized_web = web_path.rstrip("/")
            normalized_root = mp_root.rstrip("/")
            if normalized_web == normalized_root:
                return cd2_root
            prefix = f"{normalized_root}/"
            if normalized_web.startswith(prefix):
                rel_path = normalized_web[len(prefix):].lstrip("/")
                return f"{cd2_root}/{rel_path}" if rel_path else cd2_root
        return self._to_cd2_115_path(web_path)

    @staticmethod
    def _safe_path_name(value: Any, fallback: str = "Unknown") -> str:
        """清洗路径片段，避免云盘路径里出现非法字符。"""
        text = str(value or "").strip()
        text = re.sub(r'[\\/:*?"<>|]', "_", text)
        text = re.sub(r"\s+", " ", text).strip(" ._")
        return text or fallback

    def _closed_loop_show_dir_name(self, subscription: dict, mediainfo: Any = None) -> str:
        title = (
            getattr(mediainfo, "title", None)
            or getattr(mediainfo, "name", None)
            or subscription.get("title")
            or "Unknown"
        )
        year = getattr(mediainfo, "year", None) or subscription.get("year")
        base = self._safe_path_name(title)
        return f"{base} ({year})" if year else base

    def _closed_loop_episode_filename(
        self,
        subscription: dict,
        source_name: str,
        season: int,
        episodes: List[int],
        mediainfo: Any = None,
    ) -> str:
        """生成与 MP 本体默认模板接近的剧集文件名。"""
        ext = Path(source_name or "").suffix
        root = Path(source_name or "").stem
        title = (
            getattr(mediainfo, "title", None)
            or getattr(mediainfo, "name", None)
            or subscription.get("title")
            or "Unknown"
        )
        safe_title = self._safe_path_name(title)
        cleaned_eps = sorted({int(ep) for ep in episodes if ep is not None})
        if len(cleaned_eps) == 1:
            ep_tag = f"E{cleaned_eps[0]:02d}"
        elif cleaned_eps:
            ep_tag = f"E{cleaned_eps[0]:02d}-E{cleaned_eps[-1]:02d}"
        else:
            ep = self._parse_episode_number(source_name or "")
            ep_tag = f"E{ep:02d}" if ep is not None else "E00"

        season_episode = f"S{int(season or 1):02d}{ep_tag}"
        if len(cleaned_eps) == 1:
            canonical = f"{safe_title} - {season_episode} - 第 {cleaned_eps[0]} 集"
        else:
            canonical = f"{safe_title} - {season_episode}"

        raw_year = getattr(mediainfo, "year", None) or subscription.get("year")
        year_text = ""
        if raw_year:
            try:
                year_text = str(int(raw_year))
            except (TypeError, ValueError):
                year_text = str(raw_year).strip()
        suffix = root.strip()
        suffix = re.sub(r"\{tmdb-\d+\}", "", suffix, flags=re.IGNORECASE).strip(" ._-")
        title_pattern = re.escape(safe_title).replace(r"\ ", r"[\s._-]+")
        cleanup_patterns = []
        if year_text:
            cleanup_patterns.extend([
                rf"^{title_pattern}[\s._-]*(?:\({re.escape(year_text)}\)|{re.escape(year_text)})[\s._-]*",
                rf"^(?:\({re.escape(year_text)}\)|{re.escape(year_text)})[\s._-]*",
            ])
        cleanup_patterns.extend([
            rf"^{title_pattern}[\s._-]*",
            rf"^[Ss]{int(season or 1):02d}\s*[\.\-_\s]?\s*{ep_tag}\s*[\s\._\-~]*",
            rf"^[Ss]{int(season or 1)}\s*[\.\-_\s]?\s*{ep_tag}\s*[\s\._\-~]*",
            r"^第\s*\d+\s*[集话話]\s*[\s\._\-~]*",
            r"^[Ee][Pp]?\d{1,3}\b\s*[\s\._\-]*",
            r"^\d{1,3}(?:[\s\._\-~]+|$)\s*",
        ])
        for pattern in cleanup_patterns:
            new_suffix = re.sub(pattern, "", suffix, count=1, flags=re.IGNORECASE).strip(" ._-")
            if new_suffix != suffix:
                suffix = new_suffix
        suffix = re.sub(r"^第\s*\d+\s*[集话話]\s*[\s\._\-~]*", "", suffix).strip(" ._-")
        for pattern in (
            r'^[Ss]\d+\s*[\.\-_\s]?\s*[Ee]\d+(?:\s*-\s*[Ee]?\d+)?\s*[\s\._\-~]*',
            r'^[Ee][Pp]?\d{1,3}\b\s*[\s\._\-]*',
            r'^第\d+[集话話]\s*[\s\._\-]*',
            r'^\d{1,3}(?:[\s\._\-~]+|$)\s*',
        ):
            new_suffix = re.sub(pattern, "", suffix, count=1, flags=re.IGNORECASE).strip(" ._-")
            if new_suffix != suffix:
                suffix = new_suffix
                break
        if suffix.lower().startswith(safe_title.lower()):
            suffix = suffix[len(safe_title):].strip(" ._-")

        name = canonical
        if suffix:
            name = f"{name} - {self._safe_path_name(suffix)}"
        return f"{name}{ext}"

    def _ensure_cd2_dir(self, cd2, path: str) -> bool:
        """递归确保 CD2 目录存在，第一段为挂载根，只验证不创建。"""
        path = str(path or "").strip().rstrip("/")
        if not path or path == "/":
            return True
        parts = [part for part in path.split("/") if part]
        if not parts:
            return True

        current = f"/{parts[0]}"
        items = cd2.list_dir(current, force_refresh=True)
        if getattr(cd2, "last_error", ""):
            logger.warning(f"【115助手】闭环整理无法访问 CD2 根目录: {current} | {cd2.last_error}")
            return False

        for part in parts[1:]:
            found = next(
                (item for item in (items or []) if item.get("is_dir") and item.get("name") == part),
                None,
            )
            next_path = f"{current}/{part}"
            if not found:
                if not cd2.create_folder(current, part):
                    logger.warning(f"【115助手】闭环整理创建目录失败: {next_path}")
                    return False
                logger.info(f"【115助手】闭环整理创建目录: {next_path}")
                current = next_path
            else:
                current = found.get("path") or next_path
            items = cd2.list_dir(current, force_refresh=True)
            if getattr(cd2, "last_error", ""):
                logger.warning(f"【115助手】闭环整理读取目录失败: {current} | {cd2.last_error}")
                return False
        return True

    def _closed_loop_history_key(
        self,
        tmdb_id: Optional[int],
        media_type_str: str,
        season: Optional[int],
    ) -> str:
        media_id = str(tmdb_id or "unknown")
        season_part = "movie" if media_type_str == "电影" else f"S{int(season or 1):02d}"
        return f"{media_type_str}|{media_id}|{season_part}"

    def _get_closed_loop_organized_episodes(
        self,
        tmdb_id: Optional[int],
        media_type_str: str,
        season: Optional[int],
    ) -> List[int]:
        state = self._load_runtime_state()
        history = state.get(self._CLOSED_LOOP_HISTORY_KEY)
        if not isinstance(history, dict):
            return []
        key = self._closed_loop_history_key(tmdb_id, media_type_str, season)
        item = history.get(key)
        if not isinstance(item, dict):
            return []
        if media_type_str == "电影":
            return [-1] if item.get("movie_exists") else []
        episodes = set()
        for ep in item.get("episodes") or []:
            try:
                episodes.add(int(ep))
            except (TypeError, ValueError):
                continue
        return sorted(episodes)

    def _append_closed_loop_organized_records(
        self,
        subscription: dict,
        records: List[dict],
    ) -> None:
        """记录插件闭环入库账本，用于后续缺集判断和防重复通知。"""
        if not records:
            return
        state = self._load_runtime_state()
        history = state.get(self._CLOSED_LOOP_HISTORY_KEY)
        if not isinstance(history, dict):
            history = {}
            state[self._CLOSED_LOOP_HISTORY_KEY] = history

        media_type_str = subscription.get("media_type", "电视剧")
        season = subscription.get("season") or 1
        key = self._closed_loop_history_key(subscription.get("tmdb_id"), media_type_str, season)
        item = history.get(key)
        if not isinstance(item, dict):
            item = {
                "title": subscription.get("title"),
                "year": subscription.get("year"),
                "tmdb_id": subscription.get("tmdb_id"),
                "media_type": media_type_str,
                "season": season,
                "episodes": [],
                "files": [],
            }
            history[key] = item

        if media_type_str == "电影":
            item["movie_exists"] = True
        else:
            episodes = set()
            for ep in item.get("episodes") or []:
                try:
                    episodes.add(int(ep))
                except (TypeError, ValueError):
                    continue
            for record in records:
                for ep in record.get("episodes") or []:
                    try:
                        episodes.add(int(ep))
                    except (TypeError, ValueError):
                        continue
            item["episodes"] = sorted(episodes)

        files = item.get("files")
        if not isinstance(files, list):
            files = []
        seen_paths = {str(file.get("target_path") or "") for file in files if isinstance(file, dict)}
        for record in records:
            target_path = str(record.get("target_path") or "")
            if target_path and target_path in seen_paths:
                continue
            files.append({
                "name": record.get("name"),
                "source_path": record.get("source_path"),
                "target_path": target_path,
                "size": record.get("size"),
                "episodes": record.get("episodes") or [],
                "organized_at": datetime.now().isoformat(),
            })
            if target_path:
                seen_paths.add(target_path)
        item["files"] = files[-200:]
        item["updated_at"] = datetime.now().isoformat()
        self._save_runtime_state()

    @staticmethod
    def _bytes_from_metadata(value: Any) -> Optional[bytes]:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        return str(value).encode("utf-8")

    def _download_metadata_image(self, url: str) -> Optional[bytes]:
        if not url:
            return None
        try:
            with httpx.Client(timeout=25.0, follow_redirects=True) as client:
                resp = client.get(str(url))
                resp.raise_for_status()
                return resp.content
        except Exception as e:
            logger.debug(f"【115助手】下载刮削图片失败: {url} | {e}")
            return None

    def _write_closed_loop_metadata(
        self,
        cd2,
        subscription: dict,
        show_dir: str,
        season_dir: str,
        records: List[dict],
        mediainfo: Any,
        meta: Any,
    ) -> None:
        """写入基础 NFO/海报；不写每集海报。"""
        if not self._closed_loop_scrape_metadata or not records or not mediainfo or not meta:
            return

        try:
            media_chain = MediaChain()
        except Exception as e:
            logger.warning(f"【115助手】闭环刮削初始化失败: {e}")
            return

        season = int(subscription.get("season") or 1)
        try:
            tv_nfo = self._bytes_from_metadata(
                media_chain.metadata_nfo(meta=meta, mediainfo=mediainfo)
            )
            if tv_nfo:
                cd2.write_file_bytes(show_dir, "tvshow.nfo", tv_nfo, overwrite=False)
        except Exception as e:
            logger.debug(f"【115助手】写入 tvshow.nfo 失败: {e}")

        try:
            season_nfo = self._bytes_from_metadata(
                media_chain.metadata_nfo(meta=meta, mediainfo=mediainfo, season=season)
            )
            if season_nfo:
                cd2.write_file_bytes(season_dir, "season.nfo", season_nfo, overwrite=False)
        except Exception as e:
            logger.debug(f"【115助手】写入 season.nfo 失败: {e}")

        for record in records:
            record_eps = sorted({int(ep) for ep in (record.get("episodes") or []) if ep is not None})
            target_name = Path(str(record.get("target_path") or record.get("name") or "")).name
            if len(record_eps) != 1 or not target_name:
                continue
            ep_num = record_eps[0]
            try:
                ep_meta = ParseMeta(
                    f"{subscription.get('title', '')} S{season:02d}E{ep_num:02d}"
                )
                ep_meta.type = MediaType.TV
                ep_meta.begin_season = season
                ep_meta.begin_episode = ep_num
                episode_nfo = self._bytes_from_metadata(
                    media_chain.metadata_nfo(
                        meta=ep_meta,
                        mediainfo=mediainfo,
                        season=season,
                        episode=ep_num,
                    )
                )
                if episode_nfo:
                    cd2.write_file_bytes(
                        season_dir,
                        f"{Path(target_name).stem}.nfo",
                        episode_nfo,
                        overwrite=False,
                    )
            except Exception as e:
                logger.debug(f"【115助手】写入 E{ep_num:02d} nfo 失败: {e}")

        def _write_images(image_map: dict, parent_path: str, season_scope: bool = False) -> None:
            if not isinstance(image_map, dict):
                return
            for image_name, image_url in image_map.items():
                image_name = str(image_name or "").strip()
                if not image_name or not image_url:
                    continue
                image_bytes = self._download_metadata_image(str(image_url))
                if not image_bytes:
                    continue
                cd2.write_file_bytes(parent_path, image_name, image_bytes, overwrite=False)

                lower = image_name.lower()
                ext = Path(image_name).suffix or ".jpg"
                if lower.startswith("backdrop."):
                    cd2.write_file_bytes(parent_path, f"fanart{ext}", image_bytes, overwrite=False)
                if lower.startswith("thumb."):
                    cd2.write_file_bytes(parent_path, f"landscape{ext}", image_bytes, overwrite=False)

                if season_scope and lower.startswith("season") and "-poster" in lower:
                    cd2.write_file_bytes(season_dir, f"poster{ext}", image_bytes, overwrite=False)

        try:
            _write_images(media_chain.metadata_img(mediainfo=mediainfo) or {}, show_dir)
        except Exception as e:
            logger.debug(f"【115助手】写入剧集海报失败: {e}")

        try:
            _write_images(
                media_chain.metadata_img(mediainfo=mediainfo, season=season) or {},
                show_dir,
                season_scope=True,
            )
        except Exception as e:
            logger.debug(f"【115助手】写入季海报失败: {e}")

    def _organize_pending_subscription_closed_loop(
        self,
        subscription: dict,
        files: List[dict],
        pending_episodes: List[int],
        before_existing: set,
    ) -> Optional[int]:
        """插件闭环整理：直接通过 CD2 移动/重命名到正式媒体库。"""
        if not self._closed_loop_available_for_subscription(subscription):
            return None

        media_type_str = subscription.get("media_type", "电视剧")
        if media_type_str == "电影":
            logger.info("【115助手】电影闭环整理暂未接管，回退 MP 原生整理")
            return None

        title = subscription.get("title", "")
        season = int(subscription.get("season") or 1)
        library_root = self._get_closed_loop_library_path(subscription)

        try:
            cd2 = self._build_cd2_client()
        except Exception as e:
            logger.warning(f"【115助手】闭环整理初始化 CD2 失败: {e}")
            return 0
        if not cd2.test_connection():
            logger.warning(f"【115助手】闭环整理 CD2 连接失败: {self._format_cd2_connection_error(cd2)}")
            return 0

        sample_name = files[0].get("name", "") if files else title
        meta, mediainfo = self._build_notification_media_context(
            subscription,
            pending_episodes,
            sample_name,
        )
        show_dir_name = self._closed_loop_show_dir_name(subscription, mediainfo)
        show_dir = f"{library_root.rstrip('/')}/{show_dir_name}"
        if not self._ensure_cd2_dir(cd2, show_dir):
            subscription["pending_copy_status"] = "closed_loop_target_missing"
            subscription["pending_last_error"] = f"闭环整理无法创建剧集目录: {show_dir}"
            return 0
        season_dir = self._find_or_create_season_dir(cd2, show_dir, season)
        if not self._ensure_cd2_dir(cd2, season_dir):
            subscription["pending_copy_status"] = "closed_loop_target_missing"
            subscription["pending_last_error"] = f"闭环整理无法创建季目录: {season_dir}"
            return 0

        moved_records: List[dict] = []
        pending_set = set(pending_episodes)
        for item in files:
            name = item.get("name", "")
            source_path = item.get("path", "")
            if not name or not source_path:
                continue
            item_eps_set = set()
            for ep in item.get("episodes") or []:
                try:
                    ep_num = int(ep)
                except (TypeError, ValueError):
                    continue
                if ep_num in pending_set:
                    item_eps_set.add(ep_num)
            item_eps = sorted(item_eps_set)
            if not item_eps:
                ep = self._parse_episode_number(name) or self._parse_episode_number(source_path)
                item_eps = [ep] if ep and ep in pending_set else []
            if not item_eps:
                logger.info(f"【115助手】闭环整理跳过无法确认集数的文件: {name}")
                continue

            new_name = self._closed_loop_episode_filename(
                subscription=subscription,
                source_name=name,
                season=season,
                episodes=item_eps,
                mediainfo=mediainfo,
            )
            src_cd2_path = self._pending_staging_path_to_cd2_path(subscription, source_path)
            moved_paths = cd2.move_file([src_cd2_path], season_dir, conflict_policy="Rename")
            if not moved_paths:
                attempts = int(subscription.get("pending_organize_attempts") or 0) + 1
                subscription["pending_organize_attempts"] = attempts
                subscription["pending_copy_status"] = "closed_loop_move_failed"
                subscription["pending_last_error"] = f"闭环整理移动失败: {src_cd2_path}"
                logger.warning(f"【115助手】闭环整理移动失败: {src_cd2_path} -> {season_dir}")
                continue

            moved_path = moved_paths[0]
            final_path = moved_path
            if Path(moved_path).name != new_name:
                if cd2.rename_file(moved_path, new_name):
                    final_path = str(PurePosixPath(moved_path).with_name(new_name))
                else:
                    logger.warning(f"【115助手】闭环整理重命名失败，保留原名: {moved_path}")

            record = {
                "title": title,
                "year": subscription.get("year"),
                "tmdb_id": subscription.get("tmdb_id"),
                "media_type": media_type_str,
                "category": subscription.get("media_category"),
                "season": season,
                "episodes": item_eps,
                "source_files": [{
                    "name": new_name,
                    "path": final_path,
                    "size": int(item.get("size", 0) or 0),
                    "episodes": item_eps,
                }],
                "source_path": source_path,
                "target_path": final_path,
                "name": new_name,
                "size": int(item.get("size", 0) or 0),
            }
            moved_records.append(record)
            logger.info(
                f"【115助手】闭环整理入库: {source_path} -> {final_path} "
                f"({self._format_episode_preview(item_eps)})"
            )
            time.sleep(0.5)

        moved_episodes = sorted({
            int(ep)
            for record in moved_records
            for ep in (record.get("episodes") or [])
            if ep is not None
        })
        if not moved_episodes:
            return 0

        self._write_closed_loop_metadata(
            cd2=cd2,
            subscription=subscription,
            show_dir=show_dir,
            season_dir=season_dir,
            records=moved_records,
            mediainfo=mediainfo,
            meta=meta,
        )
        self._append_closed_loop_organized_records(subscription, moved_records)

        refreshed = self._refresh_cd2_watch_mediaservers(moved_records)
        if refreshed:
            logger.info(f"【115助手】闭环整理已请求局部刷新媒体库: {', '.join(refreshed)}")

        refreshed_existing = sorted(set(before_existing) | set(moved_episodes))
        remaining = sorted(set(pending_episodes) - set(refreshed_existing))
        subscription["_cache_existing_count"] = len(refreshed_existing)
        subscription["_cache_existing_episodes"] = refreshed_existing
        cache_key = self._existing_episodes_cache_key(
            title,
            subscription.get("tmdb_id"),
            media_type_str,
            season,
        )
        self._set_existing_episodes_cache(cache_key, refreshed_existing)
        if remaining:
            subscription["pending_copy_episodes"] = remaining
            self._prune_pending_source_records(subscription, remaining)
            subscription["_cache_pending_count"] = len(remaining)
            subscription["_cache_pending_episodes"] = remaining
            subscription["_cache_pending_since"] = subscription.get("pending_copy_since", "")
            subscription["pending_copy_status"] = "closed_loop_partial"
            subscription["pending_last_error"] = f"闭环整理后仍待入库: {remaining}"
        else:
            subscription.pop("pending_copy_episodes", None)
            subscription.pop("pending_copy_files", None)
            subscription.pop("pending_copy_since", None)
            subscription.pop("pending_copy_status", None)
            subscription.pop("pending_organize_attempts", None)
            subscription.pop("pending_last_error", None)
            subscription["_cache_pending_count"] = 0
            subscription["_cache_pending_episodes"] = []
            subscription["_cache_pending_since"] = ""

        if self._notify:
            notice_source_files = [
                {
                    "name": record.get("name"),
                    "path": record.get("target_path"),
                    "size": record.get("size") or 0,
                    "episodes": record.get("episodes") or [],
                }
                for record in moved_records
            ]
            self._post_organize_success_template_message(
                subscription=subscription,
                episodes=moved_episodes,
                source_files=notice_source_files,
                reason="115 入库确认",
            )

        subscription["_cache_updated"] = datetime.now().isoformat()
        return len(moved_records)

    def _organize_pending_subscription(
        self,
        subscription: dict,
        cookies: str,
        cookie_source: str,
    ) -> int:
        """对单个订阅提交 MP 原生整理。"""
        title = subscription.get("title", "")
        tmdb_id = subscription.get("tmdb_id")
        media_type_str = subscription.get("media_type", "电视剧")
        season = subscription.get("season") or 1
        pending_episodes = self._get_pending_episode_numbers(subscription)
        if not pending_episodes:
            return 0

        total_episodes = None
        try:
            total_episodes = int(subscription.get("_cache_total_episodes") or 0) or None
        except (TypeError, ValueError):
            total_episodes = None
        if media_type_str != "电影" and not total_episodes:
            total_episodes = self._get_total_episodes(title, tmdb_id, media_type_str, season)
            if total_episodes:
                subscription["_cache_total_episodes"] = total_episodes
        if media_type_str != "电影" and total_episodes:
            capped_values = set()
            for ep in pending_episodes:
                try:
                    ep_num = int(ep)
                except (TypeError, ValueError):
                    continue
                if 0 < ep_num <= int(total_episodes):
                    capped_values.add(ep_num)
            capped_pending = sorted(capped_values)
            if len(capped_pending) != len(set(pending_episodes)):
                logger.warning(
                    f"【115助手】{title} pending 集数超过 TMDB 总集数 {total_episodes}，"
                    f"本轮仅处理: {capped_pending}"
                )
                subscription["pending_copy_episodes"] = capped_pending
                subscription["_cache_pending_count"] = len(capped_pending)
                subscription["_cache_pending_episodes"] = capped_pending
            pending_episodes = capped_pending

        cached_existing = subscription.get("_cache_existing_episodes") or []
        real_existing = set(
            self._get_existing_episodes(
                title,
                tmdb_id,
                media_type_str,
                season,
                cached_episodes=cached_existing,
                total_episodes=total_episodes,
            )
        )
        before_existing = set(real_existing)
        pending_episodes = sorted(set(pending_episodes) - real_existing)
        if not pending_episodes:
            logger.info(f"【115助手】{title} 待入库集数已全部入库，清除 pending 标记")
            subscription.pop("pending_copy_episodes", None)
            subscription.pop("pending_copy_files", None)
            subscription.pop("pending_copy_since", None)
            subscription.pop("pending_copy_status", None)
            subscription.pop("pending_organize_attempts", None)
            subscription.pop("pending_last_error", None)
            subscription["_cache_pending_count"] = 0
            subscription["_cache_pending_episodes"] = []
            subscription["_cache_pending_since"] = ""
            return 0

        files = self._find_pending_staging_files(subscription, cookies, pending_episodes)
        if not files:
            attempts = int(subscription.get("pending_organize_attempts") or 0) + 1
            subscription["pending_organize_attempts"] = attempts
            subscription["pending_copy_status"] = "organize_file_missing"
            subscription["pending_last_error"] = f"待刮削目录未找到 pending 文件，attempts={attempts}"
            logger.warning(
                f"【115助手】{title} 未找到待入库文件，pending={pending_episodes}，"
                f"Cookie来源={cookie_source}，attempts={attempts}"
            )
            return 0

        closed_loop_result = self._organize_pending_subscription_closed_loop(
            subscription=subscription,
            files=files,
            pending_episodes=pending_episodes,
            before_existing=before_existing,
        )
        if closed_loop_result is not None:
            return closed_loop_result

        try:
            from app.chain.transfer import TransferChain
            from app.schemas.file import FileItem
        except Exception as e:
            logger.error(f"【115助手】导入 MP 整理链路失败: {e}")
            return 0

        transfer_chain = TransferChain()
        mtype = MediaType.MOVIE if media_type_str == "电影" else MediaType.TV
        submitted = 0
        for item in files:
            path = item.get("path", "")
            name = item.get("name", "")
            if not path or not name:
                continue

            try:
                logger.info(f"【115助手】提交 MP 原生整理(force=True): {path}")
                state, errmsg = transfer_chain.manual_transfer(
                    fileitem=FileItem(
                        storage="u115",
                        path=path,
                        type="file",
                        name=name,
                        basename=Path(name).stem,
                        extension=Path(name).suffix.lstrip("."),
                        size=int(item.get("size", 0) or 0),
                    ),
                    tmdbid=int(tmdb_id) if tmdb_id else None,
                    mtype=mtype,
                    season=int(season) if media_type_str != "电影" and season else None,
                    force=True,
                    background=False,
                )
                if state:
                    submitted += 1
                    subscription["pending_organize_attempts"] = 0
                    subscription.pop("pending_last_error", None)
                    logger.info(f"【115助手】MP 原生整理提交成功: {name}")
                else:
                    attempts = int(subscription.get("pending_organize_attempts") or 0) + 1
                    subscription["pending_organize_attempts"] = attempts
                    subscription["pending_last_error"] = errmsg or "MP 原生整理返回失败"
                    if self._looks_like_115_rate_limit(errmsg):
                        self._mark_115_cooldown(f"MP 原生整理失败: {errmsg}")
                    logger.warning(f"【115助手】MP 原生整理失败: {name} | {errmsg}")
            except Exception as e:
                attempts = int(subscription.get("pending_organize_attempts") or 0) + 1
                subscription["pending_organize_attempts"] = attempts
                subscription["pending_last_error"] = str(e)
                if self._looks_like_115_rate_limit(e):
                    self._mark_115_cooldown(f"MP 原生整理异常: {e}")
                logger.error(f"【115助手】MP 原生整理异常: {name} | {e}", exc_info=True)
            time.sleep(0.5)

        refreshed_existing = set(
            self._get_existing_episodes(
                title,
                tmdb_id,
                media_type_str,
                season,
                force_refresh=True,
                cached_episodes=subscription.get("_cache_existing_episodes") or [],
                total_episodes=total_episodes,
            )
        )
        newly_existing = sorted(refreshed_existing - before_existing)
        remaining = sorted(set(pending_episodes) - refreshed_existing)
        subscription["_cache_existing_count"] = len(refreshed_existing)
        subscription["_cache_existing_episodes"] = sorted(refreshed_existing)
        if remaining:
            subscription["pending_copy_episodes"] = remaining
            self._prune_pending_source_records(subscription, remaining)
            subscription["_cache_pending_count"] = len(remaining)
            subscription["_cache_pending_episodes"] = remaining
            subscription["_cache_pending_since"] = subscription.get("pending_copy_since", "")
            subscription["pending_copy_status"] = "organize_submitted"
            if submitted:
                subscription["pending_last_error"] = f"仍待入库: {remaining}"
        else:
            subscription.pop("pending_copy_episodes", None)
            subscription.pop("pending_copy_files", None)
            subscription.pop("pending_copy_since", None)
            subscription.pop("pending_copy_status", None)
            subscription.pop("pending_organize_attempts", None)
            subscription.pop("pending_last_error", None)
            subscription["_cache_pending_count"] = 0
            subscription["_cache_pending_episodes"] = []
            subscription["_cache_pending_since"] = ""
        if newly_existing:
            self._refresh_subscription_mediaservers(
                subscription=subscription,
                episodes=newly_existing,
            )
            if self._notify:
                self._post_organize_success_template_message(
                    subscription=subscription,
                    episodes=newly_existing,
                    source_files=files,
                    reason="115 入库确认",
                )
        subscription["_cache_updated"] = datetime.now().isoformat()
        return submitted

    @staticmethod
    def _format_system_season_episode(season: Optional[int], episodes: List[int]) -> str:
        """按 MP 原生通知习惯格式化季集，例如 S01 E15 / S01 E18-E20。"""
        try:
            season_num = int(season or 1)
        except (TypeError, ValueError):
            season_num = 1
        prefix = f"S{season_num:02d}"
        cleaned = sorted({int(ep) for ep in episodes if ep is not None})
        if not cleaned:
            return prefix
        if len(cleaned) == 1:
            ep_part = f"E{cleaned[0]:02d}"
        elif cleaned == list(range(cleaned[0], cleaned[-1] + 1)):
            ep_part = f"E{cleaned[0]:02d}-E{cleaned[-1]:02d}"
        else:
            ep_part = ",".join(f"E{ep:02d}" for ep in cleaned)
        return f"{prefix} {ep_part}"

    def _collect_notice_files_from_history(
        self,
        tmdb_id: Optional[int],
        media_type_str: str,
        season: Optional[int],
        episodes: List[int],
    ) -> List[dict]:
        """从 MP 整理历史取刚确认入库的文件信息，用于补齐系统通知里的大小。"""
        if not tmdb_id or media_type_str == "电影" or not episodes:
            return []
        try:
            from app.db.transferhistory_oper import TransferHistoryOper
        except Exception as e:
            logger.debug(f"【115助手】导入整理历史失败，跳过通知文件补全: {e}")
            return []

        wanted = {int(ep) for ep in episodes if ep is not None}
        try:
            target_season = int(season or 1)
            histories = TransferHistoryOper().get_by(
                mtype=media_type_str,
                tmdbid=int(tmdb_id),
                season=f"S{target_season:02d}",
            ) or []
        except Exception as e:
            logger.debug(f"【115助手】读取整理历史失败，跳过通知文件补全: {e}")
            return []

        matched_by_episode: Dict[int, dict] = {}
        for history in histories:
            try:
                if not getattr(history, "status", False):
                    continue
                episode_text = str(getattr(history, "episodes", "") or "").upper()
                if not episode_text:
                    episode_text = str(getattr(history, "dest", "") or "").upper()

                history_eps = set()
                for start, end in re.findall(r"E?(\d{1,3})\s*-\s*E?(\d{1,3})", episode_text):
                    start_num, end_num = int(start), int(end)
                    if start_num <= end_num:
                        history_eps.update(range(start_num, end_num + 1))
                for ep in re.findall(r"E(\d{1,3})", episode_text):
                    history_eps.add(int(ep))
                hit_eps = sorted(history_eps & wanted)
                if not hit_eps:
                    continue

                data = history.to_dict() if hasattr(history, "to_dict") else {}
                fileitem = data.get("dest_fileitem") or data.get("src_fileitem") or {}
                path = data.get("dest") or data.get("src") or fileitem.get("path") or ""
                name = fileitem.get("name") or Path(path).name
                item = {
                    "name": name,
                    "path": path,
                    "size": int(fileitem.get("size", 0) or 0),
                    "episodes": hit_eps,
                    "date": str(data.get("date") or ""),
                }
                for ep in hit_eps:
                    old = matched_by_episode.get(ep)
                    if not old or item["date"] >= old.get("date", ""):
                        matched_by_episode[ep] = item
            except Exception:
                continue

        seen = set()
        result = []
        for ep in sorted(matched_by_episode):
            item = matched_by_episode[ep]
            key = item.get("path") or item.get("name") or str(ep)
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
        return result

    def _filter_plugin_notice_episodes(
        self,
        subscription: dict,
        episodes: List[int],
    ) -> Tuple[List[int], List[int]]:
        """
        只保留需要插件补发的集数。

        MP 本体整理成功会写入 TransferHistory，并已经负责发送入库通知；
        插件补通知只覆盖“媒体库已出现，但 MP 没有整理历史”的漏通知场景。
        """
        cleaned = sorted({int(ep) for ep in episodes if ep is not None})
        if not cleaned:
            return [], []

        media_type_str = subscription.get("media_type", "电视剧")
        if media_type_str == "电影":
            return cleaned, []

        history_episodes = set(
            self._get_transfer_history_episodes(
                subscription.get("tmdb_id"),
                media_type_str,
                subscription.get("season"),
            )
        )
        history_episodes.update(
            self._get_closed_loop_organized_episodes(
                subscription.get("tmdb_id"),
                media_type_str,
                subscription.get("season"),
            )
        )
        sent_map = self._load_plugin_notice_sent_map()
        sent_episodes = {
            ep
            for ep in cleaned
            if self._plugin_notice_episode_key(subscription, ep) in sent_map
        }
        if sent_episodes:
            logger.info(
                f"【115助手】{subscription.get('title')} 插件通知账本已记录，跳过重复通知: "
                f"{self._format_episode_preview(sorted(sent_episodes))}"
            )
        if not history_episodes and not sent_episodes:
            return cleaned, []

        skipped = sorted((set(cleaned) & history_episodes) | sent_episodes)
        notice = [ep for ep in cleaned if ep not in set(skipped)]
        return notice, skipped

    def _build_subscription_refresh_records(
        self,
        subscription: dict,
        episodes: List[int],
    ) -> List[dict]:
        """Build media-server refresh records from the final library paths in MP history."""
        cleaned = sorted({int(ep) for ep in episodes if ep is not None})
        notice_files = self._collect_notice_files_from_history(
            tmdb_id=subscription.get("tmdb_id"),
            media_type_str=subscription.get("media_type", "电视剧"),
            season=subscription.get("season"),
            episodes=cleaned,
        )
        if not notice_files:
            return []

        records = []
        for item in notice_files:
            target_path = item.get("path")
            if not target_path:
                continue
            records.append({
                "title": subscription.get("title"),
                "year": subscription.get("year"),
                "tmdb_id": subscription.get("tmdb_id"),
                "media_type": subscription.get("media_type", "电视剧"),
                "category": subscription.get("media_category"),
                "season": int(subscription.get("season") or 1),
                "episodes": item.get("episodes") or cleaned,
                "source_files": [item],
                "target_path": target_path,
            })
        return records

    def _refresh_subscription_mediaservers(
        self,
        subscription: dict,
        episodes: List[int],
    ) -> List[str]:
        """Refresh media servers for files just organized by the 115 helper."""
        records = self._build_subscription_refresh_records(subscription, episodes)
        if not records:
            logger.info(
                f"【115助手】{subscription.get('title')} 未找到整理历史目标路径，跳过媒体库局部刷新"
            )
            return []

        refreshed = self._refresh_cd2_watch_mediaservers(records)
        if refreshed:
            logger.info(
                f"【115助手】{subscription.get('title')} 已请求局部刷新媒体库: {', '.join(refreshed)}"
            )
        else:
            logger.info(f"【115助手】{subscription.get('title')} 未触发可用媒体库局部刷新")
        return refreshed

    def _summarize_notice_files(
        self,
        episodes: List[int],
        source_files: Optional[List[dict]] = None,
    ) -> Tuple[int, int, str]:
        """提取通知需要的文件数、总大小和用于解析质量的样例文件名。"""
        cleaned_eps = {int(ep) for ep in episodes if ep is not None}
        source_files = source_files or []
        matched = []
        for item in source_files:
            name = str(item.get("name") or Path(str(item.get("path") or "")).name)
            path = str(item.get("path") or name)
            ep_num = self._parse_episode_number(name) or self._parse_episode_number(path)
            if cleaned_eps and ep_num not in cleaned_eps:
                continue
            matched.append(item)

        if not matched:
            matched = source_files

        file_count = len(matched) if matched else max(len(cleaned_eps), 1)
        total_size = 0
        sample_name = ""
        for item in matched:
            try:
                total_size += int(item.get("size", 0) or 0)
            except (TypeError, ValueError):
                pass
            if not sample_name:
                sample_name = str(item.get("name") or Path(str(item.get("path") or "")).name)
        return file_count, total_size, sample_name

    def _build_notification_media_context(
        self,
        subscription: dict,
        episodes: List[int],
        sample_name: str = "",
    ) -> Tuple[Optional[ParseMeta], Any]:
        """识别媒体并构造给 MP 消息模板使用的 meta/mediainfo。"""
        title = str(subscription.get("title") or "").strip()
        media_type_str = subscription.get("media_type", "电视剧")
        season = subscription.get("season")
        year = subscription.get("year")
        first_ep = sorted(episodes)[0] if episodes else None
        season_ep = ""
        if media_type_str != "电影" and first_ep:
            try:
                season_ep = f"S{int(season or 1):02d}E{int(first_ep):02d}"
            except (TypeError, ValueError):
                season_ep = f"E{int(first_ep):02d}"

        candidates = []
        if sample_name:
            candidates.append(Path(sample_name).stem)
        title_candidate = " ".join(
            part for part in (title, f"({year})" if year else "", season_ep) if part
        ).strip()
        if title_candidate:
            candidates.append(title_candidate)
        if title:
            candidates.append(title)

        expected_tmdb = None
        try:
            expected_tmdb = int(subscription.get("tmdb_id") or 0) or None
        except (TypeError, ValueError):
            expected_tmdb = None

        fallback_meta = None
        fallback_media = None
        media_chain = MediaChain()
        if expected_tmdb:
            try:
                direct_title = title_candidate or title or (Path(sample_name).stem if sample_name else "")
                direct_meta = ParseMeta(direct_title or str(expected_tmdb))
                if season:
                    direct_meta.begin_season = int(season)
                if first_ep:
                    direct_meta.begin_episode = int(first_ep)
                direct_meta.type = MediaType.MOVIE if media_type_str == "电影" else MediaType.TV
                direct_media = media_chain.recognize_media(
                    meta=direct_meta,
                    mtype=direct_meta.type,
                    tmdbid=expected_tmdb,
                )
                if direct_media:
                    try:
                        media_chain.obtain_images(mediainfo=direct_media)
                    except Exception as image_err:
                        logger.debug(f"【115助手】补全入库通知图片失败: TMDB={expected_tmdb} | {image_err}")
                    return direct_meta, direct_media
            except Exception as e:
                logger.debug(f"【115助手】按 TMDB 构造入库通知媒体上下文失败: TMDB={expected_tmdb} | {e}")

        for candidate in dict.fromkeys(candidates):
            try:
                meta = ParseMeta(candidate)
                if season:
                    meta.begin_season = int(season)
                if first_ep:
                    meta.begin_episode = int(first_ep)
                meta.type = MediaType.MOVIE if media_type_str == "电影" else MediaType.TV
                mediainfo = media_chain.recognize_by_meta(meta)
                if mediainfo:
                    try:
                        media_chain.obtain_images(mediainfo=mediainfo)
                    except Exception as image_err:
                        logger.debug(f"【115助手】补全入库通知图片失败: {candidate} | {image_err}")
                if not fallback_meta:
                    fallback_meta = meta
                    fallback_media = mediainfo
                if not mediainfo:
                    continue
                actual_tmdb = getattr(mediainfo, "tmdb_id", None)
                try:
                    actual_tmdb = int(actual_tmdb) if actual_tmdb not in (None, "") else None
                except (TypeError, ValueError):
                    actual_tmdb = None
                if not expected_tmdb or actual_tmdb == expected_tmdb:
                    return meta, mediainfo
            except Exception as e:
                logger.debug(f"【115助手】构造入库通知媒体上下文失败: {candidate} | {e}")
                continue
        return fallback_meta, fallback_media

    def _post_organize_success_template_message(
        self,
        subscription: dict,
        episodes: List[int],
        source_files: Optional[List[dict]] = None,
        reason: str = "",
        source_label: str = "115入库",
        season_episode_override: Optional[str] = None,
        mark_notice: bool = True,
    ) -> bool:
        """使用 MP 原生“整理入库成功”模板发送 115 入库确认通知。"""
        cleaned = sorted({int(ep) for ep in episodes if ep is not None})
        media_type_str = subscription.get("media_type", "电视剧")
        if media_type_str != "电影" and not cleaned:
            return False

        title = subscription.get("title", "")
        tmdb_id = subscription.get("tmdb_id")
        season = subscription.get("season")

        notice_files = self._collect_notice_files_from_history(
            tmdb_id=tmdb_id,
            media_type_str=media_type_str,
            season=season,
            episodes=cleaned,
        )
        if not notice_files:
            notice_files = source_files or []

        file_count, total_size, sample_name = self._summarize_notice_files(cleaned, notice_files)
        meta, mediainfo = self._build_notification_media_context(subscription, cleaned, sample_name)
        season_episode = season_episode_override or (
            self._format_system_season_episode(season, cleaned)
            if media_type_str != "电影"
            else None
        )

        if not mediainfo:
            ep_text = self._format_episode_preview(cleaned) if cleaned else ""
            text = reason or f"{source_label}确认入库"
            if ep_text:
                text = f"{text}，新增集数：{ep_text}"
            self.post_message(
                mtype=NotificationType.Organize,
                title=(
                    f"{title} {season_episode} 已入库"
                    if season_episode
                    else f"{title} 已入库"
                ),
                text=text,
            )
            if mark_notice:
                self._mark_plugin_notice_sent(subscription, cleaned)
            return True

        transferinfo = TransferInfo(
            file_count=file_count,
            total_size=total_size,
        )
        self.chain.post_message(
            Notification(
                mtype=NotificationType.Organize,
                ctype=ContentType.OrganizeSuccess,
                image=mediainfo.get_message_image(),
                link=settings.MP_DOMAIN("#/history"),
            ),
            meta=meta,
            mediainfo=mediainfo,
            transferinfo=transferinfo,
            season_episode=season_episode,
            source=source_label,
        )
        logger.info(
            f"【115助手】{title} 已按 MP 入库模板发送通知: "
            f"{season_episode or 'MOVIE'}, files={file_count}, size={total_size}, source={source_label}"
        )
        if mark_notice:
            self._mark_plugin_notice_sent(subscription, cleaned)
        return True

    def _notify_token_expired(self, source: str, detail: str = "") -> None:
        """
        发送 Token/Cookie 过期提醒（带节流：同一来源 4 小时内最多发一次）。
        :param source: 过期来源标识，如 "插件Cookie" / "MP的115Cookie"
        :param detail: 附加说明
        """
        now = time.time()
        last = self._token_warn_times.get(source, 0)
        if now - last < self._TOKEN_WARN_INTERVAL:
            return  # 4 小时内已提醒过，静默
        self._token_warn_times[source] = now

        if source == "插件Cookie":
            title_str = "115网盘助手 ⚠️ 插件 Cookie 已过期"
            text_str = (
                "net115helper 插件的 115 Cookie 已失效，直接转存功能将无法使用。\n"
                "请在「115网盘助手」插件页面点击「扫码登录」重新授权。\n"
                + (f"错误详情：{detail}" if detail else "")
            )
        elif source == "MP的115Cookie":
            title_str = "115网盘助手 ⚠️ MP 的 115 Cookie 已过期"
            text_str = (
                "MoviePilot 主程序使用的 115 Cookie 已失效，媒体库同步将无法正常工作。\n"
                "请在「115网盘配置（net115config）」插件里重新粘贴 Cookie。\n"
                + (f"错误详情：{detail}" if detail else "")
            )
        elif source == "扫码Cookie":
            title_str = "115网盘助手 ⚠️ 扫码 Cookie 已过期"
            text_str = (
                "p115client 扫码登录保存的 Cookie 已失效，插件会尝试使用其它可用 Cookie。\n"
                "如没有其它可用 Cookie，请在「115网盘助手」详情页重新扫码登录。\n"
                + (f"错误详情：{detail}" if detail else "")
            )
        else:
            title_str = f"115网盘助手 ⚠️ {source} 已过期"
            text_str = detail or "请重新登录或更新 Cookie。"

        logger.warning(f"【115助手】{title_str} — {detail}")
        if self._notify:
            self.post_message(
                mtype=NotificationType.MediaServer,
                title=title_str,
                text=text_str,
            )

    def _detect_total_episodes(self, search_results: List[dict], title: str) -> Optional[int]:
        """从搜索结果的描述中检测总集数"""
        for result in search_results[:10]:
            note = result.get("note", "")
            m = re.search(r"(?:全|共)\s*(\d+)\s*集", note)
            if m:
                return int(m.group(1))
            if "完结" in note:
                m2 = re.search(r"(\d+)\s*集", note)
                if m2:
                    return int(m2.group(1))
        return None

    def _rank_episode_results_for_missing(
        self,
        results: List[dict],
        subscription: dict,
        existing_episodes: Optional[List[int]] = None,
        target_missing_episodes: Optional[List[int]] = None,
        log_context: str = "",
    ) -> List[dict]:
        """
        按“标题是否明确覆盖缺失集”排序搜索结果。

        标题提示只用于决定优先级和是否值得打开分享；最终仍以后续真实文件列表
        解析为准，避免只靠标题误转。
        """
        if not results:
            return []

        if subscription.get("media_type", "电视剧") == "电影":
            return [
                {
                    "result": result,
                    "score": 0,
                    "hints": [],
                    "missing_hits": [],
                    "unknown": True,
                }
                for result in results
            ]

        existing_set = set()
        for ep in existing_episodes or []:
            try:
                existing_set.add(int(ep))
            except (TypeError, ValueError):
                continue

        missing_set = set()
        for ep in target_missing_episodes or []:
            try:
                missing_set.add(int(ep))
            except (TypeError, ValueError):
                continue
        if not missing_set:
            try:
                total_eps = int(subscription.get("_cache_total_episodes") or 0)
            except (TypeError, ValueError):
                total_eps = 0
            if total_eps > 0:
                missing_set = set(range(1, total_eps + 1)) - existing_set

        ranked = []
        skipped_existing_only = 0
        skipped_off_topic = 0
        total_eps_for_hints = subscription.get("_cache_total_episodes")
        target_season = subscription.get("season")

        for index, result in enumerate(results):
            if self._result_definitely_off_topic(result, subscription):
                skipped_off_topic += 1
                continue

            text = " ".join(
                str(result.get(key, "") or "")
                for key in ("note", "name", "title", "url")
            )
            hints = self._extract_episode_hints_from_text(
                text,
                total_episodes=total_eps_for_hints,
                target_season=target_season,
            )
            hint_set = set(hints)
            if missing_set:
                missing_hits = sorted(hint_set & missing_set)
            else:
                missing_hits = sorted(hint_set - existing_set)

            # 明确只覆盖已有集数的标题不值得打开；未知标题保留低优先级兜底。
            if hint_set and not missing_hits:
                skipped_existing_only += 1
                continue

            if missing_hits:
                score = len(missing_hits) * 1000
                if missing_set and missing_set.issubset(hint_set):
                    score += 500
                if missing_set and min(missing_set) in missing_hits:
                    score += 250
                # 同样命中缺失时，优先范围更窄的候选，例如 E18-E20 优于全 40 集。
                score -= min(len(hint_set), 120)
            else:
                score = -100

            ranked.append({
                "result": result,
                "score": score,
                "hints": sorted(hint_set),
                "missing_hits": missing_hits,
                "unknown": not bool(hint_set),
                "original_index": index,
            })

        ranked.sort(
            key=lambda item: (
                item["score"],
                len(item["missing_hits"]),
                -len(item["hints"]) if item["hints"] else -999,
                -item["original_index"],
            ),
            reverse=True,
        )

        if log_context:
            if ranked:
                preview = []
                for item in ranked[:5]:
                    note = str(item["result"].get("note", item["result"].get("url", "")))[:40]
                    hits = self._format_episode_preview(item["missing_hits"])
                    hints = self._format_episode_preview(item["hints"])
                    label = f"hit={hits}" if item["missing_hits"] else "unknown"
                    preview.append(f"{label}, hints={hints}, note={note}")
                logger.info(
                    f"【115助手】{log_context} 精准候选排序: "
                    f"{' | '.join(preview)}"
                    + (
                        f"，跳过仅含已有集数 {skipped_existing_only} 个"
                        if skipped_existing_only else ""
                    )
                    + (
                        f"，排除疑似无关结果 {skipped_off_topic} 个"
                        if skipped_off_topic else ""
                    )
                )
            elif skipped_existing_only or skipped_off_topic:
                logger.info(
                    f"【115助手】{log_context} 搜索结果均不适合打开，"
                    f"跳过已有集数 {skipped_existing_only} 个，"
                    f"排除无关结果 {skipped_off_topic} 个"
                )

        return ranked

    _KEYCAP_DIGIT_PATTERN = re.compile(r"([0-9])\ufe0f?\u20e3")
    _TITLE_DIGIT_TO_CN = str.maketrans({
        "0": "零",
        "1": "一",
        "2": "二",
        "3": "三",
        "4": "四",
        "5": "五",
        "6": "六",
        "7": "七",
        "8": "八",
        "9": "九",
    })

    @classmethod
    def _normalize_confusable_title_text(cls, text: str) -> str:
        """归一化标题匹配里的混淆数字，如 老9️⃣门 -> 老九门。"""
        normalized = unicodedata.normalize("NFKC", str(text or ""))
        normalized = cls._KEYCAP_DIGIT_PATTERN.sub(r"\1", normalized)
        normalized = normalized.replace("\ufe0f", "").replace("\u20e3", "")
        normalized = normalized.translate(cls._TITLE_DIGIT_TO_CN)
        return normalized

    @staticmethod
    def _short_title_embedded_in_other_chinese_text(compact: str, title_compact: str) -> bool:
        """短中文标题被其它中文标题包住时判为串剧，如「九门」命中「老九门」。"""
        if not compact or not title_compact:
            return False
        chinese_only_title = re.sub(r"[^\u4e00-\u9fff]", "", title_compact)
        if not (0 < len(chinese_only_title) <= 3):
            return False

        allowed_next_context = set("第更更新至全共完已连集话話季")
        for match in re.finditer(re.escape(title_compact), compact):
            start, end = match.span()
            prev_char = compact[start - 1] if start > 0 else ""
            next_char = compact[end] if end < len(compact) else ""
            if prev_char and re.match(r"[\u4e00-\u9fff]", prev_char):
                return True
            if (
                next_char
                and re.match(r"[\u4e00-\u9fff]", next_char)
                and next_char not in allowed_next_context
            ):
                return True
        return False

    @classmethod
    def _subscription_title_match_state(cls, text: str, subscription: dict) -> Optional[bool]:
        """
        判断文本里的标题命中是否可信。

        返回值：
        - True：明确命中订阅标题
        - False：标题只是短词子串，疑似命中别的剧名
        - None：文本未出现订阅标题，调用方按上下文决定是否放行
        """
        title = str(subscription.get("title") or "").strip()
        if not title:
            return True

        compact = re.sub(r"\s+", "", cls._normalize_confusable_title_text(text))
        title_compact = re.sub(r"\s+", "", cls._normalize_confusable_title_text(title))
        if not title_compact:
            return True
        if title_compact not in compact:
            return None

        chinese_only_title = re.sub(r"[^\u4e00-\u9fff]", "", title_compact)
        if 0 < len(chinese_only_title) <= 3:
            if cls._short_title_embedded_in_other_chinese_text(compact, title_compact):
                return False

            strong_contexts = (
                f"《{title_compact}》",
                f"{title_compact}(",
                f"{title_compact}（",
                f"{title_compact}[",
                f"{title_compact}【",
                f"{title_compact}/",
                f"/{title_compact}",
                f"🗄{title_compact}",
                f"剧集🗄{title_compact}",
            )
            if any(token in compact for token in strong_contexts):
                return True

            # 短标题必须是独立词边界，避免「九门」命中「老九门」。
            boundary = rf"(?<![\u4e00-\u9fff]){re.escape(title_compact)}(?![\u4e00-\u9fff])"
            if re.search(boundary, compact):
                return True

            episode_context = re.search(
                rf"(?<![\u4e00-\u9fff]){re.escape(title_compact)}"
                rf"(?:[sS]\d{{1,2}}[eE]?\d{{0,3}}|[eE]\d{{1,3}}|"
                rf"第?\d{{1,3}}[集话話]|(?:更新至|更新到|更新|更至|更)\d{{1,3}}[集话話]|"
                rf"(?:更新至|更新到|更至|更)\d{{1,3}}(?!\d)|"
                rf"全\d{{1,3}}[集话話]|全集|完结|已完结)",
                compact,
            )
            return bool(episode_context)

        return True

    @classmethod
    def _aggregated_post_link_mismatch(cls, result: dict, subscription: dict) -> bool:
        """
        聚合帖错剧防护。

        频道聚合帖会在一条结果里列多部剧、每部各带一组网盘链接，例如
        「#仙逆 … 链接组 / 电视剧：一瓯春 … 链接组 / 电视剧：兰香如故 … 链接组」。
        这种结果只要本剧名出现在帖子任意位置，标题匹配就会通过，但 PanSou 给出的
        url 可能属于帖子里另一部剧——直接转存就会把别的剧当作本剧后续集入库
        （2026-09-21 一瓯春 E11-E22 实际是兰香如故的片源）。

        返回 True 表示「这是聚合帖，且 url 不属于本剧那一段」，应当丢弃该结果。
        """
        url = str(result.get("url") or "").strip()
        text = " ".join(
            str(result.get(key, "") or "")
            for key in ("note", "name", "title")
        )
        if not url or not text:
            return False

        # 条目分隔标记：`电视剧：`、`综艺:`、`#剧名` 等
        markers = [m.start() for m in re.finditer(
            r"(?:电视剧|电影|综艺|动漫|动画|纪录片|短剧)\s*[:：]|#", text)]
        if len(markers) < 2:
            return False  # 不是聚合帖，交给其它校验

        bounds = sorted(set(markers + [0, len(text)]))
        segments = [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]

        url_seg = None
        pos = text.find(url)
        if pos < 0:
            # PanSou 可能对 url 做过清洗，退化为用分享 ID 定位
            m = re.search(r"/s/([A-Za-z0-9_-]{6,})", url)
            if m:
                pos = text.find(m.group(1))
        if pos < 0:
            return False
        for s, e in segments:
            if s <= pos < e:
                url_seg = (s, e)
                break
        if not url_seg:
            return False

        # url 所在段里必须能匹配到本剧标题
        seg_text = re.sub(r"\s+", "", text[url_seg[0]:url_seg[1]])
        if cls._subscription_title_match_state(seg_text, subscription) is True:
            return False

        logger.info(
            f"【115助手】聚合帖结果的链接不属于 {subscription.get('title')} 所在条目，丢弃: "
            f"seg={seg_text[:48]}"
        )
        return True

    @classmethod
    def _result_definitely_off_topic(cls, result: dict, subscription: dict) -> bool:
        """过滤标题党、在线观影页、课程等明显不是目标剧资源的搜索结果。"""
        title = str(subscription.get("title") or "").strip()
        if not title:
            return False

        text = " ".join(
            str(result.get(key, "") or "")
            for key in ("note", "name", "title", "url")
        )
        compact = re.sub(r"\s+", "", text)
        title_match_state = cls._subscription_title_match_state(compact, subscription)
        if title_match_state is None or title_match_state is False:
            return True

        if cls._aggregated_post_link_mismatch(result, subscription):
            return True

        # 这些通常是在线播放/课程营销页，不是可转存的网盘剧集文件。
        noise_tokens = (
            "在线观看", "免费在线观看", "无广告在线", "超清免费在线观看",
            "vs影视", "口语", "英语", "课程", "亲授", "老师", "电台",
            "社交", "旅行", "达人", "教学理念",
        )
        compact_lower = compact.lower()
        if any(token in compact_lower for token in noise_tokens):
            return True

        return False

    @staticmethod
    def _add_episode_span(episodes: set, start: int, end: Optional[int] = None,
                          max_episode: Optional[int] = None):
        """把可信的集数范围加入集合，避免异常标题生成超大范围。"""
        if start <= 0:
            return
        end = end or start
        if end < start:
            start, end = end, start
        cap = max_episode or 300
        if start > cap:
            return
        end = min(end, cap)
        if end - start > 200:
            return
        episodes.update(range(start, end + 1))

    @staticmethod
    def _format_episode_preview(episodes: List[int], limit: int = 12) -> str:
        """日志里压缩显示集数，避免刷屏。"""
        if not episodes:
            return "[]"
        preview = episodes[:limit]
        suffix = "" if len(episodes) <= limit else f"...(+{len(episodes) - limit})"
        return f"{preview}{suffix}"

    @classmethod
    def _extract_episode_hints_from_text(
            cls,
            text: str,
            total_episodes: Optional[int] = None,
            target_season: Optional[int] = None) -> List[int]:
        """
        从搜索结果标题/说明里提取“可信集数提示”。

        只用于打开分享前的降噪：返回非空时代表标题已经明确指向这些集数；
        返回空则保守继续打开分享，避免错过全集包或命名不标准资源。
        """
        raw = str(text or "")
        if not raw:
            return []

        episodes = set()
        max_episode = total_episodes if total_episodes and total_episodes > 0 else 300
        target_season_int = None
        try:
            target_season_int = int(target_season) if target_season not in (None, "") else None
        except (TypeError, ValueError):
            target_season_int = None

        # S01E21-E42 / S01.E21-42 / S01 E22
        se_pattern = re.compile(
            r'[Ss](\d{1,2})\s*[\.\-_\s]?\s*[Ee]\s*0?(\d{1,3})'
            r'(?:\s*[-~到至]\s*(?:[Ee]\s*)?0?(\d{1,3}))?',
            re.IGNORECASE,
        )
        for season_s, start_s, end_s in se_pattern.findall(raw):
            try:
                season_num = int(season_s)
                if target_season_int is not None and season_num != target_season_int:
                    continue
                cls._add_episode_span(
                    episodes,
                    int(start_s),
                    int(end_s) if end_s else None,
                    max_episode=max_episode,
                )
            except (TypeError, ValueError):
                continue

        # 第38集 / 第38-42集 / 38-42集 / 1-7集
        cn_range_pattern = re.compile(
            r'(?:第\s*)?0?(\d{1,3})\s*[-~到至]\s*0?(\d{1,3})\s*[集话話]'
        )
        for start_s, end_s in cn_range_pattern.findall(raw):
            try:
                cls._add_episode_span(
                    episodes,
                    int(start_s),
                    int(end_s),
                    max_episode=max_episode,
                )
            except (TypeError, ValueError):
                continue

        cn_single_pattern = re.compile(r'第\s*0?(\d{1,3})\s*[集话話]')
        for ep_s in cn_single_pattern.findall(raw):
            try:
                cls._add_episode_span(episodes, int(ep_s), max_episode=max_episode)
            except (TypeError, ValueError):
                continue

        # E22 / EP22。只认带 E/EP 前缀的裸集号，避免把 1080p/4K 当成集数。
        bare_ep_pattern = re.compile(r'(?<![A-Za-z0-9])[Ee][Pp]?\s*0?(\d{1,3})(?!\d)')
        for ep_s in bare_ep_pattern.findall(raw):
            try:
                cls._add_episode_span(episodes, int(ep_s), max_episode=max_episode)
            except (TypeError, ValueError):
                continue

        # 更新至44集 / 更新到44集 / 更至44集 / 至44集：可信表达为 1..44。
        for ep_s in re.findall(r'(?:更新至|更新到|更新|更至|更|连载至|连载|至)\s*0?(\d{1,3})\s*[集话話]', raw):
            try:
                cls._add_episode_span(episodes, 1, int(ep_s), max_episode=max_episode)
            except (TypeError, ValueError):
                continue

        # 更08 / 更至08：部分网盘标题会省略“集”，仍表示更新到 1..08。
        for ep_s in re.findall(r'(?:更新至|更新到|更至|更)\s*0?(\d{1,3})(?!\d)(?!\s*[pPkKgG])', raw):
            try:
                cls._add_episode_span(episodes, 1, int(ep_s), max_episode=max_episode)
            except (TypeError, ValueError):
                continue

        lower = raw.lower()

        # 全42集 / 完结42集：可信表达为 1..42。
        for ep_s in re.findall(r'(?:全|完结)\s*0?(\d{1,3})\s*[集话話]', raw):
            try:
                cls._add_episode_span(episodes, 1, int(ep_s), max_episode=max_episode)
            except (TypeError, ValueError):
                continue
        # “共30集”常见于总集数说明，不能当作当前资源已包含全集。
        # 只有明确完结语境下才把它当成全集提示。
        if any(token in raw for token in ("全集", "全季", "完结", "已完结")) or "complete" in lower:
            for ep_s in re.findall(r'共\s*0?(\d{1,3})\s*[集话話]', raw):
                try:
                    cls._add_episode_span(episodes, 1, int(ep_s), max_episode=max_episode)
                except (TypeError, ValueError):
                    continue

        # 全集/完结/Complete 但没有数字时，只有 TMDB 总集数已知才用于判断。
        if total_episodes and (
                "全集" in raw or "全季" in raw or "完结" in raw
                or "complete" in lower or "season complete" in lower):
            cls._add_episode_span(episodes, 1, int(total_episodes), max_episode=max_episode)

        return sorted(episodes)

    _CN_SEASON_NUMBERS = {
        "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
        "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
    }

    @classmethod
    def _extract_season_markers(cls, text: str) -> List[int]:
        """从标题/路径中提取季数标记，用于避免降级转存错季资源。"""
        if not text:
            return []
        seasons = set()
        raw = str(text)
        lower = raw.lower()

        if any(word in raw for word in ("特别篇", "特辑", "番外")) or "special" in lower:
            seasons.add(0)

        for start, end in re.findall(r'(?<!\d)(\d{1,2})\s*[-~到至]\s*(\d{1,2})\s*季', raw):
            a, b = int(start), int(end)
            if 0 <= a <= b <= 99:
                seasons.update(range(a, b + 1))

        patterns = [
            r'(?<![A-Za-z0-9])[Ss](\d{1,2})(?=[Ee\W_]|$)',
            r'season\s*(\d{1,2})',
            r'第\s*(\d{1,2})\s*季',
            r'(?<!\d)(\d{1,2})\s*季',
        ]
        for pattern in patterns:
            for match in re.findall(pattern, raw, flags=re.IGNORECASE):
                try:
                    seasons.add(int(match))
                except (TypeError, ValueError):
                    continue

        for match in re.findall(r'第?\s*([一二三四五六七八九十])\s*季', raw):
            seasons.add(cls._CN_SEASON_NUMBERS.get(match, 0))

        return sorted(season for season in seasons if season is not None)

    @staticmethod
    def _split_path_segments(text: str) -> List[str]:
        """把云盘路径拆成可单独判断的片段，避免父级目标剧名掩盖内层串剧目录。"""
        return [
            segment.strip()
            for segment in re.split(r"[\\/]+", str(text or ""))
            if segment and segment.strip()
        ]

    @staticmethod
    def _subscription_expected_year(subscription: dict, expected_year: Optional[int] = None) -> Optional[int]:
        if expected_year:
            try:
                return int(expected_year)
            except (TypeError, ValueError):
                return None
        try:
            year = subscription.get("year")
            return int(year) if year not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _fallback_item_reject_reason(
        self,
        item: dict,
        subscription: dict,
        expected_year: Optional[int] = None,
    ) -> str:
        """返回降级复制前的负向证据；空字符串表示暂未发现明显串剧/错年。"""
        expected = self._subscription_expected_year(subscription, expected_year)
        segments = []
        name_text = str(item.get("name", "") or "")
        path_text = str(item.get("path", "") or "")
        if name_text:
            segments.append(("name", name_text))
        for segment in self._split_path_segments(path_text):
            segments.append(("path", segment))

        for label, segment in segments:
            title_state = self._subscription_title_match_state(segment, subscription)
            if title_state is False:
                return f"疑似串剧片段({label}): {segment[:80]}"

            if expected:
                segment_year = self._extract_year(segment)
                if segment_year and segment_year != expected:
                    # 降级转存的源路径里出现其它年份，通常代表内层实际资源不是目标剧。
                    # 如“九门 更08 [2026]”分享内含“老9️⃣门（2016）”。
                    if title_state is not None or re.search(r"[\u4e00-\u9fff]|[A-Za-z]{3,}", segment):
                        return f"疑似错年片段({label}): {segment[:80]}，expected={expected}"

        return ""

    def _fallback_result_matches_subscription(self, result: dict, subscription: dict) -> bool:
        """降级转存前校验搜索结果标题和季数，避免把相近短标题/错季资源当成目标。"""
        media_type = subscription.get("media_type", "电视剧")
        if media_type == "电影":
            return True
        target_season = int(subscription.get("season") or 1)
        text = " ".join(
            str(result.get(key, "") or "")
            for key in ("note", "name", "title", "url")
        )
        year_text = " ".join(
            str(result.get(key, "") or "")
            for key in ("note", "name", "title")
        )
        if self._aggregated_post_link_mismatch(result, subscription):
            return False
        title_match_state = self._subscription_title_match_state(text, subscription)
        if title_match_state is False or title_match_state is None:
            logger.info(
                f"【115助手】[降级] 跳过疑似非目标剧资源: "
                f"title={subscription.get('title', '')}, note={str(result.get('note', ''))[:80]}"
            )
            return False
        seasons = self._extract_season_markers(text)
        if not seasons:
            expected_year = self._subscription_expected_year(subscription)
            if expected_year:
                years = {
                    int(match.group(0))
                    for match in re.finditer(r'\b(?:19|20)\d{2}\b', year_text)
                }
                if years and expected_year not in years:
                    logger.info(
                        f"【115助手】[降级] 跳过非目标年份资源: target={expected_year}, "
                        f"years={sorted(years)}, note={str(result.get('note', ''))[:80]}"
                    )
                    return False
            return True
        if target_season in seasons:
            expected_year = self._subscription_expected_year(subscription)
            if expected_year:
                years = {
                    int(match.group(0))
                    for match in re.finditer(r'\b(?:19|20)\d{2}\b', year_text)
                }
                if years and expected_year not in years:
                    logger.info(
                        f"【115助手】[降级] 跳过非目标年份资源: target={expected_year}, "
                        f"years={sorted(years)}, note={str(result.get('note', ''))[:80]}"
                    )
                    return False
            return True
        logger.info(
            f"【115助手】[降级] 跳过非目标季资源: target=S{target_season:02d}, "
            f"seasons={seasons}, note={str(result.get('note', ''))[:80]}"
        )
        return False

    def _fallback_item_matches_subscription(
        self,
        item: dict,
        subscription: dict,
        expected_year: Optional[int] = None,
    ) -> bool:
        """复制前按文件/目录路径再次校验标题和季数。"""
        media_type = subscription.get("media_type", "电视剧")
        if media_type == "电影":
            return True
        target_season = int(subscription.get("season") or 1)
        name_text = str(item.get("name", "") or "")
        path_text = str(item.get("path", "") or "")

        reject_reason = self._fallback_item_reject_reason(item, subscription, expected_year)
        if reject_reason:
            logger.info(
                f"【115助手】[降级] 跳过疑似非目标文件: "
                f"title={subscription.get('title', '')}, {reject_reason}, "
                f"path={str(item.get('path', item.get('name', '')))[:120]}"
            )
            return False

        # 先看文件/目录自身名称。路径里经常包含插件创建的目标暂存目录
        # （如 /MP临时转存/九门），不能让它覆盖「老九门」这种文件名反证。
        name_match_state = self._subscription_title_match_state(name_text, subscription)
        if name_match_state is False:
            logger.info(
                f"【115助手】[降级] 跳过疑似串剧文件: "
                f"title={subscription.get('title', '')}, path={str(item.get('path', item.get('name', '')))[:120]}"
            )
            return False
        path_match_state = self._subscription_title_match_state(path_text, subscription)
        if name_match_state is None and path_match_state is False:
            logger.info(
                f"【115助手】[降级] 跳过疑似串剧路径: "
                f"title={subscription.get('title', '')}, path={str(item.get('path', item.get('name', '')))[:120]}"
            )
            return False

        text = " ".join([path_text, name_text])
        seasons = self._extract_season_markers(text)
        if not seasons:
            return True
        if target_season in seasons:
            return True
        logger.info(
            f"【115助手】[降级] 跳过错季文件: target=S{target_season:02d}, "
            f"seasons={seasons}, path={str(item.get('path', item.get('name', '')))[:120]}"
        )
        return False

    def _existing_episodes_cache_key(self, title: str, tmdb_id: Optional[int],
                                     media_type_str: str, season: Optional[int]) -> str:
        return "|".join([
            str(tmdb_id or ""),
            str(media_type_str or ""),
            str(season or 1),
            str(title or "").strip(),
        ])

    def _get_existing_episodes_cache(self, cache_key: str) -> Optional[List[int]]:
        item = self._existing_episodes_cache.get(cache_key)
        if not item:
            return None
        expire_at, episodes = item
        if expire_at <= time.time():
            self._existing_episodes_cache.pop(cache_key, None)
            return None
        return list(episodes or [])

    def _set_existing_episodes_cache(self, cache_key: str, episodes: List[int]) -> None:
        self._existing_episodes_cache[cache_key] = (
            time.time() + self._EXISTING_EPISODES_CACHE_TTL,
            list(episodes or []),
        )

    @staticmethod
    def _normalize_existing_episode_hint(episodes: Optional[List[int]], media_type_str: str) -> List[int]:
        if not episodes:
            return []
        normalized = set()
        for ep in episodes:
            try:
                value = int(ep)
            except (TypeError, ValueError):
                continue
            if media_type_str == "电影" and value == -1:
                return [-1]
            if value > 0:
                normalized.add(value)
        return sorted(normalized)

    def _get_mediaserver_existing_episodes(
        self,
        tmdb_id: Optional[int],
        media_type_str: str,
        season: Optional[int],
        total_episodes: Optional[int] = None,
    ) -> List[int]:
        """
        从 MP 的 mediaserveritem 索引读取 Plex/Emby 已入库状态。
        默认巡检优先依赖这里，避免频繁读取 115 正式库目录。
        """
        if not tmdb_id:
            return []

        db_paths = [
            settings.CONFIG_PATH / "user.db",
            settings.CONFIG_PATH / "moviepilot.db",
            settings.CONFIG_PATH / "app.db",
        ]
        target_season = str(int(season or 1))
        rows = []

        for db_path in db_paths:
            try:
                if not db_path.exists():
                    continue
                with sqlite3.connect(str(db_path), timeout=2) as conn:
                    conn.row_factory = sqlite3.Row
                    query = (
                        "SELECT server, seasoninfo FROM mediaserveritem "
                        "WHERE tmdbid = ? AND item_type = ? AND lower(coalesce(server, '')) IN ('plex', 'emby')"
                    )
                    rows = conn.execute(query, (int(tmdb_id), media_type_str)).fetchall()
                    if rows:
                        break
            except Exception as e:
                logger.debug(f"【115助手】读取 mediaserveritem 失败 {db_path}: {e}")

        if not rows:
            return []

        if media_type_str == "电影":
            logger.info(f"【115助手】电影 TMDB={tmdb_id} 已在 Plex/Emby 媒体索引中")
            return [-1]

        server_episode_sets: Dict[str, set] = {}
        skipped_stale_rows = False
        for row in rows:
            server = str(row["server"] if isinstance(row, sqlite3.Row) else "" or "").strip().lower()
            if not server:
                server = "unknown"
            seasoninfo = row["seasoninfo"] if isinstance(row, sqlite3.Row) else None
            if not seasoninfo:
                continue
            try:
                if isinstance(seasoninfo, str):
                    seasoninfo = json.loads(seasoninfo)
            except Exception:
                continue
            if not isinstance(seasoninfo, dict):
                continue
            row_episodes = set()
            for ep in seasoninfo.get(target_season, []) or []:
                try:
                    ep_num = int(ep)
                except (TypeError, ValueError):
                    continue
                if ep_num > 0:
                    row_episodes.add(ep_num)
            if (
                total_episodes
                and row_episodes
                and max(row_episodes) > int(total_episodes)
            ):
                logger.warning(
                    f"【115助手】TMDB={tmdb_id} S{int(season or 1):02d} "
                    f"{server} 媒体索引疑似旧缓存，最大集数 {max(row_episodes)} "
                    f"超过 TMDB 总集数 {total_episodes}，本行跳过"
                )
                skipped_stale_rows = True
                continue
            if row_episodes:
                server_episode_sets.setdefault(server, set()).update(row_episodes)

        if not server_episode_sets:
            return []

        if skipped_stale_rows and len(server_episode_sets) == 1:
            logger.warning(
                f"【115助手】TMDB={tmdb_id} S{int(season or 1):02d} "
                "媒体索引已有旧缓存行，剩余单一媒体服务器结果不再单独采信"
            )
            return []

        if len(server_episode_sets) == 1:
            result = sorted(next(iter(server_episode_sets.values())))
        else:
            episode_votes: Dict[int, int] = {}
            for episodes in server_episode_sets.values():
                for ep in episodes:
                    episode_votes[ep] = episode_votes.get(ep, 0) + 1
            result = sorted(ep for ep, votes in episode_votes.items() if votes >= 2)
            if not result:
                result = sorted(min(server_episode_sets.values(), key=len))
            union_result = sorted(set().union(*server_episode_sets.values()))
            if result != union_result:
                logger.warning(
                    f"【115助手】TMDB={tmdb_id} S{int(season or 1):02d} "
                    f"Plex/Emby 集数冲突，采用保守结果: "
                    f"sources={{{', '.join(f'{k}:{sorted(v)}' for k, v in server_episode_sets.items())}}} "
                    f"-> {result}"
                )
        if result:
            logger.info(f"【115助手】TMDB={tmdb_id} S{int(season or 1):02d} 已在 Plex/Emby 索引中: {result}")
        return result

    def _get_library_existing_episodes(
        self,
        title: str,
        tmdb_id: Optional[int],
        media_type_str: str,
        season: Optional[int],
    ) -> List[int]:
        """
        显式强刷时才兜底读取媒体库文件系统（可能命中 115 正式库）。
        """
        self._apply_tmdb_settings()
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
        if not existsinfo:
            return []

        if media_type_str == "电影":
            return [-1]

        if not existsinfo.seasons:
            return []

        target_season = season or 1
        return sorted({
            int(ep)
            for ep in (existsinfo.seasons.get(target_season, []) or [])
            if ep is not None
        })

    @classmethod
    def _resolve_115_local_mounts(cls, configured: str = "") -> List[Path]:
        """
        解析 115 在本容器内的挂载根目录。

        优先用插件配置里显式填写的路径；未配置时按通用 CloudDrive 挂载约定探测，
        避免把具体部署环境的路径写死在代码里。
        """
        explicit = str(configured or "").strip()
        if explicit:
            return [Path(p.strip()) for p in explicit.split(",") if p.strip()]
        found = []
        for pattern in cls._CD2_LOCAL_MOUNT_GLOBS:
            try:
                for hit in sorted(glob.glob(pattern)):
                    p = Path(hit)
                    if p.is_dir() and p not in found:
                        found.append(p)
            except Exception:
                continue
        return found

    def _cd2_115_path_to_local_candidates(self, path: str) -> List[Path]:
        """把 /115/... 形式的 CD2 路径映射到本容器内的 115 挂载路径。"""
        normalized = str(path or "").strip().rstrip("/")
        if not normalized:
            return []
        bases = self._resolve_115_local_mounts(self._cd2_local_mount_115)
        for base in bases:
            base_str = str(base).rstrip("/")
            if normalized == base_str or normalized.startswith(base_str + "/"):
                return [Path(normalized)]
        if normalized == "/115":
            rel = ""
        elif normalized.startswith("/115/"):
            rel = normalized[len("/115/"):]
        else:
            return []
        return [(base / rel) if rel else base for base in bases]

    def _get_direct_target_existing_episodes(
        self,
        title: str,
        media_type_str: str,
        season: Optional[int],
    ) -> List[int]:
        """只扫描当前剧所在的正式库目录，兜底修正 Plex/Emby 旧缓存。"""
        if media_type_str == "电影":
            return []

        roots = []
        for configured in (
            self._closed_loop_ongoing_library_path,
            self._closed_loop_archive_library_path,
        ):
            cd2_root = self._to_cd2_115_path(configured).rstrip("/")
            roots.extend(self._cd2_115_path_to_local_candidates(cd2_root))

        safe_title = self._safe_path_name(title)
        subscription = {"title": title, "media_type": media_type_str, "season": season}
        target_season = int(season or 1)
        season_dir_names = {
            f"Season {target_season}",
            f"Season {target_season:02d}",
            f"S{target_season:02d}",
            f"第{target_season}季",
        }
        episodes = set()
        for root in roots:
            if not root.exists() or not root.is_dir():
                continue
            try:
                children = list(root.iterdir())
            except Exception as e:
                logger.debug(f"【115助手】读取正式库目录失败 {root}: {e}")
                continue
            for show_dir in children:
                if not show_dir.is_dir():
                    continue
                show_name = show_dir.name
                if self._subscription_title_match_state(show_name, subscription) is not True:
                    continue
                if safe_title not in show_name and title not in show_name:
                    continue
                scan_dirs = [show_dir]
                for child in show_dir.iterdir():
                    if child.is_dir() and child.name in season_dir_names:
                        scan_dirs.append(child)
                for scan_dir in scan_dirs:
                    try:
                        file_iter = scan_dir.iterdir()
                    except Exception:
                        continue
                    for item in file_iter:
                        if not item.is_file():
                            continue
                        if item.suffix.lower() not in self._video_extensions:
                            continue
                        ep_num = self._parse_episode_number(item.name)
                        if ep_num and ep_num > 0:
                            episodes.add(int(ep_num))

        result = sorted(episodes)
        if result:
            logger.info(f"【115助手】{title} 正式库单剧目录兜底集数: {result}")
        return result

    def _get_existing_episodes(self, title: str, tmdb_id: Optional[int],
                               media_type_str: str, season: Optional[int],
                               force_refresh: bool = False,
                               cached_episodes: Optional[List[int]] = None,
                               total_episodes: Optional[int] = None) -> List[int]:
        """
        获取媒体库中已有的集数。
        电影：存在则返回 [-1]（哨兵值），不存在返回 []。
        电视剧：返回已有的集数列表。
        默认只依赖插件缓存、MP 整理历史、Plex/Emby 媒体索引；
        仅在 force_refresh=True 时，才兜底读取媒体库文件系统。
        """
        cache_key = self._existing_episodes_cache_key(title, tmdb_id, media_type_str, season)
        if not force_refresh:
            cached = self._get_existing_episodes_cache(cache_key)
            if cached is not None:
                logger.debug(f"【115助手】{title} 已入库集数缓存命中: {cached}")
                return cached

        try:
            cached_hint = self._normalize_existing_episode_hint(cached_episodes, media_type_str)
            history_episodes = self._get_transfer_history_episodes(tmdb_id, media_type_str, season)
            closed_loop_episodes = self._get_closed_loop_organized_episodes(
                tmdb_id,
                media_type_str,
                season,
            )
            mediaserver_episodes = self._get_mediaserver_existing_episodes(
                tmdb_id=tmdb_id,
                media_type_str=media_type_str,
                season=season,
                total_episodes=total_episodes,
            )
            episode_limit = 0
            try:
                episode_limit = int(total_episodes or 0)
            except (TypeError, ValueError):
                episode_limit = 0

            def _drop_suspicious_over_total(label: str, episodes: List[int]) -> List[int]:
                if media_type_str == "电影" or not episode_limit or not episodes:
                    return episodes
                cleaned = []
                for ep in episodes:
                    try:
                        cleaned.append(int(ep))
                    except (TypeError, ValueError):
                        continue
                if cleaned and max(cleaned) > episode_limit:
                    logger.warning(
                        f"【115助手】{title} {label} 集数疑似污染，最大集数 {max(cleaned)} "
                        f"超过 TMDB 总集数 {episode_limit}，本来源本轮不采信"
                    )
                    return []
                return sorted(set(ep for ep in cleaned if ep > 0))

            cached_hint = _drop_suspicious_over_total("缓存", cached_hint)
            history_episodes = _drop_suspicious_over_total("整理历史", history_episodes)
            closed_loop_episodes = _drop_suspicious_over_total("插件闭环历史", closed_loop_episodes)
            mediaserver_episodes = _drop_suspicious_over_total("Plex/Emby索引", mediaserver_episodes)

            if media_type_str == "电影":
                if (
                    -1 in cached_hint
                    or -1 in history_episodes
                    or -1 in closed_loop_episodes
                    or -1 in mediaserver_episodes
                ):
                    result = [-1]
                    self._set_existing_episodes_cache(cache_key, result)
                    return result
                if not force_refresh:
                    self._set_existing_episodes_cache(cache_key, [])
                    return []
                library_episodes = self._get_library_existing_episodes(
                    title=title,
                    tmdb_id=tmdb_id,
                    media_type_str=media_type_str,
                    season=season,
                )
                result = [-1] if -1 in library_episodes else []
                self._set_existing_episodes_cache(cache_key, result)
                return result

            merged_episodes = sorted(
                set(cached_hint)
                | set(history_episodes)
                | set(closed_loop_episodes)
                | set(mediaserver_episodes)
            )
            if history_episodes or closed_loop_episodes or mediaserver_episodes or cached_hint:
                logger.info(
                    f"【115助手】{title} 合并缓存/整理历史/插件闭环/Plex-Emby 集数: "
                    f"cache={cached_hint}, history={history_episodes}, "
                    f"closed_loop={closed_loop_episodes}, media={mediaserver_episodes} -> {merged_episodes}"
                )
            if not force_refresh:
                if (
                    media_type_str != "电影"
                    and total_episodes
                    and not merged_episodes
                ):
                    direct_episodes = self._get_direct_target_existing_episodes(
                        title=title,
                        media_type_str=media_type_str,
                        season=season,
                    )
                    direct_episodes = _drop_suspicious_over_total("正式库单剧目录", direct_episodes)
                    if direct_episodes:
                        merged_episodes = sorted(set(merged_episodes) | set(direct_episodes))
                        logger.info(
                            f"【115助手】{title} 使用正式库单剧目录兜底修正已有集数: "
                            f"{merged_episodes}"
                        )
                self._set_existing_episodes_cache(cache_key, merged_episodes)
                return merged_episodes

            library_episodes = self._get_library_existing_episodes(
                title=title,
                tmdb_id=tmdb_id,
                media_type_str=media_type_str,
                season=season,
            )
            merged_episodes = sorted(set(merged_episodes) | set(library_episodes))
            self._set_existing_episodes_cache(cache_key, merged_episodes)
            return merged_episodes

        except Exception as e:
            logger.warning(f"【115助手】获取已有集数失败: {e}")
            return self._normalize_existing_episode_hint(cached_episodes, media_type_str)

    def _get_transfer_history_episodes(self, tmdb_id: Optional[int],
                                       media_type_str: str,
                                       season: Optional[int]) -> List[int]:
        """从 MP 整理历史读取已成功入库的集数，补足 Plex 未及时扫描的时间差。"""
        if not tmdb_id:
            return []
        try:
            from app.db.transferhistory_oper import TransferHistoryOper
            history_oper = TransferHistoryOper()
            if media_type_str == "电影":
                histories = history_oper.get_by(mtype=media_type_str, tmdbid=int(tmdb_id)) or []
                if any(getattr(h, "status", False) for h in histories):
                    return [-1]
                return []

            target_season = int(season or 1)
            season_tag = f"S{target_season:02d}"
            histories = history_oper.get_by(
                mtype=media_type_str,
                tmdbid=int(tmdb_id),
                season=season_tag,
            ) or []
            episodes = set()
            for item in histories:
                if not getattr(item, "status", False):
                    continue
                episode_text = str(getattr(item, "episodes", "") or "").upper()
                if not episode_text:
                    dest = str(getattr(item, "dest", "") or "")
                    episode_text = dest.upper()

                for start, end in re.findall(r"E?(\d{1,3})\s*-\s*E?(\d{1,3})", episode_text):
                    start_num, end_num = int(start), int(end)
                    if start_num <= end_num:
                        episodes.update(range(start_num, end_num + 1))

                for ep in re.findall(r"E(\d{1,3})", episode_text):
                    episodes.add(int(ep))

            return sorted(episodes)
        except Exception as e:
            logger.warning(f"【115助手】读取 MP 入库历史失败: {e}")
            return []

    # ================================================================
    #  CD2 降级转存
    # ================================================================

    def _get_115_target_path(self, subscription: dict) -> str:
        """
        根据订阅的媒体类型，返回 CD2 路径中 115 的目标目录。
        CD2 挂载路径格式: /115/<子目录>
        """
        media_type = subscription.get("media_type", "")
        media_category = subscription.get("media_category", "ongoing")
        if media_type == "电影":
            if media_category == "movie_archive":
                return self._cd2_target_old_movie_path.rstrip("/")
            return self._cd2_target_movie_path.rstrip("/")
        elif media_category == "archive":
            return self._cd2_target_archive_path.rstrip("/")
        else:
            return self._cd2_target_ongoing_path.rstrip("/")

    def _search_pansou_for_cloud(
        self,
        keyword: str,
        cloud_name: str,
        expected_year: Optional[int] = None,
    ) -> List[dict]:
        """
        在指定云盘类型上搜索 PanSou 资源。
        复用 _search_pansou 的认证/过滤逻辑，但只提取指定云盘的结果。

        :param keyword:       搜索关键词（已去年份）
        :param cloud_name:    云盘名称，如 "阿里云盘"、"123云盘"
        :param expected_year: 期望年份，用于过滤排序
        :return: 结果列表，每条含 url / password / note 等字段
        """
        if not self._pansou_url:
            return []
        pansou_url = self._pansou_url.rstrip("/")
        headers = {"Content-Type": "application/json"}

        # ── 认证 ──
        if self._pansou_auth_user and self._pansou_auth_pass:
            try:
                with httpx.Client(timeout=10.0) as client:
                    auth_resp = client.post(
                        f"{pansou_url}/api/auth/login",
                        json={
                            "username": self._pansou_auth_user,
                            "password": self._pansou_auth_pass,
                        },
                    )
                    token = auth_resp.json().get("token")
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
            except Exception as e:
                logger.warning(f"【115助手】PanSou 认证失败: {e}")

        # ── 过滤条件 ──
        filter_config: Dict[str, Any] = {}
        if self._quality_keywords:
            filter_config["include"] = self._quality_keywords
        if self._exclude_keywords:
            filter_config["exclude"] = self._exclude_keywords

        search_body: Dict[str, Any] = {"kw": keyword, "res": "merge"}
        if filter_config:
            search_body["filter"] = filter_config

        logger.info(
            f"【115助手】[降级] PanSou 搜索 {cloud_name}: "
            f"{json.dumps(search_body, ensure_ascii=False)}"
        )

        results = []
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    f"{pansou_url}/api/search",
                    headers=headers,
                    json=search_body,
                )
            if resp.status_code != 200:
                logger.warning(
                    f"【115助手】[降级] PanSou 搜索 HTTP {resp.status_code}"
                )
                return []

            data = resp.json().get("data", {})
            if isinstance(data, dict):
                merged = data.get("merged_by_type", {})
                if merged:
                    # 中文云盘名 → PanSou API 分组 key 映射
                    _CLOUD_KEY_MAP = {
                        "阿里云盘": ["aliyun", "ali"],
                        "123云盘": ["123"],
                        "夸克": ["quark"],
                        "百度网盘": ["baidu"],
                        "迅雷": ["xunlei"],
                    }
                    target_keys = _CLOUD_KEY_MAP.get(cloud_name, [cloud_name.lower()])
                    for cloud_key, links in merged.items():
                        key_lower = str(cloud_key).lower()
                        if any(k in key_lower or key_lower in k for k in target_keys):
                            results.extend(links or [])
                    if results:
                        logger.info(f"【115助手】[降级] 匹配 {cloud_name}: {len(results)} 个结果")
                    if not results:
                        logger.info(
                            f"【115助手】[降级] 未找到 {cloud_name} 分组, "
                            f"可用分组: {list(merged.keys())}"
                        )
        except Exception as e:
            logger.error(f"【115助手】[降级] PanSou 搜索异常: {e}")
            return []

        if results and expected_year:
            results = self._filter_and_sort_by_year(results, expected_year)

        logger.info(
            f"【115助手】[降级] {cloud_name} 共找到 {len(results)} 个结果"
        )
        return results

    def _get_fallback_staging_path(self, cloud_name: str) -> str:
        """返回备用云盘在 CD2 中的临时目录根路径。"""
        cloud_name = str(cloud_name or "")
        if "阿里" in cloud_name:
            return self._aliyun_staging_path
        if "123" in cloud_name:
            return self._123_staging_path
        if "夸克" in cloud_name:
            return self._quark_staging_path
        if "百度" in cloud_name:
            return self._baidu_staging_path
        return ""

    @staticmethod
    def _fallback_via_tag(cloud_name: str, status: str) -> str:
        """订阅状态里记录降级来源，避免所有云盘都写成 aliyun。"""
        cloud_name = str(cloud_name or "")
        if "阿里" in cloud_name:
            key = "aliyun"
        elif "123" in cloud_name:
            key = "123"
        elif "夸克" in cloud_name:
            key = "quark"
        elif "百度" in cloud_name:
            key = "baidu"
        else:
            key = "fallback"
        return f"{key}_cd2_{status}"

    @staticmethod
    def _cloud_api_staging_from_cd2_path(cloud_name: str, cd2_staging: str) -> str:
        """
        把 CD2 挂载路径转换成对应网盘 API 看到的真实路径。

        普通云盘：/阿里云盘/MP临时转存/剧名 -> /MP临时转存/剧名
        夸克 WebDAV：/WebDAV/kuake/MP临时转存/剧名 -> /MP临时转存/剧名
        """
        parts = [p for p in str(cd2_staging or "").strip("/").split("/") if p]
        if not parts:
            return "/"
        if "夸克" in str(cloud_name or "") and len(parts) >= 3 and parts[0].lower() == "webdav":
            return "/" + "/".join(parts[2:])
        if len(parts) > 1:
            return "/" + "/".join(parts[1:])
        return "/"

    def _try_fallback_cloud_transfer(
        self,
        subscription: dict,
        clean_keyword: str,
        expected_year: Optional[int] = None,
        existing_episodes: Optional[List[int]] = None,
    ):
        """
        降级转存：
          1. 阿里云盘通过 AliyunClient（aligo）把分享链接转存到临时目录
          2. 夸克/百度等备用盘通过各自插件客户端把分享链接保存到临时目录
          3. CD2 负责跨云复制：备用云盘临时目录 → 115
        只在 115 直接搜索失败时由 _process_subscription 调用。
        不改动 115 直接转存流程。
        """
        try:
            from .clients import AliyunClient as _AliyunClient
            from .clients import QuarkClient as _QuarkClient
            from .clients import BaiduClient as _BaiduClient
            from .clients import CD2Client as _CD2Client
        except ImportError:
            logger.warning("【115助手】[降级] 客户端模块不可用，跳过降级转存")
            return

        title = subscription.get("title", clean_keyword)
        effective_fallback_clouds = self._get_effective_fallback_clouds()
        logger.info(
            f"【115助手】[降级] 开始备用云盘搜索: {title}, "
            f"已启用: {self._fallback_clouds}, 执行顺序: {effective_fallback_clouds}"
        )
        if self._is_115_cooldown_active(f"{title} 的降级转存"):
            return
        if self._fallback_copy_guard_active(subscription):
            return

        # ── 检查 CD2 连接 ──
        if not (self._cd2_host and _CD2Client and (self._cd2_token or (self._cd2_username and self._cd2_password))):
            logger.warning("【115助手】[降级] CD2 未配置，跳过降级转存")
            return
        cd2 = self._build_cd2_client()
        if not cd2.test_connection():
            logger.warning(
                f"【115助手】[降级] {self._format_cd2_connection_error(cd2)} "
                f"({self._cd2_host}:{self._cd2_port})，"
                f"跳过降级转存"
            )
            return

        target_115 = self._get_115_target_path(subscription)
        if not target_115:
            logger.warning(
                f"【115助手】[降级] 未配置 {title} 对应的 115 目标目录（CD2 路径），跳过降级转存"
            )
            return

        fallback_clouds = list(effective_fallback_clouds or [])
        if not fallback_clouds:
            logger.warning("【115助手】[降级] 未启用有效备用云盘，跳过降级转存")
            return
        target_status_cache = {}
        fallback_fail_reasons = []

        def _remember_fallback_reason(reason: str) -> None:
            if reason and len(fallback_fail_reasons) < 12:
                fallback_fail_reasons.append(reason)

        # 按用户勾选的备用云盘逐个搜索；批次额度只限制最终提交到 115 的复制任务数，
        # 不能截断云盘列表，否则第一个盘无结果时会误判“所有备用云盘均未找到”。
        for cloud_name in fallback_clouds:
            results = self._search_pansou_for_cloud(
                clean_keyword, cloud_name, expected_year
            )
            if not results:
                logger.info(f"【115助手】[降级] {cloud_name} 未搜到 {title} 的资源")
                _remember_fallback_reason(f"{cloud_name}: 未搜到资源")
                continue

            ranked_results = self._rank_episode_results_for_missing(
                results,
                subscription,
                existing_episodes,
                log_context=f"[降级] {title}/{cloud_name}",
            )
            if ranked_results:
                results = [item["result"] for item in ranked_results]
            else:
                logger.info(
                    f"【115助手】[降级] {cloud_name} 搜索结果均未覆盖 {title} 当前缺失集数"
                )
                _remember_fallback_reason(f"{cloud_name}: 结果未覆盖缺失集")
                continue

            staging_root = self._get_fallback_staging_path(cloud_name).rstrip("/")
            if not staging_root:
                logger.info(f"【115助手】[降级] {cloud_name} 未配置临时目录，跳过")
                _remember_fallback_reason(f"{cloud_name}: 未配置临时目录")
                continue

            is_aliyun = "阿里" in cloud_name
            is_quark = "夸克" in cloud_name
            is_baidu = "百度" in cloud_name
            is_123 = "123" in cloud_name

            cloud_client = None
            if is_aliyun:
                cloud_client = _AliyunClient
            elif is_quark:
                cloud_client = _QuarkClient
                if cloud_client:
                    cloud_client.set_cookie(self._quark_cookie)
            elif is_baidu:
                cloud_client = _BaiduClient
                if cloud_client:
                    cloud_client.set_cookie(self._baidu_cookie)
            elif is_123:
                logger.warning(
                    "【115助手】[降级] 123云盘尚未接入插件内原生转存，按当前策略不再使用 CD2 AddSharedLink，跳过"
                )
                continue

            if cloud_client is None or not cloud_client.is_available():
                logger.warning(f"【115助手】[降级] {cloud_name} 客户端不可用，跳过")
                _remember_fallback_reason(f"{cloud_name}: 客户端不可用")
                continue

            if not cloud_client.is_logged_in():
                logger.warning(
                    f"【115助手】[降级] {cloud_name} 未登录，请先在插件登录中心/设置中完成登录"
                )
                _remember_fallback_reason(f"{cloud_name}: 未登录")
                continue

            # 最多尝试 3 个结果
            for result in results[:3]:
                if not self._fallback_result_matches_subscription(result, subscription):
                    continue
                share_url = result.get("url", "")
                share_pwd = result.get("password", "")
                if not share_url:
                    continue

                if subscription.get("media_type", "电视剧") != "电影":
                    result_text = " ".join(
                        str(result.get(key, "") or "")
                        for key in ("note", "name", "title")
                    )
                    hinted_episodes = self._extract_episode_hints_from_text(
                        result_text,
                        total_episodes=subscription.get("_cache_total_episodes"),
                        target_season=subscription.get("season"),
                    )
                    if hinted_episodes and not (set(hinted_episodes) - set(existing_episodes or [])):
                        logger.info(
                            f"【115助手】[降级] 跳过标题已明确为已有集数的{cloud_name}结果: "
                            f"集数={self._format_episode_preview(hinted_episodes)}, "
                            f"note={str(result.get('note', share_url))[:80]}"
                        )
                        continue

                if self._is_dead_share_link(share_url):
                    logger.info(
                        f"【115助手】[降级] 跳过已失效分享({cloud_name}): {str(share_url)[:70]}"
                    )
                    continue

                logger.info(
                    f"【115助手】[降级] 尝试{cloud_name}转存: "
                    f"{result.get('note', share_url[:60])}"
                )

                # 目标子目录：临时根目录 / 标题（避免多次转存混在一起）
                safe_title = re.sub(r'[\\/:*?"<>|]', "_", title)
                # cd2_staging: CD2 挂载路径，如 /阿里云盘/MP临时转存/Title
                cd2_staging = f"{staging_root}/{safe_title}"
                cloud_api_staging = self._cloud_api_staging_from_cd2_path(cloud_name, cd2_staging)

                try:
                    import sys as _sys
                    _SAVE_SHARE_TIMEOUT = 120  # 分享链接保存单次调用超时（秒）
                    _CD2_LIST_TIMEOUT = 45  # gRPC streaming 超时（秒）
                    _CD2_COPY_SUBMIT_TIMEOUT = 90  # CD2 copy_file 提交超时（秒）
                    _FILE_WAIT_MAX = 900    # 等待备用云盘文件就绪最长时间（秒）
                    _FILE_WAIT_STEP = 20   # 轮询间隔（秒）

                    # 0. 先检查 115 目标目录 / 备用云盘临时目录是否已有内容（防止重复转存）
                    _year_sfx = f" ({expected_year})" if expected_year else ""
                    _target_show_path = f"{target_115}/{safe_title}{_year_sfx}"
                    _base_existing_set = set(existing_episodes) if existing_episodes else set()
                    _target_existing_episodes = set()
                    _effective_existing_set = set(_base_existing_set)

                    def _ep_num_from_target_name(_name):
                        """从目标目录/库文件名提取集号，用于先算真正缺失集。"""
                        _name = str(_name or "")
                        _m = re.search(r'[Ss]\d+\s*[\.\-_\s]?\s*[Ee](\d+)', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.search(r'第(\d+)[集话]', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.search(r'[Ee][Pp]?(\d{1,3})\b', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.match(r'^(\d{1,3})(?:[\s\._\-~]|$)', _name)
                        if _m:
                            return int(_m.group(1))
                        return None

                    def _collect_target_episodes(_items, _depth=0):
                        _episodes = set()
                        if not _items or _depth > 4:
                            return _episodes
                        for _item in _items:
                            _name = _item.get("name", "")
                            if _item.get("is_dir"):
                                try:
                                    _sub = cd2.list_dir(_item["path"], force_refresh=True) or []
                                except Exception:
                                    _sub = []
                                _episodes.update(_collect_target_episodes(_sub, _depth + 1))
                                continue
                            _ep = _ep_num_from_target_name(_name)
                            if _ep is not None:
                                _episodes.add(_ep)
                        return _episodes

                    # 0a. 检查备用云盘临时目录：如果已有该剧的文件，交给后续缺失集过滤处理。
                    # 旧逻辑在 115 目标目录为空时直接跳过，会让旧暂存目录永久堵住新资源。
                    try:
                        _aliyun_check = cd2.list_dir(cd2_staging, force_refresh=True)
                        if _aliyun_check:
                            # 判断 115 目标是否已经有内容（说明上次复制已完成，暂存大概率是残留）
                            _staging_already_in_115 = False
                            try:
                                _target_show_name = f"{safe_title}{_year_sfx}"
                                _parent_items = cd2.list_dir(target_115, force_refresh=True) or []
                                _target_dir_item = next(
                                    (
                                        _item for _item in _parent_items
                                        if _item.get("is_dir") and _item.get("name") == _target_show_name
                                    ),
                                    None,
                                )
                                if _target_dir_item:
                                    _115_now = cd2.list_dir(
                                        _target_dir_item.get("path") or _target_show_path,
                                        force_refresh=True,
                                    )
                                    if _115_now:
                                        _staging_already_in_115 = True
                            except Exception:
                                pass

                            if _staging_already_in_115:
                                # 暂存是上次成功转存的残留，清理后允许重新转存新集
                                _sys.stdout.write(
                                    f"【降级】暂存目录是旧残留（115已有内容），清理后重新转存: {cd2_staging}\n"
                                )
                                _sys.stdout.flush()
                                try:
                                    self._cleanup_fallback_staging(
                                        cd2, _aliyun_check,
                                        cloud_client,
                                        cloud_api_staging,
                                    )
                                except Exception as _ce:
                                    _sys.stdout.write(f"【降级】清理暂存异常: {_ce}\n"); _sys.stdout.flush()
                                # 不 break，继续执行新一轮转存
                            else:
                                _sys.stdout.write(
                                    f"【降级】{cloud_name}临时目录已有内容，稍后按缺失集判断是否复用: {cd2_staging}\n"
                                )
                                _sys.stdout.flush()
                    except Exception:
                        pass  # 目录不存在或查询失败，继续检查 115

                    # 0b. 先从父目录确认剧名子目录是否存在。
                    # CD2 对“空目录”和“不存在”都可能返回空列表，直接 list 子目录会误判。
                    _cached_target_status = target_status_cache.get(_target_show_path)
                    if _cached_target_status is not None:
                        _target_existing_episodes = set(_cached_target_status.get("episodes", []))
                        _effective_existing_set = _base_existing_set | _target_existing_episodes
                    else:
                        try:
                            _target_show_name = f"{safe_title}{_year_sfx}"
                            _115_check = []
                            _target_parent_items = cd2.list_dir(target_115, force_refresh=True)
                            _target_parent_err = getattr(cd2, "last_error", "") or ""
                            _target_dir_item = next(
                                (
                                    _item for _item in (_target_parent_items or [])
                                    if _item.get("is_dir") and _item.get("name") == _target_show_name
                                ),
                                None,
                            )
                            if _target_parent_err:
                                _sys.stdout.write(
                                    f"【降级】检查115目标父目录异常: {target_115} | {_target_parent_err}，继续转存\n"
                                )
                                _sys.stdout.flush()
                            elif not _target_dir_item:
                                _sys.stdout.write(
                                    f"【降级】115目标子目录尚未创建，后续如有可复制文件会自动创建: {_target_show_path}\n"
                                )
                                _sys.stdout.flush()
                            else:
                                _115_check = cd2.list_dir(_target_dir_item.get("path") or _target_show_path, force_refresh=True)
                                _target_dir_err = getattr(cd2, "last_error", "") or ""
                                if _target_dir_err:
                                    _sys.stdout.write(
                                        f"【降级】检查115目标子目录异常: {_target_show_path} | {_target_dir_err}，继续转存\n"
                                    )
                                    _sys.stdout.flush()
                                    _115_check = []
                            if _target_dir_item and _115_check:
                                _target_existing_episodes = _collect_target_episodes(_115_check)
                                _effective_existing_set = _base_existing_set | _target_existing_episodes
                                _sys.stdout.write(
                                    f"【降级】115目标已有内容，目标目录已有集数: {sorted(_target_existing_episodes)}，"
                                    f"合并已有: {sorted(_effective_existing_set)}\n"
                                )
                                _sys.stdout.flush()
                                # 目标目录里的待入库文件也算已有；只有库+目标目录全集完整时才退出。
                                _total = subscription.get("_cache_total_episodes")
                                if _total and _total > 0:
                                    _expected_eps = set(range(1, int(_total) + 1))
                                    if _expected_eps.issubset(_effective_existing_set):
                                        if self._should_allow_auto_complete(subscription):
                                            _sys.stdout.write(
                                                f"【降级】{title} 库+目标目录已覆盖全集 "
                                                f"({len(_effective_existing_set & _expected_eps)}/{_total})，标记完结\n"
                                            )
                                            _sys.stdout.flush()
                                            subscription["auto_completed"] = True
                                            subscription["completed_time"] = datetime.now().isoformat()
                                            subscription["completed_reason"] = (
                                                f"库+目标目录全集覆盖 ({len(_effective_existing_set & _expected_eps)}/{_total})"
                                            )
                                        else:
                                            _sys.stdout.write(
                                                f"【降级】{title} 库+目标目录已覆盖当前已知全集 "
                                                f"({len(_effective_existing_set & _expected_eps)}/{_total})，"
                                                "但剧集仍在播出，暂不归档\n"
                                            )
                                            _sys.stdout.flush()
                                        return  # 当前已知集数完整，退出 fallback
                                _sys.stdout.write(
                                    f"【降级】{title} 将只补库+目标目录都缺失的集数\n"
                                )
                                _sys.stdout.flush()
                            elif _target_dir_item:
                                _sys.stdout.write(
                                    f"【降级】115目标子目录已存在但为空，继续转存: {_target_show_path}\n"
                                )
                                _sys.stdout.flush()
                            target_status_cache[_target_show_path] = {
                                "episodes": sorted(_target_existing_episodes),
                            }
                        except Exception as _check_err:
                            _sys.stdout.write(f"【降级】检查115目标目录异常: {_check_err}，继续转存\n")
                            _sys.stdout.flush()
                            target_status_cache[_target_show_path] = {"episodes": []}

                    def _cd2_list_with_timeout(_path, _force=False):
                        """带超时保护的 cd2.list_dir 封装，返回 None 表示超时"""
                        _box = [None]
                        _ev = _threading_mod.Event()
                        def _run():
                            try:
                                _box[0] = cd2.list_dir(_path, force_refresh=_force)
                            except Exception as _e:
                                _sys.stdout.write(f"【降级】cd2.list_dir 异常: {_e}\n")
                                _sys.stdout.flush()
                                _box[0] = []
                            finally:
                                _ev.set()
                        _th = _threading_mod.Thread(target=_run, daemon=True)
                        _th.start()
                        if not _ev.wait(timeout=_CD2_LIST_TIMEOUT):
                            _sys.stdout.write(
                                f"【降级】cd2.list_dir 超时 ({_CD2_LIST_TIMEOUT}s): {_path}\n"
                            )
                            _sys.stdout.flush()
                            return None
                        return _box[0]

                    def _ensure_cd2_dir(_path):
                        """递归确保 CD2 目录存在，CopyFile 的目标目录必须已存在。"""
                        _path = str(_path or "").strip().rstrip("/")
                        if not _path or _path == "/":
                            return True
                        _parts = [p for p in _path.split("/") if p]
                        if not _parts:
                            return True
                        _current = f"/{_parts[0]}"
                        # 第一级是云盘挂载名，只验证不创建。
                        _current_items = _cd2_list_with_timeout(_current, _force=True)
                        if _current_items is None:
                            return False
                        _root_err = getattr(cd2, "last_error", "") or ""
                        if _root_err:
                            _sys.stdout.write(f"【降级】目标云盘根目录不可访问: {_current} | {_root_err}\n")
                            _sys.stdout.flush()
                            return False
                        for _part in _parts[1:]:
                            _next = f"{_current}/{_part}"
                            _dir_item = next(
                                (
                                    _item for _item in (_current_items or [])
                                    if _item.get("is_dir") and _item.get("name") == _part
                                ),
                                None,
                            )
                            if _dir_item:
                                _current = _dir_item.get("path") or _next
                                _current_items = _cd2_list_with_timeout(_current, _force=True)
                                if _current_items is None:
                                    return False
                                _list_err = getattr(cd2, "last_error", "") or ""
                                if _list_err:
                                    _sys.stdout.write(f"【降级】目标目录不可访问: {_current} | {_list_err}\n")
                                    _sys.stdout.flush()
                                    return False
                                continue
                            _sys.stdout.write(f"【降级】创建缺失目标目录: {_current}/{_part}\n")
                            _sys.stdout.flush()
                            if not cd2.create_folder(_current, _part):
                                _sys.stdout.write(f"【降级】创建目标目录失败: {_current}/{_part}\n")
                                _sys.stdout.flush()
                                return False
                            _current = _next
                            _current_items = _cd2_list_with_timeout(_current, _force=True)
                            if _current_items is None:
                                return False
                            _create_list_err = getattr(cd2, "last_error", "") or ""
                            if _create_list_err:
                                _sys.stdout.write(f"【降级】创建后读取目标目录失败: {_current} | {_create_list_err}\n")
                                _sys.stdout.flush()
                                return False
                        return True

                    def _has_files_recursive(_path, _depth=2):
                        """递归检查路径下是否有实际文件（非目录），最多检查 _depth 层"""
                        if _depth < 0:
                            return False
                        _items = _cd2_list_with_timeout(_path, _force=True)
                        if not _items:
                            return False
                        for _item in _items:
                            if not _item.get("is_dir"):
                                return True  # 找到文件
                        # 全是目录，递归检查
                        for _item in _items:
                            if _has_files_recursive(_item["path"], _depth - 1):
                                return True
                        return False

                    def _has_inner_content(_folder_item):
                        """检查目录是否有实际文件内容（用于判断转存是否完成）"""
                        return _has_files_recursive(_folder_item["path"], _depth=2)

                    def _ep_num_from_fallback_name(_name):
                        """从备用云盘文件名提取集号，用于判断暂存残留是否还能复用。"""
                        _name = str(_name or "")
                        _m = re.search(r'[Ss]\d+\s*[\.\-_\s]?\s*[Ee](\d+)', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.search(r'第(\d+)[集话]', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.search(r'[Ee][Pp]?(\d{1,3})\b', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.match(r'^0?(\d{1,3})[xX](?:[\s\._\-~]|$)', _name)
                        if _m:
                            return int(_m.group(1))
                        _m = re.match(r'^(\d{1,3})(?:[\s\._\-~]|$)', _name)
                        if _m:
                            return int(_m.group(1))
                        return None

                    def _collect_staging_files(_items, _depth=0):
                        """展开暂存目录到文件层，供复用判断和后续复制共用。"""
                        _files = []
                        if not _items or _depth > 4:
                            return _files
                        for _item in _items:
                            if not _item.get("is_dir"):
                                _files.append(_item)
                                continue
                            _sub = _cd2_list_with_timeout(_item["path"], _force=True) or []
                            _files.extend(_collect_staging_files(_sub, _depth + 1))
                        return _files

                    # 1. 先检查暂存是否已有内容（上次转存未完成的遗留）
                    _existing = _cd2_list_with_timeout(cd2_staging, _force=False)
                    _skip_save_share = False
                    _wanted_episode_set = set()
                    if subscription.get("media_type", "电视剧") != "电影":
                        try:
                            _total_eps_for_staging = int(subscription.get("_cache_total_episodes") or 0)
                        except (TypeError, ValueError):
                            _total_eps_for_staging = 0
                        if _total_eps_for_staging > 0:
                            _wanted_episode_set = (
                                set(range(1, _total_eps_for_staging + 1)) - set(_effective_existing_set)
                            )
                        _result_hints = self._extract_episode_hints_from_text(
                            " ".join(
                                str(result.get(key, "") or "")
                                for key in ("note", "name", "title", "url")
                            ),
                            total_episodes=subscription.get("_cache_total_episodes"),
                            target_season=subscription.get("season"),
                        )
                        if _result_hints:
                            _hint_set = set(_result_hints)
                            _wanted_episode_set = (
                                (_wanted_episode_set & _hint_set)
                                if _wanted_episode_set
                                else (_hint_set - set(_effective_existing_set))
                            )

                    if _existing and subscription.get("media_type", "电视剧") != "电影" and _wanted_episode_set:
                        _staging_files = _collect_staging_files(_existing)
                        _staging_eps = {
                            _ep for _ep in (
                                _ep_num_from_fallback_name(_item.get("name", ""))
                                for _item in _staging_files
                            )
                            if _ep is not None
                        }
                        if not (_staging_eps & _wanted_episode_set):
                            _sys.stdout.write(
                                f"【降级】暂存目录不含本轮缺失集 "
                                f"{sorted(_wanted_episode_set)}，清理后重新转存: {cd2_staging}\n"
                            )
                            _sys.stdout.flush()
                            try:
                                self._cleanup_fallback_staging(
                                    cd2,
                                    _existing,
                                    cloud_client,
                                    cloud_api_staging,
                                )
                            except Exception as _ce:
                                _sys.stdout.write(f"【降级】清理暂存异常: {_ce}\n"); _sys.stdout.flush()
                            _existing = []
                        else:
                            _sys.stdout.write(
                                f"【降级】暂存目录包含本轮缺失集 {sorted(_staging_eps & _wanted_episode_set)}，复用暂存\n"
                            )
                            _sys.stdout.flush()

                    if _existing and all(item.get("is_dir") for item in _existing):
                        # 顶层是文件夹，检查内层是否有文件
                        if any(_has_inner_content(f) for f in _existing):
                            _sys.stdout.write(f"【降级】发现已有暂存内容，跳过重新转存: {cd2_staging}\n")
                            _sys.stdout.flush()
                            _skip_save_share = True
                    elif _existing:
                        # 顶层直接有文件
                        _sys.stdout.write(f"【降级】发现已有暂存文件，跳过重新转存: {cd2_staging}\n")
                        _sys.stdout.flush()
                        _skip_save_share = True

                    if not _skip_save_share:
                        # 2. 转存分享链接到备用云盘临时目录
                        def _save_share_in_thread():
                            if is_aliyun:
                                import asyncio as _asyncio
                                _loop = _asyncio.new_event_loop()
                                _asyncio.set_event_loop(_loop)
                                try:
                                    return cloud_client.save_shared_link(
                                        share_url, share_pwd, cloud_api_staging
                                    )
                                finally:
                                    try:
                                        _loop.close()
                                    except Exception:
                                        pass
                            return cloud_client.save_shared_link(
                                share_url, share_pwd, cloud_api_staging
                            )

                        _result_box = [None]
                        _done_event = _threading_mod.Event()

                        def _run_save_share():
                            try:
                                _result_box[0] = _save_share_in_thread()
                            except Exception as _e:
                                _sys.stdout.write(f"【降级】{cloud_name}分享保存线程异常: {_e}\n")
                                _sys.stdout.flush()
                                _result_box[0] = False
                            finally:
                                _done_event.set()

                        _t = _threading_mod.Thread(target=_run_save_share, daemon=True)
                        _t.start()
                        _sys.stdout.write(f"【降级】{cloud_name}分享保存启动: {share_url[:60]}\n"); _sys.stdout.flush()
                        _finished = _done_event.wait(timeout=_SAVE_SHARE_TIMEOUT)
                        _sys.stdout.write(f"【降级】{cloud_name}分享保存完成: finished={_finished} result={_result_box[0]}\n")
                        _sys.stdout.flush()

                        if not _finished or not _result_box[0]:
                            reason = "超时" if not _finished else "失败"
                            # 分享本身已被取消/失效时拉黑，避免每轮重复请求同一死链
                            if _finished and getattr(cloud_client, "last_share_dead", False):
                                self._mark_dead_share_link(share_url, cloud_name)
                                _remember_fallback_reason(f"{cloud_name}: 分享已失效")
                                continue
                            logger.warning(
                                f"【115助手】[降级] {cloud_name}转存{reason}: {share_url[:60]}"
                            )
                            _remember_fallback_reason(f"{cloud_name}: 分享保存{reason}")
                            continue

                    # 3. 轮询等待文件就绪（云盘服务端复制是异步的）
                    # 先强制刷新 staging 父目录，确保 CD2 拿到最新的子目录 file_id
                    _staging_parent = staging_root
                    _cd2_list_with_timeout(_staging_parent, _force=True)

                    _poll_start = time.time()
                    top_items = []
                    _staging_not_found_count = 0
                    _staging_missing_reason = ""
                    while time.time() - _poll_start < _FILE_WAIT_MAX:
                        _t2 = _cd2_list_with_timeout(cd2_staging, _force=True)
                        if _t2 is None:
                            # gRPC 超时，稍后重试
                            time.sleep(_FILE_WAIT_STEP)
                            continue
                        _list_err = getattr(cd2, "last_error", "") or ""
                        if _t2:
                            _staging_not_found_count = 0
                            if all(item.get("is_dir") for item in _t2):
                                # 顶层全是文件夹：检查内层是否有文件
                                if any(_has_inner_content(f) for f in _t2):
                                    top_items = _t2
                                    break
                            else:
                                # 顶层直接有文件
                                top_items = _t2
                                break
                        elif _list_err and "not_found" in _list_err.lower():
                            _staging_not_found_count += 1
                            _elapsed = int(time.time() - _poll_start)
                            if _staging_not_found_count >= 3 and _elapsed >= 40:
                                _staging_missing_reason = (
                                    f"暂存目录保存后仍不可见: {cd2_staging} | {_list_err}"
                                )
                                _sys.stdout.write(f"【降级】{_staging_missing_reason}\n")
                                _sys.stdout.flush()
                                break
                        else:
                            _staging_not_found_count = 0
                        # 文件未就绪，等待后重试
                        _elapsed = int(time.time() - _poll_start)
                        _sys.stdout.write(
                            f"【降级】等待文件就绪 {_elapsed}s: {cd2_staging}\n"
                        )
                        _sys.stdout.flush()
                        time.sleep(_FILE_WAIT_STEP)

                    if not top_items:
                        # 超时：不清理暂存，留待下次检查时使用
                        if _staging_missing_reason:
                            logger.warning(f"【115助手】[降级] {cloud_name}{_staging_missing_reason}")
                            _remember_fallback_reason(f"{cloud_name}: 暂存目录不可见")
                            continue
                        logger.warning(
                            f"【115助手】[降级] {cloud_name}文件等待超时 ({_FILE_WAIT_MAX}s): {cd2_staging}"
                        )
                        _remember_fallback_reason(f"{cloud_name}: 文件等待超时")
                        continue

                    # 4. 确定复制源和目标
                    # 分享通常只有一个以混淆标题命名的顶层文件夹，进入内层避免混淆名带入115
                    # 例：staging/S）死丨亡丨之丨花（2026）/Season 1/  → 115/死亡之花 (2026)/Season 1/
                    copy_items = top_items
                    copy_target = target_115
                    if top_items and all(item.get("is_dir") for item in top_items):
                        inner_items = []
                        for folder in top_items:
                            sub = _cd2_list_with_timeout(folder["path"], _force=True)
                            if sub:
                                inner_items.extend(sub)
                        if inner_items:
                            copy_items = inner_items
                            year_str = f" ({expected_year})" if expected_year else ""
                            copy_target = f"{target_115}/{safe_title}{year_str}"
                            _sys.stdout.write(
                                f"【降级】跳过混淆文件夹，复制内容到 {copy_target}\n"
                            )
                            _sys.stdout.flush()

                    season_filtered_items = [
                        _item for _item in copy_items
                        if self._fallback_item_matches_subscription(_item, subscription, expected_year)
                    ]
                    if len(season_filtered_items) < len(copy_items):
                        _sys.stdout.write(
                            f"【降级】按目标季过滤: {len(copy_items)} → {len(season_filtered_items)}\n"
                        )
                        _sys.stdout.flush()
                    copy_items = season_filtered_items
                    if not copy_items:
                        _sys.stdout.write("【降级】没有匹配目标季的文件，跳过该资源\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 没有匹配目标季文件")
                        continue

                    import re as _re_ep
                    import posixpath as _psp
                    import os.path as _osp

                    # 从文件名提取集号（无条件定义，避免后续作用域问题）
                    def _ep_num_from_name(_name):
                        """支持多种命名格式：S01E08 / S01.E08 / 第08集 / EP08 / 纯数字开头。"""
                        return _ep_num_from_fallback_name(_name)

                    def _collect_fallback_files(_items, _depth=0):
                        """把降级暂存目录展开到文件层，避免复制 titleless 的目录结构进 115。"""
                        _files = []
                        if _depth > 4:
                            return _files
                        for _it in _items:
                            if not _it.get("is_dir"):
                                _files.append(_it)
                                continue
                            _sub = _cd2_list_with_timeout(_it["path"], _force=True) or []
                            if not _sub:
                                _sys.stdout.write(
                                    f"【降级】跳过空目录: {_it.get('name', '')}\n"
                                )
                                _sys.stdout.flush()
                                continue
                            _files.extend(_collect_fallback_files(_sub, _depth + 1))
                        return _files

                    _flat_copy_items = _collect_fallback_files(copy_items)
                    if len(_flat_copy_items) != len(copy_items):
                        _sys.stdout.write(
                            f"【降级】展开目录到文件: {len(copy_items)} → {len(_flat_copy_items)}\n"
                        )
                        _sys.stdout.flush()
                    copy_items = _flat_copy_items
                    if not copy_items:
                        _sys.stdout.write("【降级】展开后没有可复制文件，跳过该资源\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 展开后无可复制文件")
                        continue

                    post_flatten_matched_items = [
                        _item for _item in copy_items
                        if self._fallback_item_matches_subscription(_item, subscription, expected_year)
                    ]
                    if len(post_flatten_matched_items) < len(copy_items):
                        _sys.stdout.write(
                            f"【降级】展开后按标题/季过滤: {len(copy_items)} → {len(post_flatten_matched_items)}\n"
                        )
                        _sys.stdout.flush()
                    copy_items = post_flatten_matched_items
                    if not copy_items:
                        _sys.stdout.write("【降级】展开后没有匹配目标剧的文件，跳过该资源\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 展开后无匹配目标剧文件")
                        continue

                    _media_type = subscription.get("media_type", "电视剧")
                    _archive_exts = getattr(self, "_ARCHIVE_EXTENSIONS", set())
                    _payload_items = []
                    for _item in copy_items:
                        if _item.get("is_dir"):
                            _payload_items.append(_item)
                            continue
                        _name = _item.get("name", "")
                        _ext = _osp.splitext(_name or "")[1].lower()
                        if _ext in _archive_exts:
                            _sys.stdout.write(f"【降级】跳过压缩包/镜像文件: {_name}\n")
                            _sys.stdout.flush()
                            continue
                        if (
                            _media_type == "电影"
                            and _ext not in self._video_extensions
                            and _ext not in self._COMPANION_EXTENSIONS
                        ):
                            _sys.stdout.write(f"【降级】跳过电影非媒体文件: {_name}\n")
                            _sys.stdout.flush()
                            continue
                        _payload_items.append(_item)
                    copy_items = _payload_items
                    if not copy_items:
                        _sys.stdout.write("【降级】过滤后没有可复制媒体文件，跳过该资源\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 无可复制媒体文件")
                        continue

                    try:
                        _total_episode_limit = int(subscription.get("_cache_total_episodes") or 0)
                    except (TypeError, ValueError):
                        _total_episode_limit = 0
                    if _media_type != "电影" and _total_episode_limit > 0:
                        _bounded_items = []
                        for _item in copy_items:
                            if _item.get("is_dir"):
                                _bounded_items.append(_item)
                                continue
                            _ep = _ep_num_from_name(_item.get("name", ""))
                            if _ep is not None and _ep > _total_episode_limit:
                                _sys.stdout.write(
                                    f"【降级】跳过超过总集数的文件 E{_ep:02d}/{_total_episode_limit}: "
                                    f"{_item.get('path') or _item.get('name', '')}\n"
                                )
                                _sys.stdout.flush()
                                continue
                            _bounded_items.append(_item)
                        if len(_bounded_items) < len(copy_items):
                            _sys.stdout.write(
                                f"【降级】按总集数上限过滤: {len(copy_items)} → {len(_bounded_items)}\n"
                            )
                            _sys.stdout.flush()
                        copy_items = _bounded_items
                        if not copy_items:
                            _sys.stdout.write("【降级】所有文件均超过总集数上限，跳过该资源\n")
                            _sys.stdout.flush()
                            _remember_fallback_reason(f"{cloud_name}: 文件超过总集数上限")
                            continue

                    # 4b. 按缺失集数过滤：库里已有 + 115目标目录已有 + pending 都不再复制
                    _existing_set = set(_effective_existing_set)

                    if _existing_set:
                        filtered_items = []
                        for _item in copy_items:
                            _ep = _ep_num_from_name(_item.get("name", ""))
                            if _ep is None or _ep not in _existing_set:
                                filtered_items.append(_item)
                            else:
                                _sys.stdout.write(
                                    f"【降级】跳过已有集数 E{_ep:02d}: {_item.get('name','')}\n"
                                )
                                _sys.stdout.flush()
                        copy_items = filtered_items

                    if subscription.get("media_type", "电视剧") != "电影":
                        _deduped_by_ep = {}
                        _deduped_unknown = []
                        for _item in copy_items:
                            _ep = _ep_num_from_name(_item.get("name", ""))
                            if _ep is None:
                                _deduped_unknown.append(_item)
                                continue
                            _current = _deduped_by_ep.get(_ep)
                            if not _current:
                                _deduped_by_ep[_ep] = _item
                                continue
                            _cur_size = int(_current.get("size", 0) or 0)
                            _new_size = int(_item.get("size", 0) or 0)
                            if _new_size > _cur_size:
                                _deduped_by_ep[_ep] = _item

                        _deduped_items = _deduped_unknown + [
                            _deduped_by_ep[_ep] for _ep in sorted(_deduped_by_ep)
                        ]
                        if len(_deduped_items) < len(copy_items):
                            _sys.stdout.write(
                                f"【降级】按集数去重: {len(copy_items)} → {len(_deduped_items)}\n"
                            )
                            _sys.stdout.flush()
                        copy_items = _deduped_items

                    if not copy_items:
                        _sys.stdout.write(f"【降级】所有文件均已存在，无需复制\n")
                        _sys.stdout.flush()
                        self._cleanup_fallback_staging(
                            cd2, top_items,
                            cloud_client,
                            cloud_api_staging,
                        )
                        _remember_fallback_reason(f"{cloud_name}: 文件均已存在")
                        continue

                    # 4c. 统一规范化剧集文件名，确保 MP 看到的是「剧名.SxxEyy」。
                    _season_num = subscription.get("season") or 1
                    _video_exts = {
                        ".mp4", ".mkv", ".ts", ".m2ts", ".avi", ".mov", ".wmv",
                        ".flv", ".webm", ".rmvb", ".mpg", ".mpeg", ".m4v",
                    }

                    def _is_video_file(_name):
                        return _osp.splitext(_name or "")[1].lower() in _video_exts

                    def _episode_filename_needs_title(_name, _ep):
                        _root, _ext = _osp.splitext(_name or "")
                        if not _root or _ep is None:
                            return None
                        _canonical = f"{safe_title}.S{_season_num:02d}E{_ep:02d}"
                        if _root.lower().startswith(_canonical.lower()):
                            return None

                        _suffix = _root.strip()
                        _patterns = [
                            r'^[Ss]\d+\s*[\.\-_\s]?\s*[Ee]\d+\s*[\s\._\-~]*',
                            r'^[Ee][Pp]?\d{1,3}\b\s*[\s\._\-]*',
                            r'^第\d+[集话]\s*[\s\._\-]*',
                            r'^\d{1,3}[xX]\s*[\s\._\-~]*',
                            r'^\d{1,3}(?:[\s\._\-~]+|$)\s*',
                        ]
                        for _pat in _patterns:
                            _next = _re_ep.sub(_pat, '', _suffix, count=1).strip(" ._-")
                            if _next != _suffix:
                                _suffix = _next
                                break

                        # 如果原名里已经含有同一个剧名但格式不标准，避免重复拼接剧名。
                        if _suffix.lower().startswith(safe_title.lower()):
                            _suffix = _suffix[len(safe_title):].strip(" ._-")

                        _new_root = _canonical
                        if _suffix:
                            _new_root += f".{_suffix}"
                        return _new_root + _ext

                    _renamed_items = []
                    for _ri in copy_items:
                        if _ri.get("is_dir"):
                            _renamed_items.append(_ri)
                            continue
                        _rname = _ri.get("name", "")
                        _rep = _ep_num_from_name(_rname)
                        _new_rname = (
                            _episode_filename_needs_title(_rname, _rep)
                            if _media_type != "电影"
                            else None
                        )
                        if _new_rname and _new_rname != _rname:
                            _rename_ok = cd2.rename_file(_ri["path"], _new_rname)
                            if _rename_ok:
                                _new_path = _psp.join(_psp.dirname(_ri["path"]), _new_rname)
                                _renamed_items.append({**_ri, "name": _new_rname, "path": _new_path})
                                _sys.stdout.write(f"【降级】规范化命名: {_rname} → {_new_rname}\n")
                                _sys.stdout.flush()
                            else:
                                _sys.stdout.write(
                                    f"【降级】规范化命名失败，跳过避免误入库: {_rname}\n"
                                )
                                _sys.stdout.flush()
                        elif _media_type != "电影" and _rep is None and _is_video_file(_rname):
                            _sys.stdout.write(
                                f"【降级】跳过无法识别集数的视频，避免误入库: {_rname}\n"
                            )
                            _sys.stdout.flush()
                        elif _media_type != "电影" and not _is_video_file(_rname):
                            _sys.stdout.write(
                                f"【降级】跳过非视频文件，避免提交无效复制: {_rname}\n"
                            )
                            _sys.stdout.flush()
                        else:
                            _renamed_items.append(_ri)
                    copy_items = _renamed_items
                    if not copy_items:
                        _sys.stdout.write("【降级】没有命名安全的文件可复制，跳过该资源\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 无命名安全文件")
                        continue

                    src_paths = [item["path"] for item in copy_items]
                    _sys.stdout.write(
                        f"【降级】开始复制 {len(src_paths)} 项到 {copy_target}\n"
                    )
                    _sys.stdout.flush()

                    submitted_count = int(getattr(self, "_fallback_copy_submits_this_run", 0) or 0)
                    if submitted_count >= self._FALLBACK_COPY_SUBMIT_LIMIT_PER_RUN:
                        subscription["pending_copy_status"] = "fallback_quota_deferred"
                        subscription["pending_last_error"] = "本轮降级复制额度已用完，留待下轮后补"
                        logger.info(
                            f"【115助手】[降级] 本轮 CD2→115 复制额度已用完 "
                            f"({submitted_count}/{self._FALLBACK_COPY_SUBMIT_LIMIT_PER_RUN})，"
                            f"{title} 留待下轮后补"
                        )
                        return

                    # 5. 确保目标目录存在（CD2 CopyFile 要求目标目录必须已存在）
                    if not _ensure_cd2_dir(copy_target):
                        _sys.stdout.write(f"【降级】目标目录无法创建，跳过复制: {copy_target}\n")
                        _sys.stdout.flush()
                        _remember_fallback_reason(f"{cloud_name}: 目标目录无法创建")
                        continue

                    # 6. CD2 复制到 115
                    def _cd2_copy_with_timeout(_src_paths, _target):
                        _box = [None]
                        _ev = _threading_mod.Event()

                        def _run_copy():
                            try:
                                _box[0] = cd2.copy_file(_src_paths, _target)
                            except Exception as _copy_err:
                                _sys.stdout.write(f"【降级】CD2 copy_file 异常: {_copy_err}\n")
                                _sys.stdout.flush()
                                _box[0] = False
                            finally:
                                _ev.set()

                        _threading_mod.Thread(target=_run_copy, daemon=True).start()
                        if not _ev.wait(timeout=_CD2_COPY_SUBMIT_TIMEOUT):
                            _sys.stdout.write(
                                f"【降级】CD2 copy_file 提交超时 "
                                f"({_CD2_COPY_SUBMIT_TIMEOUT}s)，跳过本资源避免阻塞整轮巡检\n"
                            )
                            _sys.stdout.flush()
                            return None
                        return _box[0]

                    _copy_started_at = time.time()
                    copy_ok = _cd2_copy_with_timeout(src_paths, copy_target)
                    if copy_ok is None:
                        _timeout_ep_nums = []
                        for _ci in copy_items:
                            if not _ci.get("is_dir"):
                                _cen = _ep_num_from_name(_ci.get("name", ""))
                                if _cen is not None:
                                    _timeout_ep_nums.append(_cen)
                        if _timeout_ep_nums:
                            _pending_unique = sorted(set(_timeout_ep_nums))
                            _pending_since_now = datetime.now().isoformat()
                            subscription["pending_copy_episodes"] = _pending_unique
                            subscription["pending_copy_since"] = _pending_since_now
                            subscription["pending_copy_status"] = "copy_submit_timeout"
                            subscription["_cache_pending_count"] = len(_pending_unique)
                            subscription["_cache_pending_episodes"] = _pending_unique
                            subscription["_cache_pending_since"] = _pending_since_now
                        subscription["last_found"] = datetime.now().isoformat()
                        subscription["last_found_via"] = self._fallback_via_tag(cloud_name, "copy_submit_timeout")
                        continue
                    if not copy_ok:
                        _sys.stdout.write("【降级】CD2 copy_file 提交失败\n"); _sys.stdout.flush()
                        self._cleanup_fallback_staging(
                            cd2, top_items,
                            cloud_client,
                            cloud_api_staging,
                        )
                        _remember_fallback_reason(f"{cloud_name}: CD2复制提交失败")
                        continue
                    self._fallback_copy_submits_this_run = submitted_count + 1

                    # 6. 记录待入库集数（防止 Plex 未索引时下轮重复转存）
                    _copied_ep_nums = []
                    for _ci in copy_items:
                        if not _ci.get("is_dir"):
                            _cen = _ep_num_from_name(_ci.get("name", ""))
                            if _cen is not None:
                                _copied_ep_nums.append(_cen)
                    if _copied_ep_nums:
                        _pending_unique = sorted(set(_copied_ep_nums))
                        _pending_since_now = datetime.now().isoformat()
                        subscription["pending_copy_episodes"] = _pending_unique
                        subscription["pending_copy_since"] = _pending_since_now
                        subscription["pending_copy_status"] = "copy_submitted"
                        subscription["_cache_pending_count"] = len(_pending_unique)
                        subscription["_cache_pending_episodes"] = _pending_unique
                        subscription["_cache_pending_since"] = _pending_since_now

                    # 7. 后台等待复制完成（不阻塞主循环）
                    _bg_cd2 = self._build_cd2_client()
                    _notify_flag = self._notify
                    _is_movie = subscription.get("media_type") == "电影"
                    _season = subscription.get("season")
                    _result_note = result.get("note", share_url[:60])
                    _via_copied = self._fallback_via_tag(cloud_name, "copied")
                    _via_pending = self._fallback_via_tag(cloud_name, "pending")

                    def _bg_wait(
                        _cd2=_bg_cd2,
                        _top=list(top_items),
                        _fallback_cls=cloud_client,
                        _aligo=cloud_api_staging,
                        _cloud=cloud_name,
                        _via_copied=_via_copied,
                        _title=title,
                        _sub=subscription,
                        _tgt=target_115,
                        _notify=_notify_flag,
                        _movie=_is_movie,
                        _season=_season,
                        _note=_result_note,
                        _src_paths=list(src_paths),
                        _copy_target=copy_target,
                        _started_after=_copy_started_at,
                    ):
                        try:
                            _ok = _cd2.wait_for_copy(
                                timeout=900,
                                source_paths=_src_paths,
                                dest_path=_copy_target,
                                started_after=_started_after,
                            )
                            _sys.stdout.write(f"【降级/BG】wait_for_copy 结果: {_ok} ({_title})\n")
                            _sys.stdout.flush()
                            if _ok:
                                self._cleanup_fallback_staging(_cd2, _top, _fallback_cls, _aligo)
                                logger.info(f"【115助手】[降级/BG] 转存成功: {_title}")
                                _sub["last_found"] = datetime.now().isoformat()
                                _sub["last_found_via"] = _via_copied
                                _sub["pending_copy_status"] = "copied_to_staging"
                                # 持锁更新订阅数据并持久化
                                with self._subscriptions_lock:
                                    _subs = self._load_subscriptions()
                                    _tmdb = _sub.get("tmdb_id")
                                    _seas = _sub.get("season")
                                    _pending_eps = _sub.get("pending_copy_episodes", [])
                                    for _s in _subs:
                                        if _s.get("tmdb_id") == _tmdb and _s.get("season") == _seas:
                                            _s["last_found"] = _sub["last_found"]
                                            _s["last_found_via"] = _via_copied
                                            _s["pending_copy_status"] = "copied_to_staging"
                                            if _pending_eps:
                                                _s["pending_copy_episodes"] = _pending_eps
                                                _s["pending_copy_since"] = _sub.get("pending_copy_since") or datetime.now().isoformat()
                                                _s["_cache_pending_count"] = len(_pending_eps)
                                                _s["_cache_pending_episodes"] = _pending_eps
                                                _s["_cache_pending_since"] = _s["pending_copy_since"]
                                            if _movie:
                                                _s["auto_completed"] = True
                                                _s["completed_time"] = datetime.now().isoformat()
                                                _s["completed_reason"] = "降级转存成功"
                                            break
                                    self._save_subscriptions(_subs)
                                if _pending_eps and not _movie:
                                    self._start_pending_organize_background(
                                        tmdb_id=int(_tmdb) if _tmdb else None,
                                        season=int(_seas) if _seas else None,
                                        reason=f"CD2降级复制完成后自动整理待入库: {_title}",
                                    )
                                if _notify:
                                    _season_str = f" 第{_season}季" if _season else ""
                                    self.post_message(
                                        mtype=NotificationType.MediaServer,
                                        title=f"115网盘助手 - {_title}{_season_str}",
                                        text=(
                                            f"[降级转存] 通过 {_cloud} → CD2 → 115 转存完成\n"
                                            f"目标目录: {_tgt}\n"
                                            f"来源: {_note}"
                                        ),
                                    )
                            else:
                                _sys.stdout.write(f"【降级/BG】CD2 复制超时或失败: {_title}\n")
                                _sys.stdout.flush()
                                logger.warning(f"【115助手】[降级/BG] CD2 复制超时或失败: {_title}")
                        except Exception as _e:
                            _sys.stdout.write(f"【降级/BG】后台等待异常: {_e}\n")
                            _sys.stdout.flush()

                    _threading_mod.Thread(target=_bg_wait, daemon=True).start()
                    subscription["last_found"] = datetime.now().isoformat()
                    subscription["last_found_via"] = _via_pending
                    logger.info(f"【115助手】[降级] CD2 复制任务已提交，后台处理: {title}")
                    return  # 不阻塞，立即返回

                except Exception as e:
                    _sys.stdout.write(f"【降级】转存异常: {e}\n"); _sys.stdout.flush()
                    logger.error(f"【115助手】[降级] 转存异常: {e}", exc_info=True)
                    _remember_fallback_reason(f"{cloud_name}: 转存异常 {e}")

        if fallback_fail_reasons:
            logger.info(
                f"【115助手】[降级] {title} 未能提交备用云盘转存，原因: "
                f"{'; '.join(fallback_fail_reasons)}"
            )
        else:
            logger.info(f"【115助手】[降级] {title} 在所有备用云盘均未找到可用资源")

    def _cleanup_fallback_staging(self, cd2, cd2_items, cloud_client=None, cloud_api_staging: str = ""):
        """清理备用云盘临时子目录；CD2 删除后再通过插件客户端做双保险。"""
        # 通过 CD2 删除
        for item in cd2_items:
            try:
                cd2.delete_file(item["path"])
            except Exception:
                pass
        # 备用云盘通过原生客户端删除整个子目录，避免 CD2 缓存未刷导致残留。
        if cloud_client and cloud_api_staging:
            try:
                if hasattr(cloud_client, "delete_path") and cloud_client.delete_path(cloud_api_staging):
                    return
                items = cloud_client.list_folder(cloud_api_staging)
                for f in items:
                    file_id = f.get("file_id") or f.get("fid") or f.get("fs_id")
                    if file_id and hasattr(cloud_client, "delete_file"):
                        cloud_client.delete_file(file_id)
            except Exception:
                pass

    def _cleanup_aliyun_staging(self, cd2, cd2_items, aliyun_client, cd2_staging: str, aligo_staging: str):
        """兼容旧调用：清理阿里云盘临时子目录。"""
        self._cleanup_fallback_staging(cd2, cd2_items, aliyun_client, aligo_staging)

    def _search_pansou(self, keyword: str, expected_year: Optional[int] = None) -> List[dict]:
        """
        搜索 PanSou 并返回 115 相关资源。
        策略：
          1. 用 res=merge 搜索（按网盘类型分组），提取 115 分组
          2. 如果 merged 格式无结果，用普通格式重搜，手动过滤 115 链接
          3. 如果 expected_year 有值，优先返回描述中包含该年份的结果
        """
        if not self._pansou_url:
            logger.warning("【115助手】未配置 PanSou URL")
            return []
        pansou_url = self._pansou_url.rstrip("/")
        headers = {"Content-Type": "application/json"}

        # ── PanSou 认证 ──
        if self._pansou_auth_user and self._pansou_auth_pass:
            try:
                with httpx.Client(timeout=10.0) as client:
                    auth_resp = client.post(
                        f"{pansou_url}/api/auth/login",
                        json={
                            "username": self._pansou_auth_user,
                            "password": self._pansou_auth_pass,
                        },
                    )
                    token = auth_resp.json().get("token")
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
            except Exception as e:
                logger.warning(f"【115助手】PanSou 认证失败: {e}")

        # ── 构建过滤条件 ──
        filter_config: Dict[str, Any] = {}
        if self._quality_keywords:
            filter_config["include"] = self._quality_keywords
        if self._exclude_keywords:
            filter_config["exclude"] = self._exclude_keywords

        # ── 第 1 步: merge 模式搜索（跟网页版一样，按网盘类型分组）──
        search_body: Dict[str, Any] = {
            "kw": keyword,
            "res": "merge",
        }
        if filter_config:
            search_body["filter"] = filter_config

        logger.info(f"【115助手】PanSou 搜索请求 (merge): {json.dumps(search_body, ensure_ascii=False)}")

        results_115 = []
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    f"{pansou_url}/api/search",
                    headers=headers,
                    json=search_body,
                )
            if resp.status_code == 200:
                result = resp.json()
                data = result.get("data", {})

                if isinstance(data, dict):
                    merged = data.get("merged_by_type", {})
                    if merged:
                        # 从分组中提取 115 相关的
                        for cloud_key, links in merged.items():
                            cloud_lower = str(cloud_key).lower()
                            if "115" in cloud_lower:
                                results_115.extend(links or [])
                                logger.info(
                                    f"【115助手】PanSou merge 模式: cloud_type='{cloud_key}' "
                                    f"返回 {len(links or [])} 个结果"
                                )

                        if not results_115:
                            # 没有 115 分组，列出所有分组名供调试
                            logger.info(
                                f"【115助手】PanSou merge 模式无 115 分组, "
                                f"可用分组: {list(merged.keys())}"
                            )
                            # 尝试从所有分组中按 URL 筛选
                            for cloud_key, links in merged.items():
                                for link in (links or []):
                                    url = link.get("url", "")
                                    if "115" in url or "anxia" in url or "115cdn" in url:
                                        results_115.append(link)
                    else:
                        resp_preview = json.dumps(result, ensure_ascii=False)[:500]
                        logger.info(
                            f"【115助手】PanSou merge 模式无 merged_by_type, "
                            f"响应: {resp_preview}"
                        )
            else:
                logger.warning(f"【115助手】PanSou merge 搜索 HTTP {resp.status_code}")

        except Exception as e:
            logger.warning(f"【115助手】PanSou merge 搜索异常: {e}")

        if results_115:
            results_115 = self._filter_and_sort_by_year(results_115, expected_year)
            logger.info(f"【115助手】PanSou 找到 {len(results_115)} 个 115 资源")
            return results_115

        # ── 第 2 步: 普通模式搜索，手动过滤 115 链接 ──
        logger.info(f"【115助手】merge 模式未找到 115 资源，尝试普通模式搜索...")
        search_body_plain: Dict[str, Any] = {"kw": keyword}
        if filter_config:
            search_body_plain["filter"] = filter_config

        logger.info(f"【115助手】PanSou 搜索请求 (plain): {json.dumps(search_body_plain, ensure_ascii=False)}")

        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    f"{pansou_url}/api/search",
                    headers=headers,
                    json=search_body_plain,
                )
            if resp.status_code != 200:
                logger.warning(f"【115助手】PanSou plain 搜索 HTTP {resp.status_code}")
                return []

            result = resp.json()
            data = result.get("data", {})

            # 收集所有结果
            all_results = []
            if isinstance(data, dict):
                # 可能有 merged_by_type
                merged = data.get("merged_by_type", {})
                if merged:
                    for cloud_key, links in merged.items():
                        all_results.extend(links or [])
                else:
                    items = data.get("list", data.get("items", data.get("results", [])))
                    if isinstance(items, list):
                        all_results.extend(items)
            elif isinstance(data, list):
                all_results.extend(data)

            if not all_results:
                resp_preview = json.dumps(result, ensure_ascii=False)[:500]
                logger.info(f"【115助手】PanSou plain 搜索无结果, 响应: {resp_preview}")
                return []

            # 从所有结果中筛选 115
            for r in all_results:
                url = r.get("url", "")
                cloud = str(r.get("cloud_type", r.get("type", ""))).lower()
                if "115" in url or "anxia" in url or "115cdn" in url or "115" in cloud:
                    results_115.append(r)

            if results_115:
                logger.info(
                    f"【115助手】plain 模式找到 {len(results_115)} 个 115 资源 "
                    f"(共 {len(all_results)} 个结果)"
                )
            else:
                logger.info(
                    f"【115助手】plain 模式 {len(all_results)} 个结果中无 115 资源。"
                    f"前3个 URL: {[r.get('url', '')[:60] for r in all_results[:3]]}"
                )

            results_115 = self._filter_and_sort_by_year(results_115, expected_year)
            return results_115

        except Exception as e:
            logger.error(f"【115助手】PanSou plain 搜索异常: {e}")
            return []

    def _filter_and_sort_by_year(self, results: List[dict], expected_year: Optional[int]) -> List[dict]:
        """
        按年份严格过滤 + 排序搜索结果。
        - 描述中包含期望年份 → 保留（排最前）
        - 描述中无年份信息 → 保留（排后面，无法判断）
        - 描述中有其他年份但不含期望年份 → 排除（避免同名异年剧混淆）
        """
        if not expected_year or not results:
            return results

        matched = []   # 描述包含期望年份
        unknown = []   # 描述中无年份信息
        excluded = []  # 描述有不同年份

        for r in results:
            note = r.get("note", "") + " " + r.get("url", "")
            # 提取描述中所有年份
            found_years = set(int(y) for y in re.findall(r'\b((?:19|20)\d{2})\b', note))

            if not found_years:
                unknown.append(r)
            elif expected_year in found_years:
                matched.append(r)
            elif any(abs(y - expected_year) <= 1 for y in found_years):
                # 差一年（发行年 vs 上映年的偏差），也保留但排后面
                unknown.append(r)
            else:
                excluded.append(r)

        filtered = matched + unknown

        if excluded:
            excluded_notes = [r.get("note", "")[:40] for r in excluded[:3]]
            logger.info(
                f"【115助手】年份过滤: {len(results)} → {len(filtered)} "
                f"(匹配{len(matched)}, 未知{len(unknown)}, "
                f"排除{len(excluded)}: {excluded_notes})"
            )

        if matched:
            logger.info(
                f"【115助手】年份匹配 {expected_year}: "
                f"首选 {matched[0].get('note', '')[:50]}"
            )

        return filtered

    def _list_share_files_115(self, share_code: str, receive_code: str,
                              cookies: str, cid: str = "0", depth: int = 0) -> List[dict]:
        """直接调用 115 Web API 获取分享文件列表（递归进入文件夹）"""
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
                if self._looks_like_115_rate_limit(data):
                    self._mark_115_cooldown(f"share/snap cid={cid}: {data}")
                logger.info(f"【115助手】115 share/snap 返回 state=false, cid={cid}")
                return []

            share_data = data.get("data", {})
            file_list = share_data.get("list", [])

            files = []
            for f in file_list:
                has_fid = "fid" in f
                is_dir = not has_fid
                name = f.get("n", f.get("fn", ""))

                if is_dir:
                    folder_cid = str(f.get("cid", ""))
                    if folder_cid and depth < 2:
                        time.sleep(0.5)
                        logger.info(f"【115助手】进入文件夹: {name} (cid={folder_cid})")
                        sub_files = self._list_share_files_115(
                            share_code, receive_code, cookies,
                            cid=folder_cid, depth=depth + 1
                        )
                        files.extend(sub_files)
                    else:
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
            if self._looks_like_115_rate_limit(e):
                self._mark_115_cooldown(f"share/snap 异常: {e}")
            logger.warning(f"【115助手】获取 115 分享内容失败: {e}")
            return []

    def _save_share_115(self, share_code: str, receive_code: str,
                        cookies: str, file_ids: List[str], cid: str,
                        cookie_source: str = "插件Cookie") -> bool:
        """直接调用 115 Web API 转存"""
        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies

        try:
            with httpx.Client(headers=headers, follow_redirects=True, timeout=60.0) as client:
                batch_size = 5
                all_success = True
                for i in range(0, len(file_ids), batch_size):
                    batch = file_ids[i:i + batch_size]
                    if i > 0:
                        time.sleep(1)
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
                        if self._looks_like_115_rate_limit(result):
                            self._mark_115_cooldown(
                                f"share/receive errno={errno}, {error_msg}"
                            )
                        if str(errno) == "4200045":
                            logger.info(
                                f"【115助手】115 转存批次: 文件已接收 (跳过), "
                                f"share={share_code}, 文件数={len(batch)}"
                            )
                        else:
                            logger.warning(
                                f"【115助手】115 转存批次失败: errno={errno}, error={error_msg}, "
                                f"share={share_code}, 文件数={len(batch)}"
                            )
                            all_success = False
                            # 检测 Cookie 过期
                            if str(errno) in ("990001", "40140110") or \
                                    any(k in str(error_msg) for k in ("登录", "过期", "expired", "login")):
                                self._notify_token_expired(
                                    cookie_source or "插件Cookie",
                                    f"errno={errno}, {error_msg}"
                                )
                    else:
                        logger.info(f"【115助手】115 转存批次成功: {len(batch)} 个文件")

                return all_success

        except Exception as e:
            if self._looks_like_115_rate_limit(e):
                self._mark_115_cooldown(f"share/receive 异常: {e}")
            logger.error(f"【115助手】115 转存失败: {e}")
            return False

    # ================================================================
    #  115 临时目录清理
    # ================================================================

    def _list_own_files_115(self, cookies: str, cid: str) -> List[dict]:
        """列出自己 115 网盘某个目录下的文件（非分享目录），包含上传时间"""
        if not cid or cid == "0":
            return []

        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies

        try:
            with httpx.Client(headers=headers, follow_redirects=True, timeout=30.0) as client:
                resp = client.get(
                    "https://webapi.115.com/files",
                    params={
                        "cid": cid,
                        "limit": 500,
                        "offset": 0,
                        "show_dir": 1,
                        "o": "user_ptime",
                        "asc": 0,
                    },
                )
                data = resp.json()

            if not data.get("state"):
                logger.debug(f"【115助手】列目录失败 cid={cid}: {data.get('error', '')}")
                return []

            file_list = data.get("data", [])
            results = []
            for f in file_list:
                fid = str(f.get("fid", f.get("cid", "")))
                name = f.get("n", "")
                is_dir = "fid" not in f
                # te = upload time (epoch seconds), tp = parent time, t = formatted time
                upload_time = int(f.get("te", f.get("tp", 0)) or 0)
                results.append({
                    "fid": fid,
                    "name": name,
                    "is_dir": is_dir,
                    "size": int(f.get("s", f.get("size", 0)) or 0),
                    "upload_time": upload_time,
                })
            return results

        except Exception as e:
            logger.warning(f"【115助手】列目录异常 cid={cid}: {e}")
            return []

    def _delete_files_115(self, cookies: str, fids: List[str]) -> bool:
        """批量删除 115 网盘文件（移入回收站）"""
        if not fids:
            return True

        headers = dict(_115_HEADERS)
        headers["Cookie"] = cookies

        try:
            # 115 删除 API 接受 form-urlencoded: fid[0]=xxx&fid[1]=yyy
            form_data = {}
            for i, fid in enumerate(fids):
                form_data[f"fid[{i}]"] = fid

            with httpx.Client(headers=headers, follow_redirects=True, timeout=30.0) as client:
                resp = client.post(
                    "https://webapi.115.com/rb/delete",
                    data=form_data,
                )
                result = resp.json()
                if result.get("state"):
                    return True
                else:
                    logger.warning(f"【115助手】删除文件失败: {result.get('error', result)}")
                    return False

        except Exception as e:
            logger.warning(f"【115助手】删除文件异常: {e}")
            return False

    # 清理保留时间（秒）：只删除超过这个时间的文件，给 CloudDrive2/MP 足够时间同步处理
    _CLEANUP_MIN_AGE_SECONDS = 3 * 3600  # 3 小时

    def _cleanup_staging_folders(self, config_115: dict):
        """
        清理 115 云盘临时目录中的过期文件。
        只删除上传时间超过 3 小时的文件，给 CloudDrive2 + MP 足够的同步/刮削/转移时间。
        """
        cookies = config_115.get("cookies", "")
        if not cookies:
            return

        staging_cids = {}
        if config_115.get("movie_cid"):
            staging_cids["新电影"] = config_115["movie_cid"]
        if config_115.get("old_movie_cid"):
            staging_cids["老电影"] = config_115["old_movie_cid"]
        if config_115.get("ongoing_cid"):
            staging_cids["连载剧"] = config_115["ongoing_cid"]
        if config_115.get("archive_cid"):
            staging_cids["老剧"] = config_115["archive_cid"]

        if not staging_cids:
            return

        now_ts = int(time.time())
        total_deleted = 0
        total_kept = 0

        for label, cid in staging_cids.items():
            files = self._list_own_files_115(cookies, cid)
            if not files:
                continue

            # 只删除上传超过 _CLEANUP_MIN_AGE_SECONDS 的文件
            old_files = []
            new_files = []
            for f in files:
                age = now_ts - f.get("upload_time", 0)
                if f.get("upload_time", 0) == 0 or age >= self._CLEANUP_MIN_AGE_SECONDS:
                    old_files.append(f)
                else:
                    new_files.append(f)

            if new_files:
                logger.info(
                    f"【115助手】{label} 临时目录: 保留 {len(new_files)} 个新文件 "
                    f"(上传不足 {self._CLEANUP_MIN_AGE_SECONDS // 3600} 小时): "
                    f"{[f['name'] for f in new_files[:3]]}"
                )
                total_kept += len(new_files)

            if not old_files:
                continue

            fids = [f["fid"] for f in old_files]
            names = [f["name"] for f in old_files[:5]]
            logger.info(
                f"【115助手】清理 {label} 临时目录: 删除 {len(old_files)} 个过期文件 "
                f"({', '.join(names)}{'...' if len(old_files) > 5 else ''})"
            )

            # 分批删除，每批最多 50
            for i in range(0, len(fids), 50):
                batch = fids[i:i + 50]
                if self._delete_files_115(cookies, batch):
                    total_deleted += len(batch)
                time.sleep(0.5)

        if total_deleted > 0 or total_kept > 0:
            logger.info(
                f"【115助手】临时目录清理完成: 删除 {total_deleted} 个过期文件, "
                f"保留 {total_kept} 个新文件"
            )

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

    # ── 分辨率解析辅助 ──────────────────────────────────────

    _RES_PATTERNS = [
        (re.compile(r'(?i)(2160[pP]|4[kK]|UHD)'), "4K"),
        (re.compile(r'(?i)(1080[pPiI])'), "1080p"),
        (re.compile(r'(?i)(720[pPiI])'), "720p"),
    ]

    def _detect_resolution(self, filename: str) -> str:
        """从文件名检测分辨率等级: '4K' / '1080p' / '720p' / 'other'"""
        for pat, label in self._RES_PATTERNS:
            if pat.search(filename):
                return label
        return "other"

    # ── 分辨率分级（数字越大越优先）──
    _RES_TIER = {"4K": 3, "1080p": 2, "720p": 1, "other": 0}

    def _find_missing_episodes(self, parsed_files: List[dict],
                               target_season: Optional[int],
                               existing_episodes: List[int],
                               media_type_str: str = "电视剧",
                               total_episodes: Optional[int] = None) -> List[dict]:
        """
        筛选出缺失的集数对应的文件。

        去重策略（per episode）:
        - 同一集允许保留不同分辨率档（如 4K + 1080p 各保留一个）
        - 同分辨率档内只保留最优格式：MP4 > MKV > TS > 其他
        - 同分辨率同格式则保留体积更大的文件
        """
        is_movie = (media_type_str == "电影")
        missing = []
        existing_set = set(existing_episodes)
        try:
            episode_limit = int(total_episodes or 0)
        except (TypeError, ValueError):
            episode_limit = 0

        for f in parsed_files:
            file_season = f.get("season")
            file_episodes = f.get("episode_list", [])

            if target_season is not None and file_season != target_season:
                continue

            if file_episodes:
                over_limit = False
                if episode_limit:
                    for ep in file_episodes:
                        try:
                            if int(ep) > episode_limit:
                                over_limit = True
                                break
                        except (TypeError, ValueError):
                            continue
                if over_limit:
                    logger.info(
                        f"【115助手】跳过超过总集数上限的文件: "
                        f"name={f.get('name', '')}, episodes={file_episodes}, total={episode_limit}"
                    )
                    continue
                # 电视剧：只保留缺失集
                if any(ep not in existing_set for ep in file_episodes):
                    missing.append(f)
            elif f.get("is_dir"):
                missing.append(f)
            elif is_movie and not f.get("is_dir"):
                # 电影：没有集数信息的视频文件，仅在库里没有时才保留
                # existing_set 含哨兵 -1 表示电影已入库
                if -1 not in existing_set:
                    missing.append(f)

        # ── 同一集多版本去重 ──
        if missing:
            _format_priority = {".mp4": 3, ".mkv": 2, ".ts": 1}

            # key = (season, episode, resolution_tier) → (format_pri, size, file)
            ep_best: Dict[Any, Any] = {}

            for f in missing:
                ep = f.get("episode")

                # 没有集数的条目（文件夹 / 电影）直接保留
                if not ep:
                    ep_best.setdefault(f"_noep_{f.get('file_id')}", f)
                    continue

                name = f.get("name", "").lower()
                ext = ""
                if "." in name:
                    ext = "." + name.rsplit(".", 1)[-1]
                fmt_pri = _format_priority.get(ext, 0)
                size = f.get("size", 0)
                res = self._detect_resolution(f.get("name", ""))

                # 同一集 + 同分辨率 → 只保留最优
                key = (f.get("season"), ep, res)
                if key not in ep_best:
                    ep_best[key] = (fmt_pri, size, f)
                else:
                    old_pri, old_size, _ = ep_best[key]
                    if fmt_pri > old_pri or (fmt_pri == old_pri and size > old_size):
                        ep_best[key] = (fmt_pri, size, f)

            deduped = []
            for v in ep_best.values():
                if isinstance(v, tuple):
                    deduped.append(v[2])
                else:
                    deduped.append(v)

            if len(deduped) < len(missing):
                logger.info(
                    f"【115助手】去重: {len(missing)} → {len(deduped)} 个文件 "
                    f"(同集同分辨率只保留最优格式)"
                )
            missing = deduped

        return missing

    # ================================================================
    #  订阅数据管理
    # ================================================================

    def _load_subscriptions(self) -> List[dict]:
        """加载订阅列表"""
        data = self.get_data("subscriptions")
        if isinstance(data, list):
            return data
        if self._subscriptions:
            return self._subscriptions
        return []

    @staticmethod
    def _subscription_key(sub: dict) -> Tuple[Optional[int], Optional[int]]:
        """订阅唯一键：TMDB + 季。"""
        try:
            tmdb_id = int(sub.get("tmdb_id")) if sub.get("tmdb_id") not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        try:
            season = int(sub.get("season")) if sub.get("season") not in (None, "") else None
        except (TypeError, ValueError):
            season = None
        return tmdb_id, season

    def _archived_subscription_keys(self) -> set:
        """已归档订阅键，用于防止长任务把手动归档的订阅覆盖回 active。"""
        keys = set()
        for item in self._load_history():
            key = self._subscription_key(item)
            if key[0] is not None:
                keys.add(key)
        return keys

    def _filter_archived_subscriptions(self, subscriptions: List[dict]) -> List[dict]:
        archived_keys = self._archived_subscription_keys()
        if not archived_keys:
            return subscriptions
        filtered = []
        removed = []
        for sub in subscriptions:
            key = self._subscription_key(sub)
            if key[0] is not None and key in archived_keys:
                removed.append(sub.get("title", str(key[0])))
                continue
            filtered.append(sub)
        if removed:
            logger.info(f"【115助手】跳过已归档订阅，避免重新激活: {', '.join(removed)}")
        return filtered

    def _save_subscriptions(self, subscriptions: List[dict]):
        """保存订阅列表（调用前须持有 _subscriptions_lock）"""
        subscriptions = self._filter_archived_subscriptions(subscriptions)
        self._subscriptions = subscriptions
        self.save_data("subscriptions", subscriptions)

    @staticmethod
    def _subscription_identity_matches(current: dict, processed: dict) -> bool:
        """同一订阅键下，如 created 不同，说明运行期间被删后重加，不能用旧状态覆盖。"""
        current_created = current.get("created")
        processed_created = processed.get("created")
        return not current_created or not processed_created or current_created == processed_created

    def _merge_processed_subscriptions(
        self,
        latest_subscriptions: List[dict],
        processed_subscriptions: List[dict],
        completed_keys: Optional[set] = None,
        context: str = "订阅任务",
    ) -> List[dict]:
        """
        长任务结束时合并订阅状态，避免用任务开始时的旧列表覆盖运行期间新增的订阅。
        调用方需持有 _subscriptions_lock。
        """
        completed_keys = completed_keys or set()
        processed_by_key = {}
        for sub in processed_subscriptions or []:
            key = self._subscription_key(sub)
            if key[0] is not None:
                processed_by_key[key] = sub

        merged = []
        preserved_new = []
        skipped_removed_or_readded = []
        updated_count = 0

        for current in latest_subscriptions or []:
            key = self._subscription_key(current)
            if key[0] is not None and key in completed_keys:
                continue
            processed = processed_by_key.get(key)
            if processed and self._subscription_identity_matches(current, processed):
                merged.append(processed)
                updated_count += 1
            else:
                merged.append(current)
                if key[0] is not None and key not in processed_by_key:
                    preserved_new.append(current.get("title") or str(key[0]))
                elif processed:
                    skipped_removed_or_readded.append(current.get("title") or str(key[0]))

        latest_keys = {
            self._subscription_key(s)
            for s in latest_subscriptions or []
            if self._subscription_key(s)[0] is not None
        }
        for key, sub in processed_by_key.items():
            if key not in latest_keys and key not in completed_keys:
                skipped_removed_or_readded.append(sub.get("title") or str(key[0]))

        if preserved_new:
            logger.info(
                f"【115助手】{context}保存时保留运行期间新增订阅: "
                f"{', '.join(preserved_new)}"
            )
        if skipped_removed_or_readded:
            logger.info(
                f"【115助手】{context}保存时检测到已删除或重建订阅，不用旧状态覆盖: "
                f"{', '.join(skipped_removed_or_readded)}"
            )
        logger.info(
            f"【115助手】{context}合并保存订阅: 最新={len(latest_subscriptions or [])}, "
            f"处理更新={updated_count}, 保存={len(merged)}"
        )
        return merged

    def add_subscription(self, sub: dict) -> bool:
        """添加订阅（加锁，防止与 check_subscriptions 并发覆盖）"""
        with self._subscriptions_lock:
            key = self._subscription_key(sub)
            if key[0] is not None and key in self._archived_subscription_keys():
                # 归档不再阻断重新订阅：实际目录可能已删除，需要重新追更。
                # 自动解除归档（删除历史记录）后继续走正常添加流程。
                history = self._load_history()
                new_history = [
                    item for item in history
                    if self._subscription_key(item) != key
                ]
                if len(new_history) != len(history):
                    self._save_history(new_history)
                logger.info(
                    f"【115助手】订阅命中历史归档，自动解除归档并重新添加: "
                    f"TMDB={key[0]}, 季={key[1]}, 标题={sub.get('title', '')}"
                )
            subs = self._load_subscriptions()
            for s in subs:
                if s.get("tmdb_id") == sub.get("tmdb_id") and s.get("season") == sub.get("season"):
                    logger.info(
                        f"【115助手】订阅已存在，跳过新增: "
                        f"TMDB={sub.get('tmdb_id')}, 季={sub.get('season')}, 标题={sub.get('title', '')}"
                    )
                    return False
            sub["created"] = datetime.now().isoformat()
            subs.append(sub)
            self._save_subscriptions(subs)
            logger.info(
                f"【115助手】新增订阅已保存: TMDB={sub.get('tmdb_id')}, "
                f"季={sub.get('season')}, 标题={sub.get('title', '')}, active={len(subs)}"
            )
            return True

    def remove_subscription(self, tmdb_id: int, season: Optional[int] = None) -> bool:
        """删除订阅（加锁，防止并发竞争）"""
        with self._subscriptions_lock:
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

    def archive_subscription(self, tmdb_id: int, season: Optional[int] = None,
                             reason: str = "手动归档") -> List[dict]:
        """将订阅移入历史记录（加锁，防止并发覆盖）"""
        with self._subscriptions_lock:
            subs = self._load_subscriptions()
            remaining = []
            archived = []

            for sub in subs:
                if sub.get("tmdb_id") == tmdb_id and (season is None or sub.get("season") == season):
                    item = dict(sub)
                    item["status"] = "completed"
                    item["completed_time"] = datetime.now().isoformat()
                    item["completed_reason"] = reason or "手动归档"
                    item.pop("auto_completed", None)
                    archived.append(item)
                else:
                    remaining.append(sub)

            if not archived:
                return []

            history = self._load_history()
            history.extend(archived)
            self._save_history(history)
            self._save_subscriptions(remaining)
            logger.info(
                f"【115助手】手动归档订阅: TMDB={tmdb_id}, 季={season}, "
                f"数量={len(archived)}, 原因={reason}"
            )
            return archived

    def list_subscriptions(self) -> List[dict]:
        """列出所有订阅"""
        return self._load_subscriptions()

    # ================================================================
    #  历史记录管理
    # ================================================================

    def _load_history(self) -> List[dict]:
        """加载历史记录"""
        data = self.get_data("subscription_history")
        if isinstance(data, list):
            return data
        return []

    def _save_history(self, history: List[dict]):
        """保存历史记录（最多保留 100 条）"""
        history = history[-100:]
        self.save_data("subscription_history", history)

    def list_history(self) -> List[dict]:
        """列出历史订阅"""
        return self._load_history()

    def delete_history(self, tmdb_id: int, season: Optional[int] = None) -> int:
        """删除历史归档记录（加锁）。season 为 None 时删除该 TMDB 下所有季。返回删除条数。"""
        with self._subscriptions_lock:
            history = self._load_history()
            remaining = []
            removed = 0
            for item in history:
                if (item.get("tmdb_id") == tmdb_id
                        and (season is None or item.get("season") == season)):
                    removed += 1
                    continue
                remaining.append(item)
            if removed:
                self._save_history(remaining)
                logger.info(
                    f"【115助手】删除历史归档: TMDB={tmdb_id}, 季={season}, 数量={removed}"
                )
            return removed

    # ================================================================
    #  通知辅助
    # ================================================================

    def post_message(self, mtype: NotificationType, title: str, text: str, **kwargs):
        """发送 MP 通知；失败时降级为站内系统消息。"""
        try:
            return super().post_message(mtype=mtype, title=title, text=text, **kwargs)
        except Exception as e:
            logger.warning(f"【115助手】发送 MP 通知失败，降级为系统消息: {e}")
        try:
            self.systemmessage.put(title=title, message=text, mtype=mtype)
        except TypeError:
            # 某些版本的 MessageHelper 不支持 mtype 参数
            self.systemmessage.put(title=title, message=text)

    @staticmethod
    def _classify_cd2_watch_media(dest_path: str) -> Tuple[str, str]:
        category = PurePosixPath(str(dest_path or "/")).name or ""
        lower_dest = str(dest_path or "").lower()
        if "/movie" in lower_dest or "电影" in lower_dest:
            return "电影", category or "Movie"
        if "/variety" in lower_dest or "综艺" in lower_dest:
            return "电视剧", category or "Variety"
        return "电视剧", category or "TV"

    def _build_cd2_watch_notice_records(
        self,
        rule_id: str,
        rule_name: str,
        show_key: str,
        show_name: str,
        dest_path: str,
        media_type_str: str,
        category: str,
        video_items_by_season: Dict[int, List[dict]],
        tmdb_id: Optional[int] = None,
        total_episodes: Optional[int] = None,
    ) -> List[dict]:
        title = self._strip_year(show_name) or show_name
        year = self._extract_year(show_name)
        records: List[dict] = []

        if media_type_str == "电影":
            movie_files = []
            for _, files in sorted(video_items_by_season.items()):
                movie_files.extend(files)
            if not movie_files:
                return []
            records.append({
                "rule_id": rule_id,
                "rule_name": rule_name,
                "show_key": show_key,
                "raw_name": show_name,
                "title": title,
                "year": year,
                "tmdb_id": tmdb_id,
                "total_episodes": total_episodes,
                "media_type": media_type_str,
                "category": category,
                "season": None,
                "episodes": [],
                "source_files": movie_files,
                "target_path": movie_files[0].get("path"),
            })
            return records

        for season, files in sorted(video_items_by_season.items()):
            if not files:
                continue
            episodes = sorted({
                int(item.get("episode"))
                for item in files
                if item.get("episode") is not None
            })
            records.append({
                "rule_id": rule_id,
                "rule_name": rule_name,
                "show_key": show_key,
                "raw_name": show_name,
                "title": title,
                "year": year,
                "tmdb_id": tmdb_id,
                "total_episodes": total_episodes,
                "media_type": media_type_str,
                "category": category,
                "season": int(season or 1),
                "episodes": episodes,
                "source_files": files,
                "target_path": files[0].get("path"),
            })
        return records

    def _build_cd2_watch_refresh_items(self, notices: List[dict]) -> List[Any]:
        if not notices:
            return []
        try:
            from app.schemas.mediaserver import RefreshMediaItem
        except Exception:
            try:
                from app.schemas import RefreshMediaItem
            except Exception as e:
                logger.warning(f"【CD2巡查】导入 RefreshMediaItem 失败，跳过媒体库局部刷新: {e}")
                return []

        items = []
        seen = set()
        for notice in notices:
            target_path = notice.get("target_path")
            title = str(notice.get("title") or "").strip()
            if not target_path or not title:
                continue
            key = f"{title}|{notice.get('year')}|{target_path}"
            if key in seen:
                continue
            seen.add(key)
            items.append(
                RefreshMediaItem(
                    title=title,
                    year=notice.get("year"),
                    type=MediaType.MOVIE if notice.get("media_type") == "电影" else MediaType.TV,
                    category=notice.get("category"),
                    target_path=Path(str(target_path)),
                )
            )
        return items

    def _refresh_cd2_watch_mediaservers(self, notices: List[dict]) -> List[str]:
        items = self._build_cd2_watch_refresh_items(notices)
        if not items:
            return []
        try:
            from app.helper.mediaserver import MediaServerHelper
        except Exception as e:
            logger.warning(f"【CD2巡查】导入 MediaServerHelper 失败，跳过媒体库局部刷新: {e}")
            return []

        services = MediaServerHelper().get_services() or {}
        if not services:
            logger.info("【CD2巡查】未配置可用媒体服务器，跳过媒体库局部刷新")
            return []

        refreshed = []
        for name, service in services.items():
            instance = getattr(service, "instance", None)
            if not instance:
                continue
            try:
                if hasattr(instance, "is_inactive") and instance.is_inactive():
                    logger.info(f"【CD2巡查】媒体服务器 {name} 当前未连接，跳过局部刷新")
                    continue
            except Exception:
                pass

            if not hasattr(instance, "refresh_library_by_items"):
                logger.info(f"【CD2巡查】媒体服务器 {name} 不支持局部刷新，跳过全库刷新")
                continue

            try:
                instance.refresh_library_by_items(items)
                refreshed.append(name)
            except Exception as e:
                logger.warning(f"【CD2巡查】媒体服务器 {name} 局部刷新失败: {e}")
        return refreshed

    @staticmethod
    def _build_cd2_watch_subscription(record: dict) -> dict:
        return {
            "title": str(record.get("title") or record.get("raw_name") or "").strip(),
            "tmdb_id": record.get("tmdb_id"),
            "media_type": record.get("media_type") or "电视剧",
            "season": record.get("season") or 1,
            "year": record.get("year"),
        }

    @staticmethod
    def _cd2_watch_notice_stable_key(record: dict) -> str:
        """同一剧集的 CD2 巡查通知使用稳定键，避免按复制时间重复排队。"""
        tmdb_id = record.get("tmdb_id")
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        subject = (
            f"tmdb:{tmdb_id}"
            if tmdb_id
            else str(record.get("show_key") or record.get("raw_name") or record.get("title") or "").strip()
        )
        media_type_str = str(record.get("media_type") or "电视剧")
        category = str(record.get("category") or "").strip()
        season = record.get("season")
        target_path = str(record.get("target_path") or "")
        target_parent = str(PurePosixPath(target_path).parent) if target_path else ""
        episodes = sorted({int(ep) for ep in (record.get("episodes") or []) if ep is not None})
        episodes_key = ",".join(str(ep) for ep in episodes) if episodes else "movie"
        return "|".join([
            subject,
            media_type_str,
            category,
            str(season or 0),
            episodes_key,
            target_parent,
        ])

    def _mark_cd2_watch_notice_confirmed(self, record: dict) -> None:
        rule_id = str(record.get("rule_id") or "")
        show_key = str(record.get("show_key") or "")
        if not rule_id or not show_key:
            return
        index = self._load_cd2_watch_index()
        rule_state = index.get("rules", {}).get(rule_id)
        if not isinstance(rule_state, dict):
            return
        show_state = rule_state.get("shows", {}).get(show_key)
        if not isinstance(show_state, dict):
            return

        media_type_str = str(record.get("media_type") or "电视剧")
        confirmed = self._normalize_episode_map(show_state.get("confirmed_episodes_by_season"))
        if media_type_str == "电影":
            confirmed = {1: [-1]}
        else:
            season_num = int(record.get("season") or 1)
            episodes = sorted({
                int(ep)
                for ep in (record.get("episodes") or [])
                if ep is not None
            })
            if episodes:
                confirmed[season_num] = sorted(set(confirmed.get(season_num, [])) | set(episodes))

        show_state["confirmed_episodes_by_season"] = self._normalize_episode_map(confirmed)
        show_state["confirmed_updated_at"] = datetime.now().isoformat()
        pending = self._normalize_episode_map(show_state.get("pending_episodes_by_season"))
        if pending:
            self._set_cd2_watch_pending_map(show_state, self._episode_map_difference(pending, confirmed))
        self._update_cd2_watch_completion_state(index, rule_state, show_state)
        self._save_cd2_watch_index(index)

    def _is_cd2_watch_notice_confirmed(self, record: dict) -> bool:
        subscription = self._build_cd2_watch_subscription(record)
        episodes = sorted({int(ep) for ep in (record.get("episodes") or []) if ep is not None})
        source_files = record.get("source_files") or []
        sample_name = str(
            (source_files[0].get("name") if source_files else "")
            or record.get("raw_name")
            or subscription.get("title")
            or ""
        )
        _, mediainfo = self._build_notification_media_context(subscription, episodes, sample_name)
        if not mediainfo:
            return False

        tmdb_id = getattr(mediainfo, "tmdb_id", None)
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        if tmdb_id:
            record["tmdb_id"] = tmdb_id

        media_type_str = subscription.get("media_type", "电视剧")
        season = int(subscription.get("season") or 1)
        existing_eps = set(
            self._get_existing_episodes(
                title=subscription.get("title") or "",
                tmdb_id=record.get("tmdb_id"),
                media_type_str=media_type_str,
                season=season,
            )
        )
        if media_type_str == "电影":
            return -1 in existing_eps
        if not episodes:
            return bool(existing_eps)
        return set(episodes).issubset(existing_eps)

    def _is_cd2_watch_notice_copy_confirmed(
        self,
        record: dict,
        min_age_seconds: int = 90,
        min_attempts: int = 2,
    ) -> bool:
        """
        CD2 复制完成后媒体服务器可能已扫到文件，但 MP 的 mediaserveritem
        索引不会立刻同步。避免通知长期卡住，几轮媒体库确认失败后以复制完成兜底。
        """
        copy_completed_at = self._parse_iso_datetime(record.get("copy_completed_at"))
        if not copy_completed_at:
            return False
        try:
            attempts = int(record.get("confirm_attempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        age = (datetime.now() - copy_completed_at).total_seconds()
        if attempts < min_attempts and age < min_age_seconds:
            return False
        title = record.get("title") or record.get("raw_name") or ""
        logger.info(
            f"【CD2巡查】{title} 媒体库索引暂未同步，"
            f"已按复制完成兜底确认入库: attempts={attempts}, age={int(age)}s"
        )
        return True

    def _send_cd2_watch_notice_group(self, records: List[dict]) -> str:
        """发送一部作品的聚合通知，返回 sent/skipped/deferred。"""
        filtered_records = []
        for record in records or []:
            subscription = self._build_cd2_watch_subscription(record)
            media_type_str = subscription.get("media_type", "电视剧")
            episodes = sorted({
                int(ep) for ep in (record.get("episodes") or []) if ep is not None
            })
            if media_type_str != "电影":
                notice_episodes, skipped_episodes = self._filter_plugin_notice_episodes(
                    subscription, episodes
                )
                if skipped_episodes:
                    logger.info(
                        f"【CD2巡查】{subscription.get('title')} "
                        "已有整理历史或插件通知账本，跳过插件补通知: "
                        f"{self._format_episode_preview(skipped_episodes)}"
                    )
                if episodes and not notice_episodes:
                    continue
                episodes = notice_episodes
            filtered = dict(record)
            filtered["episodes"] = episodes
            filtered_records.append(filtered)

        if not filtered_records:
            return "skipped"

        subscription = self._build_cd2_watch_subscription(filtered_records[0])
        allowed, rate_reason, retry_after = self._check_plugin_notice_rate_limit(subscription)
        if not allowed:
            reason_text = "同剧限频" if rate_reason == "same_title" else "全局限频"
            logger.warning(
                f"【CD2巡查】{subscription.get('title')} 入库通知触发{reason_text}，"
                f"约 {retry_after}s 后重试；通知记录继续保留，不会丢失"
            )
            return "deferred"

        media_type_str = subscription.get("media_type", "电视剧")
        source_files = [
            item
            for record in filtered_records
            for item in (record.get("source_files") or [])
        ]
        all_episodes = sorted({
            int(ep)
            for record in filtered_records
            for ep in (record.get("episodes") or [])
            if ep is not None
        })
        seasons = sorted({
            int(record.get("season") or 1)
            for record in filtered_records
            if record.get("media_type") != "电影"
        })
        season_episode_override = (
            self._format_multi_season_notice(filtered_records)
            if len(seasons) > 1
            else None
        )

        if media_type_str != "电影" and not all_episodes:
            title = str(subscription.get("title") or filtered_records[0].get("raw_name") or "").strip()
            self.post_message(
                NotificationType.Organize,
                f"{title} 已入库",
                "CD2巡查确认入库",
            )
            sent = True
        else:
            sent = self._post_organize_success_template_message(
                subscription=subscription,
                episodes=all_episodes,
                source_files=source_files,
                reason="CD2巡查确认入库",
                source_label="CD2巡查入库",
                season_episode_override=season_episode_override,
                mark_notice=False,
            )
        if not sent:
            return "skipped"

        self._record_plugin_notice_rate_event(subscription)
        for record in filtered_records:
            self._mark_plugin_notice_sent(
                self._build_cd2_watch_subscription(record),
                record.get("episodes") or [],
            )
        logger.info(
            f"【CD2巡查】{subscription.get('title')} 聚合发送 1 条入库通知: "
            f"{season_episode_override or self._format_episode_preview(all_episodes)}，"
            f"合并记录 {len(filtered_records)} 条"
        )
        return "sent"

    def _send_cd2_watch_notice_record(self, record: dict) -> bool:
        """兼容单条调用；真实发送统一经过作品级聚合与限频。"""
        return self._send_cd2_watch_notice_group([record]) != "deferred"

    def _confirm_cd2_watch_pending_notices(
        self,
        reason: str = "",
        initial_delay: int = 20,
        rounds: int = 3,
        sleep_seconds: int = 60,
    ) -> None:
        lock = self._get_cd2_watch_notice_lock()
        if not lock.acquire(blocking=False):
            logger.info(f"【CD2巡查】入库确认任务已在运行，跳过触发: {reason}")
            return
        try:
            if initial_delay > 0:
                time.sleep(initial_delay)

            for round_index in range(max(int(rounds or 1), 1)):
                notices = self._load_cd2_watch_pending_notices()
                if not notices:
                    return

                remaining = []
                ready_notices = []
                confirmed_records = 0
                sent_groups = 0
                for notice in notices:
                    try:
                        if (
                            self._is_cd2_watch_notice_confirmed(notice)
                            or self._is_cd2_watch_notice_copy_confirmed(notice)
                        ):
                            self._mark_cd2_watch_notice_confirmed(notice)
                            ready_notices.append(notice)
                        else:
                            notice["confirm_attempts"] = int(notice.get("confirm_attempts") or 0) + 1
                            notice.pop("notification_rate_deferred", None)
                            remaining.append(notice)
                    except Exception as e:
                        logger.warning(f"【CD2巡查】确认入库通知失败 {notice.get('title')}: {e}")
                        notice["confirm_attempts"] = int(notice.get("confirm_attempts") or 0) + 1
                        remaining.append(notice)

                for group in self._group_cd2_watch_notice_records(ready_notices):
                    try:
                        result = self._send_cd2_watch_notice_group(group)
                        if result == "deferred":
                            for notice in group:
                                notice["notification_rate_deferred"] = True
                                remaining.append(notice)
                            continue
                        confirmed_records += len(group)
                        if result == "sent":
                            sent_groups += 1
                    except Exception as e:
                        title = group[0].get("title") if group else "未知作品"
                        logger.warning(f"【CD2巡查】聚合发送入库通知失败 {title}: {e}")
                        remaining.extend(group)

                self._save_cd2_watch_pending_notices(remaining)
                if confirmed_records:
                    logger.info(
                        f"【CD2巡查】本轮确认 {confirmed_records} 条入库记录，"
                        f"按作品聚合发送 {sent_groups} 条通知"
                    )
                if not remaining or round_index >= max(int(rounds or 1), 1) - 1:
                    return

                refresh_remaining = [
                    notice for notice in remaining
                    if not notice.get("notification_rate_deferred")
                ]
                if refresh_remaining:
                    self._refresh_cd2_watch_mediaservers(refresh_remaining)
                time.sleep(sleep_seconds)
        finally:
            lock.release()

    def _start_cd2_watch_notice_background(
        self,
        reason: str = "",
        initial_delay: int = 20,
        rounds: int = 3,
        sleep_seconds: int = 60,
    ) -> bool:
        lock = self._get_cd2_watch_notice_lock()
        if lock.locked():
            logger.info(f"【CD2巡查】入库确认后台任务已在运行，跳过重复触发: {reason}")
            return False
        _threading_mod.Thread(
            target=self._confirm_cd2_watch_pending_notices,
            kwargs={
                "reason": reason,
                "initial_delay": initial_delay,
                "rounds": rounds,
                "sleep_seconds": sleep_seconds,
            },
            daemon=True,
        ).start()
        return True

    def _run_cd2_watch_notice_compensation(self) -> None:
        if not self._load_cd2_watch_pending_notices():
            return
        self._start_cd2_watch_notice_background(
            reason="CD2巡查入库通知补偿确认",
            initial_delay=0,
            rounds=1,
            sleep_seconds=60,
        )

    # ================================================================
    #  CD2 巡查同步
    # ================================================================

    # 集号解析正则：兼容 S01E04 / 第04集 / EP04 / E04 / 01x
    _EP_PATTERN = re.compile(
        r'[Ss]\d{1,2}[Ee](\d{1,3})'           # S01E04 → group(1)
        r'|第\s*(\d{1,4})\s*[集话話]'            # 第04集  → group(2)
        r'|[Ee][Pp]?\.?(\d{1,3})(?!\d)'        # EP04/E04 → group(3)
        r'|(?:^|[\\/._\-\s])0?(\d{1,3})[xX](?=[._\-\s]|$)'  # 01x.mkv → group(4)
    )
    _SEASON_PATTERN = re.compile(
        r'(?:^|[\\/._\-\s])(?:[Ss](?:eason)?\.?\s*0?(\d{1,2})(?!\d)|第\s*0?(\d{1,2})\s*季)',
        re.IGNORECASE,
    )
    _COMPANION_EXTENSIONS = {
        ".nfo", ".jpg", ".jpeg", ".png", ".webp", ".ass", ".ssa", ".srt", ".sup",
        ".idx", ".sub",
    }

    def _parse_episode_number(self, filename: str) -> Optional[int]:
        """从文件名解析集号，返回 int 或 None"""
        m = self._EP_PATTERN.search(filename)
        if not m:
            return None
        for g in m.groups():
            if g is not None:
                return int(g)
        return None

    def _parse_season_number(self, text: str) -> Optional[int]:
        """从路径或文件名中解析季号，解析不到返回 None。"""
        if not text:
            return None
        matches = list(self._SEASON_PATTERN.finditer(str(text)))
        if not matches:
            return None
        match = matches[-1]
        for group in match.groups():
            if group is not None:
                try:
                    return int(group)
                except ValueError:
                    return None
        return None

    def _infer_file_season(self, file_item: dict, default: int = 1) -> int:
        """从文件路径/文件名推断季号，默认第 1 季。"""
        season = self._parse_season_number(file_item.get("path", ""))
        if season is None:
            season = self._parse_season_number(file_item.get("name", ""))
        return season or default

    @staticmethod
    def _season_dir_name(season: int) -> str:
        return f"Season {int(season)}"

    def _find_or_create_season_dir(self, cd2, dest_show_path: str, season: int) -> str:
        """在目标剧目录下查找季目录；不存在时创建 Season 1 这类目录。"""
        season = int(season or 1)
        try:
            items = cd2.list_dir(dest_show_path, force_refresh=True)
        except Exception:
            items = []

        for item in items:
            if not item.get("is_dir"):
                continue
            item_season = self._parse_season_number(item.get("name", ""))
            if item_season == season:
                return item["path"]

        folder_name = self._season_dir_name(season)
        if cd2.create_folder(dest_show_path, folder_name):
            logger.info(f"【CD2巡查】创建季目录: {dest_show_path}/{folder_name}")
        return f"{dest_show_path}/{folder_name}"

    def _extract_chinese_title(self, name: str) -> str:
        """提取目录名中的中文部分，用于模糊匹配剧名"""
        # 去掉括号内容（年份等）
        name = re.sub(r'[\(\[].*?[\)\]]', '', name)
        # 提取所有中文字符
        chinese = re.sub(r'[^\u4e00-\u9fff]', '', name)
        if chinese:
            return chinese
        # 没有中文时取英文首词序列（去掉数字和特殊字符）
        words = re.sub(r'[^a-zA-Z\s]', ' ', name).split()
        return ' '.join(words[:3]).lower()

    def _find_matching_show_dir_from_items(self, dest_items: List[dict], show_name: str) -> Optional[str]:
        src_key = self._extract_chinese_title(show_name)
        if not src_key:
            return None

        best_path = None
        best_score = 0
        for item in dest_items:
            if not item.get("is_dir"):
                continue
            dest_key = self._extract_chinese_title(item["name"])
            if not dest_key:
                continue
            # 计算匹配分：子串包含得分
            if src_key in dest_key or dest_key in src_key:
                score = min(len(src_key), len(dest_key))
                if score > best_score:
                    best_score = score
                    best_path = item["path"]

        return best_path

    def _find_matching_show_dir(
        self,
        cd2,
        dest_path: str,
        show_name: str,
        dest_items: Optional[List[dict]] = None,
    ) -> Optional[str]:
        """
        在 dest_path 下找与 show_name 最匹配的子目录。
        匹配策略：中文子串匹配，无中文时英文词匹配。
        返回匹配目录的完整路径，未找到返回 None。
        """
        if dest_items is None:
            try:
                dest_items = cd2.list_dir(dest_path, force_refresh=True)
            except Exception as e:
                logger.warning(f"【CD2巡查】列出目标目录失败 {dest_path}: {e}")
                return None
        return self._find_matching_show_dir_from_items(dest_items, show_name)

    def _get_dest_episodes_by_season(self, cd2, dest_show_path: str) -> Dict[int, set]:
        """递归扫描目标剧目录，返回 {季号: 已有集号集合}。"""
        episodes: Dict[int, set] = {}

        def _walk(path: str, current_season: int = 1, is_show_root: bool = False):
            try:
                items = cd2.list_dir(path, force_refresh=True)
            except Exception:
                return

            root_has_season_dir = is_show_root and any(
                item.get("is_dir") and self._parse_season_number(item.get("name", ""))
                for item in items
            )
            for item in items:
                item_season = self._parse_season_number(item.get("name", "")) or current_season
                if item.get("is_dir"):
                    _walk(item["path"], item_season, False)
                    continue

                if root_has_season_dir:
                    # 已有规范季目录时，不把剧根目录的误放文件当成已入库集数。
                    # 这样历史上复制错位置的文件不会阻止后续补到 Season 1。
                    continue
                ext = os.path.splitext(item.get("name", ""))[1].lower()
                if ext not in self._video_extensions:
                    continue
                ep = self._parse_episode_number(item.get("name", ""))
                if ep is not None:
                    episodes.setdefault(item_season or 1, set()).add(ep)

        _walk(dest_show_path, 1, True)
        return episodes

    def _get_dest_episodes(self, cd2, dest_show_path: str) -> set:
        """
        递归扫描目标剧目录，解析已有集号集合。
        跳过 nfo/jpg 等非视频文件。
        """
        eps = set()
        for season_eps in self._get_dest_episodes_by_season(cd2, dest_show_path).values():
            eps |= season_eps
        return eps

    def _build_source_episode_map(self, src_files_all: List[dict]) -> Tuple[List[dict], Dict[int, List[int]]]:
        video_files = []
        episodes_by_season: Dict[int, set] = {}
        for item in src_files_all:
            if item.get("is_dir"):
                continue
            ext = os.path.splitext(item.get("name", ""))[1].lower()
            if ext not in self._video_extensions:
                continue
            video_files.append(item)
            ep = self._parse_episode_number(item.get("name", ""))
            if ep is None:
                continue
            season = self._infer_file_season(item)
            episodes_by_season.setdefault(int(season or 1), set()).add(int(ep))
        return video_files, {
            int(season): sorted(values)
            for season, values in episodes_by_season.items()
            if values
        }

    def _collect_cd2_missing_files(
        self,
        src_files_all: List[dict],
        missing_eps_by_season: Dict[int, List[int]],
    ) -> Tuple[List[dict], List[dict]]:
        missing_map = self._normalize_episode_map(missing_eps_by_season)
        missing_videos = []
        missing_all = []
        for item in src_files_all:
            if item.get("is_dir"):
                continue
            ext = os.path.splitext(item.get("name", ""))[1].lower()
            ep = self._parse_episode_number(item.get("name", ""))
            if ep is None:
                continue
            season = self._infer_file_season(item)
            if ep not in set(missing_map.get(season, [])):
                continue
            if ext in self._video_extensions:
                missing_videos.append(item)
                missing_all.append(item)
            elif ext in self._COMPANION_EXTENSIONS:
                missing_all.append(item)
        return missing_videos, missing_all

    def _sync_cd2_rule(self, rule: dict, cd2, index: Optional[dict] = None,
                       reconcile_target: bool = False) -> dict:
        """
        执行单条巡查规则：
        - 递归读取源盘剧集目录
        - 默认优先比较“本地巡查索引 + Plex/Emby/整理历史”判断缺失
        - 仅在首次见到剧集或手动强制重建时，读取 115 目标目录做 bootstrap
        - 复制真正缺失的剧集到 115 目标路径
        """
        source_path = rule.get("source_path", "").rstrip("/")
        dest_path = rule.get("dest_115_path", "").rstrip("/")
        rule_name = rule.get("name", source_path)
        rule_id = self._cd2_watch_rule_key(rule)
        media_type_str, category = self._classify_cd2_watch_media(dest_path)
        index = index or self._load_cd2_watch_index()
        rule_state = self._get_cd2_watch_rule_state(index, rule)
        show_states = rule_state.setdefault("shows", {})

        if not source_path or not dest_path:
            logger.warning(f"【CD2巡查】规则 {rule_name!r} 缺少路径配置，跳过")
            return {"submitted_count": 0, "notice_records": []}

        logger.info(f"【CD2巡查】开始巡查规则: {rule_name!r} | {source_path} → {dest_path}")

        try:
            src_shows = cd2.list_dir(source_path, force_refresh=True)
        except Exception as e:
            logger.error(f"【CD2巡查】列出源目录失败 {source_path}: {e}")
            return {"submitted_count": 0, "notice_records": []}

        src_show_dirs = [f for f in src_shows if f.get("is_dir")]
        now_dt = datetime.now()
        now_iso = now_dt.isoformat()
        if not src_show_dirs:
            logger.info(f"【CD2巡查】源目录为空: {source_path}")
            rule_state["last_empty_at"] = now_iso
            self._save_cd2_watch_index(index)
            return {"submitted_count": 0, "notice_records": []}

        total_copied = 0
        notice_records: List[dict] = []
        active_show_keys = set()
        dest_items_cache: Optional[List[dict]] = None

        for show in src_show_dirs:
            show_name = show["name"]
            show_path = show["path"]
            show_key = self._cd2_watch_show_key(show_name, media_type_str)
            active_show_keys.add(show_key)

            show_state = show_states.get(show_key)
            if not isinstance(show_state, dict):
                show_state = {}
            show_states[show_key] = show_state

            show_state["show_key"] = show_key
            show_state["show_name"] = show_name
            show_state["source_path"] = show_path
            show_state["dest_path"] = dest_path
            show_state["rule_id"] = rule_id
            show_state["rule_name"] = rule_name
            show_state["media_type"] = media_type_str
            show_state["category"] = category
            show_state["last_seen_at"] = now_iso

            try:
                src_files_all = self._list_recursive(cd2, show_path, force_refresh=True)
            except Exception as e:
                logger.warning(f"【CD2巡查】列出源剧目录失败 {show_path}: {e}")
                continue

            src_video_files, source_eps_by_season = self._build_source_episode_map(src_files_all)
            if not src_video_files:
                continue

            old_source_eps = self._normalize_episode_map(show_state.get("source_episodes_by_season"))
            source_changed = source_eps_by_season != old_source_eps
            show_state["source_episodes_by_season"] = source_eps_by_season
            show_state["source_count"] = (
                len(src_video_files)
                if media_type_str == "电影"
                else self._episode_map_count(source_eps_by_season)
            )
            show_state["source_updated_at"] = now_iso
            if source_changed:
                show_state["last_source_change_at"] = now_iso

            needs_meta = (
                source_changed
                or not show_state.get("meta_updated_at")
                or not show_state.get("tmdb_id")
                or (media_type_str != "电影" and not show_state.get("total_episodes"))
            )
            if needs_meta:
                self._hydrate_cd2_watch_show_meta(show_state, show_name, media_type_str)

            fresh_pending_map, stale_pending_map, pending_age = self._split_cd2_watch_pending_map(show_state)
            if stale_pending_map:
                stale_preview = sorted({ep for eps in stale_pending_map.values() for ep in eps})
                logger.info(
                    f"【CD2巡查】{show_name!r} 待确认副本已超时({pending_age}s)，允许重新后补: "
                    f"{self._format_episode_preview(stale_preview)}"
                )

            seasons = sorted(source_eps_by_season.keys()) or [int(show_state.get("season") or 1)]
            if source_changed or stale_pending_map or not show_state.get("confirmed_updated_at"):
                confirmed_map = self._refresh_cd2_watch_confirmed_map(
                    show_state=show_state,
                    show_name=show_name,
                    media_type_str=media_type_str,
                    seasons=seasons,
                )
            else:
                confirmed_map = self._normalize_episode_map(show_state.get("confirmed_episodes_by_season"))

            # 电影：更保守，存在 fresh pending 或已确认入库时直接跳过。
            if media_type_str == "电影":
                if -1 in set(confirmed_map.get(1, [])) or fresh_pending_map.get(1):
                    self._update_cd2_watch_completion_state(index, rule_state, show_state)
                    continue

                dest_show_path = str(show_state.get("dest_show_path") or "")
                if not dest_show_path:
                    if dest_items_cache is None:
                        try:
                            dest_items_cache = cd2.list_dir(dest_path, force_refresh=True)
                        except Exception as e:
                            logger.warning(f"【CD2巡查】列出目标目录失败 {dest_path}: {e}")
                            dest_items_cache = []
                    dest_show_path = (
                        self._find_matching_show_dir_from_items(dest_items_cache, show_name)
                        or ""
                    )
                    if dest_show_path:
                        show_state["dest_show_path"] = dest_show_path

                if dest_show_path and (reconcile_target or not show_state.get("bootstrapped_target")):
                    try:
                        dest_movie_files = self._list_recursive(cd2, dest_show_path, force_refresh=True)
                    except Exception as e:
                        logger.warning(f"【CD2巡查】列出目标电影目录失败 {dest_show_path}: {e}")
                        dest_movie_files = []
                    dest_has_video = any(
                        not item.get("is_dir")
                        and os.path.splitext(item.get("name", ""))[1].lower() in self._video_extensions
                        for item in dest_movie_files
                    )
                    show_state["bootstrapped_target"] = True
                    show_state["last_target_bootstrap_at"] = now_iso
                    if dest_has_video:
                        self._set_cd2_watch_pending_map(show_state, {1: [-1]}, now_dt)
                        self._update_cd2_watch_completion_state(index, rule_state, show_state)
                        logger.info(f"【CD2巡查】{show_name!r} 目标目录已有视频，先等待媒体库确认")
                        continue

                video_items_by_season: Dict[int, List[dict]] = {1: []}
                missing_by_dest: Dict[str, List[str]] = {}
                if dest_show_path:
                    movie_file_paths = []
                    for file_item in src_files_all:
                        if file_item.get("is_dir"):
                            continue
                        ext = os.path.splitext(file_item["name"])[1].lower()
                        if ext not in self._video_extensions and ext not in self._COMPANION_EXTENSIONS:
                            continue
                        movie_file_paths.append(file_item["path"])
                        if ext in self._video_extensions:
                            video_items_by_season[1].append({
                                "name": file_item["name"],
                                "path": str(PurePosixPath(dest_show_path) / file_item["name"]),
                                "size": int(file_item.get("size", 0) or 0),
                                "episode": None,
                            })
                    if not movie_file_paths:
                        self._update_cd2_watch_completion_state(index, rule_state, show_state)
                        continue
                    missing_by_dest = {dest_show_path: movie_file_paths}
                else:
                    target_show_path = str(PurePosixPath(dest_path) / show_name)
                    show_state["dest_show_path"] = target_show_path
                    movie_file_paths = []
                    for file_item in src_files_all:
                        if file_item.get("is_dir"):
                            continue
                        ext = os.path.splitext(file_item.get("name", ""))[1].lower()
                        if ext in self._ARCHIVE_EXTENSIONS:
                            logger.info(
                                f"【CD2巡查】{show_name!r} 跳过压缩包/镜像文件: "
                                f"{file_item.get('name', '')}"
                            )
                            continue
                        if ext not in self._video_extensions and ext not in self._COMPANION_EXTENSIONS:
                            continue
                        movie_file_paths.append(file_item["path"])
                        if ext not in self._video_extensions:
                            continue
                        src_file_path = PurePosixPath(file_item["path"])
                        try:
                            rel_path = src_file_path.relative_to(PurePosixPath(show_path))
                        except Exception:
                            rel_path = PurePosixPath(file_item["name"])
                        video_items_by_season[1].append({
                            "name": file_item["name"],
                            "path": str(PurePosixPath(target_show_path) / rel_path),
                            "size": int(file_item.get("size", 0) or 0),
                            "episode": None,
                        })
                    if not movie_file_paths:
                        self._update_cd2_watch_completion_state(index, rule_state, show_state)
                        logger.info(f"【CD2巡查】{show_name!r} 没有可复制的电影媒体文件，跳过")
                        continue
                    if cd2.create_folder(dest_path, show_name):
                        logger.info(f"【CD2巡查】{show_name!r} 目标无对应目录，已创建电影目录")
                    else:
                        logger.warning(f"【CD2巡查】{show_name!r} 创建电影目录失败，跳过复制")
                        continue
                    missing_by_dest = {target_show_path: movie_file_paths}
                    logger.info(
                        f"【CD2巡查】{show_name!r} 目标无对应目录，"
                        f"只复制视频和伴随文件共 {len(movie_file_paths)} 项"
                    )

                show_notice_records = self._build_cd2_watch_notice_records(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    show_key=show_key,
                    show_name=show_name,
                    dest_path=dest_path,
                    media_type_str=media_type_str,
                    category=category,
                    video_items_by_season=video_items_by_season,
                    tmdb_id=show_state.get("tmdb_id"),
                    total_episodes=show_state.get("total_episodes"),
                )
            else:
                if not source_changed and not stale_pending_map:
                    self._update_cd2_watch_completion_state(index, rule_state, show_state)
                    continue

                dest_show_path = str(show_state.get("dest_show_path") or "")
                if not dest_show_path:
                    if dest_items_cache is None:
                        try:
                            dest_items_cache = cd2.list_dir(dest_path, force_refresh=True)
                        except Exception as e:
                            logger.warning(f"【CD2巡查】列出目标目录失败 {dest_path}: {e}")
                            dest_items_cache = []
                    dest_show_path = (
                        self._find_matching_show_dir_from_items(dest_items_cache, show_name)
                        or ""
                    )
                    if dest_show_path:
                        show_state["dest_show_path"] = dest_show_path

                if dest_show_path and (reconcile_target or not show_state.get("bootstrapped_target")):
                    bootstrap_map = self._get_dest_episodes_by_season(cd2, dest_show_path)
                    bootstrap_pending = self._episode_map_difference(bootstrap_map, confirmed_map)
                    merged_pending = self._episode_map_union(fresh_pending_map, bootstrap_pending)
                    self._set_cd2_watch_pending_map(show_state, merged_pending, now_dt)
                    fresh_pending_map, _, _ = self._split_cd2_watch_pending_map(show_state)
                    show_state["bootstrapped_target"] = True
                    show_state["last_target_bootstrap_at"] = now_iso
                    if bootstrap_pending:
                        logger.info(
                            f"【CD2巡查】{show_name!r} 首次同步读取到 115 目标已有集数: "
                            f"{ {season: eps for season, eps in bootstrap_pending.items()} }"
                        )
                elif not dest_show_path and not show_state.get("bootstrapped_target"):
                    show_state["bootstrapped_target"] = True
                    show_state["last_target_bootstrap_at"] = now_iso

                covered_map = self._episode_map_union(confirmed_map, fresh_pending_map)
                missing_eps_by_season = self._episode_map_difference(source_eps_by_season, covered_map)
                if not missing_eps_by_season:
                    self._set_cd2_watch_pending_map(show_state, fresh_pending_map)
                    self._update_cd2_watch_completion_state(index, rule_state, show_state)
                    logger.info(f"【CD2巡查】{show_name!r} 本轮源盘无新增缺失集数，跳过复制")
                    continue

                missing_video_files, missing_files = self._collect_cd2_missing_files(
                    src_files_all=src_files_all,
                    missing_eps_by_season=missing_eps_by_season,
                )
                if not missing_video_files:
                    self._update_cd2_watch_completion_state(index, rule_state, show_state)
                    logger.info(f"【CD2巡查】{show_name!r} 缺失集未找到可复制视频文件，跳过")
                    continue

                if not dest_show_path:
                    target_show_path = str(PurePosixPath(dest_path) / show_name)
                    if cd2.create_folder(dest_path, show_name):
                        logger.info(f"【CD2巡查】{show_name!r} 首次发现，已创建目标剧目录")
                    else:
                        logger.warning(f"【CD2巡查】{show_name!r} 创建目标剧目录失败，后续复制可能失败")
                    dest_show_path = target_show_path
                    show_state["dest_show_path"] = dest_show_path

                season_dir_paths = show_state.get("season_dir_paths")
                if not isinstance(season_dir_paths, dict):
                    season_dir_paths = {}
                video_items_by_season: Dict[int, List[dict]] = {}
                missing_by_dest: Dict[str, List[str]] = {}
                for file_item in missing_video_files:
                    file_season = self._infer_file_season(file_item)
                    copy_dest = str(
                        season_dir_paths.get(str(file_season))
                        or season_dir_paths.get(file_season)
                        or ""
                    )
                    if not copy_dest:
                        copy_dest = self._find_or_create_season_dir(cd2, dest_show_path, file_season)
                        season_dir_paths[str(file_season)] = copy_dest
                    target_file_path = str(PurePosixPath(copy_dest) / file_item["name"])
                    video_items_by_season.setdefault(file_season, []).append({
                        "name": file_item["name"],
                        "path": target_file_path,
                        "size": int(file_item.get("size", 0) or 0),
                        "episode": self._parse_episode_number(file_item["name"]),
                    })

                for file_item in missing_files:
                    file_season = self._infer_file_season(file_item)
                    copy_dest = str(
                        season_dir_paths.get(str(file_season))
                        or season_dir_paths.get(file_season)
                        or ""
                    )
                    if not copy_dest:
                        copy_dest = self._find_or_create_season_dir(cd2, dest_show_path, file_season)
                        season_dir_paths[str(file_season)] = copy_dest
                    missing_by_dest.setdefault(copy_dest, []).append(file_item["path"])

                show_state["season_dir_paths"] = season_dir_paths
                ep_nums = sorted({
                    self._parse_episode_number(item["name"])
                    for item in missing_video_files
                    if self._parse_episode_number(item["name"]) is not None
                })
                logger.info(
                    f"【CD2巡查】{show_name!r} 缺失 {len(missing_video_files)} 个视频"
                    f"（集号: {ep_nums}），连同伴随文件共 {len(missing_files)} 项，开始复制"
                )
                show_notice_records = self._build_cd2_watch_notice_records(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    show_key=show_key,
                    show_name=show_name,
                    dest_path=dest_path,
                    media_type_str=media_type_str,
                    category=category,
                    video_items_by_season=video_items_by_season,
                    tmdb_id=show_state.get("tmdb_id"),
                    total_episodes=show_state.get("total_episodes"),
                )

            successful_destinations = set()
            for copy_dest, missing_paths in missing_by_dest.items():
                ok = cd2.copy_file(missing_paths, copy_dest)
                if ok:
                    total_copied += len(missing_paths)
                    successful_destinations.add(copy_dest)
                    logger.info(
                        f"【CD2巡查】{show_name!r} 复制任务已提交: "
                        f"{len(missing_paths)} 项 -> {copy_dest}"
                    )
                else:
                    logger.warning(f"【CD2巡查】{show_name!r} 复制提交失败: {copy_dest}")

            if not successful_destinations:
                self._update_cd2_watch_completion_state(index, rule_state, show_state)
                continue

            if media_type_str == "电影":
                self._set_cd2_watch_pending_map(
                    show_state,
                    self._episode_map_union(fresh_pending_map, {1: [-1]}),
                    now_dt,
                )
                notice_records.extend(show_notice_records)
                self._update_cd2_watch_completion_state(index, rule_state, show_state)
                continue

            copied_pending: Dict[int, List[int]] = {}
            for record in show_notice_records:
                target_path = str(record.get("target_path") or "")
                target_parent = str(PurePosixPath(target_path).parent) if target_path else ""
                if not target_parent or target_parent in successful_destinations or dest_path in successful_destinations:
                    notice_records.append(record)
                    season_num = int(record.get("season") or 1)
                    copied_pending.setdefault(season_num, [])
                    copied_pending[season_num].extend(
                        int(ep) for ep in (record.get("episodes") or []) if ep is not None
                    )

            if copied_pending:
                copied_pending = self._normalize_episode_map(copied_pending)
                self._set_cd2_watch_pending_map(
                    show_state,
                    self._episode_map_union(fresh_pending_map, copied_pending),
                    now_dt,
                )

            self._update_cd2_watch_completion_state(index, rule_state, show_state)

        for key, item in show_states.items():
            if key in active_show_keys:
                if isinstance(item, dict):
                    item.pop("source_missing_since", None)
                continue
            if isinstance(item, dict) and not item.get("source_missing_since"):
                item["source_missing_since"] = now_iso

        rule_state["active_count"] = len(active_show_keys)
        rule_state["completed_count"] = len([
            item for item in show_states.values()
            if isinstance(item, dict) and item.get("completed")
        ])
        rule_state["last_run_at"] = now_iso
        self._save_cd2_watch_index(index)

        return {
            "submitted_count": total_copied,
            "notice_records": notice_records,
        }

    def _list_recursive(self, cd2, path: str, force_refresh: bool = False) -> List[dict]:
        """递归列出路径下所有文件和目录"""
        result = []
        try:
            items = cd2.list_dir(path, force_refresh=force_refresh)
        except Exception:
            return result
        for item in items:
            result.append(item)
            if item.get("is_dir"):
                result.extend(self._list_recursive(cd2, item["path"], force_refresh=force_refresh))
        return result

    def _run_cd2_watch(self, force: bool = False):
        """定时任务：遍历所有启用的巡查规则并执行同步"""
        if not self._cd2_watch_rules:
            return
        if not force and not self._cd2_watch_enabled:
            return

        if not self._cd2_host or not (self._cd2_token or (self._cd2_username and self._cd2_password)):
            logger.warning("【CD2巡查】CD2 地址或认证信息未配置，跳过巡查")
            return

        try:
            cd2 = self._build_cd2_client()
        except ImportError:
            logger.error("【CD2巡查】CD2Client 导入失败")
            return

        if not cd2.test_connection():
            logger.error(f"【CD2巡查】{self._format_cd2_connection_error(cd2)}，跳过本次巡查")
            return

        active_rules = [r for r in self._cd2_watch_rules if r.get("enabled", True)]
        logger.info(f"【CD2巡查】开始巡查，共 {len(active_rules)} 条规则")

        index = self._load_cd2_watch_index()
        total = 0
        pending_notice_records: List[dict] = []
        for rule in active_rules:
            try:
                result = self._sync_cd2_rule(
                    rule,
                    cd2,
                    index=index,
                    reconcile_target=bool(force),
                )
                if isinstance(result, dict):
                    total += int(result.get("submitted_count") or 0)
                    pending_notice_records.extend(result.get("notice_records") or [])
                else:
                    total += int(result or 0)
            except Exception as e:
                logger.error(f"【CD2巡查】规则 {rule.get('name')!r} 执行异常: {e}")

        if total > 0:
            # 等待所有复制任务完成
            logger.info(f"【CD2巡查】共提交 {total} 个文件复制，等待完成...")
            ok = cd2.wait_for_copy(timeout=3600, poll_interval=10)
            if ok:
                logger.info("【CD2巡查】本次巡查全部复制完成")
                refreshed_servers: List[str] = []
                if pending_notice_records:
                    completed_at = datetime.now().isoformat()
                    for record in pending_notice_records:
                        record["copy_completed_at"] = completed_at
                        record["key"] = self._cd2_watch_notice_stable_key(record)
                    self._append_cd2_watch_pending_notices(pending_notice_records)
                    refreshed_servers = self._refresh_cd2_watch_mediaservers(pending_notice_records)
                    self._start_cd2_watch_notice_background(
                        reason="CD2巡查复制完成后确认入库",
                        initial_delay=20,
                        rounds=3,
                        sleep_seconds=60,
                    )
                refresh_text = (
                    f"，已请求局部刷新媒体库: {', '.join(refreshed_servers)}"
                    if refreshed_servers
                    else ""
                )
                logger.info(
                    f"【CD2巡查】同步完成，共 {total} 个文件{refresh_text}；"
                    "不再单独推送操作通知，入库结果由聚合通知发送"
                )
            else:
                logger.warning("【CD2巡查】部分复制任务超时或失败")
        else:
            logger.info("【CD2巡查】本次巡查无新增文件")
            if self._load_cd2_watch_pending_notices():
                self._start_cd2_watch_notice_background(
                    reason="CD2巡查补偿确认待入库通知",
                    initial_delay=0,
                    rounds=1,
                    sleep_seconds=60,
                )

    # ================================================================
    #  停止
    # ================================================================

    def stop_service(self):
        pass
