"""
阿里云盘客户端封装（基于 aligo）

功能：
- QR 码扫码登录（使用 aligo 网页 API，独立于 MP 的 Open Platform 认证）
- 把分享链接转存到阿里云盘指定目录
- 获取/创建目录（返回 file_id）
- 删除文件/目录
"""

import os
import re
import sys
import threading
import time
from io import BytesIO
from pathlib import Path
from typing import Optional

try:
    from app.log import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# aligo session 存储路径。aligo 默认写入当前工作目录下的 .aligo，
# MoviePilot 容器中通常是 /moviepilot/.aligo；保留 /config/.aligo 兼容旧配置。
ALIYUN_CONFIG_DIR = Path(os.environ.get("NET115HELPER_ALIYUN_CONFIG_DIR", "/config/.aligo"))
ALIYUN_CONFIG_DIRS = [
    ALIYUN_CONFIG_DIR,
    Path("/moviepilot/.aligo"),
    Path.cwd() / ".aligo",
]
ALIYUN_CONFIG_NAME = "net115helper"

try:
    import aligo as _aligo_mod
    from aligo import Aligo
    _ALIGO_AVAILABLE = True
except ImportError:
    _ALIGO_AVAILABLE = False
    Aligo = None  # type: ignore


class AliyunClient:
    """
    阿里云盘客户端，使用 aligo（web API）。
    每次操作都新建连接（避免持久对象导致 session 过期问题）。
    """

    # ── 单例登录线程 ──────────────────────────────────────
    _login_thread: Optional[threading.Thread] = None
    _login_qr_url: Optional[str] = None   # QR 码内容（alipay/aliyun://...）
    _login_status: str = "idle"            # idle / waiting / confirmed / error
    _login_error: str = ""
    _login_lock = threading.Lock()
    _refresh_token: str = ""

    @classmethod
    def is_available(cls) -> bool:
        return _ALIGO_AVAILABLE

    @classmethod
    def set_refresh_token(cls, token: str) -> None:
        cls._refresh_token = str(token or "").strip()

    @classmethod
    def _config_files(cls) -> list:
        seen = []
        files = []
        for directory in ALIYUN_CONFIG_DIRS:
            path = directory / f"{ALIYUN_CONFIG_NAME}.json"
            key = str(path)
            if key in seen:
                continue
            seen.append(key)
            files.append(path)
        return files

    @classmethod
    def is_logged_in(cls) -> bool:
        """检查是否有可加载的 aligo session 文件"""
        if not _ALIGO_AVAILABLE:
            return False
        has_session = any(path.exists() for path in cls._config_files())
        if not has_session and not cls._refresh_token:
            return False
        try:
            kwargs = {
                "name": ALIYUN_CONFIG_NAME,
                "re_login": False,
                "level": 50,
            }
            if cls._refresh_token:
                kwargs["refresh_token"] = cls._refresh_token
            ali = Aligo(**kwargs)
            # 只要 session 能加载并完成一次轻量调用，就视为可用。
            try:
                ali.get_personal_info()
            except Exception:
                pass
            return True
        except Exception as e:
            logger.warning(f"【AliyunClient】session 不可用: {e}")
            return False

    @classmethod
    def _get_client(cls) -> Optional["Aligo"]:
        """获取已登录的 aligo 实例（不阻塞）"""
        if not _ALIGO_AVAILABLE:
            return None
        if not cls.is_logged_in():
            return None
        try:
            kwargs = {
                "name": ALIYUN_CONFIG_NAME,
                "re_login": False,
                "level": 50,  # 关闭 aligo 自身日志
            }
            if cls._refresh_token:
                kwargs["refresh_token"] = cls._refresh_token
            ali = Aligo(**kwargs)
            return ali
        except Exception as e:
            logger.warning(f"【AliyunClient】加载 session 失败: {e}")
            return None

    # ── 扫码登录 ────────────────────────────────────────

    @classmethod
    def start_qrcode_login(cls) -> dict:
        """
        启动 QR 码登录流程（后台线程）。
        返回 {'success': True/False, 'message': ...}
        登录结果通过 get_login_status() 轮询获取，
        QR 码内容通过 get_qr_url() 获取。
        """
        if not _ALIGO_AVAILABLE:
            return {"success": False, "message": "aligo 未安装"}

        with cls._login_lock:
            cls._login_status = "waiting"
            cls._login_qr_url = None
            cls._login_error = ""

        def _run_login():
            def _capture_qr(qr_link: str):
                with cls._login_lock:
                    cls._login_qr_url = str(qr_link or "")
                logger.info(f"【AliyunClient】QR 码已生成，等待扫码")

            try:
                # 删旧 session，强制重新登录
                for old_cfg in cls._config_files():
                    if old_cfg.exists():
                        old_cfg.unlink()

                ali = Aligo(
                    name=ALIYUN_CONFIG_NAME,
                    show=_capture_qr,
                    re_login=True,
                    login_timeout=180,
                    level=50,
                )
                # 登录成功（constructor 返回说明已登录）
                with cls._login_lock:
                    cls._login_status = "confirmed"
                try:
                    info = ali.get_personal_info()
                    nick = getattr(info, "nick_name", None) or getattr(info, "nickName", None) or "OK"
                except Exception:
                    nick = "OK"
                logger.info(f"【AliyunClient】登录成功: {nick}")
            except Exception as e:
                with cls._login_lock:
                    cls._login_status = "error"
                    cls._login_error = str(e)
                logger.error(f"【AliyunClient】登录失败: {e}")

        cls._login_thread = threading.Thread(target=_run_login, daemon=True)
        cls._login_thread.start()
        return {"success": True, "message": "登录流程已启动"}

    @classmethod
    def get_login_status(cls) -> dict:
        """轮询登录状态"""
        with cls._login_lock:
            return {
                "status": cls._login_status,
                "qr_url": cls._login_qr_url,
                "error": cls._login_error,
            }

    @classmethod
    def get_qr_image_bytes(cls) -> Optional[bytes]:
        """
        把 QR 码内容（alipay://... 之类）转成 PNG 字节。
        需要 qrcode + Pillow。
        """
        with cls._login_lock:
            qr_url = cls._login_qr_url
        if not qr_url:
            return None
        try:
            import qrcode
            img = qrcode.make(qr_url)
            pil_img = getattr(img, "_img", img)
            buf = BytesIO()
            pil_img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception as e:
            logger.warning(f"【AliyunClient】生成 QR 图片失败: {e}")
            return None

    # ── 分享链接转存 ─────────────────────────────────────

    @classmethod
    def save_shared_link(
        cls,
        share_url: str,
        share_pwd: str,
        target_folder_path: str,
    ) -> bool:
        """
        把阿里云盘分享链接转存到 target_folder_path。

        :param share_url:          分享链接，如 https://www.alipan.com/s/XXXXX
        :param share_pwd:          分享密码（无密码传空字符串）
        :param target_folder_path: 阿里云盘目标路径，如 /MP临时转存/DTF圣路易日记
        :return: 成功返回 True
        """
        if not _ALIGO_AVAILABLE:
            logger.error("【AliyunClient】aligo 未安装")
            return False

        ali = cls._get_client()
        if ali is None:
            logger.error("【AliyunClient】未登录，请先扫码登录")
            return False

        # 解析 share_id
        share_id = cls._parse_share_id(share_url)
        if not share_id:
            logger.error(f"【AliyunClient】无法解析分享 ID: {share_url}")
            return False

        try:
            # 1. 获取 share_token
            share_token = ali.get_share_token(share_id, share_pwd or "")
            if not share_token:
                logger.error(f"【AliyunClient】获取 share_token 失败: {share_id}")
                return False

            # 2. 获取/创建目标目录
            folder_id = cls._ensure_folder(ali, target_folder_path)
            if not folder_id:
                logger.error(f"【AliyunClient】无法创建目录: {target_folder_path}")
                return False

            # 3. 递归转存所有文件
            # share_file_save_all_to_drive 只保存分享顶层，不递归保存嵌套文件。
            # 需要手动递归：遍历分享目录树，逐层在用户云盘创建文件夹并保存文件。
            total = cls._save_share_recursive(
                ali, share_token, "root", folder_id, depth=0
            )
            import sys as _sys
            _sys.stdout.write(f"【AliyunClient】递归转存完成: {share_id} 共 {total} 项\n")
            _sys.stdout.flush()
            return total > 0

        except Exception as e:
            logger.error(f"【AliyunClient】save_shared_link 异常: {e}")
            return False

    @classmethod
    def _save_share_recursive(
        cls,
        ali: "Aligo",
        share_token,
        share_parent_id: str,
        dest_parent_id: str,
        depth: int = 0,
    ) -> int:
        """
        递归保存分享中所有文件到用户云盘。
        :param share_token:     get_share_token 返回的 token 对象
        :param share_parent_id: 分享中的父目录 file_id（顶层传 "root"）
        :param dest_parent_id:  用户云盘目标父目录 file_id
        :param depth:           当前递归深度（防止无限递归）
        :return: 成功保存的项目数
        """
        if depth > 6:
            return 0
        try:
            items = ali.get_share_file_list(share_token, parent_file_id=share_parent_id)
        except Exception as e:
            import sys as _sys
            _sys.stdout.write(f"【AliyunClient】列分享目录失败 (depth={depth}): {e}\n")
            _sys.stdout.flush()
            return 0

        total = 0
        for item in items or []:
            item_type = getattr(item, "type", None)
            item_name = getattr(item, "name", "")
            item_id = getattr(item, "file_id", None)
            if not item_id:
                continue

            if item_type == "folder":
                # 在用户云盘创建同名文件夹
                try:
                    created = ali.create_folder(
                        name=item_name,
                        parent_file_id=dest_parent_id,
                        check_name_mode="refuse",
                    )
                    if created and getattr(created, "file_id", None):
                        new_dest_id = created.file_id
                    else:
                        # 文件夹已存在，查找它
                        existing = ali.get_file_list(parent_file_id=dest_parent_id) or []
                        found = next(
                            (f for f in existing
                             if getattr(f, "name", None) == item_name
                             and getattr(f, "type", None) == "folder"),
                            None,
                        )
                        new_dest_id = found.file_id if found else dest_parent_id
                except Exception:
                    new_dest_id = dest_parent_id

                # 递归处理子目录
                sub_count = cls._save_share_recursive(
                    ali, share_token, item_id, new_dest_id, depth + 1
                )
                total += sub_count
            else:
                # 文件：调用 share_file_save_to_drive 保存单个文件
                try:
                    result = ali.share_file_saveto_drive(
                        file_id=item_id,
                        share_token=share_token,
                        to_parent_file_id=dest_parent_id,
                    )
                    if result and getattr(result, "file_id", None):
                        total += 1
                        import sys as _sys
                        _sys.stdout.write(f"【AliyunClient】已保存: {item_name}\n")
                        _sys.stdout.flush()
                    else:
                        import sys as _sys
                        _sys.stdout.write(f"【AliyunClient】保存失败: {item_name} result={result}\n")
                        _sys.stdout.flush()
                except Exception as e:
                    import sys as _sys
                    _sys.stdout.write(f"【AliyunClient】保存文件异常 {item_name}: {e}\n")
                    _sys.stdout.flush()
        return total

    @classmethod
    def _parse_share_id(cls, url: str) -> Optional[str]:
        """从分享 URL 提取 share_id"""
        # https://www.alipan.com/s/XXXXX 或 https://aliyundrive.com/s/XXXXX
        m = re.search(r"/s/([A-Za-z0-9]+)", url)
        if m:
            return m.group(1)
        return None

    @classmethod
    def _ensure_folder(cls, ali: "Aligo", folder_path: str) -> Optional[str]:
        """
        获取或创建指定路径的目录，返回 file_id。
        folder_path 如 "/MP临时转存/DTF圣路易日记"
        """
        try:
            # 先尝试获取
            folder = ali.get_folder_by_path(folder_path, create_folder=True)
            if folder:
                return getattr(folder, "file_id", None)
        except Exception as e:
            logger.debug(f"【AliyunClient】_ensure_folder {folder_path}: {e}")
        # 手动逐级创建
        try:
            parts = [p for p in folder_path.strip("/").split("/") if p]
            current_id = "root"
            for part in parts:
                created = ali.create_folder(
                    name=part,
                    parent_file_id=current_id,
                    check_name_mode="refuse",
                )
                if created and getattr(created, "file_id", None):
                    current_id = created.file_id
                else:
                    # 目录已存在，查找它
                    items = ali.get_file_list(parent_file_id=current_id) or []
                    found = next(
                        (f for f in items if getattr(f, "name", None) == part and getattr(f, "type", None) == "folder"),
                        None,
                    )
                    if found:
                        current_id = found.file_id
                    else:
                        logger.error(f"【AliyunClient】无法创建/找到目录 {part}")
                        return None
            return current_id
        except Exception as e:
            logger.error(f"【AliyunClient】_ensure_folder 手动创建失败: {e}")
            return None

    @classmethod
    def list_folder(cls, folder_path: str) -> list:
        """列出目录内容，返回 [{name, file_id, type}] 列表"""
        ali = cls._get_client()
        if not ali:
            return []
        try:
            folder = ali.get_folder_by_path(folder_path)
            if not folder:
                return []
            items = ali.get_file_list(parent_file_id=folder.file_id) or []
            return [
                {
                    "name": getattr(f, "name", ""),
                    "file_id": getattr(f, "file_id", ""),
                    "type": getattr(f, "type", ""),
                }
                for f in items
            ]
        except Exception as e:
            logger.error(f"【AliyunClient】list_folder 异常: {e}")
            return []

    @classmethod
    def delete_file(cls, file_id: str) -> bool:
        """删除文件/目录（移入回收站）"""
        ali = cls._get_client()
        if not ali:
            return False
        try:
            ali.move_file_to_trash(file_id)
            return True
        except Exception as e:
            logger.error(f"【AliyunClient】delete_file 异常 ({file_id}): {e}")
            return False
