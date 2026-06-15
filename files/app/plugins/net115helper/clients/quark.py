"""
夸克网盘客户端封装。

用于降级转存时先把夸克分享链接保存到夸克网盘临时目录，
再交给 CloudDrive2 从夸克临时目录复制到 115。
"""

import os
import re
from pathlib import Path
from typing import Optional

import httpx

try:
    from app.log import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


QUARK_COOKIE_PATH = Path(os.environ.get("NET115HELPER_QUARK_COOKIE_PATH", "/config/.net115helper_quark_cookie"))
QUARK_API = "https://drive-pc.quark.cn/1/clouddrive"
COMMON_PARAMS = {"pr": "ucpro", "fr": "pc", "uc_param_str": ""}


class QuarkClient:
    _cookie: str = ""

    @classmethod
    def set_cookie(cls, cookie: str) -> None:
        cls._cookie = str(cookie or "").strip()

    @classmethod
    def _read_cookie(cls) -> str:
        if cls._cookie:
            return cls._cookie
        try:
            if QUARK_COOKIE_PATH.exists():
                return QUARK_COOKIE_PATH.read_text(encoding="utf-8").strip()
        except Exception:
            pass
        return ""

    @classmethod
    def is_available(cls) -> bool:
        return True

    @classmethod
    def is_logged_in(cls) -> bool:
        return bool(cls._read_cookie())

    @classmethod
    def check_login(cls) -> tuple[bool, str]:
        cookie = cls._read_cookie()
        if not cookie:
            return False, "未配置夸克 Cookie"
        try:
            with cls._client(cookie) as client:
                resp = client.get(f"{QUARK_API}/file/sort", params={**COMMON_PARAMS, "pdir_fid": "0", "_page": 1, "_size": 1})
            if resp.status_code == 200:
                data = resp.json()
                if int(data.get("status", 0) or 0) == 200:
                    return True, "已登录"
                return False, data.get("message") or data.get("error_msg") or "登录状态异常"
            return False, f"HTTP {resp.status_code}"
        except Exception as e:
            return False, str(e)

    @classmethod
    def save_shared_link(cls, share_url: str, share_pwd: str, target_folder_path: str) -> bool:
        cookie = cls._read_cookie()
        if not cookie:
            logger.warning("【QuarkClient】未配置夸克 Cookie")
            return False
        pwd_id = cls._parse_pwd_id(share_url)
        if not pwd_id:
            logger.warning(f"【QuarkClient】无法解析分享链接: {share_url}")
            return False

        try:
            with cls._client(cookie) as client:
                token = cls._get_share_token(client, pwd_id, share_pwd or "")
                if not token:
                    return False
                target_fid = cls._ensure_folder(client, target_folder_path)
                if not target_fid:
                    logger.warning(f"【QuarkClient】无法创建临时目录: {target_folder_path}")
                    return False
                items = cls._list_share_recursive(client, pwd_id, token, "0")
                if not items:
                    logger.warning(f"【QuarkClient】分享内容为空: {share_url}")
                    return False
                fid_list = [item.get("fid") for item in items if item.get("fid")]
                token_list = [item.get("share_fid_token") for item in items if item.get("share_fid_token")]
                if not fid_list or len(fid_list) != len(token_list):
                    logger.warning("【QuarkClient】分享文件 token 不完整")
                    return False
                payload = {
                    "fid_list": fid_list,
                    "fid_token_list": token_list,
                    "to_pdir_fid": target_fid,
                    "pwd_id": pwd_id,
                    "stoken": token,
                    "pdir_fid": "0",
                    "scene": "link",
                }
                resp = client.post(f"{QUARK_API}/share/sharepage/save", params=COMMON_PARAMS, json=payload)
                data = resp.json()
                ok = int(data.get("status", 0) or 0) == 200
                if not ok:
                    logger.warning(f"【QuarkClient】保存分享失败: {data}")
                return ok
        except Exception as e:
            logger.warning(f"【QuarkClient】保存分享异常: {e}")
            return False

    @classmethod
    def list_folder(cls, folder_path: str) -> list:
        cookie = cls._read_cookie()
        if not cookie:
            return []
        try:
            with cls._client(cookie) as client:
                fid = cls._find_folder(client, folder_path)
                if not fid:
                    return []
                resp = client.get(f"{QUARK_API}/file/sort", params={**COMMON_PARAMS, "pdir_fid": fid, "_page": 1, "_size": 200})
                data = resp.json().get("data", {})
                return data.get("list") or []
        except Exception:
            return []

    @classmethod
    def delete_path(cls, folder_path: str) -> bool:
        cookie = cls._read_cookie()
        if not cookie:
            return False
        try:
            with cls._client(cookie) as client:
                fid = cls._find_folder(client, folder_path)
                if not fid:
                    return True
                resp = client.post(f"{QUARK_API}/file/delete", params=COMMON_PARAMS, json={"filelist": [fid], "exclude_fids": []})
                data = resp.json()
                return int(data.get("status", 0) or 0) == 200
        except Exception:
            return False

    @classmethod
    def _client(cls, cookie: str) -> httpx.Client:
        headers = {
            "cookie": cookie,
            "origin": "https://pan.quark.cn",
            "referer": "https://pan.quark.cn/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        }
        return httpx.Client(headers=headers, timeout=30.0)

    @staticmethod
    def _parse_pwd_id(url: str) -> Optional[str]:
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url or "")
        return m.group(1) if m else None

    @classmethod
    def _get_share_token(cls, client: httpx.Client, pwd_id: str, passcode: str) -> str:
        resp = client.post(
            f"{QUARK_API}/share/sharepage/token",
            params={**COMMON_PARAMS, "_fetch_share": 1},
            json={"pwd_id": pwd_id, "passcode": passcode or ""},
        )
        data = resp.json()
        if int(data.get("status", 0) or 0) != 200:
            logger.warning(f"【QuarkClient】获取 stoken 失败: {data}")
            return ""
        return (data.get("data") or {}).get("stoken") or ""

    @classmethod
    def _list_share_recursive(cls, client: httpx.Client, pwd_id: str, stoken: str, pdir_fid: str, depth: int = 0) -> list:
        if depth > 6:
            return []
        resp = client.get(
            f"{QUARK_API}/share/sharepage/detail",
            params={**COMMON_PARAMS, "pwd_id": pwd_id, "stoken": stoken, "pdir_fid": pdir_fid, "_page": 1, "_size": 200},
        )
        data = resp.json()
        if int(data.get("status", 0) or 0) != 200:
            logger.warning(f"【QuarkClient】列分享目录失败: {data}")
            return []
        items = (data.get("data") or {}).get("list") or []
        result = []
        for item in items:
            if item.get("dir"):
                result.extend(cls._list_share_recursive(client, pwd_id, stoken, item.get("fid", ""), depth + 1))
            else:
                result.append(item)
        return result

    @classmethod
    def _ensure_folder(cls, client: httpx.Client, folder_path: str) -> str:
        parts = [p for p in str(folder_path or "").strip("/").split("/") if p]
        parent = "0"
        for part in parts:
            found = cls._find_child_folder(client, parent, part)
            if found:
                parent = found
                continue
            resp = client.post(
                f"{QUARK_API}/file",
                params=COMMON_PARAMS,
                json={"pdir_fid": parent, "file_name": part, "dir_path": "", "dir_init_lock": False},
            )
            data = resp.json()
            if int(data.get("status", 0) or 0) != 200:
                logger.warning(f"【QuarkClient】创建目录失败 {part}: {data}")
                return ""
            parent = (data.get("data") or {}).get("fid") or ""
            if not parent:
                return ""
        return parent

    @classmethod
    def _find_folder(cls, client: httpx.Client, folder_path: str) -> str:
        parent = "0"
        for part in [p for p in str(folder_path or "").strip("/").split("/") if p]:
            parent = cls._find_child_folder(client, parent, part)
            if not parent:
                return ""
        return parent

    @classmethod
    def _find_child_folder(cls, client: httpx.Client, parent: str, name: str) -> str:
        resp = client.get(f"{QUARK_API}/file/sort", params={**COMMON_PARAMS, "pdir_fid": parent, "_page": 1, "_size": 200})
        data = resp.json().get("data", {})
        for item in data.get("list") or []:
            if item.get("file_name") == name and item.get("dir"):
                return item.get("fid") or ""
        return ""
