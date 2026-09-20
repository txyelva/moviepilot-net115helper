"""
百度网盘客户端封装。

使用百度网盘 Web 接口完成 Cookie 登录检测和分享链接保存。
降级转存时先把百度分享保存到百度临时目录，再交给 CloudDrive2 复制到 115。
"""

import json
import os
import re
import time
from html import unescape
from pathlib import Path
from urllib.parse import quote, unquote, urlparse, parse_qs

import httpx

try:
    from app.log import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


BAIDU_COOKIE_PATH = Path(os.environ.get("NET115HELPER_BAIDU_COOKIE_PATH", "/config/.net115helper_baidu_cookie"))
BAIDU_APP_ID = "250528"


class BaiduClient:
    _cookie: str = ""
    # 百度对文件管理接口触发人机验证后的冷却（重试无法绕过，只能等或人工验证）
    _DELETE_BLOCK_SECONDS: int = 24 * 3600
    _delete_blocked_until: float = 0.0
    # 本次 save_shared_link 是否因「分享已取消/已失效」而失败，供上层拉黑死链
    last_share_dead: bool = False

    @classmethod
    def set_cookie(cls, cookie: str) -> None:
        cls._cookie = str(cookie or "").strip()

    @staticmethod
    def _is_verify_challenge(data: dict) -> bool:
        """百度要求人机/安全验证：errno 132，或响应里带 verify_scene / authwidget。"""
        if not isinstance(data, dict):
            return False
        if str(data.get("errno")) == "132":
            return True
        # 只要响应里出现这两个字段就说明百度要求走验证流程（值可能为空结构）
        return "verify_scene" in data or "authwidget" in data

    @classmethod
    def _delete_is_blocked(cls) -> bool:
        return time.time() < cls._delete_blocked_until

    @classmethod
    def _read_cookie(cls) -> str:
        if cls._cookie:
            return cls._cookie
        try:
            if BAIDU_COOKIE_PATH.exists():
                return BAIDU_COOKIE_PATH.read_text(encoding="utf-8").strip()
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
            return False, "未配置百度网盘 Cookie"
        try:
            headers = {
                "cookie": cookie,
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                "referer": "https://pan.baidu.com/disk/main",
            }
            with httpx.Client(headers=headers, timeout=15.0, follow_redirects=False) as client:
                resp = client.get("https://pan.baidu.com/rest/2.0/xpan/nas", params={"method": "uinfo"})
            if resp.status_code == 200:
                data = resp.json()
                if data.get("errno") in (0, "0", None) and data.get("baidu_name"):
                    return True, f"已登录: {data.get('baidu_name')}"
                if data.get("errno") in (-6, -7, 2, "2"):
                    return False, "Cookie 已失效或缺少 BDUSS/STOKEN"
                return False, str(data)[:180]
            return False, f"HTTP {resp.status_code}"
        except Exception as e:
            return False, str(e)

    @classmethod
    def save_shared_link(cls, share_url: str, share_pwd: str, target_folder_path: str) -> bool:
        cls.last_share_dead = False
        cookie = cls._read_cookie()
        if not cookie:
            logger.warning("【BaiduClient】未配置百度网盘 Cookie")
            return False
        surl = cls._parse_surl(share_url)
        if not surl:
            logger.warning(f"【BaiduClient】无法解析分享链接: {share_url}")
            return False

        # PanSou 常把提取码直接拼在链接的 ?pwd= 上而不单独给 password 字段。
        # 没有提取码就不会走 /share/verify、拿不到 BDCLND，加密分享恒 fsids=0。
        if not share_pwd:
            share_pwd = cls._extract_pwd_from_url(share_url)
            if share_pwd:
                logger.debug(f"【BaiduClient】从链接参数取得提取码: {share_pwd}")

        try:
            with cls._client(cookie) as client:
                bdstoken = cls._get_bdstoken(client)
                if not bdstoken:
                    logger.warning("【BaiduClient】无法获取 bdstoken")
                    return False

                verified_sekey = ""
                if share_pwd:
                    verified_sekey = cls._verify_share(client, surl, share_pwd, bdstoken)
                    # 加密分享后续所有取内容的请求都要带 BDCLND，否则恒返回 errno -9
                    if verified_sekey:
                        client.cookies.set("BDCLND", quote(verified_sekey), domain=".baidu.com")

                share_data = cls._get_share_data(client, share_url, surl, share_pwd)
                shareid = str(share_data.get("shareid") or "")
                uk = str(share_data.get("uk") or share_data.get("share_uk") or "")
                sekey = share_data.get("sekey") or share_data.get("randsk") or verified_sekey or ""
                fsids = share_data.get("fsids") or cls._get_root_fsids(
                    client, uk, shareid, share_url=share_url, sekey=sekey
                )

                if not shareid or not uk or not fsids:
                    logger.warning(
                        f"【BaiduClient】分享参数不完整: shareid={shareid}, uk={uk}, fsids={len(fsids)}"
                    )
                    return False

                # 百度创建/转存遇到同名内容时容易自动生成后缀目录；每次保存前只清理
                # 当前剧名暂存子目录，保证内容始终收束在配置的临时根目录下。
                cls._delete_path(client, bdstoken, target_folder_path)

                if not cls._ensure_folder(client, bdstoken, target_folder_path):
                    logger.warning(f"【BaiduClient】无法创建临时目录: {target_folder_path}")
                    return False

                params = {
                    "shareid": shareid,
                    "from": uk,
                    "sekey": sekey,
                    "ondup": "overwrite",
                    "async": 1,
                    "channel": "chunlei",
                    "web": 1,
                    "app_id": BAIDU_APP_ID,
                    "bdstoken": bdstoken,
                    "clienttype": 0,
                }
                data = {
                    "fsidlist": json.dumps(fsids, ensure_ascii=False),
                    "path": target_folder_path,
                }
                resp = client.post("https://pan.baidu.com/share/transfer", params=params, data=data)
                result = resp.json()
                errno = result.get("errno")
                if errno in (0, "0"):
                    return True
                logger.warning(f"【BaiduClient】保存分享失败: {result}")
                return False
        except Exception as e:
            logger.warning(f"【BaiduClient】保存分享异常: {e}")
            return False

    @classmethod
    def list_folder(cls, folder_path: str) -> list:
        cookie = cls._read_cookie()
        if not cookie:
            return []
        try:
            with cls._client(cookie) as client:
                bdstoken = cls._get_bdstoken(client)
                return cls._list_dir(client, folder_path, bdstoken)
        except Exception as e:
            logger.warning(f"【BaiduClient】列目录异常 {folder_path}: {e}")
            return []

    @classmethod
    def delete_path(cls, folder_path: str) -> bool:
        cookie = cls._read_cookie()
        if not cookie:
            return False
        try:
            with cls._client(cookie) as client:
                bdstoken = cls._get_bdstoken(client)
                if not bdstoken:
                    return False
                return cls._delete_path(client, bdstoken, folder_path)
        except Exception as e:
            logger.warning(f"【BaiduClient】删除路径异常 {folder_path}: {e}")
            return False

    @classmethod
    def _client(cls, cookie: str) -> httpx.Client:
        headers = {
            "cookie": cookie,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "referer": "https://pan.baidu.com/disk/main",
            "origin": "https://pan.baidu.com",
        }
        return httpx.Client(headers=headers, timeout=30.0, follow_redirects=True)

    @staticmethod
    def _extract_pwd_from_url(url: str) -> str:
        """从分享链接的 ?pwd=/&pwd= 参数取提取码（含末尾多余引号等噪声时也能取到）。"""
        try:
            qs = parse_qs(urlparse(url or "").query)
            for key in ("pwd", "passcode", "code"):
                if qs.get(key):
                    val = str(qs[key][0]).strip().strip("'\"")
                    if val:
                        return val
        except Exception:
            pass
        m = re.search(r"[?&]pwd=([A-Za-z0-9]{4})", url or "")
        return m.group(1) if m else ""

    @staticmethod
    def _parse_surl(url: str) -> str:
        parsed = urlparse(url or "")
        qs = parse_qs(parsed.query)
        if qs.get("surl"):
            return qs["surl"][0].lstrip("1")
        m = re.search(r"/s/1?([A-Za-z0-9_-]+)", url or "")
        return m.group(1) if m else ""

    @classmethod
    def _get_bdstoken(cls, client: httpx.Client) -> str:
        resp = client.get("https://pan.baidu.com/disk/main")
        text = resp.text
        for pattern in (
            r'"bdstoken"\s*:\s*"([^"]+)"',
            r"bdstoken['\"]?\s*[:=]\s*['\"]([^'\"]+)",
        ):
            m = re.search(pattern, text)
            if m:
                return unescape(m.group(1))
        return ""

    @classmethod
    def _verify_share(cls, client: httpx.Client, surl: str, passcode: str, bdstoken: str) -> str:
        params = {
            "surl": surl,
            "t": int(time.time() * 1000),
            "channel": "chunlei",
            "web": 1,
            "app_id": BAIDU_APP_ID,
            "bdstoken": bdstoken,
            "clienttype": 0,
        }
        resp = client.post("https://pan.baidu.com/share/verify", params=params, data={"pwd": passcode})
        try:
            data = resp.json()
        except Exception:
            data = {}
        randsk = data.get("randsk") or ""
        if data.get("errno") not in (0, "0"):
            logger.warning(f"【BaiduClient】提取码校验失败: {data}")
        return unquote(randsk)

    @classmethod
    def _get_share_data(cls, client: httpx.Client, share_url: str, surl: str, passcode: str) -> dict:
        url = share_url
        if passcode and "pwd=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}pwd={quote(passcode)}"
        resp = client.get(url, headers={"referer": "https://pan.baidu.com/"})
        text = resp.text
        data = {}
        for key, patterns in {
            "shareid": [r'"shareid"\s*:\s*(\d+)', r"shareid['\"]?\s*[:=]\s*['\"]?(\d+)"],
            "uk": [r'"uk"\s*:\s*(\d+)', r'"share_uk"\s*:\s*(\d+)'],
            "sekey": [r'"sekey"\s*:\s*"([^"]+)"', r'"randsk"\s*:\s*"([^"]+)"'],
        }.items():
            for pattern in patterns:
                m = re.search(pattern, text)
                if m:
                    data[key] = unquote(unescape(m.group(1)))
                    break
        if not data.get("shareid") or not data.get("uk") or str(data.get("uk")) == "0":
            # wxlist 有时能在网页参数解析失败时返回 shareid/uk/fsids。
            # 注意：wxlist 的 shorturl 需要带 /s/ 后面的前导 1，并且私密分享要带 pwd。
            for shorturl in (f"1{surl}", surl):
                params = {
                    "channel": "weixin",
                    "version": "2.9.6",
                    "clienttype": 25,
                    "web": 1,
                    "shorturl": shorturl,
                    "dir": "",
                    "root": 1,
                }
                if passcode:
                    params["pwd"] = passcode
                try:
                    resp2 = client.get("https://pan.baidu.com/share/wxlist", params=params)
                    wx = resp2.json()
                    wdata = wx.get("data") or {}
                    if not wdata:
                        continue
                    if not data.get("shareid"):
                        data["shareid"] = str(wdata.get("shareid") or "")
                    wx_uk = str(wdata.get("uk") or wdata.get("share_uk") or "")
                    if wx_uk and wx_uk != "0":
                        data["uk"] = wx_uk
                    sekey = wdata.get("seckey") or wdata.get("sekey") or wdata.get("randsk")
                    if sekey:
                        data["sekey"] = unquote(unescape(sekey))
                    items = wdata.get("list") or wx.get("list") or []
                    fsids = [
                        int(item.get("fs_id") or item.get("fsid"))
                        for item in items
                        if item.get("fs_id") or item.get("fsid")
                    ]
                    if fsids:
                        data["fsids"] = fsids
                    if data.get("shareid") and data.get("uk") and str(data.get("uk")) != "0":
                        break
                except Exception:
                    pass
        return data

    @staticmethod
    def _extract_file_list_from_page(text: str) -> list:
        """
        从分享页 HTML 里解析 file_list。

        百度已对加密分享关闭 /share/list（恒返回 errno -9「提取码验证失败」），
        文件列表改为直接嵌在分享页的 `"file_list":[...]` 中，需先带上 BDCLND。
        """
        m = re.search(r'"file_list"\s*:\s*(\[.*?\])\s*[,}]', text or "", re.S)
        if not m:
            return []
        raw = m.group(1)
        for loader in (
            lambda s: json.loads(s),
            lambda s: json.loads(s.encode().decode("unicode_escape")),
        ):
            try:
                items = loader(raw)
                if isinstance(items, list):
                    return items
            except Exception:
                continue
        # 结构再变时的兜底：至少把 fs_id 抠出来
        return [{"fs_id": int(x)} for x in re.findall(r'"fs_id"\s*:\s*"?(\d+)"?', raw)]

    @classmethod
    def _get_root_fsids(cls, client: httpx.Client, uk: str, shareid: str,
                        share_url: str = "", sekey: str = "") -> list:
        """取分享根目录的 fsid 列表：优先解析分享页，失败再退回旧接口。"""
        if share_url:
            try:
                if sekey:
                    client.cookies.set("BDCLND", quote(sekey), domain=".baidu.com")
                resp = client.get(share_url, headers={"referer": "https://pan.baidu.com/"})
                items = cls._extract_file_list_from_page(resp.text)
                fsids = [
                    int(item.get("fs_id") or item.get("fsid"))
                    for item in items
                    if item.get("fs_id") or item.get("fsid")
                ]
                if fsids:
                    return fsids
                logger.debug("【BaiduClient】分享页未解析到 file_list，回退 share/list")
            except Exception as e:
                logger.debug(f"【BaiduClient】解析分享页 file_list 失败: {e}")

        params = {
            "uk": uk,
            "shareid": shareid,
            "order": "other",
            "desc": 1,
            "showempty": 0,
            "web": 1,
            "page": 1,
            "num": 100,
            "dir": "/",
            "root": 1,
        }
        resp = client.get("https://pan.baidu.com/share/list", params=params)
        data = resp.json()
        if cls._is_dead_share(data):
            cls.last_share_dead = True
            logger.info(
                f"【BaiduClient】分享已失效(errno={data.get('errno')}): "
                f"{data.get('show_msg') or ''}"
            )
            return []
        items = data.get("list") or (data.get("data") or {}).get("list") or []
        fsids = []
        for item in items:
            fsid = item.get("fs_id") or item.get("fsid")
            if fsid:
                fsids.append(int(fsid))
        return fsids

    @staticmethod
    def _is_dead_share(data: dict) -> bool:
        """百度：errno -21 分享已被取消 / -22 等同类终态。"""
        if not isinstance(data, dict):
            return False
        if str(data.get("errno")) in ("-21", "-22"):
            return True
        msg = str(data.get("show_msg") or data.get("errmsg") or "")
        return any(k in msg for k in ("已被取消", "已失效", "已删除", "不存在"))

    @classmethod
    def _ensure_folder(cls, client: httpx.Client, bdstoken: str, folder_path: str) -> bool:
        current = ""
        for part in [p for p in str(folder_path or "").strip("/").split("/") if p]:
            current = f"{current}/{part}"
            if cls._find_path(client, current, bdstoken):
                continue
            params = {
                "a": "commit",
                "bdstoken": bdstoken,
                "channel": "chunlei",
                "web": 1,
                "app_id": BAIDU_APP_ID,
                "clienttype": 0,
            }
            data = {"path": current, "isdir": 1, "block_list": "[]", "rtype": 0}
            resp = client.post("https://pan.baidu.com/api/create", params=params, data=data)
            try:
                result = resp.json()
            except Exception:
                result = {}
            errno = result.get("errno")
            if errno in (-8, "-8", 31061, "31061") and cls._find_path(client, current, bdstoken):
                continue
            if errno not in (0, "0"):
                logger.warning(f"【BaiduClient】创建目录失败 {current}: {result}")
                return False
            real_path = result.get("path") or (result.get("info") or {}).get("path") or current
            if str(real_path).rstrip("/") != current.rstrip("/"):
                logger.warning(f"【BaiduClient】创建目录被百度改名: {current} -> {real_path}")
                cls._delete_path(client, bdstoken, str(real_path))
                return False
        return True

    @staticmethod
    def _normalize_path(path: str) -> str:
        parts = [p for p in str(path or "").strip("/").split("/") if p]
        return "/" + "/".join(parts) if parts else "/"

    @classmethod
    def _parent_and_name(cls, path: str) -> tuple[str, str]:
        parts = [p for p in cls._normalize_path(path).strip("/").split("/") if p]
        if not parts:
            return "/", ""
        parent = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
        return parent, parts[-1]

    @classmethod
    def _list_dir(cls, client: httpx.Client, folder_path: str, bdstoken: str = "") -> list:
        folder_path = cls._normalize_path(folder_path)
        result = []
        page = 1
        while page <= 20:
            params = {
                "dir": folder_path,
                "order": "name",
                "desc": 0,
                "showempty": 0,
                "web": 1,
                "page": page,
                "num": 200,
                "app_id": BAIDU_APP_ID,
                "clienttype": 0,
            }
            if bdstoken:
                params["bdstoken"] = bdstoken
            resp = client.get("https://pan.baidu.com/api/list", params=params)
            data = resp.json()
            if data.get("errno") not in (0, "0", None):
                return result
            items = data.get("list") or []
            result.extend(items)
            if len(items) < 200:
                break
            page += 1
        return result

    @classmethod
    def _find_path(cls, client: httpx.Client, path: str, bdstoken: str = "") -> dict:
        path = cls._normalize_path(path)
        if path == "/":
            return {"path": "/", "isdir": 1, "server_filename": ""}
        parent, name = cls._parent_and_name(path)
        for item in cls._list_dir(client, parent, bdstoken):
            item_name = item.get("server_filename") or item.get("filename") or item.get("name")
            if item_name == name:
                return item
        return {}

    @classmethod
    def _delete_path(cls, client: httpx.Client, bdstoken: str, folder_path: str) -> bool:
        folder_path = cls._normalize_path(folder_path)
        if folder_path == "/":
            return False
        if cls._delete_is_blocked():
            # 冷却期内直接放弃删除：残留的是空目录，不影响后续保存
            logger.debug(f"【BaiduClient】删除处于安全验证冷却期，跳过: {folder_path}")
            return False
        item = cls._find_path(client, folder_path, bdstoken)
        if not item:
            return True
        target_path = item.get("path") or folder_path
        params = {
            "opera": "delete",
            "async": 2,
            "onnest": "fail",
            "bdstoken": bdstoken,
            "channel": "chunlei",
            "web": 1,
            "app_id": BAIDU_APP_ID,
            "clienttype": 0,
        }
        resp = client.post(
            "https://pan.baidu.com/api/filemanager",
            params=params,
            data={"filelist": json.dumps([target_path], ensure_ascii=False)},
        )
        try:
            data = resp.json()
        except Exception:
            data = {}
        if data.get("errno") in (0, "0"):
            cls._delete_blocked_until = 0.0
            return True
        if cls._is_verify_challenge(data):
            # 百度对 filemanager 触发了人机/安全验证（errno 132 + verify_scene）。
            # 这种拦截靠重试无法绕过，继续撞只会每天刷上千次无效请求，
            # 因此进入长冷却，冷却期内不再尝试删除，并且只告警一次。
            first = cls._delete_blocked_until <= time.time()
            cls._delete_blocked_until = time.time() + cls._DELETE_BLOCK_SECONDS
            if first:
                logger.warning(
                    f"【BaiduClient】删除被百度安全验证拦截(errno={data.get('errno')})，"
                    f"暂停删除 {cls._DELETE_BLOCK_SECONDS // 3600} 小时；"
                    "需到百度网盘网页端完成一次验证后才能恢复。"
                    "临时目录清理将由后续保存流程跳过，不影响转存。"
                )
            else:
                logger.debug(f"【BaiduClient】删除仍被安全验证拦截 {folder_path}: {data}")
            return False
        logger.warning(f"【BaiduClient】删除路径失败 {folder_path}: {data}")
        return False
