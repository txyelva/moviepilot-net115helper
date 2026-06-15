"""
115 网盘客户端封装 - 使用 p115client 库支持扫码登录和自动续期

依赖已安装的 p115client (v0.0.8.4.6):
  - P115Client.login_qrcode_token()  → {data: {uid, time, sign, qrcode}}
  - P115Client.login_qrcode_scan_status(token_dict)  → {data: {status: 0/1/2/-1}}
  - P115Client.login_qrcode_scan_result(uid, app)  → {data: {cookie: {...}}}
  - P115Client(cookies=Path(...))  → 从文件读取 cookies（latin-1 编码的 cookie 字符串）

cookies 文件格式（非 JSON！）：
  latin-1 编码的 cookie 字符串，如 "UID=xxx; CID=xxx; SEID=xxx"
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx

try:
    from p115client import P115Client
    _P115CLIENT_AVAILABLE = True
except ImportError:
    P115Client = None  # type: ignore
    _P115CLIENT_AVAILABLE = False

from app.log import logger


# ─────────────────────────────────────────────────────────
#  Cookie 文件路径
# ─────────────────────────────────────────────────────────
COOKIES_PATH = os.environ.get("NET115HELPER_P115_COOKIES_PATH", "/config/.p115_cookies")

# ─────────────────────────────────────────────────────────
#  二维码状态管理（插件级共享）
# ─────────────────────────────────────────────────────────
class QrcodeState:
    """
    管理扫码登录状态，供 API 轮询使用。
    实例为单例（模块级变量），在 API handler 之间共享。
    """
    _instance: Optional["QrcodeState"] = None

    def __init__(self):
        self.status: str = "idle"        # idle | waiting | scanned | confirmed | expired | error
        self.qrcode_url: str = ""
        self.qrcode_image_path: str = ""
        self.qrcode_uid: str = ""
        self.created_at: float = 0.0
        self.updated_at: float = 0.0
        self.message: str = ""
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "QrcodeState":
        if cls._instance is None:
            with threading.Lock():
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def reset(self):
        with self._lock:
            self.status = "idle"
            self.qrcode_url = ""
            self.qrcode_image_path = ""
            self.qrcode_uid = ""
            self.created_at = 0.0
            self.updated_at = time.time()
            self.message = ""

    def set_waiting(self, qrcode_url: str, qrcode_image_path: str, qrcode_uid: str):
        with self._lock:
            self.status = "waiting"
            self.qrcode_url = qrcode_url
            self.qrcode_image_path = qrcode_image_path
            self.qrcode_uid = qrcode_uid
            self.created_at = time.time()
            self.updated_at = time.time()
            self.message = "请使用 115 手机 APP 扫码确认"

    def set_scanned(self):
        with self._lock:
            if self.status == "waiting":
                self.status = "scanned"
                self.updated_at = time.time()
                self.message = "已扫码，请在手机确认"

    def set_confirmed(self, cookies: dict):
        with self._lock:
            self.status = "confirmed"
            self.updated_at = time.time()
            self.message = "登录成功"
            self._cookies = cookies

    def set_expired(self):
        with self._lock:
            self.status = "expired"
            self.updated_at = time.time()
            self.message = "二维码已过期，请重新生成"

    def set_error(self, msg: str):
        with self._lock:
            self.status = "error"
            self.updated_at = time.time()
            self.message = msg

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "qrcode_url": self.qrcode_url,
                "qrcode_image_path": self.qrcode_image_path,
                "qrcode_uid": self.qrcode_uid,
                "message": self.message,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
            }


# ─────────────────────────────────────────────────────────
#  Rate Limiter（40140117 处理）
# ─────────────────────────────────────────────────────────
class RateLimiter:
    """
    115 API 限流器。
    - 基础间隔 2.5s
    - 收到 40140117 时自动指数退避
    - 成功后逐步恢复
    """

    DEFAULT_MIN_INTERVAL: float = 2.5
    MAX_INTERVAL: float = 60.0
    _jitter_ratio: float = 0.5

    def __init__(self):
        self._interval = self.DEFAULT_MIN_INTERVAL
        self._consecutive_successes = 0
        self._consecutive_backoffs = 0
        self._last_request_time = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self._interval:
                sleep_time = self._interval - elapsed
                jitter = sleep_time * self._jitter_ratio * (2 * (time.time() % 2) - 1)
                sleep_time = max(0.01, sleep_time + jitter)
                time.sleep(sleep_time)
            self._last_request_time = time.time()

    def report_success(self):
        with self._lock:
            self._consecutive_successes += 1
            self._consecutive_backoffs = 0
            if self._consecutive_successes >= 5:
                self._interval = max(self.DEFAULT_MIN_INTERVAL, self._interval / 2)
                self._consecutive_successes = 0

    def report_rate_limit(self, error_code: str):
        if error_code not in ("40140117", "40140110"):
            return
        with self._lock:
            self._consecutive_backoffs += 1
            self._consecutive_successes = 0
            new_interval = min(
                self.DEFAULT_MIN_INTERVAL * (2 ** self._consecutive_backoffs),
                self.MAX_INTERVAL
            )
            if new_interval != self._interval:
                logger.warning(
                    f"【115限流】收到 {error_code}，退避至 {new_interval:.1f}s "
                    f"(第{self._consecutive_backoffs}次连续退避)"
                )
                self._interval = new_interval

    def check_error_code(self, data: dict) -> Optional[str]:
        errno = data.get("errno") or data.get("errNo") or data.get("code") or ""
        errno_str = str(errno)
        if errno_str and errno_str.isdigit() and len(errno_str) == 8:
            return errno_str
        return None


# ─────────────────────────────────────────────────────────
#  P115ClientManager
# ─────────────────────────────────────────────────────────
class P115ClientManager:
    """
    115 网盘客户端管理器。

    核心功能：
    1. 扫码登录（login_qrcode_token → 生成二维码 → 轮询 → login_qrcode_scan_result → 保存）
    2. Cookie 持久化（latin-1 编码 cookie 字符串文件，供 P115Client 直接读取）
    3. 所有 115 API 操作都通过 RateLimiter 限流
    """

    def __init__(
        self,
        cookies_path: str = COOKIES_PATH,
        check_for_relogin: bool = False,
    ):
        if not _P115CLIENT_AVAILABLE:
            raise RuntimeError(
                "p115client 库未安装。请在容器中执行: pip install p115client"
            )

        self.cookies_path = cookies_path
        self._client: Optional[P115Client] = None
        self._lock = threading.Lock()
        self._rate_limiter = RateLimiter()

        # 确保 cookies 目录存在
        os.makedirs(os.path.dirname(cookies_path), exist_ok=True)

        # 如果 cookies 文件已存在，尝试加载
        if os.path.exists(cookies_path):
            try:
                self._client = P115Client(
                    cookies=Path(cookies_path),
                    check_for_relogin=check_for_relogin,
                )
                logger.info(f"【115客户端】P115Client 已从文件初始化: {cookies_path}")
            except Exception as e:
                logger.warning(f"【115客户端】从文件初始化失败: {e}")
                self._client = None
        else:
            logger.info(f"【115客户端】Cookie 文件不存在，需要扫码登录: {cookies_path}")

    @property
    def client(self) -> Optional[P115Client]:
        return self._client

    def is_logged_in(self) -> bool:
        """检查当前是否已登录（cookies 文件存在且非空）"""
        if not os.path.exists(self.cookies_path):
            return False
        try:
            with open(self.cookies_path, "rb") as f:
                content = f.read()
            return len(content) > 10  # 有实质性内容
        except Exception:
            return False

    def get_cookies_str(self) -> str:
        """
        返回 cookie 字符串（用于直接构建请求头）。
        文件格式是 latin-1 编码的 cookie 字符串。
        """
        if not os.path.exists(self.cookies_path):
            return ""
        try:
            with open(self.cookies_path, "rb") as f:
                content = f.read()
            return content.decode("latin-1")
        except Exception as e:
            logger.warning(f"【115客户端】读取 cookies 文件失败: {e}")
            return ""

    def _save_cookies_from_result(self, cookie_data: Any) -> bool:
        """
        将 login_qrcode_scan_result 返回的 cookie 数据保存到文件。
        cookie_data 可以是 dict {name: value} 或 cookie 字符串。
        保存格式：latin-1 编码的 cookie 字符串。
        """
        try:
            if isinstance(cookie_data, dict):
                # 转换为 "name=value; name=value" 格式
                cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_data.items())
            elif isinstance(cookie_data, str):
                cookie_str = cookie_data
            else:
                cookie_str = str(cookie_data)

            if not cookie_str:
                return False

            with open(self.cookies_path, "wb") as f:
                f.write(cookie_str.encode("latin-1"))

            logger.info(f"【115客户端】Cookie 已保存到: {self.cookies_path}")
            return True
        except Exception as e:
            logger.error(f"【115客户端】保存 Cookie 失败: {e}")
            return False

    def _reinit_client(self):
        """重新初始化客户端（登录成功后调用）"""
        try:
            self._client = P115Client(
                cookies=Path(self.cookies_path),
                check_for_relogin=False,
            )
            logger.info("【115客户端】P115Client 已重新初始化")
        except Exception as e:
            logger.warning(f"【115客户端】重新初始化失败: {e}")

    # ── 扫码登录 ──────────────────────────────────────────

    def qrcode_login(self) -> dict:
        """
        触发扫码登录全流程：
        1. 调用 P115Client.login_qrcode_token() 获取 {uid, time, sign, qrcode}
            2. 生成二维码图片并保存到 Cookie 文件同目录的 qrcode_115.png
        3. 更新 QrcodeState 状态
        4. 后台线程轮询扫码状态

        返回：dict {
            "success": bool,
            "message": str,
            "qrcode_image_path": str,
            "qrcode_base64": str,
            "qrcode_uid": str,
        }
        """
        if not _P115CLIENT_AVAILABLE or P115Client is None:
            return {"success": False, "message": "p115client 库未安装", "qrcode_image_path": "", "qrcode_url": ""}

        qr_state = QrcodeState.get_instance()
        qr_state.reset()

        try:
            # Step 1: 获取登录 token（包含 uid, time, sign, qrcode URL）
            # login_qrcode_token 是 staticmethod，不需要已登录的客户端
            resp = P115Client.login_qrcode_token()
            if not resp or not resp.get("state"):
                msg = resp.get("message", "获取二维码 token 失败") if resp else "无响应"
                return {"success": False, "message": msg, "qrcode_image_path": "", "qrcode_url": ""}

            token_data = resp.get("data", {})
            uid = token_data.get("uid", "")
            qrcode_url = token_data.get("qrcode", "") or f"https://115.com/scan/dg-{uid}"

            if not uid:
                return {"success": False, "message": "获取 uid 失败", "qrcode_image_path": "", "qrcode_url": ""}

            # Step 2: 生成二维码图片
            qrcode_image_path, qrcode_base64 = self._generate_qrcode_image(qrcode_url)
            if not qrcode_image_path:
                return {"success": False, "message": "生成二维码图片失败", "qrcode_image_path": "", "qrcode_url": qrcode_url}

            # Step 3: 更新状态
            qr_state.set_waiting(
                qrcode_url=qrcode_url,
                qrcode_image_path=qrcode_image_path,
                qrcode_uid=uid,
            )

            # Step 4: 启动后台轮询线程（传入完整 token_data 用于状态轮询）
            poll_thread = threading.Thread(
                target=self._poll_qrcode_status,
                args=(uid, token_data),
                daemon=True,
            )
            poll_thread.start()

            logger.info(f"【115扫码登录】二维码已生成: {qrcode_image_path}, uid={uid}")
            return {
                "success": True,
                "message": "二维码已生成，请使用 115 手机 APP 扫码确认",
                "qrcode_image_path": qrcode_image_path,
                "qrcode_base64": qrcode_base64,
                "qrcode_uid": uid,
            }

        except Exception as e:
            logger.error(f"【115扫码登录】异常: {e}", exc_info=True)
            qr_state.set_error(f"扫码登录异常: {e}")
            return {"success": False, "message": str(e), "qrcode_image_path": "", "qrcode_url": ""}

    def _generate_qrcode_image(self, qrcode_url: str) -> tuple:
        """
        生成二维码图片并保存到 Cookie 文件同目录，同时返回 base64 数据。
        返回: (图片路径, base64数据URL)
        """
        import base64
        import io

        try:
            import qrcode as qrcode_lib
            img = qrcode_lib.make(qrcode_url)
            # 取 PIL Image 底层对象（qrcode 的 PilImage 包装器）
            pil_img = getattr(img, "_img", img)

            output_path = os.path.join(os.path.dirname(self.cookies_path), "qrcode_115.png")
            buffer = io.BytesIO()
            pil_img.save(buffer, format="PNG")
            png_data = buffer.getvalue()
            with open(output_path, "wb") as f:
                f.write(png_data)
            logger.info(f"【115扫码登录】二维码图片已保存: {output_path}")
            img_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
            base64_url = f"data:image/png;base64,{img_base64}"

            return output_path, base64_url

        except Exception as e:
            logger.warning(f"【115扫码登录】生成二维码图片失败: {e}")
            return "", ""

    def _poll_qrcode_status(self, uid: str, token_data: dict, timeout: int = 120):
        """
        后台线程：轮询扫码状态。

        使用 P115Client.login_qrcode_scan_status(token_data) 轮询。
        token_data 格式: {uid, time, sign}（来自 login_qrcode_token 的 data 字段）

        状态码（来自 resp["data"]["status"]）：
            0  = 等待扫码
            1  = 已扫码，待手机确认
            2  = 已确认，登录成功
           -1  = 二维码已过期
           -2  = 用户已取消
        """
        qr_state = QrcodeState.get_instance()
        start_time = time.time()
        poll_interval = 2.0

        while time.time() - start_time < timeout:
            if qr_state.status in ("confirmed", "error", "expired"):
                break

            try:
                # 注意：login_qrcode_scan_status 接收完整 token_data dict
                # 参数包含 uid, time, sign（sign 是时效性签名，不能只传 uid）
                resp = P115Client.login_qrcode_scan_status(token_data)
                status_code = resp.get("data", {}).get("status")

                logger.debug(f"【115扫码轮询】uid={uid[:8]}..., status={status_code}")

                if status_code == 0:
                    pass  # 等待中
                elif status_code == 1:
                    if qr_state.status != "scanned":
                        qr_state.set_scanned()
                        logger.info("【115扫码登录】用户已扫码，等待手机确认...")
                elif status_code == 2:
                    # 已确认 — 获取 cookies
                    logger.info("【115扫码登录】确认成功，正在获取 cookies...")
                    try:
                        result = P115Client.login_qrcode_scan_result(uid, app="qandroid")
                        if result.get("state") or result.get("data"):
                            cookie_data = result.get("data", {}).get("cookie", {})
                            if cookie_data:
                                saved = self._save_cookies_from_result(cookie_data)
                                if saved:
                                    qr_state.set_confirmed(
                                        cookie_data if isinstance(cookie_data, dict) else {}
                                    )
                                    self._reinit_client()
                                    logger.info("【115扫码登录】登录成功！cookies 已保存")
                                    break
                                else:
                                    qr_state.set_error("保存 cookies 失败")
                            else:
                                # result 里可能直接有 cookie 字符串
                                cookie_str = str(result.get("data", ""))
                                if cookie_str and len(cookie_str) > 10:
                                    saved = self._save_cookies_from_result(cookie_str)
                                    if saved:
                                        qr_state.set_confirmed({})
                                        self._reinit_client()
                                        logger.info("【115扫码登录】登录成功（cookie 字符串）！")
                                        break
                                qr_state.set_error(f"获取 cookie 失败: {result}")
                        else:
                            qr_state.set_error(f"scan_result 返回异常: {result}")
                    except Exception as e:
                        logger.error(f"【115扫码登录】获取 cookies 失败: {e}", exc_info=True)
                        qr_state.set_error(f"获取 cookies 失败: {e}")
                    break
                elif status_code in (-1, -2):
                    reason = "二维码已过期" if status_code == -1 else "用户已取消"
                    qr_state.set_expired()
                    logger.warning(f"【115扫码登录】{reason}")
                    break
                else:
                    # 包括 None（API 无响应）或其他未知码
                    if status_code is not None:
                        logger.debug(f"【115扫码轮询】未知状态码: {status_code}")

            except Exception as e:
                logger.debug(f"【115扫码轮询】轮询异常（忽略）: {e}")

            time.sleep(poll_interval)

        # 超时
        if qr_state.status not in ("confirmed", "error", "expired"):
            qr_state.set_expired()
            logger.warning(f"【115扫码登录】轮询超时（{timeout}s），二维码已过期")

    # ── 登录状态查询 ──────────────────────────────────────

    def get_login_status(self) -> dict:
        """返回当前 115 登录状态。"""
        status = {
            "logged_in": False,
            "username": "",
            "cookie_file_exists": os.path.exists(self.cookies_path),
            "cookie_file_path": self.cookies_path,
            "message": "",
        }

        if not _P115CLIENT_AVAILABLE:
            status["message"] = "p115client 库未安装"
            return status

        if not os.path.exists(self.cookies_path):
            status["message"] = "未登录（无 cookies 文件）"
            return status

        try:
            with open(self.cookies_path, "rb") as f:
                content = f.read()
            cookie_str = content.decode("latin-1")

            if not cookie_str or len(cookie_str) < 10:
                status["message"] = "cookies 文件为空"
                return status

            # 从 cookie 字符串中解析 UID
            uid = ""
            for part in cookie_str.split(";"):
                part = part.strip()
                if part.upper().startswith("UID="):
                    uid = part[4:]
                    break

            if uid:
                status["logged_in"] = True
                status["username"] = f"UID:{uid}"
                status["message"] = f"已登录 (UID: {uid})"
            else:
                status["logged_in"] = True  # 有 cookies 但无 UID 字段，仍视为已登录
                status["message"] = "已登录（cookie 存在）"

        except Exception as e:
            status["message"] = f"读取 cookies 失败: {e}"

        return status

    # ── API 操作（带限流）─────────────────────────────────

    def request_with_rate_limit(
        self,
        method: str,
        url: str,
        cookies: Optional[str] = None,
        **kwargs,
    ) -> httpx.Response:
        """发起带限流的 HTTP 请求。收到 40140117 时自动退避并重试一次。"""
        self._rate_limiter.wait()

        headers = kwargs.pop("headers", {})
        if cookies:
            headers["Cookie"] = cookies
            headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            headers["Accept"] = "application/json, text/plain, */*"
            headers["Referer"] = "https://115.com/"

        max_retries = 2
        for attempt in range(max_retries):
            try:
                with httpx.Client(headers=headers, follow_redirects=True, timeout=30.0) as client:
                    if method.upper() == "GET":
                        resp = client.get(url, **kwargs)
                    else:
                        resp = client.post(url, **kwargs)

                try:
                    data = resp.json()
                    error_code = self._rate_limiter.check_error_code(data)
                    if error_code:
                        self._rate_limiter.report_rate_limit(error_code)
                        if attempt < max_retries - 1:
                            logger.warning(f"【115限流】收到 {error_code}，等待后重试...")
                            time.sleep(self._rate_limiter._interval)
                            continue
                except Exception:
                    pass

                self._rate_limiter.report_success()
                return resp

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                raise
            except httpx.ConnectError:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                raise

        raise RuntimeError("重试耗尽")

    # ── 便捷方法 ─────────────────────────────────────────

    def get_share_files(self, share_code: str, receive_code: str = "",
                        cookies: Optional[str] = None, cid: str = "0") -> List[dict]:
        """获取 115 分享文件列表（带限流）"""
        cookies = cookies or self.get_cookies_str()
        params = {
            "share_code": share_code,
            "receive_code": receive_code,
            "cid": cid,
            "limit": 200,
            "offset": 0,
        }
        url = "https://webapi.115.com/share/snap?" + urlencode(params)
        resp = self.request_with_rate_limit("GET", url, cookies=cookies)
        data = resp.json()
        if not data.get("state"):
            return []
        return data.get("data", {}).get("list", [])

    def receive_share(self, share_code: str, receive_code: str,
                      file_ids: List[str], target_cid: str,
                      cookies: Optional[str] = None) -> dict:
        """转存 115 分享（带限流）"""
        cookies = cookies or self.get_cookies_str()
        url = "https://webapi.115.com/share/receive"
        data = {
            "share_code": share_code,
            "receive_code": receive_code,
            "file_id": ",".join(file_ids),
            "cid": target_cid,
        }
        resp = self.request_with_rate_limit("POST", url, cookies=cookies, data=data)
        return resp.json()

    def check_cookie_valid(self, cookies: Optional[str] = None) -> tuple:
        """
        检查 cookie 是否有效。
        返回 (is_valid, error_message)。
        """
        cookies = cookies or self.get_cookies_str()
        if not cookies:
            return False, "Cookie 为空"

        def parse_error(data: dict) -> str:
            error_code = self._rate_limiter.check_error_code(data)
            if error_code == "40140110":
                return "Cookie 已过期（40140110）"
            if error_code == "40140117":
                return "请求过于频繁（40140117），请稍后重试"
            errno = str(data.get("errno") or data.get("errNo") or "")
            if errno == "990001":
                return "登录超时，请重新登录（990001）"
            return str(data.get("error") or data.get("error_msg") or data.get("message") or "未知错误")

        checks = [
            (
                "账号导航",
                "https://my.115.com/",
                {"ct": "ajax", "ac": "nav"},
                lambda d: bool(d.get("state") and isinstance(d.get("data"), dict) and d["data"].get("user_id")),
            ),
            (
                "根目录",
                "https://webapi.115.com/files",
                {"cid": "0", "limit": "1", "show_dir": "1"},
                lambda d: bool(d.get("state") is True or isinstance(d.get("data"), list)),
            ),
            (
                "用户信息",
                "https://webapi.115.com/users/me",
                {},
                lambda d: bool(d.get("state")),
            ),
        ]

        errors = []
        for label, url, params, is_valid in checks:
            try:
                resp = self.request_with_rate_limit("GET", url, cookies=cookies, params=params)
                data = resp.json()
                if is_valid(data):
                    return True, ""
                errors.append(f"{label}: {parse_error(data)}")
            except Exception as e:
                errors.append(f"{label}: {e}")

        return False, "；".join(errors) if errors else "Cookie 校验失败"


# ─────────────────────────────────────────────────────────
#  全局客户端实例（延迟初始化）
# ─────────────────────────────────────────────────────────
_client_manager: Optional[P115ClientManager] = None
_manager_lock = threading.Lock()


def get_client_manager() -> Optional[P115ClientManager]:
    """获取全局 P115ClientManager 单例"""
    global _client_manager
    if _client_manager is None:
        with _manager_lock:
            if _client_manager is None:
                if not _P115CLIENT_AVAILABLE:
                    logger.warning("【115客户端】p115client 未安装，无法初始化客户端管理器")
                    return None
                try:
                    _client_manager = P115ClientManager()
                except Exception as e:
                    logger.error(f"【115客户端】初始化失败: {e}")
                    return None
    return _client_manager
