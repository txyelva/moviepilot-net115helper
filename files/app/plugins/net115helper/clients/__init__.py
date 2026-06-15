"""115 网盘客户端模块"""

from .p115 import (
    P115ClientManager,
    QrcodeState,
    RateLimiter,
    get_client_manager,
    COOKIES_PATH,
)

try:
    from .cd2 import CD2Client
    _CD2_AVAILABLE = True
except ImportError:
    CD2Client = None  # type: ignore
    _CD2_AVAILABLE = False

try:
    from .aliyun import AliyunClient
    _ALIYUN_AVAILABLE = True
except ImportError:
    AliyunClient = None  # type: ignore
    _ALIYUN_AVAILABLE = False

try:
    from .quark import QuarkClient
    _QUARK_AVAILABLE = True
except ImportError:
    QuarkClient = None  # type: ignore
    _QUARK_AVAILABLE = False

try:
    from .baidu import BaiduClient
    _BAIDU_AVAILABLE = True
except ImportError:
    BaiduClient = None  # type: ignore
    _BAIDU_AVAILABLE = False

__all__ = [
    "P115ClientManager",
    "QrcodeState",
    "RateLimiter",
    "get_client_manager",
    "COOKIES_PATH",
    "CD2Client",
    "AliyunClient",
    "QuarkClient",
    "BaiduClient",
]
