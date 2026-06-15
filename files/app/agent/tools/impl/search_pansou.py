"""搜索网盘资源工具 - 通过 PanSou 搜索 115/阿里云盘/夸克等网盘资源"""

import json
from typing import List, Optional, Type

import httpx
from pydantic import BaseModel, Field

from app.agent.tools.base import MoviePilotTool
from app.core.config import settings
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger


class SearchPansouInput(BaseModel):
    """搜索网盘资源工具的输入参数模型"""
    explanation: str = Field(..., description="Clear explanation of why this tool is being used in the current context")
    keyword: str = Field(..., description="Search keyword - the movie or TV show name ONLY, without year or other metadata. "
                                       "Examples: '流浪地球3', '三体', '复仇者联盟3'. "
                                       "Do NOT append year (e.g., use '菜肉馄饨' not '菜肉馄饨 2025').")
    cloud_types: Optional[List[str]] = Field(
        None,
        description="Filter by cloud storage type. Options: '115', 'aliyun', 'quark', 'baidu', 'tianyi', 'uc', 'pikpak', 'xunlei', '123', 'magnet', 'ed2k'. "
                    "Use ['115'] to find only 115 cloud resources for saving to your account."
    )
    include_keywords: Optional[List[str]] = Field(
        None,
        description="Include filter keywords (OR logic). E.g., ['4K', '2160p', 'HDR', '蓝光'] to find high quality resources, ['合集', '全集'] for complete collections."
    )
    exclude_keywords: Optional[List[str]] = Field(
        None,
        description="Exclude filter keywords (OR logic). E.g., ['预告', '花絮', 'CAM', '枪版'] to skip low quality or unrelated content."
    )


class SearchPansouTool(MoviePilotTool):
    name: str = "search_pansou"
    description: str = (
        "Search for media resources across cloud storage platforms (115网盘, 阿里云盘, 夸克网盘, 百度网盘, etc.) using PanSou search service. "
        "Returns share links with extraction codes that can be used with 'save_115_share' tool. "
        "Best for finding resources on 115 cloud storage for transfer to your own account. "
        "Use cloud_types=['115'] to find only 115 resources. "
        "Requires PanSou URL to be configured in the Net115Helper plugin or MoviePilot settings."
    )
    args_schema: Type[BaseModel] = SearchPansouInput

    @staticmethod
    def _get_pansou_config() -> dict:
        """优先读取 115网盘助手插件配置，回退到 MoviePilot 全局设置。"""
        try:
            plugin_config = SystemConfigOper().get("plugin.Net115Helper")
            if plugin_config and isinstance(plugin_config, dict) and plugin_config.get("enabled"):
                pansou_url = str(plugin_config.get("pansou_url") or "").strip()
                if pansou_url:
                    return {
                        "url": pansou_url,
                        "auth_user": str(plugin_config.get("pansou_auth_user") or "").strip(),
                        "auth_pass": str(plugin_config.get("pansou_auth_pass") or ""),
                    }
        except Exception as e:
            logger.debug(f"读取 115网盘助手 PanSou 配置失败: {e}")

        return {
            "url": str(getattr(settings, "PANSOU_URL", "") or "").strip(),
            "auth_user": str(getattr(settings, "PANSOU_AUTH_USER", "") or "").strip(),
            "auth_pass": str(getattr(settings, "PANSOU_AUTH_PASS", "") or ""),
        }

    def get_tool_message(self, **kwargs) -> Optional[str]:
        keyword = kwargs.get("keyword", "")
        cloud_types = kwargs.get("cloud_types")
        message = f"正在搜索网盘资源: {keyword}"
        if cloud_types:
            message += f" [平台: {', '.join(cloud_types)}]"
        return message

    async def run(self, keyword: str,
                  cloud_types: Optional[List[str]] = None,
                  include_keywords: Optional[List[str]] = None,
                  exclude_keywords: Optional[List[str]] = None,
                  **kwargs) -> str:
        logger.info(
            f"执行工具: {self.name}, 参数: keyword={keyword}, cloud_types={cloud_types}, "
            f"include={include_keywords}, exclude={exclude_keywords}"
        )

        # 检查配置
        pansou_config = self._get_pansou_config()
        pansou_url = pansou_config["url"]
        if not pansou_url:
            return "错误: 未配置 PanSou 搜索服务地址，请在「115网盘助手」插件设置或 MoviePilot 全局设置中配置。"

        try:
            pansou_url = pansou_url.rstrip("/")
            headers = {"Content-Type": "application/json"}

            # 如果配置了认证，先获取 Token
            pansou_auth_user = pansou_config["auth_user"]
            pansou_auth_pass = pansou_config["auth_pass"]
            if pansou_auth_user and pansou_auth_pass:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    auth_resp = await client.post(
                        f"{pansou_url}/api/auth/login",
                        json={
                            "username": pansou_auth_user,
                            "password": pansou_auth_pass,
                        },
                    )
                    auth_data = auth_resp.json()
                    token = auth_data.get("token")
                    if token:
                        headers["Authorization"] = f"Bearer {token}"

            # 构建搜索请求
            search_body = {
                "kw": keyword,
                "res": "merge",
            }
            if cloud_types:
                search_body["cloud_types"] = cloud_types
            if include_keywords or exclude_keywords:
                filter_config = {}
                if include_keywords:
                    filter_config["include"] = include_keywords
                if exclude_keywords:
                    filter_config["exclude"] = exclude_keywords
                search_body["filter"] = filter_config

            # ── 第 1 步: merge 模式搜索 ──
            all_results = []
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{pansou_url}/api/search",
                    headers=headers,
                    json=search_body,
                )

                if resp.status_code != 200:
                    return f"搜索失败，HTTP 状态码: {resp.status_code}"

                resp_json = resp.json()

            # 正确解析嵌套结构: {"code": 0, "data": {"merged_by_type": {...}}}
            resp_data = resp_json.get("data", {})
            if isinstance(resp_data, dict):
                merged = resp_data.get("merged_by_type", {})
                if merged:
                    logger.info(f"PanSou merge 分组: {list(merged.keys())}")
                    for cloud_type, links in merged.items():
                        if not links:
                            continue
                        for link in links[:10]:
                            all_results.append({
                                "platform": cloud_type,
                                "url": link.get("url", ""),
                                "password": link.get("password", ""),
                                "description": link.get("note", ""),
                                "date": link.get("datetime", ""),
                                "source": link.get("source", ""),
                            })
                else:
                    # 尝试 list/items 格式
                    items = resp_data.get("list", resp_data.get("items", resp_data.get("results", [])))
                    if isinstance(items, list):
                        for link in items[:30]:
                            all_results.append({
                                "platform": link.get("cloud_type", link.get("type", "unknown")),
                                "url": link.get("url", ""),
                                "password": link.get("password", ""),
                                "description": link.get("note", ""),
                                "date": link.get("datetime", ""),
                                "source": link.get("source", ""),
                            })

            # ── 第 2 步: 如果指定了 115 但没找到，去掉 cloud_types 重搜 ──
            want_115 = cloud_types and "115" in cloud_types
            has_115 = any(
                "115" in r.get("url", "") or "anxia" in r.get("url", "") or
                "115cdn" in r.get("url", "") or "115" in r.get("platform", "")
                for r in all_results
            )

            if want_115 and not has_115 and all_results:
                logger.info(f"PanSou merge 无 115 结果, 从 {len(all_results)} 个结果中筛选 115 链接")
                filtered_115 = [
                    r for r in all_results
                    if "115" in r.get("url", "") or "anxia" in r.get("url", "") or
                       "115cdn" in r.get("url", "") or "115" in r.get("platform", "")
                ]
                if filtered_115:
                    all_results = filtered_115

            if want_115 and not has_115:
                # merge 模式没有 115, 尝试不带 cloud_types 和 res=merge 的普通搜索
                logger.info(f"PanSou merge 无 115, 尝试普通搜索...")
                plain_body = {"kw": keyword}
                if include_keywords or exclude_keywords:
                    plain_filter = {}
                    if include_keywords:
                        plain_filter["include"] = include_keywords
                    if exclude_keywords:
                        plain_filter["exclude"] = exclude_keywords
                    plain_body["filter"] = plain_filter

                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        resp2 = await client.post(
                            f"{pansou_url}/api/search",
                            headers=headers,
                            json=plain_body,
                        )
                    if resp2.status_code == 200:
                        resp2_json = resp2.json()
                        resp2_data = resp2_json.get("data", {})
                        plain_items = []
                        if isinstance(resp2_data, dict):
                            merged2 = resp2_data.get("merged_by_type", {})
                            if merged2:
                                for ct, links in merged2.items():
                                    for link in (links or []):
                                        plain_items.append(link)
                            else:
                                plain_items = resp2_data.get("list", resp2_data.get("items", resp2_data.get("results", [])))
                        elif isinstance(resp2_data, list):
                            plain_items = resp2_data

                        for link in (plain_items or []):
                            url = link.get("url", "")
                            if "115" in url or "anxia" in url or "115cdn" in url:
                                all_results.append({
                                    "platform": "115",
                                    "url": url,
                                    "password": link.get("password", ""),
                                    "description": link.get("note", ""),
                                    "date": link.get("datetime", ""),
                                    "source": link.get("source", ""),
                                })
                        logger.info(f"PanSou 普通搜索: 找到 {len(all_results)} 个结果")
                except Exception as e2:
                    logger.warning(f"PanSou 普通搜索失败: {e2}")

            if not all_results:
                return f"未找到「{keyword}」的网盘资源。建议尝试不同的搜索关键词（如中文/英文标题、去掉年份）。"

            # ── 如果指定 115 且有 115 结果，只返回 115 ──
            if want_115:
                results_115 = [
                    r for r in all_results
                    if "115" in r.get("url", "") or "anxia" in r.get("url", "") or
                       "115cdn" in r.get("url", "") or "115" in r.get("platform", "")
                ]
                if results_115:
                    all_results = results_115

            # 限制总结果数
            total_results = len(all_results)
            results = all_results[:30]

            result_json = json.dumps(results, ensure_ascii=False, indent=2)

            summary = f"找到 {total_results} 条「{keyword}」的网盘资源"
            if total_results > 30:
                summary += f"（显示前 30 条）"
            summary += "。\n\n"

            has_115_final = any(
                "115" in r.get("url", "") or "115cdn" in r.get("url", "") or
                "115" in r.get("platform", "")
                for r in results
            )
            if has_115_final:
                summary += "💡 包含 115 网盘资源，可使用 save_115_share 工具将分享链接转存到你的 115 账号。\n\n"

            return summary + result_json

        except httpx.TimeoutException:
            return "搜索超时，PanSou 服务响应时间过长，请稍后重试。"
        except httpx.ConnectError:
            return f"无法连接到 PanSou 服务 ({pansou_url})，请检查服务地址和网络连接。"
        except Exception as e:
            error_message = f"搜索网盘资源失败: {str(e)}"
            logger.error(f"搜索网盘资源失败: {e}", exc_info=True)
            return error_message
