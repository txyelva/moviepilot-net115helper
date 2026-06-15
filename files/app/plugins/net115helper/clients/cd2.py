"""
CloudDrive2 gRPC 客户端封装

通过 CloudDrive2 的 gRPC API 实现：
- 保存分享链接到指定目录
- 跨云复制文件
- 查询/等待复制任务
- 列出目录内容
- 删除文件/目录
"""

import os
import sys
import time
from typing import List, Optional

# ── 导入 gRPC 依赖 ──────────────────────────────────────────
try:
    import grpc
    from google.protobuf import empty_pb2
    # 优先从同目录导入 proto 生成文件
    _CLIENTS_DIR = os.path.dirname(os.path.abspath(__file__))
    if _CLIENTS_DIR not in sys.path:
        sys.path.insert(0, _CLIENTS_DIR)
    import clouddrive_pb2
    import clouddrive_pb2_grpc
    _GRPC_AVAILABLE = True
except ImportError as _e:
    _GRPC_AVAILABLE = False
    _GRPC_IMPORT_ERROR = str(_e)

try:
    from app.log import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


class CD2Client:
    """
    CloudDrive2 gRPC 客户端。

    用于在 阿里云盘 / 123云盘 → 115 之间做跨云转存：
      1. add_shared_link()  —— 把分享链接保存到临时目录
      2. list_dir()         —— 列出临时目录内容
      3. copy_file()        —— 跨云复制到 115
      4. wait_for_copy()    —— 等待所有复制任务完成
      5. delete_file()      —— 清理临时目录
    """

    def __init__(self, host: str, port: int, token: str):
        self._host = host
        self._port = int(port)
        self._token = self._normalize_token(token)
        self._stub = None
        self._last_error = ""

    # ── 内部辅助 ────────────────────────────────────────────

    def _get_stub(self):
        """懒加载 gRPC stub（不持久化连接，每次新建 channel 避免超时）"""
        if not _GRPC_AVAILABLE:
            raise RuntimeError(
                f"grpc 依赖未安装，无法使用 CD2Client: {_GRPC_IMPORT_ERROR}"
            )
        channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        return clouddrive_pb2_grpc.CloudDriveFileSrvStub(channel)

    @staticmethod
    def _normalize_token(token: str) -> str:
        token = str(token or "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        return token

    def _meta(self):
        """构造 gRPC 认证 metadata"""
        return [("authorization", f"Bearer {self._token}")]

    @property
    def last_error(self) -> str:
        return self._last_error

    @staticmethod
    def _format_error(e: Exception) -> str:
        if hasattr(e, "code") and hasattr(e, "details"):
            try:
                code = e.code()
                detail = e.details()
                if detail:
                    return f"{code.name if hasattr(code, 'name') else code}: {detail}"
            except Exception:
                pass
        return str(e)

    # ── 公共 API ────────────────────────────────────────────

    def test_connection(self) -> bool:
        """
        测试 CD2 连接是否正常（调用 GetAllCloudApis）。
        返回 True 表示连通，False 表示失败。
        """
        if not _GRPC_AVAILABLE:
            self._last_error = f"grpc 未安装: {_GRPC_IMPORT_ERROR}"
            logger.warning(f"【CD2Client】{self._last_error}")
            return False
        try:
            stub = self._get_stub()
            stub.GetAllCloudApis(empty_pb2.Empty(), metadata=self._meta(), timeout=10)
            self._last_error = ""
            return True
        except Exception as e:
            self._last_error = self._format_error(e)
            logger.warning(f"【CD2Client】连接测试失败: {self._last_error}")
            return False

    def get_token(self, username: str, password: str, totp_code: str = "") -> Optional[str]:
        """
        使用 CD2 登录账号换取 gRPC JWT。

        CD2「令牌管理」生成的 36 位 token 在部分版本里不能直接用于 gRPC 文件接口；
        GetToken 返回的 JWT 才能稳定用于 CopyFile/ListSubFiles 等调用。
        """
        if not _GRPC_AVAILABLE:
            self._last_error = f"grpc 未安装: {_GRPC_IMPORT_ERROR}"
            return None
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.GetTokenRequest(
                userName=str(username or "").strip(),
                password=str(password or ""),
            )
            if totp_code:
                req.totpCode = str(totp_code).strip()
            result = stub.GetToken(req, timeout=10)
            if result.success and result.token:
                self._token = self._normalize_token(result.token)
                self._last_error = ""
                return self._token
            self._last_error = result.errorMessage or "CD2 登录失败，未返回 token"
            logger.warning(f"【CD2Client】获取 token 失败: {self._last_error}")
            return None
        except Exception as e:
            self._last_error = self._format_error(e)
            logger.warning(f"【CD2Client】获取 token 异常: {self._last_error}")
            return None

    def add_shared_link(self, url: str, password: str, to_folder: str) -> bool:
        """
        把分享链接保存到 CD2 挂载目录下的指定文件夹。

        :param url:       分享链接（阿里云盘 / 123云盘 等格式）
        :param password:  分享密码（无密码传空字符串）
        :param to_folder: CD2 挂载路径，如 "/阿里云盘/MP临时转存"
        :return: 成功返回 True
        """
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.AddSharedLinkRequest(
                sharedLinkUrl=url,
                sharedPassword=password or "",
                toFolder=to_folder,
            )
            # AddSharedLink returns google.protobuf.Empty on success; raises on failure
            stub.AddSharedLink(req, metadata=self._meta(), timeout=60)
            logger.info(f"【CD2Client】分享链接保存成功: {url[:60]}")
            return True
        except Exception as e:
            logger.error(f"【CD2Client】add_shared_link 异常: {e}")
            return False

    def create_folder(self, parent_path: str, folder_name: str) -> bool:
        """
        在 parent_path 下创建名为 folder_name 的目录。
        已存在时也返回 True（幂等）。
        """
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.CreateFolderRequest(
                parentPath=parent_path,
                folderName=folder_name,
            )
            stub.CreateFolder(req, metadata=self._meta(), timeout=30)
            self._last_error = ""
            return True
        except Exception as e:
            self._last_error = self._format_error(e)
            err_lower = self._last_error.lower()
            if "already exists" in err_lower or "file exists" in err_lower or "已存在" in self._last_error:
                return True
            import sys as _sys
            _sys.stdout.write(f"【CD2】create_folder 异常: {self._last_error}\n"); _sys.stdout.flush()
            return False

    def copy_file(self, src_paths: List[str], dest_path: str) -> bool:
        """
        跨云复制文件/文件夹。

        :param src_paths: 源路径列表，如 ["/阿里云盘/MP临时转存/电影名"]
        :param dest_path: 目标目录路径，如 "/115/待整理电影"
        :return: gRPC 调用成功返回 True（实际复制是异步的，需用 wait_for_copy 等待）
        """
        if not src_paths:
            return False
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.CopyFileRequest(
                theFilePaths=src_paths,
                destPath=dest_path,
                conflictPolicy=clouddrive_pb2.CopyFileRequest.Rename,
            )
            result = stub.CopyFile(req, metadata=self._meta(), timeout=30)
            import sys as _sys
            if result.success:
                _sys.stdout.write(f"【CD2】复制任务已提交: {len(src_paths)} 个文件 -> {dest_path}\n"); _sys.stdout.flush()
                return True
            else:
                _sys.stdout.write(f"【CD2】复制任务提交失败: {result.errorMessage!r}\n"); _sys.stdout.flush()
                return False
        except Exception as e:
            import sys as _sys
            _sys.stdout.write(f"【CD2】copy_file 异常: {e}\n"); _sys.stdout.flush()
            return False

    def get_copy_tasks(self) -> List[dict]:
        """
        获取所有复制任务状态列表。

        返回 list of dict，每条包含:
          - source_path: str
          - dest_path: str
          - status: int  (0=Pending,1=Scanning,2=Scanned,3=Completed,4=Failed)
          - status_name: str
          - total_files: int
          - uploaded_files: int
          - failed_files: int
          - paused: bool
          - errors: list[str]
        """
        try:
            stub = self._get_stub()
            result = stub.GetCopyTasks(
                empty_pb2.Empty(), metadata=self._meta(), timeout=15
            )
            tasks = []
            for t in result.copyTasks:
                # TaskStatus: Pending=0, Scanning=1, Scanned=2, Completed=3, Failed=4
                status_name = clouddrive_pb2.CopyTask.TaskStatus.Name(t.status)
                tasks.append(
                    {
                        "source_path": t.sourcePath,
                        "dest_path": t.destPath,
                        "status": t.status,
                        "status_name": status_name,
                        "total_files": t.totalFiles,
                        "uploaded_files": t.uploadedFiles,
                        "failed_files": t.failedFiles,
                        "paused": t.paused,
                        "errors": list(t.errors),
                    }
                )
            return tasks
        except Exception as e:
            logger.error(f"【CD2Client】get_copy_tasks 异常: {e}")
            return []

    def wait_for_copy(self, timeout: int = 300, poll_interval: int = 5) -> bool:
        """
        轮询等待所有复制任务完成。

        :param timeout:       最长等待秒数（默认 300 秒）
        :param poll_interval: 每次轮询间隔秒数（默认 5 秒）
        :return: 所有任务成功完成返回 True；超时或有失败任务返回 False
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            tasks = self.get_copy_tasks()
            if not tasks:
                # 任务列表为空，可能已全部完成并被清理
                logger.info("【CD2Client】复制任务列表为空，视为完成")
                return True

            # 分析任务状态
            # Completed=3, Failed=4
            completed = [t for t in tasks if t["status"] == 3]
            failed = [t for t in tasks if t["status"] == 4]
            pending = [
                t for t in tasks if t["status"] not in (3, 4)
            ]

            logger.debug(
                f"【CD2Client】复制进度: 完成={len(completed)}, "
                f"失败={len(failed)}, 进行中={len(pending)}"
            )

            if failed:
                for t in failed:
                    logger.warning(
                        f"【CD2Client】复制任务失败: {t['source_path']} -> "
                        f"{t['dest_path']}, 错误: {t['errors']}"
                    )

            if not pending:
                # 所有任务都已终止（完成或失败）
                success = len(failed) == 0
                if success:
                    logger.info(
                        f"【CD2Client】所有复制任务完成 ({len(completed)} 个)"
                    )
                else:
                    logger.warning(
                        f"【CD2Client】复制完成但有失败: "
                        f"成功={len(completed)}, 失败={len(failed)}"
                    )
                return success

            time.sleep(poll_interval)

        logger.warning(
            f"【CD2Client】等待复制任务超时 ({timeout}s)，仍有进行中任务"
        )
        return False

    def list_dir(self, path: str, force_refresh: bool = False) -> List[dict]:
        """
        列出目录内容。

        :param path:          CD2 挂载路径，如 "/阿里云盘/MP临时转存"
        :param force_refresh: 是否强制刷新（跳过缓存）
        :return: list of dict，每条包含 name, path, is_dir, size
        """
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.ListSubFileRequest(
                path=path,
                forceRefresh=force_refresh,
            )
            files = []
            for resp in stub.GetSubFiles(req, metadata=self._meta(), timeout=30):
                for f in resp.subFiles:
                    files.append(
                        {
                            "name": f.name,
                            "path": f.fullPathName,
                            "is_dir": f.isDirectory,
                            "size": f.size,
                        }
                    )
            self._last_error = ""
            return files
        except Exception as e:
            self._last_error = self._format_error(e)
            logger.error(f"【CD2Client】list_dir 异常 ({path}): {self._last_error}")
            return []

    def rename_file(self, path: str, new_name: str) -> bool:
        """
        重命名文件（只改文件名，不移动）。

        :param path: CD2 挂载路径（原路径）
        :param new_name: 新文件名（不含路径）
        :return: 成功返回 True
        """
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.RenameFileRequest(theFilePath=path, newName=new_name)
            result = stub.RenameFile(req, metadata=self._meta(), timeout=30)
            if result.success:
                logger.info(f"【CD2Client】重命名成功: {path} → {new_name}")
                return True
            else:
                logger.warning(
                    f"【CD2Client】重命名失败: {path} | {result.errorMessage}"
                )
                return False
        except Exception as e:
            logger.error(f"【CD2Client】rename_file 异常 ({path}): {e}")
            return False

    def delete_file(self, path: str) -> bool:
        """
        删除文件或目录（移入回收站）。

        :param path: CD2 挂载路径
        :return: 成功返回 True
        """
        try:
            stub = self._get_stub()
            req = clouddrive_pb2.FileRequest(path=path)
            result = stub.DeleteFile(req, metadata=self._meta(), timeout=30)
            if result.success:
                logger.info(f"【CD2Client】删除成功: {path}")
                return True
            else:
                logger.warning(
                    f"【CD2Client】删除失败: {path} | {result.errorMessage}"
                )
                return False
        except Exception as e:
            logger.error(f"【CD2Client】delete_file 异常 ({path}): {e}")
            return False
