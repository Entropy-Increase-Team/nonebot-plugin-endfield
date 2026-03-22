"""
通用工具函数模块
包含插件中多个命令共用的辅助函数
"""
import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

from nonebot import get_plugin_config

from ..config import Config


TABLE_NAME = "endfield_bindings_v3"
_PLUGIN_DATA_DIR: Path | None = None
_PLUGIN_DB_PATH: Path | None = None
PLUGIN_CONFIG = get_plugin_config(Config)


def get_data_dir() -> Path:
	"""获取插件数据目录"""
	global _PLUGIN_DATA_DIR
	if _PLUGIN_DATA_DIR is None:
		try:
			import nonebot_plugin_localstore as store

			_PLUGIN_DATA_DIR = Path(store.get_plugin_data_dir())
		except Exception:
			_PLUGIN_DATA_DIR = Path.cwd() / ".nonebot_plugin_endfield"
		_PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
	return _PLUGIN_DATA_DIR


def get_db_path() -> Path:
	"""获取数据库文件路径"""
	global _PLUGIN_DB_PATH
	if _PLUGIN_DB_PATH is None:
		_PLUGIN_DB_PATH = get_data_dir() / "endfield_bindings_v3.db"
	return _PLUGIN_DB_PATH


def get_api_key() -> Optional[str]:
	"""获取 API Key"""
	return PLUGIN_CONFIG.endfield_api_key


def build_headers(framework_token: Optional[str] = None) -> dict[str, str]:
	"""构建 API 请求头
	
	Args:
		framework_token: 可选的 framework token
		
	Returns:
		包含认证信息的请求头字典
	"""
	headers: dict[str, str] = {}
	api_key = get_api_key()
	if api_key:
		headers["x-api-key"] = api_key
	if framework_token:
		headers["x-framework-token"] = framework_token
	return headers


def get_active_binding(user_id: str) -> Optional[dict[str, Any]]:
	"""获取用户的活跃绑定信息
	
	Args:
		user_id: 用户 ID
		
	Returns:
		包含 framework_token、role_id、server_id 的字典，未找到则返回 None
	"""
	db_path = get_db_path()
	if not db_path.exists():
		return None

	try:
		with sqlite3.connect(db_path) as conn:
			row = conn.execute(
				f"""
				SELECT framework_token, role_id, server_id, binding_info
				FROM {TABLE_NAME}
				WHERE user_id = ?
				ORDER BY is_active DESC, updated_at DESC, id DESC
				LIMIT 1
				""",
				(user_id,),
			).fetchone()
	except sqlite3.OperationalError:
		return None

	if not row:
		return None

	framework_token = str(row[0]) if row[0] else None
	role_id = str(row[1]) if row[1] else None
	server_id = str(row[2]) if row[2] else None
	binding_info_raw = row[3]

	if binding_info_raw:
		try:
			binding_info = binding_info_raw if isinstance(binding_info_raw, dict) else json.loads(binding_info_raw)
			role_id = role_id or (str(binding_info.get("roleId")) if binding_info.get("roleId") else None)
			server_id = server_id or (str(binding_info.get("serverId")) if binding_info.get("serverId") else None)
		except Exception:
			pass

	if not framework_token:
		return None

	return {
		"framework_token": framework_token,
		"role_id": role_id,
		"server_id": server_id or "1",
	}
