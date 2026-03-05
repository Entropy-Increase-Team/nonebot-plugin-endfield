import json
import sqlite3
from typing import Any, Optional

from nonebot import get_driver, on_command
from nonebot.adapters import Event

from ..config import Config
from ..lib.api import api_request
from .user_bind import TABLE_NAME, _get_db_path


user_signin = on_command("签到")


def _get_api_key() -> Optional[str]:
	cfg = Config()
	driver = get_driver()
	return getattr(driver.config, "endfield_api_key", None) or cfg.endfield_api_key


def _get_active_binding(user_id: str) -> Optional[dict[str, Any]]:
	db_path = _get_db_path()
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
		"server_id": server_id,
	}


@user_signin.handle()
async def handle_user_signin(event: Event) -> None:
	user_id = str(event.get_user_id())
	binding = _get_active_binding(user_id)
	if not binding:
		return

	api_key = _get_api_key()
	if not api_key:
		return

	headers = {
		"x-framework-token": api_key,
		"x-api-key": binding["framework_token"],
	}
	result = api_request("POST", "/api/endfield/attendance", headers=headers)
	if isinstance(result, dict) and result.get("code") == 0:
		data = result.get("data")
		if isinstance(data, dict) and data.get("already_signed") is True:
			await user_signin.finish("今日已完成森空岛签到")
		await user_signin.finish("森空岛签到成功")
