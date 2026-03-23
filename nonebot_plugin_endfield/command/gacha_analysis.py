import asyncio
import base64
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from nonebot import get_driver, on_command, on_message, logger
from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11 import GroupMessageEvent, MessageEvent, MessageSegment
from nonebot.exception import FinishedException
import nonebot_plugin_localstore as store

from ..config import Config
from ..lib.api import api_request
from ..lib.render import render_gacha_analysis_image, render_gacha_global_stats_image, render_gacha_records_image
from ..lib.utils import TABLE_NAME, get_db_path, get_data_dir, get_api_key, build_headers, get_active_binding


POLL_INTERVAL_SECONDS = 1.5
POLL_TIMEOUT_SECONDS = 180
PENDING_SELECT_TTL_SECONDS = 300
GACHA_POOLS = ("limited", "standard", "beginner", "weapon")
_DATA_DIR_LOGGED = False


gacha_records = on_command("终末地抽卡记录", aliases={"终末地同步抽卡记录", "终末地更新抽卡记录"}, priority=30, block=True)
gacha_analysis = on_command("终末地抽卡分析", priority=30, block=True)
gacha_global = on_command("终末地全服抽卡统计", priority=30, block=True)
gacha_sync_all = on_command("终末地同步全部抽卡", priority=30, block=True)
gacha_select = on_message(priority=29, block=False)


def _load_all_bindings() -> dict[str, list[dict[str, Any]]]:
	db_path = get_db_path()
	if not db_path.exists():
		return {}
	result: dict[str, list[dict[str, Any]]] = {}
	with sqlite3.connect(db_path) as conn:
		rows = conn.execute(
			f"""
			SELECT user_id, framework_token, role_id, server_id, binding_info, is_active
			FROM {TABLE_NAME}
			ORDER BY user_id ASC, is_active DESC, updated_at DESC, id DESC
			"""
		).fetchall()
	for row in rows:
		user_id = str(row[0])
		framework_token = str(row[1]) if row[1] else ""
		role_id = str(row[2]) if row[2] else None
		server_id = str(row[3]) if row[3] else "1"
		binding_info_raw = row[4]
		is_active = bool(row[5])
		if binding_info_raw:
			try:
				info = json.loads(binding_info_raw) if isinstance(binding_info_raw, str) else dict(binding_info_raw)
				role_id = role_id or (str(info.get("roleId")) if info.get("roleId") else None)
				server_id = server_id or (str(info.get("serverId")) if info.get("serverId") else "1")
			except Exception:
				pass
		if not framework_token:
			continue
		result.setdefault(user_id, []).append(
			{
				"framework_token": framework_token,
				"role_id": role_id,
				"server_id": server_id,
				"is_active": is_active,
			}
		)
	return result


def _cache_dir() -> Path:
	global _DATA_DIR_LOGGED
	d = get_data_dir() / "gacha"
	d.mkdir(parents=True, exist_ok=True)
	if not _DATA_DIR_LOGGED:
		logger.debug(f"[终末地插件][抽卡缓存]本地数据目录: {get_data_dir().resolve()}")
		_DATA_DIR_LOGGED = True
	return d


def _cache_file(user_id: str, role_id: str) -> Path:
	uid = str(user_id or "0")
	rid = str(role_id or "0")
	return _cache_dir() / f"{uid}_{rid}.json"


def _read_gacha_cache(user_id: str, role_id: str) -> Optional[dict[str, Any]]:
	file = _cache_file(user_id, role_id)
	if not file.exists():
		return None
	try:
		raw = file.read_text("utf-8")
		if not raw.strip():
			return None
		data = json.loads(raw)
		return data if isinstance(data, dict) else None
	except Exception as e:
		logger.warning(f"[终末地插件][抽卡缓存]读取失败: {e}")
		return None


def _write_gacha_cache(user_id: str, role_id: str, payload: dict[str, Any]) -> bool:
	try:
		file = _cache_file(user_id, role_id)
		file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
		return True
	except Exception as e:
		logger.warning(f"[终末地插件][抽卡缓存]写入失败: {e}")
		return False


def _pending_file() -> Path:
	return get_data_dir() / "gacha_pending_select.json"


def _load_pending_state() -> dict[str, Any]:
	path = _pending_file()
	if not path.exists():
		return {}
	try:
		return json.loads(path.read_text("utf-8"))
	except Exception:
		return {}


def _save_pending_state(state: dict[str, Any]) -> None:
	path = _pending_file()
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")


def _set_pending(user_id: str, data: dict[str, Any]) -> None:
	state = _load_pending_state()
	state[str(user_id)] = data
	_save_pending_state(state)


def _get_pending(user_id: str) -> Optional[dict[str, Any]]:
	state = _load_pending_state()
	data = state.get(str(user_id))
	if not data:
		return None
	ts = float(data.get("timestamp", 0) or 0)
	if ts <= 0 or (time.time() - ts) > PENDING_SELECT_TTL_SECONDS:
		state.pop(str(user_id), None)
		_save_pending_state(state)
		return None
	return data


def _clear_pending(user_id: str) -> None:
	state = _load_pending_state()
	if str(user_id) in state:
		state.pop(str(user_id), None)
		_save_pending_state(state)


def _parse_stats_has_records(stats_data: Optional[dict[str, Any]]) -> bool:
	if not stats_data:
		return False
	if stats_data.get("has_records") is True:
		return True
	last_fetch = stats_data.get("last_fetch")
	if last_fetch is not None and str(last_fetch).strip() != "":
		return True
	total_count = (((stats_data.get("stats") or {}).get("total_count")) or 0)
	return int(total_count) > 0


def _pool_records(cache_data: dict[str, Any], pool_key: str) -> list[dict[str, Any]]:
	pools = cache_data.get("records_by_pool") or {}
	rows = pools.get(pool_key)
	return rows if isinstance(rows, list) else []


def _pool_page(cache_data: dict[str, Any], pool_key: str, page: int = 1, limit: int = 10) -> dict[str, Any]:
	rows = _pool_records(cache_data, pool_key)
	sorted_rows = sorted(
		rows,
		key=lambda x: (
			-(int(x.get("seq_id")) if str(x.get("seq_id", "")).isdigit() else 0),
			-(int(x.get("gacha_ts") or 0)),
		),
	)
	total = len(sorted_rows)
	pages = max(1, (total + limit - 1) // limit)
	current = max(1, min(page, pages))
	start = (current - 1) * limit
	return {
		"records": sorted_rows[start : start + limit],
		"total": total,
		"pages": pages,
		"page": current,
	}


def _get_account_server_id(account: dict[str, Any]) -> str:
	sid = account.get("server_id") or account.get("serverId") or account.get("game_server_id") or 1
	return str(sid or 1)


def _format_progress_msg(msg: str, user_id: str, user_name: str) -> str:
	text = str(msg or "")
	uid = str(user_id or "")
	name = str(user_name or uid or "用户")
	return text.replace("{qq号}", uid).replace("{qqname}", name)


def _get_sender_display_name(event: MessageEvent, fallback: str) -> str:
	sender = getattr(event, "sender", None)
	if not sender:
		return fallback
	card = getattr(sender, "card", None)
	if card:
		return str(card)
	nickname = getattr(sender, "nickname", None)
	if nickname:
		return str(nickname)
	return fallback


def _unwrap_response_data(resp: Optional[dict[str, Any]]) -> Any:
	if isinstance(resp, dict) and isinstance(resp.get("data"), dict):
		return resp["data"]
	return resp


async def _fetch_note_user_overrides(framework_token: str) -> dict[str, str]:
	try:
		note_data = await _api_get("/api/endfield/note", framework_token)
		note_inner = _unwrap_response_data(note_data)
		note_payload = note_inner if isinstance(note_inner, dict) else {}
		base = note_payload.get("base") if isinstance(note_payload.get("base"), dict) else {}
		avatar_url = str(base.get("avatarUrl") or base.get("avatar") or "").strip()
		nickname = str(base.get("name") or base.get("nickname") or "").strip()
		game_uid = str(base.get("uid") or base.get("roleId") or "").strip()
		overrides: dict[str, str] = {}
		if avatar_url:
			overrides["avatar_url"] = avatar_url
		if nickname:
			overrides["nickname"] = nickname
		if game_uid:
			overrides["game_uid"] = game_uid
		return overrides
	except Exception:
		return {}


async def _api_get(path: str, framework_token: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
	p = path
	if params:
		query = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
		if query:
			sep = "&" if "?" in path else "?"
			p = f"{path}{sep}{query}"
	return await api_request("GET", p, headers=build_headers(framework_token))


async def _api_post(path: str, framework_token: Optional[str] = None, data: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
	return await api_request("POST", path, headers=build_headers(framework_token), data=data or {})


async def _fetch_gacha_icon_map(framework_token: Optional[str] = None) -> dict[str, str]:
	try:
		def _normalize_icon_url(raw: str) -> str:
			url = str(raw or "").strip()
			if not url:
				return ""

			def _escape_http_url_path(v: str) -> str:
				try:
					parts = urlsplit(v)
					if parts.scheme not in ("http", "https"):
						return v
					escaped_path = quote(parts.path or "", safe="/%:@!$&'()*+,;=-._~")
					return urlunsplit((parts.scheme, parts.netloc, escaped_path, parts.query, parts.fragment))
				except Exception:
					return v

			if url.startswith("//"):
				return _escape_http_url_path("https:" + url)
			if url.startswith(("http://", "https://", "file:///", "data:")):
				return _escape_http_url_path(url)
			base = str(get_driver().config.dict().get("endfield_api_baseurl") or "").strip()
			if not base:
				return url
			if url.startswith("/"):
				return _escape_http_url_path(f"{base.rstrip('/')}{url}")
			return _escape_http_url_path(f"{base.rstrip('/')}/{url.lstrip('/')}")

		def _walk_items(node: Any) -> list[dict[str, Any]]:
			out: list[dict[str, Any]] = []
			if isinstance(node, list):
				for it in node:
					out.extend(_walk_items(it))
				return out
			if isinstance(node, dict):
				out.append(node)
				for key in (
					"data",
					"list",
					"items",
					"results",
					"operators",
					"chars",
					"weapons",
					"pool_chars",
					"poolChars",
					"pools",
					"records",
				):
					value = node.get(key)
					if isinstance(value, (list, dict)):
						out.extend(_walk_items(value))
			return out

		icon_map: dict[str, str] = {}

		async def _collect_from_endpoint(
			path: str,
			params: Optional[dict[str, Any]] = None,
			*,
			prefer_icon_url: bool = False,
		) -> None:
			res = await _api_get(path, framework_token, params)
			for item in _walk_items(res):
				name = str(
					item.get("name")
					or item.get("weaponName")
					or item.get("weapon_name")
					or item.get("name_cn")
					or item.get("char_name")
					or item.get("item_name")
					or ""
				).strip()
				icon_raw = ""
				if prefer_icon_url:
					icon_raw = str(
						item.get("iconUrl")
						or item.get("icon_url")
						or item.get("icon")
						or item.get("avatarSqUrl")
						or item.get("avatar_sq_url")
						or item.get("avatarRtUrl")
						or item.get("avatar_rt_url")
						or item.get("avatarUrl")
						or item.get("avatar_url")
						or item.get("image")
						or item.get("cover")
						or ""
					)
				else:
					icon_raw = str(
						item.get("avatarSqUrl")
						or item.get("avatar_sq_url")
						or item.get("avatarRtUrl")
						or item.get("avatar_rt_url")
						or item.get("avatarUrl")
						or item.get("avatar_url")
						or item.get("iconUrl")
						or item.get("icon_url")
						or item.get("icon")
						or item.get("image")
						or item.get("cover")
						or ""
					)
				icon = _normalize_icon_url(
					icon_raw
				)
				if name and icon and name not in icon_map:
					icon_map[name] = icon

		# 角色/武器基础图标
		await _collect_from_endpoint("/api/endfield/search/chars")
		await _collect_from_endpoint("/api/endfield/search/weapons", prefer_icon_url=True)
		# 卡池角色/武器图标（包含武器头像）
		for params in (
			None,
			{"pool_type": "weapon"},
			{"pool_type": "limited"},
			{"pool_type": "standard"},
			{"pool_type": "beginner"},
		):
			await _collect_from_endpoint("/api/endfield/gacha/pool-chars", params)
		logger.debug(f"[终末地插件][抽卡图标]已加载 icon_map 条目数: {len(icon_map)}")

		return icon_map
	except Exception:
		return {}


async def _refresh_local_cache_from_cloud(framework_token: str, user_id: str, role_id: str) -> bool:
	for attempt in range(1, 4):
		try:
			stats_data = await _api_get("/api/endfield/gacha/stats", framework_token)
			if not stats_data or stats_data.get("code") not in (0, None):
				if attempt < 3:
					await asyncio.sleep(1.5)
					continue
				return False

			records_by_pool: dict[str, list[dict[str, Any]]] = {}
			records_total = 0
			for pool in GACHA_POOLS:
				all_rows: list[dict[str, Any]] = []
				page = 1
				while True:
					res = await _api_get(
						"/api/endfield/gacha/records",
						framework_token,
						{"pools": pool, "page": page, "limit": 500},
					)
					data = (res or {}).get("data") or {}
					rows = data.get("records") or []
					if isinstance(rows, list):
						all_rows.extend(rows)
					total_pages = int(data.get("total_pages") or data.get("pages") or 1)
					if page >= max(1, total_pages):
						break
					page += 1
				records_by_pool[pool] = all_rows
				records_total += len(all_rows)

			stats_payload = (stats_data.get("data") if isinstance(stats_data.get("data"), dict) else stats_data) or {}
			user_info = stats_payload.get("user_info") if isinstance(stats_payload.get("user_info"), dict) else {}

			note_overrides = await _fetch_note_user_overrides(framework_token)
			if note_overrides:
				note_user_info = dict(user_info)
				if note_overrides.get("nickname"):
					note_user_info["nickname"] = note_overrides["nickname"]
				if note_overrides.get("avatar_url"):
					note_user_info["avatar_url"] = note_overrides["avatar_url"]
				if note_overrides.get("game_uid") and not note_user_info.get("game_uid"):
					note_user_info["game_uid"] = note_overrides["game_uid"]
				if note_user_info != user_info:
					user_info = note_user_info
					stats_payload = dict(stats_payload)
					stats_payload["user_info"] = user_info

			try:
				up_info = await _get_bili_current_up(framework_token)
				if isinstance(up_info, dict):
					stats_payload = dict(stats_payload)
					stats_payload["up_info"] = up_info
			except Exception:
				pass

			gacha_icon_map = await _fetch_gacha_icon_map(framework_token)

			payload = {
				"version": 1,
				"user_id": str(user_id),
				"role_id": str(role_id),
				"updated_at": int(time.time() * 1000),
				"stats_data": stats_payload,
				"records_by_pool": records_by_pool,
				"gacha_icon_map": gacha_icon_map,
			}
			if not _write_gacha_cache(user_id, role_id, payload):
				return False

			stats_total = int((((payload.get("stats_data") or {}).get("stats") or {}).get("total_count") or 0))
			if records_total == 0 and stats_total == 0 and attempt < 3:
				await asyncio.sleep(1.5)
				continue
			return True
		except Exception as e:
			logger.warning(f"[终末地插件][抽卡缓存]云端刷新失败(第{attempt}次): {e}")
			if attempt < 3:
				await asyncio.sleep(1.5)
				continue
			return False
	return False


def _simple_analysis_text(stats_data: dict[str, Any], cache_data: dict[str, Any]) -> str:
	pool_stats = stats_data.get("pool_stats") or {}
	user_info = stats_data.get("user_info") or {}
	overall_stats = stats_data.get("stats") or {}

	def _get_pool(name1: str, name2: str) -> dict[str, Any]:
		return (pool_stats.get(name1) or pool_stats.get(name2) or {})

	limited = _get_pool("limited_char", "limited")
	standard = _get_pool("standard_char", "standard")
	beginner = _get_pool("beginner_char", "beginner")
	weapon = _get_pool("weapon", "weapon")

	def _fmt_rate(total: int, star6: int) -> str:
		if star6 <= 0:
			return "-"
		return f"{round(total / star6)}抽"

	lines = ["【抽卡分析】"]
	lines.append(f"角色：{user_info.get('nickname') or user_info.get('game_uid') or '未知'}")
	lines.append(
		f"总抽数：{overall_stats.get('total_count', 0)} | 六星：{overall_stats.get('star6_count', 0)} | "
		f"五星：{overall_stats.get('star5_count', 0)} | 四星：{overall_stats.get('star4_count', 0)}"
	)

	for label, data in (
		("限定池", limited),
		("常驻池", standard),
		("新手池", beginner),
		("武器池", weapon),
	):
		total = int(data.get("total") or data.get("total_count") or 0)
		star6 = int(data.get("star6") or data.get("star6_count") or 0)
		lines.append(f"{label}：{total} 抽 | 每红花费 {_fmt_rate(total, star6)}")

	cache_updated = cache_data.get("updated_at")
	if cache_updated:
		lines.append(f"缓存时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cache_updated / 1000))}")
	return "\n".join(lines)


def _simple_records_text(cache_data: dict[str, Any], page: int) -> str:
	stats = (cache_data.get("stats_data") or {}).get("stats") or {}
	lines = ["【抽卡记录】"]
	lines.append(
		f"总抽数：{stats.get('total_count', 0)} | 六星：{stats.get('star6_count', 0)} | "
		f"五星：{stats.get('star5_count', 0)} | 四星：{stats.get('star4_count', 0)}"
	)

	pool_defs = (
		("standard", "常驻角色"),
		("beginner", "新手池"),
		("weapon", "武器池"),
		("limited", "限定角色"),
	)
	for key, label in pool_defs:
		data = _pool_page(cache_data, key, page=page, limit=10)
		lines.append(f"\n【{label}】共 {data['total']} 抽（第 {data['page']}/{data['pages']} 页）")
		records = data.get("records") or []
		if not records:
			lines.append("暂无记录")
			continue
		base = (data["page"] - 1) * 10
		for idx, r in enumerate(records, start=1):
			rarity = int(r.get("rarity") or 0)
			name = r.get("char_name") or r.get("item_name") or "未知"
			lines.append(f"{base + idx}. ★{rarity} {name}")
	return "\n".join(lines)


def _to_image_segment(image_bytes: bytes) -> MessageSegment:
	img_b64 = base64.b64encode(image_bytes).decode("utf-8")
	return MessageSegment.image(f"base64://{img_b64}")


async def _get_bili_current_up(framework_token: str) -> dict[str, Any]:
	up_info: dict[str, Any] = {
		"upCharNames": [],
		"upWeaponName": "",
		"activeCharPoolName": "",
		"activeWeaponPoolName": "",
		"poolUpMap": {},
	}

	try:
		res = await _api_get("/api/bili-wiki/activities", framework_token)
		data = (res or {}).get("data") or {}
		items = data.get("items") or data.get("activities") or []
		if not isinstance(items, list):
			items = []

		active = [x for x in items if x.get("is_active") is True]
		char = next((x for x in active if x.get("type") == "特许寻访"), None)
		weapon = next((x for x in active if x.get("type") == "武库申领"), None)
		if char:
			up_name = str(char.get("up") or "").strip()
			if up_name:
				up_info["upCharNames"] = [up_name]
			name = str(char.get("name") or "").strip()
			if "·" in name:
				up_info["activeCharPoolName"] = name.split("·", 1)[1].strip()
		if weapon:
			up_name = str(weapon.get("up") or "").strip()
			if up_name:
				up_info["upWeaponName"] = up_name
			name = str(weapon.get("name") or "").strip()
			if "·" in name:
				up_info["activeWeaponPoolName"] = name.split("·", 1)[1].strip()

		for item in items:
			name = str(item.get("name") or "").strip()
			up_name = str(item.get("up") or "").strip()
			if name and up_name and "·" in name:
				up_info["poolUpMap"][name.split("·", 1)[1].strip()] = up_name
	except Exception:
		pass

	if not up_info["upCharNames"] or not up_info["upWeaponName"]:
		try:
			res = await _api_get("/api/endfield/gacha/global-stats", framework_token)
			stats_data = (res or {}).get("data") if isinstance((res or {}).get("data"), dict) else res
			stats_obj = (stats_data or {}).get("stats") if isinstance((stats_data or {}).get("stats"), dict) else (stats_data or {})
			current_pool = stats_obj.get("current_pool") if isinstance(stats_obj.get("current_pool"), dict) else {}

			if not up_info["upCharNames"]:
				up_chars = current_pool.get("up_char_names") or []
				if isinstance(up_chars, list):
					up_info["upCharNames"] = [str(x).strip() for x in up_chars if str(x).strip()]
				if not up_info["upCharNames"]:
					up_char_name = str(current_pool.get("up_char_name") or "").strip()
					if up_char_name:
						up_info["upCharNames"] = [up_char_name]

			if not up_info["upWeaponName"]:
				up_weapon_name = str(current_pool.get("up_weapon_name") or "").strip()
				if up_weapon_name:
					up_info["upWeaponName"] = up_weapon_name

			for period in stats_obj.get("pool_periods") or []:
				if not isinstance(period, dict):
					continue
				pool_name = str(period.get("pool_name") or "").strip()
				up_chars = period.get("up_char_names") or []
				if pool_name and isinstance(up_chars, list) and up_chars:
					up_name = str(up_chars[0]).strip()
					if up_name:
						up_info["poolUpMap"][pool_name] = up_name

			for period in stats_obj.get("weapon_pool_periods") or []:
				if not isinstance(period, dict):
					continue
				pool_name = str(period.get("pool_name") or "").strip()
				up_names = period.get("up_weapon_names") or []
				if pool_name and isinstance(up_names, list) and up_names:
					up_name = str(up_names[0]).strip()
					if up_name:
						up_info["poolUpMap"][pool_name] = up_name
		except Exception:
			pass

	return up_info


async def _refresh_analysis_context(
	framework_token: str,
	user_id: str,
	role_id: str,
	cache_data: Optional[dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
	base_cache = dict(cache_data or {})
	stats_payload: Optional[dict[str, Any]] = None

	stats_res = await _api_get("/api/endfield/gacha/stats", framework_token)
	if isinstance(stats_res, dict):
		stats_payload = (stats_res.get("data") if isinstance(stats_res.get("data"), dict) else stats_res) or None

	if not isinstance(stats_payload, dict):
		stats_payload = base_cache.get("stats_data") if isinstance(base_cache.get("stats_data"), dict) else None

	if not isinstance(stats_payload, dict):
		return None, base_cache

	user_info = stats_payload.get("user_info") if isinstance(stats_payload.get("user_info"), dict) else {}
	merged_user_info = dict(user_info)

	note_overrides = await _fetch_note_user_overrides(framework_token)
	if note_overrides.get("avatar_url"):
		merged_user_info["avatar_url"] = note_overrides["avatar_url"]
	if note_overrides.get("nickname"):
		merged_user_info["nickname"] = note_overrides["nickname"]
	if note_overrides.get("game_uid") and not merged_user_info.get("game_uid"):
		merged_user_info["game_uid"] = note_overrides["game_uid"]

	if merged_user_info != user_info:
		stats_payload = dict(stats_payload)
		stats_payload["user_info"] = merged_user_info

	try:
		up_info = await _get_bili_current_up(framework_token)
		if isinstance(up_info, dict):
			stats_payload = dict(stats_payload)
			stats_payload["up_info"] = up_info
	except Exception:
		pass

	try:
		gacha_icon_map = await _fetch_gacha_icon_map(framework_token)
		if isinstance(gacha_icon_map, dict) and gacha_icon_map:
			base_cache["gacha_icon_map"] = gacha_icon_map
	except Exception:
		pass

	base_cache["version"] = int(base_cache.get("version") or 1)
	base_cache["user_id"] = str(user_id)
	base_cache["role_id"] = str(role_id)
	base_cache["updated_at"] = int(time.time() * 1000)
	base_cache["stats_data"] = stats_payload

	if _write_gacha_cache(user_id, role_id, base_cache):
		return stats_payload, base_cache
	return stats_payload, (cache_data or {})


async def _sync_gacha(
	event: MessageEvent,
	user_id: str,
	*,
	after_sync_show_records: bool = False,
	after_sync_send_analysis: bool = False,
	source_from_analysis: bool = False,
	source_from_sync_cmd: bool = False,
) -> Any:
	binding = get_active_binding(user_id)
	if not binding:
		return "未绑定终末地账号，请先发送“终末地绑定”完成绑定。"

	framework_token = binding["framework_token"]
	status_data = await _api_get("/api/endfield/gacha/sync/status", framework_token)
	status_inner = _unwrap_response_data(status_data)
	if (status_inner or {}).get("status") == "syncing":
		progress = (status_inner or {}).get("progress") or 0
		completed = (status_inner or {}).get("completed_pools")
		total_pools = (status_inner or {}).get("total_pools")
		records_found = (status_inner or {}).get("records_found")
		msg = ["抽卡同步正在进行中"]
		msg.append(f"进度：{progress}%")
		if completed is not None and total_pools is not None:
			msg[-1] += f" | 卡池 {completed}/{total_pools}"
		if records_found is not None:
			msg[-1] += f" | 已获取 {records_found} 条"
		return "\n".join(msg)

	accounts_data = await _api_get("/api/endfield/gacha/accounts", framework_token)
	ad = _unwrap_response_data(accounts_data)
	accounts = (ad or {}).get("accounts") or []
	need_select = bool((ad or {}).get("need_select"))
	if not accounts:
		return "未获取到可同步账号，请重新绑定后重试。"

	if need_select and len(accounts) > 1:
		text = ["检测到多个账号，请发送序号选择要同步的账号："]
		for i, acc in enumerate(accounts, start=1):
			text.append(f"{i}. {acc.get('channel_name') or '未知'} - {acc.get('nick_name') or acc.get('game_uid') or acc.get('uid')}")
		_set_pending(
			user_id,
			{
				"timestamp": time.time(),
				"accounts": accounts,
				"framework_token": framework_token,
				"target_user_id": user_id,
				"after_sync_show_records": after_sync_show_records,
				"after_sync_send_analysis": after_sync_send_analysis,
				"source_from_analysis": source_from_analysis,
				"source_from_sync_cmd": source_from_sync_cmd,
			},
		)
		return "\n".join(text)

	account = accounts[0]
	account_uid = account.get("uid")
	server_id = _get_account_server_id(account)
	nickname = _get_sender_display_name(event, user_id)

	if source_from_analysis:
		await gacha_analysis.send("正在同步抽卡记录，完成后将自动发送分析。")
	elif source_from_sync_cmd:
		await gacha_records.send("开始同步抽卡记录，请稍候…")

	return await _start_fetch_and_poll(
		framework_token,
		account_uid,
		server_id,
		user_id,
		nickname,
		after_sync_show_records=after_sync_show_records,
		after_sync_send_analysis=after_sync_send_analysis,
	)


async def _start_fetch_and_poll(
	framework_token: str,
	account_uid: Optional[str],
	server_id: Optional[str],
	user_id: str,
	qq_name: str,
	*,
	after_sync_show_records: bool,
	after_sync_send_analysis: bool,
) -> Any:
	body: dict[str, Any] = {"server_id": str(server_id or "1")}
	if account_uid:
		body["account_uid"] = account_uid

	fetch_res = await _api_post("/api/endfield/gacha/fetch", framework_token, body)
	fetch_data = (fetch_res or {}).get("data") if isinstance((fetch_res or {}).get("data"), dict) else fetch_res
	if (fetch_data or {}).get("status") == "conflict":
		return "抽卡同步繁忙，请稍后重试。"
	if not fetch_data or not (fetch_data.get("status") or (fetch_res or {}).get("code") == 0):
		return "抽卡同步启动失败，请稍后重试。"

	last_progress_message = ""
	start_ts = time.time()
	while (time.time() - start_ts) < POLL_TIMEOUT_SECONDS:
		await asyncio.sleep(POLL_INTERVAL_SECONDS)
		status_res = await _api_get("/api/endfield/gacha/sync/status", framework_token)
		status_data = (status_res or {}).get("data") if isinstance((status_res or {}).get("data"), dict) else status_res
		if not status_data:
			continue
		status = status_data.get("status")
		message = status_data.get("message") or ""
		current_pool = status_data.get("current_pool")

		if status == "syncing" and (message or current_pool):
			progress_msg = _format_progress_msg(message or f"正在查询{current_pool}...", user_id, qq_name)
			if progress_msg and progress_msg != last_progress_message:
				last_progress_message = progress_msg
				logger.debug(f"[终末地插件][抽卡同步] {progress_msg}")

		if status == "failed":
			err = status_data.get("error") or message or "未知错误"
			return f"抽卡同步失败：{err}"

		if status == "completed":
			binding = get_active_binding(user_id)
			role_id = (binding or {}).get("role_id") or ""
			await _refresh_local_cache_from_cloud(framework_token, user_id, role_id)

			records_found = int(status_data.get("records_found") or 0)
			new_records = int(status_data.get("new_records") or 0)
			sync_msg = f"抽卡同步完成：共 {records_found} 条，新增 {new_records} 条。"

			cache_data = _read_gacha_cache(user_id, role_id) or {}
			stats_data = cache_data.get("stats_data") if isinstance(cache_data.get("stats_data"), dict) else None
			if after_sync_send_analysis and stats_data:
				try:
					image_bytes = await asyncio.to_thread(render_gacha_analysis_image, stats_data, cache_data)
					return _to_image_segment(image_bytes)
				except Exception as e:
					logger.debug(f"[终末地插件][抽卡分析]同步后渲染图失败，回退文本: {e}")
					return sync_msg + "\n\n" + _simple_analysis_text(stats_data, cache_data)
			if after_sync_show_records and cache_data:
				try:
					image_bytes = await asyncio.to_thread(render_gacha_records_image, cache_data, 1)
					return _to_image_segment(image_bytes)
				except Exception as e:
					logger.debug(f"[终末地插件][抽卡记录]同步后渲染图失败，回退文本: {e}")
					return sync_msg + "\n\n" + _simple_records_text(cache_data, page=1)
			return sync_msg

	return "抽卡同步超时，请稍后再试。"


def _is_superuser(user_id: str) -> bool:
	driver = get_driver()
	superusers = getattr(driver.config, "superusers", set()) or set()
	return str(user_id) in {str(x) for x in superusers}


@gacha_records.handle()
async def handle_gacha_records(event: MessageEvent):
	raw_msg = str(event.get_message()).strip()
	wants_sync = any(k in raw_msg for k in ("同步抽卡记录", "更新抽卡记录"))
	user_id = str(event.get_user_id())

	binding = get_active_binding(user_id)
	if not binding:
		await gacha_records.finish("未绑定终末地账号，请先发送“终末地绑定”完成绑定。")

	role_id = binding.get("role_id") or ""
	framework_token = binding.get("framework_token") or ""
	cache_data = _read_gacha_cache(user_id, role_id)
	stats_data = (cache_data or {}).get("stats_data") if isinstance((cache_data or {}).get("stats_data"), dict) else None
	has_record = _parse_stats_has_records(stats_data)

	if wants_sync:
		text = await _sync_gacha(
			event,
			user_id,
			after_sync_show_records=True,
			source_from_sync_cmd=True,
		)
		await gacha_records.finish(text)

	if not cache_data or not has_record:
		await gacha_records.send("暂无抽卡记录，开始为你同步…")
		text = await _sync_gacha(
			event,
			user_id,
			after_sync_show_records=True,
			source_from_sync_cmd=True,
		)
		await gacha_records.finish(text)

	page = 1
	try:
		arg = raw_msg.split("抽卡记录", 1)[1].strip()
		if arg:
			page = max(1, int(arg))
	except Exception:
		page = 1
	try:
		image_bytes = await asyncio.to_thread(render_gacha_records_image, cache_data, page)
	except Exception as e:
		logger.debug(f"[终末地插件][抽卡记录]渲染图失败，回退文本: {e}")
		await gacha_records.finish(_simple_records_text(cache_data, page=page))
	await gacha_records.finish(_to_image_segment(image_bytes))


@gacha_analysis.handle()
async def handle_gacha_analysis(event: MessageEvent):
	user_id = str(event.get_user_id())
	binding = get_active_binding(user_id)
	if not binding:
		await gacha_analysis.finish("未绑定终末地账号，请先发送“终末地绑定”完成绑定。")

	role_id = binding.get("role_id") or ""
	framework_token = str(binding.get("framework_token") or "")
	cache_data = _read_gacha_cache(user_id, role_id)
	stats_data = (cache_data or {}).get("stats_data") if isinstance((cache_data or {}).get("stats_data"), dict) else None
	has_record = _parse_stats_has_records(stats_data)

	if not stats_data or not has_record:
		await gacha_analysis.send("暂无抽卡记录，正在同步后生成分析…")
		text = await _sync_gacha(
			event,
			user_id,
			after_sync_send_analysis=True,
			source_from_analysis=True,
		)
		await gacha_analysis.finish(text)

	if framework_token:
		try:
			fresh_stats, fresh_cache = await _refresh_analysis_context(framework_token, user_id, role_id, cache_data)
			if isinstance(fresh_stats, dict):
				stats_data = fresh_stats
			if isinstance(fresh_cache, dict):
				cache_data = fresh_cache
		except Exception as e:
			logger.debug(f"[终末地插件][抽卡分析]上下文刷新失败，继续使用本地缓存: {e}")

	try:
		image_bytes = await asyncio.to_thread(render_gacha_analysis_image, stats_data, cache_data or {})
	except Exception as e:
		logger.debug(f"[终末地插件][抽卡分析]渲染图失败，回退文本: {e}")
		await gacha_analysis.finish(_simple_analysis_text(stats_data, cache_data or {}))
	await gacha_analysis.finish(_to_image_segment(image_bytes))


@gacha_global.handle()
async def handle_gacha_global(event: MessageEvent):
	user_id = str(event.get_user_id())
	binding = get_active_binding(user_id)
	if not binding:
		await gacha_global.finish("未绑定终末地账号，请先发送“终末地绑定”完成绑定。")
	framework_token = binding["framework_token"]

	raw_msg = str(event.get_message()).strip()
	keyword = ""
	if "全服抽卡统计" in raw_msg:
		keyword = raw_msg.split("全服抽卡统计", 1)[1].strip()

	stats_res = await _api_get("/api/endfield/gacha/global-stats", framework_token)
	stats_data = (stats_res or {}).get("data") if isinstance((stats_res or {}).get("data"), dict) else stats_res
	if not stats_data or not isinstance(stats_data, dict):
		await gacha_global.finish("获取全服抽卡统计失败，请稍后重试。")

	if keyword:
		stats_res2 = await _api_get(
			"/api/endfield/gacha/global-stats",
			framework_token,
			{"pool_name": keyword},
		)
		stats_data2 = (stats_res2 or {}).get("data") if isinstance((stats_res2 or {}).get("data"), dict) else stats_res2
		if isinstance(stats_data2, dict):
			stats_data = stats_data2

	s = stats_data.get("stats") or stats_data
	by_channel = s.get("by_channel") or {}
	by_type = s.get("by_type") or {}

	def _fmt(v: Any, ndigits: int = 2) -> str:
		try:
			return f"{float(v):.{ndigits}f}"
		except Exception:
			return "-"

	lines = ["【全服抽卡统计】"]
	lines.append(
		f"总抽数：{s.get('total_pulls', 0)} | 统计用户：{s.get('total_users', 0)} | 平均出红：{_fmt(s.get('avg_pity'))} 抽"
	)
	lines.append(
		f"六星：{s.get('star6_total', 0)} | 五星：{s.get('star5_total', 0)} | 四星：{s.get('star4_total', 0)}"
	)

	current_pool = s.get("current_pool") or {}
	up_name = current_pool.get("up_char_name") or "-"
	up_weapon = current_pool.get("up_weapon_name") or "-"
	lines.append(f"当期UP角色：{up_name} | UP武器：{up_weapon}")

	for key, label in (("beginner", "新手池"), ("standard", "常驻池"), ("weapon", "武器池"), ("limited", "限定池")):
		item = by_type.get(key) or {}
		total = int(item.get("total") or 0)
		star6 = int(item.get("star6") or 0)
		avg = _fmt(item.get("avg_pity"), 1)
		rate = (star6 / total * 100) if total > 0 else 0
		lines.append(f"{label}：{total} 抽 | 六星 {star6} | 出红率 {rate:.2f}% | 均出 {avg} 抽")

	official = by_channel.get("official")
	bilibili = by_channel.get("bilibili")
	if isinstance(official, dict):
		lines.append(
			f"官服：{official.get('total_users', 0)} 人，{official.get('total_pulls', 0)} 抽，均出 {_fmt(official.get('avg_pity'))}"
		)
	if isinstance(bilibili, dict):
		lines.append(
			f"B服：{bilibili.get('total_users', 0)} 人，{bilibili.get('total_pulls', 0)} 抽，均出 {_fmt(bilibili.get('avg_pity'))}"
		)
	try:
		image_bytes = await asyncio.to_thread(render_gacha_global_stats_image, stats_data, keyword)
	except Exception as e:
		logger.debug(f"[终末地插件][全服抽卡统计]渲染图失败，回退文本: {e}")
		await gacha_global.finish("\n".join(lines))
	await gacha_global.finish(_to_image_segment(image_bytes))


@gacha_sync_all.handle()
async def handle_sync_all(event: MessageEvent):
	user_id = str(event.get_user_id())
	if not _is_superuser(user_id):
		await gacha_sync_all.finish("该指令仅 Bot 管理员可用。")

	all_bindings = _load_all_bindings()
	if not all_bindings:
		await gacha_sync_all.finish("未找到可同步账号。")

	tasks: list[tuple[str, str, str]] = []
	for _, bindings in all_bindings.items():
		active = next((x for x in bindings if x.get("is_active")), None) or bindings[0]
		token = active.get("framework_token")
		if not token:
			continue
		accounts_res = await _api_get("/api/endfield/gacha/accounts", token)
		accounts_data = _unwrap_response_data(accounts_res)
		accounts = (accounts_data or {}).get("accounts") or []
		if not accounts:
			continue
		for account in accounts:
			tasks.append((token, str(account.get("uid") or ""), _get_account_server_id(account)))

	if not tasks:
		await gacha_sync_all.finish("未找到可同步账号。")

	triggered = 0
	skipped = 0
	for i, (token, account_uid, server_id) in enumerate(tasks):
		if i > 0:
			await asyncio.sleep(3)

		status_res = await _api_get("/api/endfield/gacha/sync/status", token)
		status_data = _unwrap_response_data(status_res)
		if (status_data or {}).get("status") == "syncing":
			skipped += 1
			continue

		fetch_res = await _api_post(
			"/api/endfield/gacha/sync/fetch",
			token,
			{"account_uid": account_uid, "server_id": str(server_id or "1")},
		)
		fetch_data = _unwrap_response_data(fetch_res)
		status = (fetch_data or {}).get("status")
		if status == "conflict":
			skipped += 1
		elif status or (fetch_res or {}).get("code") == 0:
			triggered += 1

	skipped_text = f"，跳过 {skipped} 个" if skipped > 0 else ""
	await gacha_sync_all.finish(f"已触发同步 {triggered} 个账号{skipped_text}。")


@gacha_select.handle()
async def handle_gacha_select(event: MessageEvent, bot: Bot):
	user_id = str(event.get_user_id())
	pending = _get_pending(user_id)
	if not pending:
		return

	raw = str(event.get_message()).strip()
	try:
		cleaned = raw
		for prefix in (":", "："):
			if cleaned.startswith(prefix):
				cleaned = cleaned[len(prefix) :]
		if cleaned.startswith("/zmd") or cleaned.startswith("/终末地") or cleaned.startswith("#zmd") or cleaned.startswith("#终末地"):
			cleaned = cleaned.split(maxsplit=1)[-1]
		idx = int(cleaned)
	except Exception:
		await bot.send(event, "序号无效，请发送 1-999 的数字序号。")
		raise FinishedException

	accounts = pending.get("accounts") or []
	if idx < 1 or idx > len(accounts):
		await bot.send(event, "序号超出范围，请重新输入。")
		raise FinishedException

	_clear_pending(user_id)
	account = accounts[idx - 1]
	framework_token = str(pending.get("framework_token") or "")
	if not framework_token:
		await bot.send(event, "同步状态失效，请重新发送“同步抽卡记录”。")
		raise FinishedException

	account_uid = account.get("uid")
	server_id = _get_account_server_id(account)
	target_user_id = str(pending.get("target_user_id") or user_id)
	nickname = _get_sender_display_name(event, user_id)

	await bot.send(event, "已选择账号，开始同步…")
	text = await _start_fetch_and_poll(
		framework_token,
		account_uid,
		server_id,
		target_user_id,
		nickname,
		after_sync_show_records=bool(pending.get("after_sync_show_records")),
		after_sync_send_analysis=bool(pending.get("after_sync_send_analysis")),
	)
	await bot.send(event, text, at_sender=isinstance(event, GroupMessageEvent))
	raise FinishedException

