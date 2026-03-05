import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Optional

from nonebot import get_driver, on_command
from nonebot.adapters import Event
from nonebot.adapters.onebot.v11 import MessageSegment
from PIL import Image, ImageDraw, ImageFont

from ..config import Config
from ..lib.api import api_request
from .user_bind import TABLE_NAME, _get_db_path


logger = logging.getLogger("nonebot")

gacha_analysis = on_command("终末地抽卡分析", aliases={"终末地抽卡记录", "endfield抽卡分析"})


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


def _pick_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
	candidates = [
		"C:/Windows/Fonts/msyh.ttc",
		"C:/Windows/Fonts/simhei.ttf",
		"/System/Library/Fonts/PingFang.ttc",
		"/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
	]
	for path in candidates:
		if os.path.exists(path):
			try:
				return ImageFont.truetype(path, size=size)
			except Exception:
				continue
	return ImageFont.load_default()


def _pick_bold_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
	candidates = [
		"C:/Windows/Fonts/msyhbd.ttc",
		"C:/Windows/Fonts/simhei.ttf",
		"C:/Windows/Fonts/msyh.ttc",
		"/System/Library/Fonts/PingFang.ttc",
		"/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
		"/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
	]
	for path in candidates:
		if os.path.exists(path):
			try:
				return ImageFont.truetype(path, size=size)
			except Exception:
				continue
	return ImageFont.load_default()


def _safe_int(value: Any, default: int = 0) -> int:
	try:
		return int(str(value))
	except Exception:
		return default


def _to_datetime_text(ts: Any) -> str:
	try:
		v = float(str(ts))
		if v > 1e12:
			v /= 1000.0
		return datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")
	except Exception:
		return "未知"


def _pool_category(pool_id: str) -> str:
	p = (pool_id or "").lower()
	if p.startswith("special_"):
		return "special"
	if p.startswith("standard_"):
		return "standard"
	if p.startswith("beginner_"):
		return "beginner"
	if p.startswith("weaponbox_") or p.startswith("weapon_") or p.startswith("weapon"):
		return "weapon"
	return "other"


def _rarity_color(rarity: int) -> str:
	if rarity >= 6:
		return "#ef4444"
	if rarity == 5:
		return "#f59e0b"
	return "#9ca3af"


def _fetch_all_gacha_records(api_key: str, framework_token: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
	headers = {
		"accept": "application/json",
		"X-API-Key": api_key,
		"X-Framework-Token": framework_token,
	}

	first = api_request("GET", "/api/endfield/gacha/records", headers=headers)
	if not isinstance(first, dict) or first.get("code") != 0:
		msg = first.get("message") if isinstance(first, dict) else "请求失败"
		raise RuntimeError(f"获取抽卡记录失败：{msg or '请求失败'}")

	data = first.get("data") if isinstance(first.get("data"), dict) else {}
	records = data.get("records") if isinstance(data.get("records"), list) else []
	pages = _safe_int(data.get("pages"), 1)

	all_records: list[dict[str, Any]] = [r for r in records if isinstance(r, dict)]
	for page in range(2, max(2, pages + 1)):
		resp = api_request("GET", f"/api/endfield/gacha/records?page={page}", headers=headers)
		if not isinstance(resp, dict) or resp.get("code") != 0:
			msg = resp.get("message") if isinstance(resp, dict) else "请求失败"
			raise RuntimeError(f"获取第 {page} 页失败：{msg or '请求失败'}")
		page_data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
		page_records = page_data.get("records") if isinstance(page_data.get("records"), list) else []
		all_records.extend([r for r in page_records if isinstance(r, dict)])

	all_records.sort(key=lambda item: _safe_int(item.get("gacha_ts"), 0))
	return all_records, data


def _render_column(
	title: str,
	pool_blocks: list[tuple[str, str, list[dict[str, Any]]]],
	width: int,
	bg: str,
	title_font: ImageFont.ImageFont,
	text_font: ImageFont.ImageFont,
) -> Image.Image:
	segment_w = 8
	segment_h = 18
	segment_gap = 2
	max_per_row = max(20, (width - 28) // (segment_w + segment_gap))

	tmp_h = 240 + sum(max(1, len(items)) * 24 + 58 for _, _, items in pool_blocks)
	img = Image.new("RGB", (width, tmp_h), bg)
	draw = ImageDraw.Draw(img)

	draw.rectangle((0, 0, width, 54), fill="#f3f4f6")
	draw.text((12, 12), title, fill="#111827", font=title_font)

	y = 66
	for pool_id, pool_name, records in pool_blocks:
		draw.text((12, y), pool_id or "unknown_pool", fill="#1f2937", font=text_font)
		draw.text((12, y + 22), pool_name or "未知卡池", fill="#4b5563", font=text_font)
		y += 46

		x = 12
		row_top = y
		for idx, rec in enumerate(records):
			rarity = _safe_int(rec.get("rarity"), 4)
			draw.rectangle((x, row_top, x + segment_w, row_top + segment_h), fill=_rarity_color(rarity))

			is_last = idx == len(records) - 1
			if rarity >= 6 and not is_last:
				x = 12
				row_top += segment_h + 8
				continue

			x += segment_w + segment_gap
			if x + segment_w >= width - 10 and not is_last:
				x = 12
				row_top += segment_h + 8

		y = row_top + segment_h + 16
		draw.line((12, y, width - 12, y), fill="#d1d5db", width=1)
		y += 10

	return img.crop((0, 0, width, max(120, y + 4)))


def _render_gacha_analysis_image(
	records: list[dict[str, Any]],
	meta_data: dict[str, Any],
	role_id: Optional[str],
) -> bytes:
	by_category: dict[str, dict[str, dict[str, Any]]] = {
		"special": {},
		"standard": {},
		"beginner": {},
		"weapon": {},
	}

	for rec in records:
		pool_id = str(rec.get("pool_id") or "")
		pool_name = str(rec.get("pool_name") or "")
		category = _pool_category(pool_id)
		if category not in by_category:
			continue
		block = by_category[category].setdefault(pool_id or "unknown_pool", {"pool_name": pool_name, "records": []})
		block["records"].append(rec)

	def build_blocks(cat: str) -> list[tuple[str, str, list[dict[str, Any]]]]:
		items: list[tuple[str, str, list[dict[str, Any]]]] = []
		for pool_id, payload in by_category[cat].items():
			items.append((pool_id, str(payload.get("pool_name") or ""), payload.get("records") or []))
		return items

	title_font = _pick_bold_font(24)
	text_font = _pick_font(18)
	small_font = _pick_font(16)

	columns = [
		_render_column("special_xxxx", build_blocks("special"), 420, "#ffffff", title_font, small_font),
		_render_column("standard", build_blocks("standard"), 420, "#ffffff", title_font, small_font),
		_render_column("beginner", build_blocks("beginner"), 420, "#ffffff", title_font, small_font),
		_render_column("weaponbox_xxxx", build_blocks("weapon"), 420, "#ffffff", title_font, small_font),
	]

	content_h = max(col.height for col in columns)
	top_h = 130
	width = 420 * 4 + 36
	height = top_h + content_h + 24

	img = Image.new("RGB", (width, height), "#f9fafb")
	draw = ImageDraw.Draw(img)

	stats = meta_data.get("stats") if isinstance(meta_data.get("stats"), dict) else {}
	total = _safe_int(meta_data.get("total"), len(records))
	star6 = _safe_int(stats.get("star6_count"), 0)
	star5 = _safe_int(stats.get("star5_count"), 0)
	star4 = _safe_int(stats.get("star4_count"), 0)
	start_text = _to_datetime_text(records[0].get("gacha_ts")) if records else "未知"
	end_text = _to_datetime_text(records[-1].get("gacha_ts")) if records else "未知"

	draw.rectangle((0, 0, width, 96), fill="#eef2ff")
	draw.text((12, 14), "终末地抽卡分析（全量记录）", fill="#111827", font=_pick_bold_font(34))
	draw.text(
		(12, 62),
		f"UID: {role_id or '未知'}  总抽数: {total}  6★: {star6}  5★: {star5}  4★: {star4}",
		fill="#1f2937",
		font=text_font,
	)
	draw.text(
		(12, 102),
		f"时间范围: {start_text}  →  {end_text}    说明: 按时间从旧到新，横向累加；每出现 6★ 自动换行",
		fill="#374151",
		font=small_font,
	)

	x = 12
	for col in columns:
		img.paste(col, (x, top_h))
		x += 420

	buf = io.BytesIO()
	img.save(buf, format="PNG")
	return buf.getvalue()


@gacha_analysis.handle()
async def handle_gacha_analysis(event: Event):
	api_key = _get_api_key()
	if not api_key:
		await gacha_analysis.finish("未配置 endfield_api_key，无法获取抽卡记录。")

	user_id = str(event.get_user_id())
	active_binding = _get_active_binding(user_id)
	if not active_binding:
		await gacha_analysis.finish("未找到已绑定账号，请先使用“终末地绑定”。")

	framework_token = active_binding["framework_token"]

	try:
		records, meta_data = await asyncio.to_thread(_fetch_all_gacha_records, api_key, framework_token)
	except Exception as e:
		logger.warning(f"fetch gacha records failed: {e}")
		await gacha_analysis.finish(str(e))

	if not records:
		await gacha_analysis.finish("未获取到抽卡记录。")

	try:
		image_bytes = await asyncio.to_thread(
			_render_gacha_analysis_image,
			records,
			meta_data,
			active_binding.get("role_id"),
		)
	except Exception as e:
		logger.exception(f"render gacha analysis failed: {e}")
		await gacha_analysis.finish("生成抽卡分析图片失败，请稍后重试。")

	image_b64 = base64.b64encode(image_bytes).decode("utf-8")
	await gacha_analysis.finish(MessageSegment.image(f"base64://{image_b64}"))
