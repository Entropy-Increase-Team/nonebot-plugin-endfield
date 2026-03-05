import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from nonebot import get_driver, on_command
from nonebot.adapters import Event
from nonebot.adapters.onebot.v11 import MessageSegment
from PIL import Image, ImageDraw, ImageFont

from ..config import Config
from ..lib.api import api_request
from .user_bind import TABLE_NAME, _get_db_path


logger = logging.getLogger("nonebot")

_FONT_INIT_LOCK = threading.Lock()
_FONT_INIT_DONE = False
_FALLBACK_FONT_FILES = {
	"regular": (
		"NotoSansCJKsc-Regular.otf",
		"https://gh-proxy.org/https://github.com/notofonts/noto-cjk/raw/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf",
	),
	"bold": (
		"NotoSansCJKsc-Bold.otf",
		"https://gh-proxy.org/https://github.com/notofonts/noto-cjk/raw/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Bold.otf",
	),
}

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

	_ensure_fallback_fonts()
	for path in _get_fallback_font_candidates(bold=False):
		if path.exists():
			try:
				return ImageFont.truetype(str(path), size=size)
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

	_ensure_fallback_fonts()
	for path in _get_fallback_font_candidates(bold=True):
		if path.exists():
			try:
				return ImageFont.truetype(str(path), size=size)
			except Exception:
				continue
	return ImageFont.load_default()


def _get_font_cache_dir() -> Path:
	return _get_db_path().parent / "fonts"


def _get_fallback_font_candidates(bold: bool) -> list[Path]:
	font_dir = _get_font_cache_dir()
	regular_name, _ = _FALLBACK_FONT_FILES["regular"]
	bold_name, _ = _FALLBACK_FONT_FILES["bold"]
	if bold:
		return [font_dir / bold_name, font_dir / regular_name]
	return [font_dir / regular_name, font_dir / bold_name]


def _ensure_fallback_fonts() -> None:
	global _FONT_INIT_DONE
	if _FONT_INIT_DONE:
		return

	with _FONT_INIT_LOCK:
		if _FONT_INIT_DONE:
			return

		font_dir = _get_font_cache_dir()
		font_dir.mkdir(parents=True, exist_ok=True)

		for _, (filename, url) in _FALLBACK_FONT_FILES.items():
			font_path = font_dir / filename
			if font_path.exists() and font_path.stat().st_size > 1024:
				continue
			try:
				response = httpx.get(url, timeout=20.0)
				response.raise_for_status()
				font_path.write_bytes(response.content)
			except Exception as e:
				logger.warning(f"fallback font download failed: {filename}, error={e}")

		_FONT_INIT_DONE = True


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


def _fetch_all_gacha_records(
	api_key: str,
	framework_token: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
	headers = {
		"accept": "application/json",
		"X-API-Key": api_key,
		"X-Framework-Token": framework_token,
	}

	pool_values = ["limited", "standard", "beginner", "weapon"]
	records_by_pool: dict[str, list[dict[str, Any]]] = {pool: [] for pool in pool_values}
	all_records: list[dict[str, Any]] = []
	meta_ref: dict[str, Any] = {}

	for pool in pool_values:
		first = api_request("GET", f"/api/endfield/gacha/records?pools={pool}", headers=headers)
		if not isinstance(first, dict) or first.get("code") != 0:
			msg = first.get("message") if isinstance(first, dict) else "请求失败"
			raise RuntimeError(f"获取 {pool} 卡池记录失败：{msg or '请求失败'}")

		data = first.get("data") if isinstance(first.get("data"), dict) else {}
		if not meta_ref:
			meta_ref = data

		records = data.get("records") if isinstance(data.get("records"), list) else []
		pages = _safe_int(data.get("pages"), 1)
		valid_records = [r for r in records if isinstance(r, dict)]
		records_by_pool[pool].extend(valid_records)
		all_records.extend(valid_records)

		for page in range(2, max(2, pages + 1)):
			resp = api_request("GET", f"/api/endfield/gacha/records?pools={pool}&page={page}", headers=headers)
			if not isinstance(resp, dict) or resp.get("code") != 0:
				msg = resp.get("message") if isinstance(resp, dict) else "请求失败"
				raise RuntimeError(f"获取 {pool} 卡池第 {page} 页失败：{msg or '请求失败'}")
			page_data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
			page_records = page_data.get("records") if isinstance(page_data.get("records"), list) else []
			valid_page_records = [r for r in page_records if isinstance(r, dict)]
			records_by_pool[pool].extend(valid_page_records)
			all_records.extend(valid_page_records)

	all_records.sort(key=lambda item: _safe_int(item.get("gacha_ts"), 0), reverse=True)
	for pool in pool_values:
		records_by_pool[pool].sort(key=lambda item: _safe_int(item.get("gacha_ts"), 0), reverse=True)

	stats = {
		"total_count": len(all_records),
		"star6_count": sum(1 for r in all_records if _safe_int(r.get("rarity"), 0) >= 6),
		"star5_count": sum(1 for r in all_records if _safe_int(r.get("rarity"), 0) == 5),
		"star4_count": sum(1 for r in all_records if _safe_int(r.get("rarity"), 0) <= 4),
	}
	meta_data = dict(meta_ref) if isinstance(meta_ref, dict) else {}
	meta_data["stats"] = stats
	meta_data["total"] = len(all_records)
	meta_data["all_records"] = all_records
	return records_by_pool, meta_data


def _render_column(
	title: str,
	records: list[dict[str, Any]],
	min_width: int,
	bg: str,
	title_font: ImageFont.ImageFont,
	text_font: ImageFont.ImageFont,
	yellow_threshold: int,
	pink_threshold: int,
	carry_over_between_pools: bool = False,
) -> Image.Image:
	segment_w = 8
	segment_h = 18
	segment_gap = 2
	left = 12
	right = 12

	def _bar_color(cumulative: int) -> str:
		if cumulative >= pink_threshold:
			return "#ec4899"
		if cumulative >= yellow_threshold:
			return "#eab308"
		return "#22c55e"

	pool_names = [str((rec.get("pool_name") or "未知卡池")) for rec in records if isinstance(rec, dict)]
	pool_group_count = max(1, len(dict.fromkeys(pool_names)))

	def _max_row_segments(items: list[dict[str, Any]]) -> int:
		if not items:
			return 1
		current = 0
		max_count = 1
		for idx, rec in enumerate(items):
			current += 1
			if current > max_count:
				max_count = current
			rarity = _safe_int(rec.get("rarity"), 4)
			is_last = idx == len(items) - 1
			if rarity >= 6 and not is_last:
				current = 0
		return max_count

	normal_records_all: list[dict[str, Any]] = []
	free_records_all: list[dict[str, Any]] = []
	for rec in records:
		if not isinstance(rec, dict):
			continue
		is_free_value = rec.get("is_free")
		is_free = is_free_value is True or str(is_free_value).strip().lower() in {"true", "1"}
		if is_free:
			free_records_all.append(rec)
		else:
			normal_records_all.append(rec)

	max_segments = max(_max_row_segments(normal_records_all), _max_row_segments(free_records_all))
	content_w = left + max_segments * segment_w + max(0, max_segments - 1) * segment_gap + right
	width = max(min_width, content_w)

	tmp_h = 260 + max(1, len(records)) * 26 + pool_group_count * 44
	img = Image.new("RGB", (width, tmp_h), bg)
	draw = ImageDraw.Draw(img)

	draw.rectangle((0, 0, width, 54), fill="#f3f4f6")
	draw.text((12, 12), title, fill="#111827", font=title_font)

	def _draw_strip_rows(items: list[dict[str, Any]], start_y: int, start_count: int) -> tuple[int, int]:
		if not items:
			return start_y, start_count

		x = left
		row_top = start_y
		row_count = start_count
		row_items: list[dict[str, Any]] = []

		def flush_row() -> None:
			nonlocal x, row_top, row_count, row_items
			if not row_items:
				return
			row_end_count = row_count
			row_color = _bar_color(row_end_count)
			draw_x = left
			for _ in row_items:
				draw.rectangle((draw_x, row_top, draw_x + segment_w, row_top + segment_h), fill=row_color)
				draw_x += segment_w + segment_gap
			x = left
			row_top += segment_h + 8
			row_items = []

		for idx, rec in enumerate(items):
			rarity = _safe_int(rec.get("rarity"), 4)
			row_count += 1
			row_items.append(rec)

			is_last = idx == len(items) - 1
			if rarity >= 6 and not is_last:
				flush_row()
				row_count = 0
				continue

			x += segment_w + segment_gap

		if row_items:
			row_end_count = row_count
			row_color = _bar_color(row_end_count)
			draw_x = left
			for _ in row_items:
				draw.rectangle((draw_x, row_top, draw_x + segment_w, row_top + segment_h), fill=row_color)
				draw_x += segment_w + segment_gap
			row_top += segment_h

		return row_top, row_count

	groups: dict[str, list[dict[str, Any]]] = {}
	for rec in records:
		if not isinstance(rec, dict):
			continue
		pool_name = str(rec.get("pool_name") or "未知卡池")
		groups.setdefault(pool_name, []).append(rec)

	y = 66
	inherited_count = 0
	for pool_name, group_records in groups.items():
		draw.text((12, y), pool_name, fill="#4b5563", font=text_font)
		y += 24

		normal_records: list[dict[str, Any]] = []
		free_records: list[dict[str, Any]] = []
		for rec in group_records:
			is_free_value = rec.get("is_free")
			is_free = is_free_value is True or str(is_free_value).strip().lower() in {"true", "1"}
			if is_free:
				free_records.append(rec)
			else:
				normal_records.append(rec)

		start_count = inherited_count if carry_over_between_pools else 0
		y, tail_count = _draw_strip_rows(normal_records, y, start_count)

		if free_records:
			if normal_records:
				y += 8
			draw.text((12, y), "免费十连", fill="#2563eb", font=text_font)
			y += 22
			y, tail_count = _draw_strip_rows(free_records, y, tail_count)

		if carry_over_between_pools:
			last_rarity = _safe_int(group_records[-1].get("rarity"), 4) if group_records else 6
			inherited_count = 0 if last_rarity >= 6 else tail_count

		draw.line((12, y + 10, width - 12, y + 10), fill="#d1d5db", width=1)
		y += 20

	return img.crop((0, 0, width, max(120, y + 4)))


def _render_gacha_analysis_image(
	records_by_pool: dict[str, list[dict[str, Any]]],
	meta_data: dict[str, Any],
	role_id: Optional[str],
) -> bytes:
	title_font = _pick_bold_font(24)
	text_font = _pick_font(18)
	small_font = _pick_font(16)

	columns = [
		_render_column("限定寻访", records_by_pool.get("limited", []), 420, "#ffffff", title_font, small_font, 40, 65, True),
		_render_column("常驻寻访", records_by_pool.get("standard", []), 420, "#ffffff", title_font, small_font, 40, 65),
		_render_column("新手寻访", records_by_pool.get("beginner", []), 420, "#ffffff", title_font, small_font, 40, 65),
		_render_column("武器寻访", records_by_pool.get("weapon", []), 420, "#ffffff", title_font, small_font, 20, 35),
	]

	content_h = max(col.height for col in columns)
	top_h = 130
	column_gap = 12
	width = 24 + sum(col.width for col in columns) + column_gap * (len(columns) - 1)
	height = top_h + content_h + 24

	img = Image.new("RGB", (width, height), "#f9fafb")
	draw = ImageDraw.Draw(img)

	stats = meta_data.get("stats") if isinstance(meta_data.get("stats"), dict) else {}
	all_records = meta_data.get("all_records") if isinstance(meta_data.get("all_records"), list) else []
	total = _safe_int(meta_data.get("total"), len(all_records))
	star6 = _safe_int(stats.get("star6_count"), 0)
	star5 = _safe_int(stats.get("star5_count"), 0)
	star4 = _safe_int(stats.get("star4_count"), 0)
	start_text = _to_datetime_text(all_records[-1].get("gacha_ts")) if all_records else "未知"
	end_text = _to_datetime_text(all_records[0].get("gacha_ts")) if all_records else "未知"

	draw.rectangle((0, 0, width, 96), fill="#eef2ff")
	draw.text((12, 14), "终末地抽卡分析", fill="#111827", font=_pick_bold_font(34))
	draw.text(
		(12, 62),
		f"UID: {role_id or '未知'}  总抽数: {total}  6★: {star6}  5★: {star5}  4★: {star4}",
		fill="#1f2937",
		font=text_font,
	)
	draw.text(
		(12, 102),
		f"时间范围: {start_text}  →  {end_text}",
		fill="#374151",
		font=small_font,
	)

	x = 12
	for col in columns:
		img.paste(col, (x, top_h))
		x += col.width + column_gap

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
		records_by_pool, meta_data = await asyncio.to_thread(_fetch_all_gacha_records, api_key, framework_token)
	except Exception as e:
		logger.warning(f"fetch gacha records failed: {e}")
		await gacha_analysis.finish(str(e))

	all_records = meta_data.get("all_records") if isinstance(meta_data.get("all_records"), list) else []
	if not all_records:
		await gacha_analysis.finish("未获取到抽卡记录。")

	try:
		image_bytes = await asyncio.to_thread(
			_render_gacha_analysis_image,
			records_by_pool,
			meta_data,
			active_binding.get("role_id"),
		)
	except Exception as e:
		logger.exception(f"render gacha analysis failed: {e}")
		await gacha_analysis.finish("生成抽卡分析图片失败，请稍后重试。")

	image_b64 = base64.b64encode(image_bytes).decode("utf-8")
	await gacha_analysis.finish(MessageSegment.image(f"base64://{image_b64}"))
