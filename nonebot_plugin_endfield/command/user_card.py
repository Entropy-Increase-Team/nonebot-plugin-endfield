import asyncio
import base64
import concurrent.futures
import io
import json
import logging
import math
import os
import sqlite3
import threading
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from nonebot import get_driver, on_command
from nonebot.adapters import Event
from nonebot.adapters.onebot.v11 import MessageSegment
from nonebot.rule import to_me
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

user_card = on_command("终末地信息卡", aliases={"终末地名片", "终末地卡片", "endfield信息卡"})


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


def _format_timestamp(value: Any) -> str:
	if value is None:
		return "未知"

	text = str(value).strip()
	if not text:
		return "未知"

	try:
		timestamp = float(text)
		if timestamp > 1e12:
			timestamp /= 1000.0
		if timestamp <= 0:
			return "未知"
		return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
	except Exception:
		return text


def _download_image(url: str) -> Optional[Image.Image]:
	normalized = (url or "").strip()
	if not normalized:
		return None
	try:
		response = httpx.get(normalized, timeout=8.0)
		response.raise_for_status()
		with warnings.catch_warnings():
			warnings.filterwarnings(
				"ignore",
				message="Palette images with Transparency expressed in bytes should be converted to RGBA images",
				category=UserWarning,
			)
			return Image.open(io.BytesIO(response.content)).convert("RGBA")
	except Exception:
		return None


def _safe_int(value: Any, default: int = 0) -> int:
	try:
		return int(str(value))
	except Exception:
		return default


def _prefetch_images(urls: list[str]) -> dict[str, Optional[Image.Image]]:
	unique_urls = [u for u in dict.fromkeys((url.strip() for url in urls if isinstance(url, str))) if u]
	if not unique_urls:
		return {}

	max_workers = min(12, max(1, len(unique_urls)))
	cache: dict[str, Optional[Image.Image]] = {}
	with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
		future_map = {executor.submit(_download_image, url): url for url in unique_urls}
		for future in concurrent.futures.as_completed(future_map):
			url = future_map[future]
			try:
				cache[url] = future.result()
			except Exception:
				cache[url] = None
	return cache


def _draw_progress_bar(
	draw: ImageDraw.ImageDraw,
	x: int,
	y: int,
	width: int,
	height: int,
	label: str,
	current: int,
	maximum: int,
	font: ImageFont.ImageFont,
) -> None:
	max_value = maximum if maximum > 0 else 1
	ratio = current / max_value
	clamped_ratio = max(0.0, min(ratio, 1.0))
	fill_width = int(width * clamped_ratio)

	bg_color = "#e5e7eb"
	fill_color = "#e6bc00"
	if current > maximum:
		fill_color = "#fb2c36"

	draw.text((x, y), f"{label}  {current}/{maximum}", fill="#111827", font=font)
	bar_top = y + 38
	bar_bottom = bar_top + height
	draw.rectangle((x, bar_top, x + width, bar_bottom), fill=bg_color)
	if fill_width > 0:
		draw.rectangle((x, bar_top, x + fill_width, bar_bottom), fill=fill_color)


def _render_note_card(note_data: dict[str, Any], local_role_id: Optional[str], local_server_id: Optional[str]) -> bytes:
	data = note_data.get("data") if isinstance(note_data, dict) else None
	if not isinstance(data, dict):
		data = {}

	base = data.get("base") if isinstance(data.get("base"), dict) else {}
	bp = data.get("bpSystem") if isinstance(data.get("bpSystem"), dict) else {}
	daily = data.get("dailyMission") if isinstance(data.get("dailyMission"), dict) else {}
	stamina = data.get("stamina") if isinstance(data.get("stamina"), dict) else {}
	chars = data.get("chars") if isinstance(data.get("chars"), list) else []

	role_name = str(base.get("name") or "未知用户")
	api_role_id = str(base.get("roleId") or "")
	role_id = local_role_id or api_role_id or "未知"
	level = _safe_int(base.get("level"))
	server_name = local_server_id or "未知"
	create_time = _format_timestamp(base.get("createTime"))
	last_login = _format_timestamp(base.get("lastLoginTime"))

	char_num = _safe_int(base.get("charNum"))
	weapon_num = _safe_int(base.get("weaponNum"))
	doc_num = _safe_int(base.get("docNum"))
	exp = _safe_int(base.get("exp"))

	bp_cur = _safe_int(bp.get("curLevel"))
	bp_max = _safe_int(bp.get("maxLevel"))
	activation = _safe_int(daily.get("activation"))
	activation_max = _safe_int(daily.get("maxActivation"))
	stamina_cur = _safe_int(stamina.get("current"))
	stamina_max = _safe_int(stamina.get("max"), 1)

	card_width = 1280
	padding = 28
	header_height = 110
	content_top = header_height + 24
	top_area_height = 260
	section_gap = 24

	avatar_size = 180
	avatar_x = padding
	avatar_y = content_top

	info_x = avatar_x + avatar_size + 24
	info_y = avatar_y
	info_width = 460

	bar_x = info_x + info_width + 24
	bar_y = avatar_y + 10
	bar_width = card_width - padding - bar_x

	sorted_chars = sorted(
		[item for item in chars if isinstance(item, dict)],
		key=lambda item: _safe_int(item.get("level")),
		reverse=True,
	)
	show_chars = sorted_chars

	avatar_url = str(base.get("avatarUrl") or "")
	avatar_rt_urls = [str(item.get("avatarRtUrl") or "") for item in show_chars]
	image_cache = _prefetch_images([avatar_url, *avatar_rt_urls])

	grid_title_height = 56
	grid_cols = 4
	grid_gap = 16
	cell_width = (card_width - padding * 2 - grid_gap * (grid_cols - 1)) // grid_cols
	cell_height = max(140, int(cell_width * 554 / 396))
	grid_rows = max(1, math.ceil(len(show_chars) / grid_cols))
	grid_height = grid_rows * cell_height + (grid_rows - 1) * grid_gap

	stats_block_height = 156
	grid_top = content_top + top_area_height + section_gap + stats_block_height + section_gap
	card_height = grid_top + grid_title_height + grid_height + padding

	image = Image.new("RGB", (card_width, card_height), "#ffffff")
	draw = ImageDraw.Draw(image)

	title_font = _pick_bold_font(42)
	name_font = _pick_bold_font(46)
	line_font = _pick_font(28)
	sub_font = _pick_font(24)
	bar_font = _pick_font(24)
	grid_font = _pick_font(24)
	grid_small_font = _pick_font(22)

	draw.rectangle((0, 0, card_width, 110), fill="#fff7d6")
	draw.text((padding, 28), "终末地信息卡", fill="#1f2d3d", font=title_font)

	avatar = image_cache.get(avatar_url) if avatar_url else None
	if avatar is not None:
		avatar = avatar.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
		image.paste(avatar, (avatar_x, avatar_y))
	else:
		draw.rectangle((avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size), fill="#e5e7eb")
		draw.text((avatar_x + 38, avatar_y + 70), "无头像", fill="#6b7280", font=sub_font)

	draw.text((info_x, info_y + 8), role_name, fill="#111827", font=name_font)
	draw.text((info_x, info_y + 70), f"等级：{level}", fill="#1f2937", font=line_font)
	draw.text((info_x + 210, info_y + 70), f"UID：{role_id}", fill="#1f2937", font=line_font)
	mission = base.get("mainMission") if isinstance(base.get("mainMission"), dict) else {}
	mission_text = str(mission.get("description") or "无主线信息")
	draw.text((info_x, info_y + 122), f"主线进度：{mission_text}", fill="#374151", font=sub_font)
	draw.text((info_x, info_y + 162), f"服务器：{server_name}", fill="#374151", font=sub_font)

	_draw_progress_bar(
		draw,
		bar_x,
		bar_y,
		bar_width,
		18,
		"体力",
		stamina_cur,
		stamina_max,
		bar_font,
	)
	_draw_progress_bar(
		draw,
		bar_x,
		bar_y + 68,
		bar_width,
		18,
		"活跃度",
		activation,
		activation_max,
		bar_font,
	)
	_draw_progress_bar(
		draw,
		bar_x,
		bar_y + 136,
		bar_width,
		18,
		"通行证等级",
		bp_cur,
		bp_max,
		bar_font,
	)

	stats_y = content_top + top_area_height + section_gap
	stats_line_1 = f"角色数：{char_num}    武器数：{weapon_num}    文档数：{doc_num}    经验：{exp}"
	stats_line_2 = f"注册：{create_time}    最近登录：{last_login}"

	bbox_1 = draw.textbbox((0, 0), stats_line_1, font=sub_font)
	bbox_2 = draw.textbbox((0, 0), stats_line_2, font=sub_font)
	line_1_width = max(0, bbox_1[2] - bbox_1[0])
	line_2_width = max(0, bbox_2[2] - bbox_2[0])
	line_gap = 18

	line_1_x = max(padding, (card_width - line_1_width) // 2)
	line_2_x = max(padding, (card_width - line_2_width) // 2)

	draw.text((line_1_x, stats_y), stats_line_1, fill="#1f2937", font=sub_font)
	draw.text((line_2_x, stats_y + 34 + line_gap), stats_line_2, fill="#1f2937", font=sub_font)

	draw.text((padding, grid_top), f"角色列表（共 {len(show_chars)} 名）", fill="#111827", font=_pick_bold_font(30))

	start_y = grid_top + grid_title_height
	for idx, char in enumerate(show_chars):
		row = idx // grid_cols
		col = idx % grid_cols
		x = padding + col * (cell_width + grid_gap)
		y = start_y + row * (cell_height + grid_gap)

		avatar_rt_url = str(char.get("avatarRtUrl") or "")
		bg_image = image_cache.get(avatar_rt_url) if avatar_rt_url else None
		if bg_image is not None:
			bg_image = bg_image.resize((cell_width, cell_height), Image.Resampling.LANCZOS).convert("RGBA")
			white_bg = Image.new("RGBA", (cell_width, cell_height), (255, 255, 255, 255))
			white_bg.alpha_composite(bg_image)
			overlay = Image.new("RGBA", (cell_width, cell_height), (0, 0, 0, 108))
			white_bg.alpha_composite(overlay)
			image.paste(white_bg.convert("RGB"), (x, y))
		else:
			draw.rectangle((x, y, x + cell_width, y + cell_height), fill="#d1d5db")

		name = str(char.get("name") or "未知角色")
		char_level = _safe_int(char.get("level"))
		profession = char.get("profession") if isinstance(char.get("profession"), dict) else {}
		rarity = char.get("rarity") if isinstance(char.get("rarity"), dict) else {}
		profession_name = str(profession.get("value") or "未知职业")
		rarity_name = str(rarity.get("value") or "?")

		draw.text((x + 12, y + 14), name, fill="#ffffff", font=grid_font)
		draw.text((x + 12, y + 52), f"Lv.{char_level}", fill="#f9fafb", font=grid_small_font)
		draw.text((x + 12, y + 82), f"{profession_name}  {rarity_name}★", fill="#f3f4f6", font=grid_small_font)

	buf = io.BytesIO()
	image.save(buf, format="PNG")
	return buf.getvalue()


@user_card.handle()
async def handle_user_card(event: Event):
	api_key = _get_api_key()
	if not api_key:
		await user_card.finish("未配置 endfield_api_key，无法获取信息卡。")

	user_id = str(event.get_user_id())
	active_binding = _get_active_binding(user_id)
	if not active_binding:
		await user_card.finish("未找到已绑定账号，请先使用“终末地绑定”。")
	framework_token = active_binding["framework_token"]
	local_role_id = active_binding.get("role_id")
	local_server_id = active_binding.get("server_id")

	note_data = await asyncio.to_thread(
		api_request,
		"GET",
		"/api/endfield/note",
		{
			"X-API-Key": api_key,
			"X-Framework-Token": framework_token,
		},
	)

	if not isinstance(note_data, dict) or note_data.get("code") != 0:
		msg = note_data.get("message") if isinstance(note_data, dict) else None
		await user_card.finish(f"获取信息卡数据失败：{msg or '请稍后重试'}")

	try:
		image_bytes = await asyncio.to_thread(
			_render_note_card,
			note_data,
			local_role_id,
			local_server_id,
		)
	except Exception as e:
		logger.exception(f"render note card failed: {e}")
		await user_card.finish("生成信息卡失败，请稍后重试。")

	image_b64 = base64.b64encode(image_bytes).decode("utf-8")
	await user_card.finish(MessageSegment.image(f"base64://{image_b64}"))
