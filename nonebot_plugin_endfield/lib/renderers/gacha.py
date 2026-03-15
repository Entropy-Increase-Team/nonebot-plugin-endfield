import hashlib
import mimetypes
import base64
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from nonebot import get_plugin_config, logger

from .helpers import escape_text
from .report import render_report_image
from .runtime import render_html_to_image
from ...config import Config
from ..utils import get_api_key, get_data_dir


PLUGIN_CONFIG = get_plugin_config(Config)


def render_gacha_records_image(cache_data: Dict[str, Any], page: int = 1) -> bytes:
    stats = (cache_data.get("stats_data") or {}).get("stats") or {}
    sections: List[Tuple[str, List[str]]] = []

    pool_defs = (
        ("standard", "常驻角色"),
        ("beginner", "新手池"),
        ("weapon", "武器池"),
        ("limited", "限定角色"),
    )
    for key, label in pool_defs:
        pools = cache_data.get("records_by_pool") or {}
        rows = pools.get(key) if isinstance(pools.get(key), list) else []
        sorted_rows = sorted(
            rows,
            key=lambda x: (
                -(int(x.get("seq_id")) if str(x.get("seq_id", "")).isdigit() else 0),
                -(int(x.get("gacha_ts") or 0)),
            ),
        )
        total = len(sorted_rows)
        pages = max(1, (total + 9) // 10)
        current = max(1, min(page, pages))
        start = (current - 1) * 10
        picked = sorted_rows[start : start + 10]

        lines = [f"共 {total} 抽（第 {current}/{pages} 页）"]
        if picked:
            for idx, r in enumerate(picked, start=1):
                rarity = int(r.get("rarity") or 0)
                name = r.get("char_name") or r.get("item_name") or "未知"
                lines.append(f"{start + idx}. ★{rarity} {name}")
        else:
            lines.append("暂无记录")
        sections.append((label, lines))

    subtitle = (
        f"总抽数：{stats.get('total_count', 0)} | 六星：{stats.get('star6_count', 0)} | "
        f"五星：{stats.get('star5_count', 0)} | 四星：{stats.get('star4_count', 0)}"
    )
    updated_at = cache_data.get("updated_at")
    footer = ""
    if updated_at:
        try:
            footer = f"缓存时间：{datetime.fromtimestamp(float(updated_at) / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
        except Exception:
            footer = ""
    return render_report_image("终末地 抽卡记录", sections, subtitle=subtitle, footer=footer)


def render_gacha_analysis_image(stats_data: Dict[str, Any], cache_data: Dict[str, Any]) -> bytes:
    pool_stats = stats_data.get("pool_stats") or {}
    user_info = stats_data.get("user_info") or {}
    up_info = stats_data.get("up_info") or {}
    overall_stats = stats_data.get("stats") or {}
    records_by_pool = cache_data.get("records_by_pool") or {}
    gacha_icon_map = cache_data.get("gacha_icon_map") or {}

    up_char_names = {
        str(x).strip()
        for x in ((up_info.get("upCharNames") or up_info.get("char_up_names") or []))
        if str(x).strip()
    }
    up_weapon_name = str(up_info.get("upWeaponName") or up_info.get("weapon_up_name") or "").strip()
    raw_pool_up_map = up_info.get("poolUpMap") or up_info.get("pool_up_map") or {}
    pool_up_map = {
        str(pool_name).strip(): str(up_name).strip()
        for pool_name, up_name in raw_pool_up_map.items()
        if str(pool_name).strip() and str(up_name).strip()
    }

    def _to_int(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0

    def _to_bool(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ("1", "true", "yes")
        return bool(v)

    def _pool_stat(name1: str, name2: str) -> Dict[str, Any]:
        return (pool_stats.get(name1) or pool_stats.get(name2) or {})

    def _avg_cost(total: int, star6: int) -> str:
        if star6 <= 0:
            return "-"
        return str(round(total / star6))

    def _sort_record_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def _seq(r: Dict[str, Any]) -> int:
            try:
                return int(r.get("seq_id") or 0)
            except Exception:
                return 0

        def _ts(r: Dict[str, Any]) -> int:
            try:
                return int(r.get("gacha_ts") or 0)
            except Exception:
                return 0

        return sorted(rows, key=lambda x: (_ts(x), _seq(x)))

    def _group_pool_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            pool_name = str(row.get("pool_name") or "未知")
            grouped.setdefault(pool_name, []).append(row)
        return grouped

    def _text_matches(name: str, target: str) -> bool:
        left = str(name or "").strip()
        right = str(target or "").strip()
        return bool(left and right and (left == right or left in right or right in left))

    def _pool_specific_up(pool_name: str) -> str:
        current = str(pool_name or "").strip()
        for known_pool_name, up_name in pool_up_map.items():
            if _text_matches(current, known_pool_name):
                return up_name
        return ""

    def _is_up_item(name: str, pool_key: str, pool_name: str) -> bool:
        pool_up_name = _pool_specific_up(pool_name)
        if pool_up_name:
            return _text_matches(name, pool_up_name)
        if pool_key == "limited":
            return any(_text_matches(name, up_name) for up_name in up_char_names)
        if pool_key == "weapon" and up_weapon_name:
            return _text_matches(name, up_weapon_name)
        return False

    def _build_timeline_rows(
        rows: List[Dict[str, Any]],
        *,
        pool_key: str,
        pool_name: str,
        max_pity: int,
        initial_paid_pity: int = 0,
        initial_up_guaranteed: bool = False,
        include_tail_pity: bool = True,
    ) -> Dict[str, Any]:
        sorted_rows = _sort_record_rows(rows)  # 旧 -> 新
        paid_rows = [row for row in sorted_rows if not _to_bool(row.get("is_free"))]
        free_rows = [row for row in sorted_rows if _to_bool(row.get("is_free"))]

        def _segment_timeline(
            source_rows: List[Dict[str, Any]],
            *,
            initial_count: int = 0,
            initial_guaranteed: bool = False,
            include_tail: bool = True,
        ) -> Tuple[List[Dict[str, Any]], int, bool]:
            segments: List[Dict[str, Any]] = []
            count = max(0, int(initial_count))
            guaranteed_up = bool(initial_guaranteed)
            for row in source_rows:
                count += 1
                if _to_int(row.get("rarity")) == 6:
                    name = str(row.get("char_name") or row.get("item_name") or "6星")
                    tag = ""
                    if pool_key in ("standard", "beginner"):
                        tag = ""
                    elif pool_key == "limited":
                        is_up = _is_up_item(name, pool_key, pool_name)
                        if guaranteed_up and is_up:
                            tag = "保底"
                            guaranteed_up = False
                        elif is_up:
                            tag = "UP"
                        else:
                            tag = "歪"
                            guaranteed_up = True
                    elif pool_key == "weapon":
                        tag = "UP" if _is_up_item(name, pool_key, pool_name) else "歪"
                    if _to_bool(row.get("is_free")):
                        tag = "免费"
                    segments.append({"count": count, "name": name, "is_pity": False, "tag": tag})
                    count = 0
            if include_tail and count > 0:
                segments.append({"count": count, "name": "已垫", "is_pity": True})
            return segments, count, guaranteed_up

        paid_timeline_old_to_new, paid_tail_pity, paid_tail_guaranteed = _segment_timeline(
            paid_rows,
            initial_count=initial_paid_pity,
            initial_guaranteed=initial_up_guaranteed,
            include_tail=include_tail_pity,
        )
        free_timeline_old_to_new, _, _ = _segment_timeline(free_rows)

        def _ts(v: Dict[str, Any]) -> int:
            try:
                return int(v.get("gacha_ts") or 0)
            except Exception:
                return 0

        pool_sort_ts = min((_ts(r) for r in sorted_rows), default=0)

        return {
            "paid_timeline": paid_timeline_old_to_new,
            "paid_total": len(paid_rows),
            "free_total": len(free_rows),
            "free_timeline": free_timeline_old_to_new,
            "max_pity": max_pity,
            "sort_ts": pool_sort_ts,
            "paid_tail_pity": paid_tail_pity,
            "paid_tail_guaranteed": paid_tail_guaranteed,
        }

    def _build_pool_cards(pool_key: str, max_pity: int, *, shared_paid_pity: bool = False) -> List[Dict[str, Any]]:
        rows = records_by_pool.get(pool_key)
        if not isinstance(rows, list):
            return []
        grouped = _group_pool_rows(rows)
        grouped_items = list(grouped.items())

        def _pool_sort_ts(pool_rows: List[Dict[str, Any]]) -> int:
            try:
                return min(int((row or {}).get("gacha_ts") or 0) for row in pool_rows)
            except Exception:
                return 0

        grouped_items.sort(key=lambda item: (_pool_sort_ts(item[1]), item[0]))

        cards_chrono: List[Dict[str, Any]] = []
        carry_paid_pity = 0
        carry_up_guaranteed = False
        last_index = len(grouped_items) - 1
        for idx, (pool_name, pool_rows) in enumerate(grouped_items):
            include_tail = True if (not shared_paid_pity or idx == last_index) else False
            timeline_data = _build_timeline_rows(
                pool_rows,
                pool_key=pool_key,
                pool_name=pool_name,
                max_pity=max_pity,
                initial_paid_pity=(carry_paid_pity if shared_paid_pity else 0),
                initial_up_guaranteed=(carry_up_guaranteed if shared_paid_pity and pool_key == "limited" else False),
                include_tail_pity=include_tail,
            )
            cards_chrono.append(
                {
                    "pool_name": pool_name,
                    "pool_key": pool_key,
                    "paid_timeline": timeline_data["paid_timeline"],
                    "paid_total": timeline_data["paid_total"],
                    "free_total": timeline_data["free_total"],
                    "free_timeline": timeline_data["free_timeline"],
                    "max_pity": timeline_data["max_pity"],
                    "sort_ts": timeline_data["sort_ts"],
                }
            )
            if shared_paid_pity:
                carry_paid_pity = _to_int(timeline_data.get("paid_tail_pity"))
                if pool_key == "limited":
                    carry_up_guaranteed = bool(timeline_data.get("paid_tail_guaranteed"))

        # 最新卡池在前，和旧版展示一致
        cards_chrono.sort(key=lambda x: (-(x.get("sort_ts") or 0), x["pool_name"]))
        return cards_chrono

    limited_cards = _build_pool_cards("limited", 80, shared_paid_pity=True)
    weapon_cards = _build_pool_cards("weapon", 40)
    standard_cards = _build_pool_cards("standard", 80) + _build_pool_cards("beginner", 80)

    limited_stat = _pool_stat("limited_char", "limited")
    standard_stat = _pool_stat("standard_char", "standard")
    beginner_stat = _pool_stat("beginner_char", "beginner")
    weapon_stat = _pool_stat("weapon", "weapon")

    limited_total = _to_int(limited_stat.get("total") or limited_stat.get("total_count"))
    limited_6 = _to_int(limited_stat.get("star6") or limited_stat.get("star6_count"))
    weapon_total = _to_int(weapon_stat.get("total") or weapon_stat.get("total_count"))
    weapon_6 = _to_int(weapon_stat.get("star6") or weapon_stat.get("star6_count"))
    standard_total = _to_int(standard_stat.get("total") or standard_stat.get("total_count")) + _to_int(
        beginner_stat.get("total") or beginner_stat.get("total_count")
    )
    standard_6 = _to_int(standard_stat.get("star6") or standard_stat.get("star6_count")) + _to_int(
        beginner_stat.get("star6") or beginner_stat.get("star6_count")
    )

    nickname = str(user_info.get("nickname") or user_info.get("game_uid") or "未知")
    uid = str(user_info.get("game_uid") or "-")

    def _bar_color_level(count: int, max_pity: int, is_pity: bool = False) -> str:
        if is_pity:
            return "yellow"
        ratio = count / max(1, max_pity)
        if ratio < 0.5:
            return "green"
        if ratio < 0.8:
            return "yellow"
        return "red"

    def _bar_percent(count: int, max_pity: int) -> float:
        if max_pity <= 0:
            return 50.0
        return max(14.0, min(100.0, (count / max_pity) * 100.0))

    def _cache_remote_icon(icon_url: str) -> str:
        def _file_to_data_uri(path: Path) -> str:
            try:
                suffix = path.suffix.lower()
                mime = {
                    ".png": "image/png",
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".webp": "image/webp",
                    ".gif": "image/gif",
                    ".svg": "image/svg+xml",
                }.get(suffix, "application/octet-stream")
                payload = base64.b64encode(path.read_bytes()).decode("ascii")
                return f"data:{mime};base64,{payload}"
            except Exception:
                return ""

        def _escape_http_url_path(url: str) -> str:
            try:
                parts = urlsplit(url)
                if parts.scheme not in ("http", "https"):
                    return url
                escaped_path = quote(parts.path or "", safe="/%:@!$&'()*+,;=-._~")
                return urlunsplit((parts.scheme, parts.netloc, escaped_path, parts.query, parts.fragment))
            except Exception:
                return url

        raw_url = str(icon_url or "").strip()
        if not raw_url:
            return ""

        if raw_url.startswith("//"):
            raw_url = "https:" + raw_url

        # 兼容后端返回的相对路径，统一补全为绝对 URL
        if not raw_url.startswith(("http://", "https://", "file:///", "data:")):
            base_url = str(getattr(PLUGIN_CONFIG, "endfield_api_baseurl", "") or "").strip()
            if base_url:
                if raw_url.startswith("/"):
                    raw_url = f"{base_url.rstrip('/')}{raw_url}"
                else:
                    raw_url = f"{base_url.rstrip('/')}/{raw_url.lstrip('/')}"

        if raw_url.startswith("file:///"):
            try:
                local_path = Path(raw_url.replace("file:///", "", 1))
                if local_path.exists() and local_path.is_file():
                    data_uri = _file_to_data_uri(local_path)
                    if data_uri:
                        return data_uri
            except Exception:
                pass
            return raw_url
        if not (raw_url.startswith("http://") or raw_url.startswith("https://")):
            return raw_url

        raw_url = _escape_http_url_path(raw_url)

        cache_dir = get_data_dir() / "gacha_icon_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        suffix = Path(raw_url.split("?", 1)[0]).suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}:
            suffix = ".png"
        cache_file = cache_dir / f"{hashlib.md5(raw_url.encode('utf-8')).hexdigest()}{suffix}"
        if cache_file.exists() and cache_file.stat().st_size > 0:
            data_uri = _file_to_data_uri(cache_file)
            if data_uri:
                return data_uri
            return cache_file.resolve().as_uri()

        try:
            headers: Dict[str, str] = {}
            api_key = str(get_api_key() or "").strip()
            if api_key:
                headers["x-api-key"] = api_key
            response = httpx.get(raw_url, timeout=10.0, follow_redirects=True, headers=headers or None)
            response.raise_for_status()
            if suffix == ".png":
                content_type = (response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
                guessed_ext = mimetypes.guess_extension(content_type) or ""
                if guessed_ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg"}:
                    better_file = cache_dir / f"{hashlib.md5(raw_url.encode('utf-8')).hexdigest()}{guessed_ext}"
                    cache_file = better_file
            cache_file.write_bytes(response.content)
            data_uri = _file_to_data_uri(cache_file)
            if data_uri:
                return data_uri
            return cache_file.resolve().as_uri()
        except Exception as e:
            logger.debug(f"[终末地插件][抽卡头像]下载失败: {raw_url} | {type(e).__name__}: {e}")
            return _escape_http_url_path(raw_url)

    def _pick_avatar_url_from_name(name: str) -> str:
        key = str(name or "").strip()
        if not key:
            return ""
        def _norm(s: str) -> str:
            return "".join(ch.lower() for ch in str(s or "") if ch.isalnum() or ch in ("_",))

        key_norm = _norm(key)
        mapped_icon = str(gacha_icon_map.get(key) or "").strip()
        if mapped_icon:
            return _cache_remote_icon(mapped_icon)
        for map_name, icon_url in gacha_icon_map.items() if isinstance(gacha_icon_map, dict) else []:
            map_key = str(map_name or "").strip()
            if not map_key:
                continue
            map_norm = _norm(map_key)
            if key == map_key or (key_norm and map_norm and (key_norm in map_norm or map_norm in key_norm)):
                icon = str(icon_url or "").strip()
                if icon:
                    return _cache_remote_icon(icon)
        for rows in records_by_pool.values() if isinstance(records_by_pool, dict) else []:
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_name = str(row.get("char_name") or row.get("item_name") or "").strip()
                row_norm = _norm(row_name)
                if not (key == row_name or (key_norm and row_norm and (key_norm in row_norm or row_norm in key_norm))):
                    continue
                for icon_key in (
                    "avatarSqUrl",
                    "avatar_sq_url",
                    "avatarRtUrl",
                    "avatar_rt_url",
                    "avatar",
                    "avatar_url",
                    "avatarUrl",
                    "icon",
                    "icon_url",
                    "iconUrl",
                    "char_avatar",
                    "charAvatar",
                    "char_avatar_url",
                    "charAvatarUrl",
                    "item_icon",
                    "itemIcon",
                ):
                    icon = str(row.get(icon_key) or "").strip()
                    if icon:
                        return _cache_remote_icon(icon)
                nested = row.get("char_data") if isinstance(row.get("char_data"), dict) else row.get("charData")
                if isinstance(nested, dict):
                    for icon_key in ("avatarSqUrl", "avatarRtUrl", "avatarUrl", "iconUrl", "icon"):
                        icon = str(nested.get(icon_key) or "").strip()
                        if icon:
                            return _cache_remote_icon(icon)
        return ""

    def _render_star6_rows(card: Dict[str, Any]) -> str:
        paid_timeline = list(reversed(card.get("paid_timeline") or []))
        free_timeline = list(reversed(card.get("free_timeline") or []))
        max_pity = _to_int(card.get("max_pity")) or 80
        inherited_pity = _to_int(card.get("inherited_pity") or 0)

        def _icon_html(icon_url: str, alt_name: str) -> str:
            if not icon_url:
                return "<div class=\"star6-icon star6-icon-free\">×</div>"
            return (
                f"<img class=\"star6-icon\" src=\"{escape_text(icon_url)}\" alt=\"{escape_text(alt_name)}\" "
                "onerror=\"this.outerHTML='&lt;div class=\\\"star6-icon star6-icon-free\\\"&gt;×&lt;/div&gt;'\">"
            )

        parts: List[str] = []

        # 已垫行（显示继承保底）
        if paid_timeline and bool(paid_timeline[0].get("is_pity")):
            pity_count = _to_int(paid_timeline[0].get("count"))
            pity_bar = _bar_percent(pity_count, max_pity)
            inherited_pct = min(100.0, (inherited_pity / max(1, max_pity)) * 100.0) if inherited_pity > 0 else 0.0
            parts.append(
                "<div class=\"star6-row pity-row\">"
                "<div class=\"star6-icon star6-icon-pity\">?</div>"
                "<div class=\"bar-wrap\">"
                f"<div class=\"bar-inner bar-level-{_bar_color_level(pity_count, max_pity, True)} bar-not-full\" style=\"width:{pity_bar:.2f}%;\">"
                + (f"<div class=\"bar-inherit-overlay\" style=\"width:{inherited_pct:.2f}%;\"></div>" if inherited_pct > 0 else "")
                + f"<span class=\"bar-text\">已垫 {pity_count}</span></div></div>"
                "<span class=\"star6-tag star6-tag-empty\"></span>"
                "</div>"
            )

        for row in paid_timeline:
            if bool(row.get("is_pity")):
                continue
            count = _to_int(row.get("count"))
            name = str(row.get("name") or "")
            icon = _pick_avatar_url_from_name(name)
            tag = str(row.get("tag") or "")
            badge = str(row.get("badge") or "normal")
            if tag == "UP":
                badge = "up"
            elif tag == "保底":
                badge = "baodi"
            elif tag == "歪":
                badge = "wai"

            parts.append(
                "<div class=\"star6-row\">"
                + _icon_html(icon, name)
                + "<div class=\"bar-wrap\">"
                f"<div class=\"bar-inner bar-level-{_bar_color_level(count, max_pity)}{' bar-not-full' if count < max_pity else ''}\" style=\"width:{_bar_percent(count, max_pity):.2f}%;\">"
                f"<span class=\"bar-text\">{count}抽</span></div></div>"
                f"<span class=\"star6-tag star6-tag-{badge}\">{escape_text(tag)}</span>"
                "</div>"
            )

        # 免费十连：六星与免费进度条合并在同一行（免费xx抽）
        free_rows = [row for row in free_timeline if not bool(row.get("is_pity"))]
        free_hit = free_rows[0] if free_rows else None
        free_pity_row = next((row for row in free_timeline if bool(row.get("is_pity"))), None)
        free_total = _to_int((free_hit or {}).get("count") or (free_pity_row or {}).get("count") or 0)
        if str(card.get("pool_key") or "") == "limited" and free_total > 0:
            free_name = str((free_hit or {}).get("name") or "")
            free_icon = _pick_avatar_url_from_name(free_name) if free_name else ""
            parts.append(
                "<div class=\"star6-row free-row\">"
                + _icon_html(free_icon, free_name)
                + "<div class=\"bar-wrap\">"
                f"<div class=\"bar-inner bar-level-free bar-not-full\" style=\"width:{_bar_percent(free_total, 10):.2f}%;\">"
                f"<span class=\"bar-text\">免费 - {free_total}抽</span></div></div>"
                "<span class=\"star6-tag star6-tag-empty\"></span>"
                "</div>"
            )

        return "".join(parts)

    def _render_pool_group(label: str, cards: List[Dict[str, Any]]) -> str:
        if not cards:
            return (
                "<div class=\"pool-group\">"
                "<div class=\"pool-group-header\"><span class=\"pool-group-title\">%s</span></div>"
                "<div class=\"pool-group-empty\">暂无记录</div></div>"
            ) % escape_text(label)

        entries: List[str] = []
        for card in cards:
            pool_name = str(card.get("pool_name") or "未知")
            pool_key = str(card.get("pool_key") or "")
            paid_total = _to_int(card.get("paid_total"))
            free_total = _to_int(card.get("free_total"))
            paid_timeline = card.get("paid_timeline") or []
            red_count = len([x for x in paid_timeline if not bool(x.get("is_pity"))])
            up_count = len([x for x in paid_timeline if str(x.get("tag") or "") in ("UP", "保底")])
            is_limited_pool = pool_key == "limited" or bool(_pool_specific_up(pool_name))
            if pool_key == "weapon":
                metric1_label = "每红花费"
                metric1_val = str(round((paid_total // 10) / red_count)) + "抽" if red_count > 0 else "-"
            elif is_limited_pool:
                metric1_label = "平均UP花费"
                metric1_val = str(round(paid_total / up_count)) + "抽" if up_count > 0 else "-"
            else:
                metric1_label = "每红花费"
                metric1_val = str(round(paid_total / red_count)) + "抽" if red_count > 0 else "-"

            metric2_label = "不歪率" if is_limited_pool and red_count > 0 else "出红数"
            metric2_val = f"{round((up_count / red_count) * 100, 1)}%" if is_limited_pool and red_count > 0 else str(red_count)
            total_metric = f"合计 {paid_total} 抽"
            pity_row = next((x for x in reversed(paid_timeline) if bool(x.get("is_pity"))), None)
            pity_count = _to_int((pity_row or {}).get("count"))
            if pity_count > 0:
                total_metric += f" - 垫 {pity_count}"

            entries.append(
                "<div class=\"pool-entry\">"
                f"<div class=\"pool-entry-header\"><span class=\"pool-entry-name\">{escape_text(pool_name)}</span></div>"
                "<div class=\"pool-entry-metrics\">"
                f"<span class=\"metric\">{escape_text(total_metric)}</span>"
                f"<span class=\"metric\">{escape_text(metric1_label)} {escape_text(metric1_val)}</span>"
                f"<span class=\"metric\">{escape_text(metric2_label)} {escape_text(metric2_val)}</span>"
                + (f"<span class=\"metric\">免费 {free_total} 抽</span>" if pool_key == "limited" and free_total > 0 else "")
                + "</div>"
                f"<div class=\"star6-list\">{_render_star6_rows(card)}</div>"
                "</div>"
            )

        return (
            "<div class=\"pool-group\">"
            f"<div class=\"pool-group-header\"><span class=\"pool-group-title\">{escape_text(label)}</span></div>"
            + "".join(entries)
            + "</div>"
        )

    updated_at = cache_data.get("updated_at")
    time_text = ""
    if updated_at:
        try:
            time_text = datetime.fromtimestamp(float(updated_at) / 1000).strftime("%Y-%m-%d %H:%M")
        except Exception:
            time_text = ""

    avatar_candidates = [
        str(user_info.get("avatar") or "").strip(),
        str(user_info.get("avatar_url") or "").strip(),
        str(user_info.get("avatarUrl") or "").strip(),
        str(user_info.get("head_url") or "").strip(),
        str(user_info.get("headUrl") or "").strip(),
    ]
    qq_user_id = str(cache_data.get("user_id") or "").strip()
    if qq_user_id:
        avatar_candidates.append(f"https://q1.qlogo.cn/g?b=qq&nk={qq_user_id}&s=100")
    avatar_url = next((url for url in avatar_candidates if url), "")

    analysis_time = escape_text(time_text) if time_text else ""
    sync_hint = "若需刷新，发送 /终末地同步抽卡记录"
    font_path = (Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansCJKsc-Bold.otf").as_posix()

    pool_groups_html = "".join(
        [
            _render_pool_group("特许寻访", limited_cards),
            _render_pool_group("武器池", weapon_cards),
            _render_pool_group("常驻寻访", standard_cards),
        ]
    )

    body = f"""
<div class="gacha-analysis-container">
    <div class="top-bar">
        <div class="user-info">
            {f'<img src="{escape_text(avatar_url)}" alt="头像" class="user-avatar" onerror="this.style.display=\'none\'">' if avatar_url else ''}
            <div class="user-details">
                <div class="user-name">{escape_text(nickname)}</div>
                <div class="user-uid">UID {escape_text(uid)}</div>
            </div>
        </div>
        <div class="top-bar-center">
            <h1 class="top-title">抽卡分析</h1>
            <p class="top-sub">终末地寻访统计概览</p>
        </div>
    </div>

    <div class="stats-row">
        <div class="stat-item"><div class="stat-content"><span class="stat-label-top">总抽数</span><span class="stat-num">{_to_int(overall_stats.get('total_count'))}</span></div></div>
        <div class="stat-item stat-stars"><div class="stat-content"><span class="stat-label-top">6星 / 5星 / 4星</span><div class="stat-stars-row"><span class="star-item star-6">{_to_int(overall_stats.get('star6_count'))}</span><span class="star-separator">/</span><span class="star-item star-5">{_to_int(overall_stats.get('star5_count'))}</span><span class="star-separator">/</span><span class="star-item star-4">{_to_int(overall_stats.get('star4_count'))}</span></div></div></div>
        <div class="stat-item stat-limited"><div class="stat-content"><span class="stat-label-top">特许寻访 · 平均出红</span><span class="stat-num">{escape_text(_avg_cost(limited_total, limited_6))}</span></div></div>
        <div class="stat-item stat-weapon"><div class="stat-content"><span class="stat-label-top">武器池 · 平均出红</span><span class="stat-num">{escape_text(_avg_cost(weapon_total, weapon_6))}</span></div></div>
        <div class="stat-item stat-standard"><div class="stat-content"><span class="stat-label-top">常驻寻访 · 平均出红</span><span class="stat-num">{escape_text(_avg_cost(standard_total, standard_6))}</span></div></div>
    </div>

    <div class="time-bar">
        <span class="analysis-time">{analysis_time}</span>
        <span class="sync-hint">{escape_text(sync_hint)}</span>
    </div>

    <div class="pool-groups-container">{pool_groups_html}</div>

    <footer class="footer"><div class="footer-text">Endfield Plugin | NoneBot</div></footer>
</div>
"""

    analysis_style = """
@font-face {
        font-family: 'HarmonyOS Sans SC';
        src: url('file:///FONT_PATH_PLACEHOLDER') format('opentype');
    font-weight: bold;
    font-style: normal;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

html,
body {
    width: 1500px;
    background: #f0f1f3;
    font-family: 'HarmonyOS Sans SC', -apple-system, BlinkMacSystemFont, 'Microsoft YaHei', sans-serif;
    color: #1f2937;
}

.wrap {
    padding: 0;
    width: 1500px;
}

.gacha-analysis-container {
    width: 1500px;
    padding: 14px 16px 6px;
}

.top-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    margin-bottom: 12px;
    background: #fff;
    border-radius: 10px;
    position: relative;
}

.user-info {
    display: flex;
    align-items: center;
    gap: 8px;
}

.user-avatar {
    width: 48px;
    height: 48px;
    border-radius: 10px;
    border: 2px solid #e5e7eb;
    object-fit: cover;
}

.user-details {
    display: flex;
    flex-direction: column;
    gap: 2px;
}

.user-name {
    font-size: 1rem;
    font-weight: bold;
    color: #1f2937;
}

.user-uid {
    font-size: 0.75rem;
    color: #6b7280;
}

.top-bar-center {
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    text-align: center;
}

.top-title {
    font-size: 1.4rem;
    font-weight: bold;
    color: #1f2937;
    margin-bottom: 2px;
}

.top-sub {
    font-size: 0.82rem;
    color: #6b7280;
}

.stats-row {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 10px;
    margin-bottom: 12px;
}

.stat-item {
    background: #fff;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
}

.stat-content {
    padding: 14px 12px;
    text-align: center;
}

.stat-num {
    display: block;
    font-size: 1.6rem;
    font-weight: bold;
    color: #1f2937;
    line-height: 1.1;
}

.stat-label-top {
    display: block;
    font-size: 0.7rem;
    color: #000;
    font-weight: bold;
    letter-spacing: 0.02em;
    margin-bottom: 6px;
}

.stat-stars { background: linear-gradient(135deg, #f9fafb 0%, #f3f4f6 100%); }
.stat-stars-row {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 6px;
}
.star-item {
    font-size: 1.3rem;
    font-weight: bold;
}
.star-separator {
    font-size: 1.1rem;
    color: #9ca3af;
}
.star-6 { color: #dc2626; }
.star-5 { color: #d97706; }
.star-4 { color: #7c3aed; }

.stat-limited { background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%); }
.stat-limited .stat-num { color: #dc2626; }
.stat-weapon { background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%); }
.stat-weapon .stat-num { color: #d97706; }
.stat-standard { background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%); }
.stat-standard .stat-num { color: #2563eb; }

.time-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
    padding: 0 2px;
}

.analysis-time {
    font-size: 0.82rem;
    color: #6b7280;
}

.sync-hint {
    font-size: 0.75rem;
    color: #1f2937;
}

.pool-groups-container {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin-bottom: 10px;
    align-items: start;
}

.pool-group {
    background: #fff;
    border-radius: 10px;
    border: 1px solid #e5e7eb;
    padding: 14px 16px;
}

.pool-group-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
    padding-bottom: 8px;
    border-bottom: 1px solid #f0f0f0;
}

.pool-group-title {
    font-size: 1.05rem;
    font-weight: bold;
    color: #1f2937;
    padding-left: 10px;
    border-left: 4px solid #374151;
}

.pool-group-empty {
    font-size: 0.85rem;
    color: #d1d5db;
    padding: 8px 0;
    text-align: center;
}

.pool-entry {
    padding: 10px 12px;
    margin-bottom: 8px;
    background: #f9fafb;
    border-radius: 8px;
    border: 1px solid #f0f0f0;
}

.pool-group .pool-entry:last-child {
    margin-bottom: 0;
}

.pool-entry-header {
    margin-bottom: 6px;
}

.pool-entry-name {
    font-size: 0.95rem;
    font-weight: bold;
    color: #1f2937;
}

.pool-entry-metrics {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 10px;
    font-size: 0.78rem;
    color: #9ca3af;
    margin-bottom: 8px;
    padding-bottom: 8px;
    border-bottom: 1px solid #e5e7eb;
}

.pool-entry-metrics .metric:first-child {
    font-weight: bold;
    color: #1f2937;
}

.star6-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-top: 6px;
}

.star6-row {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
}

.star6-icon {
    width: 40px;
    height: 40px;
    object-fit: cover;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
    flex-shrink: 0;
}

.star6-icon-pity {
    width: 40px;
    height: 40px;
    min-width: 40px;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
    background: #e5e7eb;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.2rem;
    font-weight: bold;
    color: #6b7280;
    flex-shrink: 0;
}

.star6-icon-free {
    width: 40px;
    height: 40px;
    min-width: 40px;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
    background: #fef3c7;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.3rem;
    font-weight: bold;
    color: #92400e;
    flex-shrink: 0;
}

.bar-wrap {
    position: relative;
    flex: 1;
    min-width: 60px;
    height: 28px;
    background: #e5e7eb;
    border-radius: 14px;
    overflow: hidden;
}

.bar-inner {
    position: relative;
    z-index: 2;
    height: 100%;
    min-width: 40px;
    border-radius: 14px;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0 8px;
    overflow: hidden;
}

.bar-not-full {
    border-radius: 14px 0 0 14px;
}

.bar-text {
    position: relative;
    z-index: 2;
    font-size: 0.82rem;
    font-weight: 600;
    color: #fff;
    white-space: nowrap;
}

.bar-level-green { background: #22c55e; }
.bar-level-yellow { background: #eab308; }
.bar-level-red { background: #ef4444; }
.bar-level-free { background: #f59e0b; }

.bar-inherit-overlay {
    position: absolute;
    top: 0;
    left: 0;
    height: 100%;
    background: #3b82f6;
    border-radius: 14px 0 0 14px;
    z-index: 1;
}

.star6-tag {
    flex-shrink: 0;
    margin-left: 4px;
    width: 28px;
    height: 22px;
    min-width: 28px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.68rem;
    font-weight: 600;
    border-radius: 4px;
    line-height: 1;
}

.star6-tag-wai { color: #fff; background: #64748b; }
.star6-tag-baodi { color: #fff; background: #0284c7; }
.star6-tag-up { color: #fff; background: #b45309; }
.star6-tag-normal { background: transparent; }
.star6-tag-empty { background: transparent; }

.pity-row {
    margin-top: 2px;
    padding-top: 4px;
    border-top: 1px dashed #e5e7eb;
}

.free-row {
    margin-top: 2px;
    padding-top: 4px;
    border-top: 1px dashed #e5e7eb;
}

.footer {
    text-align: center;
    margin-top: 4px;
    padding: 6px 0 0;
}

.footer-text {
    font-size: 0.7rem;
    color: #1f2937;
}
""".replace("FONT_PATH_PLACEHOLDER", font_path)

    return render_html_to_image(body, width=1500, extra_styles=analysis_style)


def render_gacha_global_stats_image(stats_data: Dict[str, Any], keyword: str = "") -> bytes:
    s = stats_data.get("stats") or stats_data
    by_channel = s.get("by_channel") or {}
    by_type = s.get("by_type") or {}

    def _fmt(v: Any, ndigits: int = 2) -> str:
        try:
            return f"{float(v):.{ndigits}f}"
        except Exception:
            return "-"

    current_pool = s.get("current_pool") or {}
    up_name = current_pool.get("up_char_name") or "-"
    up_weapon = current_pool.get("up_weapon_name") or "-"

    sections: List[Tuple[str, List[str]]] = []
    for key, label in (("beginner", "新手池"), ("standard", "常驻池"), ("weapon", "武器池"), ("limited", "限定池")):
        item = by_type.get(key) or {}
        total = int(item.get("total") or 0)
        star6 = int(item.get("star6") or 0)
        star5 = int(item.get("star5") or 0)
        star4 = int(item.get("star4") or 0)
        avg = _fmt(item.get("avg_pity"), 1)
        rate = (star6 / total * 100) if total > 0 else 0
        sections.append(
            (
                label,
                [
                    f"总抽数：{total}",
                    f"六星：{star6} | 五星：{star5} | 四星：{star4}",
                    f"出红率：{rate:.2f}% | 均出：{avg} 抽",
                ],
            )
        )

    official = by_channel.get("official")
    bilibili = by_channel.get("bilibili")
    if isinstance(official, dict):
        sections.append(
            (
                "官服",
                [
                    f"统计用户：{official.get('total_users', 0)}",
                    f"总抽数：{official.get('total_pulls', 0)}",
                    f"平均出红：{_fmt(official.get('avg_pity'))} 抽",
                ],
            )
        )
    if isinstance(bilibili, dict):
        sections.append(
            (
                "B服",
                [
                    f"统计用户：{bilibili.get('total_users', 0)}",
                    f"总抽数：{bilibili.get('total_pulls', 0)}",
                    f"平均出红：{_fmt(bilibili.get('avg_pity'))} 抽",
                ],
            )
        )

    subtitle = (
        f"总抽数：{s.get('total_pulls', 0)} | 统计用户：{s.get('total_users', 0)} | 平均出红：{_fmt(s.get('avg_pity'))} 抽\n"
        f"六星：{s.get('star6_total', 0)} | 五星：{s.get('star5_total', 0)} | 四星：{s.get('star4_total', 0)}\n"
        f"当期UP角色：{up_name} | UP武器：{up_weapon}"
    )
    footer = f"查询池：{keyword}" if keyword else ""
    return render_report_image("终末地 全服抽卡统计", sections, subtitle=subtitle, footer=footer)
