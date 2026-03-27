from datetime import datetime
from pathlib import Path
import json
import re
from typing import Any, Callable, Dict

from .helpers import escape_text
from .runtime import render_html_to_image, render_page_html_to_image


def build_character_list_html(
    chars: list[Any],
    safe_int: Callable[[Any, int], int],
) -> tuple[list[dict[str, Any]], str]:
    sorted_chars = sorted(
        [item for item in chars if isinstance(item, dict)],
        key=lambda item: safe_int(item.get("level"), 0),
        reverse=True,
    )

    char_cards: list[str] = []
    for char in sorted_chars:
        name = str(char.get("name") or "未知角色")
        char_level = safe_int(char.get("level"), 0)
        profession = char.get("profession") if isinstance(char.get("profession"), dict) else {}
        rarity = char.get("rarity") if isinstance(char.get("rarity"), dict) else {}
        profession_name = str(profession.get("value") or "未知职业")
        rarity_name = str(rarity.get("value") or "?")
        avatar_rt_url = str(char.get("avatarRtUrl") or "").strip()
        bg_style = (
            f"background-image:linear-gradient(180deg, rgba(15,23,42,0.2), rgba(15,23,42,0.72)),url('{escape_text(avatar_rt_url)}');"
            "background-size:cover,cover;background-position:center,center calc(30%);"
            if avatar_rt_url
            else "background:linear-gradient(135deg,#dbe7f7,#b6c8df);"
        )
        char_cards.append(
            "<article class=\"char\" style=\"%s\">"
            "<h3>%s</h3>"
            "<p>Lv.%s</p>"
            "<p>%s %s★</p>"
            "</article>"
            % (
                bg_style,
                escape_text(name),
                escape_text(char_level),
                escape_text(profession_name),
                escape_text(rarity_name),
            )
        )

    return sorted_chars, "".join(char_cards)


def render_user_char_list_card(
        note_data: Dict[str, Any],
        local_role_id: str | None,
        local_server_id: str | None,
) -> bytes:
        data = note_data.get("data") if isinstance(note_data, dict) else None
        if not isinstance(data, dict):
                data = {}

        base = data.get("base") if isinstance(data.get("base"), dict) else {}
        chars = data.get("chars") if isinstance(data.get("chars"), list) else []

        def _safe_int(value: Any, default: int = 0) -> int:
                try:
                        return int(str(value))
                except Exception:
                        return default

        role_name = str(base.get("name") or "未知用户")
        api_role_id = str(base.get("roleId") or "")
        role_id = str(local_role_id or api_role_id or "未知")
        server_name = str(local_server_id or "未知")

        sorted_chars, char_cards_html = build_character_list_html(chars, _safe_int)

        body = f"""
<div class=\"card\">
    <div class=\"head\">
        <h1>终末地角色列表</h1>
    </div>
    <div class=\"content role-list-content\">
        <section class=\"section\">
            <h2 class=\"section-title\">账号信息</h2>
            <ul class=\"section-body\">
                <li>昵称：{escape_text(role_name)}</li>
                <li>UID：{escape_text(role_id)} | 服务器：{escape_text(server_name)}</li>
            </ul>
        </section>

        <section class=\"section\">
            <h2 class=\"section-title\">角色列表（共 {len(sorted_chars)} 名）</h2>
            <div class=\"char-grid\">{char_cards_html or '<div class="block">暂无角色数据</div>'}</div>
        </section>
    </div>
</div>
"""

        role_list_style = (
                ".role-list-content{display:flex;flex-direction:column;gap:14px;}"
                ".char-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}"
                ".char{min-height:180px;border-radius:10px;padding:10px 10px 12px;color:#fff;border:1px solid rgba(255,255,255,0.2);"
                "display:flex;flex-direction:column;justify-content:flex-end;box-shadow:inset 0 -40px 80px rgba(15,23,42,0.38);}"
                ".char h3{margin:0 0 6px;font-size:21px;line-height:1.25;}"
                ".char p{margin:0 0 4px;font-size:16px;line-height:1.35;}"
        )
        return render_html_to_image(body, width=1280, extra_styles=role_list_style)


def render_user_note_card(
    note_data: Dict[str, Any],
    local_role_id: str | None,
    local_server_id: str | None,
    spaceship_data: Dict[str, Any] | None = None,
) -> bytes:
    del spaceship_data

    data = note_data.get("data") if isinstance(note_data, dict) else None
    if not isinstance(data, dict):
        data = {}

    base = data.get("base") if isinstance(data.get("base"), dict) else {}
    bp = data.get("bpSystem") if isinstance(data.get("bpSystem"), dict) else {}
    chars = data.get("chars") if isinstance(data.get("chars"), list) else []
    achieve = data.get("achieve") if isinstance(data.get("achieve"), dict) else {}

    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(str(value))
        except Exception:
            return default

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

    role_name = str(base.get("name") or "未知用户")
    api_role_id = str(base.get("roleId") or "")
    role_id = str(local_role_id or api_role_id or "未知")
    level = _safe_int(base.get("level"))
    server_name = str(local_server_id or "未知")
    create_time = _format_timestamp(base.get("createTime"))

    char_num = _safe_int(base.get("charNum"))
    weapon_num = _safe_int(base.get("weaponNum"))
    doc_num = _safe_int(base.get("docNum"))
    exp = _safe_int(base.get("exp"))

    bp_cur = _safe_int(bp.get("curLevel"))

    avatar_url = str(base.get("avatarUrl") or "").strip()
    mission = base.get("mainMission") if isinstance(base.get("mainMission"), dict) else {}
    mission_text = str(mission.get("description") or "无主线信息")

    def _normalize_url(url: Any) -> str:
        text = str(url or "").strip()
        if not text:
            return ""
        if text.startswith("http://") or text.startswith("https://"):
            return text
        if text.startswith("//"):
            return f"https:{text}"
        if text.startswith("/"):
            return f"https://bbs.hycdn.cn{text}"
        return text

    def _extract_rarity(char: dict[str, Any]) -> int:
        rarity_obj = char.get("rarity") if isinstance(char.get("rarity"), dict) else {}
        candidates = [
            rarity_obj.get("value"),
            rarity_obj.get("id"),
            rarity_obj.get("level"),
            char.get("rarity"),
            char.get("rarityLevel"),
        ]
        for item in candidates:
            text = str(item or "").strip()
            if not text:
                continue
            m = re.search(r"([1-6])", text)
            if m:
                return int(m.group(1))
            if "六" in text:
                return 6
            if "五" in text:
                return 5
            if "四" in text:
                return 4
        return 0

    def _accent_by_rarity(rarity: int) -> str:
        if rarity >= 6:
            return "#ff7000"
        if rarity == 5:
            return "#efae03"
        if rarity == 4:
            return "#9452fa"
        return "#9452fa"

    assets_root = Path(__file__).resolve().parents[2] / "assets"
    table_candidates = [
        assets_root / "templates" / "table.json",
        assets_root / "table.json",
    ]
    table_data: dict[str, dict[str, str]] = {"property": {}, "profession": {}}
    for table_path in table_candidates:
        if not table_path.exists():
            continue
        try:
            raw = json.loads(table_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                prop = raw.get("property") if isinstance(raw.get("property"), dict) else {}
                prof = raw.get("profession") if isinstance(raw.get("profession"), dict) else {}
                table_data = {
                    "property": {str(k): str(v) for k, v in prop.items()},
                    "profession": {str(k): str(v) for k, v in prof.items()},
                }
                break
        except Exception:
            continue

    medals = achieve.get("achieveMedals") if isinstance(achieve.get("achieveMedals"), list) else []
    display = achieve.get("display") if isinstance(achieve.get("display"), dict) else {}
    medal_by_id: dict[str, dict[str, Any]] = {}

    def _norm_id(value: Any) -> str:
        return str(value or "").strip().lower()

    for item in medals:
        if not isinstance(item, dict):
            continue
        achv_data = item.get("achievementData") if isinstance(item.get("achievementData"), dict) else {}
        for raw_id in (
            achv_data.get("id"),
            item.get("id"),
            item.get("achievementId"),
            item.get("medalId"),
        ):
            medal_id = _norm_id(raw_id)
            if medal_id:
                medal_by_id[medal_id] = item

    medal_cards: list[str] = []
    for slot_index in range(1, 11):
        medal_id = _norm_id(display.get(str(slot_index)) or display.get(slot_index))
        item = medal_by_id.get(medal_id) if medal_id else None
        achv_data = item.get("achievementData") if isinstance(item, dict) and isinstance(item.get("achievementData"), dict) else {}
        is_plated = bool(item.get("isPlated")) if isinstance(item, dict) else False
        medal_level = _safe_int(item.get("level")) if isinstance(item, dict) else 0

        init_icon = _normalize_url(achv_data.get("initIcon"))
        plated_icon = _normalize_url(achv_data.get("platedIcon"))
        reforge_icon = _normalize_url(achv_data.get(f"reforge{medal_level}Icon"))
        icon_url = reforge_icon or (plated_icon if (is_plated and plated_icon) else init_icon)
        name = str(achv_data.get("name") or f"徽章{slot_index}").strip()
        safe_name = escape_text(name)

        if icon_url:
            icon_html = f"<img src=\"{escape_text(icon_url)}\" alt=\"{safe_name}\" loading=\"lazy\" onerror=\"this.remove()\" />"
        else:
            icon_html = f"<img src=\"https://bbs.hycdn.cn/image/2026/03/10/25b253bc7f352eacc7d08fce197198d2.png\" alt=\"{safe_name}\" />"

        medal_cards.append(
            f"<article class=\"road-item\"><div class=\"road-icon\">{icon_html}</div></article>"
        )

    sorted_chars, _ = build_character_list_html(chars, _safe_int)
    top_chars = sorted_chars[:4]
    operator_boxes: list[str] = []
    for idx in range(4):
        char = top_chars[idx] if idx < len(top_chars) else {}
        char_level = _safe_int(char.get("level"), 0) if isinstance(char, dict) else 0
        char_rarity = _extract_rarity(char) if isinstance(char, dict) else 0
        accent_color = _accent_by_rarity(char_rarity)
        profession_name = (
            str((char.get("profession") if isinstance(char.get("profession"), dict) else {}).get("value") or "").strip()
            if isinstance(char, dict)
            else ""
        )
        property_name = (
            str((char.get("property") if isinstance(char.get("property"), dict) else {}).get("value") or "").strip()
            if isinstance(char, dict)
            else ""
        )
        profession_icon = table_data.get("profession", {}).get(profession_name, "")
        property_icon = table_data.get("property", {}).get(property_name, "")

        style_parts = [f"--accent-color:{accent_color};"]
        if profession_icon:
            style_parts.append(f"--profession-icon:url('{profession_icon}');")
        if property_icon:
            style_parts.append(f"--property-icon:url('{property_icon}');")
        box_style = " ".join(style_parts)
        avatar_rt_url = _normalize_url(char.get("avatarRtUrl") if isinstance(char, dict) else "")
        avatar_html = (
            f"<img class=\"operator-overlay-image\" src=\"{escape_text(avatar_rt_url)}\" alt=\"干员图{idx + 1}\" loading=\"lazy\" onerror=\"this.remove()\" />"
            if avatar_rt_url
            else ""
        )
        operator_boxes.append(
            f"<div class=\"operator-overlay-box\" style=\"{box_style}\">"
            "<span class=\"operator-overlay-topicon\"></span>"
            f"{avatar_html}"
            "<div class=\"operator-overlay-meta\"><span class=\"operator-level-label\">Lv.</span>"
            f"<span class=\"operator-level-value\">{char_level}</span></div>"
            "<div class=\"operator-overlay-mask\"></div>"
            "</div>"
        )

    template_candidates = [
        assets_root / "templates" / "user_note.html",
        assets_root / "user_note.html",
    ]
    template_path = next((p for p in template_candidates if p.exists()), None)
    if template_path is None:
        raise FileNotFoundError(f"未找到用户信息模板: {template_candidates[0]}")

    page_html = template_path.read_text(encoding="utf-8")

    def _replace_once(src: str, old: str, new: str) -> str:
        if old not in src:
            return src
        return src.replace(old, new, 1)

    safe_name = escape_text(role_name)
    safe_uid = escape_text(role_id)
    safe_server = escape_text(server_name)
    safe_mission = escape_text(mission_text)
    create_date = escape_text(create_time.split(" ", 1)[0] if " " in create_time else create_time)
    safe_avatar_url = escape_text(avatar_url)

    page_html = _replace_once(page_html, "<h1 class=\"profile-name\">名称</h1>", f"<h1 class=\"profile-name\">{safe_name}</h1>")
    page_html = _replace_once(
        page_html,
        "<p class=\"profile-zhuangshi\">[x ---] </p>",
        f"<p class=\"profile-zhuangshi\">[Lv.{level} - {safe_server}] </p>",
    )
    page_html = _replace_once(page_html, "<p class=\"profile-date\">2026-03-27</p>", f"<p class=\"profile-date\">{create_date}</p>")
    page_html = _replace_once(page_html, "<p class=\"profile-uid\">123456789</p>", f"<p class=\"profile-uid\">{safe_uid}</p>")

    if avatar_url:
        avatar_html = (
            f"<div class=\"avatar-placeholder\"><img src=\"{safe_avatar_url}\" alt=\"头像\" "
            "style=\"width:100%;height:100%;object-fit:cover;display:block;\" /></div>"
        )
        page_html = _replace_once(page_html, "<div class=\"avatar-placeholder\"></div>", avatar_html)

    page_html = re.sub(
        r"<div class=\"account-text-row\"><img src=\"([^\"]*权限等阶\.png)\"[^>]*alt=\"权限等阶图标\"[^>]*/>权限等阶</div>",
        (
            "<div class=\"account-text-row\">"
            "<img src=\"\\1\" alt=\"权限等阶图标\" />"
            "权限等阶"
            f"<span style=\"margin-left:auto;\">{level}</span>"
            "</div>"
        ),
        page_html,
        count=1,
    )
    page_html = re.sub(
        r"<div class=\"account-text-row\"><img src=\"([^\"]*探索等级\.png)\"[^>]*alt=\"探索等级图标\"[^>]*/>探索等级</div>",
        (
            "<div class=\"account-text-row\">"
            "<img src=\"\\1\" alt=\"通行证等级图标\" />"
            "通行证等级"
            f"<span style=\"margin-left:auto;\">{bp_cur}</span>"
            "</div>"
        ),
        page_html,
        count=1,
    )
    page_html = _replace_once(
        page_html,
        "<span class=\"chapter-value\" id=\"chapter-progress-value\">第二章 —— XXXX</span>",
        f"<span class=\"chapter-value\" id=\"chapter-progress-value\">{safe_mission}</span>",
    )

    for number in (char_num, weapon_num, doc_num):
        page_html = _replace_once(
            page_html,
            "<span class=\"extra-card-number\">00</span>",
            f"<span class=\"extra-card-number\">{number}</span>",
        )

    page_html = re.sub(
        r"<div class=\"road-flow\">[\s\S]*?</div>\s*</div>\s*<div class=\"gallery-item operator-gallery\">",
        f"<div class=\"road-flow\">{''.join(medal_cards)}</div>\n\t\t\t</div>\n\t\t\t<div class=\"gallery-item operator-gallery\">",
        page_html,
        count=1,
    )

    page_html = re.sub(
        r"<div class=\"operator-overlay-row\">[\s\S]*?</div>\s*<img class=\"gallery-image\" src=\"([^\"]*干员展示\.png)\"[^>]*alt=\"干员展示\"[^>]*/>",
        f"<div class=\"operator-overlay-row\">{''.join(operator_boxes)}</div>\n\t\t\t\t<img class=\"gallery-image\" src=\"\\1\" alt=\"干员展示\" />",
        page_html,
        count=1,
    )

    page_html = page_html.replace(
        ".operator-overlay-box:nth-child(2)::after {\n\t\t\t--accent-color: #efae03;\n\t\t}\n\n\t\t.operator-overlay-box:nth-child(3)::after,\n\t\t.operator-overlay-box:nth-child(4)::after {\n\t\t\t--accent-color: #9452fa;\n\t\t}",
        ".operator-overlay-box:nth-child(2)::after,\n\t\t.operator-overlay-box:nth-child(3)::after,\n\t\t.operator-overlay-box:nth-child(4)::after {\n\t\t\t--accent-color: inherit;\n\t\t}",
    )

    return render_page_html_to_image(
        page_html,
        width=1920,
        height=1080,
        full_page=False,
        base_dir=template_path.parent,
    )
