from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .helpers import escape_text
from .runtime import render_html_to_image


def render_user_note_card(note_data: Dict[str, Any], local_role_id: str | None, local_server_id: str | None) -> bytes:
    data = note_data.get("data") if isinstance(note_data, dict) else None
    if not isinstance(data, dict):
        data = {}

    base = data.get("base") if isinstance(data.get("base"), dict) else {}
    bp = data.get("bpSystem") if isinstance(data.get("bpSystem"), dict) else {}
    daily = data.get("dailyMission") if isinstance(data.get("dailyMission"), dict) else {}
    stamina = data.get("stamina") if isinstance(data.get("stamina"), dict) else {}
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
    last_login = _format_timestamp(base.get("lastLoginTime"))

    char_num = _safe_int(base.get("charNum"))
    weapon_num = _safe_int(base.get("weaponNum"))
    doc_num = _safe_int(base.get("docNum"))
    exp = _safe_int(base.get("exp"))

    bp_cur = _safe_int(bp.get("curLevel"))
    bp_max = max(1, _safe_int(bp.get("maxLevel"), 1))
    activation = _safe_int(daily.get("activation"))
    activation_max = max(1, _safe_int(daily.get("maxActivation"), 1))
    stamina_cur = _safe_int(stamina.get("current"))
    stamina_max = max(1, _safe_int(stamina.get("max"), 1))

    avatar_url = str(base.get("avatarUrl") or "").strip()
    mission = base.get("mainMission") if isinstance(base.get("mainMission"), dict) else {}
    mission_text = str(mission.get("description") or "无主线信息")

    medals = achieve.get("achieveMedals") if isinstance(achieve.get("achieveMedals"), list) else []
    display = achieve.get("display") if isinstance(achieve.get("display"), dict) else {}
    achieve_count = _safe_int(achieve.get("count"))

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

    display_slots: list[tuple[int, str]] = []
    for i in range(1, 11):
        slot_medal_id = _norm_id(display.get(str(i)) or display.get(i))
        display_slots.append((i, slot_medal_id))

    medal_cards: list[str] = []
    for slot_index, medal_id in display_slots:
        item = medal_by_id.get(medal_id) if medal_id else None
        achv_data = item.get("achievementData") if isinstance(item, dict) and isinstance(item.get("achievementData"), dict) else {}
        is_plated = bool(item.get("isPlated")) if isinstance(item, dict) else False
        medal_level = _safe_int(item.get("level")) if isinstance(item, dict) else 0
        init_icon = str(achv_data.get("initIcon") or "").strip()
        plated_icon = str(achv_data.get("platedIcon") or "").strip()
        reforge_icon = str(achv_data.get(f"reforge{medal_level}Icon") or "").strip()
        icon_url = reforge_icon or (plated_icon if (is_plated and plated_icon) else init_icon)
        name = str(achv_data.get("name") or f"徽章{slot_index}").strip()

        if icon_url:
            icon_html = (
                f"<img src=\"{escape_text(icon_url)}\" alt=\"{escape_text(name)}\" "
                "loading=\"lazy\" onerror=\"this.remove()\" />"
            )
        else:
            icon_html = f"<span class=\"road-empty\">{slot_index}</span>"

        medal_cards.append(
            f"<article class=\"road-item\" title=\"{escape_text(name)}\">"
            f"<div class=\"road-icon\">{icon_html}</div>"
            "</article>"
        )

    road_flow = "".join(medal_cards)

    sorted_chars = sorted(
        [item for item in chars if isinstance(item, dict)],
        key=lambda item: _safe_int(item.get("level")),
        reverse=True,
    )

    def _progress_html(title: str, current: int, maximum: int) -> str:
        max_value = max(1, maximum)
        ratio = max(0.0, min(1.0, current / max_value))
        color = "#e6bc00" if current <= maximum else "#fb2c36"
        return (
            "<div class=\"meter\">"
            f"<div class=\"meter-title\">{escape_text(title)} {current}/{maximum}</div>"
            "<div class=\"meter-track\">"
            f"<div class=\"meter-fill\" style=\"width:{ratio * 100:.2f}%;background:{color};\"></div>"
            "</div>"
            "</div>"
        )

    char_cards = []
    for char in sorted_chars:
        name = str(char.get("name") or "未知角色")
        char_level = _safe_int(char.get("level"))
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

    avatar_html = (
        f"<img src=\"{escape_text(avatar_url)}\" alt=\"头像\" loading=\"eager\" onerror=\"this.remove()\" />"
        if avatar_url
        else ""
    )

    body = f"""
<div class=\"card\">
  <div class=\"head\">
    <h1>终末地信息卡</h1>
  </div>
  <div class=\"content note-content\">
    <section class=\"profile\">
      <div class=\"avatar\">{avatar_html}</div>
      <div class=\"meta\">
        <h2>{escape_text(role_name)}</h2>
        <p>等级：{level} | UID：{escape_text(role_id)}</p>
        <p>服务器：{escape_text(server_name)}</p>
        <p>主线进度：{escape_text(mission_text)}</p>
      </div>
      <div class=\"meters\">
        {_progress_html('体力', stamina_cur, stamina_max)}
        {_progress_html('活跃度', activation, activation_max)}
        {_progress_html('通行证等级', bp_cur, bp_max)}
      </div>
    </section>

    <div class="account-road-section">
      <section class=\"section\">
        <h2 class=\"section-title\">账号概览</h2>
        <ul class=\"section-body\">
          <li>角色数：{char_num} | 武器数：{weapon_num} | 文档数：{doc_num}</li>
          <li>注册：{escape_text(create_time)}</li>
          <li>最近登录：{escape_text(last_login)}</li>
        </ul>
      </section>

      <section class="section">
          <h2 class="section-title">光荣之路（已获得 {achieve_count} 枚蚀刻章）</h2>
          <div class="road-flow">{road_flow or '<div class="block">暂无展示徽章</div>'}</div>
      </section>
    </div>

    <section class=\"section\">
      <h2 class=\"section-title\">角色列表（共 {len(sorted_chars)} 名）</h2>
      <div class=\"char-grid\">{''.join(char_cards) or '<div class="block">暂无角色数据</div>'}</div>
    </section>
  </div>
</div>
"""

    note_style = (
        ".note-content{display:flex;flex-direction:column;gap:14px;}"
        ".profile{display:grid;grid-template-columns:140px 1fr 380px;gap:16px;align-items:start;"
        "background:#f8fbff;border:1px solid #dce8f5;border-radius:14px;padding:14px;}"
        ".avatar{width:140px;height:140px;border-radius:12px;overflow:hidden;background:#e5e7eb;border:1px solid #cbd5e1;}"
        ".avatar img{width:100%;height:100%;object-fit:cover;display:block;}"
        ".meta h2{margin:2px 0 10px;font-size:30px;}"
        ".meta p{margin:0 0 8px;font-size:18px;color:#1f2937;line-height:1.45;}"
        ".meters{display:flex;flex-direction:column;gap:10px;}"
        ".meter-title{font-size:16px;color:#334155;margin-bottom:4px;}"
        ".meter-track{height:13px;border-radius:999px;background:#e5e7eb;overflow:hidden;}"
        ".meter-fill{height:100%;border-radius:999px;}"
        ".account-road-section{display:grid;grid-template-columns:1fr 1fr;gap:14px;}"
        ".char-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}"
        ".char{min-height:180px;border-radius:10px;padding:10px 10px 12px;color:#fff;border:1px solid rgba(255,255,255,0.2);"
        "display:flex;flex-direction:column;justify-content:flex-end;box-shadow:inset 0 -40px 80px rgba(15,23,42,0.38);}"
        ".char h3{margin:0 0 6px;font-size:21px;line-height:1.25;}"
        ".char p{margin:0 0 4px;font-size:16px;line-height:1.35;}"
        ".road-flow{display:grid;grid-template-columns:repeat(10,52px);width:700px;min-height:252px;margin-top:12px;row-gap:0;column-gap:0;overflow:hidden;}"
        ".road-item{width:126px;height:126px;position:relative;grid-column:span 2;}"
        ".road-item:nth-child(even){margin-top:-65px}"
        ".road-item:nth-child(1){grid-row:1;grid-column:1/span 2;}"
        ".road-item:nth-child(2){grid-row:2;grid-column:2/span 2;}"
        ".road-item:nth-child(3){grid-row:1;grid-column:3/span 2;}"
        ".road-item:nth-child(4){grid-row:2;grid-column:4/span 2;}"
        ".road-item:nth-child(5){grid-row:1;grid-column:5/span 2;}"
        ".road-item:nth-child(6){grid-row:2;grid-column:6/span 2;}"
        ".road-item:nth-child(7){grid-row:1;grid-column:7/span 2;}"
        ".road-item:nth-child(8){grid-row:2;grid-column:8/span 2;}"
        ".road-item:nth-child(9){grid-row:1;grid-column:9/span 2;}"
        ".road-item:nth-child(10){grid-row:2;grid-column:10/span 2;}"
        ".road-icon{width:126px;height:126px;background:#e2e8f0;display:flex;align-items:center;justify-content:center;"
        "overflow:hidden;clip-path:polygon(50% 0%,93.3% 25%,93.3% 75%,50% 100%,6.7% 75%,6.7% 25%);"
        "box-shadow:0 8px 18px rgba(15,23,42,0.16);position:relative;}"
        ".road-icon img{width:100%;height:100%;object-fit:cover;display:block;}"
        ".road-empty{font-size:28px;font-weight:700;color:#475569;}"
    )
    return render_html_to_image(body, width=1280, extra_styles=note_style)
