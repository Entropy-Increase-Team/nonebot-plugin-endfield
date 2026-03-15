from datetime import datetime
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

    <section class=\"section\">
      <h2 class=\"section-title\">账号概览</h2>
      <ul class=\"section-body\">
        <li>角色数：{char_num} | 武器数：{weapon_num} | 文档数：{doc_num} | 经验：{exp}</li>
        <li>注册：{escape_text(create_time)}</li>
        <li>最近登录：{escape_text(last_login)}</li>
      </ul>
    </section>

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
        ".char-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}"
        ".char{min-height:180px;border-radius:10px;padding:10px 10px 12px;color:#fff;border:1px solid rgba(255,255,255,0.2);"
        "display:flex;flex-direction:column;justify-content:flex-end;box-shadow:inset 0 -40px 80px rgba(15,23,42,0.38);}"
        ".char h3{margin:0 0 6px;font-size:21px;line-height:1.25;}"
        ".char p{margin:0 0 4px;font-size:16px;line-height:1.35;}"
    )
    return render_html_to_image(body, width=1280, extra_styles=note_style)
