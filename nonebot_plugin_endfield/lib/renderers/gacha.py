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
from .runtime import render_html_to_image
from ...config import Config
from ..utils import get_api_key, get_data_dir

PLUGIN_CONFIG = get_plugin_config(Config)

# --- 终末地视觉规范定义 - 亮色战术风格 (Light Tactical HUD) ---
ENDFIELD_THEME_CSS = """
:root {
    /* 背景改为冷灰白，面板改为纯白与浅灰，营造科幻实验室的无菌感 */
    --ef-bg: #f0f2f5;
    --ef-panel: #ffffff;
    --ef-panel-light: #e4e7ec;
    
    /* 强调色进行加深，以保证在浅色背景下的高对比度与辨识度 */
    --ef-yellow: #e09600;
    --ef-blue: #0060d1;
    --ef-red: #d92135;
    --ef-green: #00994d;
    
    /* 边框体系翻转为深灰 */
    --ef-border: #cbd0d8;
    --ef-border-hl: #9aa4b3;
    
    /* 字体颜色翻转为极夜黑与冷灰 */
    --ef-text-main: #181a1d;
    --ef-text-sub: #6c7381;
}

@font-face {
    font-family: 'HarmonyOS Sans SC';
    src: url('file:///FONT_PATH_PLACEHOLDER') format('opentype');
    font-weight: bold;
    font-style: normal;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    background-color: var(--ef-bg);
    /* 背景网点与阵列改为暗色调半透明 */
    background-image: 
        radial-gradient(rgba(0,0,0,0.06) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,0,0,0.03) 1px, transparent 1px),
        linear-gradient(rgba(0,0,0,0.03) 1px, transparent 1px);
    background-size: 20px 20px, 100px 100px, 100px 100px;
    color: var(--ef-text-main);
    font-family: 'Consolas', 'HarmonyOS Sans SC', monospace;
}

.clip-corner {
    clip-path: polygon(12px 0, 100% 0, 100% calc(100% - 12px), calc(100% - 12px) 100%, 0 100%, 0 12px);
}
.clip-corner-sm {
    clip-path: polygon(6px 0, 100% 0, 100% calc(100% - 6px), calc(100% - 6px) 100%, 0 100%, 0 6px);
}

.tactical-scanline {
    background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0,0,0,0.02) 2px,
        rgba(0,0,0,0.02) 4px
    );
}

.corner-bracket { position: relative; }
.corner-bracket::before, .corner-bracket::after {
    content: ''; position: absolute; width: 10px; height: 10px;
    border: 2px solid var(--ef-blue);
    z-index: 10;
}
.corner-bracket::before { top: -2px; left: -2px; border-right: none; border-bottom: none; }
.corner-bracket::after { bottom: -2px; right: -2px; border-left: none; border-top: none; }

.ef-panel {
    background-color: var(--ef-panel);
    border: 1px solid var(--ef-border);
    position: relative;
    /* 亮色模式下添加细微投影增加层级感 */
    box-shadow: 0 4px 12px rgba(0,0,0,0.03); 
}
.ef-header-title {
    font-size: 1.8em; font-weight: bold; letter-spacing: 2px;
    border-left: 4px solid var(--ef-yellow);
    padding-left: 12px; margin-bottom: 6px;
    text-transform: uppercase;
}
"""

# --- 基础工具函数 ---
def _to_int(v: Any) -> int:
    try: return int(v or 0)
    except Exception: return 0

def _to_bool(v: Any) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.lower() in ("1", "true", "yes")
    return bool(v)

def _cache_remote_icon(icon_url: str) -> str:
    def _file_to_data_uri(path: Path) -> str:
        try:
            suffix = path.suffix.lower()
            mime = {
                ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".webp": "image/webp", ".gif": "image/gif", ".svg": "image/svg+xml",
            }.get(suffix, "application/octet-stream")
            payload = base64.b64encode(path.read_bytes()).decode("ascii")
            return f"data:{mime};base64,{payload}"
        except Exception: return ""

    def _escape_http_url_path(url: str) -> str:
        try:
            parts = urlsplit(url)
            if parts.scheme not in ("http", "https"): return url
            escaped_path = quote(parts.path or "", safe="/%:@!$&'()*+,;=-._~")
            return urlunsplit((parts.scheme, parts.netloc, escaped_path, parts.query, parts.fragment))
        except Exception: return url

    raw_url = str(icon_url or "").strip()
    if not raw_url: return ""
    if raw_url.startswith("//"): raw_url = "https:" + raw_url
    if not raw_url.startswith(("http://", "https://", "file:///", "data:")):
        base_url = str(getattr(PLUGIN_CONFIG, "endfield_api_baseurl", "") or "").strip()
        if base_url: raw_url = f"{base_url.rstrip('/')}/{raw_url.lstrip('/')}"
    if raw_url.startswith("file:///"):
        try:
            local_path = Path(raw_url.replace("file:///", "", 1))
            if local_path.exists() and local_path.is_file():
                data_uri = _file_to_data_uri(local_path)
                if data_uri: return data_uri
        except Exception: pass
        return raw_url
    if not (raw_url.startswith("http://") or raw_url.startswith("https://")):
        return raw_url

    raw_url = _escape_http_url_path(raw_url)
    cache_dir = get_data_dir() / "gacha_icon_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(raw_url.split("?", 1)[0]).suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}: suffix = ".png"
    cache_file = cache_dir / f"{hashlib.md5(raw_url.encode('utf-8')).hexdigest()}{suffix}"
    
    if cache_file.exists() and cache_file.stat().st_size > 0:
        data_uri = _file_to_data_uri(cache_file)
        if data_uri: return data_uri
        return cache_file.resolve().as_uri()

    try:
        headers: Dict[str, str] = {}
        api_key = str(get_api_key() or "").strip()
        if api_key: headers["x-api-key"] = api_key
        response = httpx.get(raw_url, timeout=10.0, follow_redirects=True, headers=headers or None)
        response.raise_for_status()
        if suffix == ".png":
            content_type = (response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
            guessed_ext = mimetypes.guess_extension(content_type) or ""
            if guessed_ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg"}:
                cache_file = cache_dir / f"{hashlib.md5(raw_url.encode('utf-8')).hexdigest()}{guessed_ext}"
        cache_file.write_bytes(response.content)
        data_uri = _file_to_data_uri(cache_file)
        if data_uri: return data_uri
        return cache_file.resolve().as_uri()
    except Exception as e:
        logger.debug(f"[终末地插件][抽卡头像]下载失败: {raw_url} | {type(e).__name__}: {e}")
        return _escape_http_url_path(raw_url)


def _batch_records(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """批量事件聚合器：彻底修复免费十连跨页与同屏显示截断"""
    sorted_rows = sorted(rows, key=lambda x: (_to_int(x.get("gacha_ts")), _to_int(x.get("seq_id"))), reverse=True)
    batches = []
    i = 0
    while i < len(sorted_rows):
        row = sorted_rows[i]
        if _to_bool(row.get("is_free")):
            ts = row.get("gacha_ts")
            items = [row]
            j = i + 1
            while j < len(sorted_rows) and _to_bool(sorted_rows[j].get("is_free")) and sorted_rows[j].get("gacha_ts") == ts:
                items.append(sorted_rows[j])
                j += 1
            batches.append({"type": "batch", "items": items, "is_free": True})
            i = j
        else:
            batches.append({"type": "single", "items": [row], "is_free": False})
            i += 1
    return batches

# ==========================================
# 1. 抽卡记录列表图渲染 (Records Image)
# ==========================================
def render_gacha_records_image(cache_data: Dict[str, Any], page: int = 1) -> bytes:
    stats = (cache_data.get("stats_data") or {}).get("stats") or {}
    font_path = (Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansCJKsc-Bold.otf").as_posix()
    
    pool_defs = (("standard", "常驻角色"), ("beginner", "启程寻访"), ("weapon", "武器寻访"), ("limited", "特许寻访"))
    sections_html = []
    
    for key, label in pool_defs:
        pools = cache_data.get("records_by_pool") or {}
        rows = pools.get(key) if isinstance(pools.get(key), list) else []
        
        batches = _batch_records(rows)
        total_entities = len(batches)
        pages = max(1, (total_entities + 9) // 10)
        current = max(1, min(page, pages))
        start = (current - 1) * 10
        picked = batches[start : start + 10]

        items_html = []
        if picked:
            for idx, entity in enumerate(picked, start=1):
                if entity["type"] == "single":
                    r = entity["items"][0]
                    rarity = _to_int(r.get("rarity"))
                    name = escape_text(r.get("char_name") or r.get("item_name") or "未知")
                    color_var = "var(--ef-yellow)" if rarity == 6 else ("var(--ef-blue)" if rarity == 5 else "var(--ef-text-main)")
                    items_html.append(f"""
                        <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; padding: 10px; background: var(--ef-panel-light); border-bottom: 1px solid var(--ef-border);" class="clip-corner-sm">
                            <span style="color: var(--ef-text-sub); width: 40px; font-weight:bold;">#{start + idx:02d}</span>
                            <span style="color: {color_var}; font-weight: bold; width: 60px;">★{rarity}</span>
                            <span style="flex:1;">{name}</span>
                        </div>
                    """)
                else:
                    names = [escape_text(r.get("char_name") or r.get("item_name") or "未知") for r in entity["items"]]
                    names_html = " // ".join([f"<span style='color: var(--ef-yellow)'>{n}</span>" if _to_int(r.get("rarity")) == 6 else n for r, n in zip(entity["items"], names)])
                    items_html.append(f"""
                        <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; padding: 12px; background: rgba(0, 153, 77, 0.08); border-left: 4px solid var(--ef-green);" class="clip-corner-sm tactical-scanline">
                            <span style="color: var(--ef-green); width: 40px; font-weight: bold;">#{start + idx:02d}</span>
                            <span style="color: var(--ef-green); font-weight: bold; min-width: 80px;">[免费供给]</span>
                            <div style="flex:1; display:flex; flex-wrap:wrap; gap: 8px; font-size: 0.9em; line-height:1.4;">
                                {names_html}
                            </div>
                        </div>
                    """)
        else:
            items_html.append("<div style='color: var(--ef-text-sub); padding: 12px;'>[ 数据库空载 ] 暂无寻访记录</div>")

        sections_html.append(f"""
            <div class="ef-panel corner-bracket" style="padding: 20px; margin-bottom: 24px;">
                <div style="display:flex; justify-content:space-between; align-items:center; border-bottom: 1px solid var(--ef-border); padding-bottom: 10px; margin-bottom: 16px;">
                    <div style="font-size: 1.2em; color: var(--ef-text-main); font-weight:bold;">> {label}</div>
                    <div style="color: var(--ef-blue); font-size: 0.9em; font-weight:bold;">第 {current} / {pages} 页</div>
                </div>
                <div>{"".join(items_html)}</div>
            </div>
        """)

    updated_at = cache_data.get("updated_at")
    footer = f"记录同步于：{datetime.fromtimestamp(float(updated_at) / 1000).strftime('%Y-%m-%d %H:%M:%S')}" if updated_at else "记录实时生成"

    html = f"""
    <html>
    <head><style>{ENDFIELD_THEME_CSS.replace('FONT_PATH_PLACEHOLDER', font_path)}</style></head>
    <body style="width: 850px; padding: 40px;">
        <div style="margin-bottom: 30px;">
            <div class="ef-header-title">寻访溯源档案</div>
            <div style="color: var(--ef-text-sub); margin-top: 4px; font-size: 1.1em; padding-left: 16px;">数据中心日志 (节点环境：昼间)</div>
        </div>
        <div style="display:flex; justify-content:space-between; background: var(--ef-panel); border: 1px solid var(--ef-border); padding: 16px 20px; margin-bottom: 30px;" class="clip-corner tactical-scanline">
            <span style="font-weight:bold; font-size:1.1em;">寻访总计: <span style="color:var(--ef-blue)">{stats.get('total_count', 0)}</span></span>
            <span style="font-weight:bold; font-size:1.1em;">★6: <span style="color:var(--ef-yellow)">{stats.get('star6_count', 0)}</span></span>
            <span style="font-weight:bold; font-size:1.1em;">★5: <span style="color:var(--ef-blue)">{stats.get('star5_count', 0)}</span></span>
            <span style="font-weight:bold; font-size:1.1em;">★4: <span style="color:var(--ef-text-sub)">{stats.get('star4_count', 0)}</span></span>
        </div>
        {"".join(sections_html)}
        <div style="text-align: center; color: var(--ef-text-sub); font-size: 0.85em; margin-top: 30px; border-top: 1px solid var(--ef-border); padding-top: 16px;">
            {footer} | 终末地分析引擎
        </div>
    </body>
    </html>
    """
    return render_html_to_image(html, width=850)


# ==========================================
# 2. 抽卡数据分析图渲染 (Analysis Image)
# ==========================================
def render_gacha_analysis_image(stats_data: Dict[str, Any], cache_data: Dict[str, Any]) -> bytes:
    pool_stats = stats_data.get("pool_stats") or {}
    user_info = stats_data.get("user_info") or {}
    up_info = stats_data.get("up_info") or {}
    overall_stats = stats_data.get("stats") or {}
    records_by_pool = cache_data.get("records_by_pool") or {}
    gacha_icon_map = cache_data.get("gacha_icon_map") or {}

    up_char_names = {str(x).strip() for x in (up_info.get("upCharNames") or up_info.get("char_up_names") or []) if str(x).strip()}
    up_weapon_name = str(up_info.get("upWeaponName") or up_info.get("weapon_up_name") or "").strip()
    raw_pool_up_map = up_info.get("poolUpMap") or up_info.get("pool_up_map") or {}
    pool_up_map = {str(k).strip(): str(v).strip() for k, v in raw_pool_up_map.items() if str(k).strip() and str(v).strip()}

    def _pool_stat(name1: str, name2: str) -> Dict[str, Any]:
        return pool_stats.get(name1) or pool_stats.get(name2) or {}

    def _group_pool_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows: grouped.setdefault(str(row.get("pool_name") or "未知"), []).append(row)
        return grouped

    def _text_matches(name: str, target: str) -> bool:
        left, right = str(name or "").strip(), str(target or "").strip()
        return bool(left and right and (left == right or left in right or right in left))

    def _pool_specific_up(pool_name: str) -> str:
        for k, v in pool_up_map.items():
            if _text_matches(pool_name, k): return v
        return ""

    def _is_up_item(name: str, pool_key: str, pool_name: str) -> bool:
        if specific := _pool_specific_up(pool_name): return _text_matches(name, specific)
        if pool_key == "limited": return any(_text_matches(name, up) for up in up_char_names)
        if pool_key == "weapon" and up_weapon_name: return _text_matches(name, up_weapon_name)
        return False

    def _build_timeline_rows(
        rows: List[Dict[str, Any]], *, pool_key: str, pool_name: str, max_pity: int,
        initial_paid_pity: int = 0, initial_up_guaranteed: bool = False, include_tail_pity: bool = True,
    ) -> Dict[str, Any]:
        """状态机解耦：梳理每个池子的时间线，拆分免费与付费"""
        sorted_rows = sorted(rows, key=lambda x: (_to_int(x.get("gacha_ts")), _to_int(x.get("seq_id"))))
        paid_rows = [r for r in sorted_rows if not _to_bool(r.get("is_free"))]
        free_rows = [r for r in sorted_rows if _to_bool(r.get("is_free"))]

        def _segment_timeline(source_rows, init_count=0, init_guaranteed=False, include_tail=True):
            segments = []
            count = init_count
            guaranteed_up = init_guaranteed
            for row in source_rows:
                count += 1
                if _to_int(row.get("rarity")) == 6:
                    name = str(row.get("char_name") or row.get("item_name") or "6星")
                    tag = ""
                    if pool_key in ("standard", "beginner"):
                        tag = ""
                    elif pool_key == "limited":
                        is_up = _is_up_item(name, pool_key, pool_name)
                        if guaranteed_up and is_up: tag, guaranteed_up = "保底", False
                        elif is_up: tag = "UP"
                        else: tag, guaranteed_up = "歪", True
                    elif pool_key == "weapon":
                        tag = "UP" if _is_up_item(name, pool_key, pool_name) else "歪"
                    
                    if _to_bool(row.get("is_free")): tag = "免费供给"
                    
                    segments.append({"count": count, "name": name, "is_pity": False, "tag": tag})
                    count = 0
            if include_tail and count > 0:
                segments.append({"count": count, "name": "已垫", "is_pity": True})
            return segments, count, guaranteed_up

        paid_timeline, paid_tail, paid_guaranteed = _segment_timeline(paid_rows, initial_paid_pity, initial_up_guaranteed, include_tail_pity)
        free_timeline, _, _ = _segment_timeline(free_rows, 0, False, False) # 免费时间线不含垫的状态

        return {
            "paid_timeline": paid_timeline, "paid_total": len(paid_rows), "free_total": len(free_rows),
            "free_timeline": free_timeline, "max_pity": max_pity, 
            "sort_ts": min((_to_int(r.get("gacha_ts")) for r in sorted_rows), default=0),
            "paid_tail_pity": paid_tail, "paid_tail_guaranteed": paid_guaranteed
        }

    def _build_pool_cards(pool_key: str, max_pity: int, shared_paid_pity: bool = False) -> List[Dict[str, Any]]:
        rows = records_by_pool.get(pool_key)
        if not isinstance(rows, list): return []
        grouped_items = list(_group_pool_rows(rows).items())
        grouped_items.sort(key=lambda item: (min(_to_int(r.get("gacha_ts")) for r in item[1]), item[0]))

        cards = []
        carry_pity, carry_guaranteed = 0, False
        for idx, (p_name, p_rows) in enumerate(grouped_items):
            tail = not shared_paid_pity or idx == len(grouped_items) - 1
            data = _build_timeline_rows(
                p_rows, pool_key=pool_key, pool_name=p_name, max_pity=max_pity,
                initial_paid_pity=carry_pity if shared_paid_pity else 0,
                initial_up_guaranteed=carry_guaranteed if shared_paid_pity and pool_key == "limited" else False,
                include_tail_pity=tail
            )
            cards.append({"pool_name": p_name, "pool_key": pool_key, **data})
            if shared_paid_pity:
                carry_pity = data["paid_tail_pity"]
                if pool_key == "limited": carry_guaranteed = data["paid_tail_guaranteed"]
                
        cards.sort(key=lambda x: (-x["sort_ts"], x["pool_name"]))
        return cards

    # UI 视图层数据构建
    limited_cards = _build_pool_cards("limited", 80, shared_paid_pity=True)
    weapon_cards = _build_pool_cards("weapon", 40)
    standard_cards = _build_pool_cards("standard", 80) + _build_pool_cards("beginner", 80)

    def _pick_avatar(name: str) -> str:
        k = str(name or "").strip()
        if not k: return ""
        norm = lambda s: "".join(c.lower() for c in str(s) if c.isalnum() or c == "_")
        kn = norm(k)
        if mapped := gacha_icon_map.get(k): return _cache_remote_icon(mapped)
        for mk, icon in gacha_icon_map.items():
            if k == mk or (kn and norm(mk) and (kn in norm(mk) or norm(mk) in kn)): return _cache_remote_icon(icon)
        return ""

    def _render_star6_rows(card: Dict[str, Any]) -> str:
        paid_timeline = list(reversed(card.get("paid_timeline") or []))
        free_timeline = list(reversed(card.get("free_timeline") or []))
        max_pity = card.get("max_pity", 80)
        parts = []

        def get_color(c: int, is_pity: bool):
            if is_pity: return "var(--ef-blue)"
            ratio = c / max(1, max_pity)
            return "var(--ef-green)" if ratio < 0.5 else ("var(--ef-yellow)" if ratio < 0.8 else "var(--ef-red)")

        # 渲染：已垫已垫 (适配亮色主题的对比度)
        if paid_timeline and paid_timeline[0].get("is_pity"):
            pity = _to_int(paid_timeline[0].get("count"))
            pct = min(100.0, (pity / max_pity) * 100.0)
            parts.append(f"""
                <div style="display:flex; align-items:center; gap:12px; margin-top:8px;">
                    <div style="width:44px;height:44px; background:var(--ef-panel-light); display:flex; align-items:center; justify-content:center; color:var(--ef-blue); border:1px solid var(--ef-border);" class="clip-corner-sm">◆</div>
                    <div style="flex:1; height:28px; background:var(--ef-bg); border:1px solid var(--ef-border);" class="clip-corner-sm">
                        <div style="width:{pct}%; height:100%; background:var(--ef-blue); opacity:0.85;" class="tactical-scanline"></div>
                        <span style="position:relative; top:-24px; left:12px; font-size:0.85em; font-weight:bold; color:var(--ef-text-main);">当前已垫 // {pity}</span>
                    </div>
                </div>
            """)

        # 渲染：付费出的六星
        for row in paid_timeline:
            if row.get("is_pity"): continue
            c, n, t = _to_int(row.get("count")), escape_text(row.get("name")), row.get("tag")
            icon = _pick_avatar(n)
            pct = min(100.0, (c / max_pity) * 100.0)
            bar_color = get_color(c, False)
            tag_col = "var(--ef-yellow)" if t in ("UP", "保底") else ("var(--ef-red)" if t == "歪" else "var(--ef-border-hl)")
            img_html = f"<img src='{icon}' style='width:44px;height:44px;object-fit:cover;border:1px solid var(--ef-border);' class='clip-corner-sm'/>" if icon else "<div style='width:44px;height:44px;background:var(--ef-panel-light);border:1px solid var(--ef-border);' class='clip-corner-sm'></div>"
            
            parts.append(f"""
                <div style="display:flex; align-items:center; gap:12px; margin-top:8px;">
                    {img_html}
                    <div style="flex:1; height:28px; background:var(--ef-bg); border:1px solid var(--ef-border); position:relative;" class="clip-corner-sm">
                        <div style="width:{pct}%; height:100%; background:{bar_color};" class="tactical-scanline"></div>
                        <span style="position:absolute; left:12px; top:5px; font-size:0.85em; font-weight:bold; color:#ffffff; text-shadow: 0 1px 3px rgba(0,0,0,0.4);">{c} 抽</span>
                    </div>
                    <div style="width: 50px; background:{tag_col}; color:#ffffff; text-align:center; font-size:0.8em; font-weight:bold; padding:4px 0; text-shadow:0 1px 2px rgba(0,0,0,0.3);" class="clip-corner-sm">{t or '常规'}</div>
                </div>
            """)

        # 渲染：免费出的所有六星（循环处理，彻底修复双黄截断 BUG）
        free_rows = [r for r in free_timeline if not r.get("is_pity")]
        if card.get("pool_key") == "limited" and free_rows:
            parts.append('<div style="margin-top:12px; padding-top:12px; border-top:1px dashed rgba(0,153,77,0.3);">')
            for f_item in free_rows:
                f_count, f_name = _to_int(f_item["count"]), escape_text(f_item["name"])
                icon = _pick_avatar(f_name)
                img_html = f"<img src='{icon}' style='width:44px;height:44px;object-fit:cover;border:1px solid var(--ef-green);' class='clip-corner-sm'/>" if icon else ""
                parts.append(f"""
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
                        {img_html}
                        <div style="flex:1; height:28px; background:rgba(0,153,77,0.08); border:1px solid var(--ef-green); position:relative;" class="clip-corner-sm tactical-scanline">
                            <span style="position:absolute; left:12px; top:5px; font-size:0.85em; font-weight:bold; color:var(--ef-green);">免费供给 // 第 {f_count} 抽</span>
                        </div>
                    </div>
                """)
            parts.append('</div>')

        return "".join(parts)

    def _render_pool_group(label: str, cards: List[Dict[str, Any]]) -> str:
        if not cards: return ""
        entries = []
        for card in cards:
            pool_key = card.get("pool_key")
            p_total, f_total = _to_int(card.get("paid_total")), _to_int(card.get("free_total"))
            paid_tl = card.get("paid_timeline") or []
            reds = len([x for x in paid_tl if not x.get("is_pity")])
            avg = str(round(p_total/reds)) if reds > 0 else "-"
            
            entries.append(f"""
                <div style="margin-bottom: 20px; background: var(--ef-panel); border: 1px solid var(--ef-border); padding: 16px;" class="clip-corner">
                    <div style="display:flex; justify-content:space-between; border-bottom: 1px solid var(--ef-border); padding-bottom: 8px; margin-bottom: 12px;">
                        <span style="font-weight:bold; color:var(--ef-text-main); font-size:1.1em;">{escape_text(card.get("pool_name"))}</span>
                        <span style="font-size:0.9em; color:var(--ef-text-sub);">总计: <span style="color:var(--ef-text-main)">{p_total}</span> | ★6: <span style="color:var(--ef-blue)">{reds}</span> | 均出: <span style="color:var(--ef-green)">{avg}</span></span>
                    </div>
                    {_render_star6_rows(card)}
                </div>
            """)
        return f"<div class='corner-bracket ef-panel' style='padding: 24px; margin-bottom: 30px; background: var(--ef-bg);'><div class='ef-header-title' style='border-color:var(--ef-blue); margin-bottom: 20px; color:var(--ef-text-main);'>{label}</div>{''.join(entries)}</div>"

    avatar_url = next((str(user_info.get(k) or "").strip() for k in ["avatar", "avatar_url"] if user_info.get(k)), "")
    if not avatar_url and cache_data.get("user_id"): avatar_url = f"https://q1.qlogo.cn/g?b=qq&nk={cache_data.get('user_id')}&s=100"
    font_path = (Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansCJKsc-Bold.otf").as_posix()
    
    ls = _pool_stat("limited_char", "limited")
    ws = _pool_stat("weapon", "weapon")
    ss, bs = _pool_stat("standard_char", "standard"), _pool_stat("beginner_char", "beginner")

    html = f"""
    <html>
    <head><style>{ENDFIELD_THEME_CSS.replace('FONT_PATH_PLACEHOLDER', font_path)}</style></head>
    <body style="width: 1500px; padding: 50px;">
        <!-- 顶栏区域 -->
        <div style="display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid var(--ef-border-hl); padding-bottom: 20px; margin-bottom: 40px;">
            <div style="display: flex; align-items: center; gap: 24px;">
                <img src="{escape_text(avatar_url)}" style="width: 80px; height: 80px; border: 2px solid var(--ef-blue); background: var(--ef-panel);" class="clip-corner" />
                <div>
                    <h1 style="margin:0; font-size: 2.6em; color: var(--ef-text-main); letter-spacing: 1px;">抽卡分析</h1>
                    <div style="color: var(--ef-blue); font-size: 1.2em; margin-top: 6px; font-weight:bold;">NAME：{escape_text(user_info.get('nickname'))} <span style="color:var(--ef-text-sub); margin-left:10px;">| UID：{escape_text(user_info.get('game_uid'))}</span></div>
                </div>
            </div>
            <div style="text-align: right;">
                <div style="color: var(--ef-text-sub); font-size:1.1em; margin-bottom:4px; font-weight:bold;">已追踪寻访总数</div>
                <div style="font-size:2.8em; color:var(--ef-text-main); font-weight:bold; line-height:1;">{overall_stats.get('total_count', 0)}</div>
                <div style="font-size:0.8em; color:var(--ef-blue); margin-top:8px;">最后同步：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
        </div>

        <!-- 聚合数据概览 -->
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 24px; margin-bottom: 40px;">
            <div class="ef-panel clip-corner tactical-scanline" style="padding:24px; border-top: 4px solid var(--ef-yellow);">
                <div style="color:var(--ef-text-sub); font-size:1em; font-weight:bold; margin-bottom:12px;">[ 最高稀有度获取 ]</div>
                <div style="font-size:3em; font-weight:bold; color:var(--ef-yellow);">{overall_stats.get('star6_count', 0)} <span style="font-size:0.4em; color:var(--ef-text-sub);">个</span></div>
            </div>
            <div class="ef-panel clip-corner tactical-scanline" style="padding:24px; border-top: 4px solid var(--ef-blue);">
                <div style="color:var(--ef-text-sub); font-size:1em; font-weight:bold; margin-bottom:12px;">[ 特许寻访总计 ]</div>
                <div style="font-size:3em; font-weight:bold; color:var(--ef-blue);">{_to_int(ls.get('total'))}</div>
            </div>
            <div class="ef-panel clip-corner tactical-scanline" style="padding:24px; border-top: 4px solid var(--ef-green);">
                <div style="color:var(--ef-text-sub); font-size:1em; font-weight:bold; margin-bottom:12px;">[ 武器寻访总计 ]</div>
                <div style="font-size:3em; font-weight:bold; color:var(--ef-green);">{_to_int(ws.get('total'))}</div>
            </div>
            <div class="ef-panel clip-corner tactical-scanline" style="padding:24px; border-top: 4px solid var(--ef-text-main);">
                <div style="color:var(--ef-text-sub); font-size:1em; font-weight:bold; margin-bottom:12px;">[ 常驻寻访总计 ]</div>
                <div style="font-size:3em; font-weight:bold; color:var(--ef-text-main);">{_to_int(ss.get('total')) + _to_int(bs.get('total'))}</div>
            </div>
        </div>

        <!-- 卡池专栏 -->
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 30px;">
            <div>{_render_pool_group("[ 特许寻访 ]", limited_cards)}</div>
            <div>{_render_pool_group("[ 武器寻访 ]", weapon_cards)}</div>
            <div>{_render_pool_group("[ 常驻与启程 ]", standard_cards)}</div>
        </div>
    </body>
    </html>
    """
    return render_html_to_image(html, width=1500)


# ==========================================
# 3. 全服统计图渲染 (Global Stats)
# ==========================================
def render_gacha_global_stats_image(stats_data: Dict[str, Any], keyword: str = "") -> bytes:
    s = stats_data.get("stats") or stats_data
    by_channel = s.get("by_channel") or {}
    by_type = s.get("by_type") or {}
    font_path = (Path(__file__).resolve().parents[2] / "assets" / "fonts" / "NotoSansCJKsc-Bold.otf").as_posix()
    
    def _fmt(v: Any, ndigits: int = 2) -> str:
        try: return f"{float(v):.{ndigits}f}"
        except Exception: return "-"

    current_pool = s.get("current_pool") or {}
    up_name = current_pool.get("up_char_name") or "-"
    up_weapon = current_pool.get("up_weapon_name") or "-"

    panels = []
    for key, label in (("beginner", "启程寻访"), ("standard", "常驻寻访"), ("weapon", "武器寻访"), ("limited", "特许寻访")):
        item = by_type.get(key) or {}
        total, star6 = int(item.get("total") or 0), int(item.get("star6") or 0)
        rate = (star6 / total * 100) if total > 0 else 0
        panels.append(f"""
            <div class="ef-panel corner-bracket" style="padding: 20px;">
                <div style="color:var(--ef-text-main); font-weight:bold; font-size:1.1em; margin-bottom:12px; border-bottom:1px solid var(--ef-border); padding-bottom:6px;">{label}</div>
                <div style="font-size: 1em; line-height: 1.8;">
                    <div>寻访数: <span style="color:var(--ef-text-main); font-weight:bold; float:right;">{total}</span></div>
                    <div>出红率: <span style="color:var(--ef-yellow); font-weight:bold; float:right;">{rate:.2f}%</span></div>
                    <div>平均期望: <span style="color:var(--ef-green); font-weight:bold; float:right;">{_fmt(item.get('avg_pity'), 1)} 抽</span></div>
                </div>
            </div>
        """)

    ch_html = []
    for ch_key, ch_label in (("official", "官方网络节点"), ("bilibili", "BiliBili 协作节点")):
        ch_data = by_channel.get(ch_key)
        if isinstance(ch_data, dict):
            ch_html.append(f"""
                <div style="display:flex; justify-content:space-between; align-items:center; padding: 16px; background:var(--ef-panel-light); border:1px solid var(--ef-border); margin-bottom: 12px;" class="clip-corner-sm tactical-scanline">
                    <span style="color:var(--ef-blue); font-weight:bold; font-size:1.1em;">[{ch_label}]</span>
                    <span style="color:var(--ef-text-sub);">覆盖用户 <span style="color:var(--ef-text-main); font-weight:bold;">{ch_data.get('total_users', 0)}</span></span>
                    <span style="color:var(--ef-text-sub);">总计寻访 <span style="color:var(--ef-text-main); font-weight:bold;">{ch_data.get('total_pulls', 0)}</span></span>
                    <span style="color:var(--ef-text-sub);">均出红 <span style="color:var(--ef-yellow); font-size:1.1em; font-weight:bold;">{_fmt(ch_data.get('avg_pity'))} 抽</span></span>
                </div>
            """)

    html = f"""
    <html>
    <head><style>{ENDFIELD_THEME_CSS.replace('FONT_PATH_PLACEHOLDER', font_path)}</style></head>
    <body style="width: 1100px; padding: 50px;">
        <div style="border-left: 6px solid var(--ef-yellow); padding-left: 20px; margin-bottom: 40px; background: rgba(0,0,0,0.03); padding-top: 16px; padding-bottom: 16px;">
            <h1 style="margin: 0; font-size: 2.8em; letter-spacing: 2px;">全域数据节点</h1>
            <div style="color: var(--ef-text-sub); font-size: 1.2em; margin-top: 8px; font-weight:bold;">全网络寻访节点监控汇总 {f"// {keyword}" if keyword else ""}</div>
        </div>

        <div class="ef-panel clip-corner tactical-scanline" style="padding: 30px; margin-bottom: 40px; border-top: 3px solid var(--ef-blue);">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <div style="font-size:1em; color:var(--ef-text-sub); font-weight:bold; margin-bottom: 8px;">[ 核心指标监控 ]</div>
                    <div style="font-size:2.2em; font-weight:bold; color:var(--ef-text-main);">全网寻访总数: {s.get('total_pulls', 0)}</div>
                </div>
                <div style="text-align: right; line-height: 1.8; font-size: 1.1em;">
                    <div>监控操作员总数: <span style="color:var(--ef-text-main); font-weight:bold;">{s.get('total_users', 0)}</span></div>
                    <div>全网平均出红期望: <span style="color:var(--ef-green); font-size:1.3em; font-weight:bold;">{_fmt(s.get('avg_pity'))} 抽</span></div>
                </div>
            </div>
            <div style="margin-top: 20px; padding-top: 20px; border-top: 1px dashed var(--ef-border-hl); color: var(--ef-text-sub); font-size:1.1em;">
                当前指令目标 // 限定概率提升: <span style="color:var(--ef-yellow); font-weight:bold; margin:0 10px;">干员: {up_name}</span> | <span style="color:var(--ef-yellow); font-weight:bold; margin:0 10px;">武器: {up_weapon}</span>
            </div>
        </div>

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 40px;">
            {"".join(panels)}
        </div>

        <div class="ef-panel corner-bracket" style="padding: 24px;">
            <div class="ef-header-title" style="border-color:var(--ef-green); margin-bottom:20px; color:var(--ef-text-main);">[ 各频段节点分布 ]</div>
            {"".join(ch_html)}
        </div>
    </body>
    </html>
    """
    return render_html_to_image(html, width=1100)