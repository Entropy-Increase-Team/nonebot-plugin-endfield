import io
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import httpx
from PIL import Image, ImageDraw, ImageFont


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


def _normalize_url(url: str) -> str:
    return (url or "").strip()


def _safe_json_loads(raw: Any) -> Any:
    if not isinstance(raw, str):
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _wrap_text_by_pixel(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> List[str]:
    if not text:
        return [""]

    wrapped: List[str] = []
    for raw_line in text.splitlines() or [""]:
        line = raw_line
        if not line:
            wrapped.append("")
            continue

        current = ""
        for ch in line:
            candidate = current + ch
            if draw.textlength(candidate, font=font) <= max_width:
                current = candidate
                continue
            if current:
                wrapped.append(current)
            current = ch
        wrapped.append(current)
    return wrapped


def _download_image(url: str) -> Image.Image | None:
    normalized = _normalize_url(url)
    if not normalized:
        return None
    try:
        response = httpx.get(normalized, timeout=10.0)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content)).convert("RGB")
        return image
    except Exception:
        return None


def _extract_announce_blocks(data: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    title = str(data.get("title") or "Endfield 最新公告")

    text_map: Dict[str, str] = {}
    for item in data.get("texts") or []:
        if isinstance(item, dict):
            text_map[str(item.get("id", ""))] = str(item.get("content") or "")

    image_map: Dict[str, str] = {}
    for item in data.get("images") or []:
        if isinstance(item, dict):
            image_map[str(item.get("id", ""))] = _normalize_url(str(item.get("url") or ""))

    blocks: List[Dict[str, Any]] = []

    format_obj = _safe_json_loads(data.get("format"))
    format_data = format_obj.get("data") if isinstance(format_obj, dict) else None
    if isinstance(format_data, list):
        for node in format_data:
            if not isinstance(node, dict):
                continue
            node_type = node.get("type")

            if node_type == "image":
                image_id = str(node.get("imageId") or "")
                url = image_map.get(image_id, "")
                if url:
                    blocks.append({"type": "image", "url": url})
                continue

            if node_type == "paragraph":
                contents = node.get("contents")
                if not isinstance(contents, list):
                    continue
                paragraph_text = ""
                for content in contents:
                    if not isinstance(content, dict):
                        continue
                    if content.get("type") == "text":
                        content_id = str(content.get("contentId") or "")
                        paragraph_text += text_map.get(content_id, "")
                if paragraph_text:
                    blocks.append({"type": "text", "text": paragraph_text})

    if not blocks:
        for image_url in image_map.values():
            if image_url:
                blocks.append({"type": "image", "url": image_url})
        for text in text_map.values():
            if text:
                blocks.append({"type": "text", "text": text})

    if not blocks:
        blocks.append({"type": "text", "text": json.dumps(data, ensure_ascii=False, indent=2)})

    return title, blocks


def _format_publish_time(ts: Any) -> str:
    try:
        timestamp = int(ts)
        if timestamp <= 0:
            return "未知"
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "未知"

def render_announce_data_image(payload: Dict[str, Any]) -> bytes:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        data = {"content": payload}

    title, blocks = _extract_announce_blocks(data)

    title_font = _pick_bold_font(36)
    text_font = _pick_font(20)
    meta_font = _pick_font(18)
    publish_text = f"公告发布时间：{_format_publish_time(data.get('published_at_ts'))}"

    padding = 24
    width = 980
    content_width = width - padding * 2
    line_height = 30
    block_gap = 14
    title_height = 56
    meta_bbox = meta_font.getbbox(publish_text)
    meta_text_height = max(18, meta_bbox[3] - meta_bbox[1])
    footer_height = meta_text_height + 10
    note_height = 30

    temp = Image.new("RGB", (width, 10), "white")
    temp_draw = ImageDraw.Draw(temp)

    prepared_blocks: List[Dict[str, Any]] = []
    total_height = padding + title_height

    for block in blocks:
        if block.get("type") == "text":
            text = str(block.get("text") or "")
            lines = _wrap_text_by_pixel(temp_draw, text, text_font, content_width)
            block_height = max(line_height, len(lines) * line_height)
            prepared_blocks.append({"type": "text", "lines": lines, "height": block_height})
            total_height += block_height + block_gap
            continue

        if block.get("type") == "image":
            image = _download_image(str(block.get("url") or ""))
            if image is None:
                fallback = ["[图片加载失败]"]
                block_height = line_height
                prepared_blocks.append({"type": "text", "lines": fallback, "height": block_height})
                total_height += block_height + block_gap
                continue

            scale = content_width / image.width if image.width > content_width else 1.0
            resized_width = int(image.width * scale)
            resized_height = int(image.height * scale)
            resized = image.resize((resized_width, resized_height), Image.Resampling.LANCZOS)
            prepared_blocks.append({"type": "image", "image": resized, "height": resized_height})
            total_height += resized_height + block_gap

    if prepared_blocks:
        total_height -= block_gap

    max_height = 12000
    estimated_height = total_height + padding + footer_height
    overflow = estimated_height > max_height
    height = min(max_height, max(estimated_height, padding * 2 + title_height + footer_height))

    # 使用“最大可用高度”做正文布局，并固定预留截断提示区域，避免后续挤压底部信息
    content_bottom = max_height - padding - meta_text_height - 8 - note_height - 6

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)
    draw.text((padding, padding), title, fill="#111111", font=title_font)

    y = padding + title_height
    did_truncate = False
    for index, block in enumerate(prepared_blocks):
        if y >= content_bottom:
            did_truncate = True
            break

        if block["type"] == "text":
            for line in block["lines"]:
                if y + line_height > content_bottom:
                    did_truncate = True
                    break
                draw.text((padding, y), line, fill="#222222", font=text_font)
                y += line_height
            if did_truncate:
                break
            if index < len(prepared_blocks) - 1:
                y += block_gap
            continue

        if block["type"] == "image":
            block_image: Image.Image = block["image"]
            if y + block_image.height > content_bottom:
                did_truncate = True
                break
            image.paste(block_image, (padding, y))
            y += block_image.height
            if index < len(prepared_blocks) - 1:
                y += block_gap

    note_y = y
    if overflow or did_truncate:
        note = "内容过长，已截断显示。"
        note_y = max(padding + title_height, y)
        draw.text((padding, note_y), note, fill="#aa0000", font=text_font)
        content_end = note_y + note_height
    else:
        content_end = y

    footer_y = content_end + 8
    final_height = min(max_height, footer_y + meta_text_height + padding)

    draw.text((padding, footer_y), publish_text, fill="#666666", font=meta_font)

    if final_height < height:
        image = image.crop((0, 0, width, final_height))

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()
