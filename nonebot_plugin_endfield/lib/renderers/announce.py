import json
from typing import Any, Dict, List, Tuple

from .helpers import escape_text, escape_with_breaks, format_publish_time, normalize_url, safe_json_loads
from .runtime import render_html_to_image


def _extract_announce_blocks(data: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    title = str(data.get("title") or "Endfield 最新公告")

    text_map: Dict[str, str] = {}
    for item in data.get("texts") or []:
        if isinstance(item, dict):
            text_map[str(item.get("id", ""))] = str(item.get("content") or "")

    image_map: Dict[str, str] = {}
    for item in data.get("images") or []:
        if isinstance(item, dict):
            image_map[str(item.get("id", ""))] = normalize_url(str(item.get("url") or ""))

    blocks: List[Dict[str, Any]] = []

    format_obj = safe_json_loads(data.get("format"))
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


def render_announce_data_image(payload: Dict[str, Any]) -> bytes:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        data = {"content": payload}

    title, blocks = _extract_announce_blocks(data)
    publish_text = f"公告发布时间：{format_publish_time(data.get('published_at_ts'))}"

    rendered_blocks: List[str] = []
    for block in blocks:
        if block.get("type") == "text":
            rendered_blocks.append(f"<div class=\"block\">{escape_with_breaks(block.get('text'))}</div>")
            continue
        if block.get("type") == "image":
            url = normalize_url(str(block.get("url") or ""))
            if not url:
                continue
            safe_url = escape_text(url)
            rendered_blocks.append(
                """
<div class=\"img-wrap\">
  <img src=\"%s\" alt=\"公告图片\" loading=\"eager\" onerror=\"this.parentElement.remove()\" />
</div>
"""
                % safe_url
            )

    if not rendered_blocks:
        rendered_blocks.append('<div class="block">暂无可展示内容</div>')

    body = f"""
<div class=\"card\">
  <div class=\"head\">
    <h1>{escape_text(title)}</h1>
    <p>{escape_text(publish_text)}</p>
  </div>
  <div class=\"content\">{''.join(rendered_blocks)}</div>
</div>
"""
    return render_html_to_image(body, width=1080)
