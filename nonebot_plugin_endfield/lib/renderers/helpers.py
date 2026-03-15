import html
import json
from datetime import datetime
from typing import Any


def normalize_url(url: str) -> str:
    return (url or "").strip()


def safe_json_loads(raw: Any) -> Any:
    if not isinstance(raw, str):
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def escape_text(text: Any) -> str:
    return html.escape(str(text or ""), quote=True)


def escape_with_breaks(text: Any) -> str:
    return escape_text(text).replace("\n", "<br>")


def format_publish_time(ts: Any) -> str:
    try:
        timestamp = int(ts)
        if timestamp <= 0:
            return "未知"
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "未知"
