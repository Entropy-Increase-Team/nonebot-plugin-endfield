from typing import Any

import httpx
from nonebot import get_plugin_config, logger
from ..config import Config


_HTTP_CLIENT: httpx.AsyncClient | None = None
PLUGIN_CONFIG = get_plugin_config(Config)


def _get_http_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
    return _HTTP_CLIENT


def _build_url(path: str) -> str:
    base = PLUGIN_CONFIG.endfield_api_baseurl
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not base.endswith("/") and not path.startswith("/"):
        return base + "/" + path
    if base.endswith("/") and path.startswith("/"):
        return base[:-1] + path
    return base + path


async def api_request(
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    url = _build_url(path)
    if not (url.startswith("http://") or url.startswith("https://")):
        logger.debug(f"Invalid API URL constructed: {url!r}; aborting request")
        return None

    try:
        client = _get_http_client()
        response = await client.request(method, url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else "?"
        body_preview = ""
        try:
            body_preview = (e.response.text or "")[:300] if e.response is not None else ""
        except Exception:
            body_preview = ""
        log_text = (
            f"HTTP status error: {method.upper()} {url} -> {status}; "
            f"type={type(e).__name__}; body={body_preview}"
        )
        if isinstance(status, int) and status >= 500:
            logger.warning(log_text)
        else:
            logger.debug(log_text)
        return None
    except Exception as e:
        logger.debug(f"HTTP error occurred: {method.upper()} {url}; type={type(e).__name__}; detail={e!r}")
        return None