import os
import subprocess
import sys
import threading

from nonebot import logger
from playwright.sync_api import sync_playwright


_PLAYWRIGHT_BROWSER_READY = False
_PLAYWRIGHT_INIT_LOCK = threading.Lock()
_PLAYWRIGHT_CN_MIRROR_DEFAULT = "https://registry.npmmirror.com/-/binary/playwright"


def _is_missing_browser_error(exc: Exception) -> bool:
    msg = str(exc)
    return "Executable doesn't exist" in msg or "playwright install" in msg


def _verify_chromium_launchable() -> bool:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        browser.close()
    return True


def _looks_like_mainland_china() -> bool:
    marker = " ".join(
        [
            os.getenv("LANG", ""),
            os.getenv("LC_ALL", ""),
            os.getenv("LANGUAGE", ""),
            os.getenv("TZ", ""),
        ]
    ).lower()
    if "zh_cn" in marker:
        return True
    if any(city in marker for city in ("asia/shanghai", "asia/chongqing", "asia/urumqi", "prc")):
        return True
    return False


def _build_playwright_install_env_candidates() -> list[tuple[str, dict[str, str]]]:
    base_env = os.environ.copy()
    candidates: list[tuple[str, dict[str, str]]] = []

    custom_host = (os.getenv("ENDFIELD_PLAYWRIGHT_DOWNLOAD_HOST", "") or "").strip()
    if custom_host:
        custom_env = base_env.copy()
        custom_env["PLAYWRIGHT_DOWNLOAD_HOST"] = custom_host
        candidates.append(("custom", custom_env))

    if _looks_like_mainland_china():
        mirror_host = (os.getenv("ENDFIELD_PLAYWRIGHT_CN_MIRROR", "") or "").strip() or _PLAYWRIGHT_CN_MIRROR_DEFAULT
        mirror_env = base_env.copy()
        mirror_env["PLAYWRIGHT_DOWNLOAD_HOST"] = mirror_host
        candidates.append(("cn-mirror", mirror_env))

    candidates.append(("official", base_env))

    deduped: list[tuple[str, dict[str, str]]] = []
    seen_host: set[str] = set()
    for name, env in candidates:
        host = (env.get("PLAYWRIGHT_DOWNLOAD_HOST") or "__official__").strip()
        if host in seen_host:
            continue
        seen_host.add(host)
        deduped.append((name, env))

    return deduped


def ensure_playwright_browser_installed() -> bool:
    global _PLAYWRIGHT_BROWSER_READY
    if _PLAYWRIGHT_BROWSER_READY:
        return True

    with _PLAYWRIGHT_INIT_LOCK:
        if _PLAYWRIGHT_BROWSER_READY:
            return True

        try:
            _verify_chromium_launchable()
            _PLAYWRIGHT_BROWSER_READY = True
            return True
        except Exception as e:
            if not _is_missing_browser_error(e):
                logger.warning(f"[终末地插件][渲染]Playwright 启动失败: {e}")
                return False

            logger.warning("[终末地插件][渲染]检测到 Chromium 未安装，尝试自动执行 playwright install chromium")
            last_error = ""
            install_ok = False
            for source_name, install_env in _build_playwright_install_env_candidates():
                host = (install_env.get("PLAYWRIGHT_DOWNLOAD_HOST") or "official").strip()
                logger.info(f"[终末地插件][渲染]尝试安装 Chromium，下载源={source_name}, host={host}")
                try:
                    result = subprocess.run(
                        [sys.executable, "-m", "playwright", "install", "chromium"],
                        check=False,
                        capture_output=True,
                        text=True,
                        env=install_env,
                    )
                except Exception as install_error:
                    last_error = str(install_error)
                    logger.warning(f"[终末地插件][渲染]自动安装 Chromium 异常({source_name}): {install_error}")
                    continue

                if result.returncode == 0:
                    install_ok = True
                    break

                stderr_text = (result.stderr or "").strip()
                last_error = stderr_text[:300]
                logger.warning(
                    "[终末地插件][渲染]自动安装 Chromium 失败: "
                    f"source={source_name}, code={result.returncode}, stderr={stderr_text[:300]}"
                )

            if not install_ok:
                logger.warning(f"[终末地插件][渲染]自动安装 Chromium 最终失败: {last_error}")
                return False

            try:
                _verify_chromium_launchable()
                _PLAYWRIGHT_BROWSER_READY = True
                logger.info("[终末地插件][渲染]Chromium 自动安装并校验成功")
                return True
            except Exception as retry_error:
                logger.warning(f"[终末地插件][渲染]Chromium 安装后仍不可用: {retry_error}")
                return False


_BASE_STYLE = """
:root {
  --bg: #eef2f7;
  --card: #ffffff;
  --text: #0f172a;
  --muted: #475569;
  --line: #d9e2ec;
  --chip: #f8fafc;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
  color: var(--text);
  font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", sans-serif;
}
.wrap {
  width: WIDTH_PLACEHOLDERpx;
  margin: 0 auto;
  padding: 24px 20px 26px;
}
.card {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 16px;
  box-shadow: 0 8px 28px rgba(15, 23, 42, 0.06);
  overflow: hidden;
}
.head {
  padding: 18px 22px;
  border-bottom: 1px solid var(--line);
  background: linear-gradient(135deg, #ffffff 0%, #f5f8fc 100%);
}
.head h1 {
  margin: 0;
  font-size: 34px;
  line-height: 1.2;
  letter-spacing: 0.5px;
}
.head p {
  margin: 8px 0 0;
  color: var(--muted);
  font-size: 18px;
}
.content {
  padding: 18px 22px 24px;
}
.block {
  margin: 0 0 14px;
  background: var(--chip);
  border: 1px solid #e4ebf3;
  border-radius: 12px;
  padding: 14px 16px;
  font-size: 20px;
  line-height: 1.7;
  white-space: normal;
  word-break: break-word;
}
.block:last-child { margin-bottom: 0; }
.img-wrap {
  margin: 0 0 14px;
  border: 1px solid #e4ebf3;
  border-radius: 12px;
  background: #fff;
  overflow: hidden;
}
.img-wrap img {
  display: block;
  width: 100%;
  height: auto;
  object-fit: contain;
  max-height: 2200px;
  background: #ffffff;
}
.section {
  margin: 0 0 14px;
  border: 1px solid #e4ebf3;
  border-radius: 12px;
  overflow: hidden;
  background: #ffffff;
}
.section-title {
  margin: 0;
  padding: 11px 14px;
  background: #f3f7fb;
  border-bottom: 1px solid #e4ebf3;
  font-size: 21px;
  font-weight: 700;
}
.section-body {
  margin: 0;
  padding: 10px 14px 12px;
  list-style: none;
  font-size: 19px;
  line-height: 1.65;
}
.section-body li { margin: 0 0 4px; }
.section-body li:last-child { margin-bottom: 0; }
.footer {
  margin-top: 14px;
  color: #64748b;
  font-size: 16px;
  border-top: 1px dashed #d4dde7;
  padding-top: 10px;
}
"""


def render_html_to_image(image_body_html: str, *, width: int = 1200, extra_styles: str = "") -> bytes:
    if not ensure_playwright_browser_installed():
        raise RuntimeError("Playwright Chromium 不可用，请检查网络后重试，或手动执行: playwright install chromium")

    style = _BASE_STYLE.replace("WIDTH_PLACEHOLDER", str(width)) + (extra_styles or "")
    page_html = f"""
<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <style>{style}</style>
</head>
<body>
  <div class=\"wrap\">{image_body_html}</div>
</body>
</html>
"""

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": 800})
        try:
            page.set_content(page_html, wait_until="networkidle", timeout=15000)
            page.wait_for_timeout(350)
        except Exception as e:
            logger.warning(f"[终末地插件][渲染]页面内容加载异常，继续截图: {e}")

        screenshot = page.screenshot(full_page=True, type="png")
        browser.close()
        return screenshot
