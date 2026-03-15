import asyncio

from nonebot import require, logger, get_plugin_config, get_driver
from nonebot.plugin import PluginMetadata

require("nonebot_plugin_localstore")

from .config import Config
from .lib.render import ensure_playwright_browser_installed

__plugin_meta__ = PluginMetadata(
    name="Endfield",
    description="A plugin for Arknights:Endfield",
    usage="获取明日方舟终末地游戏账号信息",
    type="application",
    homepage="https://github.com/TakesBot/nonebot-plugin-endfield",
    config=Config,
    supported_adapters={"~onebot.v11"},
    extra={},
)

plugin_config = get_plugin_config(Config)
driver = get_driver()


@driver.on_startup
async def _prepare_playwright_browser() -> None:
    try:
        ok = await asyncio.to_thread(ensure_playwright_browser_installed)
        if ok:
            logger.info("Endfield Plugin: Playwright Chromium 已就绪")
        else:
            logger.warning("Endfield Plugin: Playwright Chromium 未就绪，渲染时将继续重试自动安装")
    except Exception as e:
        logger.warning(f"Endfield Plugin: Playwright Chromium 预检查失败: {e}")


if not plugin_config.endfield_api_key:
    logger.warning(
        "Endfield Plugin: 未配置 endfield_api_key，插件将无法正常工作，请在配置文件中添加 endfield_api_key"
    )
else:
    from .command import *