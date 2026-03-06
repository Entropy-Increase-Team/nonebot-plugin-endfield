from nonebot import get_driver, require
from nonebot.plugin import PluginMetadata
import logging

from .config import Config

require("nonebot_plugin_localstore")

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

driver = get_driver()
if not getattr(driver.config, "endfield_api_key", None):
    logging.getLogger("nonebot").warning(
        "Endfield Plugin: 未配置 endfield_api_key，插件将无法正常工作，请在配置文件中添加 endfield_api_key"
    )
else:
    from .command import *