import base64
import logging

from nonebot import on_command, get_driver
from nonebot.adapters.onebot.v11 import MessageSegment
from nonebot.rule import to_me
from ..config import Config
from ..lib.api import api_request
from ..lib.render import render_announce_data_image


get_last_announce = on_command("终末地公告", rule=to_me())


@get_last_announce.handle()
async def handle_get_last_announce():
    cfg = Config()
    driver = get_driver()
    api_key = getattr(driver.config, "endfield_api_key", None) or cfg.endfield_api_key
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key
    logging.getLogger("nonebot").info(f"Using headers for API request: {headers}")
    result = api_request("GET", "/api/announcements/latest", headers=headers or None)
    if result is None:
        await get_last_announce.finish("获取公告失败，请检查 endfield_api_key 和 endfield_api_baseurl 配置并查看日志。")

    data = result.get("data") if isinstance(result, dict) else None
    item_id = data.get("item_id") if isinstance(data, dict) else None
    origin_link = f"https://www.skland.com/article?id={item_id}" if item_id else None

    image_bytes = render_announce_data_image(result)
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    if origin_link:
        await get_last_announce.finish(
            MessageSegment.image(f"base64://{image_b64}")
            + MessageSegment.text(f"原文链接：{origin_link}")
        )
    await get_last_announce.finish(MessageSegment.image(f"base64://{image_b64}"))