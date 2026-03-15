import asyncio
import base64
from typing import Any, Optional

from nonebot import on_command, logger
from nonebot.adapters import Event
from nonebot.adapters.onebot.v11 import MessageSegment

from ..lib.api import api_request
from ..lib.render import render_user_note_card
from ..lib.utils import get_active_binding, get_api_key


user_card = on_command("终末地信息卡", aliases={"终末地名片", "终末地卡片", "endfield信息卡"})


def _render_note_card(note_data: dict[str, Any], local_role_id: Optional[str], local_server_id: Optional[str]) -> bytes:
    return render_user_note_card(note_data, local_role_id, local_server_id)


@user_card.handle()
async def handle_user_card(event: Event):
    api_key = get_api_key()
    if not api_key:
        await user_card.finish("未配置 endfield_api_key，无法获取信息卡。")

    user_id = str(event.get_user_id())
    active_binding = get_active_binding(user_id)
    if not active_binding:
        await user_card.finish("未找到已绑定账号，请先使用“终末地绑定”。")
    framework_token = active_binding["framework_token"]
    local_role_id = active_binding.get("role_id")
    local_server_id = active_binding.get("server_id")

    note_data = await api_request(
        "GET",
        "/api/endfield/note",
        {
            "X-API-Key": api_key,
            "X-Framework-Token": framework_token,
        },
    )

    if not isinstance(note_data, dict) or note_data.get("code") != 0:
        msg = note_data.get("message") if isinstance(note_data, dict) else None
        await user_card.finish(f"获取信息卡数据失败：{msg or '请稍后重试'}")

    try:
        image_bytes = await asyncio.to_thread(
            _render_note_card,
            note_data,
            local_role_id,
            local_server_id,
        )
    except Exception as e:
        logger.exception(f"render note card failed: {e}")
        await user_card.finish("生成信息卡失败，请稍后重试。")

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    await user_card.finish(MessageSegment.image(f"base64://{image_b64}"))
