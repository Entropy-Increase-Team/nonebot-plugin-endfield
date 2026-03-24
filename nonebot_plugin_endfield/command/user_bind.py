import asyncio
import base64
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from nonebot import get_driver, on_command, logger
from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11 import Message, MessageSegment
from nonebot.exception import ActionFailed
from nonebot.params import CommandArg
from nonebot.rule import to_me
import nonebot_plugin_localstore as store

from ..config import Config
from ..lib.api import api_request
from ..lib.utils import TABLE_NAME, get_db_path, get_api_key, build_headers


user_bind = on_command("终末地绑定", aliases={"endfield绑定", "终末地扫码绑定"})
switch_bind = on_command("终末地切换账号", aliases={"endfield切换账号", "终末地账号切换"})
bind_fmt = on_command("终末地fmt绑定", aliases={"endfield_fmt绑定", "终末地direct绑定"})


def _normalize_qrcode_for_onebot_image(qrcode: Any) -> Optional[str]:
    if qrcode is None:
        return None

    raw = str(qrcode).strip()
    if not raw:
        return None

    if raw.startswith("base64://"):
        return raw

    if raw.startswith("data:image") and "," in raw:
        b64_data = raw.split(",", 1)[1].strip()
        return f"base64://{b64_data}" if b64_data else None

    b64_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    if len(raw) > 200 and all(ch in b64_chars for ch in raw):
        normalized = raw.replace("\n", "").replace("\r", "")
        return f"base64://{normalized}"

    if raw.startswith("http://") or raw.startswith("https://"):
        try:
            response = httpx.get(raw, timeout=10.0)
            response.raise_for_status()
            b64 = base64.b64encode(response.content).decode("utf-8")
            return f"base64://{b64}"
        except Exception as e:
            logger.warning(f"download qrcode failed: {e}")
            return None

    return None


def _format_expire_time(raw_expire: Any) -> Optional[str]:
    if raw_expire is None:
        return None

    if isinstance(raw_expire, str):
        text = raw_expire.strip()
        if not text:
            return None
        if text.isdigit():
            raw_expire = int(text)
        else:
            return text

    if isinstance(raw_expire, (int, float)):
        timestamp = float(raw_expire)
        if timestamp > 1e12:
            timestamp = timestamp / 1000.0
        try:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(raw_expire)

    return str(raw_expire)


def _extract_message_id(send_result: Any) -> Optional[int]:
    if isinstance(send_result, dict):
        msg_id = send_result.get("message_id")
        if isinstance(msg_id, int):
            return msg_id
    if isinstance(send_result, int):
        return send_result
    return None


async def _safe_delete_msg(bot: Bot, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await bot.call_api("delete_msg", message_id=message_id)
    except ActionFailed:
        logger.debug(f"delete_msg failed, message_id={message_id}")
    except Exception as e:
        logger.debug(f"delete_msg exception, message_id={message_id}, error={e}")


def _create_latest_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            framework_token TEXT NOT NULL,
            user_info TEXT NOT NULL,
            binding_info TEXT NOT NULL,
            binding_id TEXT,
            role_id TEXT,
            server_id TEXT,
            is_active INTEGER NOT NULL DEFAULT 0,
            expires_at TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, role_id, server_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_user_active
        ON {TABLE_NAME}(user_id, is_active)
        """
    )


def _migrate_legacy_table(conn: sqlite3.Connection, existing_columns: set[str]) -> None:
    legacy_table = f"{TABLE_NAME}_legacy_{int(datetime.now().timestamp())}"
    conn.execute(f"ALTER TABLE {TABLE_NAME} RENAME TO {legacy_table}")
    _create_latest_schema(conn)

    select_columns = [
        "user_id",
        "framework_token",
        "user_info",
        "binding_info",
        "expires_at",
        "updated_at",
    ]
    for col in select_columns:
        if col not in existing_columns:
            raise RuntimeError(f"legacy table missing required column: {col}")

    rows = conn.execute(
        f"""
        SELECT user_id, framework_token, user_info, binding_info, expires_at, updated_at
        FROM {legacy_table}
        """
    ).fetchall()

    for row in rows:
        user_id, framework_token, user_info, binding_info_raw, expires_at, updated_at = row
        binding_id = None
        role_id = None
        server_id = None
        try:
            binding_info_obj = json.loads(binding_info_raw) if binding_info_raw else {}
            binding_id = binding_info_obj.get("id")
            role_id = binding_info_obj.get("roleId")
            server_id = binding_info_obj.get("serverId")
        except Exception:
            binding_info_obj = {}

        conn.execute(
            f"""
            INSERT INTO {TABLE_NAME}
            (user_id, framework_token, user_info, binding_info, binding_id, role_id, server_id, is_active, expires_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                str(user_id),
                str(framework_token),
                str(user_info),
                str(binding_info_raw),
                str(binding_id) if binding_id is not None else None,
                str(role_id) if role_id is not None else None,
                str(server_id) if server_id is not None else None,
                str(expires_at) if expires_at is not None else None,
                str(updated_at) if updated_at is not None else datetime.now().isoformat(),
            ),
        )

    user_ids = [r[0] for r in conn.execute(f"SELECT DISTINCT user_id FROM {TABLE_NAME}").fetchall()]
    for uid in user_ids:
        conn.execute(f"UPDATE {TABLE_NAME} SET is_active = 0 WHERE user_id = ?", (uid,))
        conn.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET is_active = 1
            WHERE id = (
                SELECT id FROM {TABLE_NAME}
                WHERE user_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
            )
            """,
            (uid,),
        )


def _ensure_table() -> None:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NAME,),
        ).fetchone()

        if not table_exists:
            _create_latest_schema(conn)
            conn.commit()
            return

        columns = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
            if len(row) > 1
        }
        required_columns = {
            "id",
            "user_id",
            "framework_token",
            "user_info",
            "binding_info",
            "binding_id",
            "role_id",
            "server_id",
            "is_active",
            "expires_at",
            "updated_at",
        }

        if required_columns.issubset(columns):
            conn.commit()
            return

        _migrate_legacy_table(conn, columns)
        conn.commit()


def _save_binding(
    user_id: str,
    framework_token: str,
    binding_id: Any,
    role_id: Any,
    server_id: Any,
    nickname: str,
    level: Any,
    expires_at: Optional[str],
) -> None:
    _ensure_table()
    db_path = get_db_path()
    user_info = {"nickname": nickname}
    binding_info = {
        "id": binding_id,
        "roleId": role_id,
        "serverId": server_id,
        "nickname": nickname,
        "level": level,
    }
    now = datetime.now().isoformat()
    binding_id_str = str(binding_id) if binding_id is not None else None
    role_id_str = str(role_id) if role_id is not None else None
    server_id_str = str(server_id) if server_id is not None else None

    with sqlite3.connect(db_path) as conn:
        conn.execute(f"UPDATE {TABLE_NAME} SET is_active = 0 WHERE user_id = ?", (user_id,))
        conn.execute(
            f"""
            INSERT INTO {TABLE_NAME}
                        (user_id, framework_token, user_info, binding_info, binding_id, role_id, server_id, is_active, expires_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(user_id, role_id, server_id) DO UPDATE SET
              framework_token = excluded.framework_token,
              user_info = excluded.user_info,
              binding_info = excluded.binding_info,
                            binding_id = excluded.binding_id,
              role_id = excluded.role_id,
              server_id = excluded.server_id,
              is_active = 1,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at
            """,
            (
                user_id,
                framework_token,
                json.dumps(user_info, ensure_ascii=False),
                json.dumps(binding_info, ensure_ascii=False),
                binding_id_str,
                role_id_str,
                server_id_str,
                expires_at,
                now,
            ),
        )
        conn.commit()


def _list_bindings(user_id: str) -> list[dict[str, Any]]:
    _ensure_table()
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, binding_id, role_id, server_id, binding_info, framework_token, is_active, updated_at
            FROM {TABLE_NAME}
            WHERE user_id = ?
            ORDER BY is_active DESC, updated_at DESC, id DESC
            """,
            (user_id,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        binding_info_raw = row["binding_info"]
        try:
            binding_info = json.loads(binding_info_raw) if binding_info_raw else {}
        except Exception:
            binding_info = {}

        result.append(
            {
                "id": row["id"],
                "binding_id": row["binding_id"] or binding_info.get("id"),
                "role_id": row["role_id"] or binding_info.get("roleId"),
                "server_id": row["server_id"] or binding_info.get("serverId"),
                "nickname": binding_info.get("nickname") or "未知角色",
                "level": binding_info.get("level"),
                "framework_token": row["framework_token"],
                "is_active": int(row["is_active"] or 0) == 1,
                "updated_at": row["updated_at"],
            }
        )
    return result


def _switch_active_binding(user_id: str, binding_row_id: int) -> None:
    _ensure_table()
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"UPDATE {TABLE_NAME} SET is_active = 0 WHERE user_id = ?", (user_id,))
        conn.execute(
            f"UPDATE {TABLE_NAME} SET is_active = 1, updated_at = ? WHERE id = ? AND user_id = ?",
            (datetime.now().isoformat(), binding_row_id, user_id),
        )
        conn.commit()


@user_bind.handle()
async def handle_user_bind(bot: Bot, event: Event):
    api_key = get_api_key()
    if not api_key:
        await user_bind.finish("未配置 endfield_api_key，无法进行绑定。")

    common_headers = {"X-API-KEY": api_key}

    qr_data = await api_request("GET", "/login/endfield/qr", headers=common_headers)
    if not isinstance(qr_data, dict) or qr_data.get("code") != 0:
        await user_bind.finish("获取二维码失败，请稍后重试。")

    qr_payload = qr_data.get("data") if isinstance(qr_data.get("data"), dict) else {}
    framework_token = qr_payload.get("framework_token")
    qrcode = qr_payload.get("qrcode")
    qr_expire = qr_payload.get("expire")
    qr_expire_text = _format_expire_time(qr_expire)
    qrcode_image = _normalize_qrcode_for_onebot_image(qrcode)

    if not framework_token or not qrcode:
        await user_bind.finish("二维码返回数据异常，请稍后重试。")

    qr_msg_id: Optional[int] = None

    try:
        qr_segment = MessageSegment.image(qrcode_image) if qrcode_image else MessageSegment.text(str(qrcode))
        qr_message = (
            MessageSegment.text("请在森空岛扫码确认登录\n")
            + qr_segment
            + MessageSegment.text(f"\n失效时间：{qr_expire_text if qr_expire_text else '请尽快扫码'}")
        )
        qr_send_result = await bot.send(event=event, message=Message(qr_message))
        qr_msg_id = _extract_message_id(qr_send_result)
    except Exception:
        qr_send_result = await bot.send(
            event=event,
            message=f"请扫码登录：{qrcode}\n失效时间：{qr_expire_text if qr_expire_text else '请尽快扫码'}",
        )
        qr_msg_id = _extract_message_id(qr_send_result)

    final_framework_token: Optional[str] = None
    status_response: dict[str, Any] = {}

    for _ in range(150):
        await asyncio.sleep(2)
        status_response = await api_request(
            "GET",
            f"/login/endfield/qr/status?framework_token={framework_token}",
            headers=common_headers,
        )
        if not isinstance(status_response, dict) or status_response.get("code") != 0:
            await _safe_delete_msg(bot, qr_msg_id)
            await user_bind.finish("二维码状态查询失败，请重新绑定。")

        status_payload = status_response.get("data") if isinstance(status_response.get("data"), dict) else {}
        status = status_payload.get("status")

        if status == "expired":
            await _safe_delete_msg(bot, qr_msg_id)
            await user_bind.finish("二维码已过期，请重新发送绑定命令。")

        if status == "done":
            confirm_data = await api_request(
                "POST",
                "/login/endfield/qr/confirm",
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
                data={"framework_token": framework_token},
            )
            if not isinstance(confirm_data, dict) or confirm_data.get("code") != 0:
                await _safe_delete_msg(bot, qr_msg_id)
                await user_bind.finish("确认登录失败，请重新扫码绑定。")

            confirm_payload = confirm_data.get("data") if isinstance(confirm_data.get("data"), dict) else {}
            final_framework_token = confirm_payload.get("framework_token")
            if not final_framework_token:
                await _safe_delete_msg(bot, qr_msg_id)
                await user_bind.finish("确认登录成功但未拿到凭证，请稍后重试。")
            break
    else:
        await _safe_delete_msg(bot, qr_msg_id)
        await user_bind.finish("等待扫码超时，请重新发送绑定命令。")

    binding_record_id = None
    binding_record_data = await api_request(
        "POST",
        "/api/v1/bindings",
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        data={
            "framework_token": final_framework_token,
            "user_identifier": str(event.get_user_id()),
        },
    )
    if isinstance(binding_record_data, dict) and binding_record_data.get("code") == 0:
        payload = binding_record_data.get("data") if isinstance(binding_record_data.get("data"), dict) else {}
        binding_record_id = payload.get("id")
    else:
        logger.warning("create binding record failed, continue with original binding flow")

    binding_data = await api_request(
        "GET",
        "/api/endfield/binding",
        headers={
            "X-Framework-Token": final_framework_token,
            "X-API-KEY": api_key,
        },
    )
    if not isinstance(binding_data, dict) or binding_data.get("code") != 0:
        await _safe_delete_msg(bot, qr_msg_id)
        await user_bind.finish("获取绑定信息失败，请稍后重试。")

    binding_payload = binding_data.get("data") if isinstance(binding_data.get("data"), dict) else {}
    binding_list = binding_payload.get("bindingList") if isinstance(binding_payload.get("bindingList"), list) else []
    if not binding_list:
        await _safe_delete_msg(bot, qr_msg_id)
        await user_bind.finish("未查询到绑定角色信息。")

    first_item = binding_list[0] if isinstance(binding_list[0], dict) else {}
    default_role = first_item.get("defaultRole") if isinstance(first_item.get("defaultRole"), dict) else {}
    if not default_role:
        await _safe_delete_msg(bot, qr_msg_id)
        await user_bind.finish("未查询到默认角色信息。")

    role_id = default_role.get("roleId")
    server_id = default_role.get("serverId")
    channelName = first_item.get("channelName")
    nickname = str(default_role.get("nickname") or "未知角色")
    level = default_role.get("level")

    expires_at = None
    raw_expire = status_response.get("expire") or qr_expire
    if raw_expire is not None:
        expires_at = _format_expire_time(raw_expire)

    _save_binding(
        user_id=str(event.get_user_id()),
        framework_token=final_framework_token,
        binding_id=binding_record_id,
        role_id=role_id,
        server_id=server_id,
        nickname=nickname,
        level=level,
        expires_at=expires_at,
    )

    await _safe_delete_msg(bot, qr_msg_id)

    await user_bind.finish(f"绑定成功\n角色：{nickname}\nUID：{role_id}\n服务器：{channelName}\n等级：{level}")


@switch_bind.handle()
async def handle_switch_bind(event: Event, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    bindings = _list_bindings(user_id)
    if not bindings:
        await switch_bind.finish("你还没有绑定任何终末地账号，请先使用“终末地绑定”。")

    arg_text = args.extract_plain_text().strip()
    if not arg_text:
        lines = ["已绑定账号列表："]
        for idx, item in enumerate(bindings, start=1):
            active_mark = "[当前]" if item["is_active"] else ""
            lines.append(
                f"{idx}. {active_mark} {item['nickname']} (roleId={item['role_id']}, serverId={item['server_id']})"
            )
        lines.append("\n发送“终末地切换账号 序号”或“终末地切换账号 角色ID”进行切换。")
        await switch_bind.finish("\n".join(lines))

    target: Optional[dict[str, Any]] = None
    if arg_text.isdigit():
        index = int(arg_text)
        if 1 <= index <= len(bindings):
            target = bindings[index - 1]
    else:
        for item in bindings:
            if str(item["role_id"]) == arg_text:
                target = item
                break

    if not target:
        await switch_bind.finish("未找到目标账号，请先发送“终末地切换账号”查看列表。")

    _switch_active_binding(user_id=user_id, binding_row_id=int(target["id"]))
    await switch_bind.finish(
        f"已切换当前账号为：{target['nickname']}\nroleId={target['role_id']}\nserverId={target['server_id']}"
    )

@bind_fmt.handle()
async def handle_bind_fmt(event: Event, args: Message = CommandArg()):
    api_key = get_api_key()
    if not api_key:
        await bind_fmt.finish("未配置 endfield_api_key，无法进行绑定。")

    framework_token = args.extract_plain_text().strip()
    if not framework_token:
        await bind_fmt.finish("请提供 framework_token，用法：终末地fmt绑定 <framework_token>")

    common_headers = {"X-API-KEY": api_key}

    binding_record_id = None
    binding_record_data = await api_request(
        "POST",
        "/api/v1/bindings",
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        data={
            "framework_token": framework_token,
            "user_identifier": str(event.get_user_id()),
        },
    )
    if isinstance(binding_record_data, dict) and binding_record_data.get("code") == 0:
        payload = binding_record_data.get("data") if isinstance(binding_record_data.get("data"), dict) else {}
        binding_record_id = payload.get("id")
    else:
        logger.warning("create binding record failed, continue with original binding flow")

    binding_data = await api_request(
        "GET",
        "/api/endfield/binding",
        headers={
            "X-Framework-Token": framework_token,
            "X-API-KEY": api_key,
        },
    )
    if not isinstance(binding_data, dict) or binding_data.get("code") != 0:
        await bind_fmt.finish("获取绑定信息失败，请检查 framework_token 是否正确。")

    binding_payload = binding_data.get("data") if isinstance(binding_data.get("data"), dict) else {}
    binding_list = binding_payload.get("bindingList") if isinstance(binding_payload.get("bindingList"), list) else []
    if not binding_list:
        await bind_fmt.finish("未查询到绑定角色信息。")

    first_item = binding_list[0] if isinstance(binding_list[0], dict) else {}
    default_role = first_item.get("defaultRole") if isinstance(first_item.get("defaultRole"), dict) else {}
    if not default_role:
        await bind_fmt.finish("未查询到默认角色信息。")

    role_id = default_role.get("roleId")
    server_id = default_role.get("serverId")
    channelName = first_item.get("channelName")
    nickname = str(default_role.get("nickname") or "未知角色")
    level = default_role.get("level")

    _save_binding(
        user_id=str(event.get_user_id()),
        framework_token=framework_token,
        binding_id=binding_record_id,
        role_id=role_id,
        server_id=server_id,
        nickname=nickname,
        level=level,
        expires_at=None,
    )

    await bind_fmt.finish(f"绑定成功\n角色：{nickname}\nUID：{role_id}\n服务器：{channelName}\n等级：{level}")