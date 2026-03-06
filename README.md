# nonebot-plugin-endfield

`nonebot-plugin-endfield` 是一个基于 NoneBot2 的《明日方舟：终末地》插件，支持扫码绑定、账号切换、信息卡、森空岛签到、公告获取与抽卡记录分析。

## 功能特性

- 扫码绑定终末地账号（自动轮询二维码状态）
- 单 QQ 多账号管理与快速切换
- 生成终末地信息卡图片
- 森空岛签到
- 获取最新终末地公告（图片化展示）
- 抽卡记录同步 / 分页查看 / 抽卡分析 / 全服抽卡统计

## 环境要求

- Python `>=3.9`
- NoneBot2 `>=2.0.0,<3.0.0`
- 适配器：`nonebot-adapter-onebot`

## 安装

### 使用 nb-cli

```bash
nb plugin install nonebot-plugin-endfield
```

### 使用包管理器

```bash
pip install nonebot-plugin-endfield
# 或
pdm add nonebot-plugin-endfield
# 或
poetry add nonebot-plugin-endfield
```

在 NoneBot 项目的 `pyproject.toml` 中启用插件：

```toml
[tool.nonebot]
plugins = ["nonebot_plugin_endfield"]
```

## 配置

`https://end.shallow.ink/` 取得API Key后
在 `.env` 文件中配置：

| 配置项 | 必填 | 默认值 | 说明 |
|:--|:--:|:--|:--|
| `endfield_api_key` | 是 | 无 | 终末地 API 服务密钥，未配置时插件核心功能不可用 |
| `endfield_api_baseurl` | 否 | `https://end-api.shallow.ink/` | API 服务基地址 |

示例：

```env
endfield_api_key=your_api_key_here
endfield_api_baseurl=https://end-api.shallow.ink/
```

## 指令说明

### 账号与基础功能

| 指令 | 别名/说明 |
|:--|:--|
| `终末地帮助` | 别名：`终末地`；查看插件帮助与指令列表 |
| `终末地绑定` | 别名：`endfield绑定`、`终末地扫码绑定` |
| `终末地切换账号` | 别名：`endfield切换账号`、`终末地账号切换`；可带序号或角色 ID |
| `终末地信息卡` | 别名：`终末地名片`、`终末地卡片`、`endfield信息卡` |
| `签到` | 执行森空岛签到（需先绑定） |
| `终末地公告` | 获取最新公告并以图片发送 |

### 抽卡相关

| 指令 | 说明 |
|:--|:--|
| `抽卡记录 [页码]` | 查看本地缓存抽卡记录；无记录时会先自动同步 |
| `抽卡分析` | 生成抽卡分析图；无记录时会先同步 |
| `全服抽卡统计 [卡池关键词]` | 查看全服统计，支持关键词筛选卡池 |
| `同步全部抽卡` | 仅 Bot `superusers` 可用，批量触发已绑定账号同步 |

当出现多账号选择时，按提示发送数字序号（`1-999`）即可继续。

## 数据存储

插件会在 `data/nonebot_plugin_endfield/`（或你配置的 `data_dir`）下写入数据：

- `endfield_bindings_v3.db`：账号绑定与当前激活账号
- `gacha/*.json`：抽卡缓存数据
- `gacha_pending_select.json`：抽卡同步中的临时选择状态

## 注意事项

- 首次渲染图片时可能自动下载中文字体文件。
- 若提示 API 请求失败，请优先检查 `endfield_api_key` 与 `endfield_api_baseurl`。
- 本插件依赖外部终末地 API 服务，服务可用性会直接影响功能。

## 许可证

[AGPL v3](./LICENSE)
