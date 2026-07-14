# RecoveryBridge

> NapCat QQ 离线消息自动恢复 + LivingMemory 记忆注入，一个插件全搞定。

当你的 QQ 机器人小号因各种原因掉线重连后，本插件会自动拉取离线期间的群聊历史，并通过 LivingMemory 的 LLM 总结管线将其转化为结构化长期记忆。全程在 AstrBot 内部完成，无需外部脚本或守护进程。

---

## 功能

- **自动检测重连** — 后台轮询 NapCat HTTP API，API 从不可达变为可达时立即触发恢复流程
- **时间戳精准筛选** — 基于消息时间戳而非 message_seq，不受 QQ 登录会话变更影响
- **直接注入 LivingMemory** — 调用 MemoryProcessor 做 LLM 总结，结果存入向量库 + 知识图谱，不经过群聊转发
- **合并转发卡片（可选）** — 恢复后向群聊发送可折叠的消息卡片，供群友查看，不影响记忆注入
- **全 AstrBot WebUI 配置** — 所有参数在插件配置页面直接修改，即时生效

---

## 前置依赖

| 依赖 | 说明 |
|------|------|
| **NapCatQQ** | 需在 WebUI 中开启 HTTP 服务（端口默认 3000） |
| **AstrBot** | ≥ 4.24.2 |
| **LivingMemory** | `astrbot_plugin_livingmemory`，负责记忆总结与存储 |

---

## 安装

将插件文件夹放入 AstrBot 的 `data/plugins/` 目录：

```
C:\Users\<用户名>\.astrbot\data\plugins\astrbot_plugin_recovery_bridge\
├── metadata.yaml
├── main.py
├── _conf_schema.json
└── README.md
```

AstrBot 会自动加载。如果没有自动加载，在 WebUI 插件管理中手动启用。

---

## 配置

在 AstrBot WebUI → **插件 → RecoveryBridge → 配置** 中修改：

### 必需配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `napcat_url` | string | `http://127.0.0.1:3000` | NapCat HTTP API 地址 |
| `groups` | string (JSON 数组) | `[722368954]` | 监控的群号列表，如 `[123456, 789012]` |

### 可选配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `initial_delay` | int | `20` | 启动后等待秒数再开始轮询 |
| `poll_interval` | int | `15` | 正常轮询间隔（秒） |
| `reconnect_poll_interval` | int | `5` | NapCat 断连时的快速轮询间隔（秒） |
| `reconnect_sync_delay` | int | `5` | 检测到重连后等待 QQ 同步的秒数 |
| `send_forward_card` | bool | `true` | 是否向群聊发送合并转发卡片 |
| `max_forward_nodes` | int | `100` | 合并转发每批最多消息条数 |
| `max_total_for_forward` | int | `5000` | 恢复消息超过此数只发统计通知 |

---

## 工作原理

```
┌──────────────────────────────────────────────────┐
│                 RecoveryBridge 插件                │
│                                                    │
│  每 15s 轮询 NapCat HTTP API                        │
│       │                                            │
│       ├─ API 正常 → 拉取各群最新 50 条消息            │
│       │     └─ 按时间戳筛选 last_check 之后的新消息    │
│       │         ├─ 有新消息 → 注入 LivingMemory       │
│       │         └─ 无新消息 → 静默更新检查时间          │
│       │                                            │
│       └─ API 不可达 → 切换到 5s 快速轮询              │
│             └─ API 恢复 → 等待 5s → 拉取新消息        │
│                                                    │
│  LivingMemory 注入管线：                             │
│    原始消息                                           │
│      → Message 对象                                  │
│      → MemoryProcessor.process_conversation()        │
│      → LLM 总结为结构化记忆                            │
│      → classify_atoms_from_metadata()                │
│      → MemoryEngine.add_memory()                     │
│      → 向量库 + 知识图谱                               │
│                                                    │
│  可选群聊通知：                                       │
│    消息 → 构建合并转发节点                              │
│      → send_group_forward_msg (QQ 折叠卡片)           │
└──────────────────────────────────────────────────┘
```

---

## NapCat HTTP API 开启方法

1. 浏览器打开 NapCat WebUI：`http://127.0.0.1:6099/webui`
2. 用 token 登录（token 在 NapCat 配置目录的 `webui.json` 中）
3. 左侧菜单 → **网络配置** → 右上角 **新建** → 选择 **HTTP 服务**
4. 填写：
   - 名称：随意（如 `recovery-api`）
   - 主机：`127.0.0.1`
   - 端口：`3000`
5. 勾选 **启用**，保存

---

## 日志示例

正常运行时：

```
[RecoveryBridge] 20 秒后开始轮询 NapCat (http://127.0.0.1:3000), 群: [722368954]
[RecoveryBridge] 群 722368954 初始化 (seq=931967538, 50 条消息)
[RecoveryBridge] ✓ 所有群无消息遗漏 (耗时 0.3s)
```

检测到断连：

```
[RecoveryBridge] NapCat API 不可达 (ping 超时 5s)，加速轮询
```

恢复成功：

```
[RecoveryBridge] NapCat API 恢复可达，检查 1 个群…
[RecoveryBridge] 群 722368954 重连等待 5s...
[RecoveryBridge] 群 722368954 ⚠ 检测到 3 条新消息
[RecoveryBridge] 群 722368954 恢复 3 条 → 注入: 成功
[MemoryProcessor] 准备调用 LLM，对话类型=群聊, 消息数=3
[RecoveryBridge] 记忆已存储: 主题=[...], 重要性=0.85
```

---

## 与 LivingMemory 的关系

本插件作为 LivingMemory 的 **数据注入管道**，不替代 LivingMemory 的任何功能：

- LivingMemory 负责：对话自动总结、记忆检索、Agent 工具、WebUI 管理
- RecoveryBridge 负责：离线消息恢复 + 批量注入

两者互补，不冲突。

---

## 常见问题

**Q: 为什么有时检测不到离线消息？**

A: 确保 `reconnect_sync_delay` 足够长（默认 5 秒）。NapCat 重连后需要时间从 QQ 服务器同步消息，如果设置太短可能拉取时消息还没同步。

**Q: LLM 总结超时怎么办？**

A: 这是 LivingMemory 的 LLM Provider 配置问题。检查 AstrBot 的提供商配置和网络代理设置。记忆注入失败不影响消息恢复——消息仍会以合并转发卡片形式发到群里。

**Q: 能同时监控多个群吗？**

A: 可以。在 `groups` 配置中填写 `[123456, 789012, ...]` 即可。

**Q: 插件会消耗大量 API Token 吗？**

A: 只在检测到新消息时才会调用 LLM 做总结。日常轮询只拉取消息（不解锁 LLM），消耗极低。
