"""
NapCat 离线消息自动恢复 + LivingMemory 记忆注入 统一插件

功能：
  1. 后台持续轮询 NapCat HTTP API，追踪群聊 message_seq
  2. 检测到 QQ 掉线重连后，自动拉取遗漏的群聊历史
  3. 调用 LivingMemory 的 MemoryProcessor + MemoryEngine 生成并存储长期记忆
  4. 可选：向群聊发送合并转发卡片供人工查看（不依赖此卡片来注入记忆）

设计要点：
  - initialize() 不做任何 import，避免阻塞其他插件启动
  - LivingMemory 交互全部延迟到首次轮询时
  - 记忆注入走内部管线，不依赖群聊消息回传
"""

import asyncio
import json
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from astrbot.api import logger
from astrbot.api.star import Context, Star, register

DEFAULTS = {
    "napcat_url": "http://127.0.0.1:3000",
    "groups": "[722368954, 397732979]",
    "initial_delay": 20,
    "poll_interval": 15,
    "reconnect_poll_interval": 5,
    "gap_threshold": 2,
    "max_gap_reset": 500000,
    "reconnect_sync_delay": 5,
    "send_forward_card": True,
    "max_forward_nodes": 100,
    "max_total_for_forward": 5000,
}


@register(
    "RecoveryBridge",
    "cherryclaw",
    "NapCat 离线消息自动恢复 → LivingMemory 记忆注入",
    "2.0.0",
)
class RecoveryBridge(Star):
    def __init__(self, context: Context, config: dict[str, Any] = None):
        super().__init__(context)
        self.context = context
        self.cfg = {**DEFAULTS, **(config or {})}
        self._task: asyncio.Task | None = None
        self._lm = None  # 缓存的 LivingMemory 实例
        self._client: httpx.AsyncClient | None = None

    # ── 生命周期 ─────────────────────────────────────────

    async def initialize(self):
        self._task = asyncio.create_task(self._run())

    async def terminate(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()

    # ── 主循环 ───────────────────────────────────────────

    async def _run(self):
        delay = self.cfg["initial_delay"]
        napcat = self.cfg["napcat_url"]
        groups = self._parse_groups()

        logger.info("[RecoveryBridge] %d 秒后开始轮询 NapCat (%s), 群: %s",
                   delay, napcat, groups)
        await asyncio.sleep(delay)

        self._client = httpx.AsyncClient(
            base_url=napcat,
            timeout=httpx.Timeout(8.0),
        )

        state = self._load_state()    # {group_id: {last_seq, ...}}
        for gid in groups:
            gs = state.get(str(gid), {})
            logger.info("[RecoveryBridge] 群 %s 已记录 seq=%s", gid, gs.get("last_seq", 0))
        api_ok = False

        while True:
            try:
                api_ok = await self._tick(state, groups, api_ok)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[RecoveryBridge] 轮询异常: %s", e, exc_info=True)

            sleep = (self.cfg["poll_interval"] if api_ok
                     else self.cfg["reconnect_poll_interval"])
            await asyncio.sleep(sleep)

    # ── 单轮轮询 ─────────────────────────────────────────

    async def _tick(self, state: dict, groups: list[int],
                    api_was_ok: bool) -> bool:
        """一轮轮询：检查连接 → 对每个群检查 seq → 恢复消息 → 注入记忆"""
        t0 = time.time()

        api_ok = await self._napcat_ping()
        if not api_ok:
            if api_was_ok:
                logger.warning("[RecoveryBridge] NapCat API 不可达 (ping 超时 %ss)，加速轮询",
                              self.cfg["reconnect_poll_interval"])
            return False

        was_reconnect = not api_was_ok
        if was_reconnect:
            logger.info("[RecoveryBridge] NapCat API 恢复可达，检查 %d 个群…", len(groups))

        any_gap = False
        for gid in groups:
            had_gap = await self._check_and_recover(state, gid, was_reconnect)
            if had_gap:
                any_gap = True

        elapsed = time.time() - t0
        if not any_gap:
            logger.info("[RecoveryBridge] ✓ 所有群无消息遗漏 (耗时 %.1fs)", elapsed)
        else:
            logger.info("[RecoveryBridge] 本轮检查完成 (耗时 %.1fs)", elapsed)

        return True

    async def _check_and_recover(self, state: dict, group_id: int,
                                  was_reconnect: bool) -> bool:
        """
        基于时间戳检测并恢复遗漏消息。

        不依赖 message_seq 做新旧判断（跨登录会话不稳定），
        但 NapCat 的 get_group_msg_history 仍需 seq 来批量拉取历史。
        因此恢复流程为：
          1. 先拉最近 50 条，按时间戳筛选出新消息；
          2. 重连/明显断层时，再用 seq 批量拉取 last_seq 到最新 seq 之间的消息；
          3. 所有消息最终按时间戳去重筛选，确保不会注入旧消息。
        """
        gk = str(group_id)
        gs = state.get(gk, {})

        # 重连时等待 QQ 同步
        if gs.get("api_was_down") or was_reconnect:
            wait = self.cfg["reconnect_sync_delay"]
            logger.info("[RecoveryBridge] 群 %s 重连等待 %ss...", group_id, wait)
            await asyncio.sleep(wait)

        # 拉取最新 50 条消息
        try:
            recent = await self._napcat_get_recent(group_id, count=50)
        except Exception as e:
            logger.warning("[RecoveryBridge] 群 %s 获取消息失败: %s", group_id, e)
            return False

        if not recent:
            return False

        # 获取最新消息的 seq（仅用于日志和批量拉取）
        latest_seq = max((m.get("message_seq", 0) for m in recent), default=0)
        now_ts = time.time()

        # 首次运行：只记录基准
        last_check_ts = gs.get("last_check_ts")
        if last_check_ts is None and gs.get("last_check"):
            # 兼容旧状态：从 ISO 字符串转换
            try:
                last_check_ts = datetime.fromisoformat(gs["last_check"]).timestamp()
            except ValueError:
                last_check_ts = None

        if last_check_ts is None:
            state[gk] = {
                "last_seq": latest_seq,
                "last_check_ts": now_ts,
                "last_check": datetime.now().isoformat(),
                "last_msg_time": recent[0].get("time", 0) if recent else 0,
            }
            self._save_state(state)
            logger.info("[RecoveryBridge] 群 %s 初始化 (seq=%s, %d 条消息)",
                       group_id, latest_seq, len(recent))
            return False

        # 按时间筛选新消息
        def filter_new(msgs: list[dict]) -> list[dict]:
            return [m for m in msgs if m.get("time", 0) and m["time"] > last_check_ts]

        new_msgs = filter_new(recent)

        # 重连或疑似大量遗漏时，批量拉取中间历史
        last_seq = gs.get("last_seq", 0)
        need_deep_fetch = was_reconnect and last_seq and last_seq < latest_seq
        if new_msgs and len(new_msgs) == len(recent):
            # 最近 N 条全是最新的，可能前面还有遗漏
            need_deep_fetch = True

        if need_deep_fetch:
            logger.info("[RecoveryBridge] 群 %s 尝试批量拉取 %s→%s 之间的历史消息",
                       group_id, last_seq, latest_seq)
            try:
                missed = await self._fetch_missed(group_id, last_seq + 1, latest_seq)
                # 合并、去重、再按时间戳筛选
                merged = {m.get("message_seq"): m for m in recent + missed
                          if m.get("message_seq")}
                new_msgs = filter_new(list(merged.values()))
            except Exception as e:
                logger.warning("[RecoveryBridge] 群 %s 批量拉取历史失败: %s", group_id, e)

        if not new_msgs:
            state[gk]["last_seq"] = latest_seq
            state[gk]["last_check_ts"] = now_ts
            state[gk]["last_check"] = datetime.now().isoformat()
            state[gk]["api_was_down"] = False
            self._save_state(state)
            if was_reconnect:
                logger.info("[RecoveryBridge] 群 %s 重连后无新消息", group_id)
            return False

        # === 检测到新消息 ===
        new_msgs.sort(key=lambda m: m.get("time", 0))
        logger.info("[RecoveryBridge] 群 %s ⚠ 检测到 %d 条新消息 (seq %s→%s)",
                   group_id, len(new_msgs),
                   gs.get("last_seq", "?"), latest_seq)

        # 注入 LivingMemory
        injected = await self._inject_memories(new_msgs, group_id)
        logger.info("[RecoveryBridge] 群 %s 恢复 %d 条 → 注入: %s",
                   group_id, len(new_msgs), "成功" if injected else "失败")

        # 可选：发送合并转发卡片
        if self.cfg["send_forward_card"]:
            await self._send_forward_card(new_msgs, group_id)

        state[gk] = {
            "last_seq": latest_seq,
            "last_check_ts": now_ts,
            "last_check": datetime.now().isoformat(),
            "api_was_down": False,
            "last_recovery": {
                "time": datetime.now().isoformat(),
                "count": len(new_msgs),
                "injected": injected,
            },
        }
        self._save_state(state)
        return True

    # ── NapCat HTTP API ──────────────────────────────────

    async def _napcat_call(self, action: str, params: dict) -> dict:
        """调用 NapCat OneBot API"""
        resp = await self._client.post(f"/{action}", json=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "ok":
            raise RuntimeError(f"API {action} failed: retcode={data.get('retcode')}")
        return data.get("data", {})

    async def _napcat_ping(self) -> bool:
        try:
            await self._napcat_call("get_login_info", {})
            return True
        except Exception:
            return False

    async def _napcat_get_recent(self, group_id: int,
                                   count: int = 50) -> list[dict]:
        """获取群最近 N 条消息（不传 message_seq，NapCat 自动从最新回溯）"""
        params = {"group_id": str(group_id), "count": min(count, 200)}
        try:
            data = await self._napcat_call("get_group_msg_history", params)
        except Exception as e:
            logger.debug("[RecoveryBridge] _napcat_get_recent 失败: %s", e)
            return []

        parsed = []
        for raw in data.get("messages", []):
            try:
                m = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(m, dict) and m.get("time"):
                    parsed.append(m)
            except Exception:
                pass
        return parsed

    async def _napcat_get_history(self, group_id: int, seq: int,
                                   count: int = 200) -> list[dict]:
        params = {
            "group_id": str(group_id),
            "message_seq": seq,
            "count": min(count, 200),
            "reverse_order": False,
        }
        data = await self._napcat_call("get_group_msg_history", params)
        parsed = []
        for raw in data.get("messages", []):
            try:
                m = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(m, dict):
                    parsed.append(m)
            except Exception:
                pass
        return parsed

    async def _napcat_send_group_forward(self, group_id: int,
                                          nodes: list[dict]) -> bool:
        try:
            await self._napcat_call("send_group_forward_msg", {
                "group_id": str(group_id),
                "messages": nodes,
            })
            return True
        except Exception as e:
            logger.warning("[RecoveryBridge] 发送合并转发失败: %s", e)
            return False

    async def _napcat_send_group_msg(self, group_id: int, text: str) -> bool:
        try:
            await self._napcat_call("send_group_msg", {
                "group_id": str(group_id),
                "message": text,
            })
            return True
        except Exception:
            return False

    # ── 消息拉取 ─────────────────────────────────────────

    async def _fetch_missed(self, group_id: int, from_seq: int,
                             to_seq: int) -> list[dict]:
        """分批拉取所有缺失消息"""
        all_msgs = []
        seq = from_seq
        fails = 0

        while seq <= to_seq and len(all_msgs) < 10000:
            try:
                batch = await self._napcat_get_history(group_id, seq, 200)
                fails = 0
            except Exception as e:
                fails += 1
                if fails >= 3:
                    logger.warning("[RecoveryBridge] 群 %s 拉取连续失败，停止", group_id)
                    break
                await asyncio.sleep(3)
                continue

            if not batch:
                seq += 1
                continue

            all_msgs.extend(batch)
            seq = max(m.get("message_seq", 0) for m in batch) + 1

            if len(batch) < 200:
                break

            await asyncio.sleep(0.3)

        # 去重 + 排序
        seen = set()
        uniq = []
        for m in all_msgs:
            s = m.get("message_seq")
            if s and s not in seen:
                seen.add(s)
                uniq.append(m)
        uniq.sort(key=lambda m: m.get("message_seq", 0))
        return uniq

    # ── 合并转发卡片 ─────────────────────────────────────

    async def _send_forward_card(self, messages: list[dict], group_id: int):
        """发送合并转发卡片到群聊（仅用于人工查看）"""
        total = len(messages)
        if total > self.cfg["max_total_for_forward"]:
            await self._napcat_send_group_msg(
                group_id,
                f"[离线消息补录] 共 {total} 条消息已自动归档。"
                f"AI 记忆已更新，可直接询问「之前讨论到哪了？」。"
            )
            return

        # 提示文字
        await self._napcat_send_group_msg(
            group_id,
            f"[离线消息补录] 机器人离线期间遗漏了 {total} 条群聊消息，"
            f"已自动归档。点击下方卡片查看详情。"
        )

        # 分批发送合并转发
        chunk_sz = self.cfg["max_forward_nodes"]
        for i in range(0, total, chunk_sz):
            chunk = messages[i:i + chunk_sz]
            nodes = self._build_nodes(chunk)
            await self._napcat_send_group_forward(group_id, nodes)

    @staticmethod
    def _build_nodes(messages: list[dict], max_n: int = 100) -> list[dict]:
        nodes = []
        for msg in messages[:max_n]:
            sender = msg.get("sender", {})
            uid = sender.get("user_id", 0)
            try:
                uid = int(uid)
            except (ValueError, TypeError):
                uid = 0
            name = sender.get("nickname", "未知")
            ts = msg.get("time", 0)
            tpre = datetime.fromtimestamp(ts).strftime("[%m-%d %H:%M] ") if ts else ""
            text = RecoveryBridge._extract_text(msg) or "[非文本消息]"

            nodes.append({
                "type": "node",
                "data": {
                    "user_id": uid,
                    "nickname": name,
                    "content": [{"type": "text", "data": {"text": tpre + text}}],
                },
            })

        omitted = len(messages) - max_n
        if omitted > 0:
            nodes.append({
                "type": "node",
                "data": {
                    "user_id": 0,
                    "nickname": "系统",
                    "content": [{"type": "text", "data": {
                        "text": f"... 还有 {omitted} 条消息已省略"
                    }}],
                },
            })
        return nodes

    @staticmethod
    def _extract_text(msg: dict) -> str:
        raw = msg.get("raw_message") or msg.get("message", "")
        if isinstance(raw, str):
            return raw.strip()
        if isinstance(raw, list):
            parts = []
            for seg in raw:
                if not isinstance(seg, dict):
                    parts.append(str(seg))
                elif seg.get("type") == "text":
                    parts.append(seg.get("data", {}).get("text", ""))
                elif seg.get("type") in ("image", "at", "face", "reply", "record", "video"):
                    parts.append(f"[{seg['type']}]")
            return " ".join(parts).strip()
        return str(raw)

    # ── LivingMemory 注入 ────────────────────────────────

    async def _inject_memories(self, messages: list[dict],
                                 group_id: int) -> bool:
        """将恢复的消息注入 LivingMemory 记忆系统"""
        lm = self._find_lm()
        if lm is None:
            logger.info("[RecoveryBridge] LivingMemory 未就绪，跳过注入")
            return False

        try:
            # 从 sys.modules 取模型
            Message = None
            get_pid = None
            for mn, mod in sys.modules.items():
                if mn.endswith(".conversation_models") and hasattr(mod, "Message"):
                    Message = mod.Message
                if mn.endswith(".utils") and hasattr(mod, "get_persona_id"):
                    get_pid = mod.get_persona_id
            if Message is None:
                logger.error(
                    "[RecoveryBridge] 找不到 Message 类；"
                    "请确认 LivingMemory 的 conversation_models 模块已加载"
                )
                return False

            sid = f"aiocqhttp:group:{group_id}"

            # 构建 Message 对象
            msg_objs = []
            for i, raw in enumerate(messages):
                snd = raw.get("sender", {})
                msg_objs.append(Message(
                    id=i + 1, session_id=sid, role="user",
                    content=self._extract_text(raw) or "[非文本消息]",
                    sender_id=str(snd.get("user_id", "0")),
                    sender_name=snd.get("nickname", "未知"),
                    group_id=str(group_id), platform="aiocqhttp",
                    timestamp=float(raw.get("time", 0) or time.time()),
                    metadata={"recovered": True},
                ))

            proc = lm.initializer.memory_processor
            eng = lm.initializer.memory_engine

            pid = None
            if get_pid:
                try:
                    pid = get_pid(self.context, sid, None)
                except Exception:
                    pass

            content, meta, importance = await proc.process_conversation(
                messages=msg_objs, is_group_chat=True, persona_id=pid,
            )
            atoms = proc.classify_atoms_from_metadata(
                metadata=meta, parent_importance=importance,
                session_id=sid, persona_id=pid,
            )
            meta["recovery_source"] = {
                "message_count": len(messages),
                "ingested_at": datetime.now().isoformat(),
            }
            await eng.add_memory(
                content=content, session_id=sid, persona_id=pid,
                importance=importance, metadata=meta, atoms=atoms,
            )
            logger.info("[RecoveryBridge] 记忆已存储: 主题=%s, 重要性=%.2f",
                       meta.get("topics", []), importance)
            return True

        except Exception as e:
            logger.error("[RecoveryBridge] 注入失败: %s", e, exc_info=True)
            return False

    # ── LivingMemory 查找 ────────────────────────────────

    def _find_lm(self):
        if self._lm is not None:
            return self._lm

        for mn, mod in sys.modules.items():
            if not mn.endswith("passive_group_capture"):
                continue
            ref = getattr(mod, "_ACTIVE_PLUGIN_REF", None)
            if ref is None:
                continue
            obj = ref()
            if obj is not None and self._lm_ready(obj):
                self._lm = obj
                logger.info("[RecoveryBridge] 连接 LivingMemory (mod=%s)", mn)
                return obj

        logger.debug("[RecoveryBridge] LivingMemory 插件尚未加载或未完成初始化")
        return None

    @staticmethod
    def _lm_ready(obj) -> bool:
        try:
            i = obj.initializer
            return i is not None and i.is_initialized and \
                   i.memory_processor is not None and i.memory_engine is not None
        except Exception:
            return False

    # ── 状态持久化 ───────────────────────────────────────

    def _state_path(self) -> Path:
        return Path(__file__).parent / "recovery_state.json"

    def _load_state(self) -> dict:
        p = self._state_path()
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save_state(self, state: dict):
        p = self._state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state, ensure_ascii=False, indent=2),
                     encoding="utf-8")

    # ── 配置解析 ─────────────────────────────────────────

    def _parse_groups(self) -> list[int]:
        raw = self.cfg.get("groups", [])
        if isinstance(raw, list):
            try:
                return [int(g) for g in raw]
            except (ValueError, TypeError):
                return []
        if isinstance(raw, str):
            try:
                return [int(g) for g in json.loads(raw)]
            except Exception:
                return []
        return []
