from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from exchange_calendars import get_calendar
except ImportError:  # pragma: no cover - optional in local/unit test env
    get_calendar = None

from backend.services.trade_shared.redis_client import RedisClient
from backend.services.simulation.services.rebalance_job_service import (
    SimulationRebalanceJobService,
)
from backend.services.simulation.services.rebalance_calculator import StrategyConfig

logger = logging.getLogger(__name__)

_SH_TZ = ZoneInfo("Asia/Shanghai")


def bootstrap_lock_key(tenant_id: str, user_id: str, strategy_id: str) -> str:
    """同一策略 24h 内只做一次启动建仓；调度器据此跳过 bootstrap 当天。"""
    return (
        "qm:hosted:simulation:bootstrap:"
        f"{tenant_id}:{user_id}:{strategy_id}"
    )


_DEFAULT_LIVE_TRADE_CONFIG: dict[str, Any] = {
    "rebalance_days": 3,
    "schedule_type": "interval",
    "trade_weekdays": [],
    "enabled_sessions": ["PM"],
    "sell_time": "14:45",
    "buy_time": "14:50",
    "sell_first": True,
    "order_type": "MARKET",
    "max_price_deviation": 0.02,
    "max_orders_per_cycle": 20,
    "trigger_window_seconds": 90,
    # 全局股票池（P3）：非空时信号与调仓只在该池内进行（严格语义）
    "pool_id": None,
}


@dataclass(frozen=True)
class SimulationScheduleDecision:
    should_trigger: bool
    phase: str
    trade_date: str
    reason: str


@dataclass(frozen=True)
class SimulationNextTrigger:
    phase: str
    trade_date: str
    target_at: datetime
    window_start_at: datetime
    window_end_at: datetime
    reason: str


def _to_int(value: Any, default: int) -> int:
    try:
        number = int(float(value))
        return number if math.isfinite(number) else default
    except Exception:
        return default


def _normalize_live_trade_config(value: Any) -> dict[str, Any]:
    cfg = value if isinstance(value, dict) else {}
    merged = {**_DEFAULT_LIVE_TRADE_CONFIG, **cfg}
    merged["schedule_type"] = str(merged.get("schedule_type") or "interval").lower()
    merged["trade_weekdays"] = [
        str(item).upper() for item in (merged.get("trade_weekdays") or [])
    ]
    merged["enabled_sessions"] = [
        str(item).upper() for item in (merged.get("enabled_sessions") or ["PM"])
    ]
    merged["order_type"] = str(merged.get("order_type") or "MARKET").upper()
    merged["rebalance_days"] = max(1, _to_int(merged.get("rebalance_days"), 3))
    # 全局股票池（P3）：空串归一为 None，避免下游把 "" 当成池引用去解析
    merged["pool_id"] = str(merged.get("pool_id") or "").strip() or None
    merged["max_orders_per_cycle"] = max(
        1, _to_int(merged.get("max_orders_per_cycle"), 20)
    )
    merged["trigger_window_seconds"] = max(
        30, _to_int(merged.get("trigger_window_seconds"), 90)
    )
    return merged


def hosted_cycle_ready(phase: str) -> bool:
    """卖/买分窗时只在 BUY/ALL 跑一整轮：SimulationEngine 是先卖后买原子调仓。"""
    return str(phase or "").upper() in {"BUY", "ALL"}


def hosted_trade_enabled(*configs: dict[str, Any] | None) -> bool:
    """All persisted permission scopes must permit automatic trading."""
    for config in configs:
        if not isinstance(config, dict):
            continue
        permission = str(config.get("trading_permission") or "").strip().lower()
        if permission and permission != "trade_enabled":
            return False
        enabled = config.get("auto_trade_enabled")
        if enabled is not None and str(enabled).strip().lower() in {
            "false", "0", "no", "off",
        }:
            return False
    return True


def report_to_hosted_result(report: Any) -> dict[str, Any]:
    error = getattr(report, "error", None)
    ordered = int(getattr(report, "order_count", 0) or 0)
    filled = int(getattr(report, "filled_count", 0) or 0)
    rejected = int(getattr(report, "rejected_count", 0) or 0)

    status = "failed" if error else "succeeded"
    if not error and ordered > 0 and filled == 0:
        # 下了单却一笔都没成交，不能报 succeeded：
        # 一是会推送「模拟策略已启动」的成功通知掩盖空转；
        # 二是下游（real_trading_lifecycle）只在 failed 时释放 bootstrap 锁，
        # 假成功会让 24h 去重配额被一次空转白白占用。
        # ordered == 0（确实无需调仓）仍算 succeeded。
        status = "failed"
        error = f"no_fill: orders={ordered} rejected={rejected}"
        logger.warning(
            "simulation hosted: 本轮无任何成交 orders=%d rejected=%d run=%s",
            ordered,
            rejected,
            getattr(report, "run_id", None),
        )

    return {
        "task_id": getattr(report, "run_id", None),
        "status": status,
        "error": error,
        "signal_count": getattr(report, "signal_count", 0),
        "order_count": ordered,
        "filled_count": filled,
        "rejected_count": rejected,
    }


async def _resolve_hosted_signal_run_id(
    tenant_id: str,
    user_id: str,
) -> tuple[str | None, str | None]:
    """解析本轮托管模拟要消费的推理批次。

    托管（含 bootstrap 首次建仓）必须绑定默认模型的单一推理批次。此前调用方
    从不传 ``signal_run_id``，执行器便退化为「取最新交易日全部信号」，
    同一天多模型并存时候选池会混合（不同模型 fusion_score 量纲不可比）。

    返回 ``(run_id, error)``：不可用时 ``run_id`` 为 None 并给出可读原因。
    校验口径复用 ``get_default_model_hosted_status``（默认模型存在性、
    兜底结果、模型来源、可执行窗口），与手动托管同一套判定。
    """
    try:
        from backend.services.live_trading.services.manual_execution_service import (
            manual_execution_service,
        )

        status = await manual_execution_service.get_default_model_hosted_status(
            tenant_id=tenant_id,
            user_id=user_id,
        )
    except Exception as exc:  # noqa: BLE001
        return None, f"signal_batch_resolve_failed: {exc}"[:300]

    if not status.get("available"):
        reason = str(status.get("reason_code") or "unavailable")
        message = str(status.get("message") or "")
        return None, f"signal_batch_unavailable:{reason} {message}"[:300]

    signal_run_id = str(status.get("latest_run_id") or "").strip()
    if not signal_run_id:
        return None, "signal_batch_unavailable:missing_run_id"
    return signal_run_id, None


async def _sync_hosted_sim_task(
    *,
    task_id: str,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    strategy_name: str,
    live_trade_config: dict[str, Any],
    trigger_context: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    """把托管模拟调仓登记进实盘任务表 + 写日志流（复用实盘「任务日志」入口）。

    模拟链路此前只写 simulation_rebalance_jobs，从不写实盘任务表与日志流，
    导致 StrategyConsole 的「查看任务日志」永远空白。这里以 ``task_type=
    hosted_sim`` 落一行（状态非 queued，避免被手动执行 worker 认领），并把关键
    步骤写进 manual_execution_log_stream，前端无需改动即可查看。

    best-effort：任何异常都吞掉，绝不影响调仓主流程。``result=None`` 表示开始。
    """
    task_id = str(task_id or "").strip()
    if not task_id:
        return
    try:
        from backend.services.live_trading.services.manual_execution_log_stream import (
            manual_execution_log_stream,
        )
        from backend.services.live_trading.services.manual_execution_persistence import (
            manual_execution_persistence,
        )

        if result is None:
            await manual_execution_persistence.create_task(
                task_id=task_id,
                tenant_id=tenant_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name or strategy_id,
                run_id=task_id,
                model_id="",
                prediction_trade_date=datetime.now(_SH_TZ).date(),
                trading_mode="SIMULATION",
                request_payload={
                    "source": "simulation_hosted",
                    "live_trade_config": live_trade_config,
                },
                created_at=datetime.now(timezone.utc),
                task_type="hosted_sim",
                task_source="simulation_hosted",
                trigger_mode="hosted",
                trigger_context=trigger_context or {"run_id": task_id},
                status="running",
                stage="running",
            )
            manual_execution_log_stream.append_log(
                task_id=task_id,
                tenant_id=tenant_id,
                user_id=user_id,
                line=f"[模拟托管] 调仓开始 strategy={strategy_id}",
                stage="running",
                status="running",
                progress=0,
            )
            return

        status = str(result.get("status") or "succeeded")
        error = result.get("error")
        await manual_execution_persistence.update_task(
            task_id=task_id,
            status=status,
            stage=status,
            order_count=int(result.get("order_count") or 0),
            success_count=int(result.get("filled_count") or 0),
            failed_count=int(result.get("rejected_count") or 0),
            progress=100,
            error_stage=None if status == "succeeded" else status,
            error_message=str(error) if error else None,
            result_payload=result,
        )
        tail = (
            f" status={status} 信号={result.get('signal_count')}"
            f" 订单={result.get('order_count')} 成交={result.get('filled_count')}"
            f" 失败={result.get('rejected_count')}"
        )
        if error:
            tail += f" error={error}"
        manual_execution_log_stream.append_log(
            task_id=task_id,
            tenant_id=tenant_id,
            user_id=user_id,
            line=f"[模拟托管] 调仓结束{tail}",
            level="error" if status != "succeeded" else "info",
            stage=status,
            status=status,
            progress=100,
            summary=result,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "simulation hosted: 任务日志同步失败 task=%s error=%s", task_id, exc
        )


async def run_simulation_cycle_for_active(
    *,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    live_trade_config: dict[str, Any] | None = None,
    run_id: str | None = None,
    strategy_name: str | None = None,
    trigger_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """托管模拟盘唯一执行入口：RebalanceCalculator + ashare_matcher。"""
    from backend.services.simulation.engine import simulation_engine

    cfg = _normalize_live_trade_config(live_trade_config)
    if not hosted_trade_enabled(cfg):
        return {
            "task_id": run_id,
            "status": "skipped",
            "error": "automatic_trading_disabled",
            "signal_count": 0,
            "order_count": 0,
            "filled_count": 0,
        }
    params_override = {
        field.name: cfg[field.name]
        for field in fields(StrategyConfig)
        if cfg.get(field.name) is not None
    }
    if cfg.get("pool_id"):
        params_override["pool_id"] = cfg["pool_id"]
    signal_run_id, gate_error = await _resolve_hosted_signal_run_id(
        tenant_id, user_id
    )
    if not signal_run_id:
        # 拿不到可用批次时按严格模式不下单；strict=0 可退回旧行为（仅告警）。
        strict = os.getenv("SIM_HOSTED_STRICT_SIGNAL_BATCH", "1").strip().lower() not in {
            "0",
            "false",
            "no",
        }
        logger.warning(
            "simulation hosted: 未取得可用信号批次 tenant=%s user=%s strategy=%s "
            "reason=%s strict=%s",
            tenant_id,
            user_id,
            strategy_id,
            gate_error,
            strict,
        )
        if strict:
            return {
                "task_id": run_id,
                "status": "skipped",
                "error": gate_error,
                "signal_count": 0,
                "order_count": 0,
                "filled_count": 0,
            }

    task_common = {
        "task_id": str(run_id or ""),
        "tenant_id": tenant_id,
        "user_id": user_id,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name or strategy_id,
        "live_trade_config": cfg,
        "trigger_context": trigger_context,
    }
    await _sync_hosted_sim_task(**task_common)
    try:
        report = await simulation_engine.run_cycle(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            run_id=run_id,
            params_override=params_override or None,
            pool_id=cfg.get("pool_id"),
            signal_run_id=signal_run_id,
            max_orders=int(cfg.get("max_orders_per_cycle") or 0) or None,
        )
    except Exception as exc:  # noqa: BLE001
        await _sync_hosted_sim_task(
            **task_common, result={"status": "failed", "error": str(exc)}
        )
        raise
    result = report_to_hosted_result(report)
    await _sync_hosted_sim_task(**task_common, result=result)
    return result


def _parse_started_at(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_SH_TZ)
        return parsed.astimezone(_SH_TZ).date()
    except Exception:
        return None


def _session_index(day: date) -> int | None:
    if get_calendar is None:
        return None
    try:
        calendar = get_calendar("XSHG")
        session = calendar.date_to_session(pd.Timestamp(day), direction="previous")
        return int(calendar.sessions.get_loc(session))
    except Exception:
        return None


def _is_interval_rebalance_day(
    current_day: date,
    *,
    started_day: date | None,
    rebalance_days: int,
) -> bool:
    if rebalance_days <= 1:
        return True
    if started_day is None:
        return True

    current_idx = _session_index(current_day)
    started_idx = _session_index(started_day)
    if current_idx is not None and started_idx is not None:
        return max(0, current_idx - started_idx) % rebalance_days == 0

    return max(0, (current_day - started_day).days) % rebalance_days == 0


def _is_trading_day(day: date) -> bool:
    if get_calendar is None:
        return day.weekday() < 5
    try:
        calendar = get_calendar("XSHG")
        return calendar.is_session(pd.Timestamp(day))
    except Exception:
        return day.weekday() < 5


def _is_enabled_session(now_hhmm: str, live_trade_config: dict[str, Any]) -> bool:
    enabled = set(live_trade_config.get("enabled_sessions") or [])
    if "AM" in enabled and "09:30" <= now_hhmm <= "11:30":
        return True
    if "PM" in enabled and "13:00" <= now_hhmm <= "15:00":
        return True
    return False


def _matches_trigger_window(
    local_now: datetime,
    target_hhmm: str,
    *,
    window_seconds: int,
) -> bool:
    target_text = str(target_hhmm or "").strip()
    if len(target_text) != 5 or ":" not in target_text:
        return False
    try:
        hour = int(target_text[:2])
        minute = int(target_text[3:5])
    except Exception:
        return False

    target_dt = local_now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0,
    )
    delta_seconds = (local_now - target_dt).total_seconds()
    return 0 <= delta_seconds <= max(1, int(window_seconds))


def _resolve_phase(local_now: datetime, live_trade_config: dict[str, Any]) -> str:
    sell_time = str(live_trade_config.get("sell_time") or "14:45")
    buy_time = str(live_trade_config.get("buy_time") or "14:50")
    window_seconds = max(30, _to_int(live_trade_config.get("trigger_window_seconds"), 90))
    sell_hit = _matches_trigger_window(
        local_now,
        sell_time,
        window_seconds=window_seconds,
    )
    buy_hit = _matches_trigger_window(
        local_now,
        buy_time,
        window_seconds=window_seconds,
    )
    if sell_time == buy_time:
        return "ALL" if sell_hit else "IDLE"

    if bool(live_trade_config.get("sell_first", True)):
        if sell_hit:
            return "SELL"
        if buy_hit:
            return "BUY"
    else:
        if buy_hit:
            return "BUY"
        if sell_hit:
            return "SELL"
    return "IDLE"


def _should_trigger(
    *,
    now: datetime,
    live_trade_config: dict[str, Any],
    started_day: date | None,
    bootstrap_done: bool = False,
) -> SimulationScheduleDecision:
    local_now = now.astimezone(_SH_TZ)
    now_hhmm = local_now.strftime("%H:%M")
    trade_date = local_now.date().isoformat()

    if not _is_trading_day(local_now.date()):
        return SimulationScheduleDecision(False, "IDLE", trade_date, "non_trading_day")

    if not _is_enabled_session(now_hhmm, live_trade_config):
        return SimulationScheduleDecision(False, "IDLE", trade_date, "outside_session")

    # 启动当天的首个 cycle 已按最新价建仓（或正在跑）。14:45 再按交易日
    # 索引算一次，rebalance_days 会命中第 0 天，重复只买 1 手碎单。
    if (
        bootstrap_done
        and started_day is not None
        and started_day == local_now.date()
    ):
        return SimulationScheduleDecision(
            False, "IDLE", trade_date, "bootstrap_day_skip"
        )

    schedule_type = str(live_trade_config.get("schedule_type") or "interval").lower()
    if schedule_type == "weekly":
        weekday = local_now.strftime("%a").upper()[:3]
        allowed = set(live_trade_config.get("trade_weekdays") or [])
        if weekday not in allowed:
            return SimulationScheduleDecision(False, "IDLE", trade_date, "weekday_skip")
    else:
        if not _is_interval_rebalance_day(
            local_now.date(),
            started_day=started_day,
            rebalance_days=max(1, _to_int(live_trade_config.get("rebalance_days"), 3)),
        ):
            return SimulationScheduleDecision(
                False, "IDLE", trade_date, "interval_skip"
            )

    phase = _resolve_phase(local_now, live_trade_config)
    if phase == "IDLE":
        return SimulationScheduleDecision(False, phase, trade_date, "before_window")
    return SimulationScheduleDecision(True, phase, trade_date, "matched")


def _is_time_in_enabled_session(target_hhmm: str, live_trade_config: dict[str, Any]) -> bool:
    return _is_enabled_session(str(target_hhmm or "").strip(), live_trade_config)


def _build_candidate_trigger_datetimes(
    *,
    current_day: date,
    live_trade_config: dict[str, Any],
) -> list[tuple[datetime, str]]:
    sell_time = str(live_trade_config.get("sell_time") or "14:45")
    buy_time = str(live_trade_config.get("buy_time") or "14:50")
    candidates: list[tuple[datetime, str]] = []

    def _append_candidate(target_hhmm: str, phase: str) -> None:
        if not _is_time_in_enabled_session(target_hhmm, live_trade_config):
            return
        try:
            hour = int(target_hhmm[:2])
            minute = int(target_hhmm[3:5])
        except Exception:
            return
        candidates.append(
            (
                datetime(
                    current_day.year,
                    current_day.month,
                    current_day.day,
                    hour,
                    minute,
                    tzinfo=_SH_TZ,
                ),
                phase,
            )
        )

    if sell_time == buy_time:
        _append_candidate(sell_time, "ALL")
    else:
        if bool(live_trade_config.get("sell_first", True)):
            _append_candidate(sell_time, "SELL")
            _append_candidate(buy_time, "BUY")
        else:
            _append_candidate(buy_time, "BUY")
            _append_candidate(sell_time, "SELL")
    candidates.sort(key=lambda item: item[0])
    return candidates


def _next_scheduled_trigger(
    *,
    now: datetime,
    live_trade_config: dict[str, Any],
    started_day: date | None,
    horizon_days: int = 30,
) -> SimulationNextTrigger | None:
    local_now = now.astimezone(_SH_TZ)
    normalized_config = _normalize_live_trade_config(live_trade_config)
    window_seconds = max(
        30, _to_int(normalized_config.get("trigger_window_seconds"), 90)
    )

    for offset in range(max(1, int(horizon_days or 30)) + 1):
        candidate_day = local_now.date() + timedelta(days=offset)
        if not _is_trading_day(candidate_day):
            continue

        for target_at, phase in _build_candidate_trigger_datetimes(
            current_day=candidate_day,
            live_trade_config=normalized_config,
        ):
            if target_at < local_now:
                continue
            probe = _should_trigger(
                now=target_at,
                live_trade_config=normalized_config,
                started_day=started_day,
            )
            if not probe.should_trigger:
                continue
            window_end_at = target_at + timedelta(seconds=window_seconds)
            return SimulationNextTrigger(
                phase=phase,
                trade_date=candidate_day.isoformat(),
                target_at=target_at,
                window_start_at=target_at,
                window_end_at=window_end_at,
                reason="future_window",
            )
    return None


def _task_id(
    *,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    trade_date: str,
    phase: str,
) -> str:
    source = json.dumps(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "strategy_id": strategy_id,
            "trade_date": trade_date,
            "phase": phase,
            "mode": "SIMULATION",
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"hosted_sim_{hashlib.sha1(source.encode('utf-8')).hexdigest()[:16]}"


def _lock_key(
    *,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    trade_date: str,
    phase: str,
) -> str:
    return (
        f"qm:hosted:simulation:{tenant_id}:{user_id}:{strategy_id}:{trade_date}:{phase}"
    )


class SimulationHostedScheduler:
    def __init__(self, redis: RedisClient, interval_seconds: int = 30):
        self.redis = redis
        self.interval_seconds = max(5, int(interval_seconds or 30))
        self._stopped = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(
            self._run(), name="simulation-hosted-scheduler"
        )

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        logger.info(
            "simulation hosted scheduler started, interval=%ss", self.interval_seconds
        )
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "simulation hosted scheduler loop failed: %s", exc, exc_info=True
                )

            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.interval_seconds
                )
            except asyncio.TimeoutError:
                continue

    async def run_once(self, *, now: datetime | None = None) -> int:
        if not self.redis.client:
            return 0

        triggered = 0
        current = now or datetime.now(_SH_TZ)
        await SimulationRebalanceJobService.expire_outdated_jobs(
            now=current.astimezone(_SH_TZ).replace(microsecond=0, tzinfo=None)
        )
        for raw_key in self.redis.client.scan_iter(
            match="trade:active_strategy:*", count=500
        ):
            try:
                did_trigger = await self._process_key(str(raw_key), now=current)
                if did_trigger:
                    triggered += 1
            except Exception as exc:
                logger.warning(
                    "simulation hosted scheduler skipped key=%s error=%s",
                    raw_key,
                    exc,
                    exc_info=True,
                )
        return triggered

    async def _process_key(self, key: str, *, now: datetime) -> bool:
        raw = self.redis.client.get(key)
        if not raw:
            return False
        try:
            active_data = json.loads(raw)
        except Exception:
            return False
        if not isinstance(active_data, dict):
            return False
        if str(active_data.get("mode") or "").upper() != "SIMULATION":
            return False
        if not hosted_trade_enabled(
            active_data,
            active_data.get("execution_config"),
            active_data.get("live_trade_config"),
        ):
            return False

        parts = key.split(":")
        if len(parts) < 4:
            return False
        from backend.shared.simulation_account_keys import resolve_active_identity

        tenant_id, user_id = resolve_active_identity(
            tenant_suffix=parts[-2], user_suffix=parts[-1], payload=active_data
        )
        strategy_id = str(active_data.get("strategy_id") or "").strip()
        if not user_id or not strategy_id:
            return False

        live_trade_config = _normalize_live_trade_config(
            active_data.get("live_trade_config")
        )
        started_day = _parse_started_at(active_data.get("started_at"))
        bootstrap_done = False
        try:
            client = getattr(self.redis, "client", None)
            if client is not None:
                bootstrap_done = bool(
                    client.exists(
                        bootstrap_lock_key(tenant_id, user_id, strategy_id)
                    )
                )
        except Exception:
            bootstrap_done = False
        decision = _should_trigger(
            now=now,
            live_trade_config=live_trade_config,
            started_day=started_day,
            bootstrap_done=bootstrap_done,
        )
        if not decision.should_trigger:
            return False
        if not hosted_cycle_ready(decision.phase):
            logger.info(
                "simulation hosted skip SELL-only window tenant=%s user=%s strategy=%s "
                "(engine runs sell+buy atomically on BUY/ALL)",
                tenant_id,
                user_id,
                strategy_id,
            )
            return False

        lock_key = _lock_key(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            trade_date=decision.trade_date,
            phase=decision.phase,
        )
        task_id = _task_id(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            trade_date=decision.trade_date,
            phase=decision.phase,
        )
        try:
            if not self.redis.client.set(lock_key, task_id, ex=36 * 3600, nx=True):
                # 重复轮询不是作业跳过：不能覆盖成功/运行状态或移动执行窗口。
                return False
        except Exception:
            logger.warning("failed to write simulation hosted lock: %s", lock_key)
            return False

        try:
            await SimulationRebalanceJobService.ensure_job(
                job_id=task_id,
                tenant_id=tenant_id,
                user_id=user_id,
                strategy_id=strategy_id,
                schedule_type=str(live_trade_config.get("schedule_type") or "interval"),
                # TIMESTAMP WITHOUT TIME ZONE：写入 naive 上海墙钟。
                planned_run_at=now.astimezone(_SH_TZ).replace(microsecond=0, tzinfo=None),
                window_seconds=max(
                    30, _to_int(live_trade_config.get("trigger_window_seconds"), 90)
                ),
                idempotency_key=lock_key,
            )
            await SimulationRebalanceJobService.mark_ready(task_id)
            await SimulationRebalanceJobService.mark_started(task_id)
            result = await run_simulation_cycle_for_active(
                tenant_id=tenant_id,
                user_id=user_id,
                strategy_id=strategy_id,
                live_trade_config=live_trade_config,
                run_id=task_id,
                strategy_name=str(active_data.get("strategy_name") or strategy_id),
                trigger_context={
                    "run_id": task_id,
                    "phase": decision.phase,
                    "trade_date": decision.trade_date,
                    "trigger": "hosted_scheduler",
                },
            )
            if result.get("status") == "skipped":
                # 没有可用信号批次：本轮不建仓，作业标记为 skipped 而非失败。
                # 必须释放分布式锁：锁 key 按 (trade_date, phase) 粒度、TTL 36h，
                # 不释放会挡住同一窗口内后续轮询（30s 一轮/窗口 90s），信号迟到
                # 也无法补上；异常路径同样删锁，两处对齐。
                await SimulationRebalanceJobService.mark_skipped(
                    task_id,
                    last_error=str(result.get("error") or "signal batch unavailable")[
                        :300
                    ],
                )
                try:
                    if self.redis.client is not None:
                        self.redis.client.delete(lock_key)
                except Exception:
                    pass
                logger.info(
                    "simulation hosted cycle skipped: tenant=%s user=%s strategy=%s phase=%s task=%s reason=%s",
                    tenant_id,
                    user_id,
                    strategy_id,
                    decision.phase,
                    task_id,
                    result.get("error"),
                )
                return False
            if result.get("status") != "succeeded":
                raise RuntimeError(str(result.get("error") or "simulation cycle failed"))
            logger.info(
                "simulation hosted cycle finished: tenant=%s user=%s strategy=%s phase=%s task=%s status=%s filled=%s",
                tenant_id,
                user_id,
                strategy_id,
                decision.phase,
                task_id,
                result.get("status"),
                result.get("filled_count"),
            )
            await SimulationRebalanceJobService.mark_finished(
                task_id,
                status="succeeded",
            )
            return True
        except Exception as exc:
            try:
                await SimulationRebalanceJobService.mark_finished(
                    task_id,
                    status="failed",
                    last_error=str(exc),
                )
            finally:
                try:
                    self.redis.client.delete(lock_key)
                except Exception:
                    pass
            raise
