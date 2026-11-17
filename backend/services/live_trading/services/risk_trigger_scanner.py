"""Independent risk-trigger scanner (default 60s, A-share session only)."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from backend.services.live_trading.services.risk_trigger_eval import parse_user_id
from backend.services.live_trading.services.risk_trigger_service import (
    apply_candidates,
    collect_needed_symbols,
    ensure_risk_events_table,
    evaluate_user_account,
    fetch_quotes_from_redis,
    filter_stale_quotes,
    is_cn_continuous_auction,
    load_implicit_stop_loss,
    load_trigger_rules,
    today_trade_date,
)
from backend.services.trade_shared.redis_client import redis_client
from backend.services.trade_shared.simulation_manager import SimulationAccountManager
from backend.shared.database_manager_v2 import get_session
from backend.shared.simulation_account_keys import ACCOUNT_KEY_PREFIX, parse_account_key

logger = logging.getLogger(__name__)


def scan_interval_seconds() -> int:
    raw = os.getenv("RISK_SCAN_INTERVAL_SEC", "60")
    try:
        return max(5, int(raw))
    except (TypeError, ValueError):
        return 60


def scan_enabled() -> bool:
    return os.getenv("RISK_SCAN_ENABLED", "true").lower() in {"1", "true", "yes", "on"}


def _fresh_quotes(quotes: dict[str, Any], *, context: str) -> dict[str, Any]:
    """行情延迟超过阈值时本轮不自动卖，等下一次扫描拿到新价再触发。

    远端行情 Redis 抖动时快照可能滞后数分钟；用旧价触发止损会把
    「当时的亏损」当成「现在的亏损」直接成交。宁可晚一轮，不拿过期价交易。
    """
    fresh = filter_stale_quotes(quotes)
    dropped = len(quotes) - len(fresh)
    if dropped:
        logger.warning(
            "risk scanner: 跳过 %d 条过期报价 context=%s", dropped, context
        )
    return fresh


class RiskTriggerScanner:
    def __init__(self, redis: Any | None = None, interval_seconds: int | None = None):
        self.redis = redis or redis_client
        self.interval_seconds = interval_seconds or scan_interval_seconds()
        self._stopped = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._loop(), name="risk-trigger-scanner")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _loop(self) -> None:
        logger.info(
            "RiskTriggerScanner started interval=%ss", self.interval_seconds
        )
        try:
            async with get_session() as db:
                await ensure_risk_events_table(db)
        except Exception as exc:
            logger.warning("risk_events table ensure failed: %s", exc)
        while not self._stopped.is_set():
            try:
                if is_cn_continuous_auction():
                    processed = await self.run_once()
                    if processed:
                        logger.info("RiskTriggerScanner cycle processed=%d", processed)
            except Exception:
                logger.exception("RiskTriggerScanner cycle failed")
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                continue

    async def run_once(self) -> int:
        if not getattr(self.redis, "client", None):
            return 0
        async with get_session() as db:
            rules = await load_trigger_rules(db)
            if not rules:
                implicit_any = False
                # Still scan active strategies for implicit stop_loss.
                from backend.services.live_trading.services.risk_trigger_service import (
                    iter_active_strategy_payloads,
                )

                implicit_any = any(
                    (row.get("payload") or {}).get("execution_config", {}).get("stop_loss")
                    for row in iter_active_strategy_payloads(self.redis)
                )
                if not implicit_any:
                    return 0

            manager = SimulationAccountManager(self.redis)
            processed = 0
            seen: set[tuple[str, int, str]] = set()
            trade_date = today_trade_date()

            client = self.redis.client
            account_keys = list(client.scan_iter(match=f"{ACCOUNT_KEY_PREFIX}*", count=500))
            for raw_key in account_keys:
                parsed = parse_account_key(str(raw_key))
                if parsed is None:
                    continue
                tenant_id, raw_user, market = parsed
                user_id = parse_user_id(raw_user)
                identity = (tenant_id, user_id, market)
                if identity in seen:
                    continue
                seen.add(identity)
                account = await manager.get_account(user_id, tenant_id=tenant_id, market=market)
                if not account:
                    continue
                positions = account.get("positions") or {}
                if not isinstance(positions, dict) or not positions:
                    continue
                implicit = load_implicit_stop_loss(self.redis, tenant_id, raw_user)
                quotes = _fresh_quotes(
                    fetch_quotes_from_redis(
                        self.redis,
                        collect_needed_symbols(
                            positions, rules + ([implicit] if implicit else [])
                        ),
                    ),
                    context=f"simulation:{tenant_id}:{user_id}:{market}",
                )
                candidates = evaluate_user_account(
                    positions=positions,
                    quotes=quotes,
                    rules=rules,
                    user_id=user_id,
                    market=market,
                    account_mode="SIMULATION",
                    implicit_rule=implicit,
                )
                if not candidates:
                    continue
                await apply_candidates(
                    db,
                    self.redis,
                    candidates,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    trade_date=trade_date,
                    dry_run=False,
                )
                processed += len(candidates)

            from backend.services.live_trading.services.risk_trigger_service import (
                iter_active_strategy_payloads,
            )

            for row in iter_active_strategy_payloads(self.redis):
                if str(row.get("mode") or "").upper() != "REAL":
                    continue
                tenant_id = str(row.get("tenant_id") or "default")
                user_id = parse_user_id(row.get("user_id"))
                implicit = load_implicit_stop_loss(self.redis, tenant_id, row.get("user_id"))
                real_rules = [
                    rule
                    for rule in rules
                    if str((rule.parameters or {}).get("trading_mode") or "").upper()
                    in {"REAL", "BOTH"}
                ]
                if not real_rules and implicit is None:
                    continue
                positions = _positions_from_real_payload(row.get("payload") or {})
                if not positions:
                    continue
                quotes = _fresh_quotes(
                    fetch_quotes_from_redis(
                        self.redis,
                        collect_needed_symbols(
                            positions, real_rules + ([implicit] if implicit else [])
                        ),
                    ),
                    context=f"real:{tenant_id}:{user_id}:CN",
                )
                if implicit is not None:
                    implicit.parameters = {
                        **implicit.parameters,
                        "trading_mode": "REAL",
                    }
                candidates = evaluate_user_account(
                    positions=positions,
                    quotes=quotes,
                    rules=real_rules,
                    user_id=user_id,
                    market="CN",
                    account_mode="REAL",
                    implicit_rule=implicit,
                )
                if not candidates:
                    continue
                await apply_candidates(
                    db,
                    self.redis,
                    candidates,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    trade_date=trade_date,
                    dry_run=False,
                )
                processed += len(candidates)
            return processed


def _positions_from_real_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = payload.get("positions") or payload.get("holdings") or []
    out: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            symbol = str(value.get("symbol") or key).strip()
            if symbol:
                out[symbol] = value
        return out
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or item.get("code") or "").strip()
            if symbol:
                out[symbol] = item
    return out


async def run_risk_trigger_scanner() -> None:
    scanner = RiskTriggerScanner()
    await scanner.start()
    try:
        await scanner._stopped.wait()
    finally:
        await scanner.stop()
