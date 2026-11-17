"""Intraday risk locks: after a trigger fires, strategy must not buy back."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from collections.abc import Iterable
from typing import Any
from zoneinfo import ZoneInfo

from backend.shared.stock_utils import StockCodeUtil

_SH_TZ = ZoneInfo("Asia/Shanghai")
ACCOUNT_LOCK_PREFIX = "risk:lock:account:"
SYMBOL_LOCK_PREFIX = "risk:lock:symbol:"
COOLDOWN_PREFIX = "risk:cooldown:symbol:"
FIRED_PREFIX = "risk:fired:"
DEFAULT_COOLDOWN_SECONDS = 7 * 86400


def normalize_lock_user(user_id: object) -> str:
    raw = str(user_id or "").strip()
    if raw.isdigit():
        return str(int(raw))
    return raw or "0"


def normalize_lock_symbol(symbol: str) -> str:
    suffix = StockCodeUtil.to_suffix(str(symbol or "").strip())
    return suffix or str(symbol or "").strip().upper()


def _trade_date_str(trade_date: date | str) -> str:
    if isinstance(trade_date, date):
        return trade_date.isoformat()
    return str(trade_date)


def account_lock_key(tenant_id: str, user_id: object, trade_date: date | str) -> str:
    return (
        f"{ACCOUNT_LOCK_PREFIX}{tenant_id or 'default'}:"
        f"{normalize_lock_user(user_id)}:{_trade_date_str(trade_date)}"
    )


def symbol_lock_key(
    tenant_id: str, user_id: object, trade_date: date | str, symbol: str
) -> str:
    return (
        f"{SYMBOL_LOCK_PREFIX}{tenant_id or 'default'}:"
        f"{normalize_lock_user(user_id)}:{_trade_date_str(trade_date)}:"
        f"{normalize_lock_symbol(symbol)}"
    )


def cooldown_symbol_key(tenant_id: str, user_id: object, symbol: str) -> str:
    return (
        f"{COOLDOWN_PREFIX}{tenant_id or 'default'}:"
        f"{normalize_lock_user(user_id)}:{normalize_lock_symbol(symbol)}"
    )


def fired_key(
    rule_id: int | None,
    tenant_id: str,
    user_id: object,
    trade_date: date | str,
    symbol: str,
) -> str:
    return (
        f"{FIRED_PREFIX}{int(rule_id or 0)}:{tenant_id or 'default'}:"
        f"{normalize_lock_user(user_id)}:{_trade_date_str(trade_date)}:"
        f"{normalize_lock_symbol(symbol)}"
    )


def lock_ttl_seconds(now: datetime | None = None) -> int:
    current = now or datetime.now(_SH_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_SH_TZ)
    else:
        current = current.astimezone(_SH_TZ)
    end_of_day = current.replace(hour=23, minute=59, second=59, microsecond=0)
    return max(60, int((end_of_day + timedelta(hours=4) - current).total_seconds()))


@dataclass
class RiskLocks:
    account_frozen: bool = False
    symbols: set[str] = field(default_factory=set)
    # 跨日冷却：止损卖出后若干天内不允许买回（与当日锁分开，便于审计）
    cooled_symbols: set[str] = field(default_factory=set)

    def is_symbol_locked(self, symbol: str) -> bool:
        if self.account_frozen:
            return True
        suffix = normalize_lock_symbol(symbol)
        prefix = StockCodeUtil.to_prefix(symbol)
        return (
            suffix in self.symbols
            or prefix in self.symbols
            or symbol in self.symbols
            or suffix in self.cooled_symbols
            or prefix in self.cooled_symbols
            or symbol in self.cooled_symbols
        )


def _raw_redis(redis: Any):
    return getattr(redis, "client", redis)


def load_risk_locks(
    redis: Any,
    tenant_id: str,
    user_id: object,
    trade_date: date | str,
    *,
    include_cooldowns: bool = True,
) -> RiskLocks:
    client = _raw_redis(redis)
    locks = RiskLocks()
    if client is None:
        return locks
    try:
        if client.get(account_lock_key(tenant_id, user_id, trade_date)):
            locks.account_frozen = True
        pattern = (
            f"{SYMBOL_LOCK_PREFIX}{tenant_id or 'default'}:"
            f"{normalize_lock_user(user_id)}:{_trade_date_str(trade_date)}:*"
        )
        for raw_key in client.scan_iter(match=pattern, count=200):
            key = str(raw_key)
            symbol = key.rsplit(":", 1)[-1]
            if symbol:
                locks.symbols.add(symbol)
        if include_cooldowns:
            cooldown_pattern = (
                f"{COOLDOWN_PREFIX}{tenant_id or 'default'}:"
                f"{normalize_lock_user(user_id)}:*"
            )
            for raw_key in client.scan_iter(match=cooldown_pattern, count=200):
                key = str(raw_key)
                symbol = key.rsplit(":", 1)[-1]
                if symbol:
                    locks.cooled_symbols.add(symbol)
    except Exception:
        return locks
    return locks


def write_account_lock(redis: Any, tenant_id: str, user_id: object, trade_date: date | str) -> None:
    client = _raw_redis(redis)
    if client is None:
        return
    client.set(account_lock_key(tenant_id, user_id, trade_date), "1", ex=lock_ttl_seconds())


def write_symbol_lock(
    redis: Any, tenant_id: str, user_id: object, trade_date: date | str, symbol: str
) -> None:
    client = _raw_redis(redis)
    if client is None:
        return
    client.set(
        symbol_lock_key(tenant_id, user_id, trade_date, symbol),
        "1",
        ex=lock_ttl_seconds(),
    )


def write_symbol_cooldown(
    redis: Any,
    tenant_id: str,
    user_id: object,
    symbol: str,
    ttl_seconds: int | None = None,
) -> None:
    """止损卖出后写入跨日冷却，防止下一轮调仓按旧信号买回。"""
    client = _raw_redis(redis)
    if client is None:
        return
    ttl = ttl_seconds
    if ttl is None or ttl <= 0:
        try:
            ttl = int(
                os.getenv(
                    "SIM_STOP_LOSS_COOLDOWN_SECONDS",
                    str(DEFAULT_COOLDOWN_SECONDS),
                )
            )
        except (TypeError, ValueError):
            ttl = DEFAULT_COOLDOWN_SECONDS
    client.set(
        cooldown_symbol_key(tenant_id, user_id, symbol),
        "1",
        ex=max(60, int(ttl)),
    )


def mark_fired(
    redis: Any,
    rule_id: int | None,
    tenant_id: str,
    user_id: object,
    trade_date: date | str,
    symbol: str,
    cooldown_seconds: int | None = None,
) -> None:
    client = _raw_redis(redis)
    if client is None:
        return
    ttl = cooldown_seconds if cooldown_seconds and cooldown_seconds > 0 else lock_ttl_seconds()
    client.set(
        fired_key(rule_id, tenant_id, user_id, trade_date, symbol),
        "1",
        ex=max(60, int(ttl)),
    )


def already_fired(
    redis: Any,
    rule_id: int | None,
    tenant_id: str,
    user_id: object,
    trade_date: date | str,
    symbol: str,
) -> bool:
    client = _raw_redis(redis)
    if client is None:
        return False
    try:
        return bool(client.get(fired_key(rule_id, tenant_id, user_id, trade_date, symbol)))
    except Exception:
        return False


def filter_buy_orders(orders: Iterable[Any], locks: RiskLocks) -> list[Any]:
    kept: list[Any] = []
    for order in orders:
        side = str(getattr(order, "side", "") or "").upper()
        symbol = str(getattr(order, "symbol", "") or "")
        if side == "BUY" and locks.is_symbol_locked(symbol):
            continue
        kept.append(order)
    return kept
