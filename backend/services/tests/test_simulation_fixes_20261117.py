"""模拟盘修复回归测试：陈旧行情、碎单、bootstrap 当日跳过、止损冷却、信号过期。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from backend.services.live_trading.services import (
    manual_execution_service as mes_module,
)
from backend.services.live_trading.services.risk_lock import (
    filter_buy_orders,
    load_risk_locks,
    normalize_lock_symbol,
)
from backend.services.live_trading.services.risk_trigger_eval import QuoteView
from backend.services.live_trading.services.risk_trigger_service import (
    filter_stale_quotes,
    quote_age_seconds,
)
from backend.services.simulation.engine import (
    SimulationEngine,
    StaleLocalMarketDataError,
)
from backend.services.simulation.services.rebalance_calculator import (
    Quote,
    RebalanceCalculator,
    SimulationAccount,
    StrategyConfig,
)
from backend.services.simulation.services.signal_loader import SignalLoader, SignalScore
from backend.services.simulation.services.simulation_hosted_scheduler import (
    _normalize_live_trade_config,
    _should_trigger,
)

SH = ZoneInfo("Asia/Shanghai")


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    def scan_iter(self, match: str | None = None, count: int | None = None):
        import fnmatch

        for key in list(self.store):
            if match is None or fnmatch.fnmatch(key, match):
                yield key


def test_stale_guard_rejects_missing_latest_partition():
    engine = SimulationEngine()
    expected = date(2026, 11, 13)
    with patch.object(
        SimulationEngine,
        "_expected_previous_trading_day",
        staticmethod(lambda current: expected),
    ):
        with pytest.raises(StaleLocalMarketDataError) as excinfo:
            engine._assert_local_data_fresh(date(2026, 11, 6), "CN")
    assert "stale_local_market_data" in str(excinfo.value)


def test_stale_guard_allows_one_session_lag_when_configured(monkeypatch):
    expected = date(2026, 11, 13)
    monkeypatch.setenv("SIM_LOCAL_DATA_MAX_STALE_SESSIONS", "1")
    with patch.object(
        SimulationEngine,
        "_expected_previous_trading_day",
        staticmethod(lambda current: expected),
    ):
        SimulationEngine._assert_local_data_fresh(date(2026, 11, 12), "CN")


def test_stale_guard_default_allows_one_session_lag(monkeypatch):
    """凌晨 bootstrap 时上一交易日日线通常还没落盘，默认放行 1 个交易日。"""
    monkeypatch.delenv("SIM_LOCAL_DATA_MAX_STALE_SESSIONS", raising=False)
    expected = date(2026, 11, 16)
    with patch.object(
        SimulationEngine,
        "_expected_previous_trading_day",
        staticmethod(lambda current: expected),
    ):
        SimulationEngine._assert_local_data_fresh(date(2026, 11, 13), "CN")


def test_stale_guard_strict_zero_rejects_one_session_lag(monkeypatch):
    monkeypatch.setenv("SIM_LOCAL_DATA_MAX_STALE_SESSIONS", "0")
    expected = date(2026, 11, 16)
    with patch.object(
        SimulationEngine,
        "_expected_previous_trading_day",
        staticmethod(lambda current: expected),
    ):
        with pytest.raises(StaleLocalMarketDataError):
            SimulationEngine._assert_local_data_fresh(date(2026, 11, 13), "CN")


def test_bootstrap_day_skips_same_day_rebalance():
    cfg = _normalize_live_trade_config(
        {
            "schedule_type": "interval",
            "rebalance_days": 5,
            "enabled_sessions": ["PM"],
            "sell_time": "14:45",
            "buy_time": "14:45",
        }
    )
    now = datetime(2026, 6, 2, 14, 45, tzinfo=SH)
    decision = _should_trigger(
        now=now,
        live_trade_config=cfg,
        started_day=now.date(),
        bootstrap_done=True,
    )
    assert decision.should_trigger is False
    assert decision.reason == "bootstrap_day_skip"


def test_min_order_amount_skips_tiny_buy_but_keeps_sell():
    calc = RebalanceCalculator()
    quotes = {"600000.SH": Quote(symbol="600000.SH", current_price=10.0)}
    orders = calc._generate_orders(
        {},
        {"600000.SH": 100},
        quotes,
        min_order_amount=5000.0,
    )
    assert orders == []

    sell_orders = calc._generate_orders(
        {"600000.SH": {"volume": 100, "cost": 10.0}},
        {},
        quotes,
        min_order_amount=5000.0,
    )
    assert [o.side for o in sell_orders] == ["SELL"]


def test_min_order_amount_capped_at_two_percent_nav():
    calc = RebalanceCalculator()
    strategy = StrategyConfig(topk=1, max_position_pct=1.0, min_order_amount=5000.0)
    signal = SignalScore(
        symbol="600000.SH",
        score=1.0,
        trade_date=date(2026, 6, 2),
        run_id="r",
        tenant_id="default",
        user_id="1",
    )
    account = SimulationAccount(cash=10_000.0, total_asset=10_000.0, positions={})
    with patch.object(calc, "_generate_orders", return_value=[]) as generate:
        calc.calculate(
            [signal],
            strategy,
            {"600000.SH": Quote(symbol="600000.SH", current_price=10.0)},
            account,
        )
    assert generate.call_args.kwargs["min_order_amount"] == pytest.approx(200.0)


def test_stop_loss_cooldown_blocks_buy_on_later_day():
    redis = FakeRedis()
    symbol = "SH600036"
    from backend.services.live_trading.services.risk_lock import write_symbol_cooldown

    write_symbol_cooldown(redis, "default", "1", symbol, ttl_seconds=7 * 86400)
    locks = load_risk_locks(redis, "default", "1", date(2026, 11, 23))
    assert normalize_lock_symbol(symbol) in locks.cooled_symbols
    assert locks.is_symbol_locked(symbol) is True

    orders = [
        SimpleNamespace(symbol=symbol, side="BUY"),
        SimpleNamespace(symbol=symbol, side="SELL"),
    ]
    kept = filter_buy_orders(orders, locks)
    assert [(o.symbol, o.side) for o in kept] == [(symbol, "SELL")]


def test_quote_age_and_stale_filter():
    now = datetime(2026, 11, 17, 15, 0, tzinfo=SH)
    age = quote_age_seconds({"timestamp": now.timestamp() - 30}, now=now)
    assert age == pytest.approx(30.0)
    # 日期型 timestamp 不能被当成 1970 年的 epoch 秒
    date_age = quote_age_seconds({"timestamp": "20261117145900"}, now=now)
    assert date_age == pytest.approx(60.0)

    fresh = QuoteView(symbol="600000.SH", price=10.0, age_seconds=30.0)
    stale = QuoteView(symbol="600001.SH", price=10.0, age_seconds=600.0)
    unknown = QuoteView(symbol="600002.SH", price=10.0)
    filtered = filter_stale_quotes(
        {"600000.SH": fresh, "600001.SH": stale, "600002.SH": unknown},
        max_age_seconds=180.0,
    )
    assert set(filtered) == {"600000.SH", "600002.SH"}


@pytest.mark.asyncio
async def test_pred_parquet_fallback_rejects_unavailable_batch(monkeypatch):
    hosted = AsyncMock(return_value={"available": False, "reason_code": "window_expired"})
    load_rows = AsyncMock(return_value=[{"symbol": "600000.SH", "fusion_score": 1.0}])
    monkeypatch.setattr(
        mes_module.manual_execution_service,
        "get_default_model_hosted_status",
        hosted,
    )
    monkeypatch.setattr(
        mes_module.manual_execution_service,
        "load_pred_parquet_signal_rows",
        load_rows,
    )
    signals = await SignalLoader()._load_pred_parquet_fallback("default", "1")
    assert signals == []
    load_rows.assert_not_awaited()
