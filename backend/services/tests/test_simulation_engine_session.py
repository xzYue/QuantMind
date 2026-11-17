from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.simulation.engine import SimulationEngine
from backend.services.simulation.services.signal_loader import _normalize_signal_symbol


@pytest.mark.asyncio
@pytest.mark.parametrize("with_series", [False, True])
async def test_live_quotes_include_snapshot_only_symbols(with_series):
    series_tick = {"price": 41.0, "timestamp": 1000, "age_s": 20}
    snapshot_tick = {
        "price": 13.37,
        "timestamp": 1000,
        "age_s": 20,
        "price_source": "redis_snapshot",
        "recent_volume": None,
    }
    series = {"600036.SH": series_tick} if with_series else {}
    snapshots = AsyncMock(return_value={"301037.SZ": snapshot_tick})
    with (
        patch(
            "backend.services.simulation.services.redis_series_quote.fetch_series_ticks",
            AsyncMock(return_value=series),
        ),
        patch(
            "backend.services.simulation.services.redis_series_quote.fetch_snapshot_ticks",
            snapshots,
        ),
    ):
        quotes, ticks = await SimulationEngine()._load_live_quotes(
            ["600036.SH", "301037.SZ"] if with_series else ["301037.SZ"]
        )
    snapshots.assert_awaited_once_with(["301037.SZ"])
    assert quotes["301037.SZ"].current_price == 13.37
    assert quotes["SZ301037"].current_price == 13.37
    assert ticks["301037.SZ"] is snapshot_tick
    if with_series:
        assert ticks["600036.SH"] is series_tick


@pytest.mark.asyncio
async def test_live_quotes_do_not_read_snapshots_when_series_covers_all():
    snapshots = AsyncMock()
    with (
        patch(
            "backend.services.simulation.services.redis_series_quote.fetch_series_ticks",
            AsyncMock(return_value={"600036.SH": {"price": 41.0}}),
        ),
        patch(
            "backend.services.simulation.services.redis_series_quote.fetch_snapshot_ticks",
            snapshots,
        ),
    ):
        await SimulationEngine()._load_live_quotes(["600036.SH"])
    snapshots.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_cycle_uses_get_session_not_db_manager_session():
    engine = SimulationEngine()
    engine.signal_loader = MagicMock()
    engine.signal_loader.load_latest_signals = AsyncMock(return_value=[])

    fake_db = MagicMock()

    @asynccontextmanager
    async def fake_get_session(*_args, **_kwargs):
        yield fake_db

    with patch("backend.services.simulation.engine.get_session", fake_get_session):
        report = await engine.run_cycle(
            tenant_id="default",
            user_id="00000001",
            strategy_id="2",
        )

    assert report.error == "无可用信号"
    engine.signal_loader.load_latest_signals.assert_awaited_once()
    assert engine.signal_loader.load_latest_signals.await_args.kwargs["db"] is fake_db
    assert engine.signal_loader.load_latest_signals.await_args.kwargs["run_id"] is None


@pytest.mark.asyncio
async def test_run_cycle_does_not_treat_task_id_as_signal_batch():
    engine = SimulationEngine()
    engine.signal_loader = MagicMock()
    engine.signal_loader.load_latest_signals = AsyncMock(return_value=[])

    fake_db = MagicMock()

    @asynccontextmanager
    async def fake_get_session(*_args, **_kwargs):
        yield fake_db

    with patch("backend.services.simulation.engine.get_session", fake_get_session):
        report = await engine.run_cycle(
            tenant_id="default",
            user_id="00000001",
            strategy_id="2",
            run_id="bootstrap_run_1789367236_2",
        )

    assert report.error == "无可用信号"
    assert engine.signal_loader.load_latest_signals.await_args.kwargs["run_id"] is None


def test_signal_loader_normalizes_bare_code_to_suffix():
    assert _normalize_signal_symbol("600928") == "600928.SH"
    assert _normalize_signal_symbol("000419") == "000419.SZ"
    assert _normalize_signal_symbol("SH600928") == "600928.SH"
    assert _normalize_signal_symbol("600928.SH") == "600928.SH"


def test_ensure_redis_attaches_connected_trade_client():
    engine = SimulationEngine()
    assert getattr(engine.redis, "client", None) is None

    fake = MagicMock()
    fake.client = object()
    with patch(
        "backend.services.trade_shared.redis_client.get_redis", return_value=fake
    ):
        engine._ensure_redis()

    assert engine.redis is fake
    assert engine.account_manager.redis is fake


def test_ticks_from_bars_marks_local_daily_close_source():
    bar = MagicMock(close=12.5, trade_date=date(2026, 11, 13))
    ticks = SimulationEngine._ticks_from_bars({"600000.SH": bar})
    tick = ticks["600000.SH"]
    assert tick["price"] == 12.5
    assert tick["price_source"] == "local_daily_close"
    assert "SH600000" in ticks
    # 本地日线也必须带真实时间戳，否则下游会把陈旧报价当成 0 秒新鲜。
    expected = datetime(
        2026, 11, 13, 15, 0, tzinfo=timezone(timedelta(hours=8))
    ).timestamp()
    assert tick["timestamp"] == pytest.approx(expected)
    assert tick["age_s"] is not None


@pytest.mark.asyncio
async def test_bootstrap_run_uses_local_bars_when_realtime_empty():
    """bootstrap_ run_id：实时行情为空时改用本地日线，不再直接 realtime_quote_unavailable。"""
    engine = SimulationEngine()
    engine.signal_loader = MagicMock()
    signal = MagicMock()
    signal.symbol = "600000.SH"
    signal.score = 1.0
    engine.signal_loader.load_latest_signals = AsyncMock(return_value=[signal])

    fake_db = MagicMock()
    fake_db.commit = AsyncMock()

    @asynccontextmanager
    async def fake_get_session(*_args, **_kwargs):
        yield fake_db

    bar = MagicMock(
        close=10.0,
        limit_up=11.0,
        limit_down=9.0,
        suspended=False,
        pre_close=10.0,
    )
    fill_result = MagicMock(
        success=True, quantity=100, price=10.0, message="ok", commission=0.0
    )

    with (
        patch("backend.services.simulation.engine.get_session", fake_get_session),
        patch.object(
            engine, "_load_strategy_config", AsyncMock(return_value=MagicMock())
        ),
        patch.object(engine, "_ensure_redis"),
        patch.object(
            engine.account_manager,
            "get_account",
            AsyncMock(
                return_value={
                    "cash": 1_000_000.0,
                    "positions": {},
                    "total_asset": 1_000_000.0,
                }
            ),
        ),
        patch.object(engine, "_build_account", return_value=MagicMock(positions={})),
        patch.object(engine, "_load_live_quotes", AsyncMock(return_value=({}, {}))),
        patch.object(
            engine, "_load_bars", AsyncMock(return_value={"600000.SH": bar})
        ) as load_bars,
        patch.object(
            engine.rebalance_calculator,
            "calculate",
            return_value=[
                MagicMock(
                    symbol="600000.SH",
                    side="BUY",
                    quantity=100,
                    price=10.0,
                    reason="boot",
                )
            ],
        ),
        patch.object(
            engine, "_apply_risk_buy_locks", side_effect=lambda orders, **_k: orders
        ),
        patch.object(
            engine, "_execute_order", AsyncMock(return_value=fill_result)
        ) as exec_order,
        patch(
            "backend.services.simulation.engine.SimulationExecutionEngine",
            return_value=MagicMock(),
        ),
        patch.object(engine, "_sync_snapshot", AsyncMock()),
    ):
        report = await engine.run_cycle(
            tenant_id="default",
            user_id="00000001",
            strategy_id="2",
            run_id="bootstrap_run_1_2",
        )

    load_bars.assert_awaited_once()
    assert load_bars.await_args.kwargs.get("require_fresh") is True
    assert report.error is None
    assert report.filled_count == 1
    assert exec_order.await_args.kwargs.get("allow_stale_fill") is True


@pytest.mark.asyncio
async def test_non_bootstrap_still_rejects_without_realtime():
    engine = SimulationEngine()
    engine.signal_loader = MagicMock()
    signal = MagicMock()
    signal.symbol = "600000.SH"
    signal.score = 1.0
    engine.signal_loader.load_latest_signals = AsyncMock(return_value=[signal])

    fake_db = MagicMock()

    @asynccontextmanager
    async def fake_get_session(*_args, **_kwargs):
        yield fake_db

    with (
        patch("backend.services.simulation.engine.get_session", fake_get_session),
        patch.object(
            engine, "_load_strategy_config", AsyncMock(return_value=MagicMock())
        ),
        patch.object(engine, "_ensure_redis"),
        patch.object(
            engine.account_manager,
            "get_account",
            AsyncMock(return_value={"cash": 1_000_000.0, "positions": {}}),
        ),
        patch.object(engine, "_build_account", return_value=MagicMock(positions={})),
        patch.object(engine, "_load_live_quotes", AsyncMock(return_value=({}, {}))),
        patch.object(engine, "_load_bars", AsyncMock()) as load_bars,
    ):
        report = await engine.run_cycle(
            tenant_id="default",
            user_id="00000001",
            strategy_id="2",
            run_id="sim_hosted_1",
        )

    load_bars.assert_not_awaited()
    assert report.error == "realtime_quote_unavailable"
