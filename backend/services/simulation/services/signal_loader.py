"""
Signal Loader - 从 engine_signal_scores 表加载最新 PK 信号
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.shared.stock_utils import StockCodeUtil

logger = logging.getLogger(__name__)

_SH_TZ = ZoneInfo("Asia/Shanghai")

# 默认模型在当前生效日（trade_date <= 今天）的最新有效批次。
# 口径与 manual_execution_service.get_default_model_hosted_status 对齐：
# 默认模型 + 已完成 + 未走兜底。
_DEFAULT_MODEL_BATCH_SQL = """
    SELECT s.run_id
    FROM engine_signal_scores s
    JOIN qm_user_models m
      ON m.tenant_id = s.tenant_id
     AND m.user_id = s.user_id
     AND m.is_default = TRUE
     AND m.status IN ('ready', 'active')
     AND COALESCE((m.metadata_json->>'system_default')::boolean, FALSE) = FALSE
    JOIN qm_model_inference_runs r
      ON r.run_id = s.run_id
     AND r.tenant_id = s.tenant_id
     AND r.user_id = s.user_id
     AND r.model_id = m.model_id
     AND r.status = 'completed'
     AND COALESCE(r.fallback_used, FALSE) = FALSE
    WHERE s.tenant_id = :tenant_id
      AND s.user_id = :user_id
      AND s.trade_date <= :today
    GROUP BY s.run_id, s.trade_date
    ORDER BY s.trade_date DESC, MAX(r.created_at) DESC
    LIMIT 1
"""

# 兜底：无默认模型批次时取当前生效日最近写入的一批。
_LATEST_BATCH_SQL = """
    SELECT run_id
    FROM engine_signal_scores
    WHERE tenant_id = :tenant_id
      AND user_id = :user_id
      AND trade_date <= :today
    GROUP BY run_id, trade_date
    ORDER BY trade_date DESC, MAX(created_at) DESC
    LIMIT 1
"""


@dataclass
class SignalScore:
    """信号得分数据结构"""
    symbol: str
    score: float
    trade_date: date
    run_id: str
    tenant_id: str
    user_id: str


def _normalize_signal_symbol(raw: object) -> str:
    """信号表可能是 6 位数字；行情层要 suffix，否则 quotes 对不上、整轮 0 单。"""
    text = str(raw or "").upper().strip()
    return StockCodeUtil.to_suffix(text) or text


def _rows_to_signals(rows: list[Any]) -> list[SignalScore]:
    return [
        SignalScore(
            symbol=_normalize_signal_symbol(row[0]),
            score=float(row[1]),
            trade_date=row[2],
            run_id=str(row[3]),
            tenant_id=str(row[4]),
            user_id=str(row[5]),
        )
        for row in rows
    ]


def _sql_limit_clause(limit: int | None) -> str:
    """limit 为 None / <=0 时不截断，避免 topk 选股被静默砍宇宙。"""
    if limit is None or int(limit) <= 0:
        return ""
    return " LIMIT :limit"


class SignalLoader:
    """
    从 engine_signal_scores 表加载最新 PK 信号。
    支持按 run_id 指定批次，或取最新 trade_date。
    """

    async def load_latest_signals(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: str,
        run_id: str | None = None,
        min_score: float = 0.0,
        limit: int | None = None,
    ) -> list[SignalScore]:
        """
        加载最新信号。

        Args:
            db: 数据库会话
            tenant_id: 租户 ID
            user_id: 用户 ID
            run_id: 指定批次 ID，若 None 则取最新 trade_date
            min_score: 最小得分阈值，低于此值的信号将被过滤
            limit: 返回数量上限；None/<=0 表示加载该截面全部行
                （托管模拟盘 topk 选股必须全量，否则可交易过滤后无法补位）

        Returns:
            信号列表，按 score 降序排列
        """
        tenant = (tenant_id or "").strip() or "default"
        uid = str(user_id or "").strip()
        limit_sql = _sql_limit_clause(limit)

        # 未显式指定批次时，先解析「当前已生效的单一批次」再按 run_id 取数。
        # 见 _resolve_effective_batch：engine_signal_scores.trade_date 是信号
        # 生效日（T+1），直接取 MAX(trade_date) 会选中尚未生效的未来批次。
        effective_run_id = run_id or await self._resolve_effective_batch(
            db, tenant, uid
        )

        if effective_run_id:
            query = text(f"""
                SELECT symbol, fusion_score, trade_date, run_id, tenant_id, user_id
                FROM engine_signal_scores
                WHERE tenant_id = :tenant_id
                  AND user_id = :user_id
                  AND run_id = :run_id
                  AND fusion_score >= :min_score
                ORDER BY fusion_score DESC
                {limit_sql}
            """)
            params: dict[str, Any] = {
                "tenant_id": tenant,
                "user_id": uid,
                "run_id": effective_run_id,
                "min_score": min_score,
            }
        else:
            # 没有「已生效」的批次：不查库，直接走下方 pred.parquet 兜底，
            # 绝不能退回 MAX(trade_date)——那会拿到尚未生效的未来批次。
            query = None
            params = {}
        if limit_sql and query is not None:
            params["limit"] = int(limit)  # type: ignore[arg-type]

        try:
            signals: list[SignalScore] = []
            if query is not None:
                result = await db.execute(query, params)
                signals = _rows_to_signals(result.fetchall())
            logger.info(
                "SignalLoader: 加载信号 %d 条, tenant=%s user=%s run_id=%s limit=%s",
                len(signals),
                tenant,
                uid,
                effective_run_id or "latest",
                limit if limit_sql else "all",
            )
            if signals or run_id:
                return signals
            # 取最新批次路径且信号表为空（如被后续补推覆盖清空）：回退默认模型
            # pred.parquet 数据日截面，与手动任务信号加载共用同一兜底源。
            return await self._load_pred_parquet_fallback(tenant, uid)
        except Exception as e:
            logger.error("SignalLoader: 加载信号失败 %s", e, exc_info=True)
            return []

    @staticmethod
    async def _first_row(
        db: AsyncSession,
        sql: str,
        params: dict[str, Any],
    ) -> Any | None:
        try:
            return (await db.execute(text(sql), params)).first()
        except Exception as exc:  # noqa: BLE001
            logger.warning("SignalLoader: 查询信号批次失败: %s", exc)
            return None

    async def _resolve_effective_batch(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: str,
    ) -> str | None:
        """解析当前已生效（trade_date <= 今天）的最新单一信号批次 run_id。

        ``engine_signal_scores.trade_date`` 是「信号生效日」(T+1) 语义
        （见 selection.py 口径说明与 script_runner 写入的 prediction_trade_date）。
        直接取 ``MAX(trade_date)`` 会选中尚未生效的未来批次——跨周末/假期时
        ``next_session`` 会跳到下一个交易日，或补跑历史写入远期日期——导致
        模拟盘提前交易，且因它恒为 MAX 而长期霸占后续每一轮。

        同一生效日存在多个模型批次时，优先取「默认模型」的那一批：不同模型的
        ``fusion_score`` 量纲与归一化不同，混合（或按写入时间取到非默认模型）
        会让 topk 选股变成跨模型不可比的结果，且用户无从察觉。默认模型批次
        缺失时退化为「最近写入」，保证不比改动前更差。
        """
        today = datetime.now(_SH_TZ).date()
        params = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "today": today,
        }
        row = await self._first_row(db, _DEFAULT_MODEL_BATCH_SQL, params)
        if not row:
            row = await self._first_row(db, _LATEST_BATCH_SQL, params)
        if row and row[0]:
            return str(row[0])
        logger.warning(
            "SignalLoader: 无已生效信号批次（trade_date <= %s）, tenant=%s user=%s",
            today,
            tenant_id,
            user_id,
        )
        return None

    async def _load_pred_parquet_fallback(
        self, tenant_id: str, user_id: str
    ) -> list[SignalScore]:
        """信号表为空时从默认模型 pred.parquet 回退最新截面。

        trade_date 用生效日（T+1，与信号表口径一致）；无生效日时用数据日。
        任何解析失败都静默返回空列表，不影响主路径。
        """
        try:
            from datetime import date as _date

            from backend.services.live_trading.services.manual_execution_service import (
                manual_execution_service,
            )

            hosted = await manual_execution_service.get_default_model_hosted_status(
                tenant_id=tenant_id, user_id=user_id
            )
            if not hosted.get("available"):
                # 过期/未进入窗口的批次不能兜底：data_trade_date 仍会返回，
                # 照着它读 pred.parquet 等于用早已失效的信号继续下单。
                logger.warning(
                    "SignalLoader: 默认模型批次不可用，跳过 pred.parquet 兜底 "
                    "tenant=%s user=%s reason=%s",
                    tenant_id,
                    user_id,
                    hosted.get("reason_code"),
                )
                return []
            model_id = str(hosted.get("latest_default_model_id") or "").strip()
            data_trade_date = str(hosted.get("data_trade_date") or "").strip()[:10]
            if not model_id or not data_trade_date:
                return []
            effective_raw = (
                hosted.get("prediction_trade_date") or data_trade_date
            )
            effective_date = _date.fromisoformat(str(effective_raw)[:10])
            fallback_run_id = f"pred_parquet_{model_id}"

            rows = await manual_execution_service.load_pred_parquet_signal_rows(
                tenant_id=tenant_id,
                user_id=user_id,
                model_id=model_id,
                data_trade_date=data_trade_date,
            )
            signals = [
                SignalScore(
                    symbol=_normalize_signal_symbol(row.get("symbol")),
                    score=float(row.get("fusion_score") or 0.0),
                    trade_date=effective_date,
                    run_id=fallback_run_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                )
                for row in rows
                if row.get("symbol")
            ]
            if signals:
                logger.warning(
                    "SignalLoader: 信号表为空，从 pred.parquet 回退 %d 条截面, "
                    "tenant=%s user=%s model=%s data_trade_date=%s effective_date=%s run_id=%s",
                    len(signals),
                    tenant_id,
                    user_id,
                    model_id,
                    data_trade_date,
                    effective_date,
                    fallback_run_id,
                )
            else:
                logger.warning(
                    "SignalLoader: 信号表为空且 pred.parquet 回退为空, tenant=%s user=%s model=%s date=%s",
                    tenant_id, user_id, model_id, data_trade_date,
                )
            return signals
        except Exception as e:
            logger.warning(
                "SignalLoader: pred.parquet 回退失败 tenant=%s user=%s model=%s date=%s err=%s",
                tenant_id, user_id, model_id if 'model_id' in locals() else "unknown",
                data_trade_date if 'data_trade_date' in locals() else "unknown", e)
            return []

    async def load_signals_for_date(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: str,
        trade_date: date,
        run_id: str | None = None,
        min_score: float = 0.0,
        limit: int | None = None,
    ) -> list[SignalScore]:
        """加载**指定交易日**的信号（时光回放用）。

        与 load_latest_signals 的区别是不取 MAX(trade_date) 而是精确匹配。
        注意 engine_signal_scores.trade_date 存的是「信号生效日」(T+1)，
        所以这里传的 trade_date 就是要交易的那一天，不需要再做偏移；
        偏移只发生在生成信号时（用 prev_session(D) 作为推理数据日）。
        """
        tenant = (tenant_id or "").strip() or "default"
        uid = str(user_id or "").strip()

        conditions = [
            "tenant_id = :tenant_id",
            "user_id = :user_id",
            "trade_date = :trade_date",
            "fusion_score >= :min_score",
        ]
        params: dict[str, Any] = {
            "tenant_id": tenant,
            "user_id": uid,
            "trade_date": trade_date,
            "min_score": min_score,
        }
        if run_id:
            conditions.append("run_id = :run_id")
            params["run_id"] = run_id
        limit_sql = _sql_limit_clause(limit)
        if limit_sql:
            params["limit"] = int(limit)  # type: ignore[arg-type]

        query = text(f"""
            SELECT symbol, fusion_score, trade_date, run_id, tenant_id, user_id
            FROM engine_signal_scores
            WHERE {" AND ".join(conditions)}
            ORDER BY fusion_score DESC
            {limit_sql}
        """)

        try:
            rows = (await db.execute(query, params)).fetchall()
            signals = _rows_to_signals(rows)
            logger.info(
                "SignalLoader: 加载 %s 的信号 %d 条, tenant=%s user=%s run_id=%s limit=%s",
                trade_date,
                len(signals),
                tenant,
                uid,
                run_id or "any",
                limit if limit_sql else "all",
            )
            return signals
        except Exception as e:
            logger.error(
                "SignalLoader: 加载指定日期信号失败 date=%s %s", trade_date, e,
                exc_info=True,
            )
            return []

    async def load_latest_run_id(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: str,
    ) -> str | None:
        """
        获取最新的 run_id。
        """
        tenant = (tenant_id or "").strip() or "default"
        uid = str(user_id or "").strip()

        query = text("""
            SELECT run_id
            FROM engine_signal_scores
            WHERE tenant_id = :tenant_id
              AND user_id = :user_id
            ORDER BY trade_date DESC, created_at DESC
            LIMIT 1
        """)
        try:
            result = await db.execute(query, {"tenant_id": tenant, "user_id": uid})
            row = result.fetchone()
            return str(row[0]) if row else None
        except Exception as e:
            logger.error("SignalLoader: 获取最新 run_id 失败 %s", e)
            return None

    async def load_signals_by_symbols(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: str,
        symbols: list[str],
        run_id: str | None = None,
    ) -> dict[str, float]:
        """
        按指定股票代码加载信号得分。

        Returns:
            {symbol: score} 字典
        """
        if not symbols:
            return {}

        tenant = (tenant_id or "").strip() or "default"
        uid = str(user_id or "").strip()
        normalized_symbols = [s.upper() for s in symbols]

        if run_id:
            query = text("""
                SELECT symbol, fusion_score
                FROM engine_signal_scores
                WHERE tenant_id = :tenant_id
                  AND user_id = :user_id
                  AND run_id = :run_id
                  AND symbol = ANY(:symbols)
            """)
            params = {
                "tenant_id": tenant,
                "user_id": uid,
                "run_id": run_id,
                "symbols": normalized_symbols,
            }
        else:
            query = text("""
                SELECT symbol, fusion_score
                FROM engine_signal_scores
                WHERE tenant_id = :tenant_id
                  AND user_id = :user_id
                  AND trade_date = (
                      SELECT MAX(trade_date) FROM engine_signal_scores
                      WHERE tenant_id = :tenant_id AND user_id = :user_id
                  )
                  AND symbol = ANY(:symbols)
            """)
            params = {
                "tenant_id": tenant,
                "user_id": uid,
                "symbols": normalized_symbols,
            }

        try:
            result = await db.execute(query, params)
            rows = result.fetchall()
            return {str(row[0]).upper(): float(row[1]) for row in rows}
        except Exception as e:
            logger.error("SignalLoader: 按股票加载信号失败 %s", e)
            return {}


signal_loader = SignalLoader()
