"""tag_rules 回归测试（stdlib unittest，无需 pytest）。

覆盖三类历史缺陷，防止再次退化：
1. between 运算符曾把区间宽度写死为 1（mid_cap 只命中 1 只）；
2. 上游指标多为小数/0-1 口径，而规则阈值按百分数写死（导致永不命中或恒为真）；
3. 上游 return_Nd 已改名 future_return_Nd（未来收益标签），标签不得再引用它。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from backend.services.engine.data_platform import tag_rules
from backend.services.engine.data_platform.quantdb_hub import QuantDBDataHub

DT = 20261030

TECH_COLS = [
    "close", "pct_change", "vol_std_20", "vol_atr_14", "ma_gap_20", "rsi_14",
    "vol_to_ma5", "vol_to_ma20", "volume_trend_3d", "macd_hist", "beta_20",
]
VAL_COLS = [
    "pe_ttm", "pe_static", "pb", "ps_ttm", "dividend_rate", "total_mv",
    "float_mv", "net_profit_ttm", "revenue_ttm", "equity",
]
L1_COLS = [
    "chip_profit_ratio_20", "chip_profit_ratio_60", "chip_concentration_20",
    "chip_cost_90_width", "concept_hot_score", "concept_rotation_score",
    "concept_crowding_max", "ind_strength_20", "ind_rotation_speed_20",
    "ind_relative_momentum_20", "ind_crowding_20", "fun_roe", "fun_peg",
    "turn_20", "fun_mv_rank", "amt_ratio_5_20", "mom_ret_5d",
    "mom_ret_20d", "mom_ret_60d", "style_beta_20", "tech_bb_width",
    "tech_bb_pos", "tech_cci_20", "tech_adx_14", "vol_parkinson_20",
]
SENTI_COLS = ["buy_pressure", "sell_pressure", "liquidity_score"]
MARGIN_COLS = ["finance_balance", "finance_net"]
L2_COLS = ["flow_net_amount", "flow_net_ratio", "flow_super_net"]


def _frame(rows: list[dict], cols: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = 0.0
    return df


def _fixtures() -> dict[str, pd.DataFrame]:
    """两只合成标的：AAA 全条件成立，BBB 全条件不成立。"""
    common = {"dt": DT}
    tech = _frame([
        {**common, "symbol": "AAA.SH", "close": 10.0, "pct_change": 1.0,
         "ma_gap_20": 2.0, "rsi_14": 25.0, "vol_to_ma5": 2.0},
        {**common, "symbol": "BBB.SH", "close": 10.0, "pct_change": -1.0,
         "ma_gap_20": -2.0, "rsi_14": 60.0, "vol_to_ma5": 0.8},
    ], TECH_COLS)  # noqa: E128
    val = _frame([
        {**common, "symbol": "AAA.SH", "pe_ttm": 10.0, "pb": 0.8,
         "dividend_rate": 0.05, "total_mv": 500e8, "float_mv": 1e10},
        {**common, "symbol": "BBB.SH", "pe_ttm": 40.0, "pb": 3.0,
         "dividend_rate": 0.001, "total_mv": 50e8, "float_mv": 1e10},
    ], VAL_COLS)
    l1 = _frame([
        {**common, "symbol": "AAA.SH", "mom_ret_5d": 0.03, "mom_ret_20d": 0.15,
         "mom_ret_60d": 0.20, "turn_20": 0.06, "chip_concentration_20": 0.05,
         "chip_profit_ratio_20": 0.85, "concept_crowding_max": 0.99,
         "concept_hot_score": 6, "ind_strength_20": 0.30,
         "ind_rotation_speed_20": 0.20, "ind_crowding_20": 0.90,
         "tech_bb_width": 0.05, "fun_roe": 20.0, "vol_parkinson_20": 0.02,
         "amt_ratio_5_20": 1.5},
        {**common, "symbol": "BBB.SH", "mom_ret_5d": 0.001, "mom_ret_20d": 0.01,
         "mom_ret_60d": 0.01, "turn_20": 0.01, "chip_concentration_20": 0.50,
         "chip_profit_ratio_20": 0.10, "concept_crowding_max": 0.50,
         "concept_hot_score": 0, "ind_strength_20": 0.01,
         "ind_rotation_speed_20": 0.01, "ind_crowding_20": 0.10,
         "tech_bb_width": 0.50, "fun_roe": 2.0, "vol_parkinson_20": 0.06,
         "amt_ratio_5_20": 0.9},
    ], L1_COLS)
    senti = _frame([
        {**common, "symbol": "AAA.SH", "buy_pressure": 0.6, "sell_pressure": 0.4},
        {**common, "symbol": "BBB.SH", "buy_pressure": 0.2, "sell_pressure": 0.8},
    ], SENTI_COLS)
    margin = _frame([
        # finance_balance 上游单位为「万元」：1e5 万元 = 10 亿元
        {**common, "symbol": "AAA.SH", "finance_balance": 1e5, "finance_net": 1.0},
        {**common, "symbol": "BBB.SH", "finance_balance": 1e3, "finance_net": -1.0},
    ], MARGIN_COLS)
    l2 = _frame([
        {**common, "symbol": "AAA.SH", "flow_net_amount": 1e8, "flow_net_ratio": 0.10},
        {**common, "symbol": "BBB.SH", "flow_net_amount": -1e8, "flow_net_ratio": -0.10},
    ], L2_COLS)
    return {
        "qdb_technical_indicators": tech,
        "qdb_valuation": val,
        "qdb_l1_factors": l1,
        "qdb_market_sentiment": senti,
        "qdb_margin_trading": margin,
        "qdb_l1_l2_factors": l2,
    }


class _FakeHub:
    def __init__(self) -> None:
        self.data_dir = Path(tempfile.mkdtemp(prefix="qm_tag_test_"))
        self._conn = duckdb.connect(":memory:")
        for name, df in _fixtures().items():
            self._conn.register(name, df)

    def _get_duck_conn(self):
        return self._conn


class TagRuleTestBase(unittest.TestCase):
    def setUp(self) -> None:
        tag_rules._metrics_cache.update({"df": None, "ts": 0.0})
        tag_rules._tag_stocks_cache.clear()
        self.hub = _FakeHub()
        self._patcher = patch.object(
            QuantDBDataHub, "get_instance", return_value=self.hub
        )
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        tag_rules._metrics_cache.update({"df": None, "ts": 0.0})


class TestBetweenOperator(TagRuleTestBase):
    def test_range_tuple(self) -> None:
        rule = tag_rules.get_tag_by_id("mid_cap")
        self.assertIsNotNone(rule)
        self.assertTrue(rule.match(pd.Series({"total_mv_yi": 500.0}))[0])
        self.assertTrue(rule.match(pd.Series({"total_mv_yi": 300.0}))[0])
        self.assertTrue(rule.match(pd.Series({"total_mv_yi": 3000.0}))[0])
        self.assertFalse(rule.match(pd.Series({"total_mv_yi": 299.9}))[0])
        self.assertFalse(rule.match(pd.Series({"total_mv_yi": 3000.1}))[0])

    def test_legacy_scalar_and_pb(self) -> None:
        pb = tag_rules.get_tag_by_id("pb_broken")
        self.assertTrue(pb.match(pd.Series({"pb": 0.8}))[0])
        self.assertFalse(pb.match(pd.Series({"pb": 1.2}))[0])
        self.assertFalse(pb.match(pd.Series({"pb": -0.1}))[0])


class TestUnitNormalization(TagRuleTestBase):
    def test_units(self) -> None:
        df = tag_rules._build_metrics().set_index("symbol")
        a = df.loc["AAA.SH"]
        self.assertAlmostEqual(a["mom_5d_pct"], 3.0, places=6)
        self.assertAlmostEqual(a["mom_20d_pct"], 15.0, places=6)
        self.assertAlmostEqual(a["mom_60d_pct"], 20.0, places=6)
        self.assertAlmostEqual(a["turn_20"], 6.0, places=6)
        self.assertAlmostEqual(a["ind_strength_20"], 30.0, places=6)
        self.assertAlmostEqual(a["ind_rotation_speed_20"], 20.0, places=6)
        self.assertAlmostEqual(a["ind_crowding_20"], 90.0, places=6)
        self.assertAlmostEqual(a["chip_profit_ratio_20"], 85.0, places=6)
        self.assertAlmostEqual(a["concept_crowding_max"], 99.0, places=6)
        self.assertAlmostEqual(a["concept_hot_score"], 60.0, places=6)

    def test_margin_ratio_wan_yuan(self) -> None:
        df = tag_rules._build_metrics().set_index("symbol")
        # 1e5 万元 = 10 亿元；流通市值 1e10 元 = 100 亿元 -> 10%
        self.assertAlmostEqual(float(df.loc["AAA.SH", "margin_ratio"]), 10.0, places=6)
        self.assertAlmostEqual(float(df.loc["BBB.SH", "margin_ratio"]), 0.1, places=6)


class TestNoLookAhead(TagRuleTestBase):
    def test_no_tag_uses_future_or_legacy_return(self) -> None:
        offenders = [
            t.id for t in tag_rules.TAGS
            if t.cond and ("return_" in str(t.cond[0]) or "future_return" in str(t.cond[0]))
        ]
        self.assertEqual(offenders, [])
        self.assertEqual(tag_rules.get_tag_by_id("breakout").cond[0], "mom_20d_pct")
        self.assertEqual(tag_rules.get_tag_by_id("momentum").cond[0], "mom_5d_pct")

    def test_removed_tags_absent_and_presets_valid(self) -> None:
        self.assertIsNone(tag_rules.get_tag_by_id("gross_margin_quality"))
        self.assertIsNone(tag_rules.get_tag_by_id("alpha112"))
        ids = {t.id for t in tag_rules.TAGS}
        for preset in tag_rules.PRESETS:
            for tid in preset["tags"]:
                self.assertIn(tid, ids, f"preset {preset['id']} 引用了不存在的标签 {tid}")


class TestSyntheticSymbols(TagRuleTestBase):
    def test_positive_symbol(self) -> None:
        tags = {t["id"] for t in tag_rules.match_tags_for_symbol("AAA.SH")}
        for expected in ("breakout", "active_small", "chip_concentrated",
                         "concept_hot", "margin_high", "mid_cap", "rsi_oversold"):
            self.assertIn(expected, tags)
        self.assertNotIn("momentum", tags)  # 5 日动量 3% < 5

    def test_negative_symbol(self) -> None:
        tags = {t["id"] for t in tag_rules.match_tags_for_symbol("BBB.SH")}
        for absent in ("breakout", "active_small", "chip_concentrated",
                       "concept_hot", "margin_high", "mid_cap"):
            self.assertNotIn(absent, tags)


if __name__ == "__main__":
    unittest.main(verbosity=2)
