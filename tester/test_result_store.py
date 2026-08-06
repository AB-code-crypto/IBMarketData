from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tester.models import (
    CompletedTrade,
    DailyResult,
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    SimulationResult,
    TesterSignal,
)
from tester.result_store import ResultStore


class ResultStoreTest(unittest.TestCase):
    def test_all_results_are_stored_in_one_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            result_dir = Path(temporary_dir)
            store = ResultStore(result_dir)
            store.save_test_settings(
                {
                    "START_DATETIME_MSK": "2026-01-01 00:00:00",
                    "END_DATETIME_MSK": "2026-07-31 23:59:59",
                    "ROLLING_BACK_MINUTES_VALUES": [90],
                    "ROLLING_TRADE_MINUTES_VALUES": [30],
                    "PEARSON_MIN_VALUES": [0.6],
                    "MINMAX_HARD_FILTER_MAX_RATIO_VALUES": [5],
                    "CANDIDATE_MIN_COUNT_VALUES": [3, 7],
                    "CANDIDATE_MAX_COUNT_VALUES": [7, 9, 11],
                    "POTENTIAL_MIN_ABS_END_DELTA_POINTS_VALUES": [30],
                    "DELAY_SECONDS_VALUES": [10],
                    "TAKE_PROFIT_POINTS_VALUES": [200],
                    "STOP_LOSS_POINTS_VALUES": [150],
                    "DAILY_TAKE_PROFIT_USD_VALUES": [0.0],
                    "FUT_SEARCH_HOUR_GROUPS_CT": {
                        15: [],
                        16: [],
                        17: [0, 1, 2],
                    },
                }
            )

            signal_variant = SignalVariant(90, 30, 0.7, 1.5, 3, 9, 10.0)
            execution_variant = ExecutionVariant(10, 50.0, 150.0, 0.0)
            signal_batch = SignalBatchResult(
                [self._signal()],
                100,
                2,
                80,
            )

            first_id = store.save_run(
                signal_variant=signal_variant,
                execution_variant=execution_variant,
                signal_batch=signal_batch,
                simulation=SimulationResult(
                    trades=[self._trade("2026-08-04 08:15:00", 50.0)],
                    daily_results=[
                        DailyResult(
                            "2026-08-04",
                            50.0,
                            1,
                            1,
                            False,
                        )
                    ],
                    metrics={"net_profit_usd": 50.0},
                ),
            )

            visible_conn = sqlite3.connect(
                str(result_dir / "results.sqlite3")
            )
            try:
                self.assertEqual(
                    visible_conn.execute(
                        "SELECT COUNT(*) FROM runs"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    visible_conn.execute(
                        "SELECT COUNT(*) FROM hourly_results"
                    ).fetchone()[0],
                    24,
                )
            finally:
                visible_conn.close()

            second_id = store.save_run(
                signal_variant=signal_variant,
                execution_variant=execution_variant,
                signal_batch=signal_batch,
                simulation=SimulationResult(
                    trades=[
                        self._trade(
                            "2026-08-04 09:05:00",
                            100.0,
                            exit_time_msk="2026-08-04 11:30:00",
                        ),
                        self._trade("2026-08-04 09:55:00", -25.0),
                        self._trade("2026-08-04 10:00:00", 10.0),
                    ],
                    daily_results=[
                        DailyResult(
                            "2026-08-04",
                            85.0,
                            3,
                            3,
                            False,
                        )
                    ],
                    metrics={"net_profit_usd": 85.0},
                ),
            )
            store.close()

            self.assertEqual(first_id, 1)
            self.assertEqual(second_id, 2)
            self.assertTrue(
                (result_dir / "results.sqlite3").is_file()
            )

            for filename in (
                "summary.csv",
                "daily_results.csv",
                "hourly_results.csv",
                "signals.csv",
                "trades.csv",
                "signals_trades.sqlite3",
                "signal_time.py",
            ):
                self.assertFalse((result_dir / filename).exists())

            conn = sqlite3.connect(
                str(result_dir / "results.sqlite3")
            )
            try:
                names = {
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type IN ('table', 'view')
                        """
                    ).fetchall()
                }
                self.assertTrue(
                    {
                        "test_settings",
                        "runs",
                        "run_metrics",
                        "signals",
                        "trades",
                        "daily_results",
                        "hourly_results",
                        "summary",
                    }.issubset(names)
                )

                settings = {
                    name: json.loads(value_json)
                    for name, value_json in conn.execute(
                        "SELECT name, value_json FROM test_settings"
                    ).fetchall()
                }
                self.assertEqual(
                    settings["CANDIDATE_MIN_COUNT_VALUES"],
                    [3, 7],
                )
                self.assertEqual(
                    settings["FUT_SEARCH_HOUR_GROUPS_CT"]["15"],
                    [],
                )

                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM runs"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM signals"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM trades"
                    ).fetchone()[0],
                    4,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM daily_results"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM hourly_results"
                    ).fetchone()[0],
                    48,
                )

                self.assertEqual(
                    conn.execute(
                        "SELECT id, net_profit_usd FROM summary"
                    ).fetchall(),
                    [(2, 85.0), (1, 50.0)],
                )

                self.assertEqual(
                    conn.execute(
                        """
                        SELECT
                            net_profit_usd,
                            trades_count,
                            winning_trades_count,
                            losing_trades_count
                        FROM hourly_results
                        WHERE run_id = 2 AND hour_msk = 9
                        """
                    ).fetchone(),
                    (75.0, 2, 1, 1),
                )

                self.assertEqual(
                    conn.execute(
                        """
                        SELECT net_realized_pnl_usd, closed_trades_count
                        FROM daily_results
                        WHERE run_id = 2
                        """
                    ).fetchone(),
                    (85.0, 3),
                )
            finally:
                conn.close()

    @staticmethod
    def _signal() -> TesterSignal:
        return TesterSignal(
            signal_bar_ts=0,
            signal_time_msk="2026-08-04 09:30:00",
            signal_time_ct="2026-08-04 01:30:00",
            direction="LONG",
            reference_price=21000.0,
            best_pearson=0.81,
            best_candidate_score=0.75,
            potential_end_delta_points=15.0,
            potential_max_profit_points=22.0,
            potential_max_drawdown_points=-8.0,
            potential_used=5,
            raw_candidates_count=2000,
            valid_candidates_count=1800,
            pearson_passed_count=7,
            minmax_passed_count=5,
        )

    @staticmethod
    def _trade(
            entry_time_msk: str,
            net_pnl_usd: float,
            *,
            exit_time_msk: str = "2026-08-04 12:00:00",
    ) -> CompletedTrade:
        return CompletedTrade(
            direction="LONG",
            entry_time_msk=entry_time_msk,
            exit_time_msk=exit_time_msk,
            entry_price=100.0,
            exit_price=101.0,
            exit_reason="TEST_END",
            net_pnl_usd=net_pnl_usd,
            mfe_points=1.0,
            mae_points=1.0,
            holding_seconds=60,
        )


if __name__ == "__main__":
    unittest.main()
