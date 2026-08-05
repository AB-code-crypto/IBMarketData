from __future__ import annotations

import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tester.models import (
    CompletedTrade,
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    SimulationResult,
    TesterSignal,
)
from tester.result_store import ResultStore


FORBIDDEN_COLUMNS = {
    "commission_per_contract_side_usd",
    "created_at_utc",
    "git_commit",
    "price_db_path",
    "price_db_size",
    "price_db_mtime_ns",
    "price_rows_count",
    "price_min_ts",
    "price_max_ts",
    "start_ts",
    "end_ts",
    "elapsed_seconds",
    "calculation_points",
    "skipped_points",
    "no_signal_points",
}


class ResultStoreTest(unittest.TestCase):
    def test_summary_is_sorted_and_details_are_stored_in_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            result_dir = Path(temporary_dir)
            store = ResultStore(result_dir)
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
                    daily_results=[],
                    metrics={"net_profit_usd": 50.0},
                ),
            )
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
                    daily_results=[],
                    metrics={"net_profit_usd": 85.0},
                ),
            )
            store.close()

            self.assertEqual(first_id, 1)
            self.assertEqual(second_id, 2)
            self.assertFalse((result_dir / "signals.csv").exists())
            self.assertFalse((result_dir / "trades.csv").exists())

            details_path = result_dir / "signals_trades.sqlite3"
            self.assertTrue(details_path.is_file())
            conn = sqlite3.connect(str(details_path))
            try:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0],
                    4,
                )

                run_row = conn.execute(
                    """
                    SELECT rolling_back_minutes, delay_seconds
                    FROM runs
                    WHERE id = 2
                    """
                ).fetchone()
                self.assertEqual(run_row, (90, 10))

                signal_row = conn.execute(
                    """
                    SELECT signal_time_msk, direction, best_pearson
                    FROM signals
                    WHERE run_id = 2 AND signal_index = 1
                    """
                ).fetchone()
                self.assertEqual(
                    signal_row,
                    ("2026-08-04 09:30:00", "LONG", 0.81),
                )

                trade_rows = conn.execute(
                    """
                    SELECT entry_time_msk, net_pnl_usd
                    FROM trades
                    WHERE run_id = 2
                    ORDER BY trade_index
                    """
                ).fetchall()
                self.assertEqual(
                    trade_rows,
                    [
                        ("2026-08-04 09:05:00", 100.0),
                        ("2026-08-04 09:55:00", -25.0),
                        ("2026-08-04 10:00:00", 10.0),
                    ],
                )

                for table_name in ("runs", "signals", "trades"):
                    columns = [
                        str(row[1])
                        for row in conn.execute(
                            f"PRAGMA table_info({table_name})"
                        ).fetchall()
                    ]
                    self._assert_clean_columns(columns)
            finally:
                conn.close()

            with (result_dir / "summary.csv").open(
                newline="", encoding="utf-8-sig"
            ) as file:
                reader = csv.DictReader(file)
                self.assertEqual(reader.fieldnames[:2], ["id", "net_profit_usd"])
                rows = list(reader)
            self.assertEqual([row["id"] for row in rows], ["2", "1"])
            self.assertEqual(rows[0]["net_profit_usd"], "85.0")

            with (result_dir / "hourly_results.csv").open(
                newline="", encoding="utf-8-sig"
            ) as file:
                hourly_rows = list(csv.DictReader(file))
            second_run_rows = [row for row in hourly_rows if row["id"] == "2"]
            self.assertEqual(len(second_run_rows), 24)

            hour_9 = next(row for row in second_run_rows if row["hour_msk"] == "9")
            self.assertEqual(float(hour_9["net_profit_usd"]), 75.0)
            self.assertEqual(int(hour_9["trades_count"]), 2)

            hour_10 = next(
                row for row in second_run_rows if row["hour_msk"] == "10"
            )
            self.assertEqual(float(hour_10["net_profit_usd"]), 10.0)
            self.assertEqual(int(hour_10["trades_count"]), 1)

            hour_11 = next(
                row for row in second_run_rows if row["hour_msk"] == "11"
            )
            self.assertEqual(float(hour_11["net_profit_usd"]), 0.0)
            self.assertEqual(int(hour_11["trades_count"]), 0)

            for filename in (
                "summary.csv",
                "daily_results.csv",
                "hourly_results.csv",
            ):
                with (result_dir / filename).open(
                    newline="", encoding="utf-8-sig"
                ) as file:
                    reader = csv.DictReader(file)
                    self._assert_clean_columns(reader.fieldnames)

    def _assert_clean_columns(self, fieldnames: list[str] | None) -> None:
        self.assertIsNotNone(fieldnames)
        names = set(fieldnames or [])
        self.assertFalse(names & FORBIDDEN_COLUMNS)
        self.assertFalse(any(name.endswith("_ts") for name in names))

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
