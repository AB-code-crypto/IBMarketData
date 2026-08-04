from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tester.models import (
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    SimulationResult,
)


RUN_METRIC_COLUMNS = [
    "net_profit_usd",
    "gross_trade_pnl_usd",
    "gross_profit_usd",
    "gross_loss_usd",
    "total_commission_usd",
    "trades_count",
    "winning_trades_count",
    "losing_trades_count",
    "win_rate",
    "profit_factor",
    "average_trade_usd",
    "median_trade_usd",
    "max_drawdown_usd",
    "max_consecutive_losses",
    "long_trades_count",
    "long_net_profit_usd",
    "short_trades_count",
    "short_net_profit_usd",
    "signals_count",
    "executed_signals_count",
    "ignored_same_direction_count",
    "ignored_daily_flat_count",
    "ignored_daily_take_profit_count",
    "ignored_missing_execution_bar_count",
    "ambiguous_tp_sl_bars_count",
    "take_profit_exits_count",
    "stop_loss_exits_count",
    "reverse_exits_count",
    "daily_flat_exits_count",
    "daily_take_profit_exits_count",
    "test_end_exits_count",
    "average_mfe_points",
    "average_mae_points",
]


class ResultStore:
    def __init__(self, result_dir: Path) -> None:
        self.result_dir = Path(result_dir)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.result_dir / "results.sqlite3"
        self.summary_path = self.result_dir / "summary.csv"
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._initialize_schema()

    def close(self) -> None:
        self.conn.close()

    def _initialize_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                git_commit TEXT NOT NULL,
                price_db_path TEXT NOT NULL,
                price_db_size INTEGER NOT NULL,
                price_db_mtime_ns INTEGER NOT NULL,
                price_rows_count INTEGER NOT NULL,
                price_min_ts INTEGER,
                price_max_ts INTEGER,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                signal_config_json TEXT NOT NULL,
                execution_config_json TEXT NOT NULL,
                commission_per_contract_side_usd REAL NOT NULL,
                multiplier_usd_per_point REAL NOT NULL,
                calculation_points INTEGER NOT NULL,
                skipped_points INTEGER NOT NULL,
                no_signal_points INTEGER NOT NULL,
                elapsed_seconds REAL NOT NULL,
                metrics_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                run_id INTEGER NOT NULL,
                signal_index INTEGER NOT NULL,
                signal_bar_ts INTEGER NOT NULL,
                signal_time_msk TEXT NOT NULL,
                signal_time_ct TEXT NOT NULL,
                direction TEXT NOT NULL,
                reference_price REAL NOT NULL,
                best_pearson REAL NOT NULL,
                best_candidate_score REAL,
                potential_end_delta_points REAL NOT NULL,
                potential_max_profit_points REAL NOT NULL,
                potential_max_drawdown_points REAL NOT NULL,
                potential_used INTEGER NOT NULL,
                raw_candidates_count INTEGER NOT NULL,
                valid_candidates_count INTEGER NOT NULL,
                pearson_passed_count INTEGER NOT NULL,
                minmax_passed_count INTEGER NOT NULL,
                PRIMARY KEY (run_id, signal_index),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                run_id INTEGER NOT NULL,
                trade_index INTEGER NOT NULL,
                direction TEXT NOT NULL,
                entry_ts INTEGER NOT NULL,
                exit_ts INTEGER NOT NULL,
                entry_time_msk TEXT NOT NULL,
                exit_time_msk TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                entry_signal_bar_ts INTEGER NOT NULL,
                exit_signal_bar_ts INTEGER,
                exit_reason TEXT NOT NULL,
                gross_points REAL NOT NULL,
                gross_pnl_usd REAL NOT NULL,
                entry_commission_usd REAL NOT NULL,
                exit_commission_usd REAL NOT NULL,
                net_pnl_usd REAL NOT NULL,
                mfe_points REAL NOT NULL,
                mae_points REAL NOT NULL,
                holding_seconds INTEGER NOT NULL,
                PRIMARY KEY (run_id, trade_index),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS daily_results (
                run_id INTEGER NOT NULL,
                moscow_day TEXT NOT NULL,
                net_realized_pnl_usd REAL NOT NULL,
                commission_usd REAL NOT NULL,
                closed_trades_count INTEGER NOT NULL,
                executed_signals_count INTEGER NOT NULL,
                daily_take_profit_triggered INTEGER NOT NULL,
                PRIMARY KEY (run_id, moscow_day),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );
            """
        )
        self.conn.commit()

    def save_run(
            self,
            *,
            git_commit: str,
            price_db_metadata: dict[str, Any],
            start_ts: int,
            end_ts: int,
            signal_variant: SignalVariant,
            execution_variant: ExecutionVariant,
            commission_per_contract_side_usd: float,
            multiplier_usd_per_point: float,
            signal_batch: SignalBatchResult,
            simulation: SimulationResult,
            elapsed_seconds: float,
    ) -> int:
        metrics_json = json.dumps(
            simulation.metrics,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        cursor = self.conn.execute(
            """
            INSERT INTO runs (
                created_at_utc,
                git_commit,
                price_db_path,
                price_db_size,
                price_db_mtime_ns,
                price_rows_count,
                price_min_ts,
                price_max_ts,
                start_ts,
                end_ts,
                signal_config_json,
                execution_config_json,
                commission_per_contract_side_usd,
                multiplier_usd_per_point,
                calculation_points,
                skipped_points,
                no_signal_points,
                elapsed_seconds,
                metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                str(git_commit),
                str(price_db_metadata["path"]),
                int(price_db_metadata["size"]),
                int(price_db_metadata["mtime_ns"]),
                int(price_db_metadata["rows_count"]),
                price_db_metadata["min_ts"],
                price_db_metadata["max_ts"],
                int(start_ts),
                int(end_ts),
                json.dumps(signal_variant.to_dict(), ensure_ascii=False, sort_keys=True),
                json.dumps(
                    execution_variant.to_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                float(commission_per_contract_side_usd),
                float(multiplier_usd_per_point),
                int(signal_batch.calculation_points),
                int(signal_batch.skipped_points),
                int(signal_batch.no_signal_points),
                float(elapsed_seconds),
                metrics_json,
            ),
        )
        run_id = int(cursor.lastrowid)

        self.conn.executemany(
            """
            INSERT INTO signals (
                run_id,
                signal_index,
                signal_bar_ts,
                signal_time_msk,
                signal_time_ct,
                direction,
                reference_price,
                best_pearson,
                best_candidate_score,
                potential_end_delta_points,
                potential_max_profit_points,
                potential_max_drawdown_points,
                potential_used,
                raw_candidates_count,
                valid_candidates_count,
                pearson_passed_count,
                minmax_passed_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    index,
                    signal.signal_bar_ts,
                    signal.signal_time_msk,
                    signal.signal_time_ct,
                    signal.direction,
                    signal.reference_price,
                    signal.best_pearson,
                    signal.best_candidate_score,
                    signal.potential_end_delta_points,
                    signal.potential_max_profit_points,
                    signal.potential_max_drawdown_points,
                    signal.potential_used,
                    signal.raw_candidates_count,
                    signal.valid_candidates_count,
                    signal.pearson_passed_count,
                    signal.minmax_passed_count,
                )
                for index, signal in enumerate(signal_batch.signals, start=1)
            ],
        )

        self.conn.executemany(
            """
            INSERT INTO trades (
                run_id,
                trade_index,
                direction,
                entry_ts,
                exit_ts,
                entry_time_msk,
                exit_time_msk,
                entry_price,
                exit_price,
                entry_signal_bar_ts,
                exit_signal_bar_ts,
                exit_reason,
                gross_points,
                gross_pnl_usd,
                entry_commission_usd,
                exit_commission_usd,
                net_pnl_usd,
                mfe_points,
                mae_points,
                holding_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    index,
                    trade.direction,
                    trade.entry_ts,
                    trade.exit_ts,
                    trade.entry_time_msk,
                    trade.exit_time_msk,
                    trade.entry_price,
                    trade.exit_price,
                    trade.entry_signal_bar_ts,
                    trade.exit_signal_bar_ts,
                    trade.exit_reason,
                    trade.gross_points,
                    trade.gross_pnl_usd,
                    trade.entry_commission_usd,
                    trade.exit_commission_usd,
                    trade.net_pnl_usd,
                    trade.mfe_points,
                    trade.mae_points,
                    trade.holding_seconds,
                )
                for index, trade in enumerate(simulation.trades, start=1)
            ],
        )

        self.conn.executemany(
            """
            INSERT INTO daily_results (
                run_id,
                moscow_day,
                net_realized_pnl_usd,
                commission_usd,
                closed_trades_count,
                executed_signals_count,
                daily_take_profit_triggered
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row.moscow_day,
                    row.net_realized_pnl_usd,
                    row.commission_usd,
                    row.closed_trades_count,
                    row.executed_signals_count,
                    int(row.daily_take_profit_triggered),
                )
                for row in simulation.daily_results
            ],
        )
        self.conn.commit()

        self._append_summary(
            run_id=run_id,
            signal_variant=signal_variant,
            execution_variant=execution_variant,
            signal_batch=signal_batch,
            metrics=simulation.metrics,
            elapsed_seconds=elapsed_seconds,
        )
        return run_id

    def _append_summary(
            self,
            *,
            run_id: int,
            signal_variant: SignalVariant,
            execution_variant: ExecutionVariant,
            signal_batch: SignalBatchResult,
            metrics: dict[str, Any],
            elapsed_seconds: float,
    ) -> None:
        signal_values = signal_variant.to_dict()
        execution_values = execution_variant.to_dict()
        row = {
            "run_id": run_id,
            **signal_values,
            **execution_values,
            "calculation_points": signal_batch.calculation_points,
            "skipped_points": signal_batch.skipped_points,
            "no_signal_points": signal_batch.no_signal_points,
            "elapsed_seconds": elapsed_seconds,
            **{name: metrics.get(name) for name in RUN_METRIC_COLUMNS},
        }
        fieldnames = list(row.keys())
        file_exists = self.summary_path.is_file()
        with self.summary_path.open("a", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


__all__ = ["ResultStore"]
