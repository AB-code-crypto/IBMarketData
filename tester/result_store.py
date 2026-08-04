from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from tester.models import (
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    SimulationResult,
)


RUN_METRIC_COLUMNS = [
    "net_profit_usd",
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
        self.signals_path = self.result_dir / "signals.csv"
        self.trades_path = self.result_dir / "trades.csv"
        self.daily_results_path = self.result_dir / "daily_results.csv"

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
                entry_time_msk TEXT NOT NULL,
                exit_time_msk TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                exit_reason TEXT NOT NULL,
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
                closed_trades_count INTEGER NOT NULL,
                executed_signals_count INTEGER NOT NULL,
                daily_take_profit_triggered INTEGER NOT NULL,
                PRIMARY KEY (run_id, moscow_day),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );
            """
        )
        self.conn.commit()

    @staticmethod
    def _append_csv_rows(
            path: Path,
            *,
            fieldnames: list[str],
            rows: Iterable[dict[str, Any]],
    ) -> None:
        materialized = list(rows)
        file_exists = path.is_file()
        with path.open("a", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(materialized)

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
            signal_batch: SignalBatchResult,
            simulation: SimulationResult,
            elapsed_seconds: float,
    ) -> int:
        signal_values = signal_variant.to_dict()
        execution_values = execution_variant.to_dict()
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
                calculation_points,
                skipped_points,
                no_signal_points,
                elapsed_seconds,
                metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(signal_values, ensure_ascii=False, sort_keys=True),
                json.dumps(execution_values, ensure_ascii=False, sort_keys=True),
                float(commission_per_contract_side_usd),
                int(signal_batch.calculation_points),
                int(signal_batch.skipped_points),
                int(signal_batch.no_signal_points),
                float(elapsed_seconds),
                metrics_json,
            ),
        )
        run_id = int(cursor.lastrowid)

        signal_db_rows = [
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
        ]
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
            signal_db_rows,
        )

        trade_db_rows = [
            (
                run_id,
                index,
                trade.direction,
                trade.entry_time_msk,
                trade.exit_time_msk,
                trade.entry_price,
                trade.exit_price,
                trade.exit_reason,
                trade.net_pnl_usd,
                trade.mfe_points,
                trade.mae_points,
                trade.holding_seconds,
            )
            for index, trade in enumerate(simulation.trades, start=1)
        ]
        self.conn.executemany(
            """
            INSERT INTO trades (
                run_id,
                trade_index,
                direction,
                entry_time_msk,
                exit_time_msk,
                entry_price,
                exit_price,
                exit_reason,
                net_pnl_usd,
                mfe_points,
                mae_points,
                holding_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            trade_db_rows,
        )

        daily_db_rows = [
            (
                run_id,
                row.moscow_day,
                row.net_realized_pnl_usd,
                row.closed_trades_count,
                row.executed_signals_count,
                int(row.daily_take_profit_triggered),
            )
            for row in simulation.daily_results
        ]
        self.conn.executemany(
            """
            INSERT INTO daily_results (
                run_id,
                moscow_day,
                net_realized_pnl_usd,
                closed_trades_count,
                executed_signals_count,
                daily_take_profit_triggered
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            daily_db_rows,
        )
        self.conn.commit()

        common_config = {
            "run_id": run_id,
            **signal_values,
            **execution_values,
            "commission_per_contract_side_usd": float(
                commission_per_contract_side_usd
            ),
        }

        self._append_summary(
            common_config=common_config,
            signal_batch=signal_batch,
            metrics=simulation.metrics,
            elapsed_seconds=elapsed_seconds,
        )
        self._append_signals_csv(
            common_config=common_config,
            signal_batch=signal_batch,
        )
        self._append_trades_csv(
            common_config=common_config,
            simulation=simulation,
        )
        self._append_daily_results_csv(
            common_config=common_config,
            simulation=simulation,
        )
        return run_id

    def _append_summary(
            self,
            *,
            common_config: dict[str, Any],
            signal_batch: SignalBatchResult,
            metrics: dict[str, Any],
            elapsed_seconds: float,
    ) -> None:
        row = {
            **common_config,
            "calculation_points": signal_batch.calculation_points,
            "skipped_points": signal_batch.skipped_points,
            "no_signal_points": signal_batch.no_signal_points,
            "elapsed_seconds": elapsed_seconds,
            **{name: metrics.get(name) for name in RUN_METRIC_COLUMNS},
        }
        self._append_csv_rows(
            self.summary_path,
            fieldnames=list(row.keys()),
            rows=[row],
        )

    def _append_signals_csv(
            self,
            *,
            common_config: dict[str, Any],
            signal_batch: SignalBatchResult,
    ) -> None:
        rows = [
            {
                **common_config,
                "signal_index": index,
                "signal_time_msk": signal.signal_time_msk,
                "signal_time_ct": signal.signal_time_ct,
                "direction": signal.direction,
                "reference_price": signal.reference_price,
                "best_pearson": signal.best_pearson,
                "best_candidate_score": signal.best_candidate_score,
                "potential_end_delta_points": signal.potential_end_delta_points,
                "potential_max_profit_points": signal.potential_max_profit_points,
                "potential_max_drawdown_points": (
                    signal.potential_max_drawdown_points
                ),
                "potential_used": signal.potential_used,
                "raw_candidates_count": signal.raw_candidates_count,
                "valid_candidates_count": signal.valid_candidates_count,
                "pearson_passed_count": signal.pearson_passed_count,
                "minmax_passed_count": signal.minmax_passed_count,
            }
            for index, signal in enumerate(signal_batch.signals, start=1)
        ]
        fieldnames = list(common_config.keys()) + [
            "signal_index",
            "signal_time_msk",
            "signal_time_ct",
            "direction",
            "reference_price",
            "best_pearson",
            "best_candidate_score",
            "potential_end_delta_points",
            "potential_max_profit_points",
            "potential_max_drawdown_points",
            "potential_used",
            "raw_candidates_count",
            "valid_candidates_count",
            "pearson_passed_count",
            "minmax_passed_count",
        ]
        self._append_csv_rows(
            self.signals_path,
            fieldnames=fieldnames,
            rows=rows,
        )

    def _append_trades_csv(
            self,
            *,
            common_config: dict[str, Any],
            simulation: SimulationResult,
    ) -> None:
        rows = [
            {
                **common_config,
                "trade_index": index,
                "direction": trade.direction,
                "entry_time_msk": trade.entry_time_msk,
                "exit_time_msk": trade.exit_time_msk,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "exit_reason": trade.exit_reason,
                "net_pnl_usd": trade.net_pnl_usd,
                "mfe_points": trade.mfe_points,
                "mae_points": trade.mae_points,
                "holding_seconds": trade.holding_seconds,
            }
            for index, trade in enumerate(simulation.trades, start=1)
        ]
        fieldnames = list(common_config.keys()) + [
            "trade_index",
            "direction",
            "entry_time_msk",
            "exit_time_msk",
            "entry_price",
            "exit_price",
            "exit_reason",
            "net_pnl_usd",
            "mfe_points",
            "mae_points",
            "holding_seconds",
        ]
        self._append_csv_rows(
            self.trades_path,
            fieldnames=fieldnames,
            rows=rows,
        )

    def _append_daily_results_csv(
            self,
            *,
            common_config: dict[str, Any],
            simulation: SimulationResult,
    ) -> None:
        rows = [
            {
                **common_config,
                "moscow_day": row.moscow_day,
                "net_realized_pnl_usd": row.net_realized_pnl_usd,
                "closed_trades_count": row.closed_trades_count,
                "executed_signals_count": row.executed_signals_count,
                "daily_take_profit_triggered": int(
                    row.daily_take_profit_triggered
                ),
            }
            for row in simulation.daily_results
        ]
        fieldnames = list(common_config.keys()) + [
            "moscow_day",
            "net_realized_pnl_usd",
            "closed_trades_count",
            "executed_signals_count",
            "daily_take_profit_triggered",
        ]
        self._append_csv_rows(
            self.daily_results_path,
            fieldnames=fieldnames,
            rows=rows,
        )


__all__ = ["ResultStore"]
