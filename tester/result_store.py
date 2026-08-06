from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime
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

        self.summary_path = self.result_dir / "summary.csv"
        self.daily_results_path = self.result_dir / "daily_results.csv"
        self.hourly_results_path = self.result_dir / "hourly_results.csv"
        self.details_db_path = self.result_dir / "signals_trades.sqlite3"

        self._next_id = 1
        self._summary_rows: list[dict[str, Any]] = []

        self.conn = sqlite3.connect(str(self.details_db_path))
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self._initialize_database()

    def flush(self) -> None:
        # SQLite-транзакции сохраняются внутри save_run, но явный commit
        # фиксирует контракт: после возврата из save_run результат уже
        # доступен другому процессу и переживёт штатную остановку тестера.
        self.conn.commit()

    def close(self) -> None:
        self.flush()
        self.conn.close()

    def _initialize_database(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY,
                rolling_back_minutes INTEGER NOT NULL,
                rolling_trade_minutes INTEGER NOT NULL,
                pearson_min REAL NOT NULL,
                minmax_hard_filter_max_ratio REAL NOT NULL,
                candidate_min_count INTEGER NOT NULL,
                candidate_max_count INTEGER NOT NULL,
                potential_min_abs_end_delta_points REAL NOT NULL,
                delay_seconds INTEGER NOT NULL,
                take_profit_points REAL NOT NULL,
                stop_loss_points REAL NOT NULL,
                daily_take_profit_usd REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                run_id INTEGER NOT NULL,
                signal_index INTEGER NOT NULL,
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
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
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
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_signals_time_msk
                ON signals(signal_time_msk);

            CREATE INDEX IF NOT EXISTS idx_trades_entry_time_msk
                ON trades(entry_time_msk);
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
            file.flush()
            os.fsync(file.fileno())

    @staticmethod
    def _write_csv_rows_atomic(
            path: Path,
            *,
            fieldnames: list[str],
            rows: Iterable[dict[str, Any]],
    ) -> None:
        temporary_path = path.with_name(f"{path.name}.tmp")
        with temporary_path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)

    @staticmethod
    def _settings_only(common_config: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in common_config.items()
            if key != "id"
        }

    def save_run(
            self,
            *,
            signal_variant: SignalVariant,
            execution_variant: ExecutionVariant,
            signal_batch: SignalBatchResult,
            simulation: SimulationResult,
    ) -> int:
        run_id = self._next_id
        self._next_id += 1

        common_config = {
            "id": run_id,
            **signal_variant.to_dict(),
            **execution_variant.to_dict(),
        }

        self._save_details_to_database(
            common_config=common_config,
            signal_batch=signal_batch,
            simulation=simulation,
        )
        self._append_summary(
            common_config=common_config,
            metrics=simulation.metrics,
        )
        self._append_daily_results_csv(
            common_config=common_config,
            simulation=simulation,
        )
        self._append_hourly_results_csv(
            common_config=common_config,
            simulation=simulation,
        )
        self.flush()
        return run_id

    def _save_details_to_database(
            self,
            *,
            common_config: dict[str, Any],
            signal_batch: SignalBatchResult,
            simulation: SimulationResult,
    ) -> None:
        run_id = int(common_config["id"])
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO runs (
                    id,
                    rolling_back_minutes,
                    rolling_trade_minutes,
                    pearson_min,
                    minmax_hard_filter_max_ratio,
                    candidate_min_count,
                    candidate_max_count,
                    potential_min_abs_end_delta_points,
                    delay_seconds,
                    take_profit_points,
                    stop_loss_points,
                    daily_take_profit_usd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(common_config["rolling_back_minutes"]),
                    int(common_config["rolling_trade_minutes"]),
                    float(common_config["pearson_min"]),
                    float(common_config["minmax_hard_filter_max_ratio"]),
                    int(common_config["candidate_min_count"]),
                    int(common_config["candidate_max_count"]),
                    float(common_config["potential_min_abs_end_delta_points"]),
                    int(common_config["delay_seconds"]),
                    float(common_config["take_profit_points"]),
                    float(common_config["stop_loss_points"]),
                    float(common_config["daily_take_profit_usd"]),
                ),
            )

            self.conn.executemany(
                """
                INSERT INTO signals (
                    run_id,
                    signal_index,
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        index,
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
                    for index, signal in enumerate(
                        signal_batch.signals,
                        start=1,
                    )
                ],
            )

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
                [
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
                    for index, trade in enumerate(
                        simulation.trades,
                        start=1,
                    )
                ],
            )

    def _append_summary(
            self,
            *,
            common_config: dict[str, Any],
            metrics: dict[str, Any],
    ) -> None:
        settings = self._settings_only(common_config)
        row = {
            "id": common_config["id"],
            "net_profit_usd": metrics.get("net_profit_usd"),
            **settings,
            **{
                name: metrics.get(name)
                for name in RUN_METRIC_COLUMNS
                if name != "net_profit_usd"
            },
        }
        self._summary_rows.append(row)
        sorted_rows = sorted(
            self._summary_rows,
            key=lambda item: (
                -float(item["net_profit_usd"] or 0.0),
                int(item["id"]),
            ),
        )
        self._write_csv_rows_atomic(
            self.summary_path,
            fieldnames=list(row.keys()),
            rows=sorted_rows,
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

    def _append_hourly_results_csv(
            self,
            *,
            common_config: dict[str, Any],
            simulation: SimulationResult,
    ) -> None:
        buckets = {
            hour: {
                "net_profit_usd": 0.0,
                "trades_count": 0,
                "winning_trades_count": 0,
                "losing_trades_count": 0,
                "flat_trades_count": 0,
                "long_trades_count": 0,
                "long_net_profit_usd": 0.0,
                "short_trades_count": 0,
                "short_net_profit_usd": 0.0,
            }
            for hour in range(24)
        }

        for trade in simulation.trades:
            entry_hour = datetime.strptime(
                trade.entry_time_msk,
                "%Y-%m-%d %H:%M:%S",
            ).hour
            bucket = buckets[entry_hour]
            net_pnl_usd = float(trade.net_pnl_usd)

            bucket["net_profit_usd"] += net_pnl_usd
            bucket["trades_count"] += 1
            if net_pnl_usd > 0.0:
                bucket["winning_trades_count"] += 1
            elif net_pnl_usd < 0.0:
                bucket["losing_trades_count"] += 1
            else:
                bucket["flat_trades_count"] += 1

            if trade.direction == "LONG":
                bucket["long_trades_count"] += 1
                bucket["long_net_profit_usd"] += net_pnl_usd
            elif trade.direction == "SHORT":
                bucket["short_trades_count"] += 1
                bucket["short_net_profit_usd"] += net_pnl_usd

        settings = self._settings_only(common_config)
        rows = []
        for hour in range(24):
            bucket = buckets[hour]
            trades_count = int(bucket["trades_count"])
            winning_trades_count = int(bucket["winning_trades_count"])
            rows.append(
                {
                    "id": common_config["id"],
                    "net_profit_usd": float(bucket["net_profit_usd"]),
                    **settings,
                    "hour_msk": hour,
                    "entry_hour_msk": f"{hour:02d}:00-{hour:02d}:59",
                    "trades_count": trades_count,
                    "winning_trades_count": winning_trades_count,
                    "losing_trades_count": int(
                        bucket["losing_trades_count"]
                    ),
                    "flat_trades_count": int(bucket["flat_trades_count"]),
                    "win_rate": (
                        winning_trades_count / trades_count
                        if trades_count
                        else 0.0
                    ),
                    "average_trade_usd": (
                        float(bucket["net_profit_usd"]) / trades_count
                        if trades_count
                        else 0.0
                    ),
                    "long_trades_count": int(bucket["long_trades_count"]),
                    "long_net_profit_usd": float(
                        bucket["long_net_profit_usd"]
                    ),
                    "short_trades_count": int(bucket["short_trades_count"]),
                    "short_net_profit_usd": float(
                        bucket["short_net_profit_usd"]
                    ),
                }
            )

        self._append_csv_rows(
            self.hourly_results_path,
            fieldnames=list(rows[0].keys()),
            rows=rows,
        )


__all__ = ["ResultStore"]
