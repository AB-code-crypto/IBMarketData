from __future__ import annotations

import json
import sqlite3
from datetime import datetime
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

INTEGER_METRIC_COLUMNS = {
    "trades_count",
    "winning_trades_count",
    "losing_trades_count",
    "max_consecutive_losses",
    "long_trades_count",
    "short_trades_count",
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
}


class ResultStore:
    def __init__(self, result_dir: Path) -> None:
        self.result_dir = Path(result_dir)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.result_dir / "results.sqlite3"

        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self._initialize_database()

        row = self.conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM runs"
        ).fetchone()
        self._next_id = int(row[0])

    def flush(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.flush()
        self.conn.close()

    def _initialize_database(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS test_settings (
                name TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );

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

            CREATE TABLE IF NOT EXISTS run_metrics (
                run_id INTEGER PRIMARY KEY,
                net_profit_usd REAL,
                trades_count INTEGER,
                winning_trades_count INTEGER,
                losing_trades_count INTEGER,
                win_rate REAL,
                profit_factor REAL,
                average_trade_usd REAL,
                median_trade_usd REAL,
                max_drawdown_usd REAL,
                max_consecutive_losses INTEGER,
                long_trades_count INTEGER,
                long_net_profit_usd REAL,
                short_trades_count INTEGER,
                short_net_profit_usd REAL,
                signals_count INTEGER,
                executed_signals_count INTEGER,
                ignored_same_direction_count INTEGER,
                ignored_daily_flat_count INTEGER,
                ignored_daily_take_profit_count INTEGER,
                ignored_missing_execution_bar_count INTEGER,
                ambiguous_tp_sl_bars_count INTEGER,
                take_profit_exits_count INTEGER,
                stop_loss_exits_count INTEGER,
                reverse_exits_count INTEGER,
                daily_flat_exits_count INTEGER,
                daily_take_profit_exits_count INTEGER,
                test_end_exits_count INTEGER,
                average_mfe_points REAL,
                average_mae_points REAL,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
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

            CREATE TABLE IF NOT EXISTS daily_results (
                run_id INTEGER NOT NULL,
                moscow_day TEXT NOT NULL,
                net_realized_pnl_usd REAL NOT NULL,
                closed_trades_count INTEGER NOT NULL,
                executed_signals_count INTEGER NOT NULL,
                daily_take_profit_triggered INTEGER NOT NULL,
                PRIMARY KEY (run_id, moscow_day),
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS hourly_results (
                run_id INTEGER NOT NULL,
                hour_msk INTEGER NOT NULL,
                entry_hour_msk TEXT NOT NULL,
                net_profit_usd REAL NOT NULL,
                trades_count INTEGER NOT NULL,
                winning_trades_count INTEGER NOT NULL,
                losing_trades_count INTEGER NOT NULL,
                flat_trades_count INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                average_trade_usd REAL NOT NULL,
                long_trades_count INTEGER NOT NULL,
                long_net_profit_usd REAL NOT NULL,
                short_trades_count INTEGER NOT NULL,
                short_net_profit_usd REAL NOT NULL,
                PRIMARY KEY (run_id, hour_msk),
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_run_metrics_net_profit
                ON run_metrics(net_profit_usd DESC);
            CREATE INDEX IF NOT EXISTS idx_signals_time_msk
                ON signals(signal_time_msk);
            CREATE INDEX IF NOT EXISTS idx_trades_entry_time_msk
                ON trades(entry_time_msk);
            CREATE INDEX IF NOT EXISTS idx_daily_results_day
                ON daily_results(moscow_day);
            CREATE INDEX IF NOT EXISTS idx_hourly_results_hour
                ON hourly_results(hour_msk);

            CREATE VIEW IF NOT EXISTS summary AS
            SELECT
                runs.id AS id,
                run_metrics.net_profit_usd AS net_profit_usd,
                runs.rolling_back_minutes AS rolling_back_minutes,
                runs.rolling_trade_minutes AS rolling_trade_minutes,
                runs.pearson_min AS pearson_min,
                runs.minmax_hard_filter_max_ratio
                    AS minmax_hard_filter_max_ratio,
                runs.candidate_min_count AS candidate_min_count,
                runs.candidate_max_count AS candidate_max_count,
                runs.potential_min_abs_end_delta_points
                    AS potential_min_abs_end_delta_points,
                runs.delay_seconds AS delay_seconds,
                runs.take_profit_points AS take_profit_points,
                runs.stop_loss_points AS stop_loss_points,
                runs.daily_take_profit_usd AS daily_take_profit_usd,
                run_metrics.trades_count AS trades_count,
                run_metrics.winning_trades_count AS winning_trades_count,
                run_metrics.losing_trades_count AS losing_trades_count,
                run_metrics.win_rate AS win_rate,
                run_metrics.profit_factor AS profit_factor,
                run_metrics.average_trade_usd AS average_trade_usd,
                run_metrics.median_trade_usd AS median_trade_usd,
                run_metrics.max_drawdown_usd AS max_drawdown_usd,
                run_metrics.max_consecutive_losses
                    AS max_consecutive_losses,
                run_metrics.long_trades_count AS long_trades_count,
                run_metrics.long_net_profit_usd AS long_net_profit_usd,
                run_metrics.short_trades_count AS short_trades_count,
                run_metrics.short_net_profit_usd AS short_net_profit_usd,
                run_metrics.signals_count AS signals_count,
                run_metrics.executed_signals_count
                    AS executed_signals_count,
                run_metrics.ignored_same_direction_count
                    AS ignored_same_direction_count,
                run_metrics.ignored_daily_flat_count
                    AS ignored_daily_flat_count,
                run_metrics.ignored_daily_take_profit_count
                    AS ignored_daily_take_profit_count,
                run_metrics.ignored_missing_execution_bar_count
                    AS ignored_missing_execution_bar_count,
                run_metrics.ambiguous_tp_sl_bars_count
                    AS ambiguous_tp_sl_bars_count,
                run_metrics.take_profit_exits_count
                    AS take_profit_exits_count,
                run_metrics.stop_loss_exits_count
                    AS stop_loss_exits_count,
                run_metrics.reverse_exits_count AS reverse_exits_count,
                run_metrics.daily_flat_exits_count
                    AS daily_flat_exits_count,
                run_metrics.daily_take_profit_exits_count
                    AS daily_take_profit_exits_count,
                run_metrics.test_end_exits_count AS test_end_exits_count,
                run_metrics.average_mfe_points AS average_mfe_points,
                run_metrics.average_mae_points AS average_mae_points
            FROM runs
            JOIN run_metrics ON run_metrics.run_id = runs.id
            ORDER BY run_metrics.net_profit_usd DESC, runs.id ASC;
            """
        )
        self.conn.commit()

    def save_test_settings(self, settings: dict[str, Any]) -> None:
        rows = [
            (
                str(name),
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            for name, value in settings.items()
        ]
        with self.conn:
            self.conn.execute("DELETE FROM test_settings")
            self.conn.executemany(
                """
                INSERT INTO test_settings (name, value_json)
                VALUES (?, ?)
                """,
                rows,
            )

    @staticmethod
    def _metric_value(
            name: str,
            metrics: dict[str, Any],
    ) -> int | float | None:
        value = metrics.get(name)
        if value is None:
            return None
        if name in INTEGER_METRIC_COLUMNS:
            return int(value)
        return float(value)

    @staticmethod
    def _build_hourly_rows(
            run_id: int,
            simulation: SimulationResult,
    ) -> list[tuple[Any, ...]]:
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
            hour = datetime.strptime(
                trade.entry_time_msk,
                "%Y-%m-%d %H:%M:%S",
            ).hour
            bucket = buckets[hour]
            pnl = float(trade.net_pnl_usd)
            bucket["net_profit_usd"] += pnl
            bucket["trades_count"] += 1

            if pnl > 0.0:
                bucket["winning_trades_count"] += 1
            elif pnl < 0.0:
                bucket["losing_trades_count"] += 1
            else:
                bucket["flat_trades_count"] += 1

            if trade.direction == "LONG":
                bucket["long_trades_count"] += 1
                bucket["long_net_profit_usd"] += pnl
            elif trade.direction == "SHORT":
                bucket["short_trades_count"] += 1
                bucket["short_net_profit_usd"] += pnl

        rows = []
        for hour, bucket in buckets.items():
            trades_count = int(bucket["trades_count"])
            wins = int(bucket["winning_trades_count"])
            net = float(bucket["net_profit_usd"])
            rows.append(
                (
                    run_id,
                    hour,
                    f"{hour:02d}:00-{hour:02d}:59",
                    net,
                    trades_count,
                    wins,
                    int(bucket["losing_trades_count"]),
                    int(bucket["flat_trades_count"]),
                    wins / trades_count if trades_count else 0.0,
                    net / trades_count if trades_count else 0.0,
                    int(bucket["long_trades_count"]),
                    float(bucket["long_net_profit_usd"]),
                    int(bucket["short_trades_count"]),
                    float(bucket["short_net_profit_usd"]),
                )
            )
        return rows

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
        config = {
            **signal_variant.to_dict(),
            **execution_variant.to_dict(),
        }

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
                    int(config["rolling_back_minutes"]),
                    int(config["rolling_trade_minutes"]),
                    float(config["pearson_min"]),
                    float(config["minmax_hard_filter_max_ratio"]),
                    int(config["candidate_min_count"]),
                    int(config["candidate_max_count"]),
                    float(config["potential_min_abs_end_delta_points"]),
                    int(config["delay_seconds"]),
                    float(config["take_profit_points"]),
                    float(config["stop_loss_points"]),
                    float(config["daily_take_profit_usd"]),
                ),
            )

            metric_columns = ["run_id", *RUN_METRIC_COLUMNS]
            metric_values = [
                run_id,
                *[
                    self._metric_value(name, simulation.metrics)
                    for name in RUN_METRIC_COLUMNS
                ],
            ]
            self.conn.execute(
                f"""
                INSERT INTO run_metrics ({", ".join(metric_columns)})
                VALUES ({", ".join("?" for _ in metric_columns)})
                """,
                metric_values,
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
                (
                    (
                        run_id,
                        index,
                        signal.signal_time_msk,
                        signal.signal_time_ct,
                        signal.direction,
                        float(signal.reference_price),
                        float(signal.best_pearson),
                        (
                            None
                            if signal.best_candidate_score is None
                            else float(signal.best_candidate_score)
                        ),
                        float(signal.potential_end_delta_points),
                        float(signal.potential_max_profit_points),
                        float(signal.potential_max_drawdown_points),
                        int(signal.potential_used),
                        int(signal.raw_candidates_count),
                        int(signal.valid_candidates_count),
                        int(signal.pearson_passed_count),
                        int(signal.minmax_passed_count),
                    )
                    for index, signal in enumerate(
                        signal_batch.signals,
                        start=1,
                    )
                ),
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
                (
                    (
                        run_id,
                        index,
                        trade.direction,
                        trade.entry_time_msk,
                        trade.exit_time_msk,
                        float(trade.entry_price),
                        float(trade.exit_price),
                        trade.exit_reason,
                        float(trade.net_pnl_usd),
                        float(trade.mfe_points),
                        float(trade.mae_points),
                        int(trade.holding_seconds),
                    )
                    for index, trade in enumerate(
                        simulation.trades,
                        start=1,
                    )
                ),
            )

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
                (
                    (
                        run_id,
                        row.moscow_day,
                        float(row.net_realized_pnl_usd),
                        int(row.closed_trades_count),
                        int(row.executed_signals_count),
                        int(row.daily_take_profit_triggered),
                    )
                    for row in simulation.daily_results
                ),
            )

            self.conn.executemany(
                """
                INSERT INTO hourly_results (
                    run_id,
                    hour_msk,
                    entry_hour_msk,
                    net_profit_usd,
                    trades_count,
                    winning_trades_count,
                    losing_trades_count,
                    flat_trades_count,
                    win_rate,
                    average_trade_usd,
                    long_trades_count,
                    long_net_profit_usd,
                    short_trades_count,
                    short_net_profit_usd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._build_hourly_rows(run_id, simulation),
            )

        self.flush()
        return run_id


__all__ = ["ResultStore"]
