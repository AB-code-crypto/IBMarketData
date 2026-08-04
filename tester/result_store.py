from __future__ import annotations

import csv
import os
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

        self.summary_path = self.result_dir / "summary.csv"
        self.signals_path = self.result_dir / "signals.csv"
        self.trades_path = self.result_dir / "trades.csv"
        self.daily_results_path = self.result_dir / "daily_results.csv"
        self.hourly_results_path = self.result_dir / "hourly_results.csv"

        self._next_id = 1
        self._summary_rows: list[dict[str, Any]] = []

    def close(self) -> None:
        # Оставлено для совместимости с текущим run_tester.py.
        return None

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
        run_id = self._next_id
        self._next_id += 1

        signal_values = signal_variant.to_dict()
        execution_values = execution_variant.to_dict()
        common_config = {
            "id": run_id,
            **signal_values,
            **execution_values,
            "commission_per_contract_side_usd": float(
                commission_per_contract_side_usd
            ),
        }

        self._append_summary(
            common_config=common_config,
            git_commit=git_commit,
            price_db_metadata=price_db_metadata,
            start_ts=start_ts,
            end_ts=end_ts,
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
        self._append_hourly_results_csv(
            common_config=common_config,
            simulation=simulation,
        )
        return run_id

    def _append_summary(
            self,
            *,
            common_config: dict[str, Any],
            git_commit: str,
            price_db_metadata: dict[str, Any],
            start_ts: int,
            end_ts: int,
            signal_batch: SignalBatchResult,
            metrics: dict[str, Any],
            elapsed_seconds: float,
    ) -> None:
        settings = self._settings_only(common_config)
        row = {
            "id": common_config["id"],
            "net_profit_usd": metrics.get("net_profit_usd"),
            **settings,
            "created_at_utc": datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "git_commit": str(git_commit),
            "price_db_path": str(price_db_metadata["path"]),
            "price_db_size": int(price_db_metadata["size"]),
            "price_db_mtime_ns": int(price_db_metadata["mtime_ns"]),
            "price_rows_count": int(price_db_metadata["rows_count"]),
            "price_min_ts": price_db_metadata["min_ts"],
            "price_max_ts": price_db_metadata["max_ts"],
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
            "calculation_points": signal_batch.calculation_points,
            "skipped_points": signal_batch.skipped_points,
            "no_signal_points": signal_batch.no_signal_points,
            "elapsed_seconds": elapsed_seconds,
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
