from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from ibmd.signal.domain.calculation import (
    CandidateWindow,
    SignalWindow,
    expected_points,
)
from ibmd.signal.domain.inputs import (
    SignalPatternInputs,
    SignalSourceBar,
)

_REQUIRED_OBJECTS = {
    ("view", "public_market_bars_v1"),
    ("view", "public_market_data_latest_v1"),
}


class SignalMarketDataError(RuntimeError):
    pass


class SignalDataNotReadyError(SignalMarketDataError):
    pass


class SQLiteSignalMarketDataReader:
    def __init__(
        self,
        database_path: str | Path,
        *,
        instrument_id: str,
        price_precision: int,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.instrument_id = str(instrument_id or "").strip()
        if not self.instrument_id:
            raise ValueError("instrument_id is required")
        self.price_precision = int(price_precision)
        if self.price_precision < 0:
            raise ValueError("price_precision must be non-negative")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self._ct = ZoneInfo("America/Chicago")

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise SignalMarketDataError(
                f"target market-data database does not exist: "
                f"{self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        connection.execute("PRAGMA temp_store = MEMORY")
        return connection

    @property
    def _mid_sql(self) -> str:
        return (
            "ROUND((bid_close + ask_close) / 2.0, "
            f"{self.price_precision})"
        )

    def validate_schema(self) -> None:
        connection = self._connect()
        try:
            objects = {
                (str(row["type"]), str(row["name"]))
                for row in connection.execute(
                    """
                    SELECT type, name
                    FROM sqlite_master
                    WHERE type IN ('table', 'view')
                    """
                ).fetchall()
            }
            missing = sorted(_REQUIRED_OBJECTS - objects)
            if missing:
                raise SignalMarketDataError(
                    f"market-data public objects are missing: {missing}"
                )
        finally:
            connection.close()

    @staticmethod
    def _source_bar_from_row(row: sqlite3.Row) -> SignalSourceBar:
        start_ts = int(row["bar_start_ts"])
        duration = int(row["bar_duration_seconds"])
        return SignalSourceBar(
            bar_id=str(row["bar_id"]),
            revision=int(row["revision"]),
            instrument_id=str(row["instrument_id"]),
            con_id=int(row["con_id"]),
            local_symbol=str(row["local_symbol"]),
            bar_start_ts=start_ts,
            bar_end_ts=start_ts + duration,
            bar_start_utc=str(row["bar_start_utc"]),
            bar_end_utc=str(row["bar_end_utc"]),
            published_at_utc=str(row["published_at_utc"]),
            entry_price=float(row["mid_close"]),
        )

    def read_latest_source_bar(self) -> SignalSourceBar | None:
        connection = self._connect()
        try:
            row = connection.execute(
                f"""
                SELECT
                    *,
                    {self._mid_sql} AS mid_close
                FROM public_market_data_latest_v1
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (self.instrument_id,),
            ).fetchone()
            return None if row is None else self._source_bar_from_row(row)
        finally:
            connection.close()

    def _read_source_bar(
        self,
        connection: sqlite3.Connection,
        *,
        signal_bar_ts: int,
        bar_size_seconds: int,
    ) -> SignalSourceBar:
        source_start = int(signal_bar_ts) - int(bar_size_seconds)
        rows = connection.execute(
            f"""
            SELECT
                *,
                {self._mid_sql} AS mid_close
            FROM public_market_bars_v1
            WHERE instrument_id = ?
              AND bar_start_ts = ?
              AND bar_duration_seconds = ?
            ORDER BY con_id
            """,
            (
                self.instrument_id,
                source_start,
                int(bar_size_seconds),
            ),
        ).fetchall()
        if len(rows) != 1:
            raise SignalDataNotReadyError(
                "expected exactly one complete source bar for signal time: "
                f"instrument={self.instrument_id}, "
                f"signal_bar_ts={signal_bar_ts}, rows={len(rows)}"
            )
        return self._source_bar_from_row(rows[0])

    def _read_contiguous_values(
        self,
        connection: sqlite3.Connection,
        *,
        start_ts: int,
        end_ts: int,
        bar_size_seconds: int,
        context: str,
    ) -> np.ndarray:
        rows = connection.execute(
            f"""
            SELECT
                bar_start_ts,
                {self._mid_sql} AS mid_close
            FROM public_market_bars_v1
            WHERE instrument_id = ?
              AND bar_start_ts >= ?
              AND bar_start_ts < ?
              AND bar_duration_seconds = ?
            ORDER BY bar_start_ts, con_id
            """,
            (
                self.instrument_id,
                int(start_ts),
                int(end_ts),
                int(bar_size_seconds),
            ),
        ).fetchall()
        count = expected_points(
            seconds=int(end_ts) - int(start_ts),
            bar_size_seconds=bar_size_seconds,
        )
        if len(rows) != count:
            raise SignalDataNotReadyError(
                f"{context}: wrong number of complete bars: "
                f"expected={count}, actual={len(rows)}"
            )
        values = np.empty((count,), dtype=float)
        for index, row in enumerate(rows):
            expected_ts = int(start_ts) + index * int(bar_size_seconds)
            actual_ts = int(row["bar_start_ts"])
            if actual_ts != expected_ts:
                raise SignalDataNotReadyError(
                    f"{context}: gap or duplicate at index={index}: "
                    f"expected={expected_ts}, actual={actual_ts}"
                )
            values[index] = float(row["mid_close"])
        return values

    def _candidate_windows(
        self,
        connection: sqlite3.Connection,
        *,
        window: SignalWindow,
        bar_size_seconds: int,
        historical_lookback_days: int,
        candidate_hour_profile: str,
    ) -> tuple[CandidateWindow, ...]:
        if candidate_hour_profile != "current_v1":
            raise SignalMarketDataError(
                "unsupported candidate_hour_profile: "
                f"{candidate_hour_profile!r}"
            )
        current_ct = datetime.fromtimestamp(
            window.signal_bar_ts,
            tz=timezone.utc,
        ).astimezone(self._ct)
        if current_ct.hour in {15, 16}:
            return ()

        min_signal_ts = (
            window.signal_bar_ts
            - int(historical_lookback_days) * 86_400
        )
        max_signal_ts = window.signal_bar_ts - window.trade_seconds
        if max_signal_ts <= 0:
            return ()
        phase = window.signal_bar_ts % 3_600
        rows = connection.execute(
            """
            SELECT
                bar_start_ts,
                bar_duration_seconds
            FROM public_market_bars_v1
            WHERE instrument_id = ?
              AND bar_start_ts >= ?
              AND bar_start_ts <= ?
              AND bar_duration_seconds = ?
              AND ((bar_start_ts + ?) % 3600) = ?
            ORDER BY bar_start_ts, con_id
            """,
            (
                self.instrument_id,
                min_signal_ts - int(bar_size_seconds),
                max_signal_ts - int(bar_size_seconds),
                int(bar_size_seconds),
                int(bar_size_seconds),
                phase,
            ),
        ).fetchall()

        values: list[CandidateWindow] = []
        seen_signal_ts: set[int] = set()
        for row in rows:
            signal_ts = (
                int(row["bar_start_ts"])
                + int(row["bar_duration_seconds"])
            )
            if signal_ts in seen_signal_ts:
                raise SignalDataNotReadyError(
                    "candidate query returned duplicate signal timestamp: "
                    f"{signal_ts}"
                )
            seen_signal_ts.add(signal_ts)
            time_ct = datetime.fromtimestamp(
                signal_ts,
                tz=timezone.utc,
            ).astimezone(self._ct).strftime("%Y-%m-%d %H:%M:%S")
            values.append(
                CandidateWindow(
                    signal_bar_ts=signal_ts,
                    signal_time_ct=time_ct,
                    pattern_start_ts=(
                        signal_ts - window.pattern_seconds
                    ),
                    pattern_end_ts=signal_ts,
                    trade_start_ts=signal_ts,
                    trade_end_ts=signal_ts + window.trade_seconds,
                )
            )
        return tuple(values)

    def _candidate_pattern_matrix(
        self,
        connection: sqlite3.Connection,
        *,
        candidates: tuple[CandidateWindow, ...],
        pattern_points: int,
        bar_size_seconds: int,
    ) -> tuple[tuple[CandidateWindow, ...], np.ndarray, int]:
        if not candidates:
            return (
                (),
                np.empty((0, pattern_points), dtype=float),
                0,
            )

        connection.execute(
            """
            CREATE TEMP TABLE temp_signal_candidate_windows (
                candidate_index INTEGER PRIMARY KEY,
                pattern_start_ts INTEGER NOT NULL,
                pattern_end_ts INTEGER NOT NULL
            )
            """
        )
        try:
            connection.executemany(
                """
                INSERT INTO temp_signal_candidate_windows (
                    candidate_index,
                    pattern_start_ts,
                    pattern_end_ts
                ) VALUES (?, ?, ?)
                """,
                [
                    (
                        index,
                        candidate.pattern_start_ts,
                        candidate.pattern_end_ts,
                    )
                    for index, candidate in enumerate(candidates)
                ],
            )
            qualified_mid = self._mid_sql.replace(
                "bid_close",
                "b.bid_close",
            ).replace(
                "ask_close",
                "b.ask_close",
            )
            rows = connection.execute(
                f"""
                SELECT
                    c.candidate_index,
                    b.bar_start_ts,
                    {qualified_mid} AS mid_close
                FROM temp_signal_candidate_windows AS c
                JOIN public_market_bars_v1 AS b
                  ON b.instrument_id = ?
                 AND b.bar_start_ts >= c.pattern_start_ts
                 AND b.bar_start_ts < c.pattern_end_ts
                 AND b.bar_duration_seconds = ?
                ORDER BY c.candidate_index, b.bar_start_ts, b.con_id
                """,
                (self.instrument_id, int(bar_size_seconds)),
            )
            matrix = np.full(
                (len(candidates), pattern_points),
                np.nan,
                dtype=float,
            )
            counts = np.zeros((len(candidates),), dtype=np.int32)
            invalid: set[int] = set()
            for row in rows:
                index = int(row["candidate_index"])
                if index in invalid:
                    continue
                point = int(counts[index])
                if point >= pattern_points:
                    invalid.add(index)
                    continue
                expected_ts = (
                    candidates[index].pattern_start_ts
                    + point * int(bar_size_seconds)
                )
                actual_ts = int(row["bar_start_ts"])
                if actual_ts != expected_ts:
                    invalid.add(index)
                    continue
                matrix[index, point] = float(row["mid_close"])
                counts[index] += 1
        finally:
            connection.execute(
                "DROP TABLE IF EXISTS temp_signal_candidate_windows"
            )

        valid_indices = [
            index
            for index in range(len(candidates))
            if (
                index not in invalid
                and int(counts[index]) == pattern_points
                and np.all(np.isfinite(matrix[index, :]))
            )
        ]
        valid_candidates = tuple(candidates[index] for index in valid_indices)
        valid_matrix = (
            matrix[valid_indices, :].astype(float)
            if valid_indices
            else np.empty((0, pattern_points), dtype=float)
        )
        return (
            valid_candidates,
            valid_matrix,
            len(candidates) - len(valid_candidates),
        )

    def load_pattern_inputs(
        self,
        *,
        window: SignalWindow,
        bar_size_seconds: int,
        historical_lookback_days: int,
        candidate_hour_profile: str,
    ) -> SignalPatternInputs:
        pattern_points = expected_points(
            seconds=window.pattern_seconds,
            bar_size_seconds=bar_size_seconds,
        )
        connection = self._connect()
        try:
            source = self._read_source_bar(
                connection,
                signal_bar_ts=window.signal_bar_ts,
                bar_size_seconds=bar_size_seconds,
            )
            current = self._read_contiguous_values(
                connection,
                start_ts=window.pattern_start_ts,
                end_ts=window.pattern_end_ts,
                bar_size_seconds=bar_size_seconds,
                context="current pattern",
            )
            candidates = self._candidate_windows(
                connection,
                window=window,
                bar_size_seconds=bar_size_seconds,
                historical_lookback_days=historical_lookback_days,
                candidate_hour_profile=candidate_hour_profile,
            )
            valid, matrix, skipped = self._candidate_pattern_matrix(
                connection,
                candidates=candidates,
                pattern_points=pattern_points,
                bar_size_seconds=bar_size_seconds,
            )
            return SignalPatternInputs(
                source_bar=source,
                current_values=current,
                candidates=valid,
                candidate_matrix=matrix,
                raw_candidate_count=len(candidates),
                skipped_candidate_count=skipped,
            )
        finally:
            connection.close()

    def load_full_candidate_values(
        self,
        *,
        candidates: tuple[CandidateWindow, ...],
        bar_size_seconds: int,
    ) -> dict[int, np.ndarray]:
        result: dict[int, np.ndarray] = {}
        connection = self._connect()
        try:
            for candidate in candidates:
                try:
                    values = self._read_contiguous_values(
                        connection,
                        start_ts=candidate.pattern_start_ts,
                        end_ts=candidate.trade_end_ts,
                        bar_size_seconds=bar_size_seconds,
                        context=(
                            "candidate full window "
                            f"{candidate.signal_bar_ts}"
                        ),
                    )
                except SignalDataNotReadyError:
                    continue
                result[candidate.signal_bar_ts] = values
        finally:
            connection.close()
        return result
