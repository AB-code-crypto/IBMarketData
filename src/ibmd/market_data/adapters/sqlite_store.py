from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ibmd.foundation.identity import new_id, validate_id
from ibmd.foundation.time import parse_utc, utc_now_text
from ibmd.market_data.domain.bar import assemble_complete_market_bar
from ibmd.public_contracts.market_data import (
    MarketBarV1,
    MarketDataSourceKind,
    MarketSideBarObservationV1,
    QuoteSide,
)

MARKET_DATA_STORE_NAME = "market_data"
MARKET_DATA_SCHEMA_VERSION = 1

_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_market_bar_sides"),
    ("table", "internal_market_bars"),
    ("table", "internal_market_data_ingest_events"),
    ("table", "internal_market_data_source_events"),
    ("table", "internal_market_data_state"),
    ("view", "public_market_bars_v1"),
    ("view", "public_market_data_latest_v1"),
    ("view", "public_market_data_status_v1"),
}


class MarketDataStoreError(RuntimeError):
    pass


class MarketDataSchemaError(MarketDataStoreError):
    pass


def _bar_from_row(row: sqlite3.Row) -> MarketBarV1:
    return MarketBarV1(
        bar_id=str(row["bar_id"]),
        instrument_id=str(row["instrument_id"]),
        con_id=int(row["con_id"]),
        local_symbol=str(row["local_symbol"]),
        bar_start_utc=str(row["bar_start_utc"]),
        bar_end_utc=str(row["bar_end_utc"]),
        bar_duration_seconds=int(row["bar_duration_seconds"]),
        bid_open=float(row["bid_open"]),
        bid_high=float(row["bid_high"]),
        bid_low=float(row["bid_low"]),
        bid_close=float(row["bid_close"]),
        ask_open=float(row["ask_open"]),
        ask_high=float(row["ask_high"]),
        ask_low=float(row["ask_low"]),
        ask_close=float(row["ask_close"]),
        source_kind=MarketDataSourceKind(str(row["source_kind"])),
        first_published_at_utc=str(row["first_published_at_utc"]),
        published_at_utc=str(row["published_at_utc"]),
        revision=int(row["revision"]),
        complete=bool(row["complete"]),
        volume=None if row["volume"] is None else float(row["volume"]),
        average=None if row["average"] is None else float(row["average"]),
        bar_count=None if row["bar_count"] is None else int(row["bar_count"]),
    )


def _fragment_from_row(row: sqlite3.Row) -> MarketSideBarObservationV1:
    return MarketSideBarObservationV1(
        instrument_id=str(row["instrument_id"]),
        con_id=int(row["con_id"]),
        local_symbol=str(row["local_symbol"]),
        side=QuoteSide(str(row["side"])),
        bar_start_utc=str(row["bar_start_utc"]),
        bar_duration_seconds=int(row["bar_duration_seconds"]),
        open_price=float(row["open_price"]),
        high_price=float(row["high_price"]),
        low_price=float(row["low_price"]),
        close_price=float(row["close_price"]),
        source_kind=MarketDataSourceKind(str(row["source_kind"])),
        source_session_id=str(row["source_session_id"]),
        source_generation_id=str(row["source_generation_id"]),
        observed_at_utc=str(row["observed_at_utc"]),
    )


class SQLiteMarketBarReader:
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.busy_timeout_ms = int(busy_timeout_ms)

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise MarketDataSchemaError(
                f"market-data database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def read_latest_complete(
        self,
        instrument_id: str,
    ) -> MarketBarV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT *
                FROM public_market_data_latest_v1
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (str(instrument_id),),
            ).fetchone()
            return None if row is None else _bar_from_row(row)
        finally:
            connection.close()

    def read_bar(self, bar_id: str) -> MarketBarV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT *
                FROM public_market_bars_v1
                WHERE bar_id = ?
                LIMIT 1
                """,
                (str(bar_id),),
            ).fetchone()
            return None if row is None else _bar_from_row(row)
        finally:
            connection.close()

    def read_range(
        self,
        *,
        instrument_id: str,
        start_utc: str,
        end_utc: str,
        con_id: int | None = None,
    ) -> tuple[MarketBarV1, ...]:
        start_ts = int(parse_utc(start_utc).timestamp())
        end_ts = int(parse_utc(end_utc).timestamp())
        if end_ts <= start_ts:
            raise ValueError("market-data range must be positive")
        connection = self._connect()
        try:
            if con_id is None:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM public_market_bars_v1
                    WHERE instrument_id = ?
                      AND bar_start_ts >= ?
                      AND bar_start_ts < ?
                    ORDER BY bar_start_ts, con_id
                    """,
                    (str(instrument_id), start_ts, end_ts),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM public_market_bars_v1
                    WHERE instrument_id = ?
                      AND con_id = ?
                      AND bar_start_ts >= ?
                      AND bar_start_ts < ?
                    ORDER BY bar_start_ts
                    """,
                    (str(instrument_id), int(con_id), start_ts, end_ts),
                ).fetchall()
            return tuple(_bar_from_row(row) for row in rows)
        finally:
            connection.close()


class SQLiteMarketBarStore:
    def __init__(
        self,
        database_path: str | Path,
        *,
        instrument_id: str,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.instrument_id = str(instrument_id or "").strip()
        if not self.instrument_id:
            raise ValueError("instrument_id is required")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self.reader = SQLiteMarketBarReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        self._writer_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return connection

    def validate_schema(self) -> None:
        if not self.database_path.is_file():
            raise MarketDataSchemaError(
                f"market-data database does not exist: {self.database_path}"
            )
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
                raise MarketDataSchemaError(
                    f"market-data schema objects are missing: {missing}"
                )
            versions = [
                int(row["version"])
                for row in connection.execute(
                    """
                    SELECT version
                    FROM schema_migrations
                    WHERE store_name = ?
                    ORDER BY version
                    """,
                    (MARKET_DATA_STORE_NAME,),
                ).fetchall()
            ]
            if versions != list(range(1, MARKET_DATA_SCHEMA_VERSION + 1)):
                raise MarketDataSchemaError(
                    "market-data schema version mismatch: "
                    f"expected=1..{MARKET_DATA_SCHEMA_VERSION}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise MarketDataSchemaError(
                f"cannot validate market-data schema: {exc}"
            ) from exc
        finally:
            connection.close()

    @staticmethod
    def _side_upsert(
        connection: sqlite3.Connection,
        fragment: MarketSideBarObservationV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_market_bar_sides (
                instrument_id,
                con_id,
                local_symbol,
                bar_start_ts,
                bar_start_utc,
                bar_end_utc,
                bar_duration_seconds,
                side,
                open_price,
                high_price,
                low_price,
                close_price,
                source_kind,
                source_session_id,
                source_generation_id,
                observed_at_utc,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                instrument_id,
                con_id,
                bar_start_ts,
                source_session_id,
                source_generation_id,
                source_kind,
                side
            )
            DO UPDATE SET
                local_symbol = excluded.local_symbol,
                bar_start_utc = excluded.bar_start_utc,
                bar_end_utc = excluded.bar_end_utc,
                bar_duration_seconds = excluded.bar_duration_seconds,
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                source_kind = excluded.source_kind,
                source_session_id = excluded.source_session_id,
                source_generation_id = excluded.source_generation_id,
                observed_at_utc = excluded.observed_at_utc,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                fragment.instrument_id,
                fragment.con_id,
                fragment.local_symbol,
                int(parse_utc(fragment.bar_start_utc).timestamp()),
                fragment.bar_start_utc,
                fragment.bar_end_utc,
                fragment.bar_duration_seconds,
                fragment.side.value,
                fragment.open_price,
                fragment.high_price,
                fragment.low_price,
                fragment.close_price,
                fragment.source_kind.value,
                fragment.source_session_id,
                fragment.source_generation_id,
                fragment.observed_at_utc,
                utc_now_text(),
            ),
        )

    @staticmethod
    def _read_side_pair(
        connection: sqlite3.Connection,
        *,
        instrument_id: str,
        con_id: int,
        bar_start_ts: int,
        source_session_id: str,
        source_generation_id: str,
        source_kind: MarketDataSourceKind,
    ) -> tuple[MarketSideBarObservationV1 | None, MarketSideBarObservationV1 | None]:
        rows = connection.execute(
            """
            SELECT *
            FROM internal_market_bar_sides
            WHERE instrument_id = ?
              AND con_id = ?
              AND bar_start_ts = ?
              AND source_session_id = ?
              AND source_generation_id = ?
              AND source_kind = ?
            ORDER BY side
            """,
            (
                instrument_id,
                con_id,
                bar_start_ts,
                source_session_id,
                source_generation_id,
                source_kind.value,
            ),
        ).fetchall()
        by_side = {
            QuoteSide(str(row["side"])): _fragment_from_row(row)
            for row in rows
        }
        return by_side.get(QuoteSide.BID), by_side.get(QuoteSide.ASK)

    @staticmethod
    def _read_existing_bar(
        connection: sqlite3.Connection,
        *,
        instrument_id: str,
        con_id: int,
        bar_start_ts: int,
    ) -> MarketBarV1 | None:
        row = connection.execute(
            """
            SELECT *
            FROM internal_market_bars
            WHERE instrument_id = ?
              AND con_id = ?
              AND bar_start_ts = ?
            LIMIT 1
            """,
            (instrument_id, con_id, bar_start_ts),
        ).fetchone()
        return None if row is None else _bar_from_row(row)

    @staticmethod
    def _insert_bar(
        connection: sqlite3.Connection,
        bar: MarketBarV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_market_bars (
                bar_id,
                instrument_id,
                con_id,
                local_symbol,
                bar_start_ts,
                bar_start_utc,
                bar_end_utc,
                bar_duration_seconds,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close,
                source_kind,
                first_published_at_utc,
                published_at_utc,
                revision,
                complete,
                volume,
                average,
                bar_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                bar.bar_id,
                bar.instrument_id,
                bar.con_id,
                bar.local_symbol,
                int(parse_utc(bar.bar_start_utc).timestamp()),
                bar.bar_start_utc,
                bar.bar_end_utc,
                bar.bar_duration_seconds,
                bar.bid_open,
                bar.bid_high,
                bar.bid_low,
                bar.bid_close,
                bar.ask_open,
                bar.ask_high,
                bar.ask_low,
                bar.ask_close,
                bar.source_kind.value,
                bar.first_published_at_utc,
                bar.published_at_utc,
                bar.revision,
                bar.volume,
                bar.average,
                bar.bar_count,
            ),
        )

    @staticmethod
    def _update_bar(
        connection: sqlite3.Connection,
        bar: MarketBarV1,
    ) -> None:
        connection.execute(
            """
            UPDATE internal_market_bars
            SET
                local_symbol = ?,
                bar_start_utc = ?,
                bar_end_utc = ?,
                bar_duration_seconds = ?,
                bid_open = ?,
                bid_high = ?,
                bid_low = ?,
                bid_close = ?,
                ask_open = ?,
                ask_high = ?,
                ask_low = ?,
                ask_close = ?,
                source_kind = ?,
                published_at_utc = ?,
                revision = ?,
                volume = ?,
                average = ?,
                bar_count = ?
            WHERE bar_id = ?
            """,
            (
                bar.local_symbol,
                bar.bar_start_utc,
                bar.bar_end_utc,
                bar.bar_duration_seconds,
                bar.bid_open,
                bar.bid_high,
                bar.bid_low,
                bar.bid_close,
                bar.ask_open,
                bar.ask_high,
                bar.ask_low,
                bar.ask_close,
                bar.source_kind.value,
                bar.published_at_utc,
                bar.revision,
                bar.volume,
                bar.average,
                bar.bar_count,
                bar.bar_id,
            ),
        )

    @staticmethod
    def _update_state(
        connection: sqlite3.Connection,
        *,
        instrument_id: str,
        bar: MarketBarV1 | None,
        observed_at_utc: str,
        source_status: str,
        error_text: str | None,
    ) -> None:
        current = connection.execute(
            """
            SELECT latest_bar_start_ts
            FROM internal_market_data_state
            WHERE instrument_id = ?
            """,
            (instrument_id,),
        ).fetchone()
        current_start = None if current is None else current["latest_bar_start_ts"]
        use_bar = (
            bar is not None
            and (
                current_start is None
                or int(parse_utc(bar.bar_start_utc).timestamp()) >= int(current_start)
            )
        )
        connection.execute(
            """
            INSERT INTO internal_market_data_state (
                instrument_id,
                latest_complete_bar_id,
                latest_bar_start_ts,
                latest_bar_end_utc,
                latest_con_id,
                latest_local_symbol,
                last_ingest_at_utc,
                last_source_status,
                last_error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                latest_complete_bar_id = CASE
                    WHEN excluded.latest_bar_start_ts IS NOT NULL
                     AND (
                        internal_market_data_state.latest_bar_start_ts IS NULL
                        OR excluded.latest_bar_start_ts >= internal_market_data_state.latest_bar_start_ts
                     )
                    THEN excluded.latest_complete_bar_id
                    ELSE internal_market_data_state.latest_complete_bar_id
                END,
                latest_bar_start_ts = CASE
                    WHEN excluded.latest_bar_start_ts IS NOT NULL
                     AND (
                        internal_market_data_state.latest_bar_start_ts IS NULL
                        OR excluded.latest_bar_start_ts >= internal_market_data_state.latest_bar_start_ts
                     )
                    THEN excluded.latest_bar_start_ts
                    ELSE internal_market_data_state.latest_bar_start_ts
                END,
                latest_bar_end_utc = CASE
                    WHEN excluded.latest_bar_start_ts IS NOT NULL
                     AND (
                        internal_market_data_state.latest_bar_start_ts IS NULL
                        OR excluded.latest_bar_start_ts >= internal_market_data_state.latest_bar_start_ts
                     )
                    THEN excluded.latest_bar_end_utc
                    ELSE internal_market_data_state.latest_bar_end_utc
                END,
                latest_con_id = CASE
                    WHEN excluded.latest_bar_start_ts IS NOT NULL
                     AND (
                        internal_market_data_state.latest_bar_start_ts IS NULL
                        OR excluded.latest_bar_start_ts >= internal_market_data_state.latest_bar_start_ts
                     )
                    THEN excluded.latest_con_id
                    ELSE internal_market_data_state.latest_con_id
                END,
                latest_local_symbol = CASE
                    WHEN excluded.latest_bar_start_ts IS NOT NULL
                     AND (
                        internal_market_data_state.latest_bar_start_ts IS NULL
                        OR excluded.latest_bar_start_ts >= internal_market_data_state.latest_bar_start_ts
                     )
                    THEN excluded.latest_local_symbol
                    ELSE internal_market_data_state.latest_local_symbol
                END,
                last_ingest_at_utc = excluded.last_ingest_at_utc,
                last_source_status = excluded.last_source_status,
                last_error_text = excluded.last_error_text
            """,
            (
                instrument_id,
                bar.bar_id if use_bar else None,
                (
                    int(parse_utc(bar.bar_start_utc).timestamp())
                    if use_bar
                    else None
                ),
                bar.bar_end_utc if use_bar else None,
                bar.con_id if use_bar else None,
                bar.local_symbol if use_bar else None,
                observed_at_utc,
                source_status,
                error_text,
            ),
        )

    def ingest_fragments(
        self,
        fragments: tuple[MarketSideBarObservationV1, ...],
    ) -> tuple[MarketBarV1, ...]:
        values = tuple(fragments)
        if not values:
            return ()
        if any(item.instrument_id != self.instrument_id for item in values):
            raise MarketDataStoreError(
                "market-data store received a fragment for another instrument"
            )

        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                affected: set[tuple[str, int, int, str, str, MarketDataSourceKind]] = set()
                latest_observed = max(
                    values,
                    key=lambda item: parse_utc(item.observed_at_utc),
                ).observed_at_utc

                for fragment in values:
                    bar_start_ts = int(
                        parse_utc(fragment.bar_start_utc).timestamp()
                    )
                    self._side_upsert(connection, fragment)
                    event_id = new_id("md_ingest")
                    connection.execute(
                        """
                        INSERT INTO internal_market_data_ingest_events (
                            event_id,
                            instrument_id,
                            con_id,
                            local_symbol,
                            bar_start_ts,
                            side,
                            source_kind,
                            source_session_id,
                            source_generation_id,
                            observed_at_utc,
                            result,
                            canonical_bar_id,
                            canonical_revision,
                            error_text,
                            created_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STAGED', NULL, NULL, NULL, ?)
                        """,
                        (
                            event_id,
                            fragment.instrument_id,
                            fragment.con_id,
                            fragment.local_symbol,
                            bar_start_ts,
                            fragment.side.value,
                            fragment.source_kind.value,
                            fragment.source_session_id,
                            fragment.source_generation_id,
                            fragment.observed_at_utc,
                            utc_now_text(),
                        ),
                    )
                    affected.add(
                        (
                            fragment.instrument_id,
                            fragment.con_id,
                            bar_start_ts,
                            fragment.source_session_id,
                            fragment.source_generation_id,
                            fragment.source_kind,
                        )
                    )

                changed: list[MarketBarV1] = []
                latest_changed: MarketBarV1 | None = None
                for (
                    instrument_id,
                    con_id,
                    bar_start_ts,
                    source_session_id,
                    source_generation_id,
                    source_kind,
                ) in sorted(
                    affected,
                    key=lambda item: (
                        item[0],
                        item[1],
                        item[2],
                        item[3],
                        item[4],
                        item[5].value,
                    ),
                ):
                    bid, ask = self._read_side_pair(
                        connection,
                        instrument_id=instrument_id,
                        con_id=con_id,
                        bar_start_ts=bar_start_ts,
                        source_session_id=source_session_id,
                        source_generation_id=source_generation_id,
                        source_kind=source_kind,
                    )
                    if bid is None or ask is None:
                        continue
                    if bid.source_generation_id != ask.source_generation_id:
                        continue
                    if bid.source_session_id != ask.source_session_id:
                        continue
                    if bid.source_kind != ask.source_kind:
                        continue

                    existing = self._read_existing_bar(
                        connection,
                        instrument_id=instrument_id,
                        con_id=con_id,
                        bar_start_ts=bar_start_ts,
                    )
                    candidate = assemble_complete_market_bar(
                        bid=bid,
                        ask=ask,
                        revision=1 if existing is None else existing.revision + 1,
                        first_published_at_utc=(
                            None
                            if existing is None
                            else existing.first_published_at_utc
                        ),
                    )
                    result_error = None
                    if existing is None:
                        self._insert_bar(connection, candidate)
                        result = "PUBLISHED"
                        changed.append(candidate)
                        latest_changed = (
                            candidate
                            if latest_changed is None
                            or parse_utc(candidate.bar_start_utc)
                            >= parse_utc(latest_changed.bar_start_utc)
                            else latest_changed
                        )
                        effective = candidate
                    elif (
                        parse_utc(candidate.published_at_utc)
                        < parse_utc(existing.published_at_utc)
                    ):
                        # A delayed pair from an older source generation must not
                        # overwrite a newer canonical bar. The observations remain
                        # in the audit/staging tables, but the public fact is stable.
                        result = "REJECTED"
                        result_error = (
                            "complete BID/ASK pair is older than the current "
                            "canonical revision"
                        )
                        effective = existing
                    elif existing.material_tuple() == candidate.material_tuple():
                        result = "UNCHANGED"
                        effective = existing
                    else:
                        self._update_bar(connection, candidate)
                        result = "PUBLISHED"
                        changed.append(candidate)
                        latest_changed = (
                            candidate
                            if latest_changed is None
                            or parse_utc(candidate.bar_start_utc)
                            >= parse_utc(latest_changed.bar_start_utc)
                            else latest_changed
                        )
                        effective = candidate

                    connection.execute(
                        """
                        UPDATE internal_market_data_ingest_events
                        SET
                            result = ?,
                            canonical_bar_id = ?,
                            canonical_revision = ?,
                            error_text = ?
                        WHERE instrument_id = ?
                          AND con_id = ?
                          AND bar_start_ts = ?
                          AND source_session_id = ?
                          AND source_generation_id = ?
                          AND source_kind = ?
                          AND result = 'STAGED'
                        """,
                        (
                            result,
                            effective.bar_id,
                            effective.revision,
                            result_error,
                            instrument_id,
                            con_id,
                            bar_start_ts,
                            source_session_id,
                            source_generation_id,
                            source_kind.value,
                        ),
                    )

                self._update_state(
                    connection,
                    instrument_id=self.instrument_id,
                    bar=latest_changed,
                    observed_at_utc=latest_observed,
                    source_status="OK",
                    error_text=None,
                )
                connection.commit()
                return tuple(
                    sorted(
                        changed,
                        key=lambda item: (
                            item.bar_start_utc,
                            item.con_id,
                        ),
                    )
                )
            except Exception as exc:
                connection.rollback()
                raise MarketDataStoreError(
                    "cannot ingest market-data fragments: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def record_source_failure(
        self,
        *,
        instrument_id: str,
        source_session_id: str,
        observed_at_utc: str,
        error_text: str,
    ) -> None:
        if str(instrument_id) != self.instrument_id:
            raise MarketDataStoreError(
                "source failure belongs to another instrument"
            )
        session_id = validate_id(
            source_session_id,
            expected_kind="ib_session",
        )
        observed = str(observed_at_utc)
        parse_utc(observed)
        detail = str(error_text or "market-data source failure").strip()
        if not detail:
            detail = "market-data source failure"
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """
                    INSERT INTO internal_market_data_source_events (
                        event_id,
                        instrument_id,
                        source_session_id,
                        observed_at_utc,
                        status,
                        detail,
                        created_at_utc
                    ) VALUES (?, ?, ?, ?, 'ERROR', ?, ?)
                    """,
                    (
                        new_id("md_source_event"),
                        self.instrument_id,
                        session_id,
                        observed,
                        detail,
                        utc_now_text(),
                    ),
                )
                self._update_state(
                    connection,
                    instrument_id=self.instrument_id,
                    bar=None,
                    observed_at_utc=observed,
                    source_status="ERROR",
                    error_text=detail,
                )
                connection.commit()
            except Exception as exc:
                connection.rollback()
                raise MarketDataStoreError(
                    "cannot record market-data source failure: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_latest_complete(
        self,
        instrument_id: str,
    ) -> MarketBarV1 | None:
        return self.reader.read_latest_complete(instrument_id)

    def count_public_bars(self) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM public_market_bars_v1"
            ).fetchone()
            return int(row["count"])
        finally:
            connection.close()

    def count_fragments(self) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM internal_market_bar_sides"
            ).fetchone()
            return int(row["count"])
        finally:
            connection.close()

    def count_source_failures(self) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM internal_market_data_source_events
                WHERE status = 'ERROR'
                """
            ).fetchone()
            return int(row["count"])
        finally:
            connection.close()
