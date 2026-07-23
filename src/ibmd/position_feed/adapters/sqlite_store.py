from __future__ import annotations

import sqlite3
from pathlib import Path

from ibmd.foundation.time import utc_now_text
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)

POSITION_FEED_STORE_NAME = "position_feed"
POSITION_FEED_SCHEMA_VERSION = 1

_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_position_snapshot_attempts"),
    ("table", "internal_broker_position_rows"),
    ("table", "internal_position_feed_state"),
    ("view", "public_broker_position_snapshots_v1"),
    ("view", "public_broker_position_rows_v1"),
    ("view", "public_broker_position_latest_v1"),
}


class PositionSnapshotStoreError(RuntimeError):
    pass


class PositionSnapshotSchemaError(PositionSnapshotStoreError):
    pass


class SQLiteBrokerPositionSnapshotReader:
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
            raise PositionSnapshotSchemaError(
                f"position-feed database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    @staticmethod
    def _snapshot_from_header(
        connection: sqlite3.Connection,
        header: sqlite3.Row,
    ) -> BrokerPositionSnapshotV1:
        rows = connection.execute(
            """
            SELECT
                con_id,
                local_symbol,
                symbol,
                sec_type,
                exchange,
                currency,
                signed_quantity,
                average_cost
            FROM public_broker_position_rows_v1
            WHERE snapshot_id = ?
            ORDER BY row_ordinal
            """,
            (str(header["snapshot_id"]),),
        ).fetchall()
        return BrokerPositionSnapshotV1.complete(
            snapshot_id=str(header["snapshot_id"]),
            account_id=str(header["account_id"]),
            captured_at_utc=str(header["captured_at_utc"]),
            published_at_utc=str(header["published_at_utc"]),
            source_session_id=str(header["source_session_id"]),
            rows=tuple(
                BrokerPositionRowV1(
                    con_id=int(row["con_id"]),
                    local_symbol=(
                        None
                        if row["local_symbol"] is None
                        else str(row["local_symbol"])
                    ),
                    symbol=str(row["symbol"]),
                    sec_type=str(row["sec_type"]),
                    exchange=(
                        None
                        if row["exchange"] is None
                        else str(row["exchange"])
                    ),
                    currency=(
                        None
                        if row["currency"] is None
                        else str(row["currency"])
                    ),
                    signed_quantity=float(row["signed_quantity"]),
                    average_cost=(
                        None
                        if row["average_cost"] is None
                        else float(row["average_cost"])
                    ),
                )
                for row in rows
            ),
        )

    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None:
        connection = self._connect()
        try:
            header = connection.execute(
                """
                SELECT
                    snapshot_id,
                    account_id,
                    captured_at_utc,
                    published_at_utc,
                    status,
                    row_count,
                    source_session_id
                FROM public_broker_position_latest_v1
                LIMIT 1
                """
            ).fetchone()
            if header is None:
                return None
            snapshot = self._snapshot_from_header(connection, header)
            if snapshot.row_count != int(header["row_count"]):
                raise PositionSnapshotStoreError(
                    "public position snapshot row_count mismatch: "
                    f"snapshot={snapshot.snapshot_id}, "
                    f"header={header['row_count']}, rows={snapshot.row_count}"
                )
            return snapshot
        finally:
            connection.close()

    def read_complete(
        self,
        snapshot_id: str,
    ) -> BrokerPositionSnapshotV1 | None:
        connection = self._connect()
        try:
            header = connection.execute(
                """
                SELECT
                    snapshot_id,
                    account_id,
                    captured_at_utc,
                    published_at_utc,
                    status,
                    row_count,
                    source_session_id
                FROM public_broker_position_snapshots_v1
                WHERE snapshot_id = ?
                LIMIT 1
                """,
                (str(snapshot_id),),
            ).fetchone()
            if header is None:
                return None
            snapshot = self._snapshot_from_header(connection, header)
            if snapshot.row_count != int(header["row_count"]):
                raise PositionSnapshotStoreError(
                    "public position snapshot row_count mismatch: "
                    f"snapshot={snapshot.snapshot_id}"
                )
            return snapshot
        finally:
            connection.close()


class SQLiteBrokerPositionSnapshotStore:
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self.reader = SQLiteBrokerPositionSnapshotReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return connection

    def validate_schema(self) -> None:
        if not self.database_path.is_file():
            raise PositionSnapshotSchemaError(
                f"position-feed database does not exist: {self.database_path}"
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
                raise PositionSnapshotSchemaError(
                    f"position-feed schema objects are missing: {missing}"
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
                    (POSITION_FEED_STORE_NAME,),
                ).fetchall()
            ]
            if versions != list(
                range(1, POSITION_FEED_SCHEMA_VERSION + 1)
            ):
                raise PositionSnapshotSchemaError(
                    "position-feed schema version mismatch: "
                    f"expected=1..{POSITION_FEED_SCHEMA_VERSION}, "
                    f"actual={versions}"
                )
            state = connection.execute(
                """
                SELECT singleton_id
                FROM internal_position_feed_state
                WHERE singleton_id = 1
                """
            ).fetchone()
            if state is None:
                raise PositionSnapshotSchemaError(
                    "position-feed singleton state row is missing"
                )
        except sqlite3.Error as exc:
            raise PositionSnapshotSchemaError(
                f"cannot validate position-feed schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def publish(self, snapshot: BrokerPositionSnapshotV1) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if snapshot.status == BrokerPositionSnapshotStatus.COMPLETE:
                connection.execute(
                    """
                    INSERT INTO internal_position_snapshot_attempts (
                        snapshot_id,
                        account_id,
                        captured_at_utc,
                        published_at_utc,
                        status,
                        row_count,
                        source_session_id,
                        error_text,
                        created_at_utc
                    ) VALUES (?, ?, ?, NULL, 'WRITING', 0, ?, NULL, ?)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.account_id,
                        snapshot.captured_at_utc,
                        snapshot.source_session_id,
                        utc_now_text(),
                    ),
                )
                for ordinal, row in enumerate(snapshot.rows):
                    connection.execute(
                        """
                        INSERT INTO internal_broker_position_rows (
                            snapshot_id,
                            row_ordinal,
                            con_id,
                            local_symbol,
                            symbol,
                            sec_type,
                            exchange,
                            currency,
                            signed_quantity,
                            average_cost
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot.snapshot_id,
                            ordinal,
                            row.con_id,
                            row.local_symbol,
                            row.symbol,
                            row.sec_type,
                            row.exchange,
                            row.currency,
                            row.signed_quantity,
                            row.average_cost,
                        ),
                    )
                connection.execute(
                    """
                    UPDATE internal_position_snapshot_attempts
                    SET
                        published_at_utc = ?,
                        status = 'COMPLETE',
                        row_count = ?
                    WHERE snapshot_id = ?
                      AND status = 'WRITING'
                    """,
                    (
                        snapshot.published_at_utc,
                        snapshot.row_count,
                        snapshot.snapshot_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE internal_position_feed_state
                    SET latest_complete_snapshot_id = ?
                    WHERE singleton_id = 1
                    """,
                    (snapshot.snapshot_id,),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO internal_position_snapshot_attempts (
                        snapshot_id,
                        account_id,
                        captured_at_utc,
                        published_at_utc,
                        status,
                        row_count,
                        source_session_id,
                        error_text,
                        created_at_utc
                    ) VALUES (?, ?, ?, ?, 'FAILED', 0, ?, ?, ?)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.account_id,
                        snapshot.captured_at_utc,
                        snapshot.published_at_utc,
                        snapshot.source_session_id,
                        snapshot.error_text,
                        utc_now_text(),
                    ),
                )
            connection.commit()
        except Exception as exc:
            connection.rollback()
            raise PositionSnapshotStoreError(
                "cannot publish broker position snapshot: "
                f"snapshot={snapshot.snapshot_id}, "
                f"status={snapshot.status.value}, "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None:
        return self.reader.read_latest_complete()

    def read_attempt_status(self, snapshot_id: str) -> str | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT status
                FROM internal_position_snapshot_attempts
                WHERE snapshot_id = ?
                """,
                (str(snapshot_id),),
            ).fetchone()
            return None if row is None else str(row["status"])
        finally:
            connection.close()

    def count_attempts(self) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM internal_position_snapshot_attempts"
            ).fetchone()
            return int(row["count"])
        finally:
            connection.close()
