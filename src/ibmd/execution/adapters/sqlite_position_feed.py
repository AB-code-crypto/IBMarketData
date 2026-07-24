from __future__ import annotations

import sqlite3
from pathlib import Path

from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)


class ExecutionPositionFeedError(RuntimeError):
    pass


class SQLiteExecutionPositionFeedReader:
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

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise ExecutionPositionFeedError(
                f"position-feed database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        connection.execute("PRAGMA query_only = ON")
        return connection

    def validate_schema(self) -> None:
        required = {
            "public_broker_position_latest_v1",
            "public_broker_position_rows_v1",
        }
        connection = self._connect()
        try:
            existing = {
                str(row["name"])
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'view'
                      AND name IN (
                          'public_broker_position_latest_v1',
                          'public_broker_position_rows_v1'
                      )
                    """
                ).fetchall()
            }
            missing = sorted(required - existing)
            if missing:
                raise ExecutionPositionFeedError(
                    f"position-feed public views are missing: {missing}"
                )
        except sqlite3.Error as exc:
            raise ExecutionPositionFeedError(
                f"cannot validate position-feed public product: {exc}"
            ) from exc
        finally:
            connection.close()

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
            if str(header["status"]) != "COMPLETE":
                raise ExecutionPositionFeedError(
                    "position-feed latest public snapshot is not COMPLETE"
                )
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
            snapshot = BrokerPositionSnapshotV1.complete(
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
            if snapshot.row_count != int(header["row_count"]):
                raise ExecutionPositionFeedError(
                    "position-feed public snapshot row count mismatch: "
                    f"header={header['row_count']}, rows={snapshot.row_count}"
                )
            return snapshot
        except sqlite3.Error as exc:
            raise ExecutionPositionFeedError(
                f"cannot read latest position-feed snapshot: {exc}"
            ) from exc
        finally:
            connection.close()
