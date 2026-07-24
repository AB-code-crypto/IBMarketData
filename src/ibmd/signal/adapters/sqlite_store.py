from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.signal import (
    SignalCalculationV1,
    SignalEventV1,
)

SIGNAL_STORE_NAME = "signal"
SIGNAL_SCHEMA_VERSION = 1

_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_signal_calculations"),
    ("table", "internal_signal_events"),
    ("table", "internal_signal_state"),
    ("view", "public_signal_calculations_v1"),
    ("view", "public_signal_events_v1"),
    ("view", "public_signal_latest_v1"),
}


class SignalStoreError(RuntimeError):
    pass


class SignalSchemaError(SignalStoreError):
    pass


def _calculation_from_payload(payload: str) -> SignalCalculationV1:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SignalStoreError(
            f"stored signal calculation JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SignalStoreError(
            "stored signal calculation payload must be an object"
        )
    return SignalCalculationV1.from_dict(value)


def _event_from_payload(payload: str) -> SignalEventV1:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SignalStoreError(
            f"stored signal event JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SignalStoreError(
            "stored signal event payload must be an object"
        )
    return SignalEventV1.from_dict(value)


def _material_payload(calculation: SignalCalculationV1) -> dict[str, object]:
    payload = calculation.to_dict()
    payload.pop("calculation_id")
    payload.pop("calculated_at_utc")
    event = payload.get("event")
    if isinstance(event, dict):
        event.pop("event_id")
        event.pop("calculation_id")
        event.pop("created_at_utc")
    return payload


class SQLiteSignalReader:
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
            raise SignalSchemaError(
                f"signal database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        connection.execute("PRAGMA query_only = ON")
        return connection

    def read_latest_calculation(
        self,
        instrument_id: str,
    ) -> SignalCalculationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_signal_latest_v1
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (str(instrument_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _calculation_from_payload(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_calculation(
        self,
        calculation_id: str,
    ) -> SignalCalculationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_signal_calculations_v1
                WHERE calculation_id = ?
                LIMIT 1
                """,
                (str(calculation_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _calculation_from_payload(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_events(
        self,
        *,
        instrument_id: str,
        start_utc: str,
        end_utc: str,
    ) -> tuple[SignalEventV1, ...]:
        start_ts = int(parse_utc(start_utc).timestamp())
        end_ts = int(parse_utc(end_utc).timestamp())
        if end_ts <= start_ts:
            raise ValueError("signal event interval must be positive")
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM public_signal_events_v1
                WHERE instrument_id = ?
                  AND signal_bar_ts >= ?
                  AND signal_bar_ts < ?
                ORDER BY signal_bar_ts
                """,
                (str(instrument_id), start_ts, end_ts),
            ).fetchall()
            return tuple(
                _event_from_payload(str(row["payload_json"]))
                for row in rows
            )
        finally:
            connection.close()


class SQLiteSignalStore:
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
        self.reader = SQLiteSignalReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        self._writer_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        return connection

    def validate_schema(self) -> None:
        if not self.database_path.is_file():
            raise SignalSchemaError(
                f"signal database does not exist: {self.database_path}"
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
                raise SignalSchemaError(
                    f"signal schema objects are missing: {missing}"
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
                    (SIGNAL_STORE_NAME,),
                ).fetchall()
            ]
            if versions != list(range(1, SIGNAL_SCHEMA_VERSION + 1)):
                raise SignalSchemaError(
                    "signal schema version mismatch: "
                    f"expected=1..{SIGNAL_SCHEMA_VERSION}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise SignalSchemaError(
                f"cannot validate signal schema: {exc}"
            ) from exc
        finally:
            connection.close()

    @staticmethod
    def _natural_key(calculation: SignalCalculationV1) -> tuple[object, ...]:
        return (
            calculation.strategy_id,
            calculation.strategy_version,
            calculation.configuration_hash,
            calculation.instrument_id,
            calculation.source_bar_id,
        )

    def publish(
        self,
        calculation: SignalCalculationV1,
    ) -> SignalCalculationV1:
        payload = canonical_json_text(calculation.to_dict())
        event_payload = (
            None
            if calculation.event is None
            else canonical_json_text(calculation.event.to_dict())
        )
        signal_ts = int(
            parse_utc(calculation.signal_bar_utc).timestamp()
        )

        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT payload_json
                    FROM internal_signal_calculations
                    WHERE strategy_id = ?
                      AND strategy_version = ?
                      AND configuration_hash = ?
                      AND instrument_id = ?
                      AND source_bar_id = ?
                    LIMIT 1
                    """,
                    self._natural_key(calculation),
                ).fetchone()
                if existing is not None:
                    stored = _calculation_from_payload(
                        str(existing["payload_json"])
                    )
                    if _material_payload(stored) != _material_payload(calculation):
                        raise SignalStoreError(
                            "conflicting signal calculation already exists "
                            "for the same strategy/source bar/configuration"
                        )
                    connection.rollback()
                    return stored

                connection.execute(
                    """
                    INSERT INTO internal_signal_calculations (
                        calculation_id,
                        strategy_id,
                        strategy_version,
                        configuration_hash,
                        instrument_id,
                        source_bar_id,
                        source_bar_revision,
                        source_con_id,
                        source_local_symbol,
                        signal_bar_ts,
                        signal_bar_utc,
                        calculated_at_utc,
                        status,
                        event_id,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        calculation.calculation_id,
                        calculation.strategy_id,
                        calculation.strategy_version,
                        calculation.configuration_hash,
                        calculation.instrument_id,
                        calculation.source_bar_id,
                        calculation.source_bar_revision,
                        calculation.source_con_id,
                        calculation.source_local_symbol,
                        signal_ts,
                        calculation.signal_bar_utc,
                        calculation.calculated_at_utc,
                        calculation.status.value,
                        (
                            None
                            if calculation.event is None
                            else calculation.event.event_id
                        ),
                        payload,
                    ),
                )

                event = calculation.event
                if event is not None:
                    connection.execute(
                        """
                        INSERT INTO internal_signal_events (
                            event_id,
                            calculation_id,
                            strategy_id,
                            strategy_version,
                            configuration_hash,
                            instrument_id,
                            source_bar_id,
                            signal_bar_ts,
                            signal_bar_utc,
                            created_at_utc,
                            direction,
                            payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event.event_id,
                            event.calculation_id,
                            event.strategy_id,
                            event.strategy_version,
                            event.configuration_hash,
                            event.instrument_id,
                            event.source_bar_id,
                            signal_ts,
                            event.signal_bar_utc,
                            event.created_at_utc,
                            event.direction.value,
                            event_payload,
                        ),
                    )

                connection.execute(
                    """
                    INSERT INTO internal_signal_state (
                        instrument_id,
                        last_source_bar_id,
                        last_signal_bar_ts,
                        last_calculation_id,
                        last_event_id,
                        last_success_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (instrument_id) DO UPDATE SET
                        last_source_bar_id = CASE
                            WHEN excluded.last_signal_bar_ts >=
                                 internal_signal_state.last_signal_bar_ts
                            THEN excluded.last_source_bar_id
                            ELSE internal_signal_state.last_source_bar_id
                        END,
                        last_signal_bar_ts = MAX(
                            internal_signal_state.last_signal_bar_ts,
                            excluded.last_signal_bar_ts
                        ),
                        last_calculation_id = CASE
                            WHEN excluded.last_signal_bar_ts >=
                                 internal_signal_state.last_signal_bar_ts
                            THEN excluded.last_calculation_id
                            ELSE internal_signal_state.last_calculation_id
                        END,
                        last_event_id = CASE
                            WHEN excluded.last_signal_bar_ts >=
                                 internal_signal_state.last_signal_bar_ts
                            THEN excluded.last_event_id
                            ELSE internal_signal_state.last_event_id
                        END,
                        last_success_at_utc = CASE
                            WHEN excluded.last_signal_bar_ts >=
                                 internal_signal_state.last_signal_bar_ts
                            THEN excluded.last_success_at_utc
                            ELSE internal_signal_state.last_success_at_utc
                        END
                    """,
                    (
                        calculation.instrument_id,
                        calculation.source_bar_id,
                        signal_ts,
                        calculation.calculation_id,
                        (
                            None
                            if calculation.event is None
                            else calculation.event.event_id
                        ),
                        calculation.calculated_at_utc,
                    ),
                )
                connection.commit()
                return calculation
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, SignalStoreError):
                    raise
                raise SignalStoreError(
                    "cannot publish signal calculation: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_latest_calculation(
        self,
        instrument_id: str,
    ) -> SignalCalculationV1 | None:
        return self.reader.read_latest_calculation(instrument_id)
