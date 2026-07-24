from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path

from ibmd.execution.domain import (
    ExecutionAdmission,
    ExecutionFoundationFixtureV1,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)

EXECUTION_STORE_NAME = "execution"
EXECUTION_SCHEMA_VERSION = 2

_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_execution_command_states"),
    ("table", "internal_execution_command_transitions"),
    ("table", "internal_strategy_positions"),
    ("table", "internal_execution_readiness"),
    ("table", "internal_daily_risk_states"),
    ("view", "public_execution_command_states_v1"),
    ("view", "public_strategy_positions_v1"),
    ("view", "public_execution_readiness_v1"),
    ("view", "public_daily_risk_states_v1"),
}


class ExecutionStoreError(RuntimeError):
    pass


class ExecutionSchemaError(ExecutionStoreError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ExecutionStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ExecutionStoreError(f"stored {context} payload must be an object")
    return value


def _command_from_payload(payload: str) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1.from_dict(
        _json_object(payload, context="execution command state")
    )


def _stable_transition_id(command_id: str, sequence_no: int) -> str:
    digest = hashlib.sha256(
        f"{command_id}:{int(sequence_no)}".encode("utf-8")
    ).hexdigest()[:32]
    return f"execution_transition_{digest}"


class SQLiteExecutionReader:
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
            raise ExecutionSchemaError(
                f"execution database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_execution_command_states_v1
                WHERE command_id = ?
                LIMIT 1
                """,
                (str(command_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _command_from_payload(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_transition_states(self, command_id: str) -> tuple[str, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT to_state
                FROM internal_execution_command_transitions
                WHERE command_id = ?
                ORDER BY sequence_no
                """,
                (str(command_id),),
            ).fetchall()
            return tuple(str(row["to_state"]) for row in rows)
        finally:
            connection.close()


class SQLiteExecutionStore:
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
        self.reader = SQLiteExecutionReader(
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
            raise ExecutionSchemaError(
                f"execution database does not exist: {self.database_path}"
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
                raise ExecutionSchemaError(
                    f"execution schema objects are missing: {missing}"
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
                    (EXECUTION_STORE_NAME,),
                ).fetchall()
            ]
            if versions != list(range(1, EXECUTION_SCHEMA_VERSION + 1)):
                raise ExecutionSchemaError(
                    "execution schema version mismatch: "
                    f"expected=1..{EXECUTION_SCHEMA_VERSION}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise ExecutionSchemaError(
                f"cannot validate execution schema: {exc}"
            ) from exc
        finally:
            connection.close()

    @staticmethod
    def _upsert_fixture_rows(
        connection: sqlite3.Connection,
        fixture: ExecutionFoundationFixtureV1,
    ) -> None:
        position = fixture.position
        readiness = fixture.readiness
        risk = fixture.daily_risk
        position_payload = canonical_json_text(position.to_dict())
        readiness_payload = canonical_json_text(readiness.to_dict())
        risk_payload = canonical_json_text(risk.to_dict())

        connection.execute(
            """
            INSERT INTO internal_strategy_positions (
                account_id,
                strategy_id,
                deployment_id,
                instrument_id,
                projection_status,
                side,
                quantity,
                updated_at_ts,
                updated_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, instrument_id)
            DO UPDATE SET
                projection_status = excluded.projection_status,
                side = excluded.side,
                quantity = excluded.quantity,
                updated_at_ts = excluded.updated_at_ts,
                updated_at_utc = excluded.updated_at_utc,
                payload_json = excluded.payload_json
            """,
            (
                position.account_id,
                position.strategy_id,
                position.deployment_id,
                position.instrument_id,
                position.projection_status.value,
                position.side.value,
                position.quantity,
                int(parse_utc(position.updated_at_utc).timestamp()),
                position.updated_at_utc,
                position_payload,
            ),
        )
        connection.execute(
            """
            INSERT INTO internal_execution_readiness (
                account_id,
                strategy_id,
                deployment_id,
                instrument_id,
                status,
                command_intake_enabled,
                broker_actions_enabled,
                updated_at_ts,
                updated_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, instrument_id)
            DO UPDATE SET
                status = excluded.status,
                command_intake_enabled = excluded.command_intake_enabled,
                broker_actions_enabled = excluded.broker_actions_enabled,
                updated_at_ts = excluded.updated_at_ts,
                updated_at_utc = excluded.updated_at_utc,
                payload_json = excluded.payload_json
            """,
            (
                readiness.account_id,
                readiness.strategy_id,
                readiness.deployment_id,
                readiness.instrument_id,
                readiness.status.value,
                int(readiness.command_intake_enabled),
                int(readiness.broker_actions_enabled),
                int(parse_utc(readiness.updated_at_utc).timestamp()),
                readiness.updated_at_utc,
                readiness_payload,
            ),
        )
        connection.execute(
            """
            INSERT INTO internal_daily_risk_states (
                account_id,
                strategy_id,
                deployment_id,
                trading_day,
                status,
                pnl_ready,
                cleanup_status,
                updated_at_ts,
                updated_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, trading_day)
            DO UPDATE SET
                status = excluded.status,
                pnl_ready = excluded.pnl_ready,
                cleanup_status = excluded.cleanup_status,
                updated_at_ts = excluded.updated_at_ts,
                updated_at_utc = excluded.updated_at_utc,
                payload_json = excluded.payload_json
            """,
            (
                risk.account_id,
                risk.strategy_id,
                risk.deployment_id,
                risk.trading_day,
                risk.status.value,
                int(risk.pnl_ready),
                risk.cleanup_status.value,
                int(parse_utc(risk.updated_at_utc).timestamp()),
                risk.updated_at_utc,
                risk_payload,
            ),
        )

    def publish_fixture(
        self,
        fixture: ExecutionFoundationFixtureV1,
    ) -> tuple[ExecutionReadinessV1, StrategyPositionV1, DailyRiskStateV1]:
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._upsert_fixture_rows(connection, fixture)
                connection.commit()
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ExecutionStoreError):
                    raise
                raise ExecutionStoreError(
                    "cannot publish execution foundation fixture: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()
        return fixture.readiness, fixture.position, fixture.daily_risk

    def publish_admission(
        self,
        admission: ExecutionAdmission,
    ) -> ExecutionCommandStateV1:
        state = admission.command_state
        state_payload = canonical_json_text(state.to_dict())
        fixture_payload = canonical_json_text(admission.fixture_payload)
        fixture_hash = hashlib.sha256(
            fixture_payload.encode("utf-8")
        ).hexdigest()

        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                fixture = ExecutionFoundationFixtureV1.from_dict(
                    admission.fixture_payload
                )
                self._upsert_fixture_rows(connection, fixture)
                existing = connection.execute(
                    """
                    SELECT payload_json, fixture_hash
                    FROM internal_execution_command_states
                    WHERE command_id = ?
                    LIMIT 1
                    """,
                    (state.command_id,),
                ).fetchone()
                if existing is not None:
                    stored = _command_from_payload(str(existing["payload_json"]))
                    if (
                        stored.to_dict() != state.to_dict()
                        or str(existing["fixture_hash"]) != fixture_hash
                    ):
                        raise ExecutionStoreError(
                            "conflicting execution command state already exists"
                        )
                    connection.rollback()
                    return stored

                connection.execute(
                    """
                    INSERT INTO internal_execution_command_states (
                        command_id,
                        strategy_id,
                        strategy_version,
                        deployment_id,
                        instrument_id,
                        command_kind,
                        desired_target_side,
                        desired_target_quantity,
                        state,
                        requested_qty,
                        filled_qty,
                        remaining_qty,
                        received_at_ts,
                        received_at_utc,
                        updated_at_ts,
                        updated_at_utc,
                        terminal_at_ts,
                        terminal_at_utc,
                        blocking_reason,
                        fixture_hash,
                        fixture_payload_json,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        state.command_id,
                        state.strategy_id,
                        state.strategy_version,
                        state.deployment_id,
                        state.instrument_id,
                        state.command_kind.value,
                        state.desired_target_side.value,
                        state.desired_target_quantity,
                        state.state.value,
                        state.requested_qty,
                        state.filled_qty,
                        state.remaining_qty,
                        int(parse_utc(state.received_at_utc).timestamp()),
                        state.received_at_utc,
                        int(parse_utc(state.updated_at_utc).timestamp()),
                        state.updated_at_utc,
                        (
                            None
                            if state.terminal_at_utc is None
                            else int(parse_utc(state.terminal_at_utc).timestamp())
                        ),
                        state.terminal_at_utc,
                        state.blocking_reason,
                        fixture_hash,
                        fixture_payload,
                        state_payload,
                    ),
                )

                transitions = [
                    (1, None, ExecutionCommandState.RECEIVED.value, None),
                    (
                        2,
                        ExecutionCommandState.RECEIVED.value,
                        state.state.value,
                        state.blocking_reason,
                    ),
                ]
                for sequence_no, from_state, to_state, reason in transitions:
                    connection.execute(
                        """
                        INSERT INTO internal_execution_command_transitions (
                            transition_id,
                            command_id,
                            sequence_no,
                            from_state,
                            to_state,
                            reason,
                            occurred_at_ts,
                            occurred_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            _stable_transition_id(state.command_id, sequence_no),
                            state.command_id,
                            sequence_no,
                            from_state,
                            to_state,
                            reason,
                            int(parse_utc(state.updated_at_utc).timestamp()),
                            state.updated_at_utc,
                        ),
                    )
                connection.commit()
                return state
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ExecutionStoreError):
                    raise
                raise ExecutionStoreError(
                    "cannot publish execution command admission: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None:
        return self.reader.read_command_state(command_id)

    def read_transition_states(self, command_id: str) -> tuple[str, ...]:
        return self.reader.read_transition_states(command_id)
