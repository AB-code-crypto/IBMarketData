from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerOrderAttemptV1,
    BrokerOrderOperationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
)


BROKER_RECONCILIATION_SCHEMA_VERSION = 2
_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_broker_order_operations"),
    ("table", "internal_broker_order_attempts"),
    ("table", "internal_broker_operation_transitions"),
    ("table", "internal_broker_attempt_transitions"),
    ("table", "internal_broker_reconciliation_observations"),
    ("view", "public_broker_order_operations_v1"),
    ("view", "public_broker_order_attempts_v1"),
}


class BrokerReconciliationStoreError(RuntimeError):
    pass


class BrokerReconciliationSchemaError(BrokerReconciliationStoreError):
    pass


def _ts(value: str | None) -> int | None:
    return None if value is None else int(parse_utc(value).timestamp())


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise BrokerReconciliationStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise BrokerReconciliationStoreError(
            f"stored {context} payload must be an object"
        )
    return value


def _operation(payload: str) -> BrokerOrderOperationV1:
    return BrokerOrderOperationV1.from_dict(
        _json_object(payload, context="broker operation")
    )


def _attempt(payload: str) -> BrokerOrderAttemptV1:
    return BrokerOrderAttemptV1.from_dict(
        _json_object(payload, context="broker attempt")
    )


def _stable_id(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]
    return f"{kind}_{digest}"


def _transition_id(entity_id: str, sequence_no: int, state: str) -> str:
    return _stable_id(
        "broker_transition",
        f"{entity_id}:{int(sequence_no)}:{state}",
    )


def _observation_id(attempt_id: str, payload: str) -> str:
    return _stable_id("broker_observation", f"{attempt_id}:{payload}")


def _fill_observation_id(exec_id: str) -> str:
    return _stable_id("broker_fill", exec_id)


def _commission_observation_id(exec_id: str) -> str:
    return _stable_id("broker_commission", exec_id)


def _base_fill(fill: BrokerFillFactV1) -> BrokerFillFactV1:
    return replace(fill, commission=None)


class SQLiteBrokerReconciliationReader:
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
            raise BrokerReconciliationSchemaError(
                f"execution database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

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
                raise BrokerReconciliationSchemaError(
                    f"broker reconciliation schema objects are missing: {missing}"
                )
            versions = [
                int(row["version"])
                for row in connection.execute(
                    """
                    SELECT version
                    FROM schema_migrations
                    WHERE store_name = 'execution'
                    ORDER BY version
                    """
                ).fetchall()
            ]
            expected = list(
                range(1, BROKER_RECONCILIATION_SCHEMA_VERSION + 1)
            )
            if versions != expected:
                raise BrokerReconciliationSchemaError(
                    "execution reconciliation schema version mismatch: "
                    f"expected={expected}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise BrokerReconciliationSchemaError(
                f"cannot validate broker reconciliation schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def _evidence_rows(self) -> tuple[sqlite3.Row, ...]:
        connection = self._connect()
        try:
            return tuple(
                connection.execute(
                    """
                    SELECT
                        observation_id,
                        operation_id,
                        attempt_id,
                        outcome,
                        payload_json
                    FROM internal_broker_reconciliation_observations
                    WHERE outcome IN ('FILL', 'COMMISSION')
                    ORDER BY operation_id, attempt_id, sequence_no
                    """
                ).fetchall()
            )
        finally:
            connection.close()

    def read_fills(self, attempt_id: str) -> tuple[BrokerFillFactV1, ...]:
        fills: dict[str, BrokerFillFactV1] = {}
        commissions: dict[str, BrokerCommissionFactV1] = {}
        expected_attempt = str(attempt_id)
        for row in self._evidence_rows():
            if str(row["attempt_id"]) != expected_attempt:
                continue
            outcome = str(row["outcome"])
            payload = _json_object(
                str(row["payload_json"]),
                context=f"broker {outcome.lower()} evidence",
            )
            if outcome == "FILL":
                fill = BrokerFillFactV1.from_dict(payload)
                fills[fill.exec_id] = fill
            else:
                commission = BrokerCommissionFactV1.from_dict(payload)
                commissions[commission.exec_id] = commission
        return tuple(
            replace(fill, commission=commissions.get(exec_id))
            for exec_id, fill in sorted(fills.items())
        )

    def read_commission_pending_operation_ids(self) -> tuple[str, ...]:
        fills: dict[str, str] = {}
        commissions: set[str] = set()
        for row in self._evidence_rows():
            payload = _json_object(
                str(row["payload_json"]),
                context="broker reconciliation evidence",
            )
            if str(row["outcome"]) == "FILL":
                fill = BrokerFillFactV1.from_dict(payload)
                fills[fill.exec_id] = str(row["operation_id"])
            else:
                commission = BrokerCommissionFactV1.from_dict(payload)
                commissions.add(commission.exec_id)
        return tuple(sorted({
            operation_id
            for exec_id, operation_id in fills.items()
            if exec_id not in commissions
        }))
