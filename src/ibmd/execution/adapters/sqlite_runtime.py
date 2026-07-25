from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ibmd.public_contracts.decision import StrategyCommandKind


class ExecutionRuntimeReadError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionRuntimeFinalizationCandidateV1:
    operation_id: str
    command_kind: StrategyCommandKind


@dataclass(frozen=True)
class ExecutionRuntimePendingWorkV1:
    subject_id: str
    detail: str


_EXECUTION_VIEWS = {
    "public_broker_order_operations_v1",
    "public_execution_command_states_v1",
    "public_position_episodes_v1",
    "public_protection_states_v1",
    "public_protective_orders_v1",
    "public_liquidation_operations_v1",
    "public_reverse_finalizations_v1",
}
_DECISION_VIEWS = {"public_strategy_command_requests_v1"}


class SQLiteExecutionRuntimeReader:
    def __init__(
        self,
        execution_database: str | Path,
        decision_database: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.execution_database = Path(execution_database)
        self.decision_database = Path(decision_database)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")

    def _connect(self, path: Path) -> sqlite3.Connection:
        if not path.is_file():
            raise ExecutionRuntimeReadError(
                f"runtime source database does not exist: {path}"
            )
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    @staticmethod
    def _views(connection: sqlite3.Connection) -> set[str]:
        return {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='view'"
            ).fetchall()
        }

    def validate_schema(self) -> None:
        execution = self._connect(self.execution_database)
        decision = self._connect(self.decision_database)
        try:
            missing_execution = sorted(
                _EXECUTION_VIEWS - self._views(execution)
            )
            missing_decision = sorted(_DECISION_VIEWS - self._views(decision))
            if missing_execution or missing_decision:
                raise ExecutionRuntimeReadError(
                    "execution runtime public views are missing: "
                    f"execution={missing_execution}, decision={missing_decision}"
                )
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot validate execution runtime sources: {exc}"
            ) from exc
        finally:
            execution.close()
            decision.close()

    def list_open_episode_ids(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[str, ...]:
        connection = self._connect(self.execution_database)
        try:
            rows = connection.execute(
                """
                SELECT position_episode_id
                FROM public_position_episodes_v1
                WHERE account_id=?
                  AND strategy_id=?
                  AND deployment_id=?
                  AND instrument_id=?
                  AND status='OPEN'
                ORDER BY opened_at_ts, position_episode_id
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchall()
            return tuple(str(row["position_episode_id"]) for row in rows)
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read open position episodes: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_next_finalization_candidate(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionRuntimeFinalizationCandidateV1 | None:
        connection = self._connect(self.execution_database)
        try:
            row = connection.execute(
                """
                SELECT o.operation_id, c.command_kind
                FROM public_broker_order_operations_v1 o
                JOIN public_execution_command_states_v1 c
                  ON c.command_id = o.command_id
                LEFT JOIN public_position_episodes_v1 e
                  ON e.source_operation_id = o.operation_id
                LEFT JOIN public_reverse_finalizations_v1 r
                  ON r.source_operation_id = o.operation_id
                WHERE o.account_id=?
                  AND o.strategy_id=?
                  AND o.deployment_id=?
                  AND o.instrument_id=?
                  AND o.state='SUCCEEDED'
                  AND (
                        (c.command_kind='OPEN' AND e.position_episode_id IS NULL)
                     OR (c.command_kind='REVERSE' AND r.source_operation_id IS NULL)
                  )
                ORDER BY o.updated_at_ts, o.operation_id
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            if row is None:
                return None
            return ExecutionRuntimeFinalizationCandidateV1(
                operation_id=str(row["operation_id"]),
                command_kind=StrategyCommandKind(str(row["command_kind"])),
            )
        except (sqlite3.Error, ValueError) as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read finalization candidate: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_active_liquidation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionRuntimePendingWorkV1 | None:
        connection = self._connect(self.execution_database)
        try:
            row = connection.execute(
                """
                SELECT liquidation_operation_id, state, next_action
                FROM public_liquidation_operations_v1
                WHERE account_id=?
                  AND strategy_id=?
                  AND deployment_id=?
                  AND instrument_id=?
                  AND state NOT IN (
                      'SUCCEEDED',
                      'FAILED_OPERATOR_REQUIRED',
                      'CANCELLED_AS_ALREADY_FLAT'
                  )
                ORDER BY updated_at_ts, liquidation_operation_id
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            if row is None:
                return None
            return ExecutionRuntimePendingWorkV1(
                subject_id=str(row["liquidation_operation_id"]),
                detail=(
                    f"state={row['state']}, next_action={row['next_action']}"
                ),
            )
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read active liquidation operation: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_pending_reverse_handoff(
        self,
        *,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionRuntimePendingWorkV1 | None:
        connection = self._connect(self.execution_database)
        try:
            row = connection.execute(
                """
                SELECT c.command_id, c.desired_target_side,
                       c.desired_target_quantity
                FROM public_execution_command_states_v1 c
                LEFT JOIN public_broker_order_operations_v1 o
                  ON o.command_id = c.command_id
                WHERE c.strategy_id=?
                  AND c.deployment_id=?
                  AND c.instrument_id=?
                  AND c.state='ADMITTED'
                  AND c.command_kind='REVERSE'
                  AND o.operation_id IS NULL
                ORDER BY c.received_at_ts, c.command_id
                LIMIT 1
                """,
                (
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            if row is None:
                return None
            return ExecutionRuntimePendingWorkV1(
                subject_id=str(row["command_id"]),
                detail=(
                    f"target={row['desired_target_side']} "
                    f"quantity={row['desired_target_quantity']}"
                ),
            )
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read reverse handoff candidate: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_pending_protective_submission(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionRuntimePendingWorkV1 | None:
        connection = self._connect(self.execution_database)
        try:
            row = connection.execute(
                """
                SELECT o.protective_order_id, o.kind, o.state,
                       o.position_episode_id
                FROM public_protective_orders_v1 o
                JOIN public_position_episodes_v1 e
                  ON e.position_episode_id = o.position_episode_id
                WHERE e.account_id=?
                  AND e.strategy_id=?
                  AND e.deployment_id=?
                  AND e.instrument_id=?
                  AND e.status='OPEN'
                  AND o.state='PLANNED'
                  AND (
                        o.kind='STOP_LOSS'
                     OR (
                          o.kind='TAKE_PROFIT'
                          AND EXISTS (
                              SELECT 1
                              FROM public_protective_orders_v1 stop
                              WHERE stop.position_episode_id=o.position_episode_id
                                AND stop.kind='STOP_LOSS'
                                AND stop.state='LIVE'
                          )
                     )
                  )
                ORDER BY o.planned_sequence, o.protective_order_id
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            if row is None:
                return None
            return ExecutionRuntimePendingWorkV1(
                subject_id=str(row["protective_order_id"]),
                detail=(
                    f"kind={row['kind']}, episode={row['position_episode_id']}"
                ),
            )
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read pending protective submission: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_next_decision_command_id(
        self,
        *,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
    ) -> str | None:
        decision = self._connect(self.decision_database)
        execution = self._connect(self.execution_database)
        try:
            rows = decision.execute(
                """
                SELECT command_id
                FROM public_strategy_command_requests_v1
                WHERE strategy_id=?
                  AND strategy_version=?
                  AND deployment_id=?
                  AND instrument_id=?
                ORDER BY created_at_ts, command_id
                LIMIT 100
                """,
                (
                    str(strategy_id),
                    int(strategy_version),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchall()
            for row in rows:
                command_id = str(row["command_id"])
                exists = execution.execute(
                    "SELECT 1 FROM public_execution_command_states_v1 "
                    "WHERE command_id=? LIMIT 1",
                    (command_id,),
                ).fetchone()
                if exists is None:
                    return command_id
            return None
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read next decision command: {exc}"
            ) from exc
        finally:
            decision.close()
            execution.close()

    def read_pending_strategic_submission(
        self,
        *,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionRuntimePendingWorkV1 | None:
        connection = self._connect(self.execution_database)
        try:
            row = connection.execute(
                """
                SELECT c.command_id, c.command_kind,
                       c.desired_target_side, c.desired_target_quantity
                FROM public_execution_command_states_v1 c
                LEFT JOIN public_broker_order_operations_v1 o
                  ON o.command_id = c.command_id
                WHERE c.strategy_id=?
                  AND c.deployment_id=?
                  AND c.instrument_id=?
                  AND c.state='ADMITTED'
                  AND o.operation_id IS NULL
                ORDER BY c.received_at_ts, c.command_id
                LIMIT 1
                """,
                (
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            if row is None:
                return None
            return ExecutionRuntimePendingWorkV1(
                subject_id=str(row["command_id"]),
                detail=(
                    f"kind={row['command_kind']}, "
                    f"target={row['desired_target_side']}, "
                    f"quantity={row['desired_target_quantity']}"
                ),
            )
        except sqlite3.Error as exc:
            raise ExecutionRuntimeReadError(
                f"cannot read pending strategic submission: {exc}"
            ) from exc
        finally:
            connection.close()
