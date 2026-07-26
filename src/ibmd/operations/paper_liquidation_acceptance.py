from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ibmd.execution.adapters import (
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_liquidation import SQLiteLiquidationStore
from ibmd.foundation.atomic_json import read_json_object
from ibmd.foundation.time import format_utc, utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactSink,
    PaperAcceptanceArtifactStore,
    SubprocessJsonCommandExecutor,
)


class PaperLiquidationAcceptanceError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        stage: str,
        broker_exposure_possible: bool = False,
    ) -> None:
        super().__init__(message)
        self.stage = str(stage)
        self.broker_exposure_possible = bool(broker_exposure_possible)


@dataclass(frozen=True)
class PaperLiquidationAcceptancePathsV1:
    repo_root: Path
    execution_database: Path
    position_feed_database: Path
    catalog_root: Path
    entry_summary: Path

    def __post_init__(self) -> None:
        for field_name in (
            "repo_root",
            "execution_database",
            "position_feed_database",
            "catalog_root",
            "entry_summary",
        ):
            object.__setattr__(
                self,
                field_name,
                Path(getattr(self, field_name)).resolve(),
            )


@dataclass(frozen=True)
class PaperLiquidationAcceptancePolicyV1:
    environment: str
    account_id: str
    deployment_id: str
    strategy_id: str
    instrument_id: str
    max_invocations: int
    poll_seconds: float
    position_max_age_seconds: float
    reconciliation_read_attempts: int
    reconciliation_poll_seconds: float
    commission_wait_seconds: float
    cancel_client_id_offset: int
    submit_client_id_offset: int
    reconciliation_client_id_offset: int
    paths: PaperLiquidationAcceptancePathsV1

    def __post_init__(self) -> None:
        environment = str(self.environment or "").strip().lower()
        account_id = str(self.account_id or "").strip()
        deployment_id = str(self.deployment_id or "").strip()
        strategy_id = str(self.strategy_id or "").strip()
        instrument_id = str(self.instrument_id or "").strip()
        if environment != "paper":
            raise PaperLiquidationAcceptanceError(
                "paper liquidation acceptance requires IBMD_ENVIRONMENT=paper",
                stage="configuration",
            )
        if not account_id.upper().startswith("D"):
            raise PaperLiquidationAcceptanceError(
                "configured account does not look like an IB paper account",
                stage="configuration",
            )
        if "paper-drill" not in deployment_id.lower():
            raise PaperLiquidationAcceptanceError(
                "paper liquidation acceptance requires a dedicated deployment_id "
                "containing 'paper-drill'",
                stage="configuration",
            )
        if not strategy_id:
            raise PaperLiquidationAcceptanceError(
                "strategy_id is required",
                stage="configuration",
            )
        if not instrument_id:
            raise PaperLiquidationAcceptanceError(
                "instrument_id is required",
                stage="configuration",
            )
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "deployment_id", deployment_id)
        object.__setattr__(self, "strategy_id", strategy_id)
        object.__setattr__(self, "instrument_id", instrument_id)
        if not isinstance(self.paths, PaperLiquidationAcceptancePathsV1):
            raise PaperLiquidationAcceptanceError(
                "paths must be PaperLiquidationAcceptancePathsV1",
                stage="configuration",
            )
        for field_name in ("max_invocations", "reconciliation_read_attempts"):
            value = int(getattr(self, field_name))
            if value <= 0:
                raise PaperLiquidationAcceptanceError(
                    f"{field_name} must be positive",
                    stage="configuration",
                )
            object.__setattr__(self, field_name, value)
        for field_name in (
            "poll_seconds",
            "position_max_age_seconds",
            "reconciliation_poll_seconds",
            "commission_wait_seconds",
        ):
            value = float(getattr(self, field_name))
            if value < 0.0 or (
                field_name == "position_max_age_seconds" and value <= 0.0
            ):
                raise PaperLiquidationAcceptanceError(
                    f"{field_name} has an invalid value: {value}",
                    stage="configuration",
                )
            object.__setattr__(self, field_name, value)
        offsets = tuple(
            int(item)
            for item in (
                self.cancel_client_id_offset,
                self.submit_client_id_offset,
                self.reconciliation_client_id_offset,
            )
        )
        if any(item < 0 for item in offsets) or len(set(offsets)) != 3:
            raise PaperLiquidationAcceptanceError(
                "cancel/submit/reconciliation client ID offsets must be distinct "
                "non-negative integers",
                stage="configuration",
            )
        object.__setattr__(self, "cancel_client_id_offset", offsets[0])
        object.__setattr__(self, "submit_client_id_offset", offsets[1])
        object.__setattr__(self, "reconciliation_client_id_offset", offsets[2])


@dataclass(frozen=True)
class FlatPositionProofV1:
    accepted: bool
    reason: str
    snapshot_id: str | None
    captured_at_utc: str | None
    source_freshness_seconds: float | None
    open_contract_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reason": self.reason,
            "snapshot_id": self.snapshot_id,
            "captured_at_utc": self.captured_at_utc,
            "source_freshness_seconds": self.source_freshness_seconds,
            "open_contract_count": self.open_contract_count,
        }


@dataclass(frozen=True)
class LiquidationStateObservationV1:
    liquidation_operation_id: str
    operation_state: str
    next_action: str
    liquidation_attempt_id: str | None
    attempt_no: int | None
    attempt_state: str | None
    order_ref: str | None
    trigger_count: int
    episode_status: str
    protection_status: str
    exposed_protective_order_count: int
    strategy_position_status: str
    strategy_position_side: str
    strategy_position_quantity: int

    @property
    def fully_closed(self) -> bool:
        return (
            self.operation_state == "SUCCEEDED"
            and self.attempt_state == "FILLED"
            and self.episode_status == "CLOSED"
            and self.protection_status == "CLOSED"
            and self.exposed_protective_order_count == 0
            and self.strategy_position_status == "FLAT"
            and self.strategy_position_side == "FLAT"
            and self.strategy_position_quantity == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "liquidation_operation_id": self.liquidation_operation_id,
            "operation_state": self.operation_state,
            "next_action": self.next_action,
            "liquidation_attempt_id": self.liquidation_attempt_id,
            "attempt_no": self.attempt_no,
            "attempt_state": self.attempt_state,
            "order_ref": self.order_ref,
            "trigger_count": self.trigger_count,
            "episode_status": self.episode_status,
            "protection_status": self.protection_status,
            "exposed_protective_order_count": (
                self.exposed_protective_order_count
            ),
            "strategy_position_status": self.strategy_position_status,
            "strategy_position_side": self.strategy_position_side,
            "strategy_position_quantity": self.strategy_position_quantity,
            "fully_closed": self.fully_closed,
        }


class JsonCommandExecutor(Protocol):
    def run_json(
        self,
        *,
        step_name: str,
        script: Path,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]: ...


class PaperLiquidationAcceptanceStateSource(Protocol):
    def validate_schema(self) -> None: ...

    def read_state(
        self,
        *,
        position_episode_id: str,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> LiquidationStateObservationV1: ...

    def read_flat_proof(
        self,
        *,
        account_id: str,
        instrument_id: str,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> FlatPositionProofV1: ...


class SQLitePaperLiquidationAcceptanceStateSource:
    _EXPOSED_PROTECTIVE_STATES = {
        "SUBMITTING",
        "LIVE",
        "CANCEL_REQUESTED",
        "UNKNOWN_OUTCOME",
    }

    def __init__(
        self,
        *,
        execution_database: str | Path,
        position_feed_database: str | Path,
    ) -> None:
        execution = Path(execution_database).resolve()
        self.liquidation = SQLiteLiquidationStore(execution)
        self.protection = SQLiteProtectionReader(execution)
        self.execution = SQLiteExecutionStateReader(execution)
        self.position_feed = SQLiteExecutionPositionFeedReader(
            Path(position_feed_database).resolve()
        )

    def validate_schema(self) -> None:
        self.liquidation.validate_schema()
        self.protection.validate_schema()
        self.execution.validate_schema()
        self.position_feed.validate_schema()

    def read_state(
        self,
        *,
        position_episode_id: str,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> LiquidationStateObservationV1:
        episode = self.protection.read_episode(position_episode_id)
        protection = self.protection.read_protection_by_episode(
            position_episode_id
        )
        liquidation = self.liquidation.read_snapshot_by_episode(
            position_episode_id
        )
        position = self.execution.read_position(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )
        if (
            episode is None
            or protection is None
            or liquidation is None
            or position is None
        ):
            raise PaperLiquidationAcceptanceError(
                "liquidation acceptance state is incomplete",
                stage="state-proof",
                broker_exposure_possible=True,
            )
        attempt = liquidation.attempt
        exposed = sum(
            item.state.value in self._EXPOSED_PROTECTIVE_STATES
            for item in protection.orders
        )
        return LiquidationStateObservationV1(
            liquidation_operation_id=(
                liquidation.operation.liquidation_operation_id
            ),
            operation_state=liquidation.operation.state.value,
            next_action=liquidation.operation.next_action.value,
            liquidation_attempt_id=(
                None if attempt is None else attempt.liquidation_attempt_id
            ),
            attempt_no=None if attempt is None else attempt.attempt_no,
            attempt_state=None if attempt is None else attempt.state.value,
            order_ref=None if attempt is None else attempt.order_ref,
            trigger_count=len(liquidation.triggers),
            episode_status=episode.status.value,
            protection_status=protection.status.value,
            exposed_protective_order_count=int(exposed),
            strategy_position_status=position.projection_status.value,
            strategy_position_side=position.side.value,
            strategy_position_quantity=position.quantity,
        )

    def read_flat_proof(
        self,
        *,
        account_id: str,
        instrument_id: str,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> FlatPositionProofV1:
        snapshot = self.position_feed.read_latest_complete()
        if snapshot is None:
            return FlatPositionProofV1(
                accepted=False,
                reason="no_complete_position_snapshot",
                snapshot_id=None,
                captured_at_utc=None,
                source_freshness_seconds=None,
                open_contract_count=0,
            )
        freshness = snapshot.freshness(
            observed_at_utc=observed_at_utc,
            max_age_seconds=max_age_seconds,
        )
        relevant = [
            row
            for row in snapshot.rows
            if abs(float(row.signed_quantity)) > 1e-9
            and (
                row.symbol.upper() == instrument_id.upper()
                or str(row.local_symbol or "").upper().startswith(
                    instrument_id.upper()
                )
            )
        ]
        reasons = []
        if snapshot.account_id != account_id:
            reasons.append("account_mismatch")
        if not freshness.is_fresh:
            reasons.append("snapshot_stale")
        if relevant:
            reasons.append(f"open_contract_count={len(relevant)}")
        accepted = not reasons
        return FlatPositionProofV1(
            accepted=accepted,
            reason="accepted" if accepted else ";".join(reasons),
            snapshot_id=snapshot.snapshot_id,
            captured_at_utc=snapshot.captured_at_utc,
            source_freshness_seconds=freshness.age_seconds,
            open_contract_count=len(relevant),
        )


@dataclass(frozen=True)
class PaperLiquidationAcceptanceResultV1:
    source_drill_id: str
    position_episode_id: str
    liquidation_operation_id: str
    liquidation_attempt_id: str
    order_ref: str
    started_at_utc: str
    finished_at_utc: str
    invocation_count: int
    take_profit_cancel_count: int
    stop_cancel_count: int
    market_close_submission_count: int
    state: LiquidationStateObservationV1
    flat_proof: FlatPositionProofV1
    artifact_directory: str
    resumed_existing_operation: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": "PaperLiquidationAcceptanceResult",
            "schema_version": 1,
            "source_drill_id": self.source_drill_id,
            "position_episode_id": self.position_episode_id,
            "liquidation_operation_id": self.liquidation_operation_id,
            "liquidation_attempt_id": self.liquidation_attempt_id,
            "order_ref": self.order_ref,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "invocation_count": self.invocation_count,
            "take_profit_cancel_count": self.take_profit_cancel_count,
            "stop_cancel_count": self.stop_cancel_count,
            "market_close_submission_count": (
                self.market_close_submission_count
            ),
            "broker_mutation_count": (
                self.take_profit_cancel_count
                + self.stop_cancel_count
                + self.market_close_submission_count
            ),
            "state": self.state.to_dict(),
            "flat_proof": self.flat_proof.to_dict(),
            "artifact_directory": self.artifact_directory,
            "resumed_existing_operation": self.resumed_existing_operation,
            "automatic_retry_enabled": False,
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "legacy_database_compatibility_required": False,
        }


class PaperLiquidationAcceptanceRunner:
    def __init__(
        self,
        *,
        policy: PaperLiquidationAcceptancePolicyV1,
        command_executor: JsonCommandExecutor,
        state_source: PaperLiquidationAcceptanceStateSource,
        artifacts: PaperAcceptanceArtifactSink,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.policy = policy
        self.command_executor = command_executor
        self.state_source = state_source
        self.artifacts = artifacts
        self.clock = clock
        self.sleeper = sleeper

    def _script(self) -> Path:
        return (
            self.policy.paths.repo_root
            / "apps"
            / "run_execution_liquidation_v2.py"
        )

    def _run_json(
        self,
        *,
        step_name: str,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]:
        return self.command_executor.run_json(
            step_name=step_name,
            script=self._script(),
            arguments=arguments,
        )

    @staticmethod
    def _mapping(
        value: object,
        *,
        field_name: str,
        stage: str,
        broker_exposure_possible: bool = False,
    ) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise PaperLiquidationAcceptanceError(
                f"{field_name} must be a JSON object",
                stage=stage,
                broker_exposure_possible=broker_exposure_possible,
            )
        return value

    @staticmethod
    def _text(
        value: object,
        *,
        field_name: str,
        stage: str,
        broker_exposure_possible: bool = False,
    ) -> str:
        text = str(value or "").strip()
        if not text:
            raise PaperLiquidationAcceptanceError(
                f"{field_name} is required",
                stage=stage,
                broker_exposure_possible=broker_exposure_possible,
            )
        return text

    def _load_entry_summary(self) -> tuple[str, str]:
        path = self.policy.paths.entry_summary
        try:
            value = read_json_object(path)
        except Exception as exc:
            raise PaperLiquidationAcceptanceError(
                f"cannot read entry acceptance summary {path}: {exc}",
                stage="entry-summary",
            ) from exc
        if (
            value.get("schema_name") != "PaperAcceptanceResult"
            or value.get("schema_version") != 1
        ):
            raise PaperLiquidationAcceptanceError(
                "entry summary is not PaperAcceptanceResult v1",
                stage="entry-summary",
            )
        position_proof = self._mapping(
            value.get("position_proof"),
            field_name="position_proof",
            stage="entry-summary",
        )
        protection = self._mapping(
            value.get("protection"),
            field_name="protection",
            stage="entry-summary",
        )
        if position_proof.get("accepted") is not True:
            raise PaperLiquidationAcceptanceError(
                "entry summary does not contain accepted broker position proof",
                stage="entry-summary",
            )
        if (
            protection.get("fully_live") is not True
            or protection.get("stop_state") != "LIVE"
            or protection.get("take_profit_state") not in {"LIVE", "NOT_REQUIRED"}
        ):
            raise PaperLiquidationAcceptanceError(
                "entry summary does not prove live protective orders",
                stage="entry-summary",
            )
        if value.get("live_position_left_protected") is not True:
            raise PaperLiquidationAcceptanceError(
                "entry summary does not declare a protected live position",
                stage="entry-summary",
            )
        return (
            self._text(
                value.get("drill_id"),
                field_name="drill_id",
                stage="entry-summary",
            ),
            self._text(
                value.get("position_episode_id"),
                field_name="position_episode_id",
                stage="entry-summary",
            ),
        )

    def _request_arguments(
        self,
        *,
        position_episode_id: str,
        source_drill_id: str,
    ) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--request-position-episode-id",
            position_episode_id,
            "--reason",
            "MANUAL_EMERGENCY",
            "--source-ref",
            f"paper-liquidation-acceptance:{source_drill_id}",
            "--execution-database",
            str(paths.execution_database),
            "--position-feed-database",
            str(paths.position_feed_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
            "--position-max-age-seconds",
            str(self.policy.position_max_age_seconds),
        )

    def _paper_arguments(self, position_episode_id: str) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--once-paper-position-episode-id",
            position_episode_id,
            "--confirm-paper-account",
            self.policy.account_id,
            "--execution-database",
            str(paths.execution_database),
            "--position-feed-database",
            str(paths.position_feed_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
            "--position-max-age-seconds",
            str(self.policy.position_max_age_seconds),
            "--cancel-client-id-offset",
            str(self.policy.cancel_client_id_offset),
            "--submit-client-id-offset",
            str(self.policy.submit_client_id_offset),
            "--reconciliation-client-id-offset",
            str(self.policy.reconciliation_client_id_offset),
            "--reconciliation-read-attempts",
            str(self.policy.reconciliation_read_attempts),
            "--reconciliation-poll-seconds",
            str(self.policy.reconciliation_poll_seconds),
            "--commission-wait-seconds",
            str(self.policy.commission_wait_seconds),
        )

    def _state(self, position_episode_id: str) -> LiquidationStateObservationV1:
        return self.state_source.read_state(
            position_episode_id=position_episode_id,
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )

    @staticmethod
    def _assert_not_unsafe_payload(payload: Mapping[str, Any]) -> None:
        operation = PaperLiquidationAcceptanceRunner._mapping(
            payload.get("liquidation_operation"),
            field_name="liquidation_operation",
            stage="liquidation",
            broker_exposure_possible=True,
        )
        attempt = payload.get("liquidation_attempt")
        if operation.get("state") == "FAILED_OPERATOR_REQUIRED":
            raise PaperLiquidationAcceptanceError(
                "liquidation requires operator intervention: "
                f"{operation.get('blocking_reason')}",
                stage="liquidation",
                broker_exposure_possible=True,
            )
        if isinstance(attempt, Mapping) and attempt.get("state") == "UNKNOWN_OUTCOME":
            raise PaperLiquidationAcceptanceError(
                "liquidation MARKET close outcome is UNKNOWN_OUTCOME",
                stage="liquidation",
                broker_exposure_possible=True,
            )
        mutation_error = str(payload.get("mutation_error") or "").strip()
        if mutation_error:
            raise PaperLiquidationAcceptanceError(
                f"liquidation broker action reported an error: {mutation_error}",
                stage="liquidation",
                broker_exposure_possible=True,
            )

    def run(self) -> PaperLiquidationAcceptanceResultV1:
        started = format_utc(self.clock())
        self.state_source.validate_schema()
        source_drill_id, position_episode_id = self._load_entry_summary()
        self.artifacts.write_json(
            "configuration",
            {
                "source_drill_id": source_drill_id,
                "position_episode_id": position_episode_id,
                "environment": self.policy.environment,
                "account_id": self.policy.account_id,
                "deployment_id": self.policy.deployment_id,
                "strategy_id": self.policy.strategy_id,
                "instrument_id": self.policy.instrument_id,
                "automatic_retry_enabled": False,
                "paths": {
                    "execution_database": str(
                        self.policy.paths.execution_database
                    ),
                    "position_feed_database": str(
                        self.policy.paths.position_feed_database
                    ),
                    "catalog_root": str(self.policy.paths.catalog_root),
                    "entry_summary": str(self.policy.paths.entry_summary),
                },
            },
        )
        request = self._run_json(
            step_name="liquidation-request",
            arguments=self._request_arguments(
                position_episode_id=position_episode_id,
                source_drill_id=source_drill_id,
            ),
        )
        if request.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "liquidation request unexpectedly performed broker mutation",
                stage="liquidation-request",
            )
        operation = self._mapping(
            request.get("liquidation_operation"),
            field_name="liquidation_operation",
            stage="liquidation-request",
        )
        operation_id = self._text(
            operation.get("liquidation_operation_id"),
            field_name="liquidation_operation_id",
            stage="liquidation-request",
        )
        resumed = request.get("operation_created") is False
        counts = {
            "CANCEL_TAKE_PROFIT": 0,
            "CANCEL_STOP": 0,
            "SUBMIT_MARKET_CLOSE": 0,
        }
        invocation_count = 0
        attempt_id: str | None = None
        order_ref: str | None = None
        state = self._state(position_episode_id)
        self.artifacts.write_json("liquidation-state-00", state.to_dict())
        for index in range(1, self.policy.max_invocations + 1):
            if state.fully_closed:
                break
            invocation_count += 1
            payload = self._run_json(
                step_name=f"liquidation-{index:02d}",
                arguments=self._paper_arguments(position_episode_id),
            )
            self._assert_not_unsafe_payload(payload)
            current_operation = self._mapping(
                payload.get("liquidation_operation"),
                field_name="liquidation_operation",
                stage="liquidation",
                broker_exposure_possible=True,
            )
            current_operation_id = self._text(
                current_operation.get("liquidation_operation_id"),
                field_name="liquidation_operation_id",
                stage="liquidation",
                broker_exposure_possible=True,
            )
            if current_operation_id != operation_id:
                raise PaperLiquidationAcceptanceError(
                    "liquidation operation identity changed during acceptance",
                    stage="liquidation",
                    broker_exposure_possible=True,
                )
            current_attempt = payload.get("liquidation_attempt")
            if isinstance(current_attempt, Mapping):
                current_attempt_id = self._text(
                    current_attempt.get("liquidation_attempt_id"),
                    field_name="liquidation_attempt_id",
                    stage="liquidation",
                    broker_exposure_possible=True,
                )
                current_order_ref = self._text(
                    current_attempt.get("order_ref"),
                    field_name="order_ref",
                    stage="liquidation",
                    broker_exposure_possible=True,
                )
                if attempt_id is None:
                    attempt_id = current_attempt_id
                    order_ref = current_order_ref
                elif (
                    current_attempt_id != attempt_id
                    or current_order_ref != order_ref
                ):
                    raise PaperLiquidationAcceptanceError(
                        "liquidation attempt identity changed during acceptance",
                        stage="liquidation",
                        broker_exposure_possible=True,
                    )
                if int(current_attempt.get("attempt_no") or 0) != 1:
                    raise PaperLiquidationAcceptanceError(
                        "liquidation acceptance created an unexpected second attempt",
                        stage="liquidation",
                        broker_exposure_possible=True,
                    )
            if payload.get("broker_mutation_performed") is True:
                action = str(payload.get("action") or "")
                if action not in counts:
                    raise PaperLiquidationAcceptanceError(
                        f"unexpected liquidation broker action: {action!r}",
                        stage="liquidation",
                        broker_exposure_possible=True,
                    )
                counts[action] += 1
                if counts[action] > 1:
                    raise PaperLiquidationAcceptanceError(
                        f"CRITICAL: liquidation action repeated: {action}",
                        stage="liquidation",
                        broker_exposure_possible=True,
                    )
            state = self._state(position_episode_id)
            self.artifacts.write_json(
                f"liquidation-state-{index:02d}",
                state.to_dict(),
            )
            if state.fully_closed:
                break
            if index < self.policy.max_invocations and self.policy.poll_seconds:
                self.sleeper(self.policy.poll_seconds)
        if not state.fully_closed:
            raise PaperLiquidationAcceptanceError(
                "liquidation did not reach broker-proven closed state within the "
                "bounded invocations",
                stage="liquidation",
                broker_exposure_possible=True,
            )
        if attempt_id is None or order_ref is None:
            raise PaperLiquidationAcceptanceError(
                "successful liquidation has no durable MARKET-close attempt",
                stage="liquidation",
                broker_exposure_possible=True,
            )
        if not resumed and any(value != 1 for value in counts.values()):
            raise PaperLiquidationAcceptanceError(
                "fresh protected liquidation did not report exactly one TP cancel, "
                "one STOP cancel and one MARKET close",
                stage="liquidation",
                broker_exposure_possible=True,
            )
        flat_proof = self.state_source.read_flat_proof(
            account_id=self.policy.account_id,
            instrument_id=self.policy.instrument_id,
            observed_at_utc=format_utc(self.clock()),
            max_age_seconds=self.policy.position_max_age_seconds,
        )
        self.artifacts.write_json("flat-proof", flat_proof.to_dict())
        if not flat_proof.accepted:
            raise PaperLiquidationAcceptanceError(
                f"independent position feed did not prove FLAT: {flat_proof.reason}",
                stage="flat-proof",
                broker_exposure_possible=True,
            )
        repeat = self._run_json(
            step_name="liquidation-idempotency",
            arguments=self._paper_arguments(position_episode_id),
        )
        if repeat.get("broker_mutation_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "CRITICAL: liquidation idempotency invocation reported another "
                "broker mutation",
                stage="liquidation-idempotency",
                broker_exposure_possible=True,
            )
        repeated = self._state(position_episode_id)
        self.artifacts.write_json(
            "liquidation-state-idempotency",
            repeated.to_dict(),
        )
        if repeated != state or not repeated.fully_closed:
            raise PaperLiquidationAcceptanceError(
                "liquidation state changed during idempotency proof",
                stage="liquidation-idempotency",
                broker_exposure_possible=True,
            )
        result = PaperLiquidationAcceptanceResultV1(
            source_drill_id=source_drill_id,
            position_episode_id=position_episode_id,
            liquidation_operation_id=operation_id,
            liquidation_attempt_id=attempt_id,
            order_ref=order_ref,
            started_at_utc=started,
            finished_at_utc=format_utc(self.clock()),
            invocation_count=invocation_count,
            take_profit_cancel_count=counts["CANCEL_TAKE_PROFIT"],
            stop_cancel_count=counts["CANCEL_STOP"],
            market_close_submission_count=counts["SUBMIT_MARKET_CLOSE"],
            state=repeated,
            flat_proof=flat_proof,
            artifact_directory=str(self.artifacts.directory),
            resumed_existing_operation=resumed,
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result


__all__ = [
    "FlatPositionProofV1",
    "LiquidationStateObservationV1",
    "PaperAcceptanceArtifactStore",
    "PaperLiquidationAcceptanceError",
    "PaperLiquidationAcceptancePathsV1",
    "PaperLiquidationAcceptancePolicyV1",
    "PaperLiquidationAcceptanceResultV1",
    "PaperLiquidationAcceptanceRunner",
    "SQLitePaperLiquidationAcceptanceStateSource",
    "SubprocessJsonCommandExecutor",
]
