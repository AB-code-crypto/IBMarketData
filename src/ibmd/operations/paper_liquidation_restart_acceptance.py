from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ibmd.foundation.atomic_json import read_json_object
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactStore
from ibmd.operations.paper_liquidation_acceptance import (
    FlatPositionProofV1,
    LiquidationStateObservationV1,
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePolicyV1,
    PaperLiquidationAcceptanceRunner,
    PaperLiquidationAcceptanceStateSource,
    SQLitePaperLiquidationAcceptanceStateSource,
)
from ibmd.operations.paper_restart_acceptance import (
    RestartSubmitCheckpointV1,
)
from ibmd.operations.restart_probe import RESTART_PROBE_EXIT_CODE


@dataclass(frozen=True)
class ProtectiveCancelIdentityV1:
    action: str
    state: str
    broker_order_id: int
    order_ref: str


class PaperLiquidationRestartStateSource(
    PaperLiquidationAcceptanceStateSource,
    Protocol,
):
    def read_protective_cancel_identity(
        self,
        *,
        position_episode_id: str,
        action: str,
    ) -> ProtectiveCancelIdentityV1: ...


class SQLitePaperLiquidationRestartStateSource(
    SQLitePaperLiquidationAcceptanceStateSource
):
    def read_protective_cancel_identity(
        self,
        *,
        position_episode_id: str,
        action: str,
    ) -> ProtectiveCancelIdentityV1:
        protection = self.protection.read_protection_by_episode(
            position_episode_id
        )
        if protection is None:
            raise PaperLiquidationAcceptanceError(
                "position episode has no protection state",
                stage="protective-identity",
                broker_exposure_possible=True,
            )
        if action == "CANCEL_TAKE_PROFIT":
            order = protection.take_profit_order
        elif action == "CANCEL_STOP":
            order = protection.stop_order
        else:
            raise PaperLiquidationAcceptanceError(
                f"unsupported protective cancellation action: {action}",
                stage="protective-identity",
                broker_exposure_possible=True,
            )
        if (
            order is None
            or order.state.value != "LIVE"
            or order.broker_order_id is None
        ):
            raise PaperLiquidationAcceptanceError(
                f"{action} has no unique LIVE broker order identity",
                stage="protective-identity",
                broker_exposure_possible=True,
            )
        return ProtectiveCancelIdentityV1(
            action=action,
            state=order.state.value,
            broker_order_id=order.broker_order_id,
            order_ref=order.order_ref,
        )


@dataclass(frozen=True)
class RestartCancelCheckpointV1:
    mutation_kind: str
    broker_order_id: int
    order_ref: str
    cancel_requested_at_utc: str
    expected_exit_code: int
    raw: Mapping[str, Any]

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "RestartCancelCheckpointV1":
        if (
            value.get("schema_name") != "PaperRestartCancelCheckpoint"
            or int(value.get("schema_version") or 0) != 1
        ):
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint has an unsupported schema",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        if value.get("mutation_kind") != "CANCEL_ORDER":
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint has an unexpected mutation kind",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        if value.get("reconciliation_started") is not False:
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint was written after reconciliation",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        if value.get("automatic_retry_enabled") is not False:
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint unexpectedly enables retry",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        request = value.get("request")
        receipt = value.get("receipt")
        if not isinstance(request, Mapping) or not isinstance(receipt, Mapping):
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint request/receipt must be objects",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        broker_order_id = int(receipt.get("broker_order_id") or 0)
        order_ref = str(receipt.get("order_ref") or "").strip()
        cancelled_at = str(
            receipt.get("cancel_requested_at_utc") or ""
        ).strip()
        exit_code = int(value.get("expected_exit_code") or 0)
        if broker_order_id <= 0 or not order_ref or not cancelled_at:
            raise PaperLiquidationAcceptanceError(
                "cancel restart checkpoint receipt is incomplete",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        parse_utc(cancelled_at)
        if exit_code != RESTART_PROBE_EXIT_CODE:
            raise PaperLiquidationAcceptanceError(
                "cancel checkpoint exit code differs from the runner contract",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        if (
            int(request.get("broker_order_id") or 0) != broker_order_id
            or str(request.get("order_ref") or "").strip() != order_ref
        ):
            raise PaperLiquidationAcceptanceError(
                "cancel checkpoint request and receipt identities differ",
                stage="restart-checkpoint",
                broker_exposure_possible=True,
            )
        return cls(
            mutation_kind="CANCEL_ORDER",
            broker_order_id=broker_order_id,
            order_ref=order_ref,
            cancel_requested_at_utc=cancelled_at,
            expected_exit_code=exit_code,
            raw=dict(value),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True)
class LiquidationRestartCheckpointV1:
    action: str
    broker_order_id: int
    order_ref: str
    acknowledged_at_utc: str
    raw: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "broker_order_id": self.broker_order_id,
            "order_ref": self.order_ref,
            "acknowledged_at_utc": self.acknowledged_at_utc,
            "raw_checkpoint": dict(self.raw),
        }


class ExpectedLiquidationRestartCrashExecutor:
    def __init__(
        self,
        *,
        python_executable: str,
        repo_root: str | Path,
        artifacts: PaperAcceptanceArtifactStore,
        timeout_seconds: float = 180.0,
    ) -> None:
        self.python_executable = str(python_executable or "").strip()
        self.repo_root = Path(repo_root).resolve()
        self.artifacts = artifacts
        self.timeout_seconds = float(timeout_seconds)
        self._sequence = 0
        if not self.python_executable:
            raise ValueError("python_executable is required")
        if not self.repo_root.is_dir():
            raise ValueError(f"repo_root does not exist: {self.repo_root}")
        if self.timeout_seconds <= 0.0:
            raise ValueError("timeout_seconds must be positive")

    def run_expected_crash(
        self,
        *,
        step_name: str,
        script: Path,
        arguments: Sequence[str],
        expected_action: str,
        expected_order_ref: str | None = None,
        expected_broker_order_id: int | None = None,
    ) -> LiquidationRestartCheckpointV1:
        self._sequence += 1
        prefix = f"liquidation-restart-{self._sequence:02d}-{step_name}"
        checkpoint_file = self.artifacts.directory / f"{prefix}-checkpoint.json"
        argv = [
            self.python_executable,
            str(Path(script).resolve()),
            *(str(item) for item in arguments),
            "--drill-crash-after-broker-action",
            "--drill-crash-checkpoint-file",
            str(checkpoint_file),
        ]
        self.artifacts.write_json(
            f"{prefix}-command",
            {
                "argv": argv,
                "cwd": str(self.repo_root),
                "started_at_utc": format_utc(utc_now()),
                "expected_exit_code": RESTART_PROBE_EXIT_CODE,
                "expected_action": expected_action,
                "expected_order_ref": expected_order_ref,
                "expected_broker_order_id": expected_broker_order_id,
            },
        )
        try:
            completed = subprocess.run(
                argv,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            self.artifacts.write_text(
                f"{prefix}-stdout",
                "" if exc.stdout is None else str(exc.stdout),
            )
            self.artifacts.write_text(
                f"{prefix}-stderr",
                "" if exc.stderr is None else str(exc.stderr),
            )
            raise PaperLiquidationAcceptanceError(
                "liquidation restart probe timed out after possible broker "
                "exposure",
                stage=step_name,
                broker_exposure_possible=True,
            ) from exc
        self.artifacts.write_text(f"{prefix}-stdout", completed.stdout)
        self.artifacts.write_text(f"{prefix}-stderr", completed.stderr)
        self.artifacts.write_json(
            f"{prefix}-result",
            {
                "returncode": completed.returncode,
                "finished_at_utc": format_utc(utc_now()),
            },
        )
        if completed.returncode != RESTART_PROBE_EXIT_CODE:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise PaperLiquidationAcceptanceError(
                "liquidation restart probe did not terminate at its checkpoint: "
                f"exit={completed.returncode}, detail={detail}",
                stage=step_name,
                broker_exposure_possible=True,
            )
        if not checkpoint_file.is_file():
            raise PaperLiquidationAcceptanceError(
                "liquidation restart probe exited without an atomic checkpoint",
                stage=step_name,
                broker_exposure_possible=True,
            )
        try:
            raw = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PaperLiquidationAcceptanceError(
                f"liquidation restart checkpoint cannot be read: {exc}",
                stage=step_name,
                broker_exposure_possible=True,
            ) from exc
        if not isinstance(raw, Mapping):
            raise PaperLiquidationAcceptanceError(
                "liquidation restart checkpoint root must be an object",
                stage=step_name,
                broker_exposure_possible=True,
            )
        if expected_action in {"CANCEL_TAKE_PROFIT", "CANCEL_STOP"}:
            cancel = RestartCancelCheckpointV1.from_mapping(raw)
            checkpoint = LiquidationRestartCheckpointV1(
                action=expected_action,
                broker_order_id=cancel.broker_order_id,
                order_ref=cancel.order_ref,
                acknowledged_at_utc=cancel.cancel_requested_at_utc,
                raw=cancel.to_dict(),
            )
        elif expected_action == "SUBMIT_MARKET_CLOSE":
            submit = RestartSubmitCheckpointV1.from_mapping(
                raw,
                expected_kind="LIQUIDATION_MARKET_CLOSE",
            )
            checkpoint = LiquidationRestartCheckpointV1(
                action=expected_action,
                broker_order_id=submit.broker_order_id,
                order_ref=submit.order_ref,
                acknowledged_at_utc=submit.submitted_at_utc,
                raw=submit.to_dict(),
            )
        else:
            raise PaperLiquidationAcceptanceError(
                f"unsupported restart action: {expected_action}",
                stage=step_name,
                broker_exposure_possible=True,
            )
        if (
            expected_order_ref is not None
            and checkpoint.order_ref != expected_order_ref
        ):
            raise PaperLiquidationAcceptanceError(
                "liquidation checkpoint orderRef differs from the expected "
                f"protective identity: expected={expected_order_ref}, "
                f"actual={checkpoint.order_ref}",
                stage=step_name,
                broker_exposure_possible=True,
            )
        if (
            expected_broker_order_id is not None
            and checkpoint.broker_order_id != expected_broker_order_id
        ):
            raise PaperLiquidationAcceptanceError(
                "liquidation checkpoint broker order ID differs from the "
                "expected protective identity",
                stage=step_name,
                broker_exposure_possible=True,
            )
        self.artifacts.write_json(
            f"{prefix}-checkpoint-validated",
            checkpoint.to_dict(),
        )
        return checkpoint


@dataclass(frozen=True)
class PaperLiquidationRestartAcceptanceResultV1:
    source_drill_id: str
    position_episode_id: str
    liquidation_operation_id: str
    liquidation_attempt_id: str
    order_ref: str
    started_at_utc: str
    finished_at_utc: str
    resume_invocation_count: int
    protective_cancel_mode: str
    checkpoints: tuple[LiquidationRestartCheckpointV1, ...]
    state: LiquidationStateObservationV1
    flat_proof: FlatPositionProofV1
    artifact_directory: str

    def to_dict(self) -> dict[str, Any]:
        actions = [item.action for item in self.checkpoints]
        return {
            "schema_name": "PaperLiquidationRestartAcceptanceResult",
            "schema_version": 1,
            "source_drill_id": self.source_drill_id,
            "position_episode_id": self.position_episode_id,
            "liquidation_operation_id": self.liquidation_operation_id,
            "liquidation_attempt_id": self.liquidation_attempt_id,
            "order_ref": self.order_ref,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "resume_invocation_count": self.resume_invocation_count,
            "protective_cancel_mode": self.protective_cancel_mode,
            "initial_advance_broker_free": True,
            "checkpoints": [item.to_dict() for item in self.checkpoints],
            "intentional_process_terminations": len(self.checkpoints),
            "broker_mutation_count": len(self.checkpoints),
            "restart_actions": actions,
            "all_resume_mutations_false": True,
            "attempt_no": 1,
            "restart_adoption_proven": True,
            "state": self.state.to_dict(),
            "flat_proof": self.flat_proof.to_dict(),
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "automatic_retry_enabled": False,
            "artifact_directory": self.artifact_directory,
        }


class PaperLiquidationRestartAcceptanceRunner(
    PaperLiquidationAcceptanceRunner
):
    def __init__(
        self,
        *,
        policy: PaperLiquidationAcceptancePolicyV1,
        command_executor,
        crash_executor: ExpectedLiquidationRestartCrashExecutor,
        state_source: PaperLiquidationRestartStateSource,
        artifacts: PaperAcceptanceArtifactStore,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        super().__init__(
            policy=policy,
            command_executor=command_executor,
            state_source=state_source,
            artifacts=artifacts,
            clock=clock,
            sleeper=sleeper,
        )
        self.crash_executor = crash_executor
        self.restart_state_source = state_source

    def _load_entry_summary(self) -> tuple[str, str]:
        path = self.policy.paths.entry_summary
        try:
            value = read_json_object(path)
        except Exception as exc:
            raise PaperLiquidationAcceptanceError(
                f"cannot read entry acceptance summary {path}: {exc}",
                stage="entry-summary",
            ) from exc
        if value.get("schema_name") not in {
            "PaperAcceptanceResult",
            "PaperRestartAcceptanceResult",
            "PaperReverseAcceptanceResult",
        } or int(value.get("schema_version") or 0) != 1:
            raise PaperLiquidationAcceptanceError(
                "entry summary is not a supported paper acceptance result",
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
                "entry summary does not prove the broker position",
                stage="entry-summary",
            )
        if (
            protection.get("fully_live") is not True
            or protection.get("stop_state") != "LIVE"
            or protection.get("take_profit_state")
            not in {"LIVE", "NOT_REQUIRED"}
        ):
            raise PaperLiquidationAcceptanceError(
                "entry summary does not prove live protection",
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

    def _normal_resume(
        self,
        *,
        position_episode_id: str,
        step_name: str,
    ) -> Mapping[str, Any]:
        payload = self._run_json(
            step_name=step_name,
            arguments=self._paper_arguments(position_episode_id),
        )
        self._assert_not_unsafe_payload(payload)
        if payload.get("broker_mutation_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "CRITICAL: liquidation restart resume performed another broker "
                "mutation",
                stage=step_name,
                broker_exposure_possible=True,
            )
        return payload

    def _initial_advance_arguments(
        self,
        position_episode_id: str,
    ) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--advance-position-episode-id",
            position_episode_id,
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

    def _initial_broker_free_advance(
        self,
        *,
        position_episode_id: str,
        operation_id: str,
    ) -> Mapping[str, Any]:
        payload = self._run_json(
            step_name="liquidation-initial-advance",
            arguments=self._initial_advance_arguments(position_episode_id),
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance unexpectedly performed a broker "
                "mutation",
                stage="liquidation-initial-advance",
            )
        if payload.get("operation_created") is not False:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance unexpectedly created an operation",
                stage="liquidation-initial-advance",
            )
        operation = self._mapping(
            payload.get("liquidation_operation"),
            field_name="liquidation_operation",
            stage="liquidation-initial-advance",
        )
        if operation.get("liquidation_operation_id") != operation_id:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance changed the operation identity",
                stage="liquidation-initial-advance",
            )
        action = self._text(
            operation.get("next_action"),
            field_name="next_action",
            stage="liquidation-initial-advance",
        )
        if action not in {"CANCEL_TAKE_PROFIT", "CANCEL_STOP"}:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance did not select a protective cancel: "
                f"{action}",
                stage="liquidation-initial-advance",
            )
        return payload

    def run(self) -> PaperLiquidationRestartAcceptanceResultV1:
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
                "intentional_exit_code": RESTART_PROBE_EXIT_CODE,
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
        if request.get("operation_created") is not True:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart acceptance requires a fresh operation",
                stage="liquidation-request",
            )
        if operation.get("next_action") != "RECONCILE_EXITS":
            raise PaperLiquidationAcceptanceError(
                "fresh liquidation operation did not start at RECONCILE_EXITS",
                stage="liquidation-request",
            )
        self._initial_broker_free_advance(
            position_episode_id=position_episode_id,
            operation_id=operation_id,
        )

        checkpoints: list[LiquidationRestartCheckpointV1] = []
        resume_count = 0
        attempt_id: str | None = None
        order_ref: str | None = None
        state = self._state(position_episode_id)
        self.artifacts.write_json("liquidation-state-00", state.to_dict())
        for index in range(1, self.policy.max_invocations + 1):
            if state.fully_closed:
                break
            action = state.next_action
            if action in {
                "CANCEL_TAKE_PROFIT",
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            }:
                if any(item.action == action for item in checkpoints):
                    raise PaperLiquidationAcceptanceError(
                        f"CRITICAL: restart action repeated: {action}",
                        stage="liquidation-restart",
                        broker_exposure_possible=True,
                    )
                expected_ref = None
                expected_id = None
                if action in {"CANCEL_TAKE_PROFIT", "CANCEL_STOP"}:
                    identity = (
                        self.restart_state_source
                        .read_protective_cancel_identity(
                            position_episode_id=position_episode_id,
                            action=action,
                        )
                    )
                    expected_ref = identity.order_ref
                    expected_id = identity.broker_order_id
                checkpoint = self.crash_executor.run_expected_crash(
                    step_name=f"{action.lower()}-crash",
                    script=self._script(),
                    arguments=self._paper_arguments(position_episode_id),
                    expected_action=action,
                    expected_order_ref=expected_ref,
                    expected_broker_order_id=expected_id,
                )
                checkpoints.append(checkpoint)
                state = self._state(position_episode_id)
                self.artifacts.write_json(
                    f"liquidation-state-{index:02d}-after-crash",
                    state.to_dict(),
                )
                if state.liquidation_operation_id != operation_id:
                    raise PaperLiquidationAcceptanceError(
                        "liquidation operation changed after the crash checkpoint",
                        stage="liquidation-restart",
                        broker_exposure_possible=True,
                    )
                if action == "SUBMIT_MARKET_CLOSE":
                    if (
                        state.liquidation_attempt_id is None
                        or state.attempt_no != 1
                        or state.order_ref != checkpoint.order_ref
                    ):
                        raise PaperLiquidationAcceptanceError(
                            "liquidation close attempt differs from its checkpoint",
                            stage="liquidation-restart",
                            broker_exposure_possible=True,
                        )
                    attempt_id = state.liquidation_attempt_id
                    order_ref = state.order_ref
                continue

            payload = self._normal_resume(
                position_episode_id=position_episode_id,
                step_name=f"liquidation-resume-{index:02d}",
            )
            resume_count += 1
            current_operation = self._mapping(
                payload.get("liquidation_operation"),
                field_name="liquidation_operation",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
            if current_operation.get("liquidation_operation_id") != operation_id:
                raise PaperLiquidationAcceptanceError(
                    "liquidation operation identity changed during restart "
                    "reconciliation",
                    stage="liquidation-restart",
                    broker_exposure_possible=True,
                )
            current_attempt = payload.get("liquidation_attempt")
            if isinstance(current_attempt, Mapping):
                current_attempt_id = self._text(
                    current_attempt.get("liquidation_attempt_id"),
                    field_name="liquidation_attempt_id",
                    stage="liquidation-restart",
                    broker_exposure_possible=True,
                )
                current_ref = self._text(
                    current_attempt.get("order_ref"),
                    field_name="order_ref",
                    stage="liquidation-restart",
                    broker_exposure_possible=True,
                )
                if int(current_attempt.get("attempt_no") or 0) != 1:
                    raise PaperLiquidationAcceptanceError(
                        "liquidation restart created a second close attempt",
                        stage="liquidation-restart",
                        broker_exposure_possible=True,
                    )
                if attempt_id is None:
                    attempt_id = current_attempt_id
                    order_ref = current_ref
                elif (
                    attempt_id != current_attempt_id
                    or order_ref != current_ref
                ):
                    raise PaperLiquidationAcceptanceError(
                        "liquidation close identity changed during restart",
                        stage="liquidation-restart",
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
                "liquidation restart acceptance did not reach the closed state",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
        entry_summary = read_json_object(self.policy.paths.entry_summary)
        protection = self._mapping(
            entry_summary.get("protection"),
            field_name="protection",
            stage="entry-summary",
        )
        actual_actions = {item.action for item in checkpoints}
        if protection.get("take_profit_state") == "LIVE":
            if "CANCEL_TAKE_PROFIT" not in actual_actions:
                raise PaperLiquidationAcceptanceError(
                    "liquidation restart omitted the TAKE PROFIT checkpoint",
                    stage="liquidation-restart",
                    broker_exposure_possible=True,
                )
            if "CANCEL_STOP" in actual_actions:
                expected_actions = {
                    "CANCEL_TAKE_PROFIT",
                    "CANCEL_STOP",
                    "SUBMIT_MARKET_CLOSE",
                }
                protective_cancel_mode = "EXPLICIT_BOTH"
            else:
                expected_actions = {
                    "CANCEL_TAKE_PROFIT",
                    "SUBMIT_MARKET_CLOSE",
                }
                protective_cancel_mode = "OCA_AUTO_CANCELLED_STOP"
        else:
            expected_actions = {
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            }
            protective_cancel_mode = "STOP_ONLY"
        if actual_actions != expected_actions:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart checkpoints differ from the expected "
                f"actions: expected={sorted(expected_actions)}, "
                f"actual={sorted(actual_actions)}",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
        if attempt_id is None or order_ref is None:
            raise PaperLiquidationAcceptanceError(
                "closed liquidation has no durable MARKET-close attempt",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
        close_checkpoints = [
            item
            for item in checkpoints
            if item.action == "SUBMIT_MARKET_CLOSE"
        ]
        if (
            len(close_checkpoints) != 1
            or close_checkpoints[0].order_ref != order_ref
        ):
            raise PaperLiquidationAcceptanceError(
                "closed liquidation attempt differs from the MARKET checkpoint",
                stage="liquidation-restart",
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
                f"position feed did not prove FLAT: {flat_proof.reason}",
                stage="flat-proof",
                broker_exposure_possible=True,
            )
        repeat = self._normal_resume(
            position_episode_id=position_episode_id,
            step_name="liquidation-restart-idempotency",
        )
        repeated = self._state(position_episode_id)
        self.artifacts.write_json(
            "liquidation-state-idempotency",
            repeated.to_dict(),
        )
        if repeated != state or not repeated.fully_closed:
            raise PaperLiquidationAcceptanceError(
                "liquidation state changed during restart idempotency proof",
                stage="liquidation-restart-idempotency",
                broker_exposure_possible=True,
            )
        if repeat.get("broker_mutation_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart idempotency performed a mutation",
                stage="liquidation-restart-idempotency",
                broker_exposure_possible=True,
            )
        result = PaperLiquidationRestartAcceptanceResultV1(
            source_drill_id=source_drill_id,
            position_episode_id=position_episode_id,
            liquidation_operation_id=operation_id,
            liquidation_attempt_id=attempt_id,
            order_ref=order_ref,
            started_at_utc=started,
            finished_at_utc=format_utc(self.clock()),
            resume_invocation_count=resume_count + 1,
            protective_cancel_mode=protective_cancel_mode,
            checkpoints=tuple(checkpoints),
            state=repeated,
            flat_proof=flat_proof,
            artifact_directory=str(self.artifacts.directory),
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result


__all__ = [
    "ExpectedLiquidationRestartCrashExecutor",
    "LiquidationRestartCheckpointV1",
    "PaperLiquidationRestartAcceptanceResultV1",
    "PaperLiquidationRestartAcceptanceRunner",
    "ProtectiveCancelIdentityV1",
    "RestartCancelCheckpointV1",
    "SQLitePaperLiquidationRestartStateSource",
]
