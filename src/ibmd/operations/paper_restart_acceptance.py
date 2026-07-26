from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    PaperAcceptanceDrillRunner,
    PaperAcceptanceError,
    PaperAcceptancePolicyV1,
    PaperAcceptanceStateSource,
    PositionProofV1,
    ProtectionObservationV1,
)
from ibmd.operations.restart_probe import RESTART_PROBE_EXIT_CODE


@dataclass(frozen=True)
class RestartSubmitCheckpointV1:
    mutation_kind: str
    broker_order_id: int
    order_ref: str
    submitted_at_utc: str
    expected_exit_code: int
    request: Mapping[str, Any]
    raw: Mapping[str, Any]

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        expected_kind: str,
    ) -> "RestartSubmitCheckpointV1":
        if (
            value.get("schema_name") != "PaperRestartSubmitCheckpoint"
            or int(value.get("schema_version") or 0) != 1
        ):
            raise PaperAcceptanceError(
                "restart checkpoint has an unsupported schema",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        kind = str(value.get("mutation_kind") or "").strip()
        if kind != expected_kind:
            raise PaperAcceptanceError(
                "restart checkpoint mutation kind differs from the expected "
                f"kind: expected={expected_kind}, actual={kind}",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        if value.get("reconciliation_started") is not False:
            raise PaperAcceptanceError(
                "restart checkpoint was written after reconciliation started",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        if value.get("automatic_retry_enabled") is not False:
            raise PaperAcceptanceError(
                "restart checkpoint unexpectedly enables automatic retry",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        receipt = value.get("receipt")
        request = value.get("request")
        if not isinstance(receipt, Mapping) or not isinstance(request, Mapping):
            raise PaperAcceptanceError(
                "restart checkpoint request/receipt must be JSON objects",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        broker_order_id = int(receipt.get("broker_order_id") or 0)
        order_ref = str(receipt.get("order_ref") or "").strip()
        submitted_at = str(receipt.get("submitted_at_utc") or "").strip()
        expected_exit = int(value.get("expected_exit_code") or 0)
        if broker_order_id <= 0 or not order_ref or not submitted_at:
            raise PaperAcceptanceError(
                "restart checkpoint broker receipt is incomplete",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        parse_utc(submitted_at)
        if expected_exit != RESTART_PROBE_EXIT_CODE:
            raise PaperAcceptanceError(
                "restart checkpoint exit code differs from the runner contract",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        if (
            int(request.get("broker_order_id") or 0) != broker_order_id
            or str(request.get("order_ref") or "").strip() != order_ref
        ):
            raise PaperAcceptanceError(
                "restart checkpoint request and receipt identities differ",
                stage="restart-checkpoint",
                position_may_be_open=True,
            )
        return cls(
            mutation_kind=kind,
            broker_order_id=broker_order_id,
            order_ref=order_ref,
            submitted_at_utc=submitted_at,
            expected_exit_code=expected_exit,
            request=dict(request),
            raw=dict(value),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


class ExpectedRestartCrashExecutor:
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
        expected_kind: str,
    ) -> RestartSubmitCheckpointV1:
        self._sequence += 1
        prefix = f"restart-{self._sequence:02d}-{step_name}"
        checkpoint_file = self.artifacts.directory / f"{prefix}-checkpoint.json"
        resolved_script = Path(script).resolve()
        argv = [
            self.python_executable,
            str(resolved_script),
            *(str(item) for item in arguments),
            "--drill-crash-after-submit",
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
                "expected_mutation_kind": expected_kind,
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
            raise PaperAcceptanceError(
                "restart probe timed out after a possible broker mutation",
                stage=step_name,
                position_may_be_open=True,
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
            raise PaperAcceptanceError(
                "restart probe did not terminate at the expected checkpoint: "
                f"exit={completed.returncode}, detail={detail}",
                stage=step_name,
                position_may_be_open=True,
            )
        if not checkpoint_file.is_file():
            raise PaperAcceptanceError(
                "restart probe exited without its atomic submit checkpoint",
                stage=step_name,
                position_may_be_open=True,
            )
        try:
            raw = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PaperAcceptanceError(
                f"restart checkpoint cannot be read: {exc}",
                stage=step_name,
                position_may_be_open=True,
            ) from exc
        if not isinstance(raw, Mapping):
            raise PaperAcceptanceError(
                "restart checkpoint JSON root must be an object",
                stage=step_name,
                position_may_be_open=True,
            )
        checkpoint = RestartSubmitCheckpointV1.from_mapping(
            raw,
            expected_kind=expected_kind,
        )
        self.artifacts.write_json(
            f"{prefix}-checkpoint-validated",
            checkpoint.to_dict(),
        )
        return checkpoint


@dataclass(frozen=True)
class PaperRestartAcceptanceResultV1:
    drill_id: str
    started_at_utc: str
    finished_at_utc: str
    command_id: str
    operation_id: str
    attempt_id: str
    order_ref: str
    position_episode_id: str
    position_proof: PositionProofV1
    protection: ProtectionObservationV1
    market_checkpoint: RestartSubmitCheckpointV1
    stop_checkpoint: RestartSubmitCheckpointV1
    take_profit_checkpoint: RestartSubmitCheckpointV1 | None
    entry_resume_invocations: int
    protective_resume_invocations: int
    artifact_directory: str

    def to_dict(self) -> dict[str, Any]:
        checkpoints = [
            self.market_checkpoint,
            self.stop_checkpoint,
            *(
                ()
                if self.take_profit_checkpoint is None
                else (self.take_profit_checkpoint,)
            ),
        ]
        return {
            "schema_name": "PaperRestartAcceptanceResult",
            "schema_version": 1,
            "drill_id": self.drill_id,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "command_id": self.command_id,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
            "order_ref": self.order_ref,
            "position_episode_id": self.position_episode_id,
            "position_proof": self.position_proof.to_dict(),
            "protection": self.protection.to_dict(),
            "market_checkpoint": self.market_checkpoint.to_dict(),
            "stop_checkpoint": self.stop_checkpoint.to_dict(),
            "take_profit_checkpoint": (
                None
                if self.take_profit_checkpoint is None
                else self.take_profit_checkpoint.to_dict()
            ),
            "entry_resume_invocations": self.entry_resume_invocations,
            "protective_resume_invocations": (
                self.protective_resume_invocations
            ),
            "intentional_process_terminations": len(checkpoints),
            "broker_mutation_count": len(checkpoints),
            "all_resume_submissions_false": True,
            "attempt_no": 1,
            "restart_adoption_proven": True,
            "automatic_retry_enabled": False,
            "manual_cleanup_required": True,
            "live_position_left_protected": True,
            "artifact_directory": self.artifact_directory,
        }


class PaperRestartAcceptanceRunner(PaperAcceptanceDrillRunner):
    def __init__(
        self,
        *,
        policy: PaperAcceptancePolicyV1,
        command_executor,
        crash_executor: ExpectedRestartCrashExecutor,
        state_source: PaperAcceptanceStateSource,
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

    def _resume_entry_after_crash(
        self,
        *,
        command_id: str,
        quantity: int,
        checkpoint: RestartSubmitCheckpointV1,
    ) -> tuple[tuple[str, str, str, int, str], int]:
        identity = None
        invocations = 0
        for index in range(1, self.policy.entry_max_invocations + 1):
            payload = self._run_json(
                step_name=f"entry-resume-{index:02d}",
                script_name="run_execution_submit_v2.py",
                arguments=self._entry_arguments(command_id),
            )
            invocations += 1
            if payload.get("submission_performed") is not False:
                raise PaperAcceptanceError(
                    "CRITICAL: restart resume reported another MARKET submission",
                    stage="entry-restart",
                    position_may_be_open=True,
                )
            current = self._entry_identity(payload)
            if current[0] != command_id:
                raise PaperAcceptanceError(
                    "restart resume belongs to another command",
                    stage="entry-restart",
                    position_may_be_open=True,
                )
            if identity is None:
                identity = current
            elif identity != current:
                raise PaperAcceptanceError(
                    "entry identity changed during restart adoption",
                    stage="entry-restart",
                    position_may_be_open=True,
                )
            if (
                current[4] != checkpoint.order_ref
                or int(payload.get("broker_order_id") or 0)
                != checkpoint.broker_order_id
            ):
                raise PaperAcceptanceError(
                    "resumed entry broker identity differs from the crash checkpoint",
                    stage="entry-restart",
                    position_may_be_open=True,
                )
            if self._entry_terminal_failure(payload):
                raise PaperAcceptanceError(
                    "entry reached a proven terminal failure after restart: "
                    f"operation={payload.get('operation_state')}, "
                    f"attempt={payload.get('attempt_state')}, "
                    f"reason={payload.get('blocking_reason')}",
                    stage="entry-restart",
                    position_may_be_open=True,
                )
            if self._entry_succeeded(payload, quantity):
                break
            if index < self.policy.entry_max_invocations:
                self.sleeper(self.policy.entry_poll_seconds)
        else:
            raise PaperAcceptanceError(
                "entry was not adopted after the intentional restart",
                stage="entry-restart",
                position_may_be_open=True,
            )
        if identity is None:
            raise PaperAcceptanceError(
                "restart adoption produced no entry identity",
                stage="entry-restart",
                position_may_be_open=True,
            )
        repeat = self._run_json(
            step_name="entry-restart-idempotency",
            script_name="run_execution_submit_v2.py",
            arguments=self._entry_arguments(command_id),
        )
        invocations += 1
        if (
            repeat.get("submission_performed") is not False
            or self._entry_identity(repeat) != identity
            or not self._entry_succeeded(repeat, quantity)
        ):
            raise PaperAcceptanceError(
                "entry restart idempotency proof failed",
                stage="entry-restart-idempotency",
                position_may_be_open=True,
            )
        return identity, invocations

    def _resume_protective_kind(
        self,
        *,
        position_episode_id: str,
        expected_kind: str,
        checkpoint: RestartSubmitCheckpointV1,
        target_state: str,
    ) -> tuple[ProtectionObservationV1, int]:
        invocations = 0
        final = self.state_source.read_protection(position_episode_id)
        for index in range(1, self.policy.protective_max_invocations + 1):
            selected_state = (
                final.stop_state
                if expected_kind == "STOP_LOSS"
                else final.take_profit_state
            )
            if selected_state == target_state:
                break
            payload = self._run_json(
                step_name=(
                    f"{expected_kind.lower()}-resume-{index:02d}"
                ),
                script_name="run_execution_protective_submit_v2.py",
                arguments=self._protective_arguments(position_episode_id),
            )
            invocations += 1
            if payload.get("submission_performed") is not False:
                raise PaperAcceptanceError(
                    "CRITICAL: protective restart resume reported another "
                    f"submission for {expected_kind}",
                    stage="protective-restart",
                    position_may_be_open=True,
                )
            final = self.state_source.read_protection(position_episode_id)
            self.artifacts.write_json(
                f"{expected_kind.lower()}-resume-state-{index:02d}",
                final.to_dict(),
            )
            self._assert_protection_safe(final)
            selected_ref = (
                final.stop_order_ref
                if expected_kind == "STOP_LOSS"
                else final.take_profit_order_ref
            )
            selected_id = (
                final.stop_broker_order_id
                if expected_kind == "STOP_LOSS"
                else final.take_profit_broker_order_id
            )
            if (
                selected_ref != checkpoint.order_ref
                or selected_id != checkpoint.broker_order_id
            ):
                raise PaperAcceptanceError(
                    "resumed protective identity differs from the crash "
                    f"checkpoint for {expected_kind}",
                    stage="protective-restart",
                    position_may_be_open=True,
                )
            if index < self.policy.protective_max_invocations:
                self.sleeper(self.policy.protective_poll_seconds)
        selected_state = (
            final.stop_state
            if expected_kind == "STOP_LOSS"
            else final.take_profit_state
        )
        if selected_state != target_state:
            raise PaperAcceptanceError(
                f"{expected_kind} was not adopted as {target_state} after restart",
                stage="protective-restart",
                position_may_be_open=True,
            )
        return final, invocations

    def run(self) -> PaperRestartAcceptanceResultV1:
        started = format_utc(self.clock())
        self.state_source.validate_schema()
        self.artifacts.write_json("configuration", self.policy.to_dict())
        (
            _prepared,
            command_id,
            con_id,
            local_symbol,
            quantity,
            resumed,
        ) = self._prepare()
        if resumed:
            raise PaperAcceptanceError(
                "restart acceptance requires a fresh command and target root",
                stage="prepare",
            )

        market_checkpoint = self.crash_executor.run_expected_crash(
            step_name="market-submit-crash",
            script=self._script("run_execution_submit_v2.py"),
            arguments=self._entry_arguments(command_id),
            expected_kind="MARKET_ENTRY",
        )
        identity, entry_resume_count = self._resume_entry_after_crash(
            command_id=command_id,
            quantity=quantity,
            checkpoint=market_checkpoint,
        )
        position_proof = self._wait_for_position(
            con_id=con_id,
            local_symbol=local_symbol,
            quantity=quantity,
        )
        episode_id, _plan = self._plan_protection(
            operation_id=identity[1],
            con_id=con_id,
            local_symbol=local_symbol,
            quantity=quantity,
        )

        initial = self.state_source.read_protection(episode_id)
        if initial.stop_state != "PLANNED":
            raise PaperAcceptanceError(
                "restart acceptance expected a PLANNED STOP",
                stage="stop-restart",
                position_may_be_open=True,
            )
        stop_checkpoint = self.crash_executor.run_expected_crash(
            step_name="stop-submit-crash",
            script=self._script("run_execution_protective_submit_v2.py"),
            arguments=self._protective_arguments(episode_id),
            expected_kind="STOP_LOSS",
        )
        protection, stop_resume_count = self._resume_protective_kind(
            position_episode_id=episode_id,
            expected_kind="STOP_LOSS",
            checkpoint=stop_checkpoint,
            target_state="LIVE",
        )

        take_profit_checkpoint = None
        tp_resume_count = 0
        if protection.take_profit_state == "PLANNED":
            take_profit_checkpoint = self.crash_executor.run_expected_crash(
                step_name="take-profit-submit-crash",
                script=self._script("run_execution_protective_submit_v2.py"),
                arguments=self._protective_arguments(episode_id),
                expected_kind="TAKE_PROFIT",
            )
            protection, tp_resume_count = self._resume_protective_kind(
                position_episode_id=episode_id,
                expected_kind="TAKE_PROFIT",
                checkpoint=take_profit_checkpoint,
                target_state="LIVE",
            )
        elif protection.take_profit_state != "NOT_REQUIRED":
            raise PaperAcceptanceError(
                "TAKE_PROFIT is neither PLANNED nor NOT_REQUIRED after STOP "
                "restart adoption",
                stage="take-profit-restart",
                position_may_be_open=True,
            )

        if not protection.fully_live:
            raise PaperAcceptanceError(
                "restart acceptance did not finish with live protection",
                stage="protective-restart",
                position_may_be_open=True,
            )
        repeat = self._run_json(
            step_name="protective-restart-idempotency",
            script_name="run_execution_protective_submit_v2.py",
            arguments=self._protective_arguments(episode_id),
        )
        if repeat.get("submission_performed") is not False:
            raise PaperAcceptanceError(
                "CRITICAL: protective restart idempotency submitted another order",
                stage="protective-restart-idempotency",
                position_may_be_open=True,
            )
        repeated = self.state_source.read_protection(episode_id)
        if repeated != protection or not repeated.fully_live:
            raise PaperAcceptanceError(
                "protective state changed during restart idempotency proof",
                stage="protective-restart-idempotency",
                position_may_be_open=True,
            )

        result = PaperRestartAcceptanceResultV1(
            drill_id=self.policy.drill_id,
            started_at_utc=started,
            finished_at_utc=format_utc(self.clock()),
            command_id=identity[0],
            operation_id=identity[1],
            attempt_id=identity[2],
            order_ref=identity[4],
            position_episode_id=episode_id,
            position_proof=position_proof,
            protection=repeated,
            market_checkpoint=market_checkpoint,
            stop_checkpoint=stop_checkpoint,
            take_profit_checkpoint=take_profit_checkpoint,
            entry_resume_invocations=entry_resume_count,
            protective_resume_invocations=(
                stop_resume_count + tp_resume_count + 1
            ),
            artifact_directory=str(self.artifacts.directory),
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result
