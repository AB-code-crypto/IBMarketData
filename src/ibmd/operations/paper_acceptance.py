from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ibmd.execution.adapters import (
    SQLiteExecutionPositionFeedReader,
    SQLiteProtectionReader,
)
from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.time import format_utc, parse_utc, utc_now

_DRILL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class PaperAcceptanceError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        stage: str,
        position_may_be_open: bool = False,
    ) -> None:
        super().__init__(message)
        self.stage = str(stage)
        self.position_may_be_open = bool(position_may_be_open)


@dataclass(frozen=True)
class PaperAcceptancePathsV1:
    repo_root: Path
    decision_database: Path
    execution_database: Path
    position_feed_database: Path
    catalog_root: Path

    def __post_init__(self) -> None:
        for field_name in (
            "repo_root",
            "decision_database",
            "execution_database",
            "position_feed_database",
            "catalog_root",
        ):
            object.__setattr__(
                self,
                field_name,
                Path(getattr(self, field_name)).resolve(),
            )


@dataclass(frozen=True)
class PaperAcceptancePolicyV1:
    environment: str
    account_id: str
    deployment_id: str
    instrument_id: str
    drill_id: str
    target_side: str
    command_ttl_seconds: int
    position_max_age_seconds: float
    entry_max_invocations: int
    entry_poll_seconds: float
    position_wait_seconds: float
    position_poll_seconds: float
    protective_max_invocations: int
    protective_poll_seconds: float
    reconciliation_read_attempts: int
    reconciliation_poll_seconds: float
    commission_wait_seconds: float
    submit_client_id_offset: int
    protective_submit_client_id_offset: int
    reconciliation_client_id_offset: int
    paths: PaperAcceptancePathsV1

    def __post_init__(self) -> None:
        environment = str(self.environment or "").strip().lower()
        account_id = str(self.account_id or "").strip()
        deployment_id = str(self.deployment_id or "").strip()
        instrument_id = str(self.instrument_id or "").strip()
        drill_id = str(self.drill_id or "").strip()
        target_side = str(self.target_side or "").strip().upper()
        if environment != "paper":
            raise PaperAcceptanceError(
                "paper acceptance requires IBMD_ENVIRONMENT=paper",
                stage="configuration",
            )
        if not account_id.upper().startswith("D"):
            raise PaperAcceptanceError(
                "configured account does not look like an IB paper account",
                stage="configuration",
            )
        if "paper-drill" not in deployment_id.lower():
            raise PaperAcceptanceError(
                "paper acceptance requires a dedicated deployment_id containing "
                "'paper-drill'",
                stage="configuration",
            )
        if not instrument_id:
            raise PaperAcceptanceError(
                "instrument_id is required",
                stage="configuration",
            )
        if not _DRILL_ID_RE.fullmatch(drill_id):
            raise PaperAcceptanceError(
                "drill_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}",
                stage="configuration",
            )
        if target_side not in {"LONG", "SHORT"}:
            raise PaperAcceptanceError(
                "target_side must be LONG or SHORT",
                stage="configuration",
            )
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "deployment_id", deployment_id)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "drill_id", drill_id)
        object.__setattr__(self, "target_side", target_side)
        if not isinstance(self.paths, PaperAcceptancePathsV1):
            raise PaperAcceptanceError(
                "paths must be PaperAcceptancePathsV1",
                stage="configuration",
            )
        integer_fields = (
            "command_ttl_seconds",
            "entry_max_invocations",
            "protective_max_invocations",
            "reconciliation_read_attempts",
        )
        for field_name in integer_fields:
            value = int(getattr(self, field_name))
            if value <= 0:
                raise PaperAcceptanceError(
                    f"{field_name} must be positive",
                    stage="configuration",
                )
            object.__setattr__(self, field_name, value)
        if not 60 <= self.command_ttl_seconds <= 900:
            raise PaperAcceptanceError(
                "command_ttl_seconds must be between 60 and 900",
                stage="configuration",
            )
        float_fields = (
            "position_max_age_seconds",
            "entry_poll_seconds",
            "position_wait_seconds",
            "position_poll_seconds",
            "protective_poll_seconds",
            "reconciliation_poll_seconds",
            "commission_wait_seconds",
        )
        for field_name in float_fields:
            value = float(getattr(self, field_name))
            if value < 0.0 or (
                field_name
                in {
                    "position_max_age_seconds",
                    "position_wait_seconds",
                }
                and value <= 0.0
            ):
                raise PaperAcceptanceError(
                    f"{field_name} has an invalid value: {value}",
                    stage="configuration",
                )
            object.__setattr__(self, field_name, value)
        offsets = tuple(
            int(item)
            for item in (
                self.submit_client_id_offset,
                self.protective_submit_client_id_offset,
                self.reconciliation_client_id_offset,
            )
        )
        if any(item < 0 for item in offsets) or len(set(offsets)) != 3:
            raise PaperAcceptanceError(
                "submit/protective/reconciliation client ID offsets must be "
                "distinct non-negative integers",
                stage="configuration",
            )
        object.__setattr__(self, "submit_client_id_offset", offsets[0])
        object.__setattr__(
            self,
            "protective_submit_client_id_offset",
            offsets[1],
        )
        object.__setattr__(
            self,
            "reconciliation_client_id_offset",
            offsets[2],
        )

    @property
    def expected_signed_quantity(self) -> int:
        return 1 if self.target_side == "LONG" else -1

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "account_id": self.account_id,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "drill_id": self.drill_id,
            "target_side": self.target_side,
            "command_ttl_seconds": self.command_ttl_seconds,
            "position_max_age_seconds": self.position_max_age_seconds,
            "entry_max_invocations": self.entry_max_invocations,
            "entry_poll_seconds": self.entry_poll_seconds,
            "position_wait_seconds": self.position_wait_seconds,
            "position_poll_seconds": self.position_poll_seconds,
            "protective_max_invocations": self.protective_max_invocations,
            "protective_poll_seconds": self.protective_poll_seconds,
            "reconciliation_read_attempts": self.reconciliation_read_attempts,
            "reconciliation_poll_seconds": self.reconciliation_poll_seconds,
            "commission_wait_seconds": self.commission_wait_seconds,
            "submit_client_id_offset": self.submit_client_id_offset,
            "protective_submit_client_id_offset": (
                self.protective_submit_client_id_offset
            ),
            "reconciliation_client_id_offset": (
                self.reconciliation_client_id_offset
            ),
            "paths": {
                "repo_root": str(self.paths.repo_root),
                "decision_database": str(self.paths.decision_database),
                "execution_database": str(self.paths.execution_database),
                "position_feed_database": str(
                    self.paths.position_feed_database
                ),
                "catalog_root": str(self.paths.catalog_root),
            },
        }


@dataclass(frozen=True)
class PositionProofV1:
    accepted: bool
    reason: str
    snapshot_id: str | None
    captured_at_utc: str | None
    source_freshness_seconds: float | None
    con_id: int | None
    local_symbol: str | None
    signed_quantity: float | None
    competing_contract_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reason": self.reason,
            "snapshot_id": self.snapshot_id,
            "captured_at_utc": self.captured_at_utc,
            "source_freshness_seconds": self.source_freshness_seconds,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "signed_quantity": self.signed_quantity,
            "competing_contract_count": self.competing_contract_count,
        }


@dataclass(frozen=True)
class ProtectionObservationV1:
    position_episode_id: str
    protection_status: str
    stop_state: str
    stop_order_ref: str
    stop_broker_order_id: int | None
    take_profit_state: str | None
    take_profit_order_ref: str | None
    take_profit_broker_order_id: int | None
    blocking_reason: str | None

    @property
    def fully_live(self) -> bool:
        return self.stop_state == "LIVE" and self.take_profit_state in {
            "LIVE",
            "NOT_REQUIRED",
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "position_episode_id": self.position_episode_id,
            "protection_status": self.protection_status,
            "stop_state": self.stop_state,
            "stop_order_ref": self.stop_order_ref,
            "stop_broker_order_id": self.stop_broker_order_id,
            "take_profit_state": self.take_profit_state,
            "take_profit_order_ref": self.take_profit_order_ref,
            "take_profit_broker_order_id": self.take_profit_broker_order_id,
            "blocking_reason": self.blocking_reason,
            "fully_live": self.fully_live,
        }


class JsonCommandExecutor(Protocol):
    def run_json(
        self,
        *,
        step_name: str,
        script: Path,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]: ...


class PaperAcceptanceStateSource(Protocol):
    def validate_schema(self) -> None: ...

    def read_position_proof(
        self,
        *,
        account_id: str,
        instrument_id: str,
        con_id: int,
        local_symbol: str,
        signed_quantity: int,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> PositionProofV1: ...

    def read_protection(
        self,
        position_episode_id: str,
    ) -> ProtectionObservationV1: ...


class PaperAcceptanceArtifactSink(Protocol):
    @property
    def directory(self) -> Path: ...

    def write_json(self, name: str, value: object) -> Path: ...


class PaperAcceptanceArtifactStore:
    def __init__(self, directory: str | Path) -> None:
        self._directory = Path(directory).resolve()
        self._directory.mkdir(parents=True, exist_ok=False)

    @property
    def directory(self) -> Path:
        return self._directory

    def write_json(self, name: str, value: object) -> Path:
        safe = _SAFE_NAME_RE.sub("-", str(name or "").strip()).strip("-")
        if not safe:
            raise ValueError("artifact name is required")
        return atomic_write_json(self._directory / f"{safe}.json", value)

    def write_text(self, name: str, value: str) -> Path:
        safe = _SAFE_NAME_RE.sub("-", str(name or "").strip()).strip("-")
        if not safe:
            raise ValueError("artifact name is required")
        target = self._directory / f"{safe}.txt"
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(str(value), encoding="utf-8")
        temporary.replace(target)
        return target


class SubprocessJsonCommandExecutor:
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
        if not self.python_executable:
            raise ValueError("python_executable is required")
        if not self.repo_root.is_dir():
            raise ValueError(f"repo_root does not exist: {self.repo_root}")
        if self.timeout_seconds <= 0.0:
            raise ValueError("timeout_seconds must be positive")
        self._sequence = 0

    def run_json(
        self,
        *,
        step_name: str,
        script: Path,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]:
        self._sequence += 1
        prefix = f"{self._sequence:02d}-{step_name}"
        resolved_script = Path(script).resolve()
        argv = [
            self.python_executable,
            str(resolved_script),
            *(str(item) for item in arguments),
        ]
        started = format_utc(utc_now())
        self.artifacts.write_json(
            f"{prefix}-command",
            {
                "argv": argv,
                "cwd": str(self.repo_root),
                "started_at_utc": started,
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
                f"child command timed out after {self.timeout_seconds}s: "
                f"{resolved_script.name}",
                stage=step_name,
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
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise PaperAcceptanceError(
                f"child command failed with exit code {completed.returncode}: "
                f"{resolved_script.name}: {detail}",
                stage=step_name,
            )
        text = completed.stdout.lstrip("\ufeff").strip()
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise PaperAcceptanceError(
                f"child command did not return one JSON object: "
                f"{resolved_script.name}: {exc}",
                stage=step_name,
            ) from exc
        if not isinstance(payload, dict):
            raise PaperAcceptanceError(
                f"child command JSON root must be an object: "
                f"{resolved_script.name}",
                stage=step_name,
            )
        self.artifacts.write_json(f"{prefix}-payload", payload)
        return payload


class SQLitePaperAcceptanceStateSource:
    def __init__(
        self,
        *,
        position_feed_database: str | Path,
        execution_database: str | Path,
    ) -> None:
        self.position_reader = SQLiteExecutionPositionFeedReader(
            Path(position_feed_database).resolve()
        )
        self.protection_reader = SQLiteProtectionReader(
            Path(execution_database).resolve()
        )

    def validate_schema(self) -> None:
        self.position_reader.validate_schema()
        self.protection_reader.validate_schema()

    def read_position_proof(
        self,
        *,
        account_id: str,
        instrument_id: str,
        con_id: int,
        local_symbol: str,
        signed_quantity: int,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> PositionProofV1:
        snapshot = self.position_reader.read_latest_complete()
        if snapshot is None:
            return PositionProofV1(
                accepted=False,
                reason="no_complete_position_snapshot",
                snapshot_id=None,
                captured_at_utc=None,
                source_freshness_seconds=None,
                con_id=None,
                local_symbol=None,
                signed_quantity=None,
                competing_contract_count=0,
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
        matching = [
            row
            for row in relevant
            if row.con_id == int(con_id)
            and str(row.local_symbol or "") == str(local_symbol)
        ]
        competing = [row for row in relevant if row not in matching]
        selected = matching[0] if len(matching) == 1 else None
        accepted = (
            snapshot.account_id == account_id
            and freshness.is_fresh
            and selected is not None
            and not competing
            and abs(float(selected.signed_quantity) - signed_quantity) <= 1e-9
        )
        reasons = []
        if snapshot.account_id != account_id:
            reasons.append("account_mismatch")
        if not freshness.is_fresh:
            reasons.append("snapshot_stale")
        if len(matching) != 1:
            reasons.append(f"matching_contract_count={len(matching)}")
        if competing:
            reasons.append(f"competing_contract_count={len(competing)}")
        if selected is not None and (
            abs(float(selected.signed_quantity) - signed_quantity) > 1e-9
        ):
            reasons.append(
                "signed_quantity_mismatch:"
                f"expected={signed_quantity},actual={selected.signed_quantity}"
            )
        return PositionProofV1(
            accepted=accepted,
            reason="accepted" if accepted else ";".join(reasons),
            snapshot_id=snapshot.snapshot_id,
            captured_at_utc=snapshot.captured_at_utc,
            source_freshness_seconds=freshness.age_seconds,
            con_id=None if selected is None else selected.con_id,
            local_symbol=(
                None if selected is None else str(selected.local_symbol or "")
            ),
            signed_quantity=(
                None if selected is None else float(selected.signed_quantity)
            ),
            competing_contract_count=len(competing),
        )

    def read_protection(
        self,
        position_episode_id: str,
    ) -> ProtectionObservationV1:
        protection = self.protection_reader.read_protection_by_episode(
            position_episode_id
        )
        if protection is None:
            raise PaperAcceptanceError(
                f"protection state does not exist: {position_episode_id}",
                stage="protection-state",
                position_may_be_open=True,
            )
        stop = protection.stop_order
        take_profit = protection.take_profit_order
        return ProtectionObservationV1(
            position_episode_id=protection.position_episode_id,
            protection_status=protection.status.value,
            stop_state=stop.state.value,
            stop_order_ref=stop.order_ref,
            stop_broker_order_id=stop.broker_order_id,
            take_profit_state=(
                None if take_profit is None else take_profit.state.value
            ),
            take_profit_order_ref=(
                None if take_profit is None else take_profit.order_ref
            ),
            take_profit_broker_order_id=(
                None if take_profit is None else take_profit.broker_order_id
            ),
            blocking_reason=protection.blocking_reason,
        )


@dataclass(frozen=True)
class PaperAcceptanceResultV1:
    drill_id: str
    started_at_utc: str
    finished_at_utc: str
    command_id: str
    operation_id: str
    attempt_id: str
    order_ref: str
    entry_submission_count: int
    entry_invocation_count: int
    position_proof: PositionProofV1
    position_episode_id: str
    stop_submission_count: int
    take_profit_submission_count: int
    protection: ProtectionObservationV1
    artifact_directory: str
    resumed_existing_command: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": "PaperAcceptanceResult",
            "schema_version": 1,
            "drill_id": self.drill_id,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "command_id": self.command_id,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
            "order_ref": self.order_ref,
            "entry_submission_count": self.entry_submission_count,
            "entry_invocation_count": self.entry_invocation_count,
            "position_proof": self.position_proof.to_dict(),
            "position_episode_id": self.position_episode_id,
            "stop_submission_count": self.stop_submission_count,
            "take_profit_submission_count": self.take_profit_submission_count,
            "protection": self.protection.to_dict(),
            "broker_mutation_count": (
                self.entry_submission_count
                + self.stop_submission_count
                + self.take_profit_submission_count
            ),
            "artifact_directory": self.artifact_directory,
            "resumed_existing_command": self.resumed_existing_command,
            "automatic_retry_enabled": False,
            "manual_cleanup_required": True,
            "live_position_left_protected": True,
            "legacy_database_compatibility_required": False,
        }


class PaperAcceptanceDrillRunner:
    def __init__(
        self,
        *,
        policy: PaperAcceptancePolicyV1,
        command_executor: JsonCommandExecutor,
        state_source: PaperAcceptanceStateSource,
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

    def _script(self, name: str) -> Path:
        return self.policy.paths.repo_root / "apps" / name

    def _run_json(
        self,
        *,
        step_name: str,
        script_name: str,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]:
        return self.command_executor.run_json(
            step_name=step_name,
            script=self._script(script_name),
            arguments=arguments,
        )

    @staticmethod
    def _mapping(
        value: object,
        *,
        field_name: str,
        stage: str = "validation",
        position_may_be_open: bool = False,
    ) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise PaperAcceptanceError(
                f"{field_name} must be a JSON object",
                stage=stage,
                position_may_be_open=position_may_be_open,
            )
        return value

    @staticmethod
    def _text(
        value: object,
        *,
        field_name: str,
        stage: str = "validation",
        position_may_be_open: bool = False,
    ) -> str:
        text = str(value or "").strip()
        if not text:
            raise PaperAcceptanceError(
                f"{field_name} is required",
                stage=stage,
                position_may_be_open=position_may_be_open,
            )
        return text

    def _prepare(self) -> tuple[Mapping[str, Any], str, int, str, int, bool]:
        paths = self.policy.paths
        payload = self._run_json(
            step_name="prepare",
            script_name="prepare_execution_paper_drill_v2.py",
            arguments=(
                "--prepare",
                "--drill-id",
                self.policy.drill_id,
                "--target-side",
                self.policy.target_side,
                "--confirm-paper-account",
                self.policy.account_id,
                "--command-ttl-seconds",
                str(self.policy.command_ttl_seconds),
                "--position-max-age-seconds",
                str(self.policy.position_max_age_seconds),
                "--decision-database",
                str(paths.decision_database),
                "--execution-database",
                str(paths.execution_database),
                "--position-feed-database",
                str(paths.position_feed_database),
                "--catalog-root",
                str(paths.catalog_root),
                "--instrument",
                self.policy.instrument_id,
            ),
        )
        if payload.get("ready_for_submit") is not True:
            raise PaperAcceptanceError(
                "paper drill preparation did not become ready_for_submit",
                stage="prepare",
            )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperAcceptanceError(
                "paper drill preparation unexpectedly performed broker mutation",
                stage="prepare",
            )
        command = self._mapping(payload.get("command"), field_name="command")
        state = self._mapping(
            payload.get("command_state"),
            field_name="command_state",
        )
        fixture = self._mapping(
            payload.get("execution_fixture"),
            field_name="execution_fixture",
        )
        position = self._mapping(
            fixture.get("position"),
            field_name="execution_fixture.position",
        )
        readiness = self._mapping(
            fixture.get("readiness"),
            field_name="execution_fixture.readiness",
        )
        active = self._mapping(
            payload.get("active_contract"),
            field_name="active_contract",
        )
        session = self._mapping(payload.get("session"), field_name="session")
        if command.get("command_kind") != "OPEN":
            raise PaperAcceptanceError(
                "paper acceptance requires an OPEN command",
                stage="prepare",
            )
        if command.get("desired_target_side") != self.policy.target_side:
            raise PaperAcceptanceError(
                "prepared target side differs from acceptance policy",
                stage="prepare",
            )
        quantity = int(command.get("desired_target_quantity", 0))
        if quantity != 1:
            raise PaperAcceptanceError(
                f"paper acceptance requires quantity=1, actual={quantity}",
                stage="prepare",
            )
        if (
            state.get("state") != "ADMITTED"
            or state.get("command_id") != command.get("command_id")
        ):
            raise PaperAcceptanceError(
                "prepared command state is not ADMITTED for the same command",
                stage="prepare",
            )
        if position.get("projection_status") != "FLAT":
            raise PaperAcceptanceError(
                "prepared broker position is not FLAT",
                stage="prepare",
            )
        if (
            readiness.get("status") != "READY"
            or readiness.get("broker_actions_enabled") is not True
            or readiness.get("reconciliation_complete") is not True
            or readiness.get("clock_healthy") is not True
        ):
            raise PaperAcceptanceError(
                "prepared execution readiness is not broker-action READY",
                stage="prepare",
            )
        if session.get("phase") != "TRADING":
            raise PaperAcceptanceError(
                "paper acceptance requires a TRADING session",
                stage="prepare",
            )
        if active.get("contract_is_active") is not True:
            raise PaperAcceptanceError(
                "prepared contract is not active",
                stage="prepare",
            )
        submit_before = parse_utc(
            self._text(payload.get("submit_before_utc"), field_name="submit_before_utc")
        )
        if self.clock() >= submit_before:
            raise PaperAcceptanceError(
                "prepared position proof expired before entry submission",
                stage="prepare",
            )
        return (
            payload,
            self._text(command.get("command_id"), field_name="command_id"),
            int(active.get("con_id", 0)),
            self._text(active.get("local_symbol"), field_name="local_symbol"),
            quantity,
            bool(payload.get("reused_existing_command")),
        )

    def _entry_arguments(self, command_id: str) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--once-command-id",
            command_id,
            "--confirm-paper-account",
            self.policy.account_id,
            "--decision-database",
            str(paths.decision_database),
            "--execution-database",
            str(paths.execution_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
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

    @staticmethod
    def _entry_identity(payload: Mapping[str, Any]) -> tuple[str, str, str, int, str]:
        values = (
            str(payload.get("command_id") or "").strip(),
            str(payload.get("operation_id") or "").strip(),
            str(payload.get("attempt_id") or "").strip(),
            int(payload.get("attempt_no") or 0),
            str(payload.get("order_ref") or "").strip(),
        )
        if not all((values[0], values[1], values[2], values[4])) or values[3] != 1:
            raise PaperAcceptanceError(
                f"entry broker identity is incomplete or attempt_no != 1: {values}",
                stage="entry",
                position_may_be_open=True,
            )
        return values

    @staticmethod
    def _entry_succeeded(payload: Mapping[str, Any], quantity: int) -> bool:
        return (
            payload.get("operation_state") == "SUCCEEDED"
            and payload.get("attempt_state") == "FILLED"
            and int(payload.get("filled_qty") or 0) == quantity
            and int(payload.get("remaining_qty") or 0) == 0
        )

    @staticmethod
    def _entry_terminal_failure(payload: Mapping[str, Any]) -> bool:
        return payload.get("operation_state") in {
            "FAILED_RETRYABLE",
            "FAILED_OPERATOR_REQUIRED",
        } or payload.get("attempt_state") in {
            "CANCELLED",
            "REJECTED",
            "FAILED",
        }

    def _complete_entry(
        self,
        *,
        command_id: str,
        quantity: int,
        resumed_existing_command: bool,
    ) -> tuple[tuple[str, str, str, int, str], int, int]:
        identity = None
        submission_count = 0
        invocation_count = 0
        succeeded = False
        for index in range(1, self.policy.entry_max_invocations + 1):
            payload = self._run_json(
                step_name=f"entry-{index:02d}",
                script_name="run_execution_submit_v2.py",
                arguments=self._entry_arguments(command_id),
            )
            invocation_count += 1
            current_identity = self._entry_identity(payload)
            if current_identity[0] != command_id:
                raise PaperAcceptanceError(
                    "entry response belongs to another command",
                    stage="entry",
                    position_may_be_open=True,
                )
            if identity is None:
                identity = current_identity
            elif current_identity != identity:
                raise PaperAcceptanceError(
                    "entry operation/attempt identity changed across invocations",
                    stage="entry",
                    position_may_be_open=True,
                )
            if payload.get("submission_performed") is True:
                submission_count += 1
                if submission_count > 1:
                    raise PaperAcceptanceError(
                        "CRITICAL: entry placeOrder was reported more than once",
                        stage="entry",
                        position_may_be_open=True,
                    )
            if self._entry_terminal_failure(payload):
                raise PaperAcceptanceError(
                    "entry reached a proven terminal failure: "
                    f"operation={payload.get('operation_state')}, "
                    f"attempt={payload.get('attempt_state')}, "
                    f"reason={payload.get('blocking_reason')}",
                    stage="entry",
                    position_may_be_open=submission_count > 0,
                )
            if self._entry_succeeded(payload, quantity):
                succeeded = True
                break
            if index < self.policy.entry_max_invocations:
                self.sleeper(self.policy.entry_poll_seconds)
        if identity is None or not succeeded:
            raise PaperAcceptanceError(
                "entry outcome remained unproven after bounded reconciliation",
                stage="entry",
                position_may_be_open=submission_count > 0,
            )
        if not resumed_existing_command and submission_count != 1:
            raise PaperAcceptanceError(
                "fresh paper acceptance did not report exactly one entry submission",
                stage="entry",
                position_may_be_open=True,
            )
        repeat = self._run_json(
            step_name="entry-idempotency",
            script_name="run_execution_submit_v2.py",
            arguments=self._entry_arguments(command_id),
        )
        invocation_count += 1
        if repeat.get("submission_performed") is not False:
            raise PaperAcceptanceError(
                "CRITICAL: idempotency invocation reported another entry submission",
                stage="entry-idempotency",
                position_may_be_open=True,
            )
        if self._entry_identity(repeat) != identity:
            raise PaperAcceptanceError(
                "entry identity changed during idempotency proof",
                stage="entry-idempotency",
                position_may_be_open=True,
            )
        if not self._entry_succeeded(repeat, quantity):
            raise PaperAcceptanceError(
                "entry idempotency invocation no longer proves FILLED/SUCCEEDED",
                stage="entry-idempotency",
                position_may_be_open=True,
            )
        return identity, submission_count, invocation_count

    def _wait_for_position(
        self,
        *,
        con_id: int,
        local_symbol: str,
        quantity: int,
    ) -> PositionProofV1:
        deadline = self.clock().timestamp() + self.policy.position_wait_seconds
        index = 0
        last = None
        while True:
            index += 1
            observed = format_utc(self.clock())
            signed = quantity if self.policy.target_side == "LONG" else -quantity
            last = self.state_source.read_position_proof(
                account_id=self.policy.account_id,
                instrument_id=self.policy.instrument_id,
                con_id=con_id,
                local_symbol=local_symbol,
                signed_quantity=signed,
                observed_at_utc=observed,
                max_age_seconds=self.policy.position_max_age_seconds,
            )
            self.artifacts.write_json(
                f"position-proof-{index:02d}",
                last.to_dict(),
            )
            if last.accepted:
                return last
            if self.clock().timestamp() >= deadline:
                break
            self.sleeper(self.policy.position_poll_seconds)
        raise PaperAcceptanceError(
            "position feed did not prove the filled entry before timeout: "
            f"{None if last is None else last.reason}",
            stage="position-proof",
            position_may_be_open=True,
        )

    def _plan_protection(
        self,
        *,
        operation_id: str,
        con_id: int,
        local_symbol: str,
        quantity: int,
    ) -> tuple[str, Mapping[str, Any]]:
        paths = self.policy.paths
        payload = self._run_json(
            step_name="protection-plan",
            script_name="run_execution_protection_v2.py",
            arguments=(
                "--plan-from-operation",
                operation_id,
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
            ),
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperAcceptanceError(
                "protection planning unexpectedly performed broker mutation",
                stage="protection-plan",
                position_may_be_open=True,
            )
        episode = self._mapping(
            payload.get("position_episode"),
            field_name="position_episode",
            stage="protection-plan",
            position_may_be_open=True,
        )
        position = self._mapping(
            payload.get("strategy_position"),
            field_name="strategy_position",
            stage="protection-plan",
            position_may_be_open=True,
        )
        protection = self._mapping(
            payload.get("protection"),
            field_name="protection",
            stage="protection-plan",
            position_may_be_open=True,
        )
        episode_id = self._text(
            episode.get("position_episode_id"),
            field_name="position_episode_id",
            stage="protection-plan",
            position_may_be_open=True,
        )
        expected = {
            "status": "OPEN",
            "source_operation_id": operation_id,
            "side": self.policy.target_side,
            "quantity": quantity,
            "con_id": con_id,
            "local_symbol": local_symbol,
        }
        actual = {key: episode.get(key) for key in expected}
        if actual != expected:
            raise PaperAcceptanceError(
                f"position episode differs from filled entry: "
                f"expected={expected}, actual={actual}",
                stage="protection-plan",
                position_may_be_open=True,
            )
        if (
            position.get("position_episode_id") != episode_id
            or position.get("projection_status") != "OPEN"
            or position.get("side") != self.policy.target_side
            or int(position.get("quantity") or 0) != quantity
        ):
            raise PaperAcceptanceError(
                "strategy position does not reference the planned OPEN episode",
                stage="protection-plan",
                position_may_be_open=True,
            )
        orders = protection.get("orders")
        if not isinstance(orders, list):
            raise PaperAcceptanceError(
                "protection orders must be a list",
                stage="protection-plan",
                position_may_be_open=True,
            )
        by_kind = {
            str(item.get("kind")): item
            for item in orders
            if isinstance(item, Mapping)
        }
        stop = by_kind.get("STOP_LOSS")
        take_profit = by_kind.get("TAKE_PROFIT")
        if (
            stop is None
            or stop.get("state") != "PLANNED"
            or int(stop.get("planned_sequence") or 0) != 1
        ):
            raise PaperAcceptanceError(
                "protection plan has no first PLANNED STOP_LOSS",
                stage="protection-plan",
                position_may_be_open=True,
            )
        if take_profit is None or (
            take_profit.get("state") not in {"PLANNED", "NOT_REQUIRED"}
            or int(take_profit.get("planned_sequence") or 0) != 2
        ):
            raise PaperAcceptanceError(
                "TAKE_PROFIT plan is missing or invalid",
                stage="protection-plan",
                position_may_be_open=True,
            )
        return episode_id, payload

    def _protective_arguments(self, position_episode_id: str) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--once-position-episode-id",
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
            "--submit-client-id-offset",
            str(self.policy.protective_submit_client_id_offset),
            "--reconciliation-client-id-offset",
            str(self.policy.reconciliation_client_id_offset),
            "--position-max-age-seconds",
            str(self.policy.position_max_age_seconds),
            "--reconciliation-read-attempts",
            str(self.policy.reconciliation_read_attempts),
            "--reconciliation-poll-seconds",
            str(self.policy.reconciliation_poll_seconds),
            "--commission-wait-seconds",
            str(self.policy.commission_wait_seconds),
        )

    @staticmethod
    def _assert_protection_safe(
        observation: ProtectionObservationV1,
    ) -> None:
        if observation.protection_status in {
            "UNPROTECTED",
            "OPERATOR_REQUIRED",
        }:
            raise PaperAcceptanceError(
                "protection entered an unsafe state: "
                f"status={observation.protection_status}, "
                f"reason={observation.blocking_reason}",
                stage="protective-submit",
                position_may_be_open=True,
            )
        if observation.stop_state in {
            "FILLED",
            "CANCELLED",
            "REJECTED",
            "FAILED",
            "UNKNOWN_OUTCOME",
            "NOT_REQUIRED",
        }:
            raise PaperAcceptanceError(
                f"STOP did not remain protective: {observation.stop_state}",
                stage="protective-submit",
                position_may_be_open=True,
            )
        if observation.take_profit_state in {
            "FILLED",
            "CANCELLED",
            "REJECTED",
            "FAILED",
            "UNKNOWN_OUTCOME",
        }:
            raise PaperAcceptanceError(
                "TAKE_PROFIT reached an unexpected state during acceptance: "
                f"{observation.take_profit_state}",
                stage="protective-submit",
                position_may_be_open=True,
            )

    def _complete_protection(
        self,
        *,
        position_episode_id: str,
        resumed_existing_command: bool,
    ) -> tuple[int, int, ProtectionObservationV1]:
        stop_submissions = 0
        take_profit_submissions = 0
        final = self.state_source.read_protection(position_episode_id)
        self.artifacts.write_json("protection-state-00", final.to_dict())
        for index in range(1, self.policy.protective_max_invocations + 1):
            if final.fully_live:
                break
            payload = self._run_json(
                step_name=f"protective-{index:02d}",
                script_name="run_execution_protective_submit_v2.py",
                arguments=self._protective_arguments(position_episode_id),
            )
            if payload.get("submission_performed") is True:
                kind = payload.get("order_kind")
                if kind == "STOP_LOSS":
                    stop_submissions += 1
                    if stop_submissions > 1:
                        raise PaperAcceptanceError(
                            "CRITICAL: STOP placeOrder was reported more than once",
                            stage="protective-submit",
                            position_may_be_open=True,
                        )
                elif kind == "TAKE_PROFIT":
                    take_profit_submissions += 1
                    if take_profit_submissions > 1:
                        raise PaperAcceptanceError(
                            "CRITICAL: TAKE_PROFIT placeOrder was reported more than once",
                            stage="protective-submit",
                            position_may_be_open=True,
                        )
                else:
                    raise PaperAcceptanceError(
                        f"protective mutation has an unknown order kind: {kind!r}",
                        stage="protective-submit",
                        position_may_be_open=True,
                    )
            final = self.state_source.read_protection(position_episode_id)
            self.artifacts.write_json(
                f"protection-state-{index:02d}",
                final.to_dict(),
            )
            self._assert_protection_safe(final)
            if final.fully_live:
                break
            if index < self.policy.protective_max_invocations:
                self.sleeper(self.policy.protective_poll_seconds)
        if not final.fully_live:
            raise PaperAcceptanceError(
                "STOP/TAKE_PROFIT did not both become LIVE within the bounded "
                "protective invocations",
                stage="protective-submit",
                position_may_be_open=True,
            )
        if not resumed_existing_command and (
            stop_submissions != 1 or take_profit_submissions != 1
        ):
            raise PaperAcceptanceError(
                "fresh paper acceptance did not report exactly one STOP and one "
                "TAKE_PROFIT submission",
                stage="protective-submit",
                position_may_be_open=True,
            )
        repeat = self._run_json(
            step_name="protective-idempotency",
            script_name="run_execution_protective_submit_v2.py",
            arguments=self._protective_arguments(position_episode_id),
        )
        if repeat.get("submission_performed") is not False:
            raise PaperAcceptanceError(
                "CRITICAL: protective idempotency invocation reported a broker "
                "submission",
                stage="protective-idempotency",
                position_may_be_open=True,
            )
        repeated = self.state_source.read_protection(position_episode_id)
        if repeated != final or not repeated.fully_live:
            raise PaperAcceptanceError(
                "protective state changed during idempotency proof",
                stage="protective-idempotency",
                position_may_be_open=True,
            )
        return stop_submissions, take_profit_submissions, repeated

    def run(self) -> PaperAcceptanceResultV1:
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
        identity, entry_submissions, entry_invocations = self._complete_entry(
            command_id=command_id,
            quantity=quantity,
            resumed_existing_command=resumed,
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
        stop_count, tp_count, protection = self._complete_protection(
            position_episode_id=episode_id,
            resumed_existing_command=resumed,
        )
        result = PaperAcceptanceResultV1(
            drill_id=self.policy.drill_id,
            started_at_utc=started,
            finished_at_utc=format_utc(self.clock()),
            command_id=identity[0],
            operation_id=identity[1],
            attempt_id=identity[2],
            order_ref=identity[4],
            entry_submission_count=entry_submissions,
            entry_invocation_count=entry_invocations,
            position_proof=position_proof,
            position_episode_id=episode_id,
            stop_submission_count=stop_count,
            take_profit_submission_count=tp_count,
            protection=protection,
            artifact_directory=str(self.artifacts.directory),
            resumed_existing_command=resumed,
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result
