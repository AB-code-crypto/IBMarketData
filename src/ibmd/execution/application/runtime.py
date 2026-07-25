from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc, utc_now


class ExecutionRuntimeError(RuntimeError):
    pass


class ExecutionRuntimeStage(str, Enum):
    STRATEGIC_RECONCILIATION = "STRATEGIC_RECONCILIATION"
    PROTECTIVE_RECONCILIATION = "PROTECTIVE_RECONCILIATION"
    LIQUIDATION_ADVANCE = "LIQUIDATION_ADVANCE"
    POSITION_FINALIZATION = "POSITION_FINALIZATION"
    POSITION_PROJECTION = "POSITION_PROJECTION"
    DAILY_RISK = "DAILY_RISK"
    LIQUIDATION_TRIGGERS = "LIQUIDATION_TRIGGERS"
    REVERSE_HANDOFF = "REVERSE_HANDOFF"
    PROTECTIVE_SUBMISSION = "PROTECTIVE_SUBMISSION"
    COMMAND_ADMISSION = "COMMAND_ADMISSION"
    STRATEGIC_SUBMISSION = "STRATEGIC_SUBMISSION"


EXECUTION_RUNTIME_STAGE_ORDER: tuple[ExecutionRuntimeStage, ...] = (
    ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
    ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
    ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
    ExecutionRuntimeStage.POSITION_FINALIZATION,
    ExecutionRuntimeStage.POSITION_PROJECTION,
    ExecutionRuntimeStage.DAILY_RISK,
    ExecutionRuntimeStage.LIQUIDATION_TRIGGERS,
    ExecutionRuntimeStage.REVERSE_HANDOFF,
    ExecutionRuntimeStage.PROTECTIVE_SUBMISSION,
    ExecutionRuntimeStage.COMMAND_ADMISSION,
    ExecutionRuntimeStage.STRATEGIC_SUBMISSION,
)


class ExecutionRuntimeStageStatus(str, Enum):
    NO_ACTION = "NO_ACTION"
    UPDATED = "UPDATED"
    BLOCKED = "BLOCKED"
    MUTATED = "MUTATED"
    FAILED = "FAILED"


class ExecutionRuntimeTickStatus(str, Enum):
    IDLE = "IDLE"
    PROGRESSED = "PROGRESSED"
    BLOCKED = "BLOCKED"
    MUTATED = "MUTATED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ExecutionRuntimeStageResultV1:
    stage: ExecutionRuntimeStage
    status: ExecutionRuntimeStageStatus
    observed_at_utc: str
    subject_id: str | None = None
    detail: str | None = None
    state_changed: bool = False
    broker_mutation_performed: bool = False
    blocks_lower_priority: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.stage, ExecutionRuntimeStage):
            raise ExecutionRuntimeError(f"invalid runtime stage: {self.stage!r}")
        if not isinstance(self.status, ExecutionRuntimeStageStatus):
            raise ExecutionRuntimeError(
                f"invalid runtime stage status: {self.status!r}"
            )
        object.__setattr__(
            self,
            "observed_at_utc",
            format_utc(parse_utc(self.observed_at_utc)),
        )
        subject = str(self.subject_id or "").strip() or None
        detail = str(self.detail or "").strip() or None
        object.__setattr__(self, "subject_id", subject)
        object.__setattr__(self, "detail", detail)

        expected = {
            ExecutionRuntimeStageStatus.NO_ACTION: (False, False, False),
            ExecutionRuntimeStageStatus.UPDATED: (True, False, False),
            ExecutionRuntimeStageStatus.BLOCKED: (False, False, True),
            ExecutionRuntimeStageStatus.MUTATED: (True, True, True),
            ExecutionRuntimeStageStatus.FAILED: (False, False, True),
        }[self.status]
        actual = (
            bool(self.state_changed),
            bool(self.broker_mutation_performed),
            bool(self.blocks_lower_priority),
        )
        if actual != expected:
            raise ExecutionRuntimeError(
                "runtime stage flags disagree with status: "
                f"stage={self.stage.value}, status={self.status.value}, "
                f"expected={expected}, actual={actual}"
            )
        if self.status in {
            ExecutionRuntimeStageStatus.BLOCKED,
            ExecutionRuntimeStageStatus.FAILED,
        } and detail is None:
            raise ExecutionRuntimeError(
                f"{self.status.value} runtime stage requires detail"
            )

    @classmethod
    def no_action(
        cls,
        stage: ExecutionRuntimeStage,
        *,
        observed_at_utc: str,
        detail: str | None = None,
    ) -> "ExecutionRuntimeStageResultV1":
        return cls(
            stage=stage,
            status=ExecutionRuntimeStageStatus.NO_ACTION,
            observed_at_utc=observed_at_utc,
            detail=detail,
        )

    @classmethod
    def updated(
        cls,
        stage: ExecutionRuntimeStage,
        *,
        observed_at_utc: str,
        subject_id: str | None = None,
        detail: str | None = None,
    ) -> "ExecutionRuntimeStageResultV1":
        return cls(
            stage=stage,
            status=ExecutionRuntimeStageStatus.UPDATED,
            observed_at_utc=observed_at_utc,
            subject_id=subject_id,
            detail=detail,
            state_changed=True,
        )

    @classmethod
    def blocked(
        cls,
        stage: ExecutionRuntimeStage,
        *,
        observed_at_utc: str,
        detail: str,
        subject_id: str | None = None,
    ) -> "ExecutionRuntimeStageResultV1":
        return cls(
            stage=stage,
            status=ExecutionRuntimeStageStatus.BLOCKED,
            observed_at_utc=observed_at_utc,
            subject_id=subject_id,
            detail=detail,
            blocks_lower_priority=True,
        )

    @classmethod
    def mutated(
        cls,
        stage: ExecutionRuntimeStage,
        *,
        observed_at_utc: str,
        subject_id: str,
        detail: str | None = None,
    ) -> "ExecutionRuntimeStageResultV1":
        return cls(
            stage=stage,
            status=ExecutionRuntimeStageStatus.MUTATED,
            observed_at_utc=observed_at_utc,
            subject_id=subject_id,
            detail=detail,
            state_changed=True,
            broker_mutation_performed=True,
            blocks_lower_priority=True,
        )

    @classmethod
    def failed(
        cls,
        stage: ExecutionRuntimeStage,
        *,
        observed_at_utc: str,
        detail: str,
        subject_id: str | None = None,
    ) -> "ExecutionRuntimeStageResultV1":
        return cls(
            stage=stage,
            status=ExecutionRuntimeStageStatus.FAILED,
            observed_at_utc=observed_at_utc,
            subject_id=subject_id,
            detail=detail,
            blocks_lower_priority=True,
        )

    def to_dict(self) -> dict:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "observed_at_utc": self.observed_at_utc,
            "subject_id": self.subject_id,
            "detail": self.detail,
            "state_changed": self.state_changed,
            "broker_mutation_performed": self.broker_mutation_performed,
            "blocks_lower_priority": self.blocks_lower_priority,
        }


class ExecutionRuntimeStageRunner(Protocol):
    stage: ExecutionRuntimeStage

    async def run_once(
        self,
        *,
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1: ...


@dataclass(frozen=True)
class ExecutionRuntimeTickV1:
    tick_id: str
    started_at_utc: str
    finished_at_utc: str
    status: ExecutionRuntimeTickStatus
    results: tuple[ExecutionRuntimeStageResultV1, ...]
    broker_mutation_count: int
    stopped_after_stage: ExecutionRuntimeStage | None

    def __post_init__(self) -> None:
        tick_id = str(self.tick_id or "").strip()
        if not tick_id.startswith("execution_runtime_tick_"):
            raise ExecutionRuntimeError(f"invalid runtime tick id: {tick_id!r}")
        object.__setattr__(self, "tick_id", tick_id)
        started = format_utc(parse_utc(self.started_at_utc))
        finished = format_utc(parse_utc(self.finished_at_utc))
        if parse_utc(finished) < parse_utc(started):
            raise ExecutionRuntimeError("runtime tick cannot finish before it starts")
        object.__setattr__(self, "started_at_utc", started)
        object.__setattr__(self, "finished_at_utc", finished)
        results = tuple(self.results)
        object.__setattr__(self, "results", results)
        if len(results) > len(EXECUTION_RUNTIME_STAGE_ORDER):
            raise ExecutionRuntimeError("runtime tick has too many stage results")
        expected_prefix = EXECUTION_RUNTIME_STAGE_ORDER[: len(results)]
        actual = tuple(item.stage for item in results)
        if actual != expected_prefix:
            raise ExecutionRuntimeError(
                "runtime tick stages are not the canonical priority prefix: "
                f"expected={expected_prefix}, actual={actual}"
            )
        mutations = sum(
            1 for item in results if item.broker_mutation_performed
        )
        if int(self.broker_mutation_count) != mutations or mutations > 1:
            raise ExecutionRuntimeError(
                "runtime tick broker mutation count is inconsistent or exceeds one"
            )
        if results:
            terminal = results[-1]
            expected_stopped = (
                terminal.stage
                if terminal.blocks_lower_priority
                else None
            )
        else:
            expected_stopped = None
        if self.stopped_after_stage != expected_stopped:
            raise ExecutionRuntimeError(
                "runtime stopped_after_stage disagrees with final stage result"
            )

    def to_dict(self) -> dict:
        return {
            "tick_id": self.tick_id,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "status": self.status.value,
            "results": [item.to_dict() for item in self.results],
            "broker_mutation_count": self.broker_mutation_count,
            "stopped_after_stage": (
                None
                if self.stopped_after_stage is None
                else self.stopped_after_stage.value
            ),
        }


class ExecutionRuntimeCoordinator:
    def __init__(
        self,
        *,
        stages: tuple[ExecutionRuntimeStageRunner, ...],
        broker_mutations_enabled: bool,
    ) -> None:
        stage_values = tuple(item.stage for item in stages)
        if stage_values != EXECUTION_RUNTIME_STAGE_ORDER:
            raise ExecutionRuntimeError(
                "runtime stages must exactly match the canonical order: "
                f"expected={EXECUTION_RUNTIME_STAGE_ORDER}, actual={stage_values}"
            )
        self.stages = tuple(stages)
        self.broker_mutations_enabled = bool(broker_mutations_enabled)
        self._sequence_no = 0

    def _tick_id(self, started_at_utc: str) -> str:
        self._sequence_no += 1
        payload = {
            "started_at_utc": started_at_utc,
            "sequence_no": self._sequence_no,
        }
        digest = hashlib.sha256(
            canonical_json_text(payload).encode("utf-8")
        ).hexdigest()[:32]
        return f"execution_runtime_tick_{digest}"

    async def run_tick(
        self,
        *,
        observed_at_utc: str | None = None,
    ) -> ExecutionRuntimeTickV1:
        started = format_utc(
            parse_utc(observed_at_utc)
            if observed_at_utc is not None
            else utc_now()
        )
        results: list[ExecutionRuntimeStageResultV1] = []
        for runner in self.stages:
            try:
                result = await runner.run_once(observed_at_utc=started)
                if result.stage != runner.stage:
                    raise ExecutionRuntimeError(
                        "runtime stage runner returned another stage: "
                        f"runner={runner.stage.value}, result={result.stage.value}"
                    )
                if (
                    result.broker_mutation_performed
                    and not self.broker_mutations_enabled
                ):
                    raise ExecutionRuntimeError(
                        "runtime stage performed a broker mutation while broker "
                        "mutations are disabled"
                    )
            except Exception as exc:
                result = ExecutionRuntimeStageResultV1.failed(
                    runner.stage,
                    observed_at_utc=started,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            results.append(result)
            if result.blocks_lower_priority:
                break

        mutation_count = sum(
            1 for item in results if item.broker_mutation_performed
        )
        if mutation_count > 1:
            raise ExecutionRuntimeError(
                "runtime tick attempted more than one broker mutation"
            )
        if results and results[-1].status == ExecutionRuntimeStageStatus.FAILED:
            status = ExecutionRuntimeTickStatus.FAILED
        elif mutation_count:
            status = ExecutionRuntimeTickStatus.MUTATED
        elif results and results[-1].status == ExecutionRuntimeStageStatus.BLOCKED:
            status = ExecutionRuntimeTickStatus.BLOCKED
        elif any(item.state_changed for item in results):
            status = ExecutionRuntimeTickStatus.PROGRESSED
        else:
            status = ExecutionRuntimeTickStatus.IDLE
        stopped_after = (
            results[-1].stage
            if results and results[-1].blocks_lower_priority
            else None
        )
        return ExecutionRuntimeTickV1(
            tick_id=self._tick_id(started),
            started_at_utc=started,
            finished_at_utc=format_utc(utc_now()),
            status=status,
            results=tuple(results),
            broker_mutation_count=mutation_count,
            stopped_after_stage=stopped_after,
        )
