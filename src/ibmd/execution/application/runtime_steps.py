from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Awaitable, Callable

from .runtime import (
    ExecutionRuntimeStage,
    ExecutionRuntimeStageResultV1,
)


RuntimeStageCallable = Callable[
    [str],
    ExecutionRuntimeStageResultV1 | Awaitable[ExecutionRuntimeStageResultV1],
]
RuntimePendingCallable = Callable[[str], tuple[str | None, str | None]]


@dataclass
class CallableExecutionRuntimeStage:
    stage: ExecutionRuntimeStage
    callback: RuntimeStageCallable

    async def run_once(
        self,
        *,
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        value = self.callback(observed_at_utc)
        if inspect.isawaitable(value):
            value = await value
        return value


@dataclass
class NoActionExecutionRuntimeStage:
    stage: ExecutionRuntimeStage
    detail: str | None = None

    async def run_once(
        self,
        *,
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        return ExecutionRuntimeStageResultV1.no_action(
            self.stage,
            observed_at_utc=observed_at_utc,
            detail=self.detail,
        )


@dataclass
class DisabledMutationExecutionRuntimeStage:
    stage: ExecutionRuntimeStage
    pending: RuntimePendingCallable
    disabled_reason: str

    async def run_once(
        self,
        *,
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        subject_id, detail = self.pending(observed_at_utc)
        if subject_id is None:
            return ExecutionRuntimeStageResultV1.no_action(
                self.stage,
                observed_at_utc=observed_at_utc,
            )
        suffix = str(detail or "").strip()
        reason = self.disabled_reason
        if suffix:
            reason = f"{reason}: {suffix}"
        return ExecutionRuntimeStageResultV1.blocked(
            self.stage,
            observed_at_utc=observed_at_utc,
            subject_id=subject_id,
            detail=reason,
        )
