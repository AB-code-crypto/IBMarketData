from __future__ import annotations

from typing import Protocol

from ibmd.execution.domain import (
    ExecutionAdmission,
    ExecutionFoundationFixtureV1,
)
from ibmd.public_contracts.decision import StrategyCommandRequestV1
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionCommandStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.health import ServiceHealthV1


class DecisionCommandSource(Protocol):
    def validate_schema(self) -> None: ...

    def read_command(self, command_id: str) -> StrategyCommandRequestV1 | None: ...


class ExecutionRepository(Protocol):
    def validate_schema(self) -> None: ...

    def publish_fixture(
        self,
        fixture: ExecutionFoundationFixtureV1,
    ) -> tuple[
        ExecutionReadinessV1,
        StrategyPositionV1,
        DailyRiskStateV1,
    ]: ...

    def publish_admission(
        self,
        admission: ExecutionAdmission,
    ) -> ExecutionCommandStateV1: ...

    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None: ...


class ServiceHealthPublisher(Protocol):
    def publish(self, health: ServiceHealthV1) -> ServiceHealthV1: ...
