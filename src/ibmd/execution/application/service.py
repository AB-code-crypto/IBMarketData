from __future__ import annotations

import os
from dataclasses import dataclass

from ibmd.execution.domain import (
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    admit_strategy_command,
)
from ibmd.foundation.time import format_utc, utc_now
from ibmd.public_contracts.decision import StrategyCommandRequestV1
from ibmd.public_contracts.execution import (
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
)
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)

from .ports import ExecutionRepository, ServiceHealthPublisher


@dataclass(frozen=True)
class ExecutionFoundationConfig:
    deployment_id: str
    instance_id: str
    application_version: str
    configuration_hash: str
    policy: ExecutionFoundationPolicyV1
    service_name: str = "execution"


class ExecutionFoundationService:
    def __init__(
        self,
        *,
        config: ExecutionFoundationConfig,
        repository: ExecutionRepository,
        health_publisher: ServiceHealthPublisher,
    ) -> None:
        self.config = config
        self.repository = repository
        self.health_publisher = health_publisher
        now = format_utc(utc_now())
        self.health = ServiceHealthV1.starting(
            service=config.service_name,
            deployment_id=config.deployment_id,
            instance_id=config.instance_id,
            pid=os.getpid(),
            application_version=config.application_version,
            configuration_hash=config.configuration_hash,
            now_utc=now,
        )

    def publish_starting(self) -> ServiceHealthV1:
        self.health_publisher.publish(self.health)
        return self.health

    def validate_dependencies(self) -> None:
        self.repository.validate_schema()

    def publish_fixture(
        self,
        fixture: ExecutionFoundationFixtureV1,
    ) -> None:
        self.repository.publish_fixture(fixture)
        readiness = fixture.readiness
        if readiness.status == ExecutionReadinessStatus.BLOCKED:
            public_readiness = Readiness.BLOCKED
            reason = "; ".join(readiness.blocking_reasons)
        elif readiness.status == ExecutionReadinessStatus.NOT_READY:
            public_readiness = Readiness.NOT_READY
            reason = "; ".join(readiness.blocking_reasons) or "execution not ready"
        elif readiness.broker_actions_enabled:
            public_readiness = Readiness.READY
            reason = None
        else:
            public_readiness = Readiness.DEGRADED
            reason = "broker order gateway disabled in execution foundation"

        self.health = self.health.heartbeat(
            now_utc=fixture.observed_at_utc,
            liveness=Liveness.RUNNING,
            readiness=public_readiness,
            last_success_at_utc=fixture.observed_at_utc,
            dependency_status=(
                DependencyStatusV1(
                    name="execution_foundation_state",
                    status=readiness.status.value,
                    detail=reason,
                    observed_at_utc=fixture.observed_at_utc,
                ),
            ),
            blocking_reason=reason,
        )
        self.health_publisher.publish(self.health)

    def ingest_command(
        self,
        *,
        command: StrategyCommandRequestV1,
        fixture: ExecutionFoundationFixtureV1,
    ) -> ExecutionCommandStateV1:
        self.publish_fixture(fixture)
        admission = admit_strategy_command(
            command=command,
            policy=self.config.policy,
            fixture=fixture,
        )
        return self.repository.publish_admission(admission)

    def publish_stopped(self) -> ServiceHealthV1:
        now = format_utc(utc_now())
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.STOPPED,
            readiness=Readiness.NOT_READY,
            blocking_reason="service stopped",
        )
        self.health_publisher.publish(self.health)
        return self.health

    def publish_failed(self, reason: str) -> ServiceHealthV1:
        now = format_utc(utc_now())
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.FAILED,
            readiness=Readiness.BLOCKED,
            blocking_reason=str(reason or "execution foundation failed"),
        )
        self.health_publisher.publish(self.health)
        return self.health
