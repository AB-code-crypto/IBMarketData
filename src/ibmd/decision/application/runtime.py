from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ibmd.decision.domain import DecisionEvaluation, ExecutionDecisionFixtureV1
from ibmd.public_contracts.signal import SignalEventV1

from .service import DecisionShadowService


class DecisionRuntimeSource(Protocol):
    def validate_schema(self) -> None: ...

    def read_next_pending_event(
        self,
        *,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
        configuration_hash: str,
        policy_hash: str,
    ) -> SignalEventV1 | None: ...

    def read_fixture(
        self,
        *,
        account_id: str,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
        observed_at_utc: str,
    ) -> ExecutionDecisionFixtureV1: ...


@dataclass(frozen=True)
class DecisionRuntimeRunV1:
    event: SignalEventV1 | None
    evaluation: DecisionEvaluation | None

    @property
    def processed(self) -> bool:
        return self.event is not None and self.evaluation is not None


class ContinuousDecisionService:
    def __init__(
        self,
        *,
        decision_service: DecisionShadowService,
        runtime_source: DecisionRuntimeSource,
        signal_configuration_hash: str,
    ) -> None:
        self.decision_service = decision_service
        self.runtime_source = runtime_source
        self.signal_configuration_hash = str(
            signal_configuration_hash or ""
        ).strip()
        if not self.signal_configuration_hash:
            raise ValueError("signal_configuration_hash is required")

    @property
    def policy(self):
        return self.decision_service.policy

    def validate_dependencies(self) -> None:
        self.decision_service.validate_dependencies()
        self.runtime_source.validate_schema()

    def run_once(self, *, observed_at_utc: str) -> DecisionRuntimeRunV1:
        policy = self.policy
        event = self.runtime_source.read_next_pending_event(
            strategy_id=policy.strategy_id,
            strategy_version=policy.strategy_version,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            configuration_hash=self.signal_configuration_hash,
            policy_hash=policy.policy_hash,
        )
        if event is None:
            return DecisionRuntimeRunV1(event=None, evaluation=None)
        fixture = self.runtime_source.read_fixture(
            account_id=policy.account_id,
            strategy_id=policy.strategy_id,
            strategy_version=policy.strategy_version,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            observed_at_utc=observed_at_utc,
        )
        evaluation = self.decision_service.evaluate_event(
            event_id=event.event_id,
            fixture=fixture,
        )
        return DecisionRuntimeRunV1(
            event=event,
            evaluation=evaluation,
        )


def decision_runtime_payload(run: DecisionRuntimeRunV1) -> dict:
    if not run.processed:
        return {
            "processed": False,
            "event": None,
            "record": None,
            "command": None,
            "broker_access": False,
        }
    evaluation = run.evaluation
    assert evaluation is not None
    return {
        "processed": True,
        "event": run.event.to_dict(),
        "record": evaluation.record.to_dict(),
        "command": (
            None
            if evaluation.command is None
            else evaluation.command.to_dict()
        ),
        "broker_access": False,
    }
