from __future__ import annotations

from ibmd.decision.domain import (
    DecisionEvaluation,
    DecisionPolicyV1,
    ExecutionDecisionFixtureV1,
    evaluate_signal,
)

from .ports import DecisionRepository, DecisionSignalSource


class DecisionServiceError(RuntimeError):
    pass


class DecisionShadowService:
    def __init__(
        self,
        *,
        policy: DecisionPolicyV1,
        signal_source: DecisionSignalSource,
        repository: DecisionRepository,
    ) -> None:
        self.policy = policy
        self.signal_source = signal_source
        self.repository = repository

    def validate_dependencies(self) -> None:
        self.signal_source.validate_schema()
        self.repository.validate_schema()

    def evaluate_event(
        self,
        *,
        event_id: str,
        fixture: ExecutionDecisionFixtureV1,
    ) -> DecisionEvaluation:
        signal = self.signal_source.read_event(event_id)
        if signal is None:
            raise DecisionServiceError(
                f"target signal event does not exist: {event_id}"
            )
        evaluation = evaluate_signal(
            signal=signal,
            fixture=fixture,
            policy=self.policy,
        )
        return self.repository.publish(evaluation)
