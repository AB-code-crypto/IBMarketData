from __future__ import annotations

from typing import Protocol

from ibmd.decision.domain import DecisionEvaluation
from ibmd.public_contracts.signal import SignalEventV1


class DecisionSignalSource(Protocol):
    def validate_schema(self) -> None:
        ...

    def read_event(self, event_id: str) -> SignalEventV1 | None:
        ...


class DecisionRepository(Protocol):
    def validate_schema(self) -> None:
        ...

    def publish(self, evaluation: DecisionEvaluation) -> DecisionEvaluation:
        ...
