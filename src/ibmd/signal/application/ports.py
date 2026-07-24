from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np

from ibmd.public_contracts.health import ServiceHealthV1
from ibmd.public_contracts.signal import SignalCalculationV1
from ibmd.signal.domain.calculation import (
    CandidateWindow,
    SignalWindow,
)
from ibmd.signal.domain.inputs import (
    SignalPatternInputs,
    SignalSourceBar,
)


@runtime_checkable
class SignalMarketDataSource(Protocol):
    def validate_schema(self) -> None:
        ...

    def read_latest_source_bar(self) -> SignalSourceBar | None:
        ...

    def load_pattern_inputs(
        self,
        *,
        window: SignalWindow,
        bar_size_seconds: int,
        historical_lookback_days: int,
        candidate_hour_profile: str,
    ) -> SignalPatternInputs:
        ...

    def load_full_candidate_values(
        self,
        *,
        candidates: tuple[CandidateWindow, ...],
        bar_size_seconds: int,
    ) -> dict[int, np.ndarray]:
        ...


@runtime_checkable
class SignalRepository(Protocol):
    def validate_schema(self) -> None:
        ...

    def publish(
        self,
        calculation: SignalCalculationV1,
    ) -> SignalCalculationV1:
        ...

    def read_latest_calculation(
        self,
        instrument_id: str,
    ) -> SignalCalculationV1 | None:
        ...


@runtime_checkable
class ServiceHealthPublisher(Protocol):
    def publish(self, health: ServiceHealthV1) -> None:
        ...
