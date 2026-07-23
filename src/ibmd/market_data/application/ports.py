from __future__ import annotations

from typing import Protocol

from ibmd.public_contracts.health import ServiceHealthV1
from ibmd.public_contracts.market_data import (
    MarketBarV1,
    MarketDataContractV1,
    MarketSideBarObservationV1,
)


class RealtimeMarketDataSubscription(Protocol):
    async def next_bar(
        self,
        *,
        timeout_seconds: float,
    ) -> MarketSideBarObservationV1 | None: ...

    async def close(self) -> None: ...


class MarketDataSource(Protocol):
    @property
    def source_session_id(self) -> str: ...

    async def fetch_recent_history(
        self,
        *,
        contract: MarketDataContractV1,
        end_at_utc: str,
        lookback_seconds: int,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> tuple[MarketSideBarObservationV1, ...]: ...

    async def open_realtime(
        self,
        *,
        contract: MarketDataContractV1,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> RealtimeMarketDataSubscription: ...

    async def close(self) -> None: ...


class MarketBarRepository(Protocol):
    def validate_schema(self) -> None: ...

    def ingest_fragments(
        self,
        fragments: tuple[MarketSideBarObservationV1, ...],
    ) -> tuple[MarketBarV1, ...]: ...

    def record_source_failure(
        self,
        *,
        instrument_id: str,
        source_session_id: str,
        observed_at_utc: str,
        error_text: str,
    ) -> None: ...

    def read_latest_complete(
        self,
        instrument_id: str,
    ) -> MarketBarV1 | None: ...


class ServiceHealthPublisher(Protocol):
    def publish(self, health: ServiceHealthV1) -> object: ...
