from __future__ import annotations

from typing import Protocol, runtime_checkable

from ibmd.public_contracts.market_data import (
    MarketDataContractV1,
    MarketSideBarObservationV1,
)


class BrokerMarketDataReadError(RuntimeError):
    pass


@runtime_checkable
class RealtimeQuoteSubscription(Protocol):
    async def next_bar(
        self,
        *,
        timeout_seconds: float,
    ) -> MarketSideBarObservationV1 | None:
        ...

    async def close(self) -> None:
        ...


@runtime_checkable
class IBMarketDataReader(Protocol):
    @property
    def source_session_id(self) -> str:
        ...

    async def fetch_recent_history(
        self,
        *,
        contract: MarketDataContractV1,
        end_at_utc: str,
        lookback_seconds: int,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> tuple[MarketSideBarObservationV1, ...]:
        ...

    async def open_realtime(
        self,
        *,
        contract: MarketDataContractV1,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> RealtimeQuoteSubscription:
        ...

    async def close(self) -> None:
        ...


# Compatibility aliases local to the new target tree. New service/domain code
# should import the versioned DTOs from public_contracts directly.
IBMarketDataContract = MarketDataContractV1
RawQuoteSideBar = MarketSideBarObservationV1
