from __future__ import annotations

import asyncio
from collections.abc import Iterable

from ibmd.foundation.identity import new_id
from ibmd.public_contracts.market_data import (
    MarketDataContractV1,
    MarketSideBarObservationV1,
)

from .market_data import (
    BrokerMarketDataReadError,
    RealtimeQuoteSubscription,
)


class ScriptedRealtimeQuoteSubscription(RealtimeQuoteSubscription):
    def __init__(
        self,
        values: Iterable[MarketSideBarObservationV1 | Exception | None],
    ) -> None:
        self._values = list(values)
        self._closed = False

    async def next_bar(
        self,
        *,
        timeout_seconds: float,
    ) -> MarketSideBarObservationV1 | None:
        _ = timeout_seconds
        if self._closed:
            raise BrokerMarketDataReadError("scripted subscription is closed")
        await asyncio.sleep(0)
        if not self._values:
            return None
        value = self._values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    async def close(self) -> None:
        self._closed = True


class ScriptedMarketDataReader:
    def __init__(
        self,
        *,
        history: Iterable[
            Iterable[MarketSideBarObservationV1] | Exception
        ] = (),
        realtime: Iterable[
            Iterable[MarketSideBarObservationV1 | Exception | None] | Exception
        ] = (),
        source_session_id: str | None = None,
    ) -> None:
        self._history = list(history)
        self._realtime = list(realtime)
        self._source_session_id = source_session_id or new_id("ib_session")
        self.history_calls: list[dict[str, object]] = []
        self.realtime_calls: list[dict[str, object]] = []
        self.closed = False

    @property
    def source_session_id(self) -> str:
        return self._source_session_id

    async def fetch_recent_history(
        self,
        *,
        contract: MarketDataContractV1,
        end_at_utc: str,
        lookback_seconds: int,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> tuple[MarketSideBarObservationV1, ...]:
        self.history_calls.append(
            {
                "contract": contract,
                "end_at_utc": end_at_utc,
                "lookback_seconds": lookback_seconds,
                "bar_duration_seconds": bar_duration_seconds,
                "use_rth": use_rth,
            }
        )
        await asyncio.sleep(0)
        if not self._history:
            return ()
        value = self._history.pop(0)
        if isinstance(value, Exception):
            raise value
        return tuple(value)

    async def open_realtime(
        self,
        *,
        contract: MarketDataContractV1,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> RealtimeQuoteSubscription:
        self.realtime_calls.append(
            {
                "contract": contract,
                "bar_duration_seconds": bar_duration_seconds,
                "use_rth": use_rth,
            }
        )
        await asyncio.sleep(0)
        if not self._realtime:
            return ScriptedRealtimeQuoteSubscription(())
        value = self._realtime.pop(0)
        if isinstance(value, Exception):
            raise value
        return ScriptedRealtimeQuoteSubscription(value)

    async def close(self) -> None:
        self.closed = True
