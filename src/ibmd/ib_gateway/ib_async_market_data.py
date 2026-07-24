from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from ib_async import Contract, IB
from ib_async.ib import StartupFetchNONE

from ibmd.foundation.identity import new_id
from ibmd.foundation.time import ensure_utc, format_utc, parse_utc, utc_now
from ibmd.public_contracts.market_data import (
    MarketDataContractV1,
    MarketDataSourceKind,
    MarketSideBarObservationV1,
    QuoteSide,
)

from .market_data import (
    BrokerMarketDataReadError,
    RealtimeQuoteSubscription,
)


@dataclass(frozen=True)
class IBMarketDataConnectionSettings:
    host: str
    port: int
    client_id: int
    account_id: str
    connect_timeout_seconds: float = 15.0
    account_timeout_seconds: float = 5.0
    historical_timeout_seconds: float = 30.0
    historical_request_spacing_seconds: float = 1.0

    def __post_init__(self) -> None:
        host = str(self.host or "").strip()
        account = str(self.account_id or "").strip()
        if not host or not account:
            raise BrokerMarketDataReadError(
                "IB host and account_id are required"
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "account_id", account)
        try:
            port = int(self.port)
            client_id = int(self.client_id)
        except (TypeError, ValueError) as exc:
            raise BrokerMarketDataReadError(
                "IB port and client_id must be integers"
            ) from exc
        if port <= 0 or port > 65_535:
            raise BrokerMarketDataReadError(f"invalid IB port: {port}")
        if client_id < 0:
            raise BrokerMarketDataReadError(
                f"IB client_id must be non-negative: {client_id}"
            )
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)
        for field_name in (
            "connect_timeout_seconds",
            "account_timeout_seconds",
            "historical_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if value <= 0.0 or not math.isfinite(value):
                raise BrokerMarketDataReadError(
                    f"{field_name} must be finite and positive: {value}"
                )
            object.__setattr__(self, field_name, value)
        spacing = float(self.historical_request_spacing_seconds)
        if spacing < 0.0 or not math.isfinite(spacing):
            raise BrokerMarketDataReadError(
                "historical_request_spacing_seconds must be finite and non-negative"
            )
        object.__setattr__(
            self,
            "historical_request_spacing_seconds",
            spacing,
        )


def build_ib_contract(spec: MarketDataContractV1) -> Contract:
    return Contract(
        secType=spec.sec_type,
        symbol=spec.symbol,
        exchange=spec.exchange,
        currency=spec.currency,
        tradingClass=spec.trading_class,
        multiplier=str(spec.multiplier),
        conId=spec.con_id,
        localSymbol=spec.local_symbol,
        lastTradeDateOrContractMonth=spec.last_trade_date,
    )


def _bar_time(value: Any) -> datetime:
    candidate = getattr(value, "time", None)
    if candidate is None:
        candidate = getattr(value, "date", None)
    if isinstance(candidate, datetime):
        return ensure_utc(candidate)
    if isinstance(candidate, str):
        return parse_utc(candidate)
    if isinstance(candidate, (int, float)) and math.isfinite(float(candidate)):
        return datetime.fromtimestamp(float(candidate), tz=timezone.utc)
    raise BrokerMarketDataReadError(
        f"IB bar has no supported UTC timestamp: {candidate!r}"
    )


def _bar_value(value: Any, field_name: str) -> float:
    if field_name == "open":
        raw = getattr(value, "open", None)
        if raw is None:
            raw = getattr(value, "open_", None)
    else:
        raw = getattr(value, field_name, None)
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise BrokerMarketDataReadError(
            f"IB bar {field_name} is not numeric: {raw!r}"
        ) from exc


def raw_quote_side_from_ib(
    value: Any,
    *,
    contract: MarketDataContractV1,
    side: QuoteSide,
    bar_duration_seconds: int,
    source_kind: MarketDataSourceKind,
    source_session_id: str,
    source_generation_id: str,
    observed_at_utc: str,
) -> MarketSideBarObservationV1:
    return MarketSideBarObservationV1(
        instrument_id=contract.instrument_id,
        con_id=contract.con_id,
        local_symbol=contract.local_symbol,
        side=side,
        bar_start_utc=format_utc(_bar_time(value)),
        bar_duration_seconds=bar_duration_seconds,
        open_price=_bar_value(value, "open"),
        high_price=_bar_value(value, "high"),
        low_price=_bar_value(value, "low"),
        close_price=_bar_value(value, "close"),
        source_kind=source_kind,
        source_session_id=source_session_id,
        source_generation_id=source_generation_id,
        observed_at_utc=observed_at_utc,
    )


class IBAsyncRealtimeQuoteSubscription(RealtimeQuoteSubscription):
    def __init__(
        self,
        *,
        ib: Any,
        rows_and_handlers: list[tuple[Any, Callable[..., None]]],
        queue: asyncio.Queue[MarketSideBarObservationV1 | Exception],
        connected: Callable[[], bool],
    ) -> None:
        self._ib = ib
        self._rows_and_handlers = rows_and_handlers
        self._queue = queue
        self._connected = connected
        self._closed = False

    async def next_bar(
        self,
        *,
        timeout_seconds: float,
    ) -> MarketSideBarObservationV1 | None:
        timeout = float(timeout_seconds)
        if timeout <= 0.0 or not math.isfinite(timeout):
            raise BrokerMarketDataReadError(
                f"timeout_seconds must be finite and positive: {timeout_seconds}"
            )
        if self._closed:
            raise BrokerMarketDataReadError("realtime subscription is closed")
        if not self._connected():
            raise BrokerMarketDataReadError(
                "IB connection is not active during realtime subscription"
            )
        try:
            value = await asyncio.wait_for(
                self._queue.get(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            if not self._connected():
                raise BrokerMarketDataReadError(
                    "IB connection was lost while waiting for realtime bars"
                )
            return None
        if isinstance(value, Exception):
            raise BrokerMarketDataReadError(
                "realtime bar mapping failed: "
                f"{type(value).__name__}: {value}"
            ) from value
        return value

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for rows, handler in self._rows_and_handlers:
            try:
                rows.updateEvent -= handler
            except Exception:
                pass
            try:
                self._ib.cancelRealTimeBars(rows)
            except Exception:
                pass
        self._rows_and_handlers.clear()


class IBAsyncMarketDataReader:
    def __init__(
        self,
        settings: IBMarketDataConnectionSettings,
        *,
        ib_factory: Callable[[], Any] = IB,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self._ib = ib_factory()
        self._clock = clock
        self._operation_lock = asyncio.Lock()
        self._source_session_id = new_id("ib_session")

    @property
    def source_session_id(self) -> str:
        return self._source_session_id

    @property
    def connected(self) -> bool:
        try:
            return bool(self._ib.isConnected())
        except Exception:
            return False

    def _disconnect_best_effort(self) -> None:
        try:
            self._ib.disconnect()
        except Exception:
            pass

    async def _validate_account_access(self) -> None:
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + self.settings.account_timeout_seconds
        while True:
            try:
                accounts = tuple(
                    str(item or "").strip()
                    for item in list(self._ib.managedAccounts() or [])
                    if str(item or "").strip()
                )
            except Exception as exc:
                raise BrokerMarketDataReadError(
                    "cannot read IB managed accounts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if accounts:
                if self.settings.account_id not in accounts:
                    raise BrokerMarketDataReadError(
                        "configured account is absent from IB session: "
                        f"expected={self.settings.account_id}, "
                        f"managed_accounts={accounts}"
                    )
                return
            if loop_time() >= deadline:
                raise BrokerMarketDataReadError(
                    "IB connection did not publish managed accounts before "
                    f"timeout: expected={self.settings.account_id}"
                )
            await asyncio.sleep(0.10)

    async def _ensure_connected(self) -> None:
        if self.connected:
            return
        self._disconnect_best_effort()
        self._source_session_id = new_id("ib_session")
        try:
            await asyncio.wait_for(
                self._ib.connectAsync(
                    host=self.settings.host,
                    port=self.settings.port,
                    clientId=self.settings.client_id,
                    account=self.settings.account_id,
                    readonly=True,
                    fetchFields=StartupFetchNONE,
                ),
                timeout=self.settings.connect_timeout_seconds,
            )
            if not self.connected:
                raise BrokerMarketDataReadError(
                    "IB connectAsync returned without an active connection"
                )
            await self._validate_account_access()
        except Exception:
            self._disconnect_best_effort()
            raise

    @staticmethod
    def _validate_bar_duration(bar_duration_seconds: int) -> int:
        duration = int(bar_duration_seconds)
        if duration != 5:
            raise BrokerMarketDataReadError(
                "IB realtime shadow currently supports 5-second bars only: "
                f"{bar_duration_seconds}"
            )
        return duration

    async def fetch_recent_history(
        self,
        *,
        contract: MarketDataContractV1,
        end_at_utc: str,
        lookback_seconds: int,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> tuple[MarketSideBarObservationV1, ...]:
        duration = self._validate_bar_duration(bar_duration_seconds)
        lookback = int(lookback_seconds)
        if lookback <= 0 or lookback > 86_400:
            raise BrokerMarketDataReadError(
                "lookback_seconds must be between 1 and 86400"
            )
        end = parse_utc(end_at_utc)
        start = end - timedelta(seconds=lookback)
        ib_contract = build_ib_contract(contract)
        generation_id = new_id("md_generation")
        values: list[MarketSideBarObservationV1] = []

        async with self._operation_lock:
            await self._ensure_connected()
            for index, side in enumerate((QuoteSide.BID, QuoteSide.ASK)):
                if index and self.settings.historical_request_spacing_seconds:
                    await asyncio.sleep(
                        self.settings.historical_request_spacing_seconds
                    )
                try:
                    bars = await asyncio.wait_for(
                        self._ib.reqHistoricalDataAsync(
                            ib_contract,
                            endDateTime=end,
                            durationStr=f"{lookback} S",
                            barSizeSetting="5 secs",
                            whatToShow=side.value,
                            useRTH=bool(use_rth),
                            formatDate=2,
                            keepUpToDate=False,
                        ),
                        timeout=self.settings.historical_timeout_seconds,
                    )
                except asyncio.TimeoutError as exc:
                    self._disconnect_best_effort()
                    raise BrokerMarketDataReadError(
                        "IB historical quote request timed out: "
                        f"side={side.value}, "
                        f"timeout={self.settings.historical_timeout_seconds:g}s"
                    ) from exc
                except Exception as exc:
                    if not self.connected:
                        self._disconnect_best_effort()
                    raise BrokerMarketDataReadError(
                        "IB historical quote request failed: "
                        f"side={side.value}, {type(exc).__name__}: {exc}"
                    ) from exc

                observed_at = format_utc(self._clock())
                for bar in list(bars or []):
                    bar_time = _bar_time(bar)
                    if not (
                        start <= bar_time
                        and bar_time + timedelta(seconds=duration) <= end
                    ):
                        continue
                    values.append(
                        raw_quote_side_from_ib(
                            bar,
                            contract=contract,
                            side=side,
                            bar_duration_seconds=duration,
                            source_kind=MarketDataSourceKind.HISTORY,
                            source_session_id=self.source_session_id,
                            source_generation_id=generation_id,
                            observed_at_utc=observed_at,
                        )
                    )
        return tuple(
            sorted(
                values,
                key=lambda item: (
                    item.bar_start_utc,
                    item.side.value,
                ),
            )
        )

    async def open_realtime(
        self,
        *,
        contract: MarketDataContractV1,
        bar_duration_seconds: int,
        use_rth: bool,
    ) -> RealtimeQuoteSubscription:
        duration = self._validate_bar_duration(bar_duration_seconds)
        await self._operation_lock.acquire()
        try:
            await self._ensure_connected()
            ib_contract = build_ib_contract(contract)
            generation_id = new_id("md_generation")
            queue: asyncio.Queue[
                MarketSideBarObservationV1 | Exception
            ] = asyncio.Queue()
            rows_and_handlers: list[tuple[Any, Callable[..., None]]] = []

            for side in (QuoteSide.BID, QuoteSide.ASK):
                try:
                    rows = self._ib.reqRealTimeBars(
                        ib_contract,
                        duration,
                        side.value,
                        bool(use_rth),
                    )
                except Exception as exc:
                    for existing_rows, existing_handler in rows_and_handlers:
                        try:
                            existing_rows.updateEvent -= existing_handler
                        except Exception:
                            pass
                        try:
                            self._ib.cancelRealTimeBars(existing_rows)
                        except Exception:
                            pass
                    raise BrokerMarketDataReadError(
                        "cannot open IB realtime quote subscription: "
                        f"side={side.value}, {type(exc).__name__}: {exc}"
                    ) from exc

                def on_update(
                    bars: Any,
                    has_new_bar: bool,
                    *,
                    quote_side: QuoteSide = side,
                ) -> None:
                    if not has_new_bar or not bars:
                        return
                    try:
                        value = raw_quote_side_from_ib(
                            bars[-1],
                            contract=contract,
                            side=quote_side,
                            bar_duration_seconds=duration,
                            source_kind=MarketDataSourceKind.REALTIME,
                            source_session_id=self.source_session_id,
                            source_generation_id=generation_id,
                            observed_at_utc=format_utc(self._clock()),
                        )
                    except Exception as exc:
                        queue.put_nowait(exc)
                        return
                    queue.put_nowait(value)

                rows.updateEvent += on_update
                rows_and_handlers.append((rows, on_update))

            return IBAsyncRealtimeQuoteSubscription(
                ib=self._ib,
                rows_and_handlers=rows_and_handlers,
                queue=queue,
                connected=lambda: self.connected,
            )
        finally:
            self._operation_lock.release()

    async def close(self) -> None:
        self._disconnect_best_effort()
