from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from ib_async import Contract, IB, MarketOrder
from ib_async.ib import StartupFetchNONE

from ibmd.foundation.time import format_utc, utc_now

from .paper_orders import (
    BrokerOrderSubmitError,
    PaperMarketOrderRequest,
    PaperOrderSubmissionReceipt,
)


@dataclass(frozen=True)
class IBPaperOrderConnectionSettings:
    host: str
    port: int
    client_id: int
    account_id: str
    connect_timeout_seconds: float = 15.0
    account_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        host = str(self.host or "").strip()
        account = str(self.account_id or "").strip()
        if not host or not account:
            raise BrokerOrderSubmitError("IB host and account_id are required")
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "account_id", account)
        try:
            port = int(self.port)
            client_id = int(self.client_id)
        except (TypeError, ValueError) as exc:
            raise BrokerOrderSubmitError(
                "IB port and client_id must be integers"
            ) from exc
        if port <= 0 or port > 65_535:
            raise BrokerOrderSubmitError(f"invalid IB port: {port}")
        if client_id < 0:
            raise BrokerOrderSubmitError(
                f"IB client_id must be non-negative: {client_id}"
            )
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)
        for field_name in (
            "connect_timeout_seconds",
            "account_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or value <= 0.0:
                raise BrokerOrderSubmitError(
                    f"{field_name} must be finite and positive: {value}"
                )
            object.__setattr__(self, field_name, value)


def _multiplier_text(value: float) -> str:
    number = float(value)
    return str(int(number)) if number.is_integer() else format(number, "g")


def build_paper_order_contract(request: PaperMarketOrderRequest) -> Contract:
    route = request.route
    return Contract(
        secType=route.sec_type,
        symbol=route.instrument_id,
        exchange=route.exchange,
        currency=route.currency,
        tradingClass=route.trading_class,
        multiplier=_multiplier_text(route.multiplier),
        conId=route.con_id,
        localSymbol=route.local_symbol,
        lastTradeDateOrContractMonth=route.last_trade_date,
    )


def build_paper_market_order(request: PaperMarketOrderRequest) -> Any:
    return MarketOrder(
        request.side.value,
        request.quantity,
        orderId=request.broker_order_id,
        account=request.account_id,
        orderRef=request.order_ref,
        transmit=True,
    )


class IBAsyncPaperOrderGateway:
    def __init__(
        self,
        settings: IBPaperOrderConnectionSettings,
        *,
        ib_factory: Callable[[], Any] = IB,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self._ib = ib_factory()
        self._clock = clock
        self._operation_lock = asyncio.Lock()
        self._allocated_order_ids: set[int] = set()

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
                raise BrokerOrderSubmitError(
                    "cannot read IB managed accounts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if accounts:
                if self.settings.account_id not in accounts:
                    raise BrokerOrderSubmitError(
                        "configured paper account is absent from IB session: "
                        f"expected={self.settings.account_id}, "
                        f"managed_accounts={accounts}"
                    )
                return
            if loop_time() >= deadline:
                raise BrokerOrderSubmitError(
                    "IB connection did not publish managed accounts before "
                    f"timeout: expected={self.settings.account_id}"
                )
            await asyncio.sleep(0.10)

    async def _ensure_connected(self) -> None:
        if self.connected:
            return
        self._disconnect_best_effort()
        try:
            await asyncio.wait_for(
                self._ib.connectAsync(
                    host=self.settings.host,
                    port=self.settings.port,
                    clientId=self.settings.client_id,
                    account=self.settings.account_id,
                    readonly=False,
                    fetchFields=StartupFetchNONE,
                ),
                timeout=self.settings.connect_timeout_seconds,
            )
            if not self.connected:
                raise BrokerOrderSubmitError(
                    "IB connectAsync returned without an active connection"
                )
            await self._validate_account_access()
        except Exception:
            self._disconnect_best_effort()
            raise

    def _require_account(self, account_id: str) -> str:
        expected = str(account_id or "").strip()
        if expected != self.settings.account_id:
            raise BrokerOrderSubmitError(
                "paper order gateway account mismatch: "
                f"configured={self.settings.account_id}, requested={expected}"
            )
        return expected

    async def allocate_order_id(self, *, account_id: str) -> int:
        self._require_account(account_id)
        async with self._operation_lock:
            await self._ensure_connected()
            try:
                order_id = int(self._ib.client.getReqId())
            except Exception as exc:
                if not self.connected:
                    self._disconnect_best_effort()
                raise BrokerOrderSubmitError(
                    "cannot allocate IB order id: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if order_id <= 0:
                raise BrokerOrderSubmitError(
                    f"IB allocated a non-positive order id: {order_id}"
                )
            self._allocated_order_ids.add(order_id)
            return order_id

    async def submit_market_order(
        self,
        request: PaperMarketOrderRequest,
    ) -> PaperOrderSubmissionReceipt:
        if not isinstance(request, PaperMarketOrderRequest):
            raise BrokerOrderSubmitError(
                "request must be PaperMarketOrderRequest"
            )
        self._require_account(request.account_id)
        async with self._operation_lock:
            await self._ensure_connected()
            if request.broker_order_id not in self._allocated_order_ids:
                raise BrokerOrderSubmitError(
                    "broker order id was not allocated by this gateway session: "
                    f"{request.broker_order_id}"
                )
            contract = build_paper_order_contract(request)
            order = build_paper_market_order(request)
            self._allocated_order_ids.discard(request.broker_order_id)
            submitted_at = format_utc(self._clock())
            try:
                trade = self._ib.placeOrder(contract, order)
                await asyncio.sleep(0)
            except Exception as exc:
                if not self.connected:
                    self._disconnect_best_effort()
                raise BrokerOrderSubmitError(
                    "IB placeOrder raised after the persisted SUBMITTING boundary: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            returned_order = getattr(trade, "order", None)
            if returned_order is None:
                raise BrokerOrderSubmitError(
                    "IB placeOrder returned no trade.order after possible submission"
                )
            returned_id = int(getattr(returned_order, "orderId", 0) or 0)
            returned_ref = str(
                getattr(returned_order, "orderRef", "") or ""
            ).strip()
            returned_account = str(
                getattr(returned_order, "account", "") or ""
            ).strip()
            if (
                returned_id != request.broker_order_id
                or returned_ref != request.order_ref
                or returned_account != request.account_id
            ):
                raise BrokerOrderSubmitError(
                    "IB placeOrder returned a conflicting order identity after possible "
                    "submission: "
                    f"expected=({request.broker_order_id}, {request.order_ref}, "
                    f"{request.account_id}), returned=({returned_id}, "
                    f"{returned_ref}, {returned_account})"
                )
            return PaperOrderSubmissionReceipt(
                broker_order_id=returned_id,
                order_ref=returned_ref,
                submitted_at_utc=submitted_at,
            )

    async def close(self) -> None:
        self._disconnect_best_effort()
