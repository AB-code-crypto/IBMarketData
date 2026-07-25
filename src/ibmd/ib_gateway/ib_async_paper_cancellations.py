from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from ib_async import IB
from ib_async.ib import StartupFetchNONE

from ibmd.foundation.time import format_utc, utc_now

from .paper_cancellations import (
    BrokerOrderCancelError,
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
)


@dataclass(frozen=True)
class IBPaperCancellationConnectionSettings:
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
            raise BrokerOrderCancelError(
                "IB host and account_id are required for cancellation"
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "account_id", account)
        try:
            port = int(self.port)
            client_id = int(self.client_id)
        except (TypeError, ValueError) as exc:
            raise BrokerOrderCancelError(
                "IB port and client_id must be integers"
            ) from exc
        if port <= 0 or port > 65_535 or client_id < 0:
            raise BrokerOrderCancelError(
                f"invalid IB cancellation endpoint: port={port}, client={client_id}"
            )
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)
        for field_name in (
            "connect_timeout_seconds",
            "account_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or value <= 0.0:
                raise BrokerOrderCancelError(
                    f"{field_name} must be finite and positive"
                )
            object.__setattr__(self, field_name, value)


class IBAsyncPaperOrderCancellationGateway:
    def __init__(
        self,
        settings: IBPaperCancellationConnectionSettings,
        *,
        ib_factory: Callable[[], Any] = IB,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self._ib = ib_factory()
        self._clock = clock
        self._operation_lock = asyncio.Lock()

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
                raise BrokerOrderCancelError(
                    "cannot read IB managed accounts before cancellation: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if accounts:
                if self.settings.account_id not in accounts:
                    raise BrokerOrderCancelError(
                        "configured paper account is absent from cancellation session: "
                        f"expected={self.settings.account_id}, accounts={accounts}"
                    )
                return
            if loop_time() >= deadline:
                raise BrokerOrderCancelError(
                    "IB cancellation session did not publish managed accounts"
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
                raise BrokerOrderCancelError(
                    "IB cancellation connect returned without active connection"
                )
            await self._validate_account_access()
        except Exception:
            self._disconnect_best_effort()
            raise

    def _require_account(self, account_id: str) -> None:
        requested = str(account_id or "").strip()
        if requested != self.settings.account_id:
            raise BrokerOrderCancelError(
                "paper cancellation account mismatch: "
                f"configured={self.settings.account_id}, requested={requested}"
            )

    async def cancel_order(
        self,
        request: PaperOrderCancelRequest,
    ) -> PaperOrderCancelReceipt:
        if not isinstance(request, PaperOrderCancelRequest):
            raise BrokerOrderCancelError(
                "request must be PaperOrderCancelRequest"
            )
        self._require_account(request.account_id)
        async with self._operation_lock:
            await self._ensure_connected()
            requested_at = format_utc(self._clock())
            try:
                self._ib.client.cancelOrder(int(request.broker_order_id))
                await asyncio.sleep(0)
            except Exception as exc:
                if not self.connected:
                    self._disconnect_best_effort()
                raise BrokerOrderCancelError(
                    "IB cancelOrder raised after persisted CANCEL_REQUESTED: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            return PaperOrderCancelReceipt(
                broker_order_id=request.broker_order_id,
                order_ref=request.order_ref,
                cancel_requested_at_utc=requested_at,
            )

    async def close(self) -> None:
        self._disconnect_best_effort()
