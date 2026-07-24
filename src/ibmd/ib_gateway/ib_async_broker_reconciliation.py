from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from ib_async import ExecutionFilter, IB
from ib_async.ib import StartupFetchNONE

from ibmd.foundation.identity import new_id, validate_id
from ibmd.foundation.time import format_utc, utc_now
from ibmd.public_contracts.broker_reconciliation import (
    BrokerFillFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)

from .broker_reconciliation import BrokerReconciliationReadError
from .broker_reconciliation_mapping import (
    _dedupe_fill_objects,
    _dedupe_orders,
    fill_fact_from_ib,
    order_fact_from_ib_trade,
)


@dataclass(frozen=True)
class IBBrokerReconciliationConnectionSettings:
    host: str
    port: int
    client_id: int
    account_id: str
    connect_timeout_seconds: float = 15.0
    account_timeout_seconds: float = 5.0
    open_orders_timeout_seconds: float = 15.0
    completed_orders_timeout_seconds: float = 15.0
    executions_timeout_seconds: float = 15.0
    commission_wait_seconds: float = 2.0

    def __post_init__(self) -> None:
        host = str(self.host or "").strip()
        account = str(self.account_id or "").strip()
        if not host or not account:
            raise BrokerReconciliationReadError(
                "IB host and account_id are required"
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "account_id", account)
        try:
            port = int(self.port)
            client_id = int(self.client_id)
        except (TypeError, ValueError) as exc:
            raise BrokerReconciliationReadError(
                "IB port and client_id must be integers"
            ) from exc
        if port <= 0 or port > 65_535:
            raise BrokerReconciliationReadError(f"invalid IB port: {port}")
        if client_id < 0:
            raise BrokerReconciliationReadError(
                f"IB client_id must be non-negative: {client_id}"
            )
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)
        for field_name in (
            "connect_timeout_seconds",
            "account_timeout_seconds",
            "open_orders_timeout_seconds",
            "completed_orders_timeout_seconds",
            "executions_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or value <= 0.0:
                raise BrokerReconciliationReadError(
                    f"{field_name} must be finite and positive: {value}"
                )
            object.__setattr__(self, field_name, value)
        wait = float(self.commission_wait_seconds)
        if not math.isfinite(wait) or wait < 0.0:
            raise BrokerReconciliationReadError(
                "commission_wait_seconds must be finite and non-negative"
            )
        object.__setattr__(self, "commission_wait_seconds", wait)


class IBAsyncBrokerReconciliationReader:
    def __init__(
        self,
        settings: IBBrokerReconciliationConnectionSettings,
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
        return validate_id(self._source_session_id, expected_kind="ib_session")

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
                raise BrokerReconciliationReadError(
                    "cannot read IB managed accounts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if accounts:
                if self.settings.account_id not in accounts:
                    raise BrokerReconciliationReadError(
                        "configured account is absent from IB session: "
                        f"expected={self.settings.account_id}, "
                        f"managed_accounts={accounts}"
                    )
                return
            if loop_time() >= deadline:
                raise BrokerReconciliationReadError(
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
                raise BrokerReconciliationReadError(
                    "IB connectAsync returned without an active connection"
                )
            await self._validate_account_access()
        except Exception:
            self._disconnect_best_effort()
            raise

    async def _request(
        self,
        awaitable: Any,
        *,
        timeout_seconds: float,
        context: str,
    ) -> list[Any]:
        try:
            result = await asyncio.wait_for(
                awaitable,
                timeout=timeout_seconds,
            )
            return list(result or [])
        except asyncio.TimeoutError as exc:
            self._disconnect_best_effort()
            raise BrokerReconciliationReadError(
                f"IB {context} request timed out: timeout={timeout_seconds:g}s"
            ) from exc
        except Exception as exc:
            if not self.connected:
                self._disconnect_best_effort()
            raise BrokerReconciliationReadError(
                f"IB {context} request failed: {type(exc).__name__}: {exc}"
            ) from exc

    @staticmethod
    def _commission_complete(fill: Any) -> bool:
        execution = getattr(fill, "execution", None)
        report = getattr(fill, "commissionReport", None)
        exec_id = str(getattr(execution, "execId", "") or "").strip()
        report_exec_id = str(getattr(report, "execId", "") or "").strip()
        return bool(exec_id and report_exec_id == exec_id)

    async def _wait_for_commissions(self, fills: tuple[Any, ...]) -> tuple[Any, ...]:
        if not fills or self.settings.commission_wait_seconds <= 0.0:
            return fills
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + self.settings.commission_wait_seconds
        current = fills
        while any(not self._commission_complete(item) for item in current):
            if loop_time() >= deadline:
                break
            await asyncio.sleep(0.05)
            cached = list(self._ib.fills() or [])
            current = _dedupe_fill_objects((*current, *cached))
        return current

    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1:
        expected = str(account_id or "").strip()
        if expected != self.settings.account_id:
            raise BrokerReconciliationReadError(
                "broker reconciliation reader account mismatch: "
                f"configured={self.settings.account_id}, requested={expected}"
            )

        async with self._operation_lock:
            await self._ensure_connected()
            open_trades = await self._request(
                self._ib.reqAllOpenOrdersAsync(),
                timeout_seconds=self.settings.open_orders_timeout_seconds,
                context="all-open-orders",
            )
            completed_trades = await self._request(
                self._ib.reqCompletedOrdersAsync(apiOnly=False),
                timeout_seconds=self.settings.completed_orders_timeout_seconds,
                context="completed-orders",
            )
            execution_fills = await self._request(
                self._ib.reqExecutionsAsync(
                    ExecutionFilter(acctCode=expected)
                ),
                timeout_seconds=self.settings.executions_timeout_seconds,
                context="executions",
            )
            fill_objects = _dedupe_fill_objects(
                (*execution_fills, *list(self._ib.fills() or []))
            )
            fill_objects = await self._wait_for_commissions(fill_objects)
            captured_at = format_utc(self._clock())

            open_facts = _dedupe_orders(
                value
                for trade in open_trades
                if (
                    value := order_fact_from_ib_trade(
                        trade,
                        expected_account_id=expected,
                        source=BrokerOrderSource.OPEN,
                        observed_at_utc=captured_at,
                    )
                )
                is not None
            )
            completed_facts = _dedupe_orders(
                value
                for trade in completed_trades
                if (
                    value := order_fact_from_ib_trade(
                        trade,
                        expected_account_id=expected,
                        source=BrokerOrderSource.COMPLETED,
                        observed_at_utc=captured_at,
                    )
                )
                is not None
            )
            fill_facts: list[BrokerFillFactV1] = []
            for fill in fill_objects:
                mapped = fill_fact_from_ib(
                    fill,
                    expected_account_id=expected,
                    observed_at_utc=captured_at,
                )
                if mapped is not None:
                    fill_facts.append(mapped)

            return BrokerReconciliationSnapshotV1(
                source_session_id=self.source_session_id,
                account_id=expected,
                captured_at_utc=captured_at,
                open_orders=open_facts,
                completed_orders=completed_facts,
                fills=tuple(fill_facts),
                requests_complete=True,
            )

    async def close(self) -> None:
        self._disconnect_best_effort()
