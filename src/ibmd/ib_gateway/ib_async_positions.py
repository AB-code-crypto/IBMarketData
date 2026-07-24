from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from ib_async import IB

from ibmd.foundation.identity import new_id

from .positions import (
    BrokerPositionReadError,
    RawBrokerPosition,
    validate_source_session_id,
)


@dataclass(frozen=True)
class IBPositionConnectionSettings:
    host: str
    port: int
    client_id: int
    account_id: str
    connect_timeout_seconds: float = 15.0
    account_timeout_seconds: float = 5.0
    position_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        host = str(self.host or "").strip()
        account = str(self.account_id or "").strip()
        if not host or not account:
            raise BrokerPositionReadError(
                "IB host and account_id are required"
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "account_id", account)

        try:
            port = int(self.port)
            client_id = int(self.client_id)
        except (TypeError, ValueError) as exc:
            raise BrokerPositionReadError(
                "IB port and client_id must be integers"
            ) from exc
        if port <= 0 or port > 65_535:
            raise BrokerPositionReadError(f"invalid IB port: {port}")
        if client_id < 0:
            raise BrokerPositionReadError(
                f"IB client_id must be non-negative: {client_id}"
            )
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)

        for field_name in (
            "connect_timeout_seconds",
            "account_timeout_seconds",
            "position_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if value <= 0.0:
                raise BrokerPositionReadError(
                    f"{field_name} must be positive: {value}"
                )
            object.__setattr__(self, field_name, value)


def raw_position_from_ib(
    value: Any,
    *,
    expected_account_id: str,
) -> RawBrokerPosition | None:
    expected = str(expected_account_id or "").strip()
    actual = str(getattr(value, "account", "") or "").strip()
    if not actual:
        raise BrokerPositionReadError(
            "IB returned a position without account identity"
        )
    if actual != expected:
        return None

    contract = getattr(value, "contract", None)
    if contract is None:
        raise BrokerPositionReadError(
            f"IB returned a position without contract: account={actual}"
        )

    try:
        signed_quantity = float(
            getattr(value, "position", 0.0) or 0.0
        )
    except (TypeError, ValueError) as exc:
        raise BrokerPositionReadError(
            "IB returned a non-numeric position quantity"
        ) from exc
    if abs(signed_quantity) <= 1e-12:
        return None

    average_cost_value = getattr(value, "avgCost", None)
    average_cost = (
        None
        if average_cost_value is None
        else float(average_cost_value)
    )
    exchange = (
        str(getattr(contract, "exchange", "") or "").strip()
        or str(getattr(contract, "primaryExchange", "") or "").strip()
        or None
    )
    return RawBrokerPosition(
        account_id=actual,
        con_id=int(getattr(contract, "conId", 0) or 0),
        local_symbol=(
            str(getattr(contract, "localSymbol", "") or "").strip()
            or None
        ),
        symbol=str(getattr(contract, "symbol", "") or "").strip(),
        sec_type=str(getattr(contract, "secType", "") or "").strip(),
        exchange=exchange,
        currency=(
            str(getattr(contract, "currency", "") or "").strip()
            or None
        ),
        signed_quantity=signed_quantity,
        average_cost=average_cost,
    )


class IBAsyncPositionReader:
    def __init__(
        self,
        settings: IBPositionConnectionSettings,
        *,
        ib_factory: Callable[[], Any] = IB,
    ) -> None:
        self.settings = settings
        self._ib = ib_factory()
        self._read_lock = asyncio.Lock()
        self._source_session_id = new_id("ib_session")

    @property
    def source_session_id(self) -> str:
        return validate_source_session_id(self._source_session_id)

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
                raise BrokerPositionReadError(
                    "cannot read IB managed accounts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            if accounts:
                if self.settings.account_id not in accounts:
                    raise BrokerPositionReadError(
                        "configured account is absent from IB session: "
                        f"expected={self.settings.account_id}, "
                        f"managed_accounts={accounts}"
                    )
                return
            if loop_time() >= deadline:
                raise BrokerPositionReadError(
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
                ),
                timeout=self.settings.connect_timeout_seconds,
            )
            if not self.connected:
                raise BrokerPositionReadError(
                    "IB connectAsync returned without an active connection"
                )
            await self._validate_account_access()
        except Exception:
            self._disconnect_best_effort()
            raise

    async def read_positions(
        self,
        *,
        account_id: str,
    ) -> tuple[RawBrokerPosition, ...]:
        expected = str(account_id or "").strip()
        if expected != self.settings.account_id:
            raise BrokerPositionReadError(
                "position reader account mismatch: "
                f"configured={self.settings.account_id}, requested={expected}"
            )

        async with self._read_lock:
            await self._ensure_connected()
            try:
                result = await asyncio.wait_for(
                    self._ib.reqPositionsAsync(),
                    timeout=self.settings.position_timeout_seconds,
                )
            except asyncio.TimeoutError as exc:
                cancel = getattr(self._ib, "cancelPositions", None)
                if cancel is not None:
                    try:
                        cancel()
                    except Exception:
                        pass
                self._disconnect_best_effort()
                raise BrokerPositionReadError(
                    "IB position request timed out: "
                    f"timeout={self.settings.position_timeout_seconds:g}s"
                ) from exc
            except Exception as exc:
                if not self.connected:
                    self._disconnect_best_effort()
                raise BrokerPositionReadError(
                    "IB position request failed: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            values = (
                list(self._ib.positions() or [])
                if result is None
                else list(result)
            )
            rows: list[RawBrokerPosition] = []
            for value in values:
                mapped = raw_position_from_ib(
                    value,
                    expected_account_id=expected,
                )
                if mapped is not None:
                    rows.append(mapped)
            return tuple(
                sorted(
                    rows,
                    key=lambda item: (
                        item.con_id,
                        item.local_symbol or "",
                        item.symbol,
                    ),
                )
            )

    async def close(self) -> None:
        self._disconnect_best_effort()
