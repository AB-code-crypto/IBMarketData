from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ibmd.foundation.identity import validate_id
from ibmd.public_contracts.positions import BrokerPositionRowV1


class BrokerPositionReadError(RuntimeError):
    pass


@dataclass(frozen=True)
class RawBrokerPosition:
    account_id: str
    con_id: int
    local_symbol: str | None
    symbol: str
    sec_type: str
    exchange: str | None
    currency: str | None
    signed_quantity: float
    average_cost: float | None

    def __post_init__(self) -> None:
        account = str(self.account_id or "").strip()
        if not account:
            raise BrokerPositionReadError("raw position account_id is required")
        object.__setattr__(self, "account_id", account)

        try:
            con_id = int(self.con_id)
        except (TypeError, ValueError) as exc:
            raise BrokerPositionReadError(
                f"raw position con_id must be an integer: {self.con_id!r}"
            ) from exc
        if con_id <= 0:
            raise BrokerPositionReadError(
                f"raw position con_id must be positive: {con_id}"
            )
        object.__setattr__(self, "con_id", con_id)

        local_symbol = (
            None
            if self.local_symbol is None
            else str(self.local_symbol).strip() or None
        )
        object.__setattr__(self, "local_symbol", local_symbol)

        symbol = str(self.symbol or "").strip()
        sec_type = str(self.sec_type or "").strip().upper()
        if not symbol or not sec_type:
            raise BrokerPositionReadError(
                "raw position symbol and sec_type are required"
            )
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "sec_type", sec_type)
        object.__setattr__(
            self,
            "exchange",
            None
            if self.exchange is None
            else str(self.exchange).strip() or None,
        )
        object.__setattr__(
            self,
            "currency",
            None
            if self.currency is None
            else str(self.currency).strip() or None,
        )

        try:
            quantity = float(self.signed_quantity)
        except (TypeError, ValueError) as exc:
            raise BrokerPositionReadError(
                "raw position signed_quantity must be numeric: "
                f"{self.signed_quantity!r}"
            ) from exc
        if not math.isfinite(quantity) or abs(quantity) <= 1e-12:
            raise BrokerPositionReadError(
                "raw position signed_quantity must be finite and non-zero: "
                f"{self.signed_quantity!r}"
            )
        object.__setattr__(self, "signed_quantity", quantity)

        if self.average_cost is not None:
            try:
                average_cost = float(self.average_cost)
            except (TypeError, ValueError) as exc:
                raise BrokerPositionReadError(
                    f"raw position average_cost must be numeric: {self.average_cost!r}"
                ) from exc
            if not math.isfinite(average_cost):
                raise BrokerPositionReadError(
                    f"raw position average_cost must be finite: {average_cost!r}"
                )
            object.__setattr__(self, "average_cost", average_cost)

    def to_public_row(self) -> BrokerPositionRowV1:
        return BrokerPositionRowV1(
            con_id=self.con_id,
            local_symbol=self.local_symbol,
            symbol=self.symbol,
            sec_type=self.sec_type,
            exchange=self.exchange,
            currency=self.currency,
            signed_quantity=self.signed_quantity,
            average_cost=self.average_cost,
        )


@runtime_checkable
class IBPositionReader(Protocol):
    @property
    def source_session_id(self) -> str:
        ...

    async def read_positions(
        self,
        *,
        account_id: str,
    ) -> tuple[RawBrokerPosition, ...]:
        ...

    async def close(self) -> None:
        ...


def validate_source_session_id(value: str) -> str:
    return validate_id(value, expected_kind="ib_session")
