from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Mapping

from ibmd.foundation.time import parse_utc

from .common import (
    CatalogError,
    LOCAL_SYMBOL_RE,
    exact_keys,
    identifier,
    parse_contract_date,
    positive_int,
    validate_content_hash,
    validate_hash,
)


@dataclass(frozen=True)
class FuturesContractSpecV1:
    instrument_id: str
    con_id: int
    local_symbol: str
    last_trade_date: str
    active_from_utc: str
    active_to_utc: str

    KEYS: ClassVar[set[str]] = {
        "instrument_id",
        "con_id",
        "local_symbol",
        "last_trade_date",
        "active_from_utc",
        "active_to_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            identifier(self.instrument_id, field_name="instrument_id"),
        )
        con_id = positive_int(self.con_id, field_name="con_id")
        if con_id == 111:
            raise CatalogError("placeholder con_id=111 is forbidden in target calendar")
        object.__setattr__(self, "con_id", con_id)
        symbol = str(self.local_symbol or "").strip()
        if not LOCAL_SYMBOL_RE.fullmatch(symbol):
            raise CatalogError(f"invalid local_symbol: {self.local_symbol!r}")
        object.__setattr__(self, "local_symbol", symbol)
        parse_contract_date(self.last_trade_date, field_name="last_trade_date")
        if self.active_from >= self.active_to:
            raise CatalogError(
                "contract active interval must be positive: "
                f"{self.local_symbol}"
            )

    @property
    def active_from(self) -> datetime:
        return parse_utc(self.active_from_utc)

    @property
    def active_to(self) -> datetime:
        return parse_utc(self.active_to_utc)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FuturesContractSpecV1":
        exact_keys(value, cls.KEYS, context="futures contract")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "last_trade_date": self.last_trade_date,
            "active_from_utc": self.active_from_utc,
            "active_to_utc": self.active_to_utc,
        }


@dataclass(frozen=True)
class DeclaredActivationGapV1:
    start_utc: str
    end_utc: str
    reason: str

    KEYS: ClassVar[set[str]] = {"start_utc", "end_utc", "reason"}

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise CatalogError("declared activation gap must have positive duration")
        object.__setattr__(
            self,
            "reason",
            identifier(self.reason, field_name="gap reason"),
        )

    @property
    def start(self) -> datetime:
        return parse_utc(self.start_utc)

    @property
    def end(self) -> datetime:
        return parse_utc(self.end_utc)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DeclaredActivationGapV1":
        exact_keys(value, cls.KEYS, context="activation gap")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class FuturesContractCalendarV1:
    calendar_version: str
    source_runtime_commit: str
    instrument_id: str
    contracts: tuple[FuturesContractSpecV1, ...]
    declared_gaps: tuple[DeclaredActivationGapV1, ...]
    content_hash: str

    SCHEMA_NAME: ClassVar[str] = "FuturesContractCalendar"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "calendar_version",
        "source_runtime_commit",
        "instrument_id",
        "contracts",
        "declared_gaps",
        "content_hash",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "calendar_version",
            identifier(self.calendar_version, field_name="calendar_version"),
        )
        source = str(self.source_runtime_commit or "").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", source):
            raise CatalogError("source_runtime_commit must be a 40-char git SHA")
        object.__setattr__(self, "source_runtime_commit", source)
        instrument_id = identifier(self.instrument_id, field_name="instrument_id")
        object.__setattr__(self, "instrument_id", instrument_id)
        if not self.contracts:
            raise CatalogError("futures contract calendar cannot be empty")
        if any(item.instrument_id != instrument_id for item in self.contracts):
            raise CatalogError("all contracts must belong to calendar instrument_id")

        con_ids = [item.con_id for item in self.contracts]
        symbols = [item.local_symbol for item in self.contracts]
        if len(con_ids) != len(set(con_ids)):
            raise CatalogError(f"duplicate con_id values: {con_ids}")
        if len(symbols) != len(set(symbols)):
            raise CatalogError(f"duplicate local_symbol values: {symbols}")

        ordered = tuple(sorted(self.contracts, key=lambda item: item.active_from))
        if ordered != self.contracts:
            raise CatalogError("contracts must be ordered by active_from_utc")

        actual_gaps: list[tuple[str, str]] = []
        for previous, current in zip(self.contracts, self.contracts[1:]):
            if current.active_from < previous.active_to:
                raise CatalogError(
                    "contract activation intervals overlap: "
                    f"{previous.local_symbol} -> {current.local_symbol}"
                )
            if current.active_from > previous.active_to:
                actual_gaps.append(
                    (previous.active_to_utc, current.active_from_utc)
                )

        declared = [(item.start_utc, item.end_utc) for item in self.declared_gaps]
        if len(declared) != len(set(declared)):
            raise CatalogError("duplicate declared activation gaps")
        if declared != actual_gaps:
            raise CatalogError(
                "declared activation gaps do not match calendar intervals: "
                f"actual={actual_gaps}, declared={declared}"
            )
        object.__setattr__(self, "content_hash", validate_hash(self.content_hash))

    @property
    def coverage_start(self) -> datetime:
        return self.contracts[0].active_from

    @property
    def coverage_end(self) -> datetime:
        return self.contracts[-1].active_to

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FuturesContractCalendarV1":
        exact_keys(value, cls.KEYS, context="futures contract calendar")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise CatalogError("unsupported futures-contract-calendar schema")
        validate_content_hash(value)
        raw_contracts = value["contracts"]
        raw_gaps = value["declared_gaps"]
        if not isinstance(raw_contracts, list) or not isinstance(raw_gaps, list):
            raise CatalogError("contracts and declared_gaps must be lists")
        return cls(
            calendar_version=str(value["calendar_version"]),
            source_runtime_commit=str(value["source_runtime_commit"]),
            instrument_id=str(value["instrument_id"]),
            contracts=tuple(
                FuturesContractSpecV1.from_dict(item)
                for item in raw_contracts
            ),
            declared_gaps=tuple(
                DeclaredActivationGapV1.from_dict(item)
                for item in raw_gaps
            ),
            content_hash=str(value["content_hash"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "calendar_version": self.calendar_version,
            "source_runtime_commit": self.source_runtime_commit,
            "instrument_id": self.instrument_id,
            "contracts": [item.to_dict() for item in self.contracts],
            "declared_gaps": [item.to_dict() for item in self.declared_gaps],
            "content_hash": self.content_hash,
        }
