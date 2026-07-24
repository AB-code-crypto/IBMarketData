from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from .common import (
    CatalogError,
    exact_keys,
    identifier,
    non_negative_int,
    positive_float,
    positive_int,
    required_text,
    validate_content_hash,
    validate_hash,
)


@dataclass(frozen=True)
class InstrumentSpecV1:
    instrument_id: str
    sec_type: str
    trading_class: str
    exchange: str
    currency: str
    multiplier: float
    price_tick: float
    price_precision: int
    default_bar_size_seconds: int
    database_name: str

    KEYS: ClassVar[set[str]] = {
        "instrument_id",
        "sec_type",
        "trading_class",
        "exchange",
        "currency",
        "multiplier",
        "price_tick",
        "price_precision",
        "default_bar_size_seconds",
        "database_name",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            identifier(self.instrument_id, field_name="instrument_id"),
        )
        sec_type = str(self.sec_type or "").strip().upper()
        if sec_type not in {"FUT", "CASH", "CRYPTO"}:
            raise CatalogError(f"unsupported sec_type: {self.sec_type!r}")
        object.__setattr__(self, "sec_type", sec_type)
        for field_name in ("trading_class", "exchange", "currency"):
            object.__setattr__(
                self,
                field_name,
                required_text(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "multiplier",
            positive_float(self.multiplier, field_name="multiplier"),
        )
        object.__setattr__(
            self,
            "price_tick",
            positive_float(self.price_tick, field_name="price_tick"),
        )
        precision = non_negative_int(
            self.price_precision,
            field_name="price_precision",
        )
        if precision > 12:
            raise CatalogError(f"price_precision is implausibly large: {precision}")
        object.__setattr__(self, "price_precision", precision)
        object.__setattr__(
            self,
            "default_bar_size_seconds",
            positive_int(
                self.default_bar_size_seconds,
                field_name="default_bar_size_seconds",
            ),
        )
        database_name = required_text(
            self.database_name,
            field_name="database_name",
        )
        if "/" in database_name or "\\" in database_name:
            raise CatalogError("database_name must be a file name, not a path")
        object.__setattr__(self, "database_name", database_name)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "InstrumentSpecV1":
        exact_keys(value, cls.KEYS, context="instrument")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "sec_type": self.sec_type,
            "trading_class": self.trading_class,
            "exchange": self.exchange,
            "currency": self.currency,
            "multiplier": self.multiplier,
            "price_tick": self.price_tick,
            "price_precision": self.price_precision,
            "default_bar_size_seconds": self.default_bar_size_seconds,
            "database_name": self.database_name,
        }


@dataclass(frozen=True)
class InstrumentMasterV1:
    artifact_version: str
    source_runtime_commit: str
    instruments: tuple[InstrumentSpecV1, ...]
    content_hash: str

    SCHEMA_NAME: ClassVar[str] = "InstrumentMaster"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "artifact_version",
        "source_runtime_commit",
        "instruments",
        "content_hash",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_version",
            identifier(self.artifact_version, field_name="artifact_version"),
        )
        source = str(self.source_runtime_commit or "").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", source):
            raise CatalogError("source_runtime_commit must be a 40-char git SHA")
        object.__setattr__(self, "source_runtime_commit", source)
        if not self.instruments:
            raise CatalogError("instrument master cannot be empty")
        ids = [item.instrument_id for item in self.instruments]
        if len(ids) != len(set(ids)):
            raise CatalogError(f"duplicate instrument_id values: {ids}")
        object.__setattr__(self, "content_hash", validate_hash(self.content_hash))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "InstrumentMasterV1":
        exact_keys(value, cls.KEYS, context="instrument master")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise CatalogError("unsupported instrument-master schema")
        validate_content_hash(value)
        raw = value["instruments"]
        if not isinstance(raw, list):
            raise CatalogError("instruments must be a list")
        return cls(
            artifact_version=str(value["artifact_version"]),
            source_runtime_commit=str(value["source_runtime_commit"]),
            instruments=tuple(InstrumentSpecV1.from_dict(item) for item in raw),
            content_hash=str(value["content_hash"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "artifact_version": self.artifact_version,
            "source_runtime_commit": self.source_runtime_commit,
            "instruments": [item.to_dict() for item in self.instruments],
            "content_hash": self.content_hash,
        }

    def require(self, instrument_id: str) -> InstrumentSpecV1:
        expected = str(instrument_id)
        for item in self.instruments:
            if item.instrument_id == expected:
                return item
        raise CatalogError(f"instrument is not registered: {instrument_id}")
