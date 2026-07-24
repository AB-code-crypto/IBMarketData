from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CONTRACT_DATE_RE = re.compile(r"^[0-9]{8}$")


class MarketDataContractError(ValueError):
    pass


class QuoteSide(str, Enum):
    BID = "BID"
    ASK = "ASK"


class MarketDataSourceKind(str, Enum):
    HISTORY = "HISTORY"
    REALTIME = "REALTIME"
    RECENT_BACKFILL = "RECENT_BACKFILL"


class MarketDataInstrumentState(str, Enum):
    NO_DATA = "NO_DATA"
    FRESH = "FRESH"
    STALE = "STALE"


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    context: str,
) -> None:
    keys = set(value)
    if keys != expected:
        raise MarketDataContractError(
            f"invalid {context} keys: "
            f"missing={sorted(expected - keys)}, "
            f"unknown={sorted(keys - expected)}"
        )


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise MarketDataContractError(f"{field_name} is required")
    return text


def _identifier(value: object, *, field_name: str) -> str:
    text = _required_text(value, field_name=field_name)
    if not _IDENTIFIER_RE.fullmatch(text):
        raise MarketDataContractError(
            f"{field_name} must match {_IDENTIFIER_RE.pattern}: {value!r}"
        )
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise MarketDataContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise MarketDataContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise MarketDataContractError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise MarketDataContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise MarketDataContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < 0 or exact != float(parsed):
        raise MarketDataContractError(
            f"{field_name} must be a non-negative integer: {value!r}"
        )
    return parsed


def _finite_number(
    value: object,
    *,
    field_name: str,
    positive: bool = False,
    non_negative: bool = False,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise MarketDataContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise MarketDataContractError(
            f"{field_name} must be finite: {value!r}"
        )
    if positive and number <= 0.0:
        raise MarketDataContractError(
            f"{field_name} must be positive: {value!r}"
        )
    if non_negative and number < 0.0:
        raise MarketDataContractError(
            f"{field_name} must be non-negative: {value!r}"
        )
    return number


def _validated_ohlc(
    *,
    prefix: str,
    open_value: object,
    high_value: object,
    low_value: object,
    close_value: object,
) -> tuple[float, float, float, float]:
    open_number = _finite_number(
        open_value,
        field_name=f"{prefix}_open",
        positive=True,
    )
    high_number = _finite_number(
        high_value,
        field_name=f"{prefix}_high",
        positive=True,
    )
    low_number = _finite_number(
        low_value,
        field_name=f"{prefix}_low",
        positive=True,
    )
    close_number = _finite_number(
        close_value,
        field_name=f"{prefix}_close",
        positive=True,
    )
    if high_number < max(open_number, low_number, close_number):
        raise MarketDataContractError(
            f"{prefix}_high is below another OHLC value"
        )
    if low_number > min(open_number, high_number, close_number):
        raise MarketDataContractError(
            f"{prefix}_low is above another OHLC value"
        )
    return open_number, high_number, low_number, close_number


@dataclass(frozen=True)
class MarketDataContractV1:
    instrument_id: str
    sec_type: str
    symbol: str
    exchange: str
    currency: str
    trading_class: str
    multiplier: float
    con_id: int
    local_symbol: str
    last_trade_date: str

    KEYS: ClassVar[set[str]] = {
        "instrument_id",
        "sec_type",
        "symbol",
        "exchange",
        "currency",
        "trading_class",
        "multiplier",
        "con_id",
        "local_symbol",
        "last_trade_date",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        sec_type = _required_text(self.sec_type, field_name="sec_type").upper()
        if sec_type != "FUT":
            raise MarketDataContractError(
                "target market-data shadow currently supports FUT only: "
                f"{sec_type}"
            )
        object.__setattr__(self, "sec_type", sec_type)
        for field_name in (
            "symbol",
            "exchange",
            "currency",
            "trading_class",
            "local_symbol",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "multiplier",
            _finite_number(
                self.multiplier,
                field_name="multiplier",
                positive=True,
            ),
        )
        con_id = _positive_int(self.con_id, field_name="con_id")
        if con_id == 111:
            raise MarketDataContractError(
                "placeholder con_id=111 is forbidden in target market data"
            )
        object.__setattr__(self, "con_id", con_id)
        last_trade_date = _required_text(
            self.last_trade_date,
            field_name="last_trade_date",
        )
        if not _CONTRACT_DATE_RE.fullmatch(last_trade_date):
            raise MarketDataContractError(
                "last_trade_date must use YYYYMMDD: "
                f"{self.last_trade_date!r}"
            )
        object.__setattr__(self, "last_trade_date", last_trade_date)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MarketDataContractV1":
        _exact_keys(value, cls.KEYS, context="market-data contract")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.KEYS}


@dataclass(frozen=True)
class MarketSideBarObservationV1:
    instrument_id: str
    con_id: int
    local_symbol: str
    side: QuoteSide
    bar_start_utc: str
    bar_duration_seconds: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    source_kind: MarketDataSourceKind
    source_session_id: str
    source_generation_id: str
    observed_at_utc: str

    KEYS: ClassVar[set[str]] = {
        "instrument_id",
        "con_id",
        "local_symbol",
        "side",
        "bar_start_utc",
        "bar_duration_seconds",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "source_kind",
        "source_session_id",
        "source_generation_id",
        "observed_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "con_id",
            _positive_int(self.con_id, field_name="con_id"),
        )
        object.__setattr__(
            self,
            "local_symbol",
            _required_text(self.local_symbol, field_name="local_symbol"),
        )
        if not isinstance(self.side, QuoteSide):
            raise MarketDataContractError(f"invalid quote side: {self.side!r}")
        start = parse_utc(self.bar_start_utc)
        duration = _positive_int(
            self.bar_duration_seconds,
            field_name="bar_duration_seconds",
        )
        start_ts = int(start.timestamp())
        if start_ts % duration != 0:
            raise MarketDataContractError(
                "bar_start_utc is not aligned to bar_duration_seconds: "
                f"start={format_utc(start)}, duration={duration}"
            )
        object.__setattr__(self, "bar_start_utc", format_utc(start))
        object.__setattr__(self, "bar_duration_seconds", duration)
        open_price, high_price, low_price, close_price = _validated_ohlc(
            prefix="side",
            open_value=self.open_price,
            high_value=self.high_price,
            low_value=self.low_price,
            close_value=self.close_price,
        )
        object.__setattr__(self, "open_price", open_price)
        object.__setattr__(self, "high_price", high_price)
        object.__setattr__(self, "low_price", low_price)
        object.__setattr__(self, "close_price", close_price)
        if not isinstance(self.source_kind, MarketDataSourceKind):
            raise MarketDataContractError(
                f"invalid source_kind: {self.source_kind!r}"
            )
        object.__setattr__(
            self,
            "source_session_id",
            validate_id(self.source_session_id, expected_kind="ib_session"),
        )
        object.__setattr__(
            self,
            "source_generation_id",
            validate_id(
                self.source_generation_id,
                expected_kind="md_generation",
            ),
        )
        observed = parse_utc(self.observed_at_utc)
        if observed < start:
            raise MarketDataContractError(
                "observed_at_utc cannot precede bar_start_utc"
            )
        object.__setattr__(self, "observed_at_utc", format_utc(observed))

    @property
    def bar_end_utc(self) -> str:
        return format_utc(
            parse_utc(self.bar_start_utc)
            + timedelta(seconds=self.bar_duration_seconds)
        )

    @property
    def natural_key(self) -> tuple[str, int, str, int]:
        return (
            self.instrument_id,
            self.con_id,
            self.bar_start_utc,
            self.bar_duration_seconds,
        )

    @property
    def assembly_key(self) -> tuple[str, int, str, int, str, str, str]:
        return (
            *self.natural_key,
            self.source_session_id,
            self.source_generation_id,
            self.source_kind.value,
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "MarketSideBarObservationV1":
        _exact_keys(value, cls.KEYS, context="market-side observation")
        try:
            side = QuoteSide(str(value["side"]))
            source_kind = MarketDataSourceKind(str(value["source_kind"]))
        except ValueError as exc:
            raise MarketDataContractError(
                "invalid side or source kind in market-side observation"
            ) from exc
        return cls(
            instrument_id=str(value["instrument_id"]),
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            side=side,
            bar_start_utc=str(value["bar_start_utc"]),
            bar_duration_seconds=value["bar_duration_seconds"],
            open_price=value["open_price"],
            high_price=value["high_price"],
            low_price=value["low_price"],
            close_price=value["close_price"],
            source_kind=source_kind,
            source_session_id=str(value["source_session_id"]),
            source_generation_id=str(value["source_generation_id"]),
            observed_at_utc=str(value["observed_at_utc"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "side": self.side.value,
            "bar_start_utc": self.bar_start_utc,
            "bar_duration_seconds": self.bar_duration_seconds,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "source_kind": self.source_kind.value,
            "source_session_id": self.source_session_id,
            "source_generation_id": self.source_generation_id,
            "observed_at_utc": self.observed_at_utc,
        }


@dataclass(frozen=True)
class MarketBarFreshnessV1:
    bar_id: str
    bar_end_utc: str
    observed_at_utc: str
    age_seconds: float
    max_age_seconds: float
    is_fresh: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "bar_id",
            validate_id(self.bar_id, expected_kind="market_bar"),
        )
        bar_end = parse_utc(self.bar_end_utc)
        observed = parse_utc(self.observed_at_utc)
        expected_age = max(0.0, (observed - bar_end).total_seconds())
        age = _finite_number(self.age_seconds, field_name="age_seconds")
        maximum = _finite_number(
            self.max_age_seconds,
            field_name="max_age_seconds",
            non_negative=True,
        )
        if abs(age - expected_age) > 1e-6:
            raise MarketDataContractError(
                "age_seconds does not match bar_end/observed timestamps: "
                f"expected={expected_age}, actual={age}"
            )
        expected_fresh = age <= maximum
        if bool(self.is_fresh) != expected_fresh:
            raise MarketDataContractError(
                "is_fresh does not match age/max-age policy"
            )
        object.__setattr__(self, "bar_end_utc", format_utc(bar_end))
        object.__setattr__(self, "observed_at_utc", format_utc(observed))
        object.__setattr__(self, "age_seconds", age)
        object.__setattr__(self, "max_age_seconds", maximum)
        object.__setattr__(self, "is_fresh", expected_fresh)


@dataclass(frozen=True)
class MarketBarV1:
    bar_id: str
    instrument_id: str
    con_id: int
    local_symbol: str
    bar_start_utc: str
    bar_end_utc: str
    bar_duration_seconds: int
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    source_kind: MarketDataSourceKind
    first_published_at_utc: str
    published_at_utc: str
    revision: int
    complete: bool = True
    volume: float | None = None
    average: float | None = None
    bar_count: int | None = None

    SCHEMA_NAME: ClassVar[str] = "MarketBar"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "bar_id",
        "instrument_id",
        "con_id",
        "local_symbol",
        "bar_start_utc",
        "bar_end_utc",
        "bar_duration_seconds",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "source_kind",
        "first_published_at_utc",
        "published_at_utc",
        "revision",
        "complete",
        "volume",
        "average",
        "bar_count",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "bar_id",
            validate_id(self.bar_id, expected_kind="market_bar"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "con_id",
            _positive_int(self.con_id, field_name="con_id"),
        )
        object.__setattr__(
            self,
            "local_symbol",
            _required_text(self.local_symbol, field_name="local_symbol"),
        )
        start = parse_utc(self.bar_start_utc)
        end = parse_utc(self.bar_end_utc)
        duration = _positive_int(
            self.bar_duration_seconds,
            field_name="bar_duration_seconds",
        )
        if int(start.timestamp()) % duration != 0:
            raise MarketDataContractError(
                "bar_start_utc is not aligned to bar_duration_seconds"
            )
        expected_end = start + timedelta(seconds=duration)
        if end != expected_end:
            raise MarketDataContractError(
                "bar_end_utc does not match start + duration: "
                f"expected={format_utc(expected_end)}, actual={format_utc(end)}"
            )
        object.__setattr__(self, "bar_start_utc", format_utc(start))
        object.__setattr__(self, "bar_end_utc", format_utc(end))
        object.__setattr__(self, "bar_duration_seconds", duration)

        bid = _validated_ohlc(
            prefix="bid",
            open_value=self.bid_open,
            high_value=self.bid_high,
            low_value=self.bid_low,
            close_value=self.bid_close,
        )
        ask = _validated_ohlc(
            prefix="ask",
            open_value=self.ask_open,
            high_value=self.ask_high,
            low_value=self.ask_low,
            close_value=self.ask_close,
        )
        for field_name, value in zip(
            ("bid_open", "bid_high", "bid_low", "bid_close"),
            bid,
        ):
            object.__setattr__(self, field_name, value)
        for field_name, value in zip(
            ("ask_open", "ask_high", "ask_low", "ask_close"),
            ask,
        ):
            object.__setattr__(self, field_name, value)

        if not isinstance(self.source_kind, MarketDataSourceKind):
            raise MarketDataContractError(
                f"invalid source_kind: {self.source_kind!r}"
            )
        first = parse_utc(self.first_published_at_utc)
        published = parse_utc(self.published_at_utc)
        if published < first:
            raise MarketDataContractError(
                "published_at_utc cannot precede first_published_at_utc"
            )
        if first < start:
            raise MarketDataContractError(
                "first_published_at_utc cannot precede bar_start_utc"
            )
        object.__setattr__(
            self,
            "first_published_at_utc",
            format_utc(first),
        )
        object.__setattr__(self, "published_at_utc", format_utc(published))
        object.__setattr__(
            self,
            "revision",
            _positive_int(self.revision, field_name="revision"),
        )
        if self.complete is not True:
            raise MarketDataContractError(
                "MarketBarV1 is public only when complete=True"
            )
        if self.volume is not None:
            object.__setattr__(
                self,
                "volume",
                _finite_number(
                    self.volume,
                    field_name="volume",
                    non_negative=True,
                ),
            )
        if self.average is not None:
            object.__setattr__(
                self,
                "average",
                _finite_number(
                    self.average,
                    field_name="average",
                    positive=True,
                ),
            )
        if self.bar_count is not None:
            object.__setattr__(
                self,
                "bar_count",
                _non_negative_int(self.bar_count, field_name="bar_count"),
            )

    @property
    def mid_close(self) -> float:
        return (self.bid_close + self.ask_close) / 2.0

    def rounded_mid_close(self, digits: int) -> float:
        precision = _non_negative_int(digits, field_name="digits")
        return round(self.mid_close, precision)

    def material_tuple(self) -> tuple[object, ...]:
        return (
            self.instrument_id,
            self.con_id,
            self.local_symbol,
            self.bar_start_utc,
            self.bar_end_utc,
            self.bar_duration_seconds,
            self.bid_open,
            self.bid_high,
            self.bid_low,
            self.bid_close,
            self.ask_open,
            self.ask_high,
            self.ask_low,
            self.ask_close,
            self.source_kind.value,
            self.volume,
            self.average,
            self.bar_count,
        )

    def freshness(
        self,
        *,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> MarketBarFreshnessV1:
        end = parse_utc(self.bar_end_utc)
        observed = parse_utc(observed_at_utc)
        age = max(0.0, (observed - end).total_seconds())
        maximum = _finite_number(
            max_age_seconds,
            field_name="max_age_seconds",
            non_negative=True,
        )
        return MarketBarFreshnessV1(
            bar_id=self.bar_id,
            bar_end_utc=format_utc(end),
            observed_at_utc=format_utc(observed),
            age_seconds=age,
            max_age_seconds=maximum,
            is_fresh=age <= maximum,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MarketBarV1":
        _exact_keys(value, cls.KEYS, context="market bar")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise MarketDataContractError("unsupported market-bar schema")
        try:
            source_kind = MarketDataSourceKind(str(value["source_kind"]))
        except ValueError as exc:
            raise MarketDataContractError(
                f"invalid source_kind: {value['source_kind']!r}"
            ) from exc
        return cls(
            bar_id=str(value["bar_id"]),
            instrument_id=str(value["instrument_id"]),
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            bar_start_utc=str(value["bar_start_utc"]),
            bar_end_utc=str(value["bar_end_utc"]),
            bar_duration_seconds=value["bar_duration_seconds"],
            bid_open=value["bid_open"],
            bid_high=value["bid_high"],
            bid_low=value["bid_low"],
            bid_close=value["bid_close"],
            ask_open=value["ask_open"],
            ask_high=value["ask_high"],
            ask_low=value["ask_low"],
            ask_close=value["ask_close"],
            source_kind=source_kind,
            first_published_at_utc=str(value["first_published_at_utc"]),
            published_at_utc=str(value["published_at_utc"]),
            revision=value["revision"],
            complete=value["complete"],
            volume=value["volume"],
            average=value["average"],
            bar_count=value["bar_count"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "bar_id": self.bar_id,
            "instrument_id": self.instrument_id,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "bar_start_utc": self.bar_start_utc,
            "bar_end_utc": self.bar_end_utc,
            "bar_duration_seconds": self.bar_duration_seconds,
            "bid_open": self.bid_open,
            "bid_high": self.bid_high,
            "bid_low": self.bid_low,
            "bid_close": self.bid_close,
            "ask_open": self.ask_open,
            "ask_high": self.ask_high,
            "ask_low": self.ask_low,
            "ask_close": self.ask_close,
            "source_kind": self.source_kind.value,
            "first_published_at_utc": self.first_published_at_utc,
            "published_at_utc": self.published_at_utc,
            "revision": self.revision,
            "complete": self.complete,
            "volume": self.volume,
            "average": self.average,
            "bar_count": self.bar_count,
        }


@dataclass(frozen=True)
class MarketDataInstrumentStatusV1:
    instrument_id: str
    observed_at_utc: str
    max_age_seconds: float
    state: MarketDataInstrumentState
    latest_bar_id: str | None = None
    latest_bar_end_utc: str | None = None
    latest_con_id: int | None = None
    latest_local_symbol: str | None = None
    age_seconds: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "observed_at_utc",
            format_utc(parse_utc(self.observed_at_utc)),
        )
        object.__setattr__(
            self,
            "max_age_seconds",
            _finite_number(
                self.max_age_seconds,
                field_name="max_age_seconds",
                non_negative=True,
            ),
        )
        if not isinstance(self.state, MarketDataInstrumentState):
            raise MarketDataContractError(
                f"invalid market-data state: {self.state!r}"
            )
        if self.state == MarketDataInstrumentState.NO_DATA:
            if any(
                value is not None
                for value in (
                    self.latest_bar_id,
                    self.latest_bar_end_utc,
                    self.latest_con_id,
                    self.latest_local_symbol,
                    self.age_seconds,
                )
            ):
                raise MarketDataContractError(
                    "NO_DATA status cannot contain latest-bar fields"
                )
            return
        if self.latest_bar_id is None or self.latest_bar_end_utc is None:
            raise MarketDataContractError(
                "latest bar identity/time are required outside NO_DATA"
            )
        object.__setattr__(
            self,
            "latest_bar_id",
            validate_id(self.latest_bar_id, expected_kind="market_bar"),
        )
        object.__setattr__(
            self,
            "latest_bar_end_utc",
            format_utc(parse_utc(self.latest_bar_end_utc)),
        )
        object.__setattr__(
            self,
            "latest_con_id",
            _positive_int(self.latest_con_id, field_name="latest_con_id"),
        )
        object.__setattr__(
            self,
            "latest_local_symbol",
            _required_text(
                self.latest_local_symbol,
                field_name="latest_local_symbol",
            ),
        )
        age = _finite_number(
            self.age_seconds,
            field_name="age_seconds",
            non_negative=True,
        )
        object.__setattr__(self, "age_seconds", age)
        expected_state = (
            MarketDataInstrumentState.FRESH
            if age <= self.max_age_seconds
            else MarketDataInstrumentState.STALE
        )
        if self.state != expected_state:
            raise MarketDataContractError(
                "market-data state does not match age/max-age policy"
            )

    @classmethod
    def from_latest(
        cls,
        *,
        instrument_id: str,
        latest: MarketBarV1 | None,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> "MarketDataInstrumentStatusV1":
        if latest is None:
            return cls(
                instrument_id=instrument_id,
                observed_at_utc=observed_at_utc,
                max_age_seconds=max_age_seconds,
                state=MarketDataInstrumentState.NO_DATA,
            )
        if latest.instrument_id != str(instrument_id):
            raise MarketDataContractError(
                "latest market bar belongs to another instrument"
            )
        freshness = latest.freshness(
            observed_at_utc=observed_at_utc,
            max_age_seconds=max_age_seconds,
        )
        return cls(
            instrument_id=instrument_id,
            observed_at_utc=observed_at_utc,
            max_age_seconds=max_age_seconds,
            state=(
                MarketDataInstrumentState.FRESH
                if freshness.is_fresh
                else MarketDataInstrumentState.STALE
            ),
            latest_bar_id=latest.bar_id,
            latest_bar_end_utc=latest.bar_end_utc,
            latest_con_id=latest.con_id,
            latest_local_symbol=latest.local_symbol,
            age_seconds=freshness.age_seconds,
        )
