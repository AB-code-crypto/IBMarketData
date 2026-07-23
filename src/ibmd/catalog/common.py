from __future__ import annotations

import hashlib
import math
import re
from datetime import date, datetime
from typing import Any, Mapping

from ibmd.foundation.atomic_json import canonical_json_text

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
LOCAL_SYMBOL_RE = re.compile(r"^[A-Za-z0-9._-]{1,32}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
CONTRACT_DATE_RE = re.compile(r"^\d{8}$")
TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$|^24:00:00$")
WEEKDAYS = (
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
)


class CatalogError(ValueError):
    pass


def exact_keys(value: Mapping[str, Any], expected: set[str], *, context: str) -> None:
    keys = set(value)
    if keys != expected:
        raise CatalogError(
            f"invalid {context} keys: missing={sorted(expected - keys)}, "
            f"unknown={sorted(keys - expected)}"
        )


def identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(text):
        raise CatalogError(f"invalid {field_name}: {value!r}")
    return text


def required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CatalogError(f"{field_name} is required")
    return text


def positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
        raise CatalogError(f"{field_name} must be an integer: {value!r}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"{field_name} must be an integer: {value!r}") from exc
    if parsed <= 0:
        raise CatalogError(f"{field_name} must be positive: {parsed}")
    return parsed


def non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
        raise CatalogError(f"{field_name} must be an integer: {value!r}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"{field_name} must be an integer: {value!r}") from exc
    if parsed < 0:
        raise CatalogError(f"{field_name} must be non-negative: {parsed}")
    return parsed


def boolean(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise CatalogError(f"{field_name} must be a boolean: {value!r}")
    return value


def positive_float(value: object, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"{field_name} must be numeric: {value!r}") from exc
    if parsed <= 0.0 or not math.isfinite(parsed):
        raise CatalogError(f"{field_name} must be positive and finite: {parsed!r}")
    return parsed


def non_negative_float(value: object, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"{field_name} must be numeric: {value!r}") from exc
    if parsed < 0.0 or not math.isfinite(parsed):
        raise CatalogError(f"{field_name} must be non-negative and finite: {parsed!r}")
    return parsed


def parse_date(value: object, *, field_name: str) -> date:
    text = str(value or "").strip()
    if not DATE_RE.fullmatch(text):
        raise CatalogError(f"{field_name} must use YYYY-MM-DD: {value!r}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise CatalogError(f"invalid {field_name}: {value!r}") from exc


def parse_contract_date(value: object, *, field_name: str) -> date:
    text = str(value or "").strip()
    if not CONTRACT_DATE_RE.fullmatch(text):
        raise CatalogError(f"{field_name} must use YYYYMMDD: {value!r}")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise CatalogError(f"invalid {field_name}: {value!r}") from exc


def seconds_of_day(value: object, *, field_name: str) -> int:
    text = str(value or "").strip()
    if not TIME_RE.fullmatch(text):
        raise CatalogError(f"{field_name} must use HH:MM:SS: {value!r}")
    if text == "24:00:00":
        return 24 * 60 * 60
    hour, minute, second = (int(part) for part in text.split(":"))
    return hour * 3600 + minute * 60 + second


def validate_hash(value: object, *, field_name: str = "content_hash") -> str:
    text = str(value or "").strip()
    if not HASH_RE.fullmatch(text):
        raise CatalogError(f"{field_name} must be lowercase SHA-256 hex")
    return text


def compute_content_hash(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("content_hash", None)
    return hashlib.sha256(canonical_json_text(payload).encode("utf-8")).hexdigest()


def validate_content_hash(value: Mapping[str, Any]) -> str:
    actual = validate_hash(value.get("content_hash"))
    expected = compute_content_hash(value)
    if actual != expected:
        raise CatalogError(
            f"artifact content_hash mismatch: expected={expected}, actual={actual}"
        )
    return actual
