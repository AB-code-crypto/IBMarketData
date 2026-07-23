from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc, utc_now_text

_REQUIRED_KEYS = {
    "schema_name",
    "schema_version",
    "producer_version",
    "created_at_utc",
    "correlation_id",
    "payload",
}


class ContractValidationError(ValueError):
    pass


def _validate_schema_name(value: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 128:
        raise ContractValidationError(f"schema_name is invalid: {value!r}")
    if not all(character.isalnum() or character in "._-" for character in text):
        raise ContractValidationError(f"schema_name contains unsupported characters: {value!r}")
    return text


@dataclass(frozen=True)
class ContractEnvelopeV1:
    schema_name: str
    producer_version: str
    created_at_utc: str
    payload: Mapping[str, Any]
    correlation_id: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        _validate_schema_name(self.schema_name)
        if int(self.schema_version) != 1:
            raise ContractValidationError(
                f"ContractEnvelopeV1 requires schema_version=1: {self.schema_version}"
            )
        if not str(self.producer_version or "").strip():
            raise ContractValidationError("producer_version is required")
        parse_utc(self.created_at_utc)
        if self.correlation_id is not None and not str(self.correlation_id).strip():
            raise ContractValidationError("correlation_id cannot be blank")
        if not isinstance(self.payload, Mapping):
            raise ContractValidationError("payload must be a mapping")
        canonical_json_text(dict(self.payload))

    @classmethod
    def create(
        cls,
        *,
        schema_name: str,
        producer_version: str,
        payload: Mapping[str, Any],
        correlation_id: str | None = None,
        created_at_utc: str | None = None,
    ) -> "ContractEnvelopeV1":
        return cls(
            schema_name=schema_name,
            producer_version=producer_version,
            created_at_utc=created_at_utc or utc_now_text(),
            payload=deepcopy(dict(payload)),
            correlation_id=correlation_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "producer_version": self.producer_version,
            "created_at_utc": self.created_at_utc,
            "correlation_id": self.correlation_id,
            "payload": deepcopy(dict(self.payload)),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ContractEnvelopeV1":
        keys = set(value.keys())
        if keys != _REQUIRED_KEYS:
            missing = sorted(_REQUIRED_KEYS - keys)
            unknown = sorted(keys - _REQUIRED_KEYS)
            raise ContractValidationError(
                f"invalid envelope keys: missing={missing}, unknown={unknown}"
            )
        payload = value["payload"]
        if not isinstance(payload, Mapping):
            raise ContractValidationError("payload must be a mapping")
        correlation = value["correlation_id"]
        return cls(
            schema_name=str(value["schema_name"]),
            schema_version=int(value["schema_version"]),
            producer_version=str(value["producer_version"]),
            created_at_utc=str(value["created_at_utc"]),
            correlation_id=None if correlation is None else str(correlation),
            payload=deepcopy(dict(payload)),
        )
