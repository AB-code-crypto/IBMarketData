from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.time import parse_utc, utc_now_text

_SERVICE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_HEALTH_KEYS = {
    "schema_name",
    "schema_version",
    "service",
    "deployment_id",
    "instance_id",
    "pid",
    "liveness",
    "readiness",
    "started_at_utc",
    "last_heartbeat_at_utc",
    "last_success_at_utc",
    "published_at_utc",
    "source_freshness_seconds",
    "dependency_status",
    "blocking_reason",
    "application_version",
    "configuration_hash",
}
_DEPENDENCY_KEYS = {"name", "status", "detail", "observed_at_utc"}


class HealthContractError(ValueError):
    pass


class Liveness(str, Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class Readiness(str, Enum):
    NOT_READY = "NOT_READY"
    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class DependencyStatusV1:
    name: str
    status: str
    detail: str | None = None
    observed_at_utc: str | None = None

    def __post_init__(self) -> None:
        if not str(self.name or "").strip():
            raise HealthContractError("dependency name is required")
        if not str(self.status or "").strip():
            raise HealthContractError("dependency status is required")
        if self.observed_at_utc is not None:
            parse_utc(self.observed_at_utc)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "observed_at_utc": self.observed_at_utc,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DependencyStatusV1":
        keys = set(value.keys())
        if keys != _DEPENDENCY_KEYS:
            raise HealthContractError(
                "invalid dependency-status keys: "
                f"missing={sorted(_DEPENDENCY_KEYS - keys)}, "
                f"unknown={sorted(keys - _DEPENDENCY_KEYS)}"
            )
        detail = value["detail"]
        observed = value["observed_at_utc"]
        return cls(
            name=str(value["name"]),
            status=str(value["status"]),
            detail=None if detail is None else str(detail),
            observed_at_utc=None if observed is None else str(observed),
        )


@dataclass(frozen=True)
class ServiceHealthV1:
    SCHEMA_NAME: ClassVar[str] = "ServiceHealth"
    SCHEMA_VERSION: ClassVar[int] = 1

    service: str
    deployment_id: str
    instance_id: str
    pid: int
    liveness: Liveness
    readiness: Readiness
    started_at_utc: str
    last_heartbeat_at_utc: str
    published_at_utc: str
    application_version: str
    configuration_hash: str
    last_success_at_utc: str | None = None
    source_freshness_seconds: float | None = None
    dependency_status: tuple[DependencyStatusV1, ...] = ()
    blocking_reason: str | None = None

    def __post_init__(self) -> None:
        if not _SERVICE_RE.fullmatch(str(self.service or "")):
            raise HealthContractError(f"invalid service name: {self.service!r}")
        if not str(self.deployment_id or "").strip():
            raise HealthContractError("deployment_id is required")
        if not str(self.instance_id or "").strip():
            raise HealthContractError("instance_id is required")
        if int(self.pid) <= 0:
            raise HealthContractError(f"pid must be positive: {self.pid}")
        if not isinstance(self.liveness, Liveness):
            raise HealthContractError(f"invalid liveness: {self.liveness!r}")
        if not isinstance(self.readiness, Readiness):
            raise HealthContractError(f"invalid readiness: {self.readiness!r}")
        for value in (
            self.started_at_utc,
            self.last_heartbeat_at_utc,
            self.published_at_utc,
        ):
            parse_utc(value)
        if self.last_success_at_utc is not None:
            parse_utc(self.last_success_at_utc)
        if self.source_freshness_seconds is not None:
            freshness = float(self.source_freshness_seconds)
            if freshness < 0.0 or not math.isfinite(freshness):
                raise HealthContractError(
                    "source_freshness_seconds must be finite and non-negative: "
                    f"{self.source_freshness_seconds!r}"
                )
        if not str(self.application_version or "").strip():
            raise HealthContractError("application_version is required")
        if not _HASH_RE.fullmatch(str(self.configuration_hash or "")):
            raise HealthContractError(
                "configuration_hash must be a lowercase SHA-256 hex string"
            )
        dependency_names = [dependency.name for dependency in self.dependency_status]
        if len(dependency_names) != len(set(dependency_names)):
            raise HealthContractError("dependency names must be unique")
        if self.readiness == Readiness.READY and self.blocking_reason:
            raise HealthContractError("READY health cannot have a blocking_reason")

    @classmethod
    def starting(
        cls,
        *,
        service: str,
        deployment_id: str,
        instance_id: str,
        pid: int,
        application_version: str,
        configuration_hash: str,
        now_utc: str | None = None,
    ) -> "ServiceHealthV1":
        now = now_utc or utc_now_text()
        return cls(
            service=service,
            deployment_id=deployment_id,
            instance_id=instance_id,
            pid=pid,
            liveness=Liveness.STARTING,
            readiness=Readiness.NOT_READY,
            started_at_utc=now,
            last_heartbeat_at_utc=now,
            published_at_utc=now,
            application_version=application_version,
            configuration_hash=configuration_hash,
        )

    def heartbeat(
        self,
        *,
        now_utc: str | None = None,
        liveness: Liveness | None = None,
        readiness: Readiness | None = None,
        last_success_at_utc: str | None = None,
        source_freshness_seconds: float | None = None,
        dependency_status: tuple[DependencyStatusV1, ...] | None = None,
        blocking_reason: str | None = None,
    ) -> "ServiceHealthV1":
        now = now_utc or utc_now_text()
        next_readiness = self.readiness if readiness is None else readiness
        next_blocking = blocking_reason
        if next_readiness == Readiness.READY:
            next_blocking = None
        return replace(
            self,
            liveness=self.liveness if liveness is None else liveness,
            readiness=next_readiness,
            last_heartbeat_at_utc=now,
            published_at_utc=now,
            last_success_at_utc=(
                self.last_success_at_utc
                if last_success_at_utc is None
                else last_success_at_utc
            ),
            source_freshness_seconds=(
                self.source_freshness_seconds
                if source_freshness_seconds is None
                else source_freshness_seconds
            ),
            dependency_status=(
                self.dependency_status
                if dependency_status is None
                else tuple(dependency_status)
            ),
            blocking_reason=next_blocking,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "service": self.service,
            "deployment_id": self.deployment_id,
            "instance_id": self.instance_id,
            "pid": self.pid,
            "liveness": self.liveness.value,
            "readiness": self.readiness.value,
            "started_at_utc": self.started_at_utc,
            "last_heartbeat_at_utc": self.last_heartbeat_at_utc,
            "last_success_at_utc": self.last_success_at_utc,
            "published_at_utc": self.published_at_utc,
            "source_freshness_seconds": self.source_freshness_seconds,
            "dependency_status": [
                dependency.to_dict()
                for dependency in sorted(
                    self.dependency_status,
                    key=lambda item: item.name,
                )
            ],
            "blocking_reason": self.blocking_reason,
            "application_version": self.application_version,
            "configuration_hash": self.configuration_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ServiceHealthV1":
        keys = set(value.keys())
        if keys != _HEALTH_KEYS:
            raise HealthContractError(
                "invalid ServiceHealthV1 keys: "
                f"missing={sorted(_HEALTH_KEYS - keys)}, "
                f"unknown={sorted(keys - _HEALTH_KEYS)}"
            )
        if value["schema_name"] != cls.SCHEMA_NAME:
            raise HealthContractError(
                f"unexpected health schema_name: {value['schema_name']!r}"
            )
        if int(value["schema_version"]) != cls.SCHEMA_VERSION:
            raise HealthContractError(
                f"unsupported health schema_version: {value['schema_version']!r}"
            )
        dependencies = value["dependency_status"]
        if not isinstance(dependencies, list):
            raise HealthContractError("dependency_status must be a list")
        last_success = value["last_success_at_utc"]
        freshness = value["source_freshness_seconds"]
        blocking = value["blocking_reason"]
        return cls(
            service=str(value["service"]),
            deployment_id=str(value["deployment_id"]),
            instance_id=str(value["instance_id"]),
            pid=int(value["pid"]),
            liveness=Liveness(str(value["liveness"])),
            readiness=Readiness(str(value["readiness"])),
            started_at_utc=str(value["started_at_utc"]),
            last_heartbeat_at_utc=str(value["last_heartbeat_at_utc"]),
            last_success_at_utc=(
                None if last_success is None else str(last_success)
            ),
            published_at_utc=str(value["published_at_utc"]),
            source_freshness_seconds=(
                None if freshness is None else float(freshness)
            ),
            dependency_status=tuple(
                DependencyStatusV1.from_dict(item)
                for item in dependencies
            ),
            blocking_reason=None if blocking is None else str(blocking),
            application_version=str(value["application_version"]),
            configuration_hash=str(value["configuration_hash"]),
        )
