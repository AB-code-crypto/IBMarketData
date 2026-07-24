from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .atomic_json import canonical_json_text

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SERVICE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ALLOWED_ENVIRONMENTS = {"development", "test", "paper", "live"}


class ConfigurationError(ValueError):
    pass


def _required(mapping: Mapping[str, str], name: str) -> str:
    value = str(mapping.get(name, "") or "").strip()
    if not value:
        raise ConfigurationError(f"required setting is missing: {name}")
    return value


def _identifier(value: str, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ConfigurationError(
            f"{field_name} must match [A-Za-z0-9][A-Za-z0-9._-]{{0,127}}: {value!r}"
        )
    return text


def _positive_int(value: str, *, field_name: str) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{field_name} must be an integer: {value!r}") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{field_name} must be positive: {parsed}")
    return parsed


def _non_negative_int(value: str, *, field_name: str) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{field_name} must be an integer: {value!r}") from exc
    if parsed < 0:
        raise ConfigurationError(f"{field_name} must be non-negative: {parsed}")
    return parsed


@dataclass(frozen=True)
class ServicePaths:
    service_name: str
    data_dir: Path
    health_file: Path
    lock_file: Path


@dataclass(frozen=True)
class DeploymentSettings:
    robot_name: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    environment: str
    application_version: str
    ib_host: str
    ib_port: int
    ib_client_id: int
    ib_account_id: str
    data_root: Path

    @classmethod
    def from_mapping(
        cls,
        mapping: Mapping[str, str],
        *,
        base_dir: str | Path | None = None,
    ) -> "DeploymentSettings":
        environment = str(mapping.get("IBMD_ENVIRONMENT", "paper") or "paper").strip().lower()
        if environment not in _ALLOWED_ENVIRONMENTS:
            raise ConfigurationError(
                f"IBMD_ENVIRONMENT must be one of {sorted(_ALLOWED_ENVIRONMENTS)}: "
                f"{environment!r}"
            )

        raw_data_root = _required(mapping, "IBMD_DATA_ROOT")
        data_root = Path(raw_data_root).expanduser()
        if not data_root.is_absolute():
            base = Path.cwd() if base_dir is None else Path(base_dir)
            data_root = base / data_root
        data_root = data_root.resolve()

        ib_port = _positive_int(_required(mapping, "IB_PORT"), field_name="IB_PORT")
        if ib_port > 65_535:
            raise ConfigurationError(f"IB_PORT must be <= 65535: {ib_port}")

        strategy_version = _positive_int(
            str(mapping.get("IBMD_STRATEGY_VERSION", "1")),
            field_name="IBMD_STRATEGY_VERSION",
        )

        return cls(
            robot_name=_identifier(
                str(mapping.get("IBMD_ROBOT_NAME", "IBMarketData")),
                field_name="IBMD_ROBOT_NAME",
            ),
            strategy_id=_identifier(
                str(mapping.get("IBMD_STRATEGY_ID", "IBMarketData.rolling")),
                field_name="IBMD_STRATEGY_ID",
            ),
            strategy_version=strategy_version,
            deployment_id=_identifier(
                _required(mapping, "IBMD_DEPLOYMENT_ID"),
                field_name="IBMD_DEPLOYMENT_ID",
            ),
            environment=environment,
            application_version=_identifier(
                str(mapping.get("IBMD_APPLICATION_VERSION", "dev")),
                field_name="IBMD_APPLICATION_VERSION",
            ),
            ib_host=_required(mapping, "IB_HOST"),
            ib_port=ib_port,
            ib_client_id=_non_negative_int(
                _required(mapping, "IB_CLIENT_ID"),
                field_name="IB_CLIENT_ID",
            ),
            ib_account_id=_identifier(
                _required(mapping, "IB_ACCOUNT_ID"),
                field_name="IB_ACCOUNT_ID",
            ),
            data_root=data_root,
        )

    def canonical_payload(self) -> dict[str, object]:
        return {
            "application_version": self.application_version,
            "data_root": str(self.data_root),
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "ib_account_id": self.ib_account_id,
            "ib_client_id": self.ib_client_id,
            "ib_host": self.ib_host,
            "ib_port": self.ib_port,
            "robot_name": self.robot_name,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
        }

    @property
    def configuration_hash(self) -> str:
        payload = canonical_json_text(self.canonical_payload()).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def paths_for(self, service_name: str) -> ServicePaths:
        service = str(service_name or "").strip()
        if not _SERVICE_RE.fullmatch(service):
            raise ConfigurationError(
                "service_name must match [a-z][a-z0-9_]{0,63}: "
                f"{service_name!r}"
            )
        return ServicePaths(
            service_name=service,
            data_dir=self.data_root / service,
            health_file=self.data_root / "runtime" / "health" / f"{service}.json",
            lock_file=self.data_root / "runtime" / "locks" / f"{service}.lock",
        )


def load_deployment_settings(
    environ: Mapping[str, str] | None = None,
    *,
    base_dir: str | Path | None = None,
) -> DeploymentSettings:
    source = os.environ if environ is None else environ
    return DeploymentSettings.from_mapping(source, base_dir=base_dir)
