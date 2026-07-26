from __future__ import annotations

import ctypes
import json
import os
import socket
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from ibmd.catalog import CatalogError, load_catalog_bundle
from ibmd.foundation.atomic_json import canonical_json_text, read_json_object
from ibmd.foundation.config import DeploymentSettings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import (
    ProcessLockError,
    ServiceAlreadyRunningError,
    ServiceProcessLock,
)
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.operations.acceptance_manifest import (
    REQUIRED_PAPER_SOAK_GATES,
    TargetAcceptanceError,
    TargetAcceptanceManifestV1,
    load_target_acceptance_manifest,
    verify_acceptance_manifest,
)
from ibmd.operations.bootstrap import (
    TargetBootstrapError,
    TargetDeploymentBootstrapper,
)


class CutoverPreflightError(RuntimeError):
    pass


class CutoverMode(str, Enum):
    PAPER_SOAK = "PAPER_SOAK"
    LIVE_CUTOVER = "LIVE_CUTOVER"


@dataclass(frozen=True)
class PreflightCheckV1:
    check_name: str
    passed: bool
    blocking: bool
    detail: str
    evidence: Mapping[str, Any]

    def __post_init__(self) -> None:
        name = str(self.check_name or "").strip()
        detail = str(self.detail or "").strip()
        if not name or not detail:
            raise CutoverPreflightError(
                "preflight check name/detail are required"
            )
        if not isinstance(self.passed, bool) or not isinstance(self.blocking, bool):
            raise CutoverPreflightError(
                "preflight check passed/blocking values must be boolean"
            )
        if not isinstance(self.evidence, Mapping):
            raise CutoverPreflightError("preflight evidence must be a mapping")
        object.__setattr__(self, "check_name", name)
        object.__setattr__(self, "detail", detail)
        object.__setattr__(self, "evidence", dict(self.evidence))

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "blocking": self.blocking,
            "detail": self.detail,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class TargetRuntimeAuthorizationV1:
    authorization_id: str
    mode: CutoverMode
    environment: str
    account_id: str
    deployment_id: str
    application_version: str
    data_root: str
    acceptance_manifest_hash: str
    bootstrap_hash: str
    catalog_bundle_hash: str
    issued_at_utc: str
    expires_at_utc: str
    allow_unqualified_session: bool

    SCHEMA_NAME = "TargetRuntimeAuthorization"
    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        if not isinstance(self.mode, CutoverMode):
            raise CutoverPreflightError("authorization mode must be CutoverMode")
        if self.mode != CutoverMode.PAPER_SOAK:
            raise CutoverPreflightError(
                "runtime authorization currently supports PAPER_SOAK only"
            )
        for field_name in (
            "authorization_id",
            "environment",
            "account_id",
            "deployment_id",
            "application_version",
            "data_root",
            "acceptance_manifest_hash",
            "bootstrap_hash",
            "catalog_bundle_hash",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise CutoverPreflightError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        if self.environment != "paper" or not self.account_id.upper().startswith("D"):
            raise CutoverPreflightError(
                "PAPER_SOAK authorization requires a paper environment/account"
            )
        if not Path(self.data_root).is_absolute():
            raise CutoverPreflightError(
                "authorization data_root must be absolute"
            )
        for field_name in (
            "acceptance_manifest_hash",
            "bootstrap_hash",
            "catalog_bundle_hash",
        ):
            value = str(getattr(self, field_name))
            if len(value) != 64 or any(
                item not in "0123456789abcdef" for item in value
            ):
                raise CutoverPreflightError(
                    f"{field_name} must be lowercase SHA-256 hex"
                )
        issued = format_utc(parse_utc(self.issued_at_utc))
        expires = format_utc(parse_utc(self.expires_at_utc))
        if parse_utc(expires) <= parse_utc(issued):
            raise CutoverPreflightError(
                "authorization expires_at_utc must follow issued_at_utc"
            )
        if not isinstance(self.allow_unqualified_session, bool):
            raise CutoverPreflightError(
                "allow_unqualified_session must be boolean"
            )
        object.__setattr__(self, "issued_at_utc", issued)
        object.__setattr__(self, "expires_at_utc", expires)

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "authorization_id": self.authorization_id,
            "mode": self.mode.value,
            "environment": self.environment,
            "account_id": self.account_id,
            "deployment_id": self.deployment_id,
            "application_version": self.application_version,
            "data_root": self.data_root,
            "acceptance_manifest_hash": self.acceptance_manifest_hash,
            "bootstrap_hash": self.bootstrap_hash,
            "catalog_bundle_hash": self.catalog_bundle_hash,
            "issued_at_utc": self.issued_at_utc,
            "expires_at_utc": self.expires_at_utc,
            "allow_unqualified_session": self.allow_unqualified_session,
            "continuous_broker_mutations_authorized": True,
            "live_account_enablement": False,
            "automatic_retry_enabled": False,
        }

    @property
    def content_hash(self) -> str:
        import hashlib

        return hashlib.sha256(
            canonical_json_text(self.unsigned_payload()).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.unsigned_payload(),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TargetRuntimeAuthorizationV1":
        expected = {
            "schema_name",
            "schema_version",
            "authorization_id",
            "mode",
            "environment",
            "account_id",
            "deployment_id",
            "application_version",
            "data_root",
            "acceptance_manifest_hash",
            "bootstrap_hash",
            "catalog_bundle_hash",
            "issued_at_utc",
            "expires_at_utc",
            "allow_unqualified_session",
            "continuous_broker_mutations_authorized",
            "live_account_enablement",
            "automatic_retry_enabled",
            "content_hash",
        }
        if set(value) != expected:
            raise CutoverPreflightError(
                "runtime authorization fields mismatch: "
                f"missing={sorted(expected - set(value))}, "
                f"unknown={sorted(set(value) - expected)}"
            )
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or int(value["schema_version"]) != cls.SCHEMA_VERSION
        ):
            raise CutoverPreflightError(
                "unsupported runtime authorization schema"
            )
        if value["continuous_broker_mutations_authorized"] is not True:
            raise CutoverPreflightError(
                "runtime authorization must explicitly authorize mutations"
            )
        if value["live_account_enablement"] is not False:
            raise CutoverPreflightError(
                "runtime authorization cannot enable a live account"
            )
        if value["automatic_retry_enabled"] is not False:
            raise CutoverPreflightError(
                "runtime authorization cannot enable automatic retry"
            )
        try:
            mode = CutoverMode(str(value["mode"]))
        except ValueError as exc:
            raise CutoverPreflightError(
                f"unknown runtime authorization mode: {value['mode']!r}"
            ) from exc
        authorization = cls(
            authorization_id=str(value["authorization_id"]),
            mode=mode,
            environment=str(value["environment"]),
            account_id=str(value["account_id"]),
            deployment_id=str(value["deployment_id"]),
            application_version=str(value["application_version"]),
            data_root=str(value["data_root"]),
            acceptance_manifest_hash=str(value["acceptance_manifest_hash"]),
            bootstrap_hash=str(value["bootstrap_hash"]),
            catalog_bundle_hash=str(value["catalog_bundle_hash"]),
            issued_at_utc=str(value["issued_at_utc"]),
            expires_at_utc=str(value["expires_at_utc"]),
            allow_unqualified_session=value["allow_unqualified_session"],
        )
        if str(value["content_hash"]) != authorization.content_hash:
            raise CutoverPreflightError(
                "runtime authorization content_hash does not match"
            )
        return authorization


@dataclass(frozen=True)
class CutoverPreflightResultV1:
    mode: CutoverMode
    checked_at_utc: str
    checks: tuple[PreflightCheckV1, ...]
    acceptance_manifest: TargetAcceptanceManifestV1 | None
    bootstrap_hash: str | None
    catalog_bundle_hash: str | None
    authorization: TargetRuntimeAuthorizationV1 | None

    @property
    def blocking_failures(self) -> tuple[PreflightCheckV1, ...]:
        return tuple(
            item for item in self.checks if item.blocking and not item.passed
        )

    @property
    def ready(self) -> bool:
        return not self.blocking_failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": "TargetCutoverPreflightResult",
            "schema_version": 1,
            "mode": self.mode.value,
            "checked_at_utc": self.checked_at_utc,
            "ready": self.ready,
            "checks": [item.to_dict() for item in self.checks],
            "blocking_failures": [
                item.check_name for item in self.blocking_failures
            ],
            "acceptance_manifest_hash": (
                None
                if self.acceptance_manifest is None
                else self.acceptance_manifest.content_hash
            ),
            "bootstrap_hash": self.bootstrap_hash,
            "catalog_bundle_hash": self.catalog_bundle_hash,
            "authorization": (
                None if self.authorization is None else self.authorization.to_dict()
            ),
            "live_account_enablement": False,
            "automatic_retry_enabled": False,
        }


def _check(
    name: str,
    passed: bool,
    detail: str,
    *,
    blocking: bool = True,
    evidence: Mapping[str, Any] | None = None,
) -> PreflightCheckV1:
    return PreflightCheckV1(
        check_name=name,
        passed=bool(passed),
        blocking=bool(blocking),
        detail=detail,
        evidence={} if evidence is None else evidence,
    )


def _pid_alive(pid: int) -> bool:
    pid = int(pid)
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    process_query_limited_information = 0x1000
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(
        process_query_limited_information,
        0,
        pid,
    )
    if not handle:
        return False
    try:
        exit_code = ctypes.c_uint32()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return int(exit_code.value) == still_active
    finally:
        kernel32.CloseHandle(handle)


def _active_legacy_services(runtime_dir: Path) -> tuple[dict[str, Any], ...]:
    if not runtime_dir.is_dir():
        return ()
    values = []
    for path in sorted(runtime_dir.glob("*.json")):
        try:
            raw = read_json_object(path)
        except Exception:
            continue
        state = str(raw.get("state") or "").strip().lower()
        pid = int(raw.get("pid") or 0)
        if state in {"starting", "running"} and _pid_alive(pid):
            values.append(
                {
                    "status_file": str(path),
                    "service_key": str(raw.get("service_key") or path.stem),
                    "state": state,
                    "pid": pid,
                    "script": str(raw.get("script") or ""),
                }
            )
    return tuple(values)


def _target_lock_check(
    settings: DeploymentSettings,
) -> tuple[bool, tuple[dict[str, Any], ...]]:
    service_names = (
        "supervisor",
        "market_data",
        "broker_position_feed",
        "signal",
        "decision",
        "execution",
    )
    locks: list[ServiceProcessLock] = []
    conflicts = []
    try:
        for service in service_names:
            lock = ServiceProcessLock(
                settings.paths_for(service).lock_file,
                service_name=f"preflight_{service}",
                deployment_id=settings.deployment_id,
                instance_id=new_id("instance"),
            )
            try:
                lock.acquire()
            except ServiceAlreadyRunningError as exc:
                metadata = exc.metadata
                conflicts.append(
                    {
                        "service": service,
                        "lock_file": str(exc.path),
                        "pid": None if metadata is None else metadata.pid,
                        "instance_id": (
                            None if metadata is None else metadata.instance_id
                        ),
                    }
                )
                break
            locks.append(lock)
    finally:
        for lock in reversed(locks):
            lock.release()
    return not conflicts, tuple(conflicts)


class TargetCutoverPreflight:
    def __init__(
        self,
        *,
        settings: DeploymentSettings,
        source_root: str | Path,
        bootstrap_manifest_path: str | Path,
        acceptance_manifest_path: str | Path,
        legacy_runtime_dir: str | Path,
        mode: CutoverMode,
        allow_unqualified_session: bool = False,
        authorization_valid_hours: float = 24.0,
    ) -> None:
        self.settings = settings
        self.source_root = Path(source_root).resolve()
        self.bootstrap_manifest_path = Path(bootstrap_manifest_path).resolve()
        self.acceptance_manifest_path = Path(acceptance_manifest_path).resolve()
        self.legacy_runtime_dir = Path(legacy_runtime_dir).resolve()
        self.mode = mode
        self.allow_unqualified_session = bool(allow_unqualified_session)
        self.authorization_valid_hours = float(authorization_valid_hours)
        if self.authorization_valid_hours <= 0.0:
            raise CutoverPreflightError(
                "authorization_valid_hours must be positive"
            )
        if not isinstance(self.mode, CutoverMode):
            raise CutoverPreflightError("mode must be CutoverMode")

    def run(
        self,
        *,
        observed_at_utc: str,
        issue_authorization: bool,
    ) -> CutoverPreflightResultV1:
        observed = format_utc(parse_utc(observed_at_utc))
        checks: list[PreflightCheckV1] = []
        manifest = None
        bootstrap_hash = None
        catalog_hash = None

        if self.mode == CutoverMode.LIVE_CUTOVER:
            checks.append(
                _check(
                    "live_account_enablement",
                    False,
                    "live-account enablement is intentionally absent",
                    evidence={"environment": self.settings.environment},
                )
            )
        else:
            paper_scope = (
                self.settings.environment == "paper"
                and self.settings.ib_account_id.upper().startswith("D")
            )
            checks.append(
                _check(
                    "paper_scope",
                    paper_scope,
                    (
                        "paper environment/account are exact"
                        if paper_scope
                        else "PAPER_SOAK requires environment=paper and a D* account"
                    ),
                    evidence={
                        "environment": self.settings.environment,
                        "account_id": self.settings.ib_account_id,
                    },
                )
            )

        try:
            bootstrap = TargetDeploymentBootstrapper(
                source_root=self.source_root,
                bootstrap_manifest_path=self.bootstrap_manifest_path,
                target_root=self.settings.data_root,
                application_version=self.settings.application_version,
                require_production_sessions=(
                    self.mode == CutoverMode.LIVE_CUTOVER
                    or not self.allow_unqualified_session
                ),
            ).validate_target()
            bootstrap_hash = bootstrap.bootstrap_hash
            catalog_hash = bootstrap.catalog_bundle_hash
            checks.append(
                _check(
                    "bootstrap_integrity",
                    True,
                    "target bootstrap, schema ledgers and artifact hashes are valid",
                    evidence={
                        "bootstrap_hash": bootstrap_hash,
                        "catalog_bundle_hash": catalog_hash,
                        "application_version": bootstrap.application_version,
                        "store_count": len(bootstrap.stores),
                    },
                )
            )
        except (CatalogError, OSError, TargetBootstrapError, ValueError) as exc:
            checks.append(
                _check(
                    "bootstrap_integrity",
                    False,
                    f"target bootstrap validation failed: {type(exc).__name__}: {exc}",
                )
            )

        try:
            manifest = load_target_acceptance_manifest(
                self.acceptance_manifest_path
            )
            verified = verify_acceptance_manifest(
                manifest,
                settings=self.settings,
                required_gates=REQUIRED_PAPER_SOAK_GATES,
            )
            checks.append(
                _check(
                    "acceptance_evidence",
                    True,
                    "all required paper broker-safety summaries are immutable and verified",
                    evidence={
                        "manifest_hash": manifest.content_hash,
                        "gates": [item.gate.value for item in verified],
                        "gate_count": len(verified),
                    },
                )
            )
        except (OSError, TargetAcceptanceError, ValueError) as exc:
            checks.append(
                _check(
                    "acceptance_evidence",
                    False,
                    f"acceptance manifest validation failed: {type(exc).__name__}: {exc}",
                )
            )

        try:
            bundle = load_catalog_bundle(
                self.settings.data_root / "catalog",
                require_production_sessions=False,
            )
            instrument_policy = bundle.strategy_policy.require("MNQ")
            session = bundle.session_calendar.require(
                instrument_policy.daily_flat.session_id
            )
            local_date = parse_utc(observed).astimezone(session.zone).date()
            qualified = session.is_production_qualified_for(local_date)
            permitted = qualified or (
                self.mode == CutoverMode.PAPER_SOAK
                and self.allow_unqualified_session
            )
            checks.append(
                _check(
                    "session_calendar",
                    permitted,
                    (
                        "session calendar is production-qualified for the current local date"
                        if qualified
                        else (
                            "paper-only unqualified-session override is explicit"
                            if permitted
                            else "session calendar is not production-qualified for the current local date"
                        )
                    ),
                    evidence={
                        "session_id": session.session_id,
                        "local_date": local_date.isoformat(),
                        "production_qualified": qualified,
                        "paper_override": self.allow_unqualified_session,
                        "calendar_version": bundle.session_calendar.calendar_version,
                    },
                )
            )
            if catalog_hash is not None and bundle.content_hash != catalog_hash:
                checks.append(
                    _check(
                        "catalog_runtime_hash",
                        False,
                        "loaded target catalog hash differs from bootstrap evidence",
                        evidence={
                            "loaded": bundle.content_hash,
                            "bootstrap": catalog_hash,
                        },
                    )
                )
            else:
                checks.append(
                    _check(
                        "catalog_runtime_hash",
                        True,
                        "loaded target catalog matches bootstrap evidence",
                        evidence={"catalog_bundle_hash": bundle.content_hash},
                    )
                )
        except (CatalogError, OSError, ValueError) as exc:
            checks.append(
                _check(
                    "session_calendar",
                    False,
                    f"target catalog validation failed: {type(exc).__name__}: {exc}",
                )
            )

        legacy = _active_legacy_services(self.legacy_runtime_dir)
        checks.append(
            _check(
                "legacy_runtime_stopped",
                not legacy,
                (
                    "no active legacy runtime service was detected"
                    if not legacy
                    else "active legacy runtime services still own the account"
                ),
                evidence={"active_services": list(legacy)},
            )
        )

        try:
            locks_free, conflicts = _target_lock_check(self.settings)
        except (OSError, ProcessLockError, ValueError) as exc:
            locks_free = False
            conflicts = (
                {
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        checks.append(
            _check(
                "target_service_locks_free",
                locks_free,
                (
                    "all target service locks are available"
                    if locks_free
                    else "one or more target services are already running"
                ),
                evidence={"conflicts": list(conflicts)},
            )
        )

        authorization = None
        blockers = tuple(
            item for item in checks if item.blocking and not item.passed
        )
        if issue_authorization and not blockers:
            if manifest is None or bootstrap_hash is None or catalog_hash is None:
                raise CutoverPreflightError(
                    "preflight passed without required authorization evidence"
                )
            expires = parse_utc(observed) + timedelta(
                hours=self.authorization_valid_hours
            )
            authorization = TargetRuntimeAuthorizationV1(
                authorization_id=new_id("runtime_authorization"),
                mode=self.mode,
                environment=self.settings.environment,
                account_id=self.settings.ib_account_id,
                deployment_id=self.settings.deployment_id,
                application_version=self.settings.application_version,
                data_root=str(self.settings.data_root),
                acceptance_manifest_hash=manifest.content_hash,
                bootstrap_hash=bootstrap_hash,
                catalog_bundle_hash=catalog_hash,
                issued_at_utc=observed,
                expires_at_utc=format_utc(expires),
                allow_unqualified_session=self.allow_unqualified_session,
            )
        return CutoverPreflightResultV1(
            mode=self.mode,
            checked_at_utc=observed,
            checks=tuple(checks),
            acceptance_manifest=manifest,
            bootstrap_hash=bootstrap_hash,
            catalog_bundle_hash=catalog_hash,
            authorization=authorization,
        )


def load_runtime_authorization(
    path: str | Path,
) -> TargetRuntimeAuthorizationV1:
    source = Path(path)
    try:
        value = read_json_object(source)
    except Exception as exc:
        raise CutoverPreflightError(
            f"cannot read runtime authorization {source}: {exc}"
        ) from exc
    return TargetRuntimeAuthorizationV1.from_dict(value)


def verify_runtime_authorization(
    authorization: TargetRuntimeAuthorizationV1,
    *,
    settings: DeploymentSettings,
    acceptance_manifest: TargetAcceptanceManifestV1,
    bootstrap_hash: str,
    catalog_bundle_hash: str,
    observed_at_utc: str,
) -> None:
    expected = (
        CutoverMode.PAPER_SOAK,
        settings.environment,
        settings.ib_account_id,
        settings.deployment_id,
        settings.application_version,
        str(settings.data_root.resolve()),
        acceptance_manifest.content_hash,
        bootstrap_hash,
        catalog_bundle_hash,
    )
    actual = (
        authorization.mode,
        authorization.environment,
        authorization.account_id,
        authorization.deployment_id,
        authorization.application_version,
        str(Path(authorization.data_root).resolve()),
        authorization.acceptance_manifest_hash,
        authorization.bootstrap_hash,
        authorization.catalog_bundle_hash,
    )
    if actual != expected:
        raise CutoverPreflightError(
            "runtime authorization scope/evidence differs from current deployment"
        )
    observed = parse_utc(observed_at_utc)
    if observed < parse_utc(authorization.issued_at_utc):
        raise CutoverPreflightError(
            "runtime authorization cannot be used before issued_at_utc"
        )
    if observed >= parse_utc(authorization.expires_at_utc):
        raise CutoverPreflightError("runtime authorization has expired")


__all__ = [
    "CutoverMode",
    "CutoverPreflightError",
    "CutoverPreflightResultV1",
    "PreflightCheckV1",
    "TargetCutoverPreflight",
    "TargetRuntimeAuthorizationV1",
    "load_runtime_authorization",
    "verify_runtime_authorization",
]
