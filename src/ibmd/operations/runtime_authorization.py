from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ibmd.catalog import CatalogError, load_catalog_bundle
from ibmd.foundation.config import DeploymentSettings
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.operations.acceptance_manifest import (
    TargetAcceptanceError,
    load_target_acceptance_manifest,
    verify_acceptance_manifest,
)
from ibmd.operations.bootstrap import (
    TargetBootstrapError,
    TargetDeploymentBootstrapper,
)
from ibmd.operations.cutover_preflight import (
    CutoverPreflightError,
    TargetRuntimeAuthorizationV1,
    load_runtime_authorization,
    verify_runtime_authorization,
)


class RuntimeAuthorizationError(RuntimeError):
    pass


@dataclass(frozen=True)
class RuntimeAuthorizationProofV1:
    authorization: TargetRuntimeAuthorizationV1
    verified_at_utc: str
    acceptance_manifest_path: str
    bootstrap_manifest_path: str
    catalog_root: str
    acceptance_gate_count: int
    session_id: str
    session_local_date: str
    session_production_qualified: bool

    def __post_init__(self) -> None:
        if not isinstance(self.authorization, TargetRuntimeAuthorizationV1):
            raise RuntimeAuthorizationError(
                "authorization must be TargetRuntimeAuthorizationV1"
            )
        for field_name in (
            "acceptance_manifest_path",
            "bootstrap_manifest_path",
            "catalog_root",
            "session_id",
            "session_local_date",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise RuntimeAuthorizationError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        count = int(self.acceptance_gate_count)
        if count <= 0:
            raise RuntimeAuthorizationError(
                "acceptance_gate_count must be positive"
            )
        object.__setattr__(self, "acceptance_gate_count", count)
        if not isinstance(self.session_production_qualified, bool):
            raise RuntimeAuthorizationError(
                "session_production_qualified must be boolean"
            )
        object.__setattr__(
            self,
            "verified_at_utc",
            format_utc(parse_utc(self.verified_at_utc)),
        )

    def to_dict(self) -> dict:
        return {
            "schema_name": "RuntimeAuthorizationProof",
            "schema_version": 1,
            "authorization_id": self.authorization.authorization_id,
            "authorization_hash": self.authorization.content_hash,
            "verified_at_utc": self.verified_at_utc,
            "expires_at_utc": self.authorization.expires_at_utc,
            "mode": self.authorization.mode.value,
            "environment": self.authorization.environment,
            "account_id": self.authorization.account_id,
            "deployment_id": self.authorization.deployment_id,
            "application_version": self.authorization.application_version,
            "data_root": self.authorization.data_root,
            "acceptance_manifest_path": self.acceptance_manifest_path,
            "acceptance_manifest_hash": (
                self.authorization.acceptance_manifest_hash
            ),
            "acceptance_gate_count": self.acceptance_gate_count,
            "bootstrap_manifest_path": self.bootstrap_manifest_path,
            "bootstrap_hash": self.authorization.bootstrap_hash,
            "catalog_root": self.catalog_root,
            "catalog_bundle_hash": self.authorization.catalog_bundle_hash,
            "session_id": self.session_id,
            "session_local_date": self.session_local_date,
            "session_production_qualified": (
                self.session_production_qualified
            ),
            "allow_unqualified_session": (
                self.authorization.allow_unqualified_session
            ),
            "continuous_broker_mutations_authorized": True,
            "continuous_broker_mutation_adapters_enabled": False,
            "live_account_enablement": False,
            "automatic_retry_enabled": False,
        }


def verify_runtime_start_authorization(
    *,
    settings: DeploymentSettings,
    source_root: str | Path,
    authorization_path: str | Path,
    acceptance_manifest_path: str | Path,
    bootstrap_manifest_path: str | Path,
    catalog_root: str | Path,
    observed_at_utc: str,
) -> RuntimeAuthorizationProofV1:
    if settings.environment != "paper":
        raise RuntimeAuthorizationError(
            "authorized runtime currently requires IBMD_ENVIRONMENT=paper"
        )
    if not settings.ib_account_id.upper().startswith("D"):
        raise RuntimeAuthorizationError(
            "authorized runtime requires an IB paper account"
        )
    source = Path(source_root).resolve()
    authorization_file = Path(authorization_path).resolve()
    acceptance_file = Path(acceptance_manifest_path).resolve()
    bootstrap_file = Path(bootstrap_manifest_path).resolve()
    target_catalog = Path(catalog_root).resolve()
    observed = format_utc(parse_utc(observed_at_utc))

    try:
        authorization = load_runtime_authorization(authorization_file)
        manifest = load_target_acceptance_manifest(acceptance_file)
        evidence = verify_acceptance_manifest(
            manifest,
            settings=settings,
        )
        bootstrap = TargetDeploymentBootstrapper(
            source_root=source,
            bootstrap_manifest_path=bootstrap_file,
            target_root=settings.data_root,
            application_version=settings.application_version,
            require_production_sessions=(
                not authorization.allow_unqualified_session
            ),
        ).validate_target()
        bundle = load_catalog_bundle(
            target_catalog,
            require_production_sessions=False,
        )
        verify_runtime_authorization(
            authorization,
            settings=settings,
            acceptance_manifest=manifest,
            bootstrap_hash=bootstrap.bootstrap_hash,
            catalog_bundle_hash=bootstrap.catalog_bundle_hash,
            observed_at_utc=observed,
        )
    except (
        CatalogError,
        CutoverPreflightError,
        OSError,
        TargetAcceptanceError,
        TargetBootstrapError,
        ValueError,
    ) as exc:
        raise RuntimeAuthorizationError(
            "runtime authorization verification failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if bundle.bundle_hash != bootstrap.catalog_bundle_hash:
        raise RuntimeAuthorizationError(
            "runtime catalog hash differs from bootstrap authorization evidence"
        )
    instrument_policy = bundle.strategy_policy.require("MNQ")
    session = bundle.session_calendar.require(
        instrument_policy.daily_flat.session_id
    )
    local_date = parse_utc(observed).astimezone(session.zone).date()
    production_qualified = session.is_production_qualified_for(local_date)
    if (
        not production_qualified
        and not authorization.allow_unqualified_session
    ):
        raise RuntimeAuthorizationError(
            "runtime session calendar is not production-qualified for the "
            f"current local date: session={session.session_id}, "
            f"date={local_date.isoformat()}"
        )
    return RuntimeAuthorizationProofV1(
        authorization=authorization,
        verified_at_utc=observed,
        acceptance_manifest_path=str(acceptance_file),
        bootstrap_manifest_path=str(bootstrap_file),
        catalog_root=str(target_catalog),
        acceptance_gate_count=len(evidence),
        session_id=session.session_id,
        session_local_date=local_date.isoformat(),
        session_production_qualified=production_qualified,
    )


__all__ = [
    "RuntimeAuthorizationError",
    "RuntimeAuthorizationProofV1",
    "verify_runtime_start_authorization",
]
