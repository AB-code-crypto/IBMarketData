from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.decision import (
    DecisionDomainError,
    DecisionPolicyV1,
    DecisionServiceError,
    DecisionShadowService,
    ExecutionDecisionFixtureV1,
)
from ibmd.decision.adapters import (
    DecisionSchemaError,
    DecisionSignalReadError,
    DecisionSignalSchemaError,
    DecisionStoreError,
    SQLiteDecisionSignalReader,
    SQLiteDecisionStore,
)
from ibmd.foundation.atomic_json import (
    JsonDataError,
    canonical_json_text,
    read_json_object,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now
from ibmd.operations.health import ServiceHealthFile
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)

SERVICE_NAME = "decision"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target decision component in shadow mode. It reads one "
            "target SignalEventV1 plus an explicit execution/position fixture, "
            "writes only the decision store and never connects to IB."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate target signal and decision stores, then exit",
    )
    parser.add_argument(
        "--once-event-id",
        default=None,
        help="evaluate one explicit target signal event id, then exit",
    )
    parser.add_argument(
        "--execution-fixture",
        type=Path,
        default=None,
        help="UTF-8 JSON fixture containing target execution/position facts",
    )
    parser.add_argument("--signal-database", type=Path, default=None)
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {ROOT / 'migrations' / 'decision.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def service_configuration_hash(
    *,
    deployment_hash: str,
    catalog_hash: str,
    signal_database: Path,
    decision_database: Path,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "signal_database": str(signal_database),
        "decision_database": str(decision_database),
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _publish_health(
    file: ServiceHealthFile,
    health: ServiceHealthV1,
    *,
    liveness: Liveness,
    readiness: Readiness,
    reason: str | None,
    signal_status: str,
    decision_status: str,
    last_success_at_utc: str | None = None,
) -> ServiceHealthV1:
    now = format_utc(utc_now())
    updated = health.heartbeat(
        now_utc=now,
        liveness=liveness,
        readiness=readiness,
        last_success_at_utc=last_success_at_utc,
        dependency_status=(
            DependencyStatusV1(
                name="target_signal",
                status=signal_status,
                detail=reason if signal_status != "READY" else None,
                observed_at_utc=now,
            ),
            DependencyStatusV1(
                name="decision_store",
                status=decision_status,
                detail=reason if decision_status != "READY" else None,
                observed_at_utc=now,
            ),
        ),
        blocking_reason=reason,
    )
    file.publish(updated)
    return updated


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    instrument_policy = bundle.strategy_policy.require(instrument_id)

    signal_database = (
        arguments.signal_database.resolve()
        if arguments.signal_database is not None
        else settings.data_root / "signal" / "signal.sqlite3"
    )
    decision_database = (
        arguments.decision_database.resolve()
        if arguments.decision_database is not None
        else settings.data_root / "decision" / "decision.sqlite3"
    )

    signal_source = SQLiteDecisionSignalReader(signal_database)
    repository = SQLiteDecisionStore(decision_database)
    policy = DecisionPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument.instrument_id,
        target_quantity=instrument_policy.target_quantity,
        max_signal_age_seconds=(
            instrument_policy.signal.decision_pipeline_max_age_seconds
        ),
        policy_hash=bundle.strategy_policy.content_hash,
    )
    service = DecisionShadowService(
        policy=policy,
        signal_source=signal_source,
        repository=repository,
    )

    instance_id = new_id("instance")
    paths = settings.paths_for(SERVICE_NAME)
    health_file = ServiceHealthFile(
        paths.health_file,
        expected_service=SERVICE_NAME,
    )
    health = ServiceHealthV1.starting(
        service=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        pid=__import__("os").getpid(),
        application_version=settings.application_version,
        configuration_hash=service_configuration_hash(
            deployment_hash=settings.configuration_hash,
            catalog_hash=bundle.bundle_hash,
            signal_database=signal_database,
            decision_database=decision_database,
        ),
        now_utc=format_utc(utc_now()),
    )

    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        health_file.publish(health)
        try:
            try:
                service.validate_dependencies()
            except DecisionSchemaError as exc:
                reason = f"target decision store is not ready: {exc}"
                _publish_health(
                    health_file,
                    health,
                    liveness=Liveness.RUNNING,
                    readiness=Readiness.BLOCKED,
                    reason=reason,
                    signal_status="UNKNOWN",
                    decision_status="ERROR",
                )
                print(reason, file=sys.stderr)
                print(
                    "Apply the explicit offline migration before startup:",
                    file=sys.stderr,
                )
                print(
                    migration_command(
                        database_path=decision_database,
                        application_version=settings.application_version,
                    ),
                    file=sys.stderr,
                )
                return 2
            except DecisionSignalSchemaError as exc:
                reason = f"target signal store is not ready: {exc}"
                _publish_health(
                    health_file,
                    health,
                    liveness=Liveness.RUNNING,
                    readiness=Readiness.BLOCKED,
                    reason=reason,
                    signal_status="ERROR",
                    decision_status="READY",
                )
                print(reason, file=sys.stderr)
                return 3

            health = _publish_health(
                health_file,
                health,
                liveness=Liveness.RUNNING,
                readiness=Readiness.READY,
                reason=None,
                signal_status="READY",
                decision_status="READY",
            )

            if arguments.validate_store_only:
                print(
                    "decision dependencies are compatible: "
                    f"signal={signal_database}, decision={decision_database}"
                )
                _publish_health(
                    health_file,
                    health,
                    liveness=Liveness.STOPPED,
                    readiness=Readiness.NOT_READY,
                    reason="service stopped",
                    signal_status="READY",
                    decision_status="READY",
                )
                return 0

            if arguments.once_event_id is None:
                raise ValueError(
                    "--once-event-id is required outside --validate-store-only"
                )
            if arguments.execution_fixture is None:
                raise ValueError(
                    "--execution-fixture is required with --once-event-id"
                )

            fixture = ExecutionDecisionFixtureV1.from_dict(
                read_json_object(arguments.execution_fixture.resolve())
            )
            evaluation = await asyncio.to_thread(
                service.evaluate_event,
                event_id=str(arguments.once_event_id),
                fixture=fixture,
            )
            payload = {
                "record": evaluation.record.to_dict(),
                "command": (
                    None
                    if evaluation.command is None
                    else evaluation.command.to_dict()
                ),
            }
            print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            _publish_health(
                health_file,
                health,
                liveness=Liveness.STOPPED,
                readiness=Readiness.NOT_READY,
                reason="service stopped",
                signal_status="READY",
                decision_status="READY",
                last_success_at_utc=evaluation.record.evaluated_at_utc,
            )
            return 0
        except Exception as exc:
            try:
                _publish_health(
                    health_file,
                    health,
                    liveness=Liveness.FAILED,
                    readiness=Readiness.BLOCKED,
                    reason=f"{type(exc).__name__}: {exc}",
                    signal_status="ERROR",
                    decision_status="ERROR",
                )
            except Exception:
                pass
            raise


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(arguments))
    except (
        DecisionDomainError,
        DecisionServiceError,
        DecisionSignalReadError,
        DecisionStoreError,
        JsonDataError,
        ValueError,
    ) as exc:
        print(f"decision shadow failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
