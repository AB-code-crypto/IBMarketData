from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.decision.adapters.sqlite_runtime import (
    DecisionRuntimeReadError,
    DecisionRuntimeSchemaError,
    DecisionRuntimeStateIncomplete,
    SQLiteDecisionRuntimeReader,
)
from ibmd.decision.adapters.sqlite_signal import (
    DecisionSignalReadError,
    DecisionSignalSchemaError,
    SQLiteDecisionSignalReader,
)
from ibmd.decision.adapters.sqlite_store import (
    DecisionSchemaError,
    DecisionStoreError,
    SQLiteDecisionStore,
)
from ibmd.decision.application.runtime import (
    ContinuousDecisionService,
    decision_runtime_payload,
)
from ibmd.decision.application.service import (
    DecisionServiceError,
    DecisionShadowService,
)
from ibmd.decision.domain import DecisionDomainError, DecisionPolicyV1
from ibmd.foundation.atomic_json import canonical_json_text
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
LOGGER = logging.getLogger("ibmd.decision.runtime")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously consume unprocessed target SignalEventV1 values, "
            "build execution facts from public execution products and publish "
            "idempotent decision/command records. This process has no IB access."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--signal-database", type=Path, default=None)
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    return parser


def service_configuration_hash(
    *,
    deployment_hash: str,
    catalog_hash: str,
    signal_database: Path,
    decision_database: Path,
    execution_database: Path,
    poll_interval_seconds: float,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "signal_database": str(signal_database),
        "decision_database": str(decision_database),
        "execution_database": str(execution_database),
        "poll_interval_seconds": float(poll_interval_seconds),
        "broker_access": False,
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _publish_health(
    file: ServiceHealthFile,
    health: ServiceHealthV1,
    *,
    readiness: Readiness,
    reason: str | None,
    signal_status: str,
    execution_status: str,
    decision_status: str,
    last_success_at_utc: str | None = None,
    liveness: Liveness = Liveness.RUNNING,
) -> ServiceHealthV1:
    observed = format_utc(utc_now())
    updated = health.heartbeat(
        now_utc=observed,
        liveness=liveness,
        readiness=readiness,
        last_success_at_utc=last_success_at_utc,
        dependency_status=(
            DependencyStatusV1(
                name="target_signal",
                status=signal_status,
                detail=None if signal_status == "READY" else reason,
                observed_at_utc=observed,
            ),
            DependencyStatusV1(
                name="target_execution",
                status=execution_status,
                detail=None if execution_status == "READY" else reason,
                observed_at_utc=observed,
            ),
            DependencyStatusV1(
                name="decision_store",
                status=decision_status,
                detail=None if decision_status == "READY" else reason,
                observed_at_utc=observed,
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
    interval = float(arguments.poll_interval_seconds)
    if interval <= 0.0:
        raise ValueError("poll_interval_seconds must be positive")

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
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )

    signal_source = SQLiteDecisionSignalReader(signal_database)
    repository = SQLiteDecisionStore(decision_database)
    runtime_source = SQLiteDecisionRuntimeReader(
        signal_database=signal_database,
        decision_database=decision_database,
        execution_database=execution_database,
    )
    decision_service = DecisionShadowService(
        policy=DecisionPolicyV1(
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
        ),
        signal_source=signal_source,
        repository=repository,
    )
    service = ContinuousDecisionService(
        decision_service=decision_service,
        runtime_source=runtime_source,
        signal_configuration_hash=bundle.strategy_policy.content_hash,
    )

    instance_id = new_id("instance")
    health_file = ServiceHealthFile(
        settings.paths_for(SERVICE_NAME).health_file,
        expected_service=SERVICE_NAME,
    )
    health = ServiceHealthV1.starting(
        service=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        pid=os.getpid(),
        application_version=settings.application_version,
        configuration_hash=service_configuration_hash(
            deployment_hash=settings.configuration_hash,
            catalog_hash=bundle.bundle_hash,
            signal_database=signal_database,
            decision_database=decision_database,
            execution_database=execution_database,
            poll_interval_seconds=interval,
        ),
        now_utc=format_utc(utc_now()),
    )

    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        health_file.publish(health)
        try:
            service.validate_dependencies()
        except Exception as exc:
            reason = f"decision runtime dependencies are not ready: {type(exc).__name__}: {exc}"
            _publish_health(
                health_file,
                health,
                readiness=Readiness.BLOCKED,
                reason=reason,
                signal_status="ERROR",
                execution_status="ERROR",
                decision_status="ERROR",
                liveness=Liveness.FAILED,
            )
            print(reason, file=sys.stderr)
            return 2

        if arguments.validate_store_only:
            print(
                json.dumps(
                    {
                        "decision_runtime_dependencies_compatible": True,
                        "signal_database": str(signal_database),
                        "decision_database": str(decision_database),
                        "execution_database": str(execution_database),
                        "broker_access": False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            _publish_health(
                health_file,
                health,
                readiness=Readiness.NOT_READY,
                reason="service stopped",
                signal_status="READY",
                execution_status="READY",
                decision_status="READY",
                liveness=Liveness.STOPPED,
            )
            return 0

        health = _publish_health(
            health_file,
            health,
            readiness=Readiness.READY,
            reason=None,
            signal_status="READY",
            execution_status="READY",
            decision_status="READY",
        )
        while True:
            observed = format_utc(utc_now())
            try:
                result = await asyncio.to_thread(
                    service.run_once,
                    observed_at_utc=observed,
                )
            except DecisionRuntimeStateIncomplete as exc:
                reason = str(exc)
                health = _publish_health(
                    health_file,
                    health,
                    readiness=Readiness.BLOCKED,
                    reason=reason,
                    signal_status="READY",
                    execution_status="BLOCKED",
                    decision_status="READY",
                )
                if arguments.once:
                    print(reason, file=sys.stderr)
                    return 3
                LOGGER.warning("decision runtime blocked: %s", reason)
                await asyncio.sleep(interval)
                continue

            payload = decision_runtime_payload(result)
            last_success = (
                None
                if result.evaluation is None
                else result.evaluation.record.evaluated_at_utc
            )
            health = _publish_health(
                health_file,
                health,
                readiness=Readiness.READY,
                reason=None,
                signal_status="READY",
                execution_status="READY",
                decision_status="READY",
                last_success_at_utc=last_success,
            )
            if arguments.once:
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
                    readiness=Readiness.NOT_READY,
                    reason="service stopped",
                    signal_status="READY",
                    execution_status="READY",
                    decision_status="READY",
                    last_success_at_utc=last_success,
                    liveness=Liveness.STOPPED,
                )
                return 0
            if result.processed:
                LOGGER.info(
                    "decision event processed: %s",
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                )
            await asyncio.sleep(interval)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    arguments = build_parser().parse_args(argv)
    if arguments.validate_store_only and arguments.once:
        print(
            "decision runtime accepts at most one of --validate-store-only or --once",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        DecisionDomainError,
        DecisionRuntimeReadError,
        DecisionRuntimeSchemaError,
        DecisionSchemaError,
        DecisionServiceError,
        DecisionSignalReadError,
        DecisionSignalSchemaError,
        DecisionStoreError,
        ValueError,
    ) as exc:
        print(
            f"decision runtime failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
