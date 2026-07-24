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
from ibmd.execution import (
    ExecutionDomainError,
    ExecutionFoundationConfig,
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    ExecutionFoundationService,
)
from ibmd.execution.adapters import (
    ExecutionDecisionSourceError,
    ExecutionSchemaError,
    ExecutionStoreError,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionStore,
)
from ibmd.foundation.atomic_json import (
    JsonDataError,
    canonical_json_text,
    read_json_object,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.operations.health import ServiceHealthFile

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target execution foundation without broker order access. "
            "It publishes local execution read models and admits at most one "
            "existing StrategyCommandRequestV1 into its own ledger."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate the explicit execution schema and exit",
    )
    parser.add_argument(
        "--publish-fixture-only",
        action="store_true",
        help="publish one strict foundation fixture without reading a command",
    )
    parser.add_argument(
        "--once-command-id",
        default=None,
        help="ingest one explicit decision command id, then exit",
    )
    parser.add_argument(
        "--foundation-fixture",
        type=Path,
        default=None,
        help="strict UTF-8 JSON fixture for position/readiness/daily-risk facts",
    )
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
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
        f"--manifest {ROOT / 'migrations' / 'execution.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def service_configuration_hash(
    *,
    deployment_hash: str,
    catalog_hash: str,
    decision_database: Path,
    execution_database: Path,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "decision_database": str(decision_database),
        "execution_database": str(execution_database),
        "broker_order_gateway": "disabled",
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)

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

    repository = SQLiteExecutionStore(execution_database)
    command_source = SQLiteExecutionDecisionReader(decision_database)
    instance_id = new_id("instance")
    policy = ExecutionFoundationPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument.instrument_id,
        policy_hash=bundle.strategy_policy.content_hash,
    )
    health_file = ServiceHealthFile(
        settings.paths_for(SERVICE_NAME).health_file,
        expected_service=SERVICE_NAME,
    )
    service = ExecutionFoundationService(
        config=ExecutionFoundationConfig(
            deployment_id=settings.deployment_id,
            instance_id=instance_id,
            application_version=settings.application_version,
            configuration_hash=service_configuration_hash(
                deployment_hash=settings.configuration_hash,
                catalog_hash=bundle.bundle_hash,
                decision_database=decision_database,
                execution_database=execution_database,
            ),
            policy=policy,
        ),
        repository=repository,
        health_publisher=health_file,
    )

    paths = settings.paths_for(SERVICE_NAME)
    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        service.publish_starting()
        try:
            try:
                repository.validate_schema()
            except ExecutionSchemaError as exc:
                reason = f"target execution store is not ready: {exc}"
                service.publish_failed(reason)
                print(reason, file=sys.stderr)
                print(
                    "Apply the explicit offline migration before startup:",
                    file=sys.stderr,
                )
                print(
                    migration_command(
                        database_path=execution_database,
                        application_version=settings.application_version,
                    ),
                    file=sys.stderr,
                )
                return 2

            if arguments.validate_store_only:
                print(
                    "execution foundation store is compatible: "
                    f"execution={execution_database}"
                )
                service.publish_stopped()
                return 0

            if arguments.foundation_fixture is None:
                raise ValueError(
                    "--foundation-fixture is required outside --validate-store-only"
                )
            fixture = ExecutionFoundationFixtureV1.from_dict(
                read_json_object(arguments.foundation_fixture.resolve())
            )

            if arguments.publish_fixture_only:
                await asyncio.to_thread(service.publish_fixture, fixture)
                payload = {
                    "readiness": fixture.readiness.to_dict(),
                    "position": fixture.position.to_dict(),
                    "daily_risk": fixture.daily_risk.to_dict(),
                    "broker_order_gateway": "disabled",
                }
                print(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                service.publish_stopped()
                return 0

            if arguments.once_command_id is None:
                raise ValueError(
                    "--once-command-id or --publish-fixture-only is required"
                )
            command_source.validate_schema()
            command = await asyncio.to_thread(
                command_source.read_command,
                str(arguments.once_command_id),
            )
            if command is None:
                raise ExecutionDecisionSourceError(
                    f"strategy command not found: {arguments.once_command_id}"
                )
            state = await asyncio.to_thread(
                service.ingest_command,
                command=command,
                fixture=fixture,
            )
            print(
                json.dumps(
                    {
                        "command_state": state.to_dict(),
                        "broker_order_gateway": "disabled",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            service.publish_stopped()
            return 0
        except Exception as exc:
            try:
                service.publish_failed(f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            raise


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = sum(
        (
            bool(arguments.validate_store_only),
            bool(arguments.publish_fixture_only),
            arguments.once_command_id is not None,
        )
    )
    if selected != 1:
        print(
            "execution foundation requires exactly one of "
            "--validate-store-only, --publish-fixture-only or --once-command-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        ExecutionDecisionSourceError,
        ExecutionDomainError,
        ExecutionStoreError,
        JsonDataError,
        ValueError,
    ) as exc:
        print(
            f"execution foundation failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
