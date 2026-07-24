from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import (
    ActiveContractStatus,
    load_catalog_bundle,
    resolve_active_contract,
)
from ibmd.execution import (
    ExecutionDomainError,
    ExecutionFoundationConfig,
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    ExecutionFoundationService,
    PositionProjectionError,
    PositionProjectionPolicyV1,
    RegisteredFuturesContractV1,
    merge_position_projection_readiness,
    project_strategy_position,
)
from ibmd.execution.adapters import (
    ExecutionDecisionSourceError,
    ExecutionPositionFeedError,
    ExecutionSchemaError,
    ExecutionStateReadError,
    ExecutionStoreError,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
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
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.operations.health import ServiceHealthFile
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target execution foundation without broker order access. "
            "It can project one strategy position from the public position-feed "
            "snapshot or admit one existing StrategyCommandRequestV1 locally."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate the explicit execution schema and exit",
    )
    parser.add_argument(
        "--project-position-only",
        action="store_true",
        help=(
            "read one latest COMPLETE broker-position snapshot, publish one "
            "StrategyPositionV1/readiness update and exit"
        ),
    )
    parser.add_argument(
        "--publish-fixture-only",
        action="store_true",
        help=(
            "administratively publish one strict foundation fixture without "
            "reading a command"
        ),
    )
    parser.add_argument(
        "--once-command-id",
        default=None,
        help=(
            "ingest one explicit decision command id using persisted execution "
            "position/readiness/daily-risk facts, then exit"
        ),
    )
    parser.add_argument(
        "--foundation-fixture",
        type=Path,
        default=None,
        help=(
            "strict UTF-8 JSON fixture; valid only with --publish-fixture-only"
        ),
    )
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument(
        "--position-max-age-seconds",
        type=float,
        default=10.0,
    )
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
    position_feed_database: Path,
    position_max_age_seconds: float,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "catalog_hash": catalog_hash,
        "decision_database": str(decision_database),
        "execution_database": str(execution_database),
        "position_feed_database": str(position_feed_database),
        "position_max_age_seconds": float(position_max_age_seconds),
        "broker_order_gateway": "disabled",
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _safe_daily_risk(
    *,
    policy: ExecutionFoundationPolicyV1,
    target_pnl: float,
    timezone_name: str,
    observed_at_utc: str,
) -> DailyRiskStateV1:
    observed = parse_utc(observed_at_utc)
    trading_day = observed.astimezone(ZoneInfo(timezone_name)).date().isoformat()
    return DailyRiskStateV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        trading_day=trading_day,
        status=DailyRiskStatus.NOT_READY,
        realized_pnl=None,
        unrealized_pnl=None,
        total_pnl=None,
        target_pnl=target_pnl,
        pnl_ready=False,
        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
        updated_at_utc=observed_at_utc,
    )


def _current_fixture(
    *,
    state_source: SQLiteExecutionStateReader,
    policy: ExecutionFoundationPolicyV1,
    observed_at_utc: str,
    timezone_name: str,
) -> ExecutionFoundationFixtureV1:
    position = state_source.read_position(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
    )
    readiness = state_source.read_readiness(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
    )
    risk = state_source.read_latest_daily_risk(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
    )
    missing = [
        name
        for name, value in (
            ("strategy_position", position),
            ("execution_readiness", readiness),
            ("daily_risk", risk),
        )
        if value is None
    ]
    if missing:
        raise ExecutionStateReadError(
            "execution current state is incomplete: "
            f"missing={missing}; run --project-position-only and initialize "
            "execution control/risk facts before command admission"
        )

    observed = parse_utc(observed_at_utc)
    expected_day = observed.astimezone(ZoneInfo(timezone_name)).date().isoformat()
    if risk.trading_day != expected_day:
        raise ExecutionStateReadError(
            "latest daily-risk state belongs to another trading day: "
            f"expected={expected_day}, actual={risk.trading_day}"
        )

    return ExecutionFoundationFixtureV1(
        observed_at_utc=observed_at_utc,
        readiness=readiness,
        position=position,
        daily_risk=risk,
    )


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    instrument_policy = bundle.strategy_policy.require(instrument_id)

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
    position_feed_database = (
        arguments.position_feed_database.resolve()
        if arguments.position_feed_database is not None
        else settings.data_root
        / "position_feed"
        / "broker_positions.sqlite3"
    )

    repository = SQLiteExecutionStore(execution_database)
    state_source = SQLiteExecutionStateReader(execution_database)
    command_source = SQLiteExecutionDecisionReader(decision_database)
    position_source = SQLiteExecutionPositionFeedReader(
        position_feed_database
    )
    instance_id = new_id("instance")
    policy = ExecutionFoundationPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument.instrument_id,
        policy_hash=bundle.strategy_policy.content_hash,
    )
    projection_policy = PositionProjectionPolicyV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        max_snapshot_age_seconds=arguments.position_max_age_seconds,
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
                position_feed_database=position_feed_database,
                position_max_age_seconds=(
                    projection_policy.max_snapshot_age_seconds
                ),
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
                state_source.validate_schema()
            except (ExecutionSchemaError, ExecutionStateReadError) as exc:
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

            if arguments.project_position_only:
                position_source.validate_schema()
                observed = utc_now()
                observed_text = format_utc(observed)
                resolution = resolve_active_contract(
                    bundle.contract_calendar,
                    observed,
                )
                active_con_id = (
                    resolution.contract.con_id
                    if resolution.status == ActiveContractStatus.ACTIVE
                    and resolution.contract is not None
                    else None
                )
                registry = tuple(
                    RegisteredFuturesContractV1(
                        con_id=item.con_id,
                        local_symbol=item.local_symbol,
                        contract_is_active=(
                            item.con_id == active_con_id
                        ),
                    )
                    for item in bundle.contract_calendar.contracts
                )
                previous_position = state_source.read_position(
                    account_id=policy.account_id,
                    strategy_id=policy.strategy_id,
                    deployment_id=policy.deployment_id,
                    instrument_id=policy.instrument_id,
                )
                snapshot = await asyncio.to_thread(
                    position_source.read_latest_complete
                )
                projection = project_strategy_position(
                    snapshot=snapshot,
                    previous=previous_position,
                    policy=projection_policy,
                    registry=registry,
                    observed_at_utc=observed_text,
                    active_contract_available=(
                        active_con_id is not None
                    ),
                )
                previous_readiness = state_source.read_readiness(
                    account_id=policy.account_id,
                    strategy_id=policy.strategy_id,
                    deployment_id=policy.deployment_id,
                    instrument_id=policy.instrument_id,
                )
                readiness = merge_position_projection_readiness(
                    previous=previous_readiness,
                    projection=projection,
                    policy=projection_policy,
                    observed_at_utc=observed_text,
                )
                risk = state_source.read_latest_daily_risk(
                    account_id=policy.account_id,
                    strategy_id=policy.strategy_id,
                    deployment_id=policy.deployment_id,
                )
                if risk is None:
                    risk = _safe_daily_risk(
                        policy=policy,
                        target_pnl=instrument_policy.daily_pnl.target_usd,
                        timezone_name=instrument_policy.daily_pnl.timezone,
                        observed_at_utc=observed_text,
                    )
                fixture = ExecutionFoundationFixtureV1(
                    observed_at_utc=observed_text,
                    readiness=readiness,
                    position=projection.position,
                    daily_risk=risk,
                )
                await asyncio.to_thread(service.publish_fixture, fixture)
                print(
                    json.dumps(
                        {
                            "active_contract_status": resolution.status.value,
                            "position": projection.position.to_dict(),
                            "readiness": readiness.to_dict(),
                            "daily_risk": risk.to_dict(),
                            "broker_order_gateway": "disabled",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                service.publish_stopped()
                return 0

            if arguments.publish_fixture_only:
                if arguments.foundation_fixture is None:
                    raise ValueError(
                        "--foundation-fixture is required with "
                        "--publish-fixture-only"
                    )
                fixture = ExecutionFoundationFixtureV1.from_dict(
                    read_json_object(arguments.foundation_fixture.resolve())
                )
                await asyncio.to_thread(service.publish_fixture, fixture)
                print(
                    json.dumps(
                        {
                            "readiness": fixture.readiness.to_dict(),
                            "position": fixture.position.to_dict(),
                            "daily_risk": fixture.daily_risk.to_dict(),
                            "broker_order_gateway": "disabled",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                service.publish_stopped()
                return 0

            if arguments.foundation_fixture is not None:
                raise ValueError(
                    "--foundation-fixture is valid only with "
                    "--publish-fixture-only"
                )
            if arguments.once_command_id is None:
                raise ValueError(
                    "--once-command-id is required in command-admission mode"
                )

            existing = repository.read_command_state(
                str(arguments.once_command_id)
            )
            if existing is not None:
                print(
                    json.dumps(
                        {
                            "command_state": existing.to_dict(),
                            "broker_order_gateway": "disabled",
                            "reused_existing_state": True,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        indent=2,
                    )
                )
                service.publish_stopped()
                return 0

            command_source.validate_schema()
            command = await asyncio.to_thread(
                command_source.read_command,
                str(arguments.once_command_id),
            )
            if command is None:
                raise ExecutionDecisionSourceError(
                    f"strategy command not found: {arguments.once_command_id}"
                )
            observed_text = format_utc(utc_now())
            fixture = _current_fixture(
                state_source=state_source,
                policy=policy,
                observed_at_utc=observed_text,
                timezone_name=instrument_policy.daily_pnl.timezone,
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
                        "reused_existing_state": False,
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
            bool(arguments.project_position_only),
            bool(arguments.publish_fixture_only),
            arguments.once_command_id is not None,
        )
    )
    if selected != 1:
        print(
            "execution foundation requires exactly one of "
            "--validate-store-only, --project-position-only, "
            "--publish-fixture-only or --once-command-id",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        ExecutionDecisionSourceError,
        ExecutionDomainError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        ExecutionStoreError,
        JsonDataError,
        PositionProjectionError,
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
