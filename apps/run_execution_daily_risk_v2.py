from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.execution.adapters import (
    ExecutionSchemaError,
    ExecutionStateReadError,
    ProtectionSchemaError,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_sources import (
    DailyRiskSourceError,
    SQLiteDailyRiskExecutionReader,
    SQLiteDailyRiskMarketDataReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_store import (
    DailyRiskSchemaError,
    DailyRiskStoreError,
    SQLiteDailyRiskStore,
)
from ibmd.execution.adapters.sqlite_reverse_finalization import (
    ReverseFinalizationSchemaError,
    SQLiteReverseFinalizationStore,
)
from ibmd.execution.application.daily_risk import (
    DailyRiskService,
    DailyRiskServiceError,
    daily_risk_payload,
)
from ibmd.execution.domain.daily_risk import (
    DailyRiskDomainError,
    DailyRiskPolicyV1,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, utc_now

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate and persist strategy-owned daily PnL and DailyRiskStateV1. "
            "The process reads execution evidence and target market data only; it "
            "does not connect to IB or perform broker mutations."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--market-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--market-max-age-seconds", type=float, default=None)
    return parser


def _component_command(
    script_name: str,
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / script_name} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


def _base_command(
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


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    if not strategy_policy.daily_pnl.enabled:
        raise DailyRiskServiceError(
            f"daily PnL policy is disabled for {instrument_id}"
        )
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )
    market_database = (
        arguments.market_database.resolve()
        if arguments.market_database is not None
        else settings.data_root / "market_data" / instrument.database_name
    )

    execution_store = SQLiteExecutionStore(execution_database)
    execution_state = SQLiteExecutionStateReader(execution_database)
    episode_source = SQLiteProtectionReader(execution_database)
    evidence_source = SQLiteDailyRiskExecutionReader(execution_database)
    daily_risk_store = SQLiteDailyRiskStore(execution_database)
    reverse_store = SQLiteReverseFinalizationStore(execution_database)
    market_source = SQLiteDailyRiskMarketDataReader(
        market_database,
        instrument_id=instrument.instrument_id,
        price_precision=instrument.price_precision,
    )
    execution_store.validate_schema()
    execution_state.validate_schema()
    episode_source.validate_schema()
    evidence_source.validate_schema()
    daily_risk_store.validate_schema()
    reverse_store.validate_schema()
    market_source.validate_schema()

    if arguments.validate_store_only:
        print(
            "execution daily-risk dependencies are compatible: "
            f"execution={execution_database}, market_data={market_database}"
        )
        return 0

    market_max_age = (
        float(arguments.market_max_age_seconds)
        if arguments.market_max_age_seconds is not None
        else float(strategy_policy.signal.max_complete_bar_lag_seconds)
    )
    service = DailyRiskService(
        policy=DailyRiskPolicyV1(
            account_id=settings.ib_account_id,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            timezone_name=strategy_policy.daily_pnl.timezone,
            target_pnl=strategy_policy.daily_pnl.target_usd,
            contract_multiplier=instrument.multiplier,
            market_max_age_seconds=market_max_age,
        ),
        execution_state_source=execution_state,
        episode_source=episode_source,
        owned_fill_source=evidence_source,
        market_mark_source=market_source,
        repository=daily_risk_store,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        result = service.run_once(
            observed_at_utc=format_utc(utc_now())
        )
    print(
        json.dumps(
            daily_risk_payload(result),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(bool(arguments.once))
    if selected != 1:
        print(
            "execution daily risk requires exactly one of "
            "--validate-store-only or --once",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        DailyRiskDomainError,
        DailyRiskSchemaError,
        DailyRiskServiceError,
        DailyRiskSourceError,
        DailyRiskStoreError,
        ExecutionSchemaError,
        ExecutionStateReadError,
        ProtectionSchemaError,
        ReverseFinalizationSchemaError,
        ValueError,
    ) as exc:
        print(
            f"execution daily risk failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        settings = load_deployment_settings()
        execution_database = (
            arguments.execution_database.resolve()
            if arguments.execution_database is not None
            else settings.data_root / "execution" / "execution.sqlite3"
        )
        print("Apply fresh target schemas if needed:", file=sys.stderr)
        for command in (
            _base_command(
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            _component_command(
                "run_execution_protective_lifecycle_schema.py",
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            _component_command(
                "run_execution_liquidation_schema.py",
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            _component_command(
                "run_execution_reverse_finalization_schema.py",
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            _component_command(
                "run_execution_daily_risk_schema.py",
                database_path=execution_database,
                application_version=settings.application_version,
            ),
        ):
            print(command, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
