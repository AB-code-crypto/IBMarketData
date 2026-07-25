from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import CatalogError, load_catalog_bundle
from ibmd.execution.adapters.sqlite_liquidation import (
    LiquidationSchemaError,
    LiquidationStoreError,
    SQLiteLiquidationStore,
)
from ibmd.execution.adapters.sqlite_liquidation_triggers import (
    LiquidationTriggerReadError,
    SQLiteLiquidationTriggerReader,
)
from ibmd.execution.application.liquidation_triggers import (
    LiquidationTriggerProducerError,
    LiquidationTriggerProducerPolicyV1,
    LiquidationTriggerProducerService,
    liquidation_trigger_producer_payload,
)
from ibmd.execution.domain.liquidation import LiquidationDomainError
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc, utc_now

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate broker-free DAILY_FLAT, DAILY_HALT, missing-STOP and "
            "rollover conditions for all open position episodes, then append "
            "durable triggers to the single liquidation operation."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--once-at", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--missing-stop-grace-seconds",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--allow-unqualified-session",
        action="store_true",
        help=(
            "Development/paper override only. Production daily-flat triggers "
            "must use a production-qualified session calendar."
        ),
    )
    return parser


def _execution_database(arguments: argparse.Namespace, settings) -> Path:
    return (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )


def _observed_at(arguments: argparse.Namespace) -> str:
    if arguments.once_at is None:
        return format_utc(utc_now())
    return format_utc(parse_utc(str(arguments.once_at)))


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(
        arguments.catalog_root.resolve(),
        require_production_sessions=False,
    )
    instrument_id = str(arguments.instrument or "").strip()
    if not instrument_id:
        raise ValueError("--instrument is required")
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    if not strategy_policy.trading_enabled:
        raise ValueError(
            f"strategy trading is disabled for instrument={instrument_id}"
        )
    database = _execution_database(arguments, settings)
    source = SQLiteLiquidationTriggerReader(database)
    repository = SQLiteLiquidationStore(database)
    source.validate_schema()
    repository.validate_schema()
    if arguments.validate_store_only:
        print(
            "execution liquidation-trigger dependencies are compatible: "
            f"execution={database}, catalog={arguments.catalog_root.resolve()}"
        )
        return 0
    policy = LiquidationTriggerProducerPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument_id,
        missing_stop_grace_seconds=float(
            arguments.missing_stop_grace_seconds
        ),
        require_production_session=(
            not bool(arguments.allow_unqualified_session)
        ),
    )
    service = LiquidationTriggerProducerService(
        policy=policy,
        bundle=bundle,
        state_source=source,
        repository=repository,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        result = service.run_once(observed_at_utc=_observed_at(arguments))
    payload = liquidation_trigger_producer_payload(result)
    payload["session_calendar"] = {
        "calendar_version": bundle.session_calendar.calendar_version,
        "production_required": policy.require_production_session,
        "development_override": bool(arguments.allow_unqualified_session),
    }
    payload["contract_calendar"] = {
        "calendar_version": bundle.contract_calendar.calendar_version,
    }
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if int(bool(arguments.validate_store_only)) + int(bool(arguments.once)) != 1:
        print(
            "execution liquidation triggers require exactly one mode: "
            "--validate-store-only or --once",
            file=sys.stderr,
        )
        return 2
    if arguments.once_at is not None and not arguments.once:
        print("--once-at requires --once", file=sys.stderr)
        return 2
    try:
        return run(arguments)
    except (
        CatalogError,
        LiquidationDomainError,
        LiquidationSchemaError,
        LiquidationStoreError,
        LiquidationTriggerProducerError,
        LiquidationTriggerReadError,
        ValueError,
    ) as exc:
        print(
            "execution liquidation triggers failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
