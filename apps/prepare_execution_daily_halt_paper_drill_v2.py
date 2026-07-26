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
    ExecutionStateReadError,
    ProtectionSchemaError,
    SQLiteExecutionStateReader,
    SQLiteProtectionReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_sources import (
    DailyRiskSourceError,
    SQLiteDailyRiskExecutionReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_store import (
    DailyRiskSchemaError,
    DailyRiskStoreError,
    SQLiteDailyRiskStore,
)
from ibmd.execution.application.paper_daily_halt_drill import (
    PaperDailyHaltDrillError,
    PaperDailyHaltDrillPolicyV1,
    PaperDailyHaltDrillService,
)
from ibmd.execution.domain.daily_risk import DailyRiskDomainError
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc, utc_now

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Persist one paper-drill DailyRiskState=TRIGGERED by combining real "
            "strategy-owned fill evidence with an explicitly synthetic market "
            "mark. This entrypoint is broker-free and tests sticky halt/liquidation "
            "integration, not live market PnL pricing."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--prepare-position-episode-id", default=None)
    parser.add_argument("--drill-id", default=None)
    parser.add_argument("--observed-at-utc", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--market-max-age-seconds", type=float, default=None)
    parser.add_argument("--trigger-cushion-usd", type=float, default=1.0)
    return parser


def _database(arguments: argparse.Namespace, settings) -> Path:
    return (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    database = _database(arguments, settings)
    state_source = SQLiteExecutionStateReader(database)
    episode_source = SQLiteProtectionReader(database)
    evidence_source = SQLiteDailyRiskExecutionReader(database)
    repository = SQLiteDailyRiskStore(database)
    state_source.validate_schema()
    episode_source.validate_schema()
    evidence_source.validate_schema()
    repository.validate_schema()
    if arguments.validate_store_only:
        print(
            "paper daily-halt drill dependencies are compatible: "
            f"execution={database}, catalog={arguments.catalog_root.resolve()}"
        )
        return 0

    episode_id = str(arguments.prepare_position_episode_id or "").strip()
    drill_id = str(arguments.drill_id or "").strip()
    if not episode_id:
        raise ValueError("--prepare-position-episode-id is required")
    if not drill_id:
        raise ValueError("--drill-id is required")
    observed = (
        format_utc(parse_utc(arguments.observed_at_utc))
        if arguments.observed_at_utc is not None
        else format_utc(utc_now())
    )
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    strategy_policy = bundle.strategy_policy.require(instrument_id)
    if not strategy_policy.daily_pnl.enabled:
        raise PaperDailyHaltDrillError(
            f"daily PnL policy is disabled for {instrument_id}"
        )
    market_max_age = (
        float(arguments.market_max_age_seconds)
        if arguments.market_max_age_seconds is not None
        else float(strategy_policy.signal.max_complete_bar_lag_seconds)
    )
    service = PaperDailyHaltDrillService(
        policy=PaperDailyHaltDrillPolicyV1(
            drill_id=drill_id,
            account_id=settings.ib_account_id,
            environment=settings.environment,
            strategy_id=bundle.strategy_policy.strategy_id,
            strategy_version=bundle.strategy_policy.strategy_version,
            deployment_id=settings.deployment_id,
            instrument_id=instrument.instrument_id,
            timezone_name=strategy_policy.daily_pnl.timezone,
            target_pnl=strategy_policy.daily_pnl.target_usd,
            contract_multiplier=instrument.multiplier,
            market_max_age_seconds=market_max_age,
            price_tick=instrument.price_tick,
            trigger_cushion_usd=float(arguments.trigger_cushion_usd),
        ),
        execution_state_source=state_source,
        episode_source=episode_source,
        evidence_source=evidence_source,
        repository=repository,
    )
    position = state_source.read_position(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        deployment_id=settings.deployment_id,
        instrument_id=instrument.instrument_id,
    )
    if position is None or position.position_episode_id != episode_id:
        raise PaperDailyHaltDrillError(
            "requested episode is not the current owned strategy position"
        )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        result = service.run_once(observed_at_utc=observed)
    print(
        json.dumps(
            result.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.prepare_position_episode_id is not None
    )
    if selected != 1:
        print(
            "paper daily-halt drill requires exactly one mode: "
            "--validate-store-only or --prepare-position-episode-id",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        ConfigurationError,
        DailyRiskDomainError,
        DailyRiskSchemaError,
        DailyRiskSourceError,
        DailyRiskStoreError,
        ExecutionStateReadError,
        PaperDailyHaltDrillError,
        ProtectionSchemaError,
        ValueError,
    ) as exc:
        print(
            "paper daily-halt drill preparation failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
