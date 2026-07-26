from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle
from ibmd.execution.adapters import (
    ExecutionPositionFeedError,
    ExecutionStateReadError,
    ProtectionSchemaError,
)
from ibmd.execution.adapters.sqlite_daily_risk_sources import (
    DailyRiskSourceError,
    SQLiteDailyRiskMarketDataReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_store import (
    DailyRiskSchemaError,
)
from ibmd.execution.adapters.sqlite_liquidation import LiquidationSchemaError
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    SubprocessJsonCommandExecutor,
)
from ibmd.operations.paper_daily_halt_acceptance import (
    PaperDailyHaltAcceptanceRunner,
)
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePathsV1,
    PaperLiquidationAcceptancePolicyV1,
    SQLitePaperLiquidationAcceptanceStateSource,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one paper DAILY_HALT acceptance. Real strategy-owned fills and "
            "the current protected episode are combined with an explicit "
            "synthetic market mark to cross the configured target. The runner "
            "then persists DAILY_HALT, performs real paper liquidation and "
            "requires sticky HALTED/COMPLETE after broker-proven FLAT."
        )
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--source-summary", type=Path, default=None)
    parser.add_argument("--drill-id", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument("--market-database", type=Path, default=None)
    parser.add_argument("--artifacts-root", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--max-invocations", type=int, default=20)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--position-max-age-seconds", type=float, default=30.0)
    parser.add_argument("--reconciliation-read-attempts", type=int, default=5)
    parser.add_argument("--reconciliation-poll-seconds", type=float, default=1.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    parser.add_argument("--cancel-client-id-offset", type=int, default=140)
    parser.add_argument("--submit-client-id-offset", type=int, default=160)
    parser.add_argument(
        "--reconciliation-client-id-offset",
        type=int,
        default=100,
    )
    parser.add_argument("--daily-risk-max-invocations", type=int, default=5)
    parser.add_argument("--daily-risk-poll-seconds", type=float, default=1.0)
    parser.add_argument("--child-timeout-seconds", type=float, default=180.0)
    return parser


def _paths(arguments: argparse.Namespace, data_root: Path, instrument):
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else data_root / "execution" / "execution.sqlite3"
    )
    position_feed_database = (
        arguments.position_feed_database.resolve()
        if arguments.position_feed_database is not None
        else data_root / "position_feed" / "broker_positions.sqlite3"
    )
    market_database = (
        arguments.market_database.resolve()
        if arguments.market_database is not None
        else data_root / "market_data" / instrument.database_name
    )
    return execution_database, position_feed_database, market_database


def _drill_id(value: str | None) -> str:
    if value is not None and str(value).strip():
        return str(value).strip()
    return "paper-daily-halt-" + utc_now().strftime("%Y%m%dT%H%M%SZ")


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    bundle = load_catalog_bundle(arguments.catalog_root.resolve())
    instrument_id = str(arguments.instrument or "").strip()
    instrument = bundle.instrument_master.require(instrument_id)
    execution_database, position_feed_database, market_database = _paths(
        arguments,
        settings.data_root,
        instrument,
    )
    state = SQLitePaperLiquidationAcceptanceStateSource(
        execution_database=execution_database,
        position_feed_database=position_feed_database,
    )
    market = SQLiteDailyRiskMarketDataReader(
        market_database,
        instrument_id=instrument.instrument_id,
        price_precision=instrument.price_precision,
    )
    state.validate_schema()
    market.validate_schema()
    if arguments.validate_only:
        print(
            json.dumps(
                {
                    "paper_daily_halt_dependencies_compatible": True,
                    "environment": settings.environment,
                    "account_id": settings.ib_account_id,
                    "deployment_id": settings.deployment_id,
                    "execution_database": str(execution_database),
                    "position_feed_database": str(position_feed_database),
                    "market_database": str(market_database),
                    "target_pnl": (
                        bundle.strategy_policy.require(instrument_id)
                        .daily_pnl.target_usd
                    ),
                    "synthetic_market_mark_only": True,
                    "real_owned_fill_evidence_only": True,
                    "interactive_confirmation_required": False,
                    "automatic_retry_enabled": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0

    if arguments.source_summary is None:
        raise ValueError("--source-summary is required with --run")
    source_summary = arguments.source_summary.resolve()
    drill_id = _drill_id(arguments.drill_id)
    artifacts_root = (
        arguments.artifacts_root.resolve()
        if arguments.artifacts_root is not None
        else settings.data_root / "runtime" / "paper_daily_halt_acceptance"
    )
    run_id = "run-" + utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    artifacts = PaperAcceptanceArtifactStore(
        artifacts_root / drill_id / run_id
    )
    paths = PaperLiquidationAcceptancePathsV1(
        repo_root=ROOT,
        execution_database=execution_database,
        position_feed_database=position_feed_database,
        catalog_root=arguments.catalog_root,
        entry_summary=source_summary,
    )
    policy = PaperLiquidationAcceptancePolicyV1(
        environment=settings.environment,
        account_id=settings.ib_account_id,
        deployment_id=settings.deployment_id,
        strategy_id=settings.strategy_id,
        instrument_id=instrument.instrument_id,
        max_invocations=arguments.max_invocations,
        poll_seconds=arguments.poll_seconds,
        position_max_age_seconds=arguments.position_max_age_seconds,
        reconciliation_read_attempts=arguments.reconciliation_read_attempts,
        reconciliation_poll_seconds=arguments.reconciliation_poll_seconds,
        commission_wait_seconds=arguments.commission_wait_seconds,
        cancel_client_id_offset=arguments.cancel_client_id_offset,
        submit_client_id_offset=arguments.submit_client_id_offset,
        reconciliation_client_id_offset=(
            arguments.reconciliation_client_id_offset
        ),
        paths=paths,
    )
    executor = SubprocessJsonCommandExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    runner = PaperDailyHaltAcceptanceRunner(
        policy=policy,
        drill_id=drill_id,
        market_database=market_database,
        command_executor=executor,
        state_source=state,
        artifacts=artifacts,
        daily_risk_max_invocations=arguments.daily_risk_max_invocations,
        daily_risk_poll_seconds=arguments.daily_risk_poll_seconds,
        sleeper=time.sleep,
    )
    try:
        result = runner.run()
    except Exception as exc:
        failure = {
            "schema_name": "PaperDailyHaltAcceptanceFailure",
            "schema_version": 1,
            "drill_id": drill_id,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stage": getattr(exc, "stage", "unhandled"),
            "broker_exposure_possible": bool(
                getattr(exc, "broker_exposure_possible", False)
            ),
            "artifact_directory": str(artifacts.directory),
            "synthetic_market_mark_only": True,
            "automatic_retry_enabled": False,
            "required_operator_action": (
                "Inspect TWS and saved artifacts when broker exposure is "
                "possible. Do not clear or replace the TRIGGERED/CLOSING state "
                "and do not create another liquidation operation."
            ),
        }
        artifacts.write_json("failure", failure)
        print(json.dumps(failure, ensure_ascii=False, sort_keys=True, indent=2))
        return 2
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
    if int(bool(arguments.validate_only)) + int(bool(arguments.run)) != 1:
        print(
            "paper daily-halt acceptance requires exactly one mode: "
            "--validate-only or --run",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        ConfigurationError,
        DailyRiskSchemaError,
        DailyRiskSourceError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        LiquidationSchemaError,
        PaperLiquidationAcceptanceError,
        ProtectionSchemaError,
        ValueError,
    ) as exc:
        print(
            "paper daily-halt acceptance failed before runner startup: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
