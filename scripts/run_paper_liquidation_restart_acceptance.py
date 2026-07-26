from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.execution.adapters import (
    ExecutionPositionFeedError,
    ExecutionStateReadError,
    ProtectionSchemaError,
)
from ibmd.execution.adapters.sqlite_liquidation import LiquidationSchemaError
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    SubprocessJsonCommandExecutor,
)
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePathsV1,
    PaperLiquidationAcceptancePolicyV1,
)
from ibmd.operations.paper_liquidation_restart_acceptance import (
    ExpectedLiquidationRestartCrashExecutor,
    PaperLiquidationRestartAcceptanceRunner,
    SQLitePaperLiquidationRestartStateSource,
)
from ibmd.operations.restart_probe import RestartProbeError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one deterministic paper liquidation restart drill. The TP "
            "cancel, STOP cancel and MARKET-close child processes terminate "
            "immediately after their confirmed broker action; ordinary "
            "invocations must then reconcile without another mutation."
        )
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--entry-summary", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument("--artifacts-root", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--max-invocations", type=int, default=24)
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
    parser.add_argument("--child-timeout-seconds", type=float, default=180.0)
    return parser


def _database_paths(
    arguments: argparse.Namespace,
    data_root: Path,
) -> tuple[Path, Path]:
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
    return execution_database, position_feed_database


def _validation_payload(
    *,
    settings,
    execution_database: Path,
    position_feed_database: Path,
    catalog_root: Path,
) -> dict:
    return {
        "paper_liquidation_restart_dependencies_compatible": True,
        "environment": settings.environment,
        "account_id": settings.ib_account_id,
        "deployment_id": settings.deployment_id,
        "execution_database": str(execution_database),
        "position_feed_database": str(position_feed_database),
        "catalog_root": str(catalog_root.resolve()),
        "intentional_child_exit_code": 86,
        "restart_actions": [
            "CANCEL_TAKE_PROFIT",
            "CANCEL_STOP",
            "SUBMIT_MARKET_CLOSE",
        ],
        "interactive_confirmation_required": False,
        "automatic_retry_enabled": False,
        "paper_account_left_flat_after_success": True,
    }


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    execution_database, position_feed_database = _database_paths(
        arguments,
        settings.data_root,
    )
    state = SQLitePaperLiquidationRestartStateSource(
        execution_database=execution_database,
        position_feed_database=position_feed_database,
    )
    state.validate_schema()
    if arguments.validate_only:
        print(
            json.dumps(
                _validation_payload(
                    settings=settings,
                    execution_database=execution_database,
                    position_feed_database=position_feed_database,
                    catalog_root=arguments.catalog_root,
                ),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0

    if arguments.entry_summary is None:
        raise ValueError("--entry-summary is required with --run")
    entry_summary = arguments.entry_summary.resolve()
    artifacts_root = (
        arguments.artifacts_root.resolve()
        if arguments.artifacts_root is not None
        else settings.data_root
        / "runtime"
        / "paper_restart_acceptance"
        / "liquidation"
    )
    run_id = "run-" + utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    artifacts = PaperAcceptanceArtifactStore(artifacts_root / run_id)
    paths = PaperLiquidationAcceptancePathsV1(
        repo_root=ROOT,
        execution_database=execution_database,
        position_feed_database=position_feed_database,
        catalog_root=arguments.catalog_root,
        entry_summary=entry_summary,
    )
    policy = PaperLiquidationAcceptancePolicyV1(
        environment=settings.environment,
        account_id=settings.ib_account_id,
        deployment_id=settings.deployment_id,
        strategy_id=settings.strategy_id,
        instrument_id=str(arguments.instrument or "").strip(),
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
    normal_executor = SubprocessJsonCommandExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    crash_executor = ExpectedLiquidationRestartCrashExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    runner = PaperLiquidationRestartAcceptanceRunner(
        policy=policy,
        command_executor=normal_executor,
        crash_executor=crash_executor,
        state_source=state,
        artifacts=artifacts,
    )
    try:
        result = runner.run()
    except Exception as exc:
        failure = {
            "schema_name": "PaperLiquidationRestartAcceptanceFailure",
            "schema_version": 1,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stage": getattr(exc, "stage", "unhandled"),
            "broker_exposure_possible": bool(
                getattr(exc, "broker_exposure_possible", False)
            ),
            "artifact_directory": str(artifacts.directory),
            "automatic_retry_enabled": False,
            "required_operator_action": (
                "Inspect TWS and the saved restart checkpoint immediately when "
                "broker_exposure_possible=true. Do not submit another cancel or "
                "MARKET close until the existing liquidation operation is "
                "reconciled."
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
            "paper liquidation restart acceptance requires exactly one mode: "
            "--validate-only or --run",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        ConfigurationError,
        ExecutionPositionFeedError,
        ExecutionStateReadError,
        LiquidationSchemaError,
        PaperLiquidationAcceptanceError,
        ProtectionSchemaError,
        RestartProbeError,
        ValueError,
    ) as exc:
        print(
            "paper liquidation restart acceptance failed before runner startup: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
