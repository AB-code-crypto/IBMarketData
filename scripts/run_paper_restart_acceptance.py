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

from ibmd.execution.adapters import (
    BrokerAttemptSchemaError,
    ExecutionDecisionSourceError,
    ExecutionPositionFeedError,
    ExecutionSchemaError,
    ExecutionStateReadError,
    ProtectionSchemaError,
    SQLiteBrokerAttemptStore,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
    SQLiteProtectionReader,
)
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    PaperAcceptanceError,
    PaperAcceptancePathsV1,
    PaperAcceptancePolicyV1,
    SQLitePaperAcceptanceStateSource,
    SubprocessJsonCommandExecutor,
)
from ibmd.operations.paper_restart_acceptance import (
    ExpectedRestartCrashExecutor,
    PaperRestartAcceptanceRunner,
)
from ibmd.operations.restart_probe import RestartProbeError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one paper-only deterministic restart drill. Each MARKET, STOP "
            "and TAKE PROFIT child terminates immediately after its successful "
            "broker call; ordinary entrypoints must then adopt the same order "
            "without another submission."
        )
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--drill-id", default=None)
    parser.add_argument(
        "--target-side",
        choices=("LONG", "SHORT"),
        default="LONG",
    )
    parser.add_argument("--decision-database", type=Path, default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
    parser.add_argument("--artifacts-root", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--command-ttl-seconds", type=int, default=600)
    parser.add_argument("--position-max-age-seconds", type=float, default=30.0)
    parser.add_argument("--entry-max-invocations", type=int, default=12)
    parser.add_argument("--entry-poll-seconds", type=float, default=1.0)
    parser.add_argument("--position-wait-seconds", type=float, default=60.0)
    parser.add_argument("--position-poll-seconds", type=float, default=1.0)
    parser.add_argument("--protective-max-invocations", type=int, default=16)
    parser.add_argument("--protective-poll-seconds", type=float, default=1.0)
    parser.add_argument("--reconciliation-read-attempts", type=int, default=5)
    parser.add_argument("--reconciliation-poll-seconds", type=float, default=1.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    parser.add_argument("--submit-client-id-offset", type=int, default=120)
    parser.add_argument(
        "--protective-submit-client-id-offset",
        type=int,
        default=140,
    )
    parser.add_argument(
        "--reconciliation-client-id-offset",
        type=int,
        default=100,
    )
    parser.add_argument("--child-timeout-seconds", type=float, default=180.0)
    return parser


def _drill_id(value: str | None) -> str:
    if value is not None and str(value).strip():
        return str(value).strip()
    return "paper-restart-" + utc_now().strftime("%Y%m%dT%H%M%SZ")


def _paths(arguments: argparse.Namespace, data_root: Path) -> PaperAcceptancePathsV1:
    return PaperAcceptancePathsV1(
        repo_root=ROOT,
        decision_database=(
            arguments.decision_database
            if arguments.decision_database is not None
            else data_root / "decision" / "decision.sqlite3"
        ),
        execution_database=(
            arguments.execution_database
            if arguments.execution_database is not None
            else data_root / "execution" / "execution.sqlite3"
        ),
        position_feed_database=(
            arguments.position_feed_database
            if arguments.position_feed_database is not None
            else data_root / "position_feed" / "broker_positions.sqlite3"
        ),
        catalog_root=arguments.catalog_root,
    )


def _validate_core_stores(paths: PaperAcceptancePathsV1) -> None:
    validators = (
        SQLiteExecutionStore(paths.execution_database),
        SQLiteExecutionStateReader(paths.execution_database),
        SQLiteExecutionDecisionReader(paths.decision_database),
        SQLiteBrokerAttemptStore(paths.execution_database),
        SQLiteProtectionReader(paths.execution_database),
        SQLiteExecutionPositionFeedReader(paths.position_feed_database),
    )
    for validator in validators:
        validator.validate_schema()


def _validation_payload(*, settings, paths: PaperAcceptancePathsV1) -> dict:
    return {
        "paper_restart_acceptance_dependencies_compatible": True,
        "environment": settings.environment,
        "account_id": settings.ib_account_id,
        "deployment_id": settings.deployment_id,
        "decision_database": str(paths.decision_database),
        "execution_database": str(paths.execution_database),
        "position_feed_database": str(paths.position_feed_database),
        "catalog_root": str(paths.catalog_root),
        "target_side_default": "LONG",
        "intentional_child_exit_code": 86,
        "intentional_process_terminations": [
            "MARKET_ENTRY",
            "STOP_LOSS",
            "TAKE_PROFIT",
        ],
        "automatic_retry_enabled": False,
        "manual_cleanup_required_after_success": True,
    }


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    paths = _paths(arguments, settings.data_root)
    _validate_core_stores(paths)
    if arguments.validate_only:
        print(
            json.dumps(
                _validation_payload(settings=settings, paths=paths),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0

    drill_id = _drill_id(arguments.drill_id)
    artifacts_root = (
        arguments.artifacts_root.resolve()
        if arguments.artifacts_root is not None
        else settings.data_root / "runtime" / "paper_restart_acceptance"
    )
    run_id = "run-" + utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    artifacts = PaperAcceptanceArtifactStore(
        artifacts_root / drill_id / run_id
    )
    policy = PaperAcceptancePolicyV1(
        environment=settings.environment,
        account_id=settings.ib_account_id,
        deployment_id=settings.deployment_id,
        instrument_id=str(arguments.instrument or "").strip(),
        drill_id=drill_id,
        target_side=arguments.target_side,
        command_ttl_seconds=arguments.command_ttl_seconds,
        position_max_age_seconds=arguments.position_max_age_seconds,
        entry_max_invocations=arguments.entry_max_invocations,
        entry_poll_seconds=arguments.entry_poll_seconds,
        position_wait_seconds=arguments.position_wait_seconds,
        position_poll_seconds=arguments.position_poll_seconds,
        protective_max_invocations=arguments.protective_max_invocations,
        protective_poll_seconds=arguments.protective_poll_seconds,
        reconciliation_read_attempts=arguments.reconciliation_read_attempts,
        reconciliation_poll_seconds=arguments.reconciliation_poll_seconds,
        commission_wait_seconds=arguments.commission_wait_seconds,
        submit_client_id_offset=arguments.submit_client_id_offset,
        protective_submit_client_id_offset=(
            arguments.protective_submit_client_id_offset
        ),
        reconciliation_client_id_offset=(
            arguments.reconciliation_client_id_offset
        ),
        paths=paths,
    )
    state = SQLitePaperAcceptanceStateSource(
        position_feed_database=paths.position_feed_database,
        execution_database=paths.execution_database,
    )
    normal_executor = SubprocessJsonCommandExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    crash_executor = ExpectedRestartCrashExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    runner = PaperRestartAcceptanceRunner(
        policy=policy,
        command_executor=normal_executor,
        crash_executor=crash_executor,
        state_source=state,
        artifacts=artifacts,
        sleeper=time.sleep,
    )
    try:
        result = runner.run()
    except Exception as exc:
        failure = {
            "schema_name": "PaperRestartAcceptanceFailure",
            "schema_version": 1,
            "drill_id": drill_id,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stage": getattr(exc, "stage", "unhandled"),
            "position_may_be_open": bool(
                getattr(exc, "position_may_be_open", False)
            ),
            "artifact_directory": str(artifacts.directory),
            "automatic_retry_enabled": False,
            "required_operator_action": (
                "Inspect TWS and the saved restart checkpoints. Never create a "
                "new drill_id while an existing order outcome is uncertain. "
                "If the position is open without a proven live STOP, close it "
                "manually on the paper account."
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
            "paper restart acceptance requires exactly one mode: "
            "--validate-only or --run",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        BrokerAttemptSchemaError,
        ConfigurationError,
        ExecutionDecisionSourceError,
        ExecutionPositionFeedError,
        ExecutionSchemaError,
        ExecutionStateReadError,
        PaperAcceptanceError,
        ProtectionSchemaError,
        RestartProbeError,
        ValueError,
    ) as exc:
        print(
            "paper restart acceptance failed before runner startup: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
