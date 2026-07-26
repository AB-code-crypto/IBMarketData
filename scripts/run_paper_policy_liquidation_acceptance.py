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
    ExecutionPositionFeedError,
    ExecutionStateReadError,
    ProtectionSchemaError,
)
from ibmd.execution.adapters.sqlite_liquidation import (
    LiquidationSchemaError,
)
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import parse_utc, utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    SubprocessJsonCommandExecutor,
)
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePathsV1,
    PaperLiquidationAcceptancePolicyV1,
    SQLitePaperLiquidationAcceptanceStateSource,
)
from ibmd.operations.paper_policy_liquidation_acceptance import (
    PaperPolicyLiquidationAcceptanceRunner,
)
from ibmd.public_contracts.liquidation import LiquidationReason

_SCENARIOS = ("DAILY_FLAT", "ROLLOVER")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one paper policy-trigger liquidation acceptance. The runner "
            "evaluates DAILY_FLAT or ROLLOVER at an explicit logical UTC time, "
            "persists only that trigger and closes the real protected paper "
            "position through the standard liquidation coordinator."
        )
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--source-summary", type=Path, default=None)
    parser.add_argument("--scenario", choices=_SCENARIOS, default=None)
    parser.add_argument("--trigger-at-utc", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--position-feed-database", type=Path, default=None)
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
    parser.add_argument("--child-timeout-seconds", type=float, default=180.0)
    parser.add_argument(
        "--allow-unqualified-session",
        action="store_true",
        help=(
            "paper-only DAILY_FLAT override for the committed parity session "
            "calendar"
        ),
    )
    return parser


def _paths(arguments: argparse.Namespace, data_root: Path):
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
        "paper_policy_liquidation_dependencies_compatible": True,
        "environment": settings.environment,
        "account_id": settings.ib_account_id,
        "deployment_id": settings.deployment_id,
        "execution_database": str(execution_database),
        "position_feed_database": str(position_feed_database),
        "catalog_root": str(catalog_root.resolve()),
        "supported_scenarios": list(_SCENARIOS),
        "interactive_confirmation_required": False,
        "automatic_retry_enabled": False,
        "paper_account_left_flat_after_success": True,
    }


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    execution_database, position_feed_database = _paths(
        arguments,
        settings.data_root,
    )
    state = SQLitePaperLiquidationAcceptanceStateSource(
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

    if arguments.source_summary is None:
        raise ValueError("--source-summary is required with --run")
    if arguments.scenario is None:
        raise ValueError("--scenario is required with --run")
    if arguments.trigger_at_utc is None:
        raise ValueError("--trigger-at-utc is required with --run")
    trigger_at = parse_utc(str(arguments.trigger_at_utc))
    scenario = LiquidationReason(arguments.scenario)
    if scenario == LiquidationReason.DAILY_FLAT and not arguments.allow_unqualified_session:
        raise ValueError(
            "the committed session calendar is parity-only; DAILY_FLAT paper "
            "acceptance currently requires --allow-unqualified-session"
        )
    source_summary = arguments.source_summary.resolve()
    artifacts_root = (
        arguments.artifacts_root.resolve()
        if arguments.artifacts_root is not None
        else settings.data_root
        / "runtime"
        / "paper_policy_liquidation_acceptance"
    )
    run_id = (
        scenario.value.lower()
        + "-run-"
        + utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    )
    artifacts = PaperAcceptanceArtifactStore(artifacts_root / run_id)
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
    executor = SubprocessJsonCommandExecutor(
        python_executable=sys.executable,
        repo_root=ROOT,
        artifacts=artifacts,
        timeout_seconds=arguments.child_timeout_seconds,
    )
    runner = PaperPolicyLiquidationAcceptanceRunner(
        policy=policy,
        scenario=scenario,
        logical_trigger_at_utc=trigger_at.isoformat().replace("+00:00", "Z"),
        allow_unqualified_session=arguments.allow_unqualified_session,
        command_executor=executor,
        state_source=state,
        artifacts=artifacts,
        sleeper=time.sleep,
    )
    try:
        result = runner.run()
    except Exception as exc:
        failure = {
            "schema_name": "PaperPolicyLiquidationAcceptanceFailure",
            "schema_version": 1,
            "scenario": scenario.value,
            "logical_trigger_at_utc": trigger_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "stage": getattr(exc, "stage", "unhandled"),
            "broker_exposure_possible": bool(
                getattr(exc, "broker_exposure_possible", False)
            ),
            "artifact_directory": str(artifacts.directory),
            "automatic_retry_enabled": False,
            "required_operator_action": (
                "Inspect TWS and saved artifacts when broker exposure is "
                "possible. Never create a second liquidation operation for the "
                "same position episode."
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
            "paper policy liquidation acceptance requires exactly one mode: "
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
        ValueError,
    ) as exc:
        print(
            "paper policy liquidation acceptance failed before runner startup: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
