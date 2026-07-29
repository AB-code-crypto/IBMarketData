from __future__ import annotations

from dataclasses import replace

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
    evaluate_liquidation_trigger_candidates,
)
from ibmd.execution.domain.liquidation import (
    LiquidationDomainError,
    LiquidationRequestResult,
    request_liquidation,
)
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.public_contracts.liquidation import LiquidationReason

SERVICE_NAME = "execution"
_SUPPORTED_REASONS = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.DAILY_HALT,
    LiquidationReason.ROLLOVER,
}


class PolicyLiquidationDrillError(RuntimeError):
    pass


def _record_request_lifecycle_at(
    request: LiquidationRequestResult,
    *,
    recorded_at_utc: str,
) -> LiquidationRequestResult:
    recorded = format_utc(parse_utc(recorded_at_utc))
    operation = replace(
        request.snapshot.operation,
        created_at_utc=recorded,
        updated_at_utc=recorded,
    )
    snapshot = replace(request.snapshot, operation=operation)
    readiness = replace(
        request.execution_readiness,
        updated_at_utc=recorded,
    )
    return replace(
        request,
        snapshot=snapshot,
        execution_readiness=readiness,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate one explicit DAILY_FLAT, DAILY_HALT or ROLLOVER "
            "condition at a "
            "logical UTC time and persist only that durable liquidation trigger. "
            "This paper-drill entrypoint is broker-free."
        )
    )
    parser.add_argument("--validate-store-only", action="store_true")
    parser.add_argument("--prepare-position-episode-id", default=None)
    parser.add_argument(
        "--reason",
        choices=tuple(item.value for item in sorted(_SUPPORTED_REASONS, key=lambda x: x.value)),
        default=None,
    )
    parser.add_argument("--observed-at-utc", default=None)
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=ROOT / "catalog",
    )
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument(
        "--allow-unqualified-session",
        action="store_true",
        help=(
            "paper-drill override for DAILY_FLAT when the committed session "
            "calendar is not production-qualified"
        ),
    )
    return parser


def _database(arguments: argparse.Namespace, settings) -> Path:
    return (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )


def _require_paper_gate(settings) -> None:
    if settings.environment != "paper":
        raise PolicyLiquidationDrillError(
            "policy liquidation drill requires IBMD_ENVIRONMENT=paper"
        )
    if not settings.ib_account_id.upper().startswith("D"):
        raise PolicyLiquidationDrillError(
            "configured account does not look like an IB paper account"
        )
    if "paper-drill" not in settings.deployment_id.lower():
        raise PolicyLiquidationDrillError(
            "policy liquidation drill requires deployment_id containing "
            "'paper-drill'"
        )


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    database = _database(arguments, settings)
    source = SQLiteLiquidationTriggerReader(database)
    repository = SQLiteLiquidationStore(database)
    source.validate_schema()
    repository.validate_schema()
    if arguments.validate_store_only:
        print(
            "policy liquidation drill dependencies are compatible: "
            f"execution={database}, catalog={arguments.catalog_root.resolve()}"
        )
        return 0

    _require_paper_gate(settings)
    episode_id = str(arguments.prepare_position_episode_id or "").strip()
    reason_text = str(arguments.reason or "").strip()
    observed_text = str(arguments.observed_at_utc or "").strip()
    if not episode_id:
        raise ValueError("--prepare-position-episode-id is required")
    if not reason_text:
        raise ValueError("--reason is required")
    if not observed_text:
        raise ValueError("--observed-at-utc is required")
    reason = LiquidationReason(reason_text)
    if reason not in _SUPPORTED_REASONS:
        raise PolicyLiquidationDrillError(
            f"unsupported policy liquidation drill reason: {reason.value}"
        )
    observed = format_utc(parse_utc(observed_text))
    bundle = load_catalog_bundle(
        arguments.catalog_root.resolve(),
        require_production_sessions=False,
    )
    instrument_id = str(arguments.instrument or "").strip()
    if not instrument_id:
        raise ValueError("--instrument is required")
    policy = LiquidationTriggerProducerPolicyV1(
        account_id=settings.ib_account_id,
        strategy_id=bundle.strategy_policy.strategy_id,
        strategy_version=bundle.strategy_policy.strategy_version,
        deployment_id=settings.deployment_id,
        instrument_id=instrument_id,
        require_production_session=(
            not bool(arguments.allow_unqualified_session)
        ),
    )
    episodes = source.list_open_episodes(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
    )
    matching = [
        item for item in episodes if item.position_episode_id == episode_id
    ]
    if len(matching) != 1:
        raise PolicyLiquidationDrillError(
            "policy liquidation drill requires exactly one matching OPEN "
            f"position episode: requested={episode_id}, matches={len(matching)}"
        )
    episode = matching[0]
    protection = source.read_protection_by_episode(episode_id)
    position = source.read_position(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
    )
    readiness = source.read_readiness(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
    )
    daily_risk = source.read_latest_daily_risk(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
    )
    if protection is None or position is None or readiness is None:
        raise PolicyLiquidationDrillError(
            "policy liquidation drill source state is incomplete"
        )
    existing = repository.read_snapshot_by_episode(episode_id)
    if existing is not None:
        raise PolicyLiquidationDrillError(
            "policy liquidation acceptance requires a fresh episode without an "
            "existing liquidation operation"
        )
    candidates, blockers = evaluate_liquidation_trigger_candidates(
        bundle=bundle,
        producer_policy=policy,
        episode=episode,
        protection=protection,
        daily_risk=daily_risk,
        existing=None,
        observed_at_utc=observed,
    )
    selected = [item for item in candidates if item.reason == reason]
    if len(selected) != 1:
        raise PolicyLiquidationDrillError(
            "requested policy trigger was not produced exactly once: "
            f"reason={reason.value}, candidates="
            f"{[item.reason.value for item in candidates]}, blockers={blockers}"
        )
    candidate = selected[0]
    request = request_liquidation(
        episode=episode,
        position=position,
        readiness=readiness,
        reason=candidate.reason,
        source_ref=candidate.source_ref,
        observed_at_utc=observed,
        existing=None,
    )
    recorded_at = format_utc(utc_now())
    request = _record_request_lifecycle_at(
        request,
        recorded_at_utc=recorded_at,
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=new_id("instance"),
    ):
        persisted = repository.publish_request(
            current=None,
            result=request,
        )
    payload = {
        "position_episode_id": episode_id,
        "observed_at_utc": observed,
        "operation_recorded_at_utc": recorded_at,
        "logical_trigger_time_decoupled": True,
        "selected_reason": candidate.reason.value,
        "selected_source_ref": candidate.source_ref,
        "selected_detail": candidate.detail,
        "all_candidates": [
            {
                "reason": item.reason.value,
                "source_ref": item.source_ref,
                "detail": item.detail,
            }
            for item in candidates
        ],
        "blocked_reasons": list(blockers),
        "liquidation_operation": persisted.operation.to_dict(),
        "liquidation_trigger": request.trigger.to_dict(),
        "operation_created": request.operation_created,
        "trigger_created": request.trigger_created,
        "broker_mutations_performed": False,
        "automatic_retry_enabled": False,
        "development_session_override": bool(
            arguments.allow_unqualified_session
        ),
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(
        arguments.prepare_position_episode_id is not None
    )
    if selected != 1:
        print(
            "policy liquidation drill requires exactly one mode: "
            "--validate-store-only or --prepare-position-episode-id",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        CatalogError,
        ConfigurationError,
        LiquidationDomainError,
        LiquidationSchemaError,
        LiquidationStoreError,
        LiquidationTriggerProducerError,
        LiquidationTriggerReadError,
        PolicyLiquidationDrillError,
        ValueError,
    ) as exc:
        print(
            "policy liquidation drill preparation failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
