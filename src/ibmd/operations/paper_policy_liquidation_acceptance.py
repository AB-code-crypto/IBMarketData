from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.foundation.atomic_json import read_json_object
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactSink
from ibmd.operations.paper_liquidation_acceptance import (
    FlatPositionProofV1,
    LiquidationStateObservationV1,
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePolicyV1,
    PaperLiquidationAcceptanceResultV1,
    PaperLiquidationAcceptanceRunner,
    PaperLiquidationAcceptanceStateSource,
)
from ibmd.public_contracts.liquidation import LiquidationReason

_SUPPORTED = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.DAILY_HALT,
    LiquidationReason.ROLLOVER,
}
_SOURCE_SCHEMAS = {
    "PaperAcceptanceResult",
    "PaperRestartAcceptanceResult",
    "PaperReverseAcceptanceResult",
}


@dataclass(frozen=True)
class PaperPolicyLiquidationAcceptanceResultV1:
    scenario: LiquidationReason
    logical_trigger_at_utc: str
    trigger_source_ref: str
    trigger_detail: str
    trigger_id: str
    trigger_candidate_reasons: tuple[str, ...]
    blocked_reasons: tuple[str, ...]
    liquidation: PaperLiquidationAcceptanceResultV1

    def to_dict(self) -> dict[str, Any]:
        base = self.liquidation.to_dict()
        return {
            "schema_name": "PaperPolicyLiquidationAcceptanceResult",
            "schema_version": 1,
            "scenario": self.scenario.value,
            "logical_trigger_at_utc": self.logical_trigger_at_utc,
            "trigger_source_ref": self.trigger_source_ref,
            "trigger_detail": self.trigger_detail,
            "trigger_id": self.trigger_id,
            "trigger_candidate_reasons": list(
                self.trigger_candidate_reasons
            ),
            "blocked_reasons": list(self.blocked_reasons),
            "source_drill_id": base["source_drill_id"],
            "position_episode_id": base["position_episode_id"],
            "liquidation_operation_id": base[
                "liquidation_operation_id"
            ],
            "liquidation_attempt_id": base["liquidation_attempt_id"],
            "order_ref": base["order_ref"],
            "started_at_utc": base["started_at_utc"],
            "finished_at_utc": base["finished_at_utc"],
            "invocation_count": base["invocation_count"],
            "take_profit_cancel_count": base[
                "take_profit_cancel_count"
            ],
            "stop_cancel_count": base["stop_cancel_count"],
            "market_close_submission_count": base[
                "market_close_submission_count"
            ],
            "broker_mutation_count": base["broker_mutation_count"],
            "state": base["state"],
            "flat_proof": base["flat_proof"],
            "artifact_directory": base["artifact_directory"],
            "resumed_existing_operation": base[
                "resumed_existing_operation"
            ],
            "automatic_retry_enabled": False,
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "policy_trigger_proven": True,
            "legacy_database_compatibility_required": False,
        }


class PaperPolicyLiquidationAcceptanceRunner(
    PaperLiquidationAcceptanceRunner
):
    def __init__(
        self,
        *,
        policy: PaperLiquidationAcceptancePolicyV1,
        scenario: LiquidationReason,
        logical_trigger_at_utc: str,
        allow_unqualified_session: bool,
        command_executor,
        state_source: PaperLiquidationAcceptanceStateSource,
        artifacts: PaperAcceptanceArtifactSink,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        super().__init__(
            policy=policy,
            command_executor=command_executor,
            state_source=state_source,
            artifacts=artifacts,
            clock=clock,
            sleeper=sleeper,
        )
        if scenario not in _SUPPORTED:
            raise PaperLiquidationAcceptanceError(
                f"unsupported policy liquidation scenario: {scenario.value}",
                stage="configuration",
            )
        self.scenario = scenario
        self.logical_trigger_at_utc = format_utc(
            parse_utc(logical_trigger_at_utc)
        )
        self.allow_unqualified_session = bool(
            allow_unqualified_session
        )
        self._trigger_payload: Mapping[str, Any] | None = None

    def _load_entry_summary(self) -> tuple[str, str]:
        path = self.policy.paths.entry_summary
        try:
            value = read_json_object(path)
        except Exception as exc:
            raise PaperLiquidationAcceptanceError(
                f"cannot read source acceptance summary {path}: {exc}",
                stage="source-summary",
            ) from exc
        if (
            value.get("schema_name") not in _SOURCE_SCHEMAS
            or int(value.get("schema_version") or 0) != 1
        ):
            raise PaperLiquidationAcceptanceError(
                "source summary is not a supported protected-position result",
                stage="source-summary",
            )
        position_proof = self._mapping(
            value.get("position_proof"),
            field_name="position_proof",
            stage="source-summary",
        )
        protection = self._mapping(
            value.get("protection"),
            field_name="protection",
            stage="source-summary",
        )
        if position_proof.get("accepted") is not True:
            raise PaperLiquidationAcceptanceError(
                "source summary does not prove the broker position",
                stage="source-summary",
            )
        if (
            protection.get("fully_live") is not True
            or protection.get("stop_state") != "LIVE"
            or protection.get("take_profit_state")
            not in {"LIVE", "NOT_REQUIRED"}
        ):
            raise PaperLiquidationAcceptanceError(
                "source summary does not prove live protection",
                stage="source-summary",
            )
        if value.get("live_position_left_protected") is not True:
            raise PaperLiquidationAcceptanceError(
                "source summary does not declare a live protected position",
                stage="source-summary",
            )
        return (
            self._text(
                value.get("drill_id"),
                field_name="drill_id",
                stage="source-summary",
            ),
            self._text(
                value.get("position_episode_id"),
                field_name="position_episode_id",
                stage="source-summary",
            ),
        )

    def _request_arguments(
        self,
        *,
        position_episode_id: str,
        source_drill_id: str,
    ) -> tuple[str, ...]:
        del source_drill_id
        paths = self.policy.paths
        values = [
            "--prepare-position-episode-id",
            position_episode_id,
            "--reason",
            self.scenario.value,
            "--observed-at-utc",
            self.logical_trigger_at_utc,
            "--execution-database",
            str(paths.execution_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
        ]
        if self.allow_unqualified_session:
            values.append("--allow-unqualified-session")
        return tuple(values)

    def _run_json(
        self,
        *,
        step_name: str,
        arguments: Sequence[str],
    ) -> Mapping[str, Any]:
        if step_name != "liquidation-request":
            return super()._run_json(
                step_name=step_name,
                arguments=arguments,
            )
        script = (
            self.policy.paths.repo_root
            / "apps"
            / "prepare_execution_policy_liquidation_paper_drill_v2.py"
        )
        payload = self.command_executor.run_json(
            step_name=step_name,
            script=script,
            arguments=arguments,
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "policy trigger preparation unexpectedly mutated broker state",
                stage="liquidation-request",
            )
        if payload.get("selected_reason") != self.scenario.value:
            raise PaperLiquidationAcceptanceError(
                "policy trigger preparation selected another reason",
                stage="liquidation-request",
            )
        payload_observed = str(payload.get("observed_at_utc") or "").strip()
        try:
            same_observation = (
                parse_utc(payload_observed)
                == parse_utc(self.logical_trigger_at_utc)
            )
        except ValueError:
            same_observation = False
        if not same_observation:
            raise PaperLiquidationAcceptanceError(
                "policy trigger observation time changed",
                stage="liquidation-request",
            )
        if (
            payload.get("operation_created") is not True
            or payload.get("trigger_created") is not True
        ):
            raise PaperLiquidationAcceptanceError(
                "policy liquidation acceptance requires a fresh operation and "
                "trigger",
                stage="liquidation-request",
            )
        trigger = self._mapping(
            payload.get("liquidation_trigger"),
            field_name="liquidation_trigger",
            stage="liquidation-request",
        )
        if trigger.get("reason") != self.scenario.value:
            raise PaperLiquidationAcceptanceError(
                "persisted policy trigger reason differs from the scenario",
                stage="liquidation-request",
            )
        self._trigger_payload = dict(payload)
        return payload

    def run(self) -> PaperPolicyLiquidationAcceptanceResultV1:
        self.artifacts.write_json(
            "policy-scenario",
            {
                "scenario": self.scenario.value,
                "logical_trigger_at_utc": self.logical_trigger_at_utc,
                "allow_unqualified_session": (
                    self.allow_unqualified_session
                ),
                "automatic_retry_enabled": False,
            },
        )
        liquidation = super().run()
        if self._trigger_payload is None:
            raise PaperLiquidationAcceptanceError(
                "policy trigger payload disappeared",
                stage="liquidation-request",
            )
        trigger = self._mapping(
            self._trigger_payload.get("liquidation_trigger"),
            field_name="liquidation_trigger",
            stage="liquidation-request",
        )
        candidates = self._trigger_payload.get("all_candidates")
        if not isinstance(candidates, list):
            raise PaperLiquidationAcceptanceError(
                "policy trigger candidates must be a list",
                stage="liquidation-request",
            )
        result = PaperPolicyLiquidationAcceptanceResultV1(
            scenario=self.scenario,
            logical_trigger_at_utc=self.logical_trigger_at_utc,
            trigger_source_ref=self._text(
                self._trigger_payload.get("selected_source_ref"),
                field_name="selected_source_ref",
                stage="liquidation-request",
            ),
            trigger_detail=self._text(
                self._trigger_payload.get("selected_detail"),
                field_name="selected_detail",
                stage="liquidation-request",
            ),
            trigger_id=self._text(
                trigger.get("trigger_id"),
                field_name="trigger_id",
                stage="liquidation-request",
            ),
            trigger_candidate_reasons=tuple(
                str(item.get("reason"))
                for item in candidates
                if isinstance(item, Mapping)
            ),
            blocked_reasons=tuple(
                str(item)
                for item in self._trigger_payload.get(
                    "blocked_reasons", []
                )
            ),
            liquidation=liquidation,
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result


__all__ = [
    "PaperPolicyLiquidationAcceptanceResultV1",
    "PaperPolicyLiquidationAcceptanceRunner",
]
