from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.foundation.time import format_utc, utc_now
from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactSink
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePolicyV1,
    PaperLiquidationAcceptanceStateSource,
)
from ibmd.operations.paper_policy_liquidation_acceptance import (
    PaperPolicyLiquidationAcceptanceResultV1,
    PaperPolicyLiquidationAcceptanceRunner,
)
from ibmd.public_contracts.liquidation import LiquidationReason


@dataclass(frozen=True)
class PaperDailyHaltAcceptanceResultV1:
    drill_id: str
    synthetic_trigger: Mapping[str, Any]
    policy_liquidation: PaperPolicyLiquidationAcceptanceResultV1
    final_daily_risk_state: Mapping[str, Any]
    final_execution_readiness: Mapping[str, Any]
    daily_risk_finalization_invocations: int
    artifact_directory: str

    def to_dict(self) -> dict[str, Any]:
        liquidation = self.policy_liquidation.to_dict()
        return {
            "schema_name": "PaperDailyHaltAcceptanceResult",
            "schema_version": 1,
            "drill_id": self.drill_id,
            "scenario": "DAILY_HALT",
            "synthetic_trigger": dict(self.synthetic_trigger),
            "synthetic_market_mark_only": True,
            "real_owned_fill_evidence_only": True,
            "position_episode_id": liquidation["position_episode_id"],
            "liquidation_operation_id": liquidation[
                "liquidation_operation_id"
            ],
            "liquidation_attempt_id": liquidation[
                "liquidation_attempt_id"
            ],
            "trigger_id": liquidation["trigger_id"],
            "trigger_source_ref": liquidation["trigger_source_ref"],
            "broker_mutation_count": liquidation[
                "broker_mutation_count"
            ],
            "liquidation_state": liquidation["state"],
            "flat_proof": liquidation["flat_proof"],
            "final_daily_risk_state": dict(self.final_daily_risk_state),
            "final_execution_readiness": dict(
                self.final_execution_readiness
            ),
            "daily_risk_finalization_invocations": (
                self.daily_risk_finalization_invocations
            ),
            "daily_halt_sticky": True,
            "cleanup_status_complete": True,
            "command_intake_enabled": False,
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "automatic_retry_enabled": False,
            "artifact_directory": self.artifact_directory,
            "acceptance_scope": (
                "daily-risk sticky state, DAILY_HALT trigger and real paper "
                "liquidation integration; live market PnL pricing remains a "
                "separate acceptance"
            ),
        }


class PaperDailyHaltAcceptanceRunner(
    PaperPolicyLiquidationAcceptanceRunner
):
    def __init__(
        self,
        *,
        policy: PaperLiquidationAcceptancePolicyV1,
        drill_id: str,
        market_database: str | Path,
        command_executor,
        state_source: PaperLiquidationAcceptanceStateSource,
        artifacts: PaperAcceptanceArtifactSink,
        daily_risk_max_invocations: int = 5,
        daily_risk_poll_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        logical_time = format_utc(clock())
        super().__init__(
            policy=policy,
            scenario=LiquidationReason.DAILY_HALT,
            logical_trigger_at_utc=logical_time,
            allow_unqualified_session=False,
            command_executor=command_executor,
            state_source=state_source,
            artifacts=artifacts,
            clock=clock,
            sleeper=sleeper,
        )
        self.drill_id = str(drill_id or "").strip()
        self.market_database = Path(market_database).resolve()
        self.daily_risk_max_invocations = int(daily_risk_max_invocations)
        self.daily_risk_poll_seconds = float(daily_risk_poll_seconds)
        if not self.drill_id:
            raise PaperLiquidationAcceptanceError(
                "daily-halt acceptance drill_id is required",
                stage="configuration",
            )
        if self.daily_risk_max_invocations <= 0:
            raise PaperLiquidationAcceptanceError(
                "daily_risk_max_invocations must be positive",
                stage="configuration",
            )
        if self.daily_risk_poll_seconds < 0.0:
            raise PaperLiquidationAcceptanceError(
                "daily_risk_poll_seconds must be non-negative",
                stage="configuration",
            )

    def _synthetic_trigger_arguments(
        self,
        position_episode_id: str,
    ) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--prepare-position-episode-id",
            position_episode_id,
            "--drill-id",
            self.drill_id,
            "--observed-at-utc",
            self.logical_trigger_at_utc,
            "--execution-database",
            str(paths.execution_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
        )

    def _prepare_synthetic_trigger(
        self,
        position_episode_id: str,
    ) -> Mapping[str, Any]:
        script = (
            self.policy.paths.repo_root
            / "apps"
            / "prepare_execution_daily_halt_paper_drill_v2.py"
        )
        payload = self.command_executor.run_json(
            step_name="daily-halt-synthetic-trigger",
            script=script,
            arguments=self._synthetic_trigger_arguments(
                position_episode_id
            ),
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "daily-halt trigger preparation unexpectedly mutated broker "
                "state",
                stage="daily-halt-trigger",
                broker_exposure_possible=True,
            )
        if payload.get("synthetic_market_mark_only") is not True:
            raise PaperLiquidationAcceptanceError(
                "daily-halt trigger does not declare its synthetic market mark",
                stage="daily-halt-trigger",
                broker_exposure_possible=True,
            )
        if payload.get("real_owned_fill_evidence_only") is not True:
            raise PaperLiquidationAcceptanceError(
                "daily-halt trigger does not prove real owned fill evidence",
                stage="daily-halt-trigger",
                broker_exposure_possible=True,
            )
        state = self._mapping(
            payload.get("daily_risk_state"),
            field_name="daily_risk_state",
            stage="daily-halt-trigger",
            broker_exposure_possible=True,
        )
        readiness = self._mapping(
            payload.get("execution_readiness"),
            field_name="execution_readiness",
            stage="daily-halt-trigger",
            broker_exposure_possible=True,
        )
        calculation = self._mapping(
            payload.get("triggered_calculation"),
            field_name="triggered_calculation",
            stage="daily-halt-trigger",
            broker_exposure_possible=True,
        )
        if (
            state.get("status") != "TRIGGERED"
            or state.get("cleanup_status") != "PENDING"
            or state.get("pnl_ready") is not True
            or float(state.get("total_pnl") or 0.0)
            < float(state.get("target_pnl") or 0.0)
            or readiness.get("status") != "BLOCKED"
            or readiness.get("command_intake_enabled") is not False
            or readiness.get("broker_actions_enabled") is not True
            or calculation.get("open_position_episode_id")
            != position_episode_id
        ):
            raise PaperLiquidationAcceptanceError(
                "daily-halt synthetic trigger did not persist the expected "
                "TRIGGERED/PENDING fail-closed state",
                stage="daily-halt-trigger",
                broker_exposure_possible=True,
            )
        self.artifacts.write_json("daily-halt-trigger", payload)
        return payload

    def _daily_risk_arguments(self) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--once",
            "--execution-database",
            str(paths.execution_database),
            "--market-database",
            str(self.market_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
        )

    def _finalize_halted_state(
        self,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any], int]:
        script = (
            self.policy.paths.repo_root
            / "apps"
            / "run_execution_daily_risk_v2.py"
        )
        final_state = None
        final_readiness = None
        invocations = 0
        for index in range(1, self.daily_risk_max_invocations + 1):
            payload = self.command_executor.run_json(
                step_name=f"daily-risk-finalize-{index:02d}",
                script=script,
                arguments=self._daily_risk_arguments(),
            )
            invocations += 1
            if payload.get("broker_mutations_performed") is not False:
                raise PaperLiquidationAcceptanceError(
                    "daily-risk finalization unexpectedly mutated broker state",
                    stage="daily-risk-finalization",
                    broker_exposure_possible=True,
                )
            state = self._mapping(
                payload.get("daily_risk_state"),
                field_name="daily_risk_state",
                stage="daily-risk-finalization",
                broker_exposure_possible=True,
            )
            readiness = self._mapping(
                payload.get("execution_readiness"),
                field_name="execution_readiness",
                stage="daily-risk-finalization",
                broker_exposure_possible=True,
            )
            final_state = state
            final_readiness = readiness
            if (
                state.get("status") == "HALTED"
                and state.get("cleanup_status") == "COMPLETE"
                and readiness.get("status") == "BLOCKED"
                and readiness.get("command_intake_enabled") is False
            ):
                break
            if (
                state.get("status") not in {"TRIGGERED", "CLOSING", "HALTED"}
                or readiness.get("command_intake_enabled") is True
            ):
                raise PaperLiquidationAcceptanceError(
                    "daily-risk sticky halt was lost after liquidation: "
                    f"state={state.get('status')}, "
                    f"cleanup={state.get('cleanup_status')}",
                    stage="daily-risk-finalization",
                    broker_exposure_possible=True,
                )
            if index < self.daily_risk_max_invocations and self.daily_risk_poll_seconds:
                self.sleeper(self.daily_risk_poll_seconds)
        if final_state is None or final_readiness is None:
            raise PaperLiquidationAcceptanceError(
                "daily-risk finalization produced no state",
                stage="daily-risk-finalization",
                broker_exposure_possible=True,
            )
        if (
            final_state.get("status") != "HALTED"
            or final_state.get("cleanup_status") != "COMPLETE"
            or final_readiness.get("status") != "BLOCKED"
            or final_readiness.get("command_intake_enabled") is not False
        ):
            raise PaperLiquidationAcceptanceError(
                "daily-risk state did not reach HALTED/COMPLETE after FLAT",
                stage="daily-risk-finalization",
                broker_exposure_possible=True,
            )
        repeat = self.command_executor.run_json(
            step_name="daily-risk-halted-idempotency",
            script=script,
            arguments=self._daily_risk_arguments(),
        )
        invocations += 1
        if repeat.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "HALTED idempotency calculation mutated broker state",
                stage="daily-risk-idempotency",
                broker_exposure_possible=True,
            )
        repeated_state = self._mapping(
            repeat.get("daily_risk_state"),
            field_name="daily_risk_state",
            stage="daily-risk-idempotency",
            broker_exposure_possible=True,
        )
        repeated_readiness = self._mapping(
            repeat.get("execution_readiness"),
            field_name="execution_readiness",
            stage="daily-risk-idempotency",
            broker_exposure_possible=True,
        )
        if (
            repeated_state.get("status") != "HALTED"
            or repeated_state.get("cleanup_status") != "COMPLETE"
            or repeated_readiness.get("status") != "BLOCKED"
            or repeated_readiness.get("command_intake_enabled") is not False
        ):
            raise PaperLiquidationAcceptanceError(
                "daily-risk HALTED state was not sticky on repeat",
                stage="daily-risk-idempotency",
                broker_exposure_possible=True,
            )
        return repeated_state, repeated_readiness, invocations

    def run(self) -> PaperDailyHaltAcceptanceResultV1:
        _source_drill_id, position_episode_id = self._load_entry_summary()
        trigger = self._prepare_synthetic_trigger(position_episode_id)
        liquidation = super().run()
        final_state, final_readiness, invocations = (
            self._finalize_halted_state()
        )
        result = PaperDailyHaltAcceptanceResultV1(
            drill_id=self.drill_id,
            synthetic_trigger=trigger,
            policy_liquidation=liquidation,
            final_daily_risk_state=final_state,
            final_execution_readiness=final_readiness,
            daily_risk_finalization_invocations=invocations,
            artifact_directory=str(self.artifacts.directory),
        )
        self.artifacts.write_json("summary", result.to_dict())
        return result


__all__ = [
    "PaperDailyHaltAcceptanceResultV1",
    "PaperDailyHaltAcceptanceRunner",
]
