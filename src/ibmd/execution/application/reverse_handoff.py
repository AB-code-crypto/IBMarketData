from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol

from ibmd.execution.application.protective_lifecycle import (
    ProtectiveLifecycleService,
    ProtectiveLifecycleUpdate,
)
from ibmd.execution.domain.reverse_handoff import (
    ReverseHandoffAction,
    ReverseHandoffAssessmentV1,
    ReverseHandoffError,
    assess_reverse_handoff,
    mark_reverse_cancel_requested,
    reverse_handoff_readiness,
    validate_reverse_handoff_scope,
)
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.ib_gateway.paper_cancellations import (
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
    PaperOrderCancellationGateway,
)
from ibmd.public_contracts.decision import (
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderKind,
)


class PaperReverseHandoffError(RuntimeError):
    pass


class CommandStateSource(Protocol):
    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None: ...


class CommandRequestSource(Protocol):
    def read_command(
        self,
        command_id: str,
    ) -> StrategyCommandRequestV1 | None: ...


class ExecutionStateSource(Protocol):
    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None: ...

    def read_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionReadinessV1 | None: ...


class ProtectionStateSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class ProtectionStateRepository(Protocol):
    def publish_state_and_readiness(
        self,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
        readiness: ExecutionReadinessV1,
    ) -> ProtectionStateV1: ...


class LiquidationStateSource(Protocol):
    def read_snapshot_by_episode(self, position_episode_id: str): ...


@dataclass(frozen=True)
class PaperReverseHandoffPolicyV1:
    account_id: str
    environment: str
    confirmed_paper_account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    policy_hash: str
    position_max_age_seconds: float = 10.0

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "environment",
            "confirmed_paper_account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "policy_hash",
        ):
            parsed = str(getattr(self, field_name) or "").strip()
            if not parsed:
                raise PaperReverseHandoffError(
                    f"{field_name} is required"
                )
            object.__setattr__(self, field_name, parsed)
        object.__setattr__(self, "environment", self.environment.lower())
        version = int(self.strategy_version)
        if version <= 0:
            raise PaperReverseHandoffError(
                "strategy_version must be positive"
            )
        object.__setattr__(self, "strategy_version", version)
        max_age = float(self.position_max_age_seconds)
        if max_age <= 0.0:
            raise PaperReverseHandoffError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)


@dataclass(frozen=True)
class PaperReverseHandoffRunV1:
    command_id: str
    position_episode_id: str
    before_protection: ProtectionStateV1
    after_protection: ProtectionStateV1
    assessment: ReverseHandoffAssessmentV1
    execution_readiness: ExecutionReadinessV1
    lifecycle_before: ProtectiveLifecycleUpdate
    lifecycle_after: ProtectiveLifecycleUpdate | None
    cancel_receipt: PaperOrderCancelReceipt | None
    broker_mutation_performed: bool
    mutation_error: str | None

    @property
    def ready_for_reverse_submit(self) -> bool:
        return self.assessment.action == ReverseHandoffAction.READY_TO_SUBMIT


def require_paper_reverse_handoff_gate(
    policy: PaperReverseHandoffPolicyV1,
) -> None:
    if policy.environment != "paper":
        raise PaperReverseHandoffError(
            "reverse handoff broker mutation requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperReverseHandoffError(
            "paper account confirmation differs from configured account"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperReverseHandoffError(
            "configured account does not look like an IB paper account"
        )


class PaperReverseHandoffCoordinator:
    def __init__(
        self,
        *,
        policy: PaperReverseHandoffPolicyV1,
        command_state_source: CommandStateSource,
        command_request_source: CommandRequestSource,
        execution_state_source: ExecutionStateSource,
        protection_state_source: ProtectionStateSource,
        position_snapshot_source: PositionSnapshotSource,
        protection_repository: ProtectionStateRepository,
        liquidation_state_source: LiquidationStateSource,
        lifecycle_service: ProtectiveLifecycleService,
        cancellation_gateway: PaperOrderCancellationGateway,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.policy = policy
        self.command_state_source = command_state_source
        self.command_request_source = command_request_source
        self.execution_state_source = execution_state_source
        self.protection_state_source = protection_state_source
        self.position_snapshot_source = position_snapshot_source
        self.protection_repository = protection_repository
        self.liquidation_state_source = liquidation_state_source
        self.lifecycle_service = lifecycle_service
        self.cancellation_gateway = cancellation_gateway
        self.clock = clock

    def _scope(self) -> tuple[str, int, str, str]:
        return (
            self.policy.strategy_id,
            self.policy.strategy_version,
            self.policy.deployment_id,
            self.policy.instrument_id,
        )

    def _load_command(
        self,
        command_id: str,
        *,
        observed_at_utc: str,
    ) -> tuple[ExecutionCommandStateV1, StrategyCommandRequestV1]:
        command = self.command_state_source.read_command_state(command_id)
        request = self.command_request_source.read_command(command_id)
        if command is None or request is None:
            raise PaperReverseHandoffError(
                "reverse command state/request is missing"
            )
        if command.state != ExecutionCommandState.ADMITTED:
            raise PaperReverseHandoffError(
                "reverse handoff requires an ADMITTED execution command"
            )
        if command.command_kind != StrategyCommandKind.REVERSE:
            raise PaperReverseHandoffError(
                "reverse handoff requires command_kind=REVERSE"
            )
        request_scope = (
            request.strategy_id,
            request.strategy_version,
            request.deployment_id,
            request.instrument_id,
        )
        command_scope = (
            command.strategy_id,
            command.strategy_version,
            command.deployment_id,
            command.instrument_id,
        )
        if request_scope != self._scope() or command_scope != self._scope():
            raise PaperReverseHandoffError(
                "reverse command belongs to another policy scope"
            )
        if (
            request.command_id != command.command_id
            or request.command_kind != command.command_kind
            or request.desired_target_side != command.desired_target_side
            or request.desired_target_quantity
            != command.desired_target_quantity
        ):
            raise PaperReverseHandoffError(
                "decision and execution reverse command facts differ"
            )
        if request.policy_hash != self.policy.policy_hash:
            raise PaperReverseHandoffError(
                "reverse command policy hash is not current"
            )
        if parse_utc(observed_at_utc) >= parse_utc(request.expires_at_utc):
            raise PaperReverseHandoffError(
                "reverse command expired before protective handoff"
            )
        return command, request

    def _load_state(
        self,
        command: ExecutionCommandStateV1,
    ) -> tuple[
        StrategyPositionV1,
        ExecutionReadinessV1,
        PositionEpisodeV1,
        ProtectionStateV1,
    ]:
        position = self.execution_state_source.read_position(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        readiness = self.execution_state_source.read_readiness(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        if position is None or readiness is None:
            raise PaperReverseHandoffError(
                "execution position/readiness is incomplete"
            )
        if position.position_episode_id is None:
            raise PaperReverseHandoffError(
                "reverse position has no position_episode_id"
            )
        episode = self.protection_state_source.read_episode(
            position.position_episode_id
        )
        protection = self.protection_state_source.read_protection_by_episode(
            position.position_episode_id
        )
        if episode is None or protection is None:
            raise PaperReverseHandoffError(
                "source episode/protection state is missing"
            )
        try:
            validate_reverse_handoff_scope(
                command=command,
                position=position,
                episode=episode,
                protection=protection,
            )
        except ReverseHandoffError as exc:
            raise PaperReverseHandoffError(str(exc)) from exc
        if not readiness.reconciliation_complete or not readiness.clock_healthy:
            raise PaperReverseHandoffError(
                "reverse handoff requires reconciliation and clock health"
            )
        if self.liquidation_state_source.read_snapshot_by_episode(
            episode.position_episode_id
        ) is not None:
            raise PaperReverseHandoffError(
                "position episode already has a liquidation operation; "
                "reverse handoff is forbidden"
            )
        return position, readiness, episode, protection

    def _require_exact_broker_position(
        self,
        *,
        episode: PositionEpisodeV1,
        observed_at_utc: str,
    ) -> None:
        snapshot = self.position_snapshot_source.read_latest_complete()
        if snapshot is None or snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
            raise PaperReverseHandoffError(
                "no COMPLETE broker position snapshot is available"
            )
        if snapshot.account_id != self.policy.account_id:
            raise PaperReverseHandoffError(
                "broker position snapshot account mismatch"
            )
        freshness = snapshot.freshness(
            observed_at_utc=observed_at_utc,
            max_age_seconds=self.policy.position_max_age_seconds,
        )
        if not freshness.is_fresh:
            raise PaperReverseHandoffError(
                "broker position snapshot is stale for reverse handoff: "
                f"age={freshness.age_seconds:.6f}s"
            )
        relevant = [
            row
            for row in snapshot.rows
            if row.symbol.upper() == episode.instrument_id.upper()
            or str(row.local_symbol or "").upper().startswith(
                episode.instrument_id.upper()
            )
        ]
        nonzero = [
            row for row in relevant if abs(float(row.signed_quantity)) > 1e-9
        ]
        exact = [
            row
            for row in nonzero
            if row.con_id == episode.con_id
            and str(row.local_symbol or "") == episode.local_symbol
        ]
        expected_signed = (
            episode.quantity
            if episode.side == StrategyPositionSide.LONG
            else -episode.quantity
        )
        if (
            len(nonzero) != 1
            or len(exact) != 1
            or exact[0].sec_type != "FUT"
            or abs(float(exact[0].signed_quantity) - expected_signed) > 1e-9
        ):
            raise PaperReverseHandoffError(
                "broker position does not exactly prove the source episode"
            )

    def _load_after_lifecycle(
        self,
        command: ExecutionCommandStateV1,
    ) -> tuple[
        StrategyPositionV1,
        ExecutionReadinessV1,
        PositionEpisodeV1,
        ProtectionStateV1,
    ]:
        return self._load_state(command)

    async def run_once(self, *, command_id: str) -> PaperReverseHandoffRunV1:
        require_paper_reverse_handoff_gate(self.policy)
        command_id = str(command_id or "").strip()
        if not command_id:
            raise PaperReverseHandoffError("command_id is required")
        observed = format_utc(self.clock())
        command, _request = self._load_command(
            command_id,
            observed_at_utc=observed,
        )
        _position, readiness, episode, before_protection = self._load_state(
            command
        )
        self._require_exact_broker_position(
            episode=episode,
            observed_at_utc=observed,
        )
        lifecycle_before = await self.lifecycle_service.run_once(
            position_episode_id=episode.position_episode_id,
            observed_at_utc=observed,
        )
        position, readiness, episode, protection = self._load_after_lifecycle(
            command
        )
        if position.projection_status != StrategyPositionStatus.OPEN:
            raise PaperReverseHandoffError(
                "protective lifecycle no longer proves the source position OPEN"
            )
        assessment = assess_reverse_handoff(protection)
        handoff_readiness = reverse_handoff_readiness(
            readiness,
            command_id=command_id,
            assessment=assessment,
            observed_at_utc=format_utc(self.clock()),
        )
        if assessment.action == ReverseHandoffAction.READY_TO_SUBMIT:
            persisted = self.protection_repository.publish_state_and_readiness(
                current=protection,
                updated=protection,
                readiness=handoff_readiness,
            )
            return PaperReverseHandoffRunV1(
                command_id=command_id,
                position_episode_id=episode.position_episode_id,
                before_protection=before_protection,
                after_protection=persisted,
                assessment=assessment,
                execution_readiness=handoff_readiness,
                lifecycle_before=lifecycle_before,
                lifecycle_after=None,
                cancel_receipt=None,
                broker_mutation_performed=False,
                mutation_error=None,
            )
        if assessment.action in {
            ReverseHandoffAction.RECONCILE_EXITS,
            ReverseHandoffAction.OPERATOR_REQUIRED,
        }:
            persisted = self.protection_repository.publish_state_and_readiness(
                current=protection,
                updated=protection,
                readiness=handoff_readiness,
            )
            return PaperReverseHandoffRunV1(
                command_id=command_id,
                position_episode_id=episode.position_episode_id,
                before_protection=before_protection,
                after_protection=persisted,
                assessment=assessment,
                execution_readiness=handoff_readiness,
                lifecycle_before=lifecycle_before,
                lifecycle_after=None,
                cancel_receipt=None,
                broker_mutation_performed=False,
                mutation_error=None,
            )
        if not readiness.broker_actions_enabled:
            raise PaperReverseHandoffError(
                "reverse handoff cancellation requires broker_actions_enabled=true"
            )
        kind = (
            ProtectiveOrderKind.TAKE_PROFIT
            if assessment.action == ReverseHandoffAction.CANCEL_TAKE_PROFIT
            else ProtectiveOrderKind.STOP_LOSS
        )
        order = (
            protection.take_profit_order
            if kind == ProtectiveOrderKind.TAKE_PROFIT
            else protection.stop_order
        )
        if order is None or order.broker_order_id is None:
            raise PaperReverseHandoffError(
                f"{kind.value} has no broker order id for cancellation"
            )
        requested_at = format_utc(self.clock())
        cancel_requested = mark_reverse_cancel_requested(
            protection,
            kind=kind,
            command_id=command_id,
            observed_at_utc=requested_at,
        )
        requested_assessment = assess_reverse_handoff(cancel_requested)
        requested_readiness = reverse_handoff_readiness(
            readiness,
            command_id=command_id,
            assessment=requested_assessment,
            observed_at_utc=requested_at,
        )
        self.protection_repository.publish_state_and_readiness(
            current=protection,
            updated=cancel_requested,
            readiness=requested_readiness,
        )
        receipt = None
        mutation_error = None
        try:
            receipt = await self.cancellation_gateway.cancel_order(
                PaperOrderCancelRequest(
                    account_id=self.policy.account_id,
                    broker_order_id=order.broker_order_id,
                    order_ref=order.order_ref,
                )
            )
        except Exception as exc:
            mutation_error = f"{type(exc).__name__}: {exc}"
        lifecycle_after = None
        try:
            lifecycle_after = await self.lifecycle_service.run_once(
                position_episode_id=episode.position_episode_id,
                observed_at_utc=format_utc(self.clock()),
            )
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            mutation_error = (
                detail
                if mutation_error is None
                else f"{mutation_error}; reconciliation_error={detail}"
            )
        position, final_readiness, episode, final_protection = (
            self._load_after_lifecycle(command)
        )
        final_assessment = assess_reverse_handoff(final_protection)
        final_readiness = reverse_handoff_readiness(
            final_readiness,
            command_id=command_id,
            assessment=final_assessment,
            observed_at_utc=format_utc(self.clock()),
        )
        self.protection_repository.publish_state_and_readiness(
            current=final_protection,
            updated=final_protection,
            readiness=final_readiness,
        )
        return PaperReverseHandoffRunV1(
            command_id=command_id,
            position_episode_id=episode.position_episode_id,
            before_protection=before_protection,
            after_protection=final_protection,
            assessment=final_assessment,
            execution_readiness=final_readiness,
            lifecycle_before=lifecycle_before,
            lifecycle_after=lifecycle_after,
            cancel_receipt=receipt,
            broker_mutation_performed=True,
            mutation_error=mutation_error,
        )


class PersistedReverseSubmitGuard:
    def __init__(
        self,
        *,
        protection_state_source: ProtectionStateSource,
        liquidation_state_source: LiquidationStateSource,
    ) -> None:
        self.protection_state_source = protection_state_source
        self.liquidation_state_source = liquidation_state_source

    def require_ready(
        self,
        *,
        command: ExecutionCommandStateV1,
        position: StrategyPositionV1,
    ) -> None:
        if position.position_episode_id is None:
            raise PaperReverseHandoffError(
                "REVERSE position has no position_episode_id"
            )
        episode = self.protection_state_source.read_episode(
            position.position_episode_id
        )
        protection = self.protection_state_source.read_protection_by_episode(
            position.position_episode_id
        )
        if episode is None or protection is None:
            raise PaperReverseHandoffError(
                "REVERSE source episode/protection state is missing"
            )
        if self.liquidation_state_source.read_snapshot_by_episode(
            position.position_episode_id
        ) is not None:
            raise PaperReverseHandoffError(
                "REVERSE is forbidden because liquidation already owns the episode"
            )
        try:
            from ibmd.execution.domain.reverse_handoff import (
                require_reverse_ready_for_submit,
            )

            require_reverse_ready_for_submit(
                command=command,
                position=position,
                episode=episode,
                protection=protection,
            )
        except ReverseHandoffError as exc:
            raise PaperReverseHandoffError(str(exc)) from exc


def paper_reverse_handoff_payload(
    run: PaperReverseHandoffRunV1,
) -> dict:
    return {
        "command_id": run.command_id,
        "position_episode_id": run.position_episode_id,
        "action": run.assessment.action.value,
        "blocking_reason": run.assessment.blocking_reason,
        "ready_for_reverse_submit": run.ready_for_reverse_submit,
        "protection": run.after_protection.to_dict(),
        "execution_readiness": run.execution_readiness.to_dict(),
        "cancel_receipt": (
            None
            if run.cancel_receipt is None
            else {
                "broker_order_id": run.cancel_receipt.broker_order_id,
                "order_ref": run.cancel_receipt.order_ref,
                "cancel_requested_at_utc": (
                    run.cancel_receipt.cancel_requested_at_utc
                ),
            }
        ),
        "broker_mutation_performed": run.broker_mutation_performed,
        "mutation_error": run.mutation_error,
        "lifecycle_before": {
            "broker_position_state": run.lifecycle_before.broker_position_state,
            "evidence_count": len(run.lifecycle_before.evidence),
        },
        "lifecycle_after": (
            None
            if run.lifecycle_after is None
            else {
                "broker_position_state": (
                    run.lifecycle_after.broker_position_state
                ),
                "evidence_count": len(run.lifecycle_after.evidence),
            }
        ),
        "automatic_retry_enabled": False,
        "market_reverse_submission_enabled": False,
        "legacy_database_compatibility_required": False,
    }
