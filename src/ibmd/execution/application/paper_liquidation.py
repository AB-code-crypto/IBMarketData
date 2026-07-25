from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Protocol

from ibmd.execution.domain.liquidation import (
    LiquidationSnapshot,
    assess_next_action,
    apply_close_observation,
    liquidation_readiness,
    mark_broker_flat,
    mark_close_submitting,
    mark_protective_cancel_requested,
    plan_close_attempt,
)
from ibmd.execution.domain.liquidation_completion import (
    LiquidationCompletion,
    complete_liquidation_after_flat,
)
from ibmd.execution.domain.liquidation_exits import (
    LiquidationExitReconciliation,
    reconcile_liquidation_exits,
)
from ibmd.execution.domain.liquidation_position import (
    LiquidationBrokerPositionProof,
    prove_liquidation_broker_position,
)
from ibmd.execution.domain.liquidation_reconciliation import (
    LiquidationReconciliationResult,
    reconcile_liquidation_attempt_snapshot,
)
from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway.paper_cancellations import (
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
    PaperOrderCancellationGateway,
)
from ibmd.ib_gateway.paper_orders import (
    PaperMarketOrderRequest,
    PaperOrderGateway,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
)
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerFillFactV1,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationAttemptState,
    LiquidationNextAction,
    LiquidationOperationState,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)


class PaperLiquidationError(RuntimeError):
    pass


class ProtectionStateSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


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


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class BrokerSnapshotSource(Protocol):
    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...


class PaperLiquidationRepository(Protocol):
    def read_snapshot_by_episode(
        self,
        position_episode_id: str,
    ) -> LiquidationSnapshot | None: ...

    def publish_state(
        self,
        *,
        current: LiquidationSnapshot,
        updated: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        current_protection: ProtectionStateV1 | None = None,
        updated_protection: ProtectionStateV1 | None = None,
        episode: PositionEpisodeV1 | None = None,
        strategy_position: StrategyPositionV1 | None = None,
        observation: BrokerOrderObservationV1 | None = None,
        source_session_id: str | None = None,
        captured_at_utc: str | None = None,
        fills: tuple[BrokerFillFactV1, ...] = (),
    ) -> LiquidationSnapshot: ...


@dataclass(frozen=True)
class PaperLiquidationPolicy:
    account_id: str
    environment: str
    confirmed_paper_account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    order_route: PaperOrderRoute
    position_max_age_seconds: float = 10.0
    reconciliation_read_attempts: int = 5
    reconciliation_poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "environment",
            "confirmed_paper_account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            parsed = str(getattr(self, field_name) or "").strip()
            if not parsed:
                raise PaperLiquidationError(f"{field_name} is required")
            object.__setattr__(self, field_name, parsed)
        object.__setattr__(self, "environment", self.environment.lower())
        version = int(self.strategy_version)
        if version <= 0:
            raise PaperLiquidationError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        if not isinstance(self.order_route, PaperOrderRoute):
            raise PaperLiquidationError("order_route must be PaperOrderRoute")
        if self.order_route.instrument_id != self.instrument_id:
            raise PaperLiquidationError(
                "liquidation route belongs to another instrument"
            )
        max_age = float(self.position_max_age_seconds)
        if max_age <= 0.0:
            raise PaperLiquidationError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)
        attempts = int(self.reconciliation_read_attempts)
        if attempts <= 0:
            raise PaperLiquidationError(
                "reconciliation_read_attempts must be positive"
            )
        object.__setattr__(self, "reconciliation_read_attempts", attempts)
        poll = float(self.reconciliation_poll_seconds)
        if poll < 0.0:
            raise PaperLiquidationError(
                "reconciliation_poll_seconds must be non-negative"
            )
        object.__setattr__(self, "reconciliation_poll_seconds", poll)


@dataclass(frozen=True)
class PaperLiquidationRun:
    before: LiquidationSnapshot
    after: LiquidationSnapshot
    action: LiquidationNextAction
    broker_position_proof: LiquidationBrokerPositionProof
    execution_readiness: ExecutionReadinessV1
    completion: LiquidationCompletion | None
    cancel_receipt: PaperOrderCancelReceipt | None
    submission_receipt: PaperOrderSubmissionReceipt | None
    broker_mutation_performed: bool
    mutation_error: str | None
    broker_snapshot_reads: int
    close_reconciliation_result: LiquidationReconciliationResult | None
    exit_reconciliation: tuple[LiquidationExitReconciliation, ...]


def require_paper_liquidation_gate(policy: PaperLiquidationPolicy) -> None:
    if policy.environment != "paper":
        raise PaperLiquidationError(
            "liquidation broker mutation requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperLiquidationError(
            "paper account confirmation differs from configured account"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperLiquidationError(
            "configured account does not look like an IB paper account"
        )


class PaperLiquidationCoordinator:
    def __init__(
        self,
        *,
        policy: PaperLiquidationPolicy,
        protection_source: ProtectionStateSource,
        execution_state_source: ExecutionStateSource,
        position_snapshot_source: PositionSnapshotSource,
        repository: PaperLiquidationRepository,
        order_gateway: PaperOrderGateway,
        cancellation_gateway: PaperOrderCancellationGateway,
        broker_snapshot_source: BrokerSnapshotSource,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.policy = policy
        self.protection_source = protection_source
        self.execution_state_source = execution_state_source
        self.position_snapshot_source = position_snapshot_source
        self.repository = repository
        self.order_gateway = order_gateway
        self.cancellation_gateway = cancellation_gateway
        self.broker_snapshot_source = broker_snapshot_source
        self.clock = clock

    def _load(
        self,
        position_episode_id: str,
    ) -> tuple[
        PositionEpisodeV1,
        ProtectionStateV1,
        StrategyPositionV1,
        ExecutionReadinessV1,
        LiquidationSnapshot,
    ]:
        episode_id = str(position_episode_id or "").strip()
        if not episode_id:
            raise PaperLiquidationError("position_episode_id is required")
        episode = self.protection_source.read_episode(episode_id)
        protection = self.protection_source.read_protection_by_episode(episode_id)
        if episode is None or protection is None:
            raise PaperLiquidationError(
                "position episode/protection state is missing"
            )
        expected = (
            self.policy.account_id,
            self.policy.strategy_id,
            self.policy.strategy_version,
            self.policy.deployment_id,
            self.policy.instrument_id,
        )
        actual = (
            episode.account_id,
            episode.strategy_id,
            episode.strategy_version,
            episode.deployment_id,
            episode.instrument_id,
        )
        if actual != expected:
            raise PaperLiquidationError(
                "position episode belongs to another liquidation scope"
            )
        route = self.policy.order_route
        if (
            route.con_id != episode.con_id
            or route.local_symbol != episode.local_symbol
            or route.sec_type != "FUT"
        ):
            raise PaperLiquidationError(
                "liquidation route differs from held position episode contract"
            )
        position = self.execution_state_source.read_position(
            account_id=episode.account_id,
            strategy_id=episode.strategy_id,
            deployment_id=episode.deployment_id,
            instrument_id=episode.instrument_id,
        )
        readiness = self.execution_state_source.read_readiness(
            account_id=episode.account_id,
            strategy_id=episode.strategy_id,
            deployment_id=episode.deployment_id,
            instrument_id=episode.instrument_id,
        )
        liquidation = self.repository.read_snapshot_by_episode(episode_id)
        if position is None or readiness is None or liquidation is None:
            raise PaperLiquidationError(
                "execution/liquidation state is incomplete"
            )
        if (
            not readiness.reconciliation_complete
            or not readiness.clock_healthy
        ):
            raise PaperLiquidationError(
                "liquidation recovery requires reconciliation and clock health"
            )
        return episode, protection, position, readiness, liquidation

    def _position_proof(
        self,
        *,
        episode: PositionEpisodeV1,
        observed_at_utc: str,
    ) -> LiquidationBrokerPositionProof:
        snapshot = self.position_snapshot_source.read_latest_complete()
        if snapshot is None:
            raise PaperLiquidationError(
                "no COMPLETE broker position snapshot is available"
            )
        return prove_liquidation_broker_position(
            snapshot=snapshot,
            episode=episode,
            observed_at_utc=observed_at_utc,
            max_age_seconds=self.policy.position_max_age_seconds,
        )

    def _readiness(
        self,
        current: ExecutionReadinessV1,
        liquidation: LiquidationSnapshot,
        observed_at_utc: str,
    ) -> ExecutionReadinessV1:
        return liquidation_readiness(
            current,
            operation=liquidation.operation,
            observed_at_utc=observed_at_utc,
        )

    @staticmethod
    def _require_broker_mutation(
        readiness: ExecutionReadinessV1,
    ) -> None:
        if not readiness.broker_actions_enabled:
            raise PaperLiquidationError(
                "liquidation broker mutation requires broker_actions_enabled=true"
            )

    async def _read_broker_snapshot(self) -> BrokerReconciliationSnapshotV1:
        return await self.broker_snapshot_source.read_snapshot(
            account_id=self.policy.account_id
        )

    async def _reconcile_exits(
        self,
        *,
        episode: PositionEpisodeV1,
        protection: ProtectionStateV1,
        position_open: bool,
        liquidation: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
    ) -> tuple[
        LiquidationSnapshot,
        ProtectionStateV1,
        ExecutionReadinessV1,
        tuple[LiquidationExitReconciliation, ...],
        int,
        list[str],
    ]:
        current_liquidation = liquidation
        current_protection = protection
        current_readiness = readiness
        results: list[LiquidationExitReconciliation] = []
        reads = 0
        errors: list[str] = []
        for index in range(self.policy.reconciliation_read_attempts):
            try:
                broker_snapshot = await self._read_broker_snapshot()
                reads += 1
                result = reconcile_liquidation_exits(
                    episode=episode,
                    protection=current_protection,
                    broker_snapshot=broker_snapshot,
                    position_open=position_open,
                )
                results.append(result)
                if result.protection.to_dict() != current_protection.to_dict():
                    updated_liquidation = assess_next_action(
                        snapshot=current_liquidation,
                        protection=result.protection,
                        broker_position_state=("OPEN" if position_open else "FLAT"),
                        observed_at_utc=broker_snapshot.captured_at_utc,
                    )
                    updated_readiness = self._readiness(
                        current_readiness,
                        updated_liquidation,
                        broker_snapshot.captured_at_utc,
                    )
                    current_liquidation = self.repository.publish_state(
                        current=current_liquidation,
                        updated=updated_liquidation,
                        readiness=updated_readiness,
                        current_protection=current_protection,
                        updated_protection=result.protection,
                    )
                    current_protection = result.protection
                    current_readiness = updated_readiness
                unresolved = [
                    item
                    for item in current_protection.orders
                    if item.state in {
                        ProtectiveOrderState.SUBMITTING,
                        ProtectiveOrderState.CANCEL_REQUESTED,
                        ProtectiveOrderState.UNKNOWN_OUTCOME,
                    }
                ]
                if not unresolved:
                    break
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
            if index + 1 < self.policy.reconciliation_read_attempts:
                if self.policy.reconciliation_poll_seconds:
                    await asyncio.sleep(self.policy.reconciliation_poll_seconds)
        return (
            current_liquidation,
            current_protection,
            current_readiness,
            tuple(results),
            reads,
            errors,
        )

    async def _cancel_one_exit(
        self,
        *,
        episode: PositionEpisodeV1,
        protection: ProtectionStateV1,
        position_open: bool,
        liquidation: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        kind: ProtectiveOrderKind,
        proof: LiquidationBrokerPositionProof,
    ) -> PaperLiquidationRun:
        self._require_broker_mutation(readiness)
        before = liquidation
        order = (
            protection.stop_order
            if kind == ProtectiveOrderKind.STOP_LOSS
            else protection.take_profit_order
        )
        if order is None or order.state != ProtectiveOrderState.LIVE:
            raise PaperLiquidationError(
                f"{kind.value} is not a unique LIVE protective order"
            )
        if order.broker_order_id is None:
            raise PaperLiquidationError(
                f"{kind.value} has no broker order id for cancellation"
            )
        observed = format_utc(self.clock())
        cancel_requested = mark_protective_cancel_requested(
            protection,
            kind=kind,
            observed_at_utc=observed,
        )
        operation = replace(
            liquidation.operation,
            state=LiquidationOperationState.CANCELING_EXITS,
            next_action=LiquidationNextAction.RECONCILE_EXITS,
            updated_at_utc=observed,
            blocking_reason=f"cancel_requested:{kind.value}",
        )
        requested_liquidation = replace(liquidation, operation=operation)
        requested_readiness = self._readiness(
            readiness,
            requested_liquidation,
            observed,
        )
        persisted = self.repository.publish_state(
            current=liquidation,
            updated=requested_liquidation,
            readiness=requested_readiness,
            current_protection=protection,
            updated_protection=cancel_requested,
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
        (
            after,
            after_protection,
            after_readiness,
            exit_results,
            reads,
            read_errors,
        ) = await self._reconcile_exits(
            episode=episode,
            protection=cancel_requested,
            position_open=position_open,
            liquidation=persisted,
            readiness=requested_readiness,
        )
        if read_errors:
            suffix = " | ".join(read_errors)
            mutation_error = (
                suffix
                if mutation_error is None
                else f"{mutation_error}; reconciliation_errors={suffix}"
            )
        assessed = assess_next_action(
            snapshot=after,
            protection=after_protection,
            broker_position_state=proof.state,
            observed_at_utc=format_utc(self.clock()),
        )
        if assessed.operation.to_dict() != after.operation.to_dict():
            assessed_readiness = self._readiness(
                after_readiness,
                assessed,
                assessed.operation.updated_at_utc,
            )
            after = self.repository.publish_state(
                current=after,
                updated=assessed,
                readiness=assessed_readiness,
            )
            after_readiness = assessed_readiness
        return PaperLiquidationRun(
            before=before,
            after=after,
            action=(
                LiquidationNextAction.CANCEL_STOP
                if kind == ProtectiveOrderKind.STOP_LOSS
                else LiquidationNextAction.CANCEL_TAKE_PROFIT
            ),
            broker_position_proof=proof,
            execution_readiness=after_readiness,
            completion=None,
            cancel_receipt=receipt,
            submission_receipt=None,
            broker_mutation_performed=True,
            mutation_error=mutation_error,
            broker_snapshot_reads=reads,
            close_reconciliation_result=None,
            exit_reconciliation=exit_results,
        )

    async def _reconcile_close(
        self,
        *,
        current: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        before: LiquidationSnapshot,
        action: LiquidationNextAction,
        proof: LiquidationBrokerPositionProof,
        submission_receipt: PaperOrderSubmissionReceipt | None,
        submission_error: str | None,
        mutation_performed: bool,
    ) -> PaperLiquidationRun:
        working = current
        working_readiness = readiness
        reads = 0
        last_result = None
        errors: list[str] = []
        for index in range(self.policy.reconciliation_read_attempts):
            try:
                broker_snapshot = await self._read_broker_snapshot()
                reads += 1
                result = reconcile_liquidation_attempt_snapshot(
                    broker_snapshot=broker_snapshot,
                    current=working,
                )
                last_result = result
                if (
                    result.observation.outcome
                    == BrokerObservationOutcome.NOT_FOUND
                    and index + 1 < self.policy.reconciliation_read_attempts
                ):
                    if self.policy.reconciliation_poll_seconds:
                        await asyncio.sleep(
                            self.policy.reconciliation_poll_seconds
                        )
                    continue
                updated = apply_close_observation(
                    working,
                    observation=result.observation,
                )
                updated_readiness = self._readiness(
                    working_readiness,
                    updated,
                    result.captured_at_utc,
                )
                working = self.repository.publish_state(
                    current=working,
                    updated=updated,
                    readiness=updated_readiness,
                    observation=result.observation,
                    source_session_id=result.source_session_id,
                    captured_at_utc=result.captured_at_utc,
                    fills=result.fills,
                )
                working_readiness = updated_readiness
                break
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
                if index + 1 < self.policy.reconciliation_read_attempts:
                    if self.policy.reconciliation_poll_seconds:
                        await asyncio.sleep(
                            self.policy.reconciliation_poll_seconds
                        )
        if last_result is None and errors:
            attempt = working.attempt
            if attempt is None:
                raise PaperLiquidationError(
                    "liquidation reconciliation lost its attempt"
                )
            observed = format_utc(self.clock())
            observation = BrokerOrderObservationV1(
                order_ref=attempt.order_ref,
                outcome=BrokerObservationOutcome.NOT_FOUND,
                observed_at_utc=observed,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                requested_qty=None,
                filled_qty=None,
                remaining_qty=None,
                detail="read_errors=" + " | ".join(errors),
            )
            updated = apply_close_observation(
                working,
                observation=observation,
            )
            updated_readiness = self._readiness(
                working_readiness,
                updated,
                observed,
            )
            working = self.repository.publish_state(
                current=working,
                updated=updated,
                readiness=updated_readiness,
                observation=observation,
                source_session_id="ib_session_00000000000000000000000000000000",
                captured_at_utc=observed,
            )
            working_readiness = updated_readiness
        mutation_error = submission_error
        if errors:
            detail = " | ".join(errors)
            mutation_error = (
                detail
                if mutation_error is None
                else f"{mutation_error}; reconciliation_errors={detail}"
            )
        return PaperLiquidationRun(
            before=before,
            after=working,
            action=action,
            broker_position_proof=proof,
            execution_readiness=working_readiness,
            completion=None,
            cancel_receipt=None,
            submission_receipt=submission_receipt,
            broker_mutation_performed=mutation_performed,
            mutation_error=mutation_error,
            broker_snapshot_reads=reads,
            close_reconciliation_result=last_result,
            exit_reconciliation=(),
        )

    async def _submit_close(
        self,
        *,
        liquidation: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        proof: LiquidationBrokerPositionProof,
    ) -> PaperLiquidationRun:
        self._require_broker_mutation(readiness)
        before = liquidation
        working = liquidation
        working_readiness = readiness
        if working.attempt is None:
            working = plan_close_attempt(
                working,
                broker_quantity=proof.quantity,
                observed_at_utc=format_utc(self.clock()),
            )
            working_readiness = self._readiness(
                readiness,
                working,
                working.operation.updated_at_utc,
            )
            working = self.repository.publish_state(
                current=liquidation,
                updated=working,
                readiness=working_readiness,
            )
        if (
            working.operation.state != LiquidationOperationState.PREPARING
            or working.attempt is None
            or working.attempt.state != LiquidationAttemptState.PREPARING
        ):
            raise PaperLiquidationError(
                "liquidation close is not PREPARING"
            )
        broker_order_id = await self.order_gateway.allocate_order_id(
            account_id=self.policy.account_id
        )
        submitting = mark_close_submitting(
            working,
            broker_order_id=broker_order_id,
            observed_at_utc=format_utc(self.clock()),
        )
        submitting_readiness = self._readiness(
            working_readiness,
            submitting,
            submitting.operation.updated_at_utc,
        )
        submitting = self.repository.publish_state(
            current=working,
            updated=submitting,
            readiness=submitting_readiness,
        )
        attempt = submitting.attempt
        if attempt is None:
            raise PaperLiquidationError(
                "SUBMITTING liquidation attempt is missing"
            )
        receipt = None
        submission_error = None
        try:
            receipt = await self.order_gateway.submit_market_order(
                PaperMarketOrderRequest(
                    account_id=self.policy.account_id,
                    broker_order_id=broker_order_id,
                    order_ref=attempt.order_ref,
                    side=attempt.side,
                    quantity=attempt.requested_qty,
                    route=self.policy.order_route,
                )
            )
        except Exception as exc:
            submission_error = f"{type(exc).__name__}: {exc}"
        return await self._reconcile_close(
            current=submitting,
            readiness=submitting_readiness,
            before=before,
            action=LiquidationNextAction.SUBMIT_MARKET_CLOSE,
            proof=proof,
            submission_receipt=receipt,
            submission_error=submission_error,
            mutation_performed=True,
        )

    def _complete_flat(
        self,
        *,
        episode: PositionEpisodeV1,
        protection: ProtectionStateV1,
        position: StrategyPositionV1,
        readiness: ExecutionReadinessV1,
        liquidation: LiquidationSnapshot,
        proof: LiquidationBrokerPositionProof,
    ) -> PaperLiquidationRun:
        before = liquidation
        terminal = mark_broker_flat(
            liquidation,
            observed_at_utc=format_utc(self.clock()),
        )
        completion = complete_liquidation_after_flat(
            liquidation=terminal,
            episode=episode,
            protection=protection,
            current_position=position,
            current_readiness=readiness,
            position_proof=proof,
            observed_at_utc=terminal.operation.updated_at_utc,
        )
        terminal = self.repository.publish_state(
            current=liquidation,
            updated=terminal,
            readiness=completion.execution_readiness,
            current_protection=protection,
            updated_protection=completion.protection,
            episode=completion.episode,
            strategy_position=completion.strategy_position,
        )
        return PaperLiquidationRun(
            before=before,
            after=terminal,
            action=LiquidationNextAction.WAIT_FOR_FLAT,
            broker_position_proof=proof,
            execution_readiness=completion.execution_readiness,
            completion=completion,
            cancel_receipt=None,
            submission_receipt=None,
            broker_mutation_performed=False,
            mutation_error=None,
            broker_snapshot_reads=0,
            close_reconciliation_result=None,
            exit_reconciliation=(),
        )

    async def run_once(
        self,
        *,
        position_episode_id: str,
    ) -> PaperLiquidationRun:
        require_paper_liquidation_gate(self.policy)
        episode, protection, position, readiness, liquidation = self._load(
            position_episode_id
        )
        before = liquidation
        proof = self._position_proof(
            episode=episode,
            observed_at_utc=format_utc(self.clock()),
        )
        if liquidation.operation.state in {
            LiquidationOperationState.SUCCEEDED,
            LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
            LiquidationOperationState.FAILED_OPERATOR_REQUIRED,
        }:
            return PaperLiquidationRun(
                before=before,
                after=liquidation,
                action=LiquidationNextAction.NONE,
                broker_position_proof=proof,
                execution_readiness=readiness,
                completion=None,
                cancel_receipt=None,
                submission_receipt=None,
                broker_mutation_performed=False,
                mutation_error=None,
                broker_snapshot_reads=0,
                close_reconciliation_result=None,
                exit_reconciliation=(),
            )
        if liquidation.attempt is not None and liquidation.attempt.state in {
            LiquidationAttemptState.SUBMITTING,
            LiquidationAttemptState.LIVE,
            LiquidationAttemptState.UNKNOWN_OUTCOME,
        }:
            return await self._reconcile_close(
                current=liquidation,
                readiness=readiness,
                before=before,
                action=LiquidationNextAction.RECONCILE_MARKET_CLOSE,
                proof=proof,
                submission_receipt=None,
                submission_error=None,
                mutation_performed=False,
            )

        unresolved_protection = any(
            item.state
            in {
                ProtectiveOrderState.SUBMITTING,
                ProtectiveOrderState.LIVE,
                ProtectiveOrderState.CANCEL_REQUESTED,
                ProtectiveOrderState.UNKNOWN_OUTCOME,
            }
            for item in protection.orders
        )
        if proof.state == "FLAT" and not unresolved_protection:
            return self._complete_flat(
                episode=episode,
                protection=protection,
                position=position,
                readiness=readiness,
                liquidation=liquidation,
                proof=proof,
            )

        assessed = assess_next_action(
            snapshot=liquidation,
            protection=protection,
            broker_position_state=proof.state,
            observed_at_utc=format_utc(self.clock()),
        )
        if assessed.operation.to_dict() != liquidation.operation.to_dict():
            assessed_readiness = self._readiness(
                readiness,
                assessed,
                assessed.operation.updated_at_utc,
            )
            assessed = self.repository.publish_state(
                current=liquidation,
                updated=assessed,
                readiness=assessed_readiness,
            )
            liquidation = assessed
            readiness = assessed_readiness

        action = liquidation.operation.next_action
        if action == LiquidationNextAction.RECONCILE_EXITS:
            (
                after,
                after_protection,
                after_readiness,
                results,
                reads,
                errors,
            ) = await self._reconcile_exits(
                episode=episode,
                protection=protection,
                position_open=proof.state == "OPEN",
                liquidation=liquidation,
                readiness=readiness,
            )
            reassessed = assess_next_action(
                snapshot=after,
                protection=after_protection,
                broker_position_state=proof.state,
                observed_at_utc=format_utc(self.clock()),
            )
            if reassessed.operation.to_dict() != after.operation.to_dict():
                reassessed_readiness = self._readiness(
                    after_readiness,
                    reassessed,
                    reassessed.operation.updated_at_utc,
                )
                after = self.repository.publish_state(
                    current=after,
                    updated=reassessed,
                    readiness=reassessed_readiness,
                )
                after_readiness = reassessed_readiness
            return PaperLiquidationRun(
                before=before,
                after=after,
                action=action,
                broker_position_proof=proof,
                execution_readiness=after_readiness,
                completion=None,
                cancel_receipt=None,
                submission_receipt=None,
                broker_mutation_performed=False,
                mutation_error=(" | ".join(errors) or None),
                broker_snapshot_reads=reads,
                close_reconciliation_result=None,
                exit_reconciliation=results,
            )
        if action == LiquidationNextAction.CANCEL_TAKE_PROFIT:
            return await self._cancel_one_exit(
                episode=episode,
                protection=protection,
                position_open=proof.state == "OPEN",
                liquidation=liquidation,
                readiness=readiness,
                kind=ProtectiveOrderKind.TAKE_PROFIT,
                proof=proof,
            )
        if action == LiquidationNextAction.CANCEL_STOP:
            return await self._cancel_one_exit(
                episode=episode,
                protection=protection,
                position_open=proof.state == "OPEN",
                liquidation=liquidation,
                readiness=readiness,
                kind=ProtectiveOrderKind.STOP_LOSS,
                proof=proof,
            )
        if action == LiquidationNextAction.SUBMIT_MARKET_CLOSE:
            if proof.state != "OPEN":
                raise PaperLiquidationError(
                    "MARKET close requires broker-proven OPEN position"
                )
            return await self._submit_close(
                liquidation=liquidation,
                readiness=readiness,
                proof=proof,
            )
        if action == LiquidationNextAction.RECONCILE_MARKET_CLOSE:
            return await self._reconcile_close(
                current=liquidation,
                readiness=readiness,
                before=before,
                action=action,
                proof=proof,
                submission_receipt=None,
                submission_error=None,
                mutation_performed=False,
            )
        if action == LiquidationNextAction.WAIT_FOR_FLAT:
            if proof.state == "FLAT":
                return self._complete_flat(
                    episode=episode,
                    protection=protection,
                    position=position,
                    readiness=readiness,
                    liquidation=liquidation,
                    proof=proof,
                )
        return PaperLiquidationRun(
            before=before,
            after=liquidation,
            action=action,
            broker_position_proof=proof,
            execution_readiness=readiness,
            completion=None,
            cancel_receipt=None,
            submission_receipt=None,
            broker_mutation_performed=False,
            mutation_error=None,
            broker_snapshot_reads=0,
            close_reconciliation_result=None,
            exit_reconciliation=(),
        )


def paper_liquidation_payload(run: PaperLiquidationRun) -> dict:
    return {
        "liquidation_operation": run.after.operation.to_dict(),
        "liquidation_attempt": (
            None if run.after.attempt is None else run.after.attempt.to_dict()
        ),
        "triggers": [item.to_dict() for item in run.after.triggers],
        "action": run.action.value,
        "broker_position_proof": {
            "state": run.broker_position_proof.state,
            "snapshot_id": run.broker_position_proof.snapshot_id,
            "freshness_seconds": run.broker_position_proof.freshness_seconds,
            "quantity": run.broker_position_proof.quantity,
            "side": (
                None
                if run.broker_position_proof.side is None
                else run.broker_position_proof.side.value
            ),
            "reason": run.broker_position_proof.reason,
        },
        "execution_readiness": run.execution_readiness.to_dict(),
        "episode_closed": run.completion is not None,
        "broker_mutation_performed": run.broker_mutation_performed,
        "mutation_error": run.mutation_error,
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
        "submission_receipt": (
            None
            if run.submission_receipt is None
            else {
                "broker_order_id": run.submission_receipt.broker_order_id,
                "order_ref": run.submission_receipt.order_ref,
                "submitted_at_utc": run.submission_receipt.submitted_at_utc,
            }
        ),
        "broker_snapshot_reads": run.broker_snapshot_reads,
        "close_reconciliation": (
            None
            if run.close_reconciliation_result is None
            else {
                "observation": (
                    run.close_reconciliation_result.observation.to_dict()
                ),
                "source_session_id": (
                    run.close_reconciliation_result.source_session_id
                ),
                "captured_at_utc": (
                    run.close_reconciliation_result.captured_at_utc
                ),
                "exec_ids": [
                    item.exec_id
                    for item in run.close_reconciliation_result.fills
                ],
                "commission_complete": (
                    run.close_reconciliation_result.commission_complete
                ),
            }
        ),
        "exit_reconciliation": [
            [
                {
                    "observation": item.observation.to_dict(),
                    "source_session_id": item.source_session_id,
                    "captured_at_utc": item.captured_at_utc,
                    "exec_ids": [fill.exec_id for fill in item.fills],
                }
                for item in result.evidence
            ]
            for result in run.exit_reconciliation
        ],
        "automatic_retry_enabled": False,
        "live_account_enabled": False,
        "legacy_database_compatibility_required": False,
    }
