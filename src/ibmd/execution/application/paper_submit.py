from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibmd.execution.domain.broker_attempt import (
    BrokerOperationSnapshot,
    apply_broker_observation,
    begin_reconciliation,
    mark_attempt_submitting,
    mark_unknown_outcome,
    plan_broker_operation,
)
from ibmd.execution.domain.ib_reconciliation import (
    BrokerAttemptReconciliationResult,
    reconcile_broker_attempt_snapshot,
)
from ibmd.execution.domain.position_projection import RegisteredFuturesContractV1
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.ib_gateway.paper_orders import (
    PaperMarketOrderRequest,
    PaperOrderGateway,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
)
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerObservationOutcome,
    BrokerOperationState,
    BrokerOrderObservationV1,
    BrokerOrderOperationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.decision import StrategyCommandRequestV1
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionV1,
)


class PaperSubmitError(RuntimeError):
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

    def read_latest_daily_risk(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None: ...


class BrokerAttemptRepository(Protocol):
    def read_operation_by_command(
        self,
        command_id: str,
    ) -> BrokerOrderOperationV1 | None: ...

    def read_snapshot(
        self,
        operation_id: str,
    ) -> BrokerOperationSnapshot | None: ...

    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]: ...

    def publish_initial(
        self,
        snapshot: BrokerOperationSnapshot,
    ) -> BrokerOperationSnapshot: ...

    def publish_state(
        self,
        snapshot: BrokerOperationSnapshot,
        *,
        observation: BrokerOrderObservationV1 | None = None,
    ) -> BrokerOperationSnapshot: ...


class BrokerReconciliationRepository(Protocol):
    def publish(
        self,
        *,
        current: BrokerOperationSnapshot,
        updated: BrokerOperationSnapshot,
        result: BrokerAttemptReconciliationResult,
    ) -> BrokerOperationSnapshot: ...


class BrokerSnapshotSource(Protocol):
    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...


@dataclass(frozen=True)
class PaperSubmitPolicy:
    account_id: str
    environment: str
    confirmed_paper_account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    policy_hash: str
    daily_risk_timezone: str
    active_contract: RegisteredFuturesContractV1 | None
    order_route: PaperOrderRoute | None
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
            "policy_hash",
            "daily_risk_timezone",
        ):
            text = str(getattr(self, field_name) or "").strip()
            if not text:
                raise PaperSubmitError(f"{field_name} is required")
            object.__setattr__(self, field_name, text)
        object.__setattr__(self, "environment", self.environment.lower())
        version = int(self.strategy_version)
        if version <= 0:
            raise PaperSubmitError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        attempts = int(self.reconciliation_read_attempts)
        if attempts <= 0:
            raise PaperSubmitError(
                "reconciliation_read_attempts must be positive"
            )
        object.__setattr__(self, "reconciliation_read_attempts", attempts)
        poll = float(self.reconciliation_poll_seconds)
        if poll < 0.0:
            raise PaperSubmitError(
                "reconciliation_poll_seconds must be non-negative"
            )
        object.__setattr__(self, "reconciliation_poll_seconds", poll)
        try:
            ZoneInfo(self.daily_risk_timezone)
        except ZoneInfoNotFoundError as exc:
            raise PaperSubmitError(
                f"unknown daily risk timezone: {self.daily_risk_timezone!r}"
            ) from exc
        if self.active_contract is not None and not isinstance(
            self.active_contract,
            RegisteredFuturesContractV1,
        ):
            raise PaperSubmitError(
                "active_contract must be RegisteredFuturesContractV1 or None"
            )
        if self.order_route is not None and not isinstance(
            self.order_route,
            PaperOrderRoute,
        ):
            raise PaperSubmitError(
                "order_route must be PaperOrderRoute or None"
            )
        if (self.active_contract is None) != (self.order_route is None):
            raise PaperSubmitError(
                "active_contract and order_route must be supplied together"
            )
        if self.active_contract is not None and self.order_route is not None:
            if (
                self.active_contract.con_id != self.order_route.con_id
                or self.active_contract.local_symbol
                != self.order_route.local_symbol
                or self.order_route.instrument_id != self.instrument_id
            ):
                raise PaperSubmitError(
                    "active contract and paper order route disagree"
                )


@dataclass(frozen=True)
class PaperSubmitRun:
    before: BrokerOperationSnapshot
    after: BrokerOperationSnapshot
    submission_performed: bool
    receipt: PaperOrderSubmissionReceipt | None
    submission_error: str | None
    reconciliation_result: BrokerAttemptReconciliationResult | None
    broker_snapshot_reads: int


_EXPOSURE_OPERATION_STATES = {
    BrokerOperationState.SUBMITTING,
    BrokerOperationState.LIVE,
    BrokerOperationState.RECONCILING,
    BrokerOperationState.UNKNOWN_OUTCOME,
}
_EXPOSURE_ATTEMPT_STATES = {
    BrokerAttemptState.SUBMITTING,
    BrokerAttemptState.LIVE,
    BrokerAttemptState.UNKNOWN_OUTCOME,
}
_NO_SUBMIT_OPERATION_STATES = {
    BrokerOperationState.SUCCEEDED,
    BrokerOperationState.FAILED_RETRYABLE,
    BrokerOperationState.FAILED_OPERATOR_REQUIRED,
}


def require_paper_submit_gate(policy: PaperSubmitPolicy) -> None:
    if policy.environment != "paper":
        raise PaperSubmitError(
            "broker mutation requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperSubmitError(
            "paper account confirmation does not match configured account: "
            f"configured={policy.account_id}, "
            f"confirmed={policy.confirmed_paper_account_id}"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperSubmitError(
            "configured account does not look like an Interactive Brokers paper "
            f"account: {policy.account_id!r}"
        )


def _scope(policy: PaperSubmitPolicy) -> tuple[str, int, str, str]:
    return (
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )


def _validate_command_pair(
    *,
    request: StrategyCommandRequestV1,
    state: ExecutionCommandStateV1,
    policy: PaperSubmitPolicy,
    observed_at_utc: str,
) -> None:
    if state.state != ExecutionCommandState.ADMITTED:
        raise PaperSubmitError(
            "paper submission requires an ADMITTED execution command: "
            f"state={state.state.value}"
        )
    request_scope = (
        request.strategy_id,
        request.strategy_version,
        request.deployment_id,
        request.instrument_id,
    )
    state_scope = (
        state.strategy_id,
        state.strategy_version,
        state.deployment_id,
        state.instrument_id,
    )
    expected_scope = _scope(policy)
    if request_scope != expected_scope or state_scope != expected_scope:
        raise PaperSubmitError(
            "command scope differs from paper submit policy: "
            f"request={request_scope}, state={state_scope}, expected={expected_scope}"
        )
    if request.command_id != state.command_id:
        raise PaperSubmitError("decision and execution command ids differ")
    if request.policy_hash != policy.policy_hash:
        raise PaperSubmitError("strategy command policy hash is not current")
    material_request = (
        request.command_kind,
        request.desired_target_side,
        request.desired_target_quantity,
    )
    material_state = (
        state.command_kind,
        state.desired_target_side,
        state.desired_target_quantity,
    )
    if material_request != material_state:
        raise PaperSubmitError(
            "decision and execution command material fields differ"
        )
    if parse_utc(observed_at_utc) >= parse_utc(request.expires_at_utc):
        raise PaperSubmitError(
            "strategy command expired before broker submission: "
            f"expires_at={request.expires_at_utc}, observed_at={observed_at_utc}"
        )


def _validate_current_state(
    *,
    position: StrategyPositionV1,
    readiness: ExecutionReadinessV1,
    daily_risk: DailyRiskStateV1,
    policy: PaperSubmitPolicy,
    observed_at_utc: str,
) -> None:
    expected_position_scope = (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
        policy.instrument_id,
    )
    position_scope = (
        position.account_id,
        position.strategy_id,
        position.deployment_id,
        position.instrument_id,
    )
    readiness_scope = (
        readiness.account_id,
        readiness.strategy_id,
        readiness.deployment_id,
        readiness.instrument_id,
    )
    if position_scope != expected_position_scope:
        raise PaperSubmitError(
            "strategy position belongs to another execution scope"
        )
    if readiness_scope != expected_position_scope:
        raise PaperSubmitError(
            "execution readiness belongs to another execution scope"
        )
    risk_scope = (
        daily_risk.account_id,
        daily_risk.strategy_id,
        daily_risk.deployment_id,
    )
    expected_risk_scope = (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
    )
    if risk_scope != expected_risk_scope:
        raise PaperSubmitError("daily risk belongs to another execution scope")
    if (
        readiness.status != ExecutionReadinessStatus.READY
        or not readiness.command_intake_enabled
        or not readiness.broker_actions_enabled
        or not readiness.reconciliation_complete
        or not readiness.clock_healthy
        or readiness.blocking_reasons
    ):
        raise PaperSubmitError(
            "paper submission requires READY execution with broker actions enabled"
        )
    if daily_risk.status != DailyRiskStatus.MONITORING or not daily_risk.pnl_ready:
        raise PaperSubmitError(
            "paper submission requires MONITORING daily risk with ready PnL"
        )
    expected_day = (
        parse_utc(observed_at_utc)
        .astimezone(ZoneInfo(policy.daily_risk_timezone))
        .date()
        .isoformat()
    )
    if daily_risk.trading_day != expected_day:
        raise PaperSubmitError(
            "daily risk belongs to another trading day: "
            f"expected={expected_day}, actual={daily_risk.trading_day}"
        )


def _submission_context_detail(
    result: BrokerAttemptReconciliationResult,
    submission_error: str | None,
) -> BrokerAttemptReconciliationResult:
    if submission_error is None or result.observation.outcome not in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        return result
    observation = result.observation
    detail = "; ".join(
        item
        for item in (
            observation.detail,
            f"submit_call_error={submission_error}",
        )
        if item
    )
    return BrokerAttemptReconciliationResult(
        observation=BrokerOrderObservationV1(
            order_ref=observation.order_ref,
            outcome=observation.outcome,
            observed_at_utc=observation.observed_at_utc,
            broker_order_id=None,
            broker_perm_id=None,
            broker_status=None,
            requested_qty=None,
            filled_qty=None,
            remaining_qty=None,
            detail=detail,
        ),
        fills=result.fills,
        source_session_id=result.source_session_id,
        captured_at_utc=result.captured_at_utc,
    )


class PaperOrderSubmitCoordinator:
    def __init__(
        self,
        *,
        policy: PaperSubmitPolicy,
        command_state_source: CommandStateSource,
        command_request_source: CommandRequestSource,
        execution_state_source: ExecutionStateSource,
        attempt_repository: BrokerAttemptRepository,
        reconciliation_repository: BrokerReconciliationRepository,
        order_gateway: PaperOrderGateway,
        broker_snapshot_source: BrokerSnapshotSource,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.policy = policy
        self.command_state_source = command_state_source
        self.command_request_source = command_request_source
        self.execution_state_source = execution_state_source
        self.attempt_repository = attempt_repository
        self.reconciliation_repository = reconciliation_repository
        self.order_gateway = order_gateway
        self.broker_snapshot_source = broker_snapshot_source
        self.clock = clock

    def _load_command(
        self,
        command_id: str,
    ) -> tuple[ExecutionCommandStateV1, StrategyCommandRequestV1]:
        state = self.command_state_source.read_command_state(command_id)
        if state is None:
            raise PaperSubmitError(
                f"execution command state does not exist: {command_id}"
            )
        request = self.command_request_source.read_command(command_id)
        if request is None:
            raise PaperSubmitError(
                f"strategy command request does not exist: {command_id}"
            )
        return state, request

    def _load_current_state(
        self,
    ) -> tuple[StrategyPositionV1, ExecutionReadinessV1, DailyRiskStateV1]:
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
        daily_risk = self.execution_state_source.read_latest_daily_risk(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        missing = [
            name
            for name, value in (
                ("strategy_position", position),
                ("execution_readiness", readiness),
                ("daily_risk", daily_risk),
            )
            if value is None
        ]
        if missing:
            raise PaperSubmitError(
                f"execution state is incomplete before paper submission: {missing}"
            )
        return position, readiness, daily_risk

    def _existing_snapshot(self, command_id: str) -> BrokerOperationSnapshot | None:
        operation = self.attempt_repository.read_operation_by_command(command_id)
        if operation is None:
            return None
        snapshot = self.attempt_repository.read_snapshot(operation.operation_id)
        if snapshot is None:
            raise PaperSubmitError(
                "broker operation exists without its current attempt: "
                f"{operation.operation_id}"
            )
        return snapshot

    def _assert_no_other_unresolved(self, command_id: str) -> None:
        conflicts = [
            item.operation.operation_id
            for item in self.attempt_repository.read_unresolved()
            if item.operation.command_id != command_id
            and (
                item.operation.account_id,
                item.operation.strategy_id,
                item.operation.deployment_id,
                item.operation.instrument_id,
            )
            == (
                self.policy.account_id,
                self.policy.strategy_id,
                self.policy.deployment_id,
                self.policy.instrument_id,
            )
        ]
        if conflicts:
            raise PaperSubmitError(
                "another unresolved broker operation already owns this execution "
                f"scope: {conflicts}"
            )

    async def _finalize_reconciliation(
        self,
        *,
        current: BrokerOperationSnapshot,
        result: BrokerAttemptReconciliationResult,
    ) -> BrokerOperationSnapshot:
        reconciling = current
        if current.operation.state in {
            BrokerOperationState.SUBMITTING,
            BrokerOperationState.LIVE,
            BrokerOperationState.UNKNOWN_OUTCOME,
        }:
            reconciling = begin_reconciliation(
                current,
                observed_at_utc=result.captured_at_utc,
            )
            reconciling = self.attempt_repository.publish_state(reconciling)
        elif current.operation.state != BrokerOperationState.RECONCILING:
            raise PaperSubmitError(
                "broker operation is not eligible for reconciliation: "
                f"{current.operation.state.value}"
            )
        updated = apply_broker_observation(
            reconciling,
            observation=result.observation,
        )
        return self.reconciliation_repository.publish(
            current=reconciling,
            updated=updated,
            result=result,
        )

    async def _reconcile_exposure(
        self,
        *,
        current: BrokerOperationSnapshot,
        before: BrokerOperationSnapshot,
        submission_performed: bool,
        receipt: PaperOrderSubmissionReceipt | None,
        submission_error: str | None,
    ) -> PaperSubmitRun:
        reads = 0
        last_result: BrokerAttemptReconciliationResult | None = None
        read_errors: list[str] = []

        for index in range(self.policy.reconciliation_read_attempts):
            try:
                broker_snapshot = await self.broker_snapshot_source.read_snapshot(
                    account_id=self.policy.account_id
                )
                reads += 1
                result = reconcile_broker_attempt_snapshot(
                    broker_snapshot=broker_snapshot,
                    current=current,
                )
                result = _submission_context_detail(result, submission_error)
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
                after = await self._finalize_reconciliation(
                    current=current,
                    result=result,
                )
                return PaperSubmitRun(
                    before=before,
                    after=after,
                    submission_performed=submission_performed,
                    receipt=receipt,
                    submission_error=submission_error,
                    reconciliation_result=result,
                    broker_snapshot_reads=reads,
                )
            except Exception as exc:
                read_errors.append(f"{type(exc).__name__}: {exc}")
                if index + 1 < self.policy.reconciliation_read_attempts:
                    if self.policy.reconciliation_poll_seconds:
                        await asyncio.sleep(
                            self.policy.reconciliation_poll_seconds
                        )
                    continue

        reason_parts = [
            "paper_order_outcome_unproven_after_bounded_reconciliation"
        ]
        if submission_error:
            reason_parts.append(f"submit_call_error={submission_error}")
        if last_result is not None and last_result.observation.detail:
            reason_parts.append(last_result.observation.detail)
        if read_errors:
            reason_parts.append("read_errors=" + " | ".join(read_errors))
        unknown = mark_unknown_outcome(
            current,
            observed_at_utc=format_utc(self.clock()),
            reason="; ".join(reason_parts),
        )
        after = self.attempt_repository.publish_state(unknown)
        return PaperSubmitRun(
            before=before,
            after=after,
            submission_performed=submission_performed,
            receipt=receipt,
            submission_error=submission_error,
            reconciliation_result=last_result,
            broker_snapshot_reads=reads,
        )

    async def run_once(self, *, command_id: str) -> PaperSubmitRun:
        require_paper_submit_gate(self.policy)
        command_id = str(command_id or "").strip()
        if not command_id:
            raise PaperSubmitError("command_id is required")
        command_state, command_request = self._load_command(command_id)
        existing = self._existing_snapshot(command_id)

        if existing is not None:
            if existing.operation.state in _NO_SUBMIT_OPERATION_STATES:
                return PaperSubmitRun(
                    before=existing,
                    after=existing,
                    submission_performed=False,
                    receipt=None,
                    submission_error=None,
                    reconciliation_result=None,
                    broker_snapshot_reads=0,
                )
            if (
                existing.operation.state in _EXPOSURE_OPERATION_STATES
                and existing.attempt.state in _EXPOSURE_ATTEMPT_STATES
            ) or (
                existing.operation.state == BrokerOperationState.RECONCILING
                and existing.attempt.state in _EXPOSURE_ATTEMPT_STATES
            ):
                return await self._reconcile_exposure(
                    current=existing,
                    before=existing,
                    submission_performed=False,
                    receipt=None,
                    submission_error=None,
                )
            if not (
                existing.operation.state == BrokerOperationState.PREPARING
                and existing.attempt.state == BrokerAttemptState.PREPARING
            ):
                raise PaperSubmitError(
                    "existing broker operation cannot be submitted automatically: "
                    f"operation={existing.operation.state.value}, "
                    f"attempt={existing.attempt.state.value}"
                )

        observed_at = format_utc(self.clock())
        _validate_command_pair(
            request=command_request,
            state=command_state,
            policy=self.policy,
            observed_at_utc=observed_at,
        )
        position, readiness, daily_risk = self._load_current_state()
        _validate_current_state(
            position=position,
            readiness=readiness,
            daily_risk=daily_risk,
            policy=self.policy,
            observed_at_utc=observed_at,
        )
        if self.policy.active_contract is None or self.policy.order_route is None:
            raise PaperSubmitError(
                "one active registered contract is required for paper submission"
            )
        if not self.policy.active_contract.contract_is_active:
            raise PaperSubmitError("paper submission contract is not active")
        if self.policy.order_route.sec_type != "FUT":
            raise PaperSubmitError(
                "target paper submission currently supports futures only"
            )

        planned = plan_broker_operation(
            command=command_state,
            position=position,
            active_contract=self.policy.active_contract,
            account_id=self.policy.account_id,
            observed_at_utc=(
                observed_at
                if existing is None
                else existing.operation.created_at_utc
            ),
        )
        if existing is None:
            self._assert_no_other_unresolved(command_id)
            planned = self.attempt_repository.publish_initial(planned)
        elif planned != existing:
            raise PaperSubmitError(
                "persisted PREPARING operation no longer matches current command, "
                "position and active contract"
            )

        broker_order_id = await self.order_gateway.allocate_order_id(
            account_id=self.policy.account_id
        )
        submitting_at = format_utc(self.clock())
        if parse_utc(submitting_at) >= parse_utc(command_request.expires_at_utc):
            raise PaperSubmitError(
                "strategy command expired before the SUBMITTING boundary"
            )
        submitting = mark_attempt_submitting(
            planned,
            observed_at_utc=submitting_at,
            broker_order_id=broker_order_id,
        )
        submitting = self.attempt_repository.publish_state(submitting)
        request = PaperMarketOrderRequest(
            account_id=self.policy.account_id,
            broker_order_id=broker_order_id,
            order_ref=submitting.attempt.order_ref,
            side=submitting.attempt.side,
            quantity=submitting.attempt.requested_qty,
            route=self.policy.order_route,
        )

        receipt: PaperOrderSubmissionReceipt | None = None
        submission_error: str | None = None
        try:
            receipt = await self.order_gateway.submit_market_order(request)
        except Exception as exc:
            submission_error = f"{type(exc).__name__}: {exc}"

        return await self._reconcile_exposure(
            current=submitting,
            before=planned,
            submission_performed=True,
            receipt=receipt,
            submission_error=submission_error,
        )


def paper_submit_payload(run: PaperSubmitRun) -> dict:
    result = run.reconciliation_result
    return {
        "command_id": run.after.operation.command_id,
        "operation_id": run.after.operation.operation_id,
        "attempt_id": run.after.attempt.attempt_id,
        "attempt_no": run.after.attempt.attempt_no,
        "order_ref": run.after.attempt.order_ref,
        "broker_order_id": run.after.attempt.broker_order_id,
        "submission_performed": run.submission_performed,
        "submission_receipt": (
            None
            if run.receipt is None
            else {
                "broker_order_id": run.receipt.broker_order_id,
                "order_ref": run.receipt.order_ref,
                "submitted_at_utc": run.receipt.submitted_at_utc,
            }
        ),
        "submission_error": run.submission_error,
        "broker_snapshot_reads": run.broker_snapshot_reads,
        "reconciliation_outcome": (
            None if result is None else result.observation.outcome.value
        ),
        "persisted_exec_ids": (
            [] if result is None else [item.exec_id for item in result.fills]
        ),
        "commission_complete": (
            None if result is None else result.commission_complete
        ),
        "operation_state": run.after.operation.state.value,
        "attempt_state": run.after.attempt.state.value,
        "filled_qty": run.after.operation.filled_qty,
        "remaining_qty": run.after.operation.remaining_qty,
        "automatic_retry_enabled": False,
        "cancel_enabled": False,
        "protective_orders_enabled": False,
        "paper_order_mutation_enabled": run.submission_performed,
    }
