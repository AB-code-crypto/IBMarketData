from __future__ import annotations

from dataclasses import dataclass, replace

from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.protective_submission import (
    ProtectiveOrderReconciliationResult,
    reconcile_protective_order_snapshot,
)
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerObservationOutcome
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
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
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderV1,
)


class ProtectiveLifecycleError(ValueError):
    pass


@dataclass(frozen=True)
class ProtectiveLifecyclePolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    position_max_age_seconds: float = 10.0

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise ProtectiveLifecycleError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        version = int(self.strategy_version)
        if version <= 0:
            raise ProtectiveLifecycleError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        max_age = float(self.position_max_age_seconds)
        if max_age <= 0.0:
            raise ProtectiveLifecycleError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)


@dataclass(frozen=True)
class ProtectiveOrderLifecycleEvidence:
    kind: ProtectiveOrderKind
    result: ProtectiveOrderReconciliationResult

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ProtectiveOrderKind):
            raise ProtectiveLifecycleError("kind must be ProtectiveOrderKind")
        if not isinstance(self.result, ProtectiveOrderReconciliationResult):
            raise ProtectiveLifecycleError(
                "result must be ProtectiveOrderReconciliationResult"
            )


@dataclass(frozen=True)
class ProtectiveLifecycleUpdate:
    episode: PositionEpisodeV1
    protection: ProtectionStateV1
    strategy_position: StrategyPositionV1
    execution_readiness: ExecutionReadinessV1
    evidence: tuple[ProtectiveOrderLifecycleEvidence, ...]
    broker_position_state: str
    episode_closed: bool
    commission_complete: bool | None

    def __post_init__(self) -> None:
        if self.broker_position_state not in {"OPEN", "FLAT", "INCIDENT"}:
            raise ProtectiveLifecycleError(
                f"invalid broker_position_state: {self.broker_position_state!r}"
            )
        if not isinstance(self.episode_closed, bool):
            raise ProtectiveLifecycleError("episode_closed must be boolean")
        if self.commission_complete is not None and not isinstance(
            self.commission_complete,
            bool,
        ):
            raise ProtectiveLifecycleError(
                "commission_complete must be boolean or None"
            )


@dataclass(frozen=True)
class _BrokerPositionProof:
    state: str
    snapshot_id: str
    freshness_seconds: float
    reason: str | None


_EXPOSED_ORDER_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.LIVE,
    ProtectiveOrderState.CANCEL_REQUESTED,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}
_TERMINAL_ORDER_STATES = {
    ProtectiveOrderState.FILLED,
    ProtectiveOrderState.CANCELLED,
    ProtectiveOrderState.REJECTED,
    ProtectiveOrderState.FAILED,
    ProtectiveOrderState.NOT_REQUIRED,
}
_SAFE_SIBLING_STATES = {
    ProtectiveOrderState.CANCELLED,
    ProtectiveOrderState.REJECTED,
    ProtectiveOrderState.FAILED,
    ProtectiveOrderState.NOT_REQUIRED,
}
_TERMINAL_OUTCOMES = {
    ProtectiveOrderState.FILLED: BrokerObservationOutcome.FILLED,
    ProtectiveOrderState.CANCELLED: BrokerObservationOutcome.CANCELLED,
    ProtectiveOrderState.REJECTED: BrokerObservationOutcome.REJECTED,
    ProtectiveOrderState.FAILED: BrokerObservationOutcome.FAILED,
}


def _scope(value: PositionEpisodeV1) -> tuple[str, str, int, str, str]:
    return (
        value.account_id,
        value.strategy_id,
        value.strategy_version,
        value.deployment_id,
        value.instrument_id,
    )


def _validate_scope(
    *,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    position: StrategyPositionV1,
    readiness: ExecutionReadinessV1,
    policy: ProtectiveLifecyclePolicyV1,
) -> None:
    expected = (
        policy.account_id,
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )
    if _scope(episode) != expected:
        raise ProtectiveLifecycleError(
            "position episode belongs to another lifecycle scope"
        )
    protection_scope = (
        protection.account_id,
        protection.strategy_id,
        protection.strategy_version,
        protection.deployment_id,
        protection.instrument_id,
    )
    if protection_scope != expected:
        raise ProtectiveLifecycleError(
            "protection state belongs to another lifecycle scope"
        )
    state_scope = (
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
    expected_state_scope = (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
        policy.instrument_id,
    )
    if state_scope != expected_state_scope or readiness_scope != expected_state_scope:
        raise ProtectiveLifecycleError(
            "execution position/readiness belongs to another lifecycle scope"
        )
    if protection.position_episode_id != episode.position_episode_id:
        raise ProtectiveLifecycleError(
            "protection state belongs to another position episode"
        )
    if episode.status == PositionEpisodeStatus.OPEN and (
        position.position_episode_id != episode.position_episode_id
        or position.projection_status
        not in {StrategyPositionStatus.OPEN, StrategyPositionStatus.UNKNOWN}
    ):
        raise ProtectiveLifecycleError(
            "open episode is not represented by current execution position"
        )


def _position_proof(
    *,
    snapshot: BrokerPositionSnapshotV1,
    episode: PositionEpisodeV1,
    observed_at_utc: str,
    max_age_seconds: float,
) -> _BrokerPositionProof:
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise ProtectiveLifecycleError(
            "broker position snapshot is not COMPLETE"
        )
    if snapshot.account_id != episode.account_id:
        raise ProtectiveLifecycleError(
            "broker position snapshot account differs from position episode"
        )
    freshness = snapshot.freshness(
        observed_at_utc=observed_at_utc,
        max_age_seconds=max_age_seconds,
    )
    if not freshness.is_fresh:
        raise ProtectiveLifecycleError(
            "broker position snapshot is stale for protective lifecycle: "
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
    nonzero = [row for row in relevant if abs(float(row.signed_quantity)) > 1e-9]
    if not nonzero:
        return _BrokerPositionProof(
            state="FLAT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            reason=None,
        )
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
        len(nonzero) == 1
        and len(exact) == 1
        and exact[0].sec_type == "FUT"
        and abs(float(exact[0].signed_quantity) - expected_signed) <= 1e-9
    ):
        return _BrokerPositionProof(
            state="OPEN",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            reason=None,
        )
    summary = [
        (
            row.con_id,
            row.local_symbol,
            row.signed_quantity,
            row.sec_type,
        )
        for row in nonzero
    ]
    return _BrokerPositionProof(
        state="INCIDENT",
        snapshot_id=snapshot.snapshot_id,
        freshness_seconds=freshness.age_seconds,
        reason=f"broker_position_differs_from_episode:{summary}",
    )


def _order_by_kind(
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
) -> ProtectiveOrderV1 | None:
    values = [item for item in protection.orders if item.kind == kind]
    if not values:
        return None
    if len(values) != 1:
        raise ProtectiveLifecycleError(
            f"protection contains duplicate {kind.value} orders"
        )
    return values[0]


def _not_required(
    order: ProtectiveOrderV1,
    *,
    observed_at_utc: str,
    reason: str,
) -> ProtectiveOrderV1:
    if order.state != ProtectiveOrderState.PLANNED:
        return order
    return replace(
        order,
        state=ProtectiveOrderState.NOT_REQUIRED,
        updated_at_utc=observed_at_utc,
        terminal_at_utc=observed_at_utc,
        failure_reason=reason,
    )


def _apply_reconciliation(
    *,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    broker_snapshot: BrokerReconciliationSnapshotV1,
) -> tuple[
    ProtectionStateV1,
    tuple[ProtectiveOrderLifecycleEvidence, ...],
    tuple[str, ...],
]:
    working = protection
    evidence: list[ProtectiveOrderLifecycleEvidence] = []
    incidents: list[str] = []
    for kind in (
        ProtectiveOrderKind.STOP_LOSS,
        ProtectiveOrderKind.TAKE_PROFIT,
    ):
        order = _order_by_kind(working, kind)
        if order is None or order.state in {
            ProtectiveOrderState.PLANNED,
            ProtectiveOrderState.NOT_REQUIRED,
        }:
            continue
        result = reconcile_protective_order_snapshot(
            broker_snapshot=broker_snapshot,
            episode=episode,
            protection=working,
            kind=kind,
        )
        evidence.append(ProtectiveOrderLifecycleEvidence(kind=kind, result=result))
        current = _order_by_kind(working, kind)
        if current is None:
            raise ProtectiveLifecycleError("protective order disappeared")
        outcome = result.observation.outcome
        expected_terminal = _TERMINAL_OUTCOMES.get(current.state)
        if current.state in _TERMINAL_ORDER_STATES:
            if outcome == BrokerObservationOutcome.FILLED:
                working = apply_protective_observation(
                    protection=working,
                    kind=kind,
                    observation=result.observation,
                    position_open=True,
                )
            elif expected_terminal is not None and outcome == expected_terminal:
                working = apply_protective_observation(
                    protection=working,
                    kind=kind,
                    observation=result.observation,
                    position_open=True,
                )
            elif outcome == BrokerObservationOutcome.AMBIGUOUS:
                incidents.append(
                    f"terminal_{kind.value.lower()}_broker_fact_ambiguous"
                )
            elif outcome == BrokerObservationOutcome.LIVE:
                incidents.append(
                    f"terminal_{kind.value.lower()}_reappeared_live"
                )
            continue
        if current.state in _EXPOSED_ORDER_STATES:
            working = apply_protective_observation(
                protection=working,
                kind=kind,
                observation=result.observation,
                position_open=True,
            )
    return working, tuple(evidence), tuple(incidents)


def _unknown_position(
    *,
    episode: PositionEpisodeV1,
    proof: _BrokerPositionProof,
    observed_at_utc: str,
) -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=episode.account_id,
        strategy_id=episode.strategy_id,
        deployment_id=episode.deployment_id,
        instrument_id=episode.instrument_id,
        position_episode_id=episode.position_episode_id,
        side=StrategyPositionSide.UNKNOWN,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.UNKNOWN,
        broker_snapshot_id=proof.snapshot_id,
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=proof.freshness_seconds,
    )


def _open_position(
    *,
    current: StrategyPositionV1,
    proof: _BrokerPositionProof,
    observed_at_utc: str,
) -> StrategyPositionV1:
    if current.projection_status != StrategyPositionStatus.OPEN:
        raise ProtectiveLifecycleError(
            "broker proves OPEN but execution position is not OPEN"
        )
    return replace(
        current,
        broker_snapshot_id=proof.snapshot_id,
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=proof.freshness_seconds,
    )


def _flat_position(
    *,
    episode: PositionEpisodeV1,
    proof: _BrokerPositionProof,
    observed_at_utc: str,
) -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=episode.account_id,
        strategy_id=episode.strategy_id,
        deployment_id=episode.deployment_id,
        instrument_id=episode.instrument_id,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=proof.snapshot_id,
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=proof.freshness_seconds,
    )


def _protection_status_from_open_orders(
    protection: ProtectionStateV1,
) -> tuple[ProtectionSetStatus, str | None]:
    stop = protection.stop_order
    tp = protection.take_profit_order
    if stop.state == ProtectiveOrderState.PLANNED:
        return ProtectionSetStatus.PLANNED, "stop_not_submitted"
    if stop.state == ProtectiveOrderState.SUBMITTING:
        return ProtectionSetStatus.STOP_SUBMITTING, "stop_submission_in_progress"
    if stop.state == ProtectiveOrderState.LIVE:
        if tp is None or tp.state in {
            ProtectiveOrderState.LIVE,
            ProtectiveOrderState.CANCELLED,
            ProtectiveOrderState.REJECTED,
            ProtectiveOrderState.FAILED,
            ProtectiveOrderState.NOT_REQUIRED,
        }:
            reason = None
            if tp is not None and tp.state != ProtectiveOrderState.LIVE:
                reason = "take_profit_unavailable_stop_live"
            return ProtectionSetStatus.PROTECTED, reason
        return ProtectionSetStatus.STOP_LIVE, (
            "take_profit_outcome_unresolved_stop_live"
            if tp.state
            in {
                ProtectiveOrderState.SUBMITTING,
                ProtectiveOrderState.UNKNOWN_OUTCOME,
            }
            else "take_profit_not_submitted_stop_live"
        )
    if stop.state == ProtectiveOrderState.FILLED:
        return ProtectionSetStatus.EXITED, None
    return ProtectionSetStatus.UNPROTECTED, (
        stop.failure_reason or f"stop_{stop.state.value.lower()}"
    )


def _readiness(
    current: ExecutionReadinessV1,
    *,
    protection: ProtectionStateV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    other_reasons = tuple(
        item
        for item in current.blocking_reasons
        if not item.startswith("protection:")
    )
    stop_live = protection.stop_order.state == ProtectiveOrderState.LIVE
    tp = protection.take_profit_order
    tp_unresolved = tp is not None and tp.state in {
        ProtectiveOrderState.SUBMITTING,
        ProtectiveOrderState.UNKNOWN_OUTCOME,
    }
    safe_for_new_commands = protection.status == ProtectionSetStatus.CLOSED or (
        stop_live
        and protection.status
        in {ProtectionSetStatus.STOP_LIVE, ProtectionSetStatus.PROTECTED}
        and not tp_unresolved
    )
    if safe_for_new_commands:
        reasons = other_reasons
        if reasons:
            status = ExecutionReadinessStatus.BLOCKED
            intake = False
        elif current.reconciliation_complete and current.clock_healthy:
            status = ExecutionReadinessStatus.READY
            intake = True
        else:
            status = ExecutionReadinessStatus.NOT_READY
            intake = False
    else:
        detail = protection.blocking_reason or protection.status.value.lower()
        reasons = other_reasons + (f"protection:{detail}",)
        status = ExecutionReadinessStatus.BLOCKED
        intake = False
    return ExecutionReadinessV1(
        account_id=current.account_id,
        strategy_id=current.strategy_id,
        deployment_id=current.deployment_id,
        instrument_id=current.instrument_id,
        status=status,
        command_intake_enabled=intake,
        broker_actions_enabled=current.broker_actions_enabled,
        reconciliation_complete=current.reconciliation_complete,
        clock_healthy=current.clock_healthy,
        blocking_reasons=reasons,
        updated_at_utc=observed_at_utc,
    )


def reconcile_protective_lifecycle(
    *,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    strategy_position: StrategyPositionV1,
    execution_readiness: ExecutionReadinessV1,
    broker_snapshot: BrokerReconciliationSnapshotV1,
    position_snapshot: BrokerPositionSnapshotV1,
    policy: ProtectiveLifecyclePolicyV1,
    observed_at_utc: str,
) -> ProtectiveLifecycleUpdate:
    if not isinstance(episode, PositionEpisodeV1):
        raise ProtectiveLifecycleError("episode must be PositionEpisodeV1")
    if not isinstance(protection, ProtectionStateV1):
        raise ProtectiveLifecycleError("protection must be ProtectionStateV1")
    if not isinstance(strategy_position, StrategyPositionV1):
        raise ProtectiveLifecycleError(
            "strategy_position must be StrategyPositionV1"
        )
    if not isinstance(execution_readiness, ExecutionReadinessV1):
        raise ProtectiveLifecycleError(
            "execution_readiness must be ExecutionReadinessV1"
        )
    if not isinstance(broker_snapshot, BrokerReconciliationSnapshotV1):
        raise ProtectiveLifecycleError(
            "broker_snapshot must be BrokerReconciliationSnapshotV1"
        )
    if not broker_snapshot.requests_complete:
        raise ProtectiveLifecycleError(
            "broker reconciliation snapshot is incomplete"
        )
    if broker_snapshot.account_id != episode.account_id:
        raise ProtectiveLifecycleError(
            "broker reconciliation snapshot account mismatch"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    _validate_scope(
        episode=episode,
        protection=protection,
        position=strategy_position,
        readiness=execution_readiness,
        policy=policy,
    )
    proof = _position_proof(
        snapshot=position_snapshot,
        episode=episode,
        observed_at_utc=observed,
        max_age_seconds=policy.position_max_age_seconds,
    )

    working, evidence, reconciliation_incidents = _apply_reconciliation(
        episode=episode,
        protection=protection,
        broker_snapshot=broker_snapshot,
    )
    orders = list(working.orders)
    filled = [item for item in orders if item.state == ProtectiveOrderState.FILLED]
    incident_reasons = list(reconciliation_incidents)
    close_safe = False

    if proof.state == "INCIDENT":
        incident_reasons.append(proof.reason or "broker_position_incident")

    if len(filled) > 1:
        incident_reasons.append("multiple_protective_orders_filled")
    elif len(filled) == 1:
        filled_order = filled[0]
        sibling = next(
            (
                item
                for item in orders
                if item.protective_order_id
                != filled_order.protective_order_id
            ),
            None,
        )
        if sibling is not None and sibling.state == ProtectiveOrderState.PLANNED:
            replacement = _not_required(
                sibling,
                observed_at_utc=observed,
                reason="position_exited_before_sibling_submission",
            )
            orders = [
                replacement
                if item.protective_order_id == sibling.protective_order_id
                else item
                for item in orders
            ]
            sibling = replacement
        if sibling is None or sibling.state in _SAFE_SIBLING_STATES:
            close_safe = True
        else:
            incident_reasons.append(
                "oca_sibling_not_terminal_after_protective_fill:"
                f"{sibling.state.value}"
            )
    elif proof.state == "FLAT":
        unsafe = [
            item
            for item in orders
            if item.state
            in {
                ProtectiveOrderState.SUBMITTING,
                ProtectiveOrderState.LIVE,
                ProtectiveOrderState.CANCEL_REQUESTED,
                ProtectiveOrderState.UNKNOWN_OUTCOME,
                ProtectiveOrderState.FILLED,
            }
        ]
        if unsafe:
            incident_reasons.append(
                "flat_position_with_unresolved_protective_orders:"
                + ",".join(item.state.value for item in unsafe)
            )
        else:
            orders = [
                _not_required(
                    item,
                    observed_at_utc=observed,
                    reason="position_flat_before_protective_submission",
                )
                for item in orders
            ]
            close_safe = True

    if incident_reasons:
        status = ProtectionSetStatus.OPERATOR_REQUIRED
        blocking_reason = ";".join(dict.fromkeys(incident_reasons))
        terminal_at = None
    elif len(filled) == 1 and close_safe:
        if proof.state == "FLAT":
            status = ProtectionSetStatus.CLOSED
            blocking_reason = None
            terminal_at = observed
        else:
            status = ProtectionSetStatus.EXITED
            blocking_reason = "exit_fill_waiting_for_flat_position"
            terminal_at = observed
    elif proof.state == "FLAT" and close_safe:
        status = ProtectionSetStatus.CLOSED
        blocking_reason = None
        terminal_at = observed
    else:
        provisional = ProtectionStateV1(
            protection_set_id=working.protection_set_id,
            position_episode_id=working.position_episode_id,
            account_id=working.account_id,
            strategy_id=working.strategy_id,
            strategy_version=working.strategy_version,
            deployment_id=working.deployment_id,
            instrument_id=working.instrument_id,
            status=working.status,
            orders=tuple(orders),
            created_at_utc=working.created_at_utc,
            updated_at_utc=observed,
            terminal_at_utc=(
                working.terminal_at_utc
                if working.status
                in {ProtectionSetStatus.EXITED, ProtectionSetStatus.CLOSED}
                else None
            ),
            blocking_reason=working.blocking_reason,
        )
        status, blocking_reason = _protection_status_from_open_orders(provisional)
        terminal_at = observed if status == ProtectionSetStatus.EXITED else None

    updated_protection = ProtectionStateV1(
        protection_set_id=working.protection_set_id,
        position_episode_id=working.position_episode_id,
        account_id=working.account_id,
        strategy_id=working.strategy_id,
        strategy_version=working.strategy_version,
        deployment_id=working.deployment_id,
        instrument_id=working.instrument_id,
        status=status,
        orders=tuple(orders),
        created_at_utc=working.created_at_utc,
        updated_at_utc=observed,
        terminal_at_utc=terminal_at,
        blocking_reason=blocking_reason,
    )

    episode_closed = status == ProtectionSetStatus.CLOSED and proof.state == "FLAT"
    updated_episode = episode
    if episode_closed and episode.status != PositionEpisodeStatus.CLOSED:
        updated_episode = replace(
            episode,
            status=PositionEpisodeStatus.CLOSED,
            closed_at_utc=observed,
            closing_operation_id=None,
        )

    if episode_closed:
        updated_position = _flat_position(
            episode=episode,
            proof=proof,
            observed_at_utc=observed,
        )
    elif proof.state == "OPEN":
        updated_position = _open_position(
            current=strategy_position,
            proof=proof,
            observed_at_utc=observed,
        )
    else:
        updated_position = _unknown_position(
            episode=episode,
            proof=proof,
            observed_at_utc=observed,
        )

    updated_readiness = _readiness(
        execution_readiness,
        protection=updated_protection,
        observed_at_utc=observed,
    )
    all_fills = tuple(
        fill
        for item in evidence
        for fill in item.result.fills
    )
    commission_complete = (
        None
        if not all_fills
        else all(fill.commission_complete for fill in all_fills)
    )
    return ProtectiveLifecycleUpdate(
        episode=updated_episode,
        protection=updated_protection,
        strategy_position=updated_position,
        execution_readiness=updated_readiness,
        evidence=evidence,
        broker_position_state=proof.state,
        episode_closed=episode_closed,
        commission_complete=commission_complete,
    )
