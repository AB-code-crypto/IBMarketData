from __future__ import annotations

import math
from dataclasses import dataclass

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)

POSITION_PROJECTION_REASON_PREFIX = "position_projection:"


class PositionProjectionError(ValueError):
    pass


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise PositionProjectionError(f"{field_name} is required")
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise PositionProjectionError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise PositionProjectionError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise PositionProjectionError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PositionProjectionError(f"{field_name} must be a boolean")
    return value


@dataclass(frozen=True)
class RegisteredFuturesContractV1:
    con_id: int
    local_symbol: str
    contract_is_active: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "con_id",
            _positive_int(self.con_id, field_name="con_id"),
        )
        object.__setattr__(
            self,
            "local_symbol",
            _required_text(self.local_symbol, field_name="local_symbol"),
        )
        object.__setattr__(
            self,
            "contract_is_active",
            _strict_bool(
                self.contract_is_active,
                field_name="contract_is_active",
            ),
        )


@dataclass(frozen=True)
class PositionProjectionPolicyV1:
    account_id: str
    strategy_id: str
    deployment_id: str
    instrument_id: str
    max_snapshot_age_seconds: float

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        maximum = float(self.max_snapshot_age_seconds)
        if not math.isfinite(maximum) or maximum <= 0.0:
            raise PositionProjectionError(
                "max_snapshot_age_seconds must be finite and positive"
            )
        object.__setattr__(self, "max_snapshot_age_seconds", maximum)


@dataclass(frozen=True)
class PositionProjectionResult:
    position: StrategyPositionV1
    blocking_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.position, StrategyPositionV1):
            raise PositionProjectionError(
                "position must be StrategyPositionV1"
            )
        reasons = tuple(str(item).strip() for item in self.blocking_reasons)
        if any(not item for item in reasons):
            raise PositionProjectionError(
                "blocking reasons cannot contain empty values"
            )
        if len(reasons) != len(set(reasons)):
            raise PositionProjectionError(
                "blocking reasons must be unique"
            )
        object.__setattr__(self, "blocking_reasons", reasons)


def _scope(position: StrategyPositionV1) -> tuple[str, str, str, str]:
    return (
        position.account_id,
        position.strategy_id,
        position.deployment_id,
        position.instrument_id,
    )


def _expected_scope(
    policy: PositionProjectionPolicyV1,
) -> tuple[str, str, str, str]:
    return (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
        policy.instrument_id,
    )


def _validate_previous_scope(
    previous: StrategyPositionV1 | None,
    policy: PositionProjectionPolicyV1,
) -> None:
    if previous is not None and _scope(previous) != _expected_scope(policy):
        raise PositionProjectionError(
            "previous strategy position belongs to another execution scope"
        )


def _contract_signature(
    contracts: tuple[PositionContractV1, ...],
) -> tuple[tuple[int, str, int], ...]:
    return tuple(
        sorted(
            (
                item.con_id,
                item.local_symbol,
                item.signed_quantity,
            )
            for item in contracts
        )
    )


def _previous_proves_ownership(
    previous: StrategyPositionV1 | None,
    contracts: tuple[PositionContractV1, ...],
    *,
    side: StrategyPositionSide,
    quantity: int,
) -> bool:
    if previous is None or previous.position_episode_id is None:
        return False
    if previous.side != side or previous.quantity != quantity:
        return False
    return _contract_signature(previous.contracts) == _contract_signature(
        contracts
    )


def _with_current_active_flags(
    contracts: tuple[PositionContractV1, ...],
    registry: tuple[RegisteredFuturesContractV1, ...],
) -> tuple[PositionContractV1, ...]:
    by_con_id = {item.con_id: item for item in registry}
    values: list[PositionContractV1] = []
    for item in contracts:
        registered = by_con_id.get(item.con_id)
        values.append(
            PositionContractV1(
                con_id=item.con_id,
                local_symbol=item.local_symbol,
                signed_quantity=item.signed_quantity,
                contract_is_active=(
                    False
                    if registered is None
                    else registered.contract_is_active
                ),
            )
        )
    return tuple(values)


def _stale_position(
    *,
    policy: PositionProjectionPolicyV1,
    previous: StrategyPositionV1 | None,
    registry: tuple[RegisteredFuturesContractV1, ...],
    observed_at_utc: str,
    broker_snapshot_id: str | None,
    freshness_seconds: float | None,
) -> StrategyPositionV1:
    if previous is not None and previous.position_episode_id is not None:
        return StrategyPositionV1(
            account_id=policy.account_id,
            strategy_id=policy.strategy_id,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            position_episode_id=previous.position_episode_id,
            side=previous.side,
            quantity=previous.quantity,
            contracts=_with_current_active_flags(
                previous.contracts,
                registry,
            ),
            projection_status=StrategyPositionStatus.STALE,
            broker_snapshot_id=(
                broker_snapshot_id
                if broker_snapshot_id is not None
                else previous.broker_snapshot_id
            ),
            updated_at_utc=observed_at_utc,
            source_freshness_seconds=freshness_seconds,
        )
    return StrategyPositionV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        position_episode_id=None,
        side=StrategyPositionSide.UNKNOWN,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.STALE,
        broker_snapshot_id=broker_snapshot_id,
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=freshness_seconds,
    )


def _unknown_position(
    *,
    policy: PositionProjectionPolicyV1,
    observed_at_utc: str,
    broker_snapshot_id: str | None,
    freshness_seconds: float | None,
    contracts: tuple[PositionContractV1, ...] = (),
) -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        position_episode_id=None,
        side=StrategyPositionSide.UNKNOWN,
        quantity=0,
        contracts=contracts,
        projection_status=StrategyPositionStatus.UNKNOWN,
        broker_snapshot_id=broker_snapshot_id,
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=freshness_seconds,
    )


def _recognized_contracts(
    *,
    snapshot: BrokerPositionSnapshotV1,
    policy: PositionProjectionPolicyV1,
    registry: tuple[RegisteredFuturesContractV1, ...],
) -> tuple[tuple[PositionContractV1, ...], tuple[str, ...]]:
    by_con_id = {item.con_id: item for item in registry}
    by_symbol = {item.local_symbol: item for item in registry}
    if len(by_con_id) != len(registry) or len(by_symbol) != len(registry):
        raise PositionProjectionError(
            "registered futures contracts must have unique identities"
        )

    recognized: list[PositionContractV1] = []
    reasons: set[str] = set()
    instrument = policy.instrument_id.upper()

    for row in snapshot.rows:
        local_symbol = str(row.local_symbol or "").strip()
        symbol = str(row.symbol or "").strip().upper()
        registered_by_con = by_con_id.get(row.con_id)
        registered_by_symbol = by_symbol.get(local_symbol)
        candidate = (
            symbol == instrument
            or registered_by_con is not None
            or registered_by_symbol is not None
        )
        if not candidate:
            continue
        if str(row.sec_type or "").strip().upper() != "FUT":
            reasons.add(
                f"{POSITION_PROJECTION_REASON_PREFIX}contract_identity_unknown"
            )
            continue
        if (
            symbol != instrument
            or registered_by_con is None
            or registered_by_symbol is None
            or registered_by_con != registered_by_symbol
        ):
            reasons.add(
                f"{POSITION_PROJECTION_REASON_PREFIX}contract_identity_unknown"
            )
            continue

        raw_quantity = float(row.signed_quantity)
        rounded = round(raw_quantity)
        if (
            not math.isfinite(raw_quantity)
            or abs(raw_quantity - rounded) > 1e-9
            or rounded == 0
        ):
            reasons.add(
                f"{POSITION_PROJECTION_REASON_PREFIX}quantity_not_integral"
            )
            continue

        recognized.append(
            PositionContractV1(
                con_id=registered_by_con.con_id,
                local_symbol=registered_by_con.local_symbol,
                signed_quantity=int(rounded),
                contract_is_active=registered_by_con.contract_is_active,
            )
        )

    identities = [
        (item.con_id, item.local_symbol)
        for item in recognized
    ]
    if len(identities) != len(set(identities)):
        reasons.add(
            f"{POSITION_PROJECTION_REASON_PREFIX}duplicate_contract_row"
        )

    return (
        tuple(
            sorted(
                recognized,
                key=lambda item: (item.con_id, item.local_symbol),
            )
        ),
        tuple(sorted(reasons)),
    )


def project_strategy_position(
    *,
    snapshot: BrokerPositionSnapshotV1 | None,
    previous: StrategyPositionV1 | None,
    policy: PositionProjectionPolicyV1,
    registry: tuple[RegisteredFuturesContractV1, ...],
    observed_at_utc: str,
    active_contract_available: bool,
) -> PositionProjectionResult:
    if not isinstance(policy, PositionProjectionPolicyV1):
        raise PositionProjectionError(
            "policy must be PositionProjectionPolicyV1"
        )
    if any(
        not isinstance(item, RegisteredFuturesContractV1)
        for item in registry
    ):
        raise PositionProjectionError(
            "registry must contain RegisteredFuturesContractV1 values"
        )
    if not registry:
        raise PositionProjectionError(
            "registered futures contract set cannot be empty"
        )
    active_count = sum(1 for item in registry if item.contract_is_active)
    if active_count > 1:
        raise PositionProjectionError(
            "registered contract set contains multiple active contracts"
        )
    _validate_previous_scope(previous, policy)

    observed = parse_utc(observed_at_utc)
    observed_text = format_utc(observed)
    active_available = _strict_bool(
        active_contract_available,
        field_name="active_contract_available",
    )
    if active_available != (active_count == 1):
        raise PositionProjectionError(
            "active_contract_available disagrees with registered contract flags"
        )

    if snapshot is None:
        position = _stale_position(
            policy=policy,
            previous=previous,
            registry=registry,
            observed_at_utc=observed_text,
            broker_snapshot_id=None,
            freshness_seconds=None,
        )
        return PositionProjectionResult(
            position=position,
            blocking_reasons=(
                f"{POSITION_PROJECTION_REASON_PREFIX}snapshot_missing",
            ),
        )

    if not isinstance(snapshot, BrokerPositionSnapshotV1):
        raise PositionProjectionError(
            "snapshot must be BrokerPositionSnapshotV1 or None"
        )
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise PositionProjectionError(
            "strategy position projector requires a COMPLETE broker snapshot"
        )
    if snapshot.account_id != policy.account_id:
        return PositionProjectionResult(
            position=_unknown_position(
                policy=policy,
                observed_at_utc=observed_text,
                broker_snapshot_id=snapshot.snapshot_id,
                freshness_seconds=None,
            ),
            blocking_reasons=(
                f"{POSITION_PROJECTION_REASON_PREFIX}snapshot_account_mismatch",
            ),
        )

    captured = parse_utc(snapshot.captured_at_utc)
    age = max(0.0, (observed - captured).total_seconds())
    if age > policy.max_snapshot_age_seconds:
        return PositionProjectionResult(
            position=_stale_position(
                policy=policy,
                previous=previous,
                registry=registry,
                observed_at_utc=observed_text,
                broker_snapshot_id=snapshot.snapshot_id,
                freshness_seconds=age,
            ),
            blocking_reasons=(
                f"{POSITION_PROJECTION_REASON_PREFIX}snapshot_stale",
            ),
        )

    contracts, identity_reasons = _recognized_contracts(
        snapshot=snapshot,
        policy=policy,
        registry=registry,
    )
    reasons = set(identity_reasons)
    if not active_available:
        reasons.add(
            f"{POSITION_PROJECTION_REASON_PREFIX}active_contract_unavailable"
        )

    if identity_reasons:
        return PositionProjectionResult(
            position=_unknown_position(
                policy=policy,
                observed_at_utc=observed_text,
                broker_snapshot_id=snapshot.snapshot_id,
                freshness_seconds=age,
                contracts=contracts,
            ),
            blocking_reasons=tuple(sorted(reasons)),
        )

    if not contracts:
        return PositionProjectionResult(
            position=StrategyPositionV1(
                account_id=policy.account_id,
                strategy_id=policy.strategy_id,
                deployment_id=policy.deployment_id,
                instrument_id=policy.instrument_id,
                position_episode_id=None,
                side=StrategyPositionSide.FLAT,
                quantity=0,
                contracts=(),
                projection_status=StrategyPositionStatus.FLAT,
                broker_snapshot_id=snapshot.snapshot_id,
                updated_at_utc=observed_text,
                source_freshness_seconds=age,
            ),
            blocking_reasons=tuple(sorted(reasons)),
        )

    if len(contracts) > 1:
        reasons.add(
            f"{POSITION_PROJECTION_REASON_PREFIX}multi_contract_incident"
        )
        return PositionProjectionResult(
            position=StrategyPositionV1(
                account_id=policy.account_id,
                strategy_id=policy.strategy_id,
                deployment_id=policy.deployment_id,
                instrument_id=policy.instrument_id,
                position_episode_id=None,
                side=StrategyPositionSide.UNKNOWN,
                quantity=0,
                contracts=contracts,
                projection_status=(
                    StrategyPositionStatus.MULTI_CONTRACT_INCIDENT
                ),
                broker_snapshot_id=snapshot.snapshot_id,
                updated_at_utc=observed_text,
                source_freshness_seconds=age,
            ),
            blocking_reasons=tuple(sorted(reasons)),
        )

    held = contracts[0]
    side = (
        StrategyPositionSide.LONG
        if held.signed_quantity > 0
        else StrategyPositionSide.SHORT
    )
    quantity = abs(held.signed_quantity)
    if _previous_proves_ownership(
        previous,
        contracts,
        side=side,
        quantity=quantity,
    ):
        if not held.contract_is_active:
            reasons.add(
                f"{POSITION_PROJECTION_REASON_PREFIX}held_contract_not_active"
            )
        return PositionProjectionResult(
            position=StrategyPositionV1(
                account_id=policy.account_id,
                strategy_id=policy.strategy_id,
                deployment_id=policy.deployment_id,
                instrument_id=policy.instrument_id,
                position_episode_id=previous.position_episode_id,
                side=side,
                quantity=quantity,
                contracts=contracts,
                projection_status=StrategyPositionStatus.OPEN,
                broker_snapshot_id=snapshot.snapshot_id,
                updated_at_utc=observed_text,
                source_freshness_seconds=age,
            ),
            blocking_reasons=tuple(sorted(reasons)),
        )

    reasons.add(
        f"{POSITION_PROJECTION_REASON_PREFIX}ownership_unproven"
    )
    if not held.contract_is_active:
        reasons.add(
            f"{POSITION_PROJECTION_REASON_PREFIX}held_contract_not_active"
        )
    return PositionProjectionResult(
        position=StrategyPositionV1(
            account_id=policy.account_id,
            strategy_id=policy.strategy_id,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            position_episode_id=None,
            side=side,
            quantity=quantity,
            contracts=contracts,
            projection_status=StrategyPositionStatus.OWNERSHIP_UNPROVEN,
            broker_snapshot_id=snapshot.snapshot_id,
            updated_at_utc=observed_text,
            source_freshness_seconds=age,
        ),
        blocking_reasons=tuple(sorted(reasons)),
    )


def merge_position_projection_readiness(
    *,
    previous: ExecutionReadinessV1 | None,
    projection: PositionProjectionResult,
    policy: PositionProjectionPolicyV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    if not isinstance(projection, PositionProjectionResult):
        raise PositionProjectionError(
            "projection must be PositionProjectionResult"
        )
    observed_text = format_utc(parse_utc(observed_at_utc))

    if previous is None:
        command_intake = False
        broker_actions = False
        reconciliation_complete = False
        clock_healthy = False
        previous_status = ExecutionReadinessStatus.NOT_READY
        base_reasons = ["execution_control_state_missing"]
    else:
        expected = (
            policy.account_id,
            policy.strategy_id,
            policy.deployment_id,
            policy.instrument_id,
        )
        actual = (
            previous.account_id,
            previous.strategy_id,
            previous.deployment_id,
            previous.instrument_id,
        )
        if actual != expected:
            raise PositionProjectionError(
                "previous execution readiness belongs to another scope"
            )
        command_intake = previous.command_intake_enabled
        broker_actions = previous.broker_actions_enabled
        reconciliation_complete = previous.reconciliation_complete
        clock_healthy = previous.clock_healthy
        previous_status = previous.status
        base_reasons = [
            item
            for item in previous.blocking_reasons
            if not item.startswith(POSITION_PROJECTION_REASON_PREFIX)
        ]

    if not command_intake:
        base_reasons.append("execution_control:command_intake_disabled")
    if not reconciliation_complete:
        base_reasons.append("execution_control:reconciliation_incomplete")
    if not clock_healthy:
        base_reasons.append("execution_control:clock_unhealthy")

    control_reasons = tuple(dict.fromkeys(base_reasons))
    projection_reasons = projection.blocking_reasons
    all_reasons = tuple(
        dict.fromkeys((*control_reasons, *projection_reasons))
    )

    if projection_reasons:
        status = ExecutionReadinessStatus.BLOCKED
    elif (
        previous_status == ExecutionReadinessStatus.BLOCKED
        and control_reasons
    ):
        status = ExecutionReadinessStatus.BLOCKED
    elif (
        command_intake
        and reconciliation_complete
        and clock_healthy
        and not control_reasons
    ):
        status = ExecutionReadinessStatus.READY
        all_reasons = ()
    else:
        status = ExecutionReadinessStatus.NOT_READY

    return ExecutionReadinessV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        status=status,
        command_intake_enabled=command_intake,
        broker_actions_enabled=broker_actions,
        reconciliation_complete=reconciliation_complete,
        clock_healthy=clock_healthy,
        blocking_reasons=all_reasons,
        updated_at_utc=observed_text,
    )
