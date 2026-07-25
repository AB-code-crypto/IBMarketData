from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.daily_risk import DailyRiskCalculationV1
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationOperationState,
    LiquidationOperationV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
)


class DailyRiskDomainError(ValueError):
    pass


class DailyRiskFillKind(str, Enum):
    STRATEGIC_OPEN = "STRATEGIC_OPEN"
    STRATEGIC_REVERSE = "STRATEGIC_REVERSE"
    PROTECTIVE_EXIT = "PROTECTIVE_EXIT"
    LIQUIDATION_EXIT = "LIQUIDATION_EXIT"


@dataclass(frozen=True)
class DailyRiskOwnedFillV1:
    kind: DailyRiskFillKind
    fill: BrokerFillFactV1

    def __post_init__(self) -> None:
        if not isinstance(self.kind, DailyRiskFillKind):
            raise DailyRiskDomainError(
                "daily-risk fill kind must be DailyRiskFillKind"
            )
        if not isinstance(self.fill, BrokerFillFactV1):
            raise DailyRiskDomainError(
                "daily-risk fill must be BrokerFillFactV1"
            )


@dataclass(frozen=True)
class DailyRiskMarketMarkV1:
    bar_id: str
    instrument_id: str
    con_id: int
    local_symbol: str
    bar_end_utc: str
    mid_price: float
    age_seconds: float

    def __post_init__(self) -> None:
        bar_id = str(self.bar_id or "").strip()
        instrument_id = str(self.instrument_id or "").strip()
        local_symbol = str(self.local_symbol or "").strip()
        if not bar_id or not instrument_id or not local_symbol:
            raise DailyRiskDomainError(
                "market mark bar/instrument/local-symbol values are required"
            )
        con_id = int(self.con_id)
        if con_id <= 0:
            raise DailyRiskDomainError("market mark con_id must be positive")
        try:
            price = float(self.mid_price)
            age = float(self.age_seconds)
        except (TypeError, ValueError) as exc:
            raise DailyRiskDomainError(
                "market mark price/age must be numeric"
            ) from exc
        if not math.isfinite(price) or price <= 0.0:
            raise DailyRiskDomainError(
                "market mark mid_price must be finite and positive"
            )
        if not math.isfinite(age) or age < 0.0:
            raise DailyRiskDomainError(
                "market mark age_seconds must be finite and non-negative"
            )
        object.__setattr__(self, "bar_id", bar_id)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(self, "local_symbol", local_symbol)
        object.__setattr__(
            self,
            "bar_end_utc",
            format_utc(parse_utc(self.bar_end_utc)),
        )
        object.__setattr__(self, "mid_price", price)
        object.__setattr__(self, "age_seconds", age)


@dataclass(frozen=True)
class DailyRiskPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    timezone_name: str
    target_pnl: float
    contract_multiplier: float
    market_max_age_seconds: float

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "timezone_name",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise DailyRiskDomainError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        version = int(self.strategy_version)
        if version <= 0:
            raise DailyRiskDomainError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise DailyRiskDomainError(
                f"unknown daily-risk timezone: {self.timezone_name!r}"
            ) from exc
        for field_name in (
            "target_pnl",
            "contract_multiplier",
            "market_max_age_seconds",
        ):
            try:
                value = float(getattr(self, field_name))
            except (TypeError, ValueError) as exc:
                raise DailyRiskDomainError(
                    f"{field_name} must be numeric"
                ) from exc
            if not math.isfinite(value) or value <= 0.0:
                raise DailyRiskDomainError(
                    f"{field_name} must be finite and positive"
                )
            object.__setattr__(self, field_name, value)


@dataclass(frozen=True)
class DailyRiskUpdateV1:
    calculation: DailyRiskCalculationV1
    state: DailyRiskStateV1
    execution_readiness: ExecutionReadinessV1


def _stable_id(kind: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def _scope(value) -> tuple[str, str, str]:
    return (
        value.account_id,
        value.strategy_id,
        value.deployment_id,
    )


def _calculation_id(payload: dict[str, Any]) -> str:
    return _stable_id("daily_risk_calculation", payload)


def _fill_categories(
    fills: tuple[DailyRiskOwnedFillV1, ...],
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    strategic = tuple(
        item.fill.exec_id
        for item in fills
        if item.kind
        in {
            DailyRiskFillKind.STRATEGIC_OPEN,
            DailyRiskFillKind.STRATEGIC_REVERSE,
        }
    )
    protective = tuple(
        item.fill.exec_id
        for item in fills
        if item.kind == DailyRiskFillKind.PROTECTIVE_EXIT
    )
    liquidation = tuple(
        item.fill.exec_id
        for item in fills
        if item.kind == DailyRiskFillKind.LIQUIDATION_EXIT
    )
    return strategic, protective, liquidation


def _owned_fills_for_day(
    fills: tuple[DailyRiskOwnedFillV1, ...],
    *,
    trading_day: str,
    zone: ZoneInfo,
    policy: DailyRiskPolicyV1,
) -> tuple[DailyRiskOwnedFillV1, ...]:
    selected = []
    seen: set[str] = set()
    for item in fills:
        fill = item.fill
        if fill.exec_id in seen:
            raise DailyRiskDomainError(
                f"owned execution appears more than once: {fill.exec_id}"
            )
        seen.add(fill.exec_id)
        if fill.account_id != policy.account_id:
            raise DailyRiskDomainError(
                f"owned execution account mismatch: {fill.exec_id}"
            )
        local_day = parse_utc(fill.executed_at_utc).astimezone(zone).date().isoformat()
        if local_day == trading_day:
            selected.append(item)
    return tuple(
        sorted(
            selected,
            key=lambda item: (
                parse_utc(item.fill.executed_at_utc),
                item.fill.exec_id,
            ),
        )
    )


def _realized_pnl(
    fills: tuple[DailyRiskOwnedFillV1, ...],
) -> tuple[float | None, tuple[str, ...], str | None]:
    missing = []
    total = 0.0
    for item in fills:
        fill = item.fill
        commission = fill.commission
        if commission is None:
            missing.append(fill.exec_id)
            continue
        if item.kind == DailyRiskFillKind.STRATEGIC_OPEN:
            total -= float(commission.commission)
            continue
        if commission.realized_pnl is None:
            missing.append(fill.exec_id)
            continue
        total += float(commission.realized_pnl)
    if missing:
        return (
            None,
            tuple(missing),
            "commission or realized-PnL evidence is incomplete for owned executions",
        )
    if not math.isfinite(total):
        return None, (), "realized PnL is not finite"
    return total, (), None


def _unrealized_pnl(
    *,
    position: StrategyPositionV1,
    episode: PositionEpisodeV1 | None,
    mark: DailyRiskMarketMarkV1 | None,
    policy: DailyRiskPolicyV1,
) -> tuple[float | None, str | None]:
    if position.projection_status == StrategyPositionStatus.FLAT:
        if position.side != StrategyPositionSide.FLAT or position.quantity != 0:
            return None, "FLAT execution position has inconsistent side/quantity"
        return 0.0, None
    if position.projection_status != StrategyPositionStatus.OPEN:
        return (
            None,
            f"execution position is not usable for PnL: {position.projection_status.value}",
        )
    if episode is None:
        return None, "OPEN execution position has no position episode"
    if episode.status != PositionEpisodeStatus.OPEN:
        return None, "execution position references a non-OPEN position episode"
    if position.position_episode_id != episode.position_episode_id:
        return None, "execution position and position episode identities differ"
    if position.side != episode.side or position.quantity != episode.quantity:
        return None, "execution position and position episode quantities differ"
    if mark is None:
        return None, "OPEN position has no current market mark"
    if mark.instrument_id != policy.instrument_id:
        return None, "market mark belongs to another instrument"
    if mark.con_id != episode.con_id or mark.local_symbol != episode.local_symbol:
        return None, "market mark does not match the held contract"
    if mark.age_seconds > policy.market_max_age_seconds:
        return (
            None,
            "market mark is stale: "
            f"age={mark.age_seconds:.6f}s, max={policy.market_max_age_seconds:.6f}s",
        )
    direction = 1.0 if episode.side == StrategyPositionSide.LONG else -1.0
    value = (
        (mark.mid_price - episode.entry_average_price)
        * direction
        * episode.quantity
        * policy.contract_multiplier
    )
    if not math.isfinite(value):
        return None, "unrealized PnL is not finite"
    return float(value), None


def _not_ready_calculation(
    *,
    policy: DailyRiskPolicyV1,
    trading_day: str,
    observed_at_utc: str,
    reason_code: str,
    reason_detail: str,
    position: StrategyPositionV1,
    mark: DailyRiskMarketMarkV1 | None,
    daily_fills: tuple[DailyRiskOwnedFillV1, ...],
    missing_exec_ids: tuple[str, ...],
    liquidation: LiquidationOperationV1 | None,
) -> DailyRiskCalculationV1:
    strategic, protective, liquidation_execs = _fill_categories(daily_fills)
    open_episode_id = (
        position.position_episode_id
        if position.projection_status == StrategyPositionStatus.OPEN
        else None
    )
    evidence = {
        "scope": (
            policy.account_id,
            policy.strategy_id,
            policy.strategy_version,
            policy.deployment_id,
            policy.instrument_id,
        ),
        "trading_day": trading_day,
        "calculated_at_utc": observed_at_utc,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "position": position.to_dict(),
        "mark": None if mark is None else mark.__dict__,
        "exec_ids": strategic + protective + liquidation_execs,
        "missing_exec_ids": missing_exec_ids,
        "liquidation": None if liquidation is None else liquidation.to_dict(),
    }
    return DailyRiskCalculationV1(
        calculation_id=_calculation_id(evidence),
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        strategy_version=policy.strategy_version,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        trading_day=trading_day,
        timezone_name=policy.timezone_name,
        calculated_at_utc=observed_at_utc,
        pnl_ready=False,
        reason_code=reason_code,
        reason_detail=reason_detail,
        realized_pnl=None,
        unrealized_pnl=None,
        total_pnl=None,
        target_pnl=policy.target_pnl,
        open_position_episode_id=open_episode_id,
        market_bar_id=(None if open_episode_id is None or mark is None else mark.bar_id),
        market_bar_end_utc=(
            None if open_episode_id is None or mark is None else mark.bar_end_utc
        ),
        mark_price=(
            None if open_episode_id is None or mark is None else mark.mid_price
        ),
        strategic_exec_ids=strategic,
        protective_exec_ids=protective,
        liquidation_exec_ids=liquidation_execs,
        missing_commission_exec_ids=missing_exec_ids,
        liquidation_operation_id=(
            None if liquidation is None else liquidation.liquidation_operation_id
        ),
        liquidation_state=(None if liquidation is None else liquidation.state.value),
    )


def calculate_daily_risk(
    *,
    policy: DailyRiskPolicyV1,
    owned_fills: tuple[DailyRiskOwnedFillV1, ...],
    position: StrategyPositionV1,
    episode: PositionEpisodeV1 | None,
    market_mark: DailyRiskMarketMarkV1 | None,
    current_state: DailyRiskStateV1 | None,
    current_readiness: ExecutionReadinessV1,
    liquidation: LiquidationOperationV1 | None,
    observed_at_utc: str,
) -> DailyRiskUpdateV1:
    if not isinstance(policy, DailyRiskPolicyV1):
        raise DailyRiskDomainError("policy must be DailyRiskPolicyV1")
    if not isinstance(position, StrategyPositionV1):
        raise DailyRiskDomainError("position must be StrategyPositionV1")
    if not isinstance(current_readiness, ExecutionReadinessV1):
        raise DailyRiskDomainError(
            "current_readiness must be ExecutionReadinessV1"
        )
    expected_scope = (
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
        current_readiness.account_id,
        current_readiness.strategy_id,
        current_readiness.deployment_id,
        current_readiness.instrument_id,
    )
    if position_scope != expected_scope or readiness_scope != expected_scope:
        raise DailyRiskDomainError(
            "daily-risk position/readiness belongs to another scope"
        )
    if current_state is not None and _scope(current_state) != expected_scope[:3]:
        raise DailyRiskDomainError(
            "current daily-risk state belongs to another scope"
        )
    if liquidation is not None:
        liquidation_scope = (
            liquidation.account_id,
            liquidation.strategy_id,
            liquidation.deployment_id,
            liquidation.instrument_id,
        )
        if liquidation_scope != expected_scope:
            raise DailyRiskDomainError(
                "liquidation operation belongs to another daily-risk scope"
            )
    observed = format_utc(parse_utc(observed_at_utc))
    zone = ZoneInfo(policy.timezone_name)
    trading_day = parse_utc(observed).astimezone(zone).date().isoformat()
    daily_fills = _owned_fills_for_day(
        tuple(owned_fills),
        trading_day=trading_day,
        zone=zone,
        policy=policy,
    )
    realized, missing_exec_ids, realized_error = _realized_pnl(daily_fills)
    unrealized, unrealized_error = _unrealized_pnl(
        position=position,
        episode=episode,
        mark=market_mark,
        policy=policy,
    )
    if realized_error is not None:
        calculation = _not_ready_calculation(
            policy=policy,
            trading_day=trading_day,
            observed_at_utc=observed,
            reason_code="EXECUTION_EVIDENCE_INCOMPLETE",
            reason_detail=realized_error,
            position=position,
            mark=market_mark,
            daily_fills=daily_fills,
            missing_exec_ids=missing_exec_ids,
            liquidation=liquidation,
        )
    elif unrealized_error is not None:
        calculation = _not_ready_calculation(
            policy=policy,
            trading_day=trading_day,
            observed_at_utc=observed,
            reason_code="UNREALIZED_PNL_NOT_READY",
            reason_detail=unrealized_error,
            position=position,
            mark=market_mark,
            daily_fills=daily_fills,
            missing_exec_ids=(),
            liquidation=liquidation,
        )
    else:
        total = float(realized) + float(unrealized)
        strategic, protective, liquidation_execs = _fill_categories(daily_fills)
        evidence = {
            "scope": (
                policy.account_id,
                policy.strategy_id,
                policy.strategy_version,
                policy.deployment_id,
                policy.instrument_id,
            ),
            "trading_day": trading_day,
            "calculated_at_utc": observed,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_pnl": total,
            "position": position.to_dict(),
            "episode": None if episode is None else episode.to_dict(),
            "mark": None if market_mark is None else market_mark.__dict__,
            "exec_ids": strategic + protective + liquidation_execs,
            "liquidation": None if liquidation is None else liquidation.to_dict(),
        }
        calculation = DailyRiskCalculationV1(
            calculation_id=_calculation_id(evidence),
            account_id=policy.account_id,
            strategy_id=policy.strategy_id,
            strategy_version=policy.strategy_version,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            trading_day=trading_day,
            timezone_name=policy.timezone_name,
            calculated_at_utc=observed,
            pnl_ready=True,
            reason_code="READY",
            reason_detail=None,
            realized_pnl=float(realized),
            unrealized_pnl=float(unrealized),
            total_pnl=total,
            target_pnl=policy.target_pnl,
            open_position_episode_id=(
                position.position_episode_id
                if position.projection_status == StrategyPositionStatus.OPEN
                else None
            ),
            market_bar_id=(
                None if market_mark is None else market_mark.bar_id
            ),
            market_bar_end_utc=(
                None if market_mark is None else market_mark.bar_end_utc
            ),
            mark_price=(
                None if market_mark is None else market_mark.mid_price
            ),
            strategic_exec_ids=strategic,
            protective_exec_ids=protective,
            liquidation_exec_ids=liquidation_execs,
            missing_commission_exec_ids=(),
            liquidation_operation_id=(
                None
                if liquidation is None
                else liquidation.liquidation_operation_id
            ),
            liquidation_state=(
                None if liquidation is None else liquidation.state.value
            ),
        )

    state = _next_daily_risk_state(
        policy=policy,
        calculation=calculation,
        position=position,
        current_state=current_state,
        current_readiness=current_readiness,
        liquidation=liquidation,
        observed_at_utc=observed,
    )
    readiness = daily_risk_readiness(
        current_readiness,
        state=state,
        calculation=calculation,
        observed_at_utc=observed,
    )
    return DailyRiskUpdateV1(
        calculation=calculation,
        state=state,
        execution_readiness=readiness,
    )


def _non_daily_blockers(
    readiness: ExecutionReadinessV1,
) -> tuple[str, ...]:
    return tuple(
        item
        for item in readiness.blocking_reasons
        if not item.startswith("daily_risk:")
    )


def _cleanup_failed(liquidation: LiquidationOperationV1 | None) -> bool:
    return (
        liquidation is not None
        and liquidation.state == LiquidationOperationState.FAILED_OPERATOR_REQUIRED
    )


def _cleanup_in_progress(liquidation: LiquidationOperationV1 | None) -> bool:
    return liquidation is not None and liquidation.state not in {
        LiquidationOperationState.SUCCEEDED,
        LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
        LiquidationOperationState.FAILED_OPERATOR_REQUIRED,
    }


def _flat_and_cleanup_safe(
    *,
    position: StrategyPositionV1,
    readiness: ExecutionReadinessV1,
    liquidation: LiquidationOperationV1 | None,
) -> bool:
    if position.projection_status != StrategyPositionStatus.FLAT:
        return False
    if _non_daily_blockers(readiness):
        return False
    return liquidation is None or liquidation.state in {
        LiquidationOperationState.SUCCEEDED,
        LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
    }


def _next_daily_risk_state(
    *,
    policy: DailyRiskPolicyV1,
    calculation: DailyRiskCalculationV1,
    position: StrategyPositionV1,
    current_state: DailyRiskStateV1 | None,
    current_readiness: ExecutionReadinessV1,
    liquidation: LiquidationOperationV1 | None,
    observed_at_utc: str,
) -> DailyRiskStateV1:
    same_day = (
        current_state is not None
        and current_state.trading_day == calculation.trading_day
    )
    prior_status = (
        None if current_state is None else current_state.status
    )
    sticky = prior_status in {
        DailyRiskStatus.TRIGGERED,
        DailyRiskStatus.CLOSING,
        DailyRiskStatus.HALTED,
    }
    carryover = (
        current_state is not None
        and not same_day
        and prior_status in {DailyRiskStatus.TRIGGERED, DailyRiskStatus.CLOSING}
        and current_state.cleanup_status != DailyRiskCleanupStatus.COMPLETE
    )

    if same_day and prior_status == DailyRiskStatus.HALTED:
        return current_state

    if sticky or carryover:
        if _flat_and_cleanup_safe(
            position=position,
            readiness=current_readiness,
            liquidation=liquidation,
        ):
            status = DailyRiskStatus.HALTED
            cleanup = DailyRiskCleanupStatus.COMPLETE
        elif _cleanup_failed(liquidation):
            status = DailyRiskStatus.CLOSING
            cleanup = DailyRiskCleanupStatus.FAILED
        elif liquidation is not None or prior_status == DailyRiskStatus.CLOSING:
            status = DailyRiskStatus.CLOSING
            cleanup = DailyRiskCleanupStatus.PENDING
        else:
            status = DailyRiskStatus.TRIGGERED
            cleanup = DailyRiskCleanupStatus.PENDING
    elif not calculation.pnl_ready:
        status = DailyRiskStatus.NOT_READY
        cleanup = DailyRiskCleanupStatus.NOT_REQUIRED
    elif float(calculation.total_pnl) >= policy.target_pnl:
        if _flat_and_cleanup_safe(
            position=position,
            readiness=current_readiness,
            liquidation=liquidation,
        ):
            status = DailyRiskStatus.HALTED
            cleanup = DailyRiskCleanupStatus.COMPLETE
        else:
            status = DailyRiskStatus.TRIGGERED
            cleanup = DailyRiskCleanupStatus.PENDING
    else:
        status = DailyRiskStatus.MONITORING
        cleanup = DailyRiskCleanupStatus.NOT_REQUIRED

    if calculation.pnl_ready:
        realized = calculation.realized_pnl
        unrealized = calculation.unrealized_pnl
        total = calculation.total_pnl
        pnl_ready = True
    else:
        realized = None
        unrealized = None
        total = None
        pnl_ready = False
    return DailyRiskStateV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        trading_day=calculation.trading_day,
        status=status,
        realized_pnl=realized,
        unrealized_pnl=unrealized,
        total_pnl=total,
        target_pnl=policy.target_pnl,
        pnl_ready=pnl_ready,
        cleanup_status=cleanup,
        updated_at_utc=observed_at_utc,
    )


def daily_risk_readiness(
    current: ExecutionReadinessV1,
    *,
    state: DailyRiskStateV1,
    calculation: DailyRiskCalculationV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    other = _non_daily_blockers(current)
    if state.status == DailyRiskStatus.MONITORING:
        reasons = other
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
        detail = (
            calculation.reason_code
            if state.status == DailyRiskStatus.NOT_READY
            else state.status.value
        )
        reasons = other + (
            f"daily_risk:{state.trading_day}:{detail}",
        )
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
        updated_at_utc=format_utc(parse_utc(observed_at_utc)),
    )
