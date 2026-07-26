from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Protocol

from ibmd.execution.domain.daily_risk import (
    DailyRiskMarketMarkV1,
    DailyRiskOwnedFillV1,
    DailyRiskPolicyV1,
    DailyRiskUpdateV1,
    calculate_daily_risk,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import LiquidationOperationV1
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderState,
)


class PaperDailyHaltDrillError(RuntimeError):
    pass


class DailyHaltExecutionStateSource(Protocol):
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


class DailyHaltEpisodeSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class DailyHaltEvidenceSource(Protocol):
    def read_owned_fills(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[DailyRiskOwnedFillV1, ...]: ...

    def read_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
        position_episode_id: str | None,
    ) -> LiquidationOperationV1 | None: ...


class DailyHaltRepository(Protocol):
    def read_latest_state(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None: ...

    def publish(
        self,
        *,
        current_state: DailyRiskStateV1 | None,
        current_readiness: ExecutionReadinessV1,
        update: DailyRiskUpdateV1,
    ) -> DailyRiskUpdateV1: ...


@dataclass(frozen=True)
class PaperDailyHaltDrillPolicyV1:
    drill_id: str
    account_id: str
    environment: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    timezone_name: str
    target_pnl: float
    contract_multiplier: float
    market_max_age_seconds: float
    price_tick: float
    trigger_cushion_usd: float = 1.0

    def __post_init__(self) -> None:
        for field_name in (
            "drill_id",
            "account_id",
            "environment",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "timezone_name",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise PaperDailyHaltDrillError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        object.__setattr__(self, "environment", self.environment.lower())
        if self.environment != "paper":
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires IBMD_ENVIRONMENT=paper"
            )
        if not self.account_id.upper().startswith("D"):
            raise PaperDailyHaltDrillError(
                "configured account does not look like an IB paper account"
            )
        if "paper-drill" not in self.deployment_id.lower():
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires deployment_id containing 'paper-drill'"
            )
        version = int(self.strategy_version)
        if version <= 0:
            raise PaperDailyHaltDrillError(
                "strategy_version must be positive"
            )
        object.__setattr__(self, "strategy_version", version)
        for field_name in (
            "target_pnl",
            "contract_multiplier",
            "market_max_age_seconds",
            "price_tick",
            "trigger_cushion_usd",
        ):
            value = float(getattr(self, field_name))
            if value <= 0.0:
                raise PaperDailyHaltDrillError(
                    f"{field_name} must be positive"
                )
            object.__setattr__(self, field_name, value)

    @property
    def domain_policy(self) -> DailyRiskPolicyV1:
        return DailyRiskPolicyV1(
            account_id=self.account_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            deployment_id=self.deployment_id,
            instrument_id=self.instrument_id,
            timezone_name=self.timezone_name,
            target_pnl=self.target_pnl,
            contract_multiplier=self.contract_multiplier,
            market_max_age_seconds=self.market_max_age_seconds,
        )


@dataclass(frozen=True)
class PaperDailyHaltDrillResultV1:
    drill_id: str
    position_episode_id: str
    owned_fill_count: int
    trial_update: DailyRiskUpdateV1
    triggered_update: DailyRiskUpdateV1
    synthetic_mark: DailyRiskMarketMarkV1
    target_total_pnl: float
    synthetic_mark_only: bool = True
    broker_mutations_performed: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": "PaperDailyHaltDrillResult",
            "schema_version": 1,
            "drill_id": self.drill_id,
            "position_episode_id": self.position_episode_id,
            "owned_fill_count": self.owned_fill_count,
            "synthetic_market_mark": {
                "bar_id": self.synthetic_mark.bar_id,
                "instrument_id": self.synthetic_mark.instrument_id,
                "con_id": self.synthetic_mark.con_id,
                "local_symbol": self.synthetic_mark.local_symbol,
                "bar_end_utc": self.synthetic_mark.bar_end_utc,
                "mid_price": self.synthetic_mark.mid_price,
                "age_seconds": self.synthetic_mark.age_seconds,
            },
            "synthetic_market_mark_only": self.synthetic_mark_only,
            "real_owned_fill_evidence_only": True,
            "target_total_pnl": self.target_total_pnl,
            "trial_calculation": self.trial_update.calculation.to_dict(),
            "triggered_calculation": (
                self.triggered_update.calculation.to_dict()
            ),
            "daily_risk_state": self.triggered_update.state.to_dict(),
            "execution_readiness": (
                self.triggered_update.execution_readiness.to_dict()
            ),
            "broker_mutations_performed": self.broker_mutations_performed,
            "automatic_retry_enabled": False,
            "acceptance_scope": (
                "sticky daily-risk threshold and liquidation integration; "
                "not an acceptance of live market PnL pricing"
            ),
        }


def _stable_market_bar_id(
    *,
    drill_id: str,
    episode_id: str,
    observed_at_utc: str,
) -> str:
    payload = {
        "drill_id": drill_id,
        "position_episode_id": episode_id,
        "observed_at_utc": observed_at_utc,
        "kind": "paper_daily_halt_synthetic_market_mark",
    }
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"market_bar_{digest}"


def _favourable_tick_price(
    *,
    raw_price: float,
    tick: float,
    side: StrategyPositionSide,
) -> float:
    raw = Decimal(str(raw_price))
    increment = Decimal(str(tick))
    rounding = (
        ROUND_CEILING
        if side == StrategyPositionSide.LONG
        else ROUND_FLOOR
    )
    ticks = (raw / increment).to_integral_value(rounding=rounding)
    price = float(ticks * increment)
    if price <= 0.0:
        raise PaperDailyHaltDrillError(
            "synthetic daily-halt mark is not positive"
        )
    return price


class PaperDailyHaltDrillService:
    def __init__(
        self,
        *,
        policy: PaperDailyHaltDrillPolicyV1,
        execution_state_source: DailyHaltExecutionStateSource,
        episode_source: DailyHaltEpisodeSource,
        evidence_source: DailyHaltEvidenceSource,
        repository: DailyHaltRepository,
    ) -> None:
        self.policy = policy
        self.execution_state_source = execution_state_source
        self.episode_source = episode_source
        self.evidence_source = evidence_source
        self.repository = repository

    def _scope(self) -> dict[str, str]:
        return {
            "account_id": self.policy.account_id,
            "strategy_id": self.policy.strategy_id,
            "deployment_id": self.policy.deployment_id,
            "instrument_id": self.policy.instrument_id,
        }

    def run_once(self, *, observed_at_utc: str) -> PaperDailyHaltDrillResultV1:
        observed = format_utc(parse_utc(observed_at_utc))
        scope = self._scope()
        position = self.execution_state_source.read_position(**scope)
        readiness = self.execution_state_source.read_readiness(**scope)
        if position is None or readiness is None:
            raise PaperDailyHaltDrillError(
                "daily-halt drill position/readiness is incomplete"
            )
        if (
            position.projection_status != StrategyPositionStatus.OPEN
            or position.position_episode_id is None
            or position.side
            not in {StrategyPositionSide.LONG, StrategyPositionSide.SHORT}
            or position.quantity <= 0
            or len(position.contracts) != 1
        ):
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires one owned OPEN futures position"
            )
        if (
            readiness.status != ExecutionReadinessStatus.READY
            or not readiness.command_intake_enabled
            or not readiness.broker_actions_enabled
            or not readiness.reconciliation_complete
            or not readiness.clock_healthy
        ):
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires fully READY execution state"
            )
        episode = self.episode_source.read_episode(
            position.position_episode_id
        )
        protection = self.episode_source.read_protection_by_episode(
            position.position_episode_id
        )
        if episode is None or protection is None:
            raise PaperDailyHaltDrillError(
                "daily-halt drill episode/protection is missing"
            )
        if (
            episode.status != PositionEpisodeStatus.OPEN
            or episode.position_episode_id != position.position_episode_id
            or episode.side != position.side
            or episode.quantity != position.quantity
        ):
            raise PaperDailyHaltDrillError(
                "daily-halt drill episode does not match the owned position"
            )
        if protection.status not in {
            ProtectionSetStatus.STOP_LIVE,
            ProtectionSetStatus.PROTECTED,
        }:
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires proven STOP protection"
            )
        if protection.stop_order.state != ProtectiveOrderState.LIVE:
            raise PaperDailyHaltDrillError(
                "daily-halt drill STOP is not LIVE"
            )
        if protection.take_profit_order is not None and (
            protection.take_profit_order.state
            not in {
                ProtectiveOrderState.LIVE,
                ProtectiveOrderState.NOT_REQUIRED,
            }
        ):
            raise PaperDailyHaltDrillError(
                "daily-halt drill TAKE PROFIT is not LIVE/NOT_REQUIRED"
            )
        current_state = self.repository.read_latest_state(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        if current_state is None or current_state.status != DailyRiskStatus.MONITORING:
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires current DailyRiskState=MONITORING"
            )
        liquidation = self.evidence_source.read_liquidation_operation(
            **scope,
            position_episode_id=episode.position_episode_id,
        )
        if liquidation is not None:
            raise PaperDailyHaltDrillError(
                "daily-halt drill requires no existing liquidation operation"
            )
        fills = self.evidence_source.read_owned_fills(**scope)
        held = position.contracts[0]
        trial_mark = DailyRiskMarketMarkV1(
            bar_id=_stable_market_bar_id(
                drill_id=self.policy.drill_id + "-trial",
                episode_id=episode.position_episode_id,
                observed_at_utc=observed,
            ),
            instrument_id=self.policy.instrument_id,
            con_id=held.con_id,
            local_symbol=held.local_symbol,
            bar_end_utc=observed,
            mid_price=episode.entry_average_price,
            age_seconds=0.0,
        )
        trial = calculate_daily_risk(
            policy=self.policy.domain_policy,
            owned_fills=fills,
            position=position,
            episode=episode,
            market_mark=trial_mark,
            current_state=current_state,
            current_readiness=readiness,
            liquidation=None,
            observed_at_utc=observed,
        )
        if not trial.calculation.pnl_ready:
            raise PaperDailyHaltDrillError(
                "daily-halt drill cannot use incomplete owned fill evidence: "
                f"{trial.calculation.reason_code}: "
                f"{trial.calculation.reason_detail}"
            )
        if trial.state.status != DailyRiskStatus.MONITORING:
            raise PaperDailyHaltDrillError(
                "daily-halt trial at entry price is already non-MONITORING; "
                "refuse to hide an existing threshold or sticky state"
            )
        realized = float(trial.calculation.realized_pnl)
        target_total = (
            self.policy.target_pnl + self.policy.trigger_cushion_usd
        )
        required_unrealized = target_total - realized
        if required_unrealized <= 0.0:
            raise PaperDailyHaltDrillError(
                "realized PnL already reaches the drill target; use the normal "
                "daily-risk path instead of a synthetic mark"
            )
        movement = required_unrealized / (
            position.quantity * self.policy.contract_multiplier
        )
        raw_price = (
            episode.entry_average_price + movement
            if position.side == StrategyPositionSide.LONG
            else episode.entry_average_price - movement
        )
        synthetic_price = _favourable_tick_price(
            raw_price=raw_price,
            tick=self.policy.price_tick,
            side=position.side,
        )
        mark = DailyRiskMarketMarkV1(
            bar_id=_stable_market_bar_id(
                drill_id=self.policy.drill_id,
                episode_id=episode.position_episode_id,
                observed_at_utc=observed,
            ),
            instrument_id=self.policy.instrument_id,
            con_id=held.con_id,
            local_symbol=held.local_symbol,
            bar_end_utc=observed,
            mid_price=synthetic_price,
            age_seconds=0.0,
        )
        triggered = calculate_daily_risk(
            policy=self.policy.domain_policy,
            owned_fills=fills,
            position=position,
            episode=episode,
            market_mark=mark,
            current_state=current_state,
            current_readiness=readiness,
            liquidation=None,
            observed_at_utc=observed,
        )
        if (
            not triggered.calculation.pnl_ready
            or triggered.state.status != DailyRiskStatus.TRIGGERED
            or triggered.state.cleanup_status.value != "PENDING"
            or float(triggered.calculation.total_pnl)
            < self.policy.target_pnl
            or triggered.execution_readiness.status
            != ExecutionReadinessStatus.BLOCKED
            or triggered.execution_readiness.command_intake_enabled
            or not triggered.execution_readiness.broker_actions_enabled
        ):
            raise PaperDailyHaltDrillError(
                "synthetic mark did not produce the expected sticky TRIGGERED "
                "daily-risk state"
            )
        persisted = self.repository.publish(
            current_state=current_state,
            current_readiness=readiness,
            update=triggered,
        )
        if persisted != triggered:
            raise PaperDailyHaltDrillError(
                "persisted daily-halt update differs from the domain result"
            )
        return PaperDailyHaltDrillResultV1(
            drill_id=self.policy.drill_id,
            position_episode_id=episode.position_episode_id,
            owned_fill_count=len(fills),
            trial_update=trial,
            triggered_update=persisted,
            synthetic_mark=mark,
            target_total_pnl=target_total,
        )
