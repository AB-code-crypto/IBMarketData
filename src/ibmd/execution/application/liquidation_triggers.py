from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from ibmd.catalog import CatalogBundleV1
from ibmd.catalog.sessions import (
    SessionDefinitionV1,
    SessionExceptionStatus,
)
from ibmd.catalog.strategy import DailyFlatPolicyV1, StrategyInstrumentPolicyV1
from ibmd.execution.domain.liquidation import (
    LiquidationRequestResult,
    LiquidationSnapshot,
    request_liquidation,
)
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import LiquidationReason
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderState,
)


class LiquidationTriggerProducerError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiquidationTriggerProducerPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    missing_stop_grace_seconds: float = 30.0
    require_production_session: bool = True

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            parsed = str(getattr(self, field_name) or "").strip()
            if not parsed:
                raise LiquidationTriggerProducerError(
                    f"{field_name} is required"
                )
            object.__setattr__(self, field_name, parsed)
        version = int(self.strategy_version)
        if version <= 0:
            raise LiquidationTriggerProducerError(
                "strategy_version must be positive"
            )
        object.__setattr__(self, "strategy_version", version)
        grace = float(self.missing_stop_grace_seconds)
        if grace < 0.0:
            raise LiquidationTriggerProducerError(
                "missing_stop_grace_seconds must be non-negative"
            )
        object.__setattr__(self, "missing_stop_grace_seconds", grace)
        if not isinstance(self.require_production_session, bool):
            raise LiquidationTriggerProducerError(
                "require_production_session must be boolean"
            )


@dataclass(frozen=True)
class LiquidationTriggerCandidateV1:
    reason: LiquidationReason
    source_ref: str
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, LiquidationReason):
            raise LiquidationTriggerProducerError(
                "candidate reason must be LiquidationReason"
            )
        source = str(self.source_ref or "").strip()
        detail = str(self.detail or "").strip()
        if not source or len(source) > 256:
            raise LiquidationTriggerProducerError(
                "candidate source_ref must contain 1..256 characters"
            )
        if not detail:
            raise LiquidationTriggerProducerError(
                "candidate detail is required"
            )
        object.__setattr__(self, "source_ref", source)
        object.__setattr__(self, "detail", detail)


@dataclass(frozen=True)
class PersistedLiquidationTriggerV1:
    candidate: LiquidationTriggerCandidateV1
    operation_id: str
    trigger_id: str
    operation_created: bool
    trigger_created: bool


@dataclass(frozen=True)
class LiquidationTriggerEpisodeRunV1:
    position_episode_id: str
    candidates: tuple[LiquidationTriggerCandidateV1, ...]
    persisted: tuple[PersistedLiquidationTriggerV1, ...]
    blocked_reasons: tuple[str, ...]


@dataclass(frozen=True)
class LiquidationTriggerProducerRunV1:
    observed_at_utc: str
    open_episode_count: int
    episodes: tuple[LiquidationTriggerEpisodeRunV1, ...]
    broker_mutations_performed: bool = False

    @property
    def candidate_count(self) -> int:
        return sum(len(item.candidates) for item in self.episodes)

    @property
    def trigger_created_count(self) -> int:
        return sum(
            1
            for item in self.episodes
            for persisted in item.persisted
            if persisted.trigger_created
        )

    @property
    def operation_created_count(self) -> int:
        return sum(
            1
            for item in self.episodes
            for persisted in item.persisted
            if persisted.operation_created
        )


class LiquidationTriggerStateSource(Protocol):
    def list_open_episodes(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[PositionEpisodeV1, ...]: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...

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


class LiquidationTriggerRepository(Protocol):
    def read_snapshot_by_episode(
        self,
        position_episode_id: str,
    ) -> LiquidationSnapshot | None: ...

    def publish_request(
        self,
        *,
        current: LiquidationSnapshot | None,
        result: LiquidationRequestResult,
    ) -> LiquidationSnapshot: ...


def _seconds_of_day(value: str, *, field_name: str) -> int:
    text = str(value or "").strip()
    parts = text.split(":")
    if len(parts) != 3:
        raise LiquidationTriggerProducerError(
            f"{field_name} must be HH:MM:SS: {value!r}"
        )
    try:
        hour, minute, second = (int(item) for item in parts)
    except ValueError as exc:
        raise LiquidationTriggerProducerError(
            f"{field_name} must be HH:MM:SS: {value!r}"
        ) from exc
    if not (0 <= hour <= 24 and 0 <= minute < 60 and 0 <= second < 60):
        raise LiquidationTriggerProducerError(
            f"invalid {field_name}: {value!r}"
        )
    if hour == 24 and (minute != 0 or second != 0):
        raise LiquidationTriggerProducerError(
            f"24-hour boundary must be 24:00:00: {value!r}"
        )
    return hour * 3600 + minute * 60 + second


def _trading_intervals_for_date(
    session: SessionDefinitionV1,
    local_date: date,
):
    date_text = local_date.isoformat()
    exception = next(
        (item for item in session.exceptions if item.local_date == date_text),
        None,
    )
    if exception is not None:
        if exception.status == SessionExceptionStatus.CLOSED:
            return ()
        return exception.trading_intervals
    return session.weekly_days[local_date.weekday()].trading_intervals


def _daily_flat_boundary_utc(
    *,
    session: SessionDefinitionV1,
    policy: DailyFlatPolicyV1,
    local_date: date,
) -> datetime | None:
    anchor = _seconds_of_day(
        policy.risk_blocked_until_local,
        field_name="risk_blocked_until_local",
    )
    configured_start = _seconds_of_day(
        policy.liquidation_start_local,
        field_name="liquidation_start_local",
    )
    lead = anchor - configured_start
    if lead <= 0:
        raise LiquidationTriggerProducerError(
            "daily-flat liquidation lead must be positive"
        )
    intervals = _trading_intervals_for_date(session, local_date)
    close_candidates = [
        item.end_seconds
        for item in intervals
        if 0 < item.end_seconds <= anchor
    ]
    if not close_candidates:
        return None
    actual_close = max(close_candidates)
    due_seconds = actual_close - lead
    if due_seconds < 0:
        raise LiquidationTriggerProducerError(
            "daily-flat early-close boundary precedes local midnight"
        )
    local_midnight = datetime(
        local_date.year,
        local_date.month,
        local_date.day,
        tzinfo=session.zone,
    )
    return (local_midnight + timedelta(seconds=due_seconds)).astimezone(
        parse_utc("1970-01-01T00:00:00Z").tzinfo
    )


def _daily_flat_candidate(
    *,
    bundle: CatalogBundleV1,
    strategy_policy: StrategyInstrumentPolicyV1,
    episode: PositionEpisodeV1,
    observed_at_utc: str,
    require_production_session: bool,
) -> tuple[LiquidationTriggerCandidateV1 | None, str | None]:
    policy = strategy_policy.daily_flat
    if not policy.enabled:
        return None, None
    session = bundle.session_calendar.require(policy.session_id)
    opened = parse_utc(episode.opened_at_utc)
    observed = parse_utc(observed_at_utc)
    if observed < opened:
        raise LiquidationTriggerProducerError(
            "trigger observation precedes position episode open time"
        )
    opened_local = opened.astimezone(session.zone)
    observed_local = observed.astimezone(session.zone)
    current = opened_local.date()
    due_boundary = None
    while current <= observed_local.date():
        boundary = _daily_flat_boundary_utc(
            session=session,
            policy=policy,
            local_date=current,
        )
        if boundary is not None and opened <= boundary <= observed:
            due_boundary = boundary
            break
        current += timedelta(days=1)
    if due_boundary is None:
        return None, None
    if require_production_session and not session.production_qualified:
        return (
            None,
            "daily_flat_session_not_production_qualified:"
            f"{session.session_id}",
        )
    due_local = due_boundary.astimezone(session.zone)
    return (
        LiquidationTriggerCandidateV1(
            reason=LiquidationReason.DAILY_FLAT,
            source_ref=(
                f"daily-flat:{session.session_id}:"
                f"{due_local.date().isoformat()}"
            ),
            detail=(
                f"daily-flat boundary reached at {format_utc(due_boundary)}; "
                f"local={due_local.isoformat()}, "
                f"calendar={bundle.session_calendar.calendar_version}"
            ),
        ),
        None,
    )


def _missing_stop_candidate(
    *,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    existing: LiquidationSnapshot | None,
    observed_at_utc: str,
    grace_seconds: float,
) -> LiquidationTriggerCandidateV1 | None:
    if protection.position_episode_id != episode.position_episode_id:
        raise LiquidationTriggerProducerError(
            "protection belongs to another position episode"
        )
    stop = protection.stop_order
    if stop.state == ProtectiveOrderState.LIVE:
        return None
    if existing is not None and stop.state in {
        ProtectiveOrderState.CANCEL_REQUESTED,
        ProtectiveOrderState.CANCELLED,
        ProtectiveOrderState.NOT_REQUIRED,
    }:
        return None
    age = (
        parse_utc(observed_at_utc) - parse_utc(episode.opened_at_utc)
    ).total_seconds()
    if age < 0.0:
        raise LiquidationTriggerProducerError(
            "trigger observation precedes episode open time"
        )
    if stop.state in {
        ProtectiveOrderState.PLANNED,
        ProtectiveOrderState.SUBMITTING,
    } and age < grace_seconds:
        return None
    if stop.state == ProtectiveOrderState.REJECTED:
        return LiquidationTriggerCandidateV1(
            reason=LiquidationReason.STOP_REJECTED,
            source_ref=f"stop-rejected:{stop.protective_order_id}",
            detail=(
                "mandatory STOP was rejected: "
                f"status={stop.broker_status}, reason={stop.failure_reason}"
            ),
        )
    if stop.state == ProtectiveOrderState.FILLED:
        return LiquidationTriggerCandidateV1(
            reason=LiquidationReason.STOP_BREACHED,
            source_ref=f"stop-breached:{stop.protective_order_id}",
            detail="STOP is filled while the position episode remains open",
        )
    return LiquidationTriggerCandidateV1(
        reason=LiquidationReason.MISSING_STOP,
        source_ref=f"missing-stop:{stop.protective_order_id}",
        detail=(
            "mandatory STOP is not broker-proven LIVE: "
            f"state={stop.state.value}, age_seconds={age:.6f}"
        ),
    )


def _daily_halt_candidate(
    *,
    policy: StrategyInstrumentPolicyV1,
    daily_risk: DailyRiskStateV1 | None,
) -> LiquidationTriggerCandidateV1 | None:
    if not policy.daily_pnl.enabled or daily_risk is None:
        return None
    if daily_risk.status not in {
        DailyRiskStatus.TRIGGERED,
        DailyRiskStatus.CLOSING,
        DailyRiskStatus.HALTED,
    }:
        return None
    return LiquidationTriggerCandidateV1(
        reason=LiquidationReason.DAILY_HALT,
        source_ref=f"daily-halt:{daily_risk.trading_day}",
        detail=(
            f"daily risk state requires liquidation: "
            f"status={daily_risk.status.value}, "
            f"cleanup={daily_risk.cleanup_status.value}, "
            f"total_pnl={daily_risk.total_pnl}"
        ),
    )


def _rollover_candidate(
    *,
    bundle: CatalogBundleV1,
    episode: PositionEpisodeV1,
    observed_at_utc: str,
) -> tuple[LiquidationTriggerCandidateV1 | None, str | None]:
    matches = [
        item
        for item in bundle.contract_calendar.contracts
        if item.con_id == episode.con_id
        and item.local_symbol == episode.local_symbol
    ]
    if len(matches) != 1:
        return (
            None,
            "rollover_held_contract_unregistered_or_ambiguous:"
            f"{episode.con_id}:{episode.local_symbol}",
        )
    held = matches[0]
    observed = parse_utc(observed_at_utc)
    if observed < held.active_to:
        return None, None
    return (
        LiquidationTriggerCandidateV1(
            reason=LiquidationReason.ROLLOVER,
            source_ref=(
                f"rollover:{bundle.contract_calendar.calendar_version}:"
                f"{held.local_symbol}:{format_utc(held.active_to)}"
            ),
            detail=(
                "held futures contract is outside its active interval: "
                f"held={held.local_symbol}/{held.con_id}, "
                f"active_to={format_utc(held.active_to)}"
            ),
        ),
        None,
    )


def evaluate_liquidation_trigger_candidates(
    *,
    bundle: CatalogBundleV1,
    producer_policy: LiquidationTriggerProducerPolicyV1,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    daily_risk: DailyRiskStateV1 | None,
    existing: LiquidationSnapshot | None,
    observed_at_utc: str,
) -> tuple[tuple[LiquidationTriggerCandidateV1, ...], tuple[str, ...]]:
    if episode.status != PositionEpisodeStatus.OPEN:
        return (), ("episode_not_open",)
    expected_scope = (
        producer_policy.account_id,
        producer_policy.strategy_id,
        producer_policy.strategy_version,
        producer_policy.deployment_id,
        producer_policy.instrument_id,
    )
    actual_scope = (
        episode.account_id,
        episode.strategy_id,
        episode.strategy_version,
        episode.deployment_id,
        episode.instrument_id,
    )
    if actual_scope != expected_scope:
        raise LiquidationTriggerProducerError(
            "position episode belongs to another trigger-producer scope"
        )
    if (
        bundle.strategy_policy.strategy_id != producer_policy.strategy_id
        or bundle.strategy_policy.strategy_version
        != producer_policy.strategy_version
    ):
        raise LiquidationTriggerProducerError(
            "catalog strategy identity differs from trigger-producer policy"
        )
    strategy_policy = bundle.strategy_policy.require(
        producer_policy.instrument_id
    )
    candidates: list[LiquidationTriggerCandidateV1] = []
    blockers: list[str] = []

    stop_candidate = _missing_stop_candidate(
        episode=episode,
        protection=protection,
        existing=existing,
        observed_at_utc=observed_at_utc,
        grace_seconds=producer_policy.missing_stop_grace_seconds,
    )
    if stop_candidate is not None:
        candidates.append(stop_candidate)

    halt_candidate = _daily_halt_candidate(
        policy=strategy_policy,
        daily_risk=daily_risk,
    )
    if halt_candidate is not None:
        candidates.append(halt_candidate)

    flat_candidate, flat_blocker = _daily_flat_candidate(
        bundle=bundle,
        strategy_policy=strategy_policy,
        episode=episode,
        observed_at_utc=observed_at_utc,
        require_production_session=(
            producer_policy.require_production_session
        ),
    )
    if flat_candidate is not None:
        candidates.append(flat_candidate)
    if flat_blocker is not None:
        blockers.append(flat_blocker)

    rollover_candidate, rollover_blocker = _rollover_candidate(
        bundle=bundle,
        episode=episode,
        observed_at_utc=observed_at_utc,
    )
    if rollover_candidate is not None:
        candidates.append(rollover_candidate)
    if rollover_blocker is not None:
        blockers.append(rollover_blocker)

    unique: list[LiquidationTriggerCandidateV1] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        key = (candidate.reason.value, candidate.source_ref)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return tuple(unique), tuple(dict.fromkeys(blockers))


class LiquidationTriggerProducerService:
    def __init__(
        self,
        *,
        policy: LiquidationTriggerProducerPolicyV1,
        bundle: CatalogBundleV1,
        state_source: LiquidationTriggerStateSource,
        repository: LiquidationTriggerRepository,
    ) -> None:
        self.policy = policy
        self.bundle = bundle
        self.state_source = state_source
        self.repository = repository

    def run_once(self, *, observed_at_utc: str) -> LiquidationTriggerProducerRunV1:
        observed = format_utc(parse_utc(observed_at_utc))
        episodes = self.state_source.list_open_episodes(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        daily_risk = self.state_source.read_latest_daily_risk(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        runs: list[LiquidationTriggerEpisodeRunV1] = []
        for episode in episodes:
            protection = self.state_source.read_protection_by_episode(
                episode.position_episode_id
            )
            position = self.state_source.read_position(
                account_id=episode.account_id,
                strategy_id=episode.strategy_id,
                deployment_id=episode.deployment_id,
                instrument_id=episode.instrument_id,
            )
            readiness = self.state_source.read_readiness(
                account_id=episode.account_id,
                strategy_id=episode.strategy_id,
                deployment_id=episode.deployment_id,
                instrument_id=episode.instrument_id,
            )
            missing = []
            if protection is None:
                missing.append("protection_state_missing")
            if position is None:
                missing.append("strategy_position_missing")
            if readiness is None:
                missing.append("execution_readiness_missing")
            if missing:
                runs.append(
                    LiquidationTriggerEpisodeRunV1(
                        position_episode_id=episode.position_episode_id,
                        candidates=(),
                        persisted=(),
                        blocked_reasons=tuple(missing),
                    )
                )
                continue
            existing = self.repository.read_snapshot_by_episode(
                episode.position_episode_id
            )
            candidates, blockers = evaluate_liquidation_trigger_candidates(
                bundle=self.bundle,
                producer_policy=self.policy,
                episode=episode,
                protection=protection,
                daily_risk=daily_risk,
                existing=existing,
                observed_at_utc=observed,
            )
            persisted_values: list[PersistedLiquidationTriggerV1] = []
            for candidate in candidates:
                current = self.repository.read_snapshot_by_episode(
                    episode.position_episode_id
                )
                current_readiness = self.state_source.read_readiness(
                    account_id=episode.account_id,
                    strategy_id=episode.strategy_id,
                    deployment_id=episode.deployment_id,
                    instrument_id=episode.instrument_id,
                )
                if current_readiness is None:
                    raise LiquidationTriggerProducerError(
                        "execution readiness disappeared while persisting triggers"
                    )
                result = request_liquidation(
                    episode=episode,
                    position=position,
                    readiness=current_readiness,
                    reason=candidate.reason,
                    source_ref=candidate.source_ref,
                    observed_at_utc=observed,
                    existing=current,
                )
                snapshot = self.repository.publish_request(
                    current=current,
                    result=result,
                )
                persisted_values.append(
                    PersistedLiquidationTriggerV1(
                        candidate=candidate,
                        operation_id=(
                            snapshot.operation.liquidation_operation_id
                        ),
                        trigger_id=result.trigger.trigger_id,
                        operation_created=result.operation_created,
                        trigger_created=result.trigger_created,
                    )
                )
            runs.append(
                LiquidationTriggerEpisodeRunV1(
                    position_episode_id=episode.position_episode_id,
                    candidates=candidates,
                    persisted=tuple(persisted_values),
                    blocked_reasons=blockers,
                )
            )
        return LiquidationTriggerProducerRunV1(
            observed_at_utc=observed,
            open_episode_count=len(episodes),
            episodes=tuple(runs),
        )


def liquidation_trigger_producer_payload(
    run: LiquidationTriggerProducerRunV1,
) -> dict:
    return {
        "observed_at_utc": run.observed_at_utc,
        "open_episode_count": run.open_episode_count,
        "candidate_count": run.candidate_count,
        "operation_created_count": run.operation_created_count,
        "trigger_created_count": run.trigger_created_count,
        "episodes": [
            {
                "position_episode_id": item.position_episode_id,
                "candidates": [
                    {
                        "reason": candidate.reason.value,
                        "source_ref": candidate.source_ref,
                        "detail": candidate.detail,
                    }
                    for candidate in item.candidates
                ],
                "persisted": [
                    {
                        "reason": value.candidate.reason.value,
                        "source_ref": value.candidate.source_ref,
                        "operation_id": value.operation_id,
                        "trigger_id": value.trigger_id,
                        "operation_created": value.operation_created,
                        "trigger_created": value.trigger_created,
                    }
                    for value in item.persisted
                ],
                "blocked_reasons": list(item.blocked_reasons),
            }
            for item in run.episodes
        ],
        "broker_mutations_performed": False,
        "automatic_retry_enabled": False,
        "legacy_database_compatibility_required": False,
    }
