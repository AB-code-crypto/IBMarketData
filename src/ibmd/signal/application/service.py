from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from datetime import datetime, timezone

from ibmd.foundation.identity import new_id
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)
from ibmd.public_contracts.signal import (
    SignalCalculationStatus,
    SignalCalculationV1,
    SignalDirection,
    SignalEventV1,
)
from ibmd.signal.domain.calculation import (
    build_potential,
    build_signal_window,
    get_due_signal_bar_ts,
    rank_candidates,
    signal_direction,
)

from .config import SignalShadowConfig
from .ports import (
    ServiceHealthPublisher,
    SignalMarketDataSource,
    SignalRepository,
)


class SignalServiceError(RuntimeError):
    pass


class SignalShadowService:
    def __init__(
        self,
        *,
        config: SignalShadowConfig,
        market_data: SignalMarketDataSource,
        repository: SignalRepository,
        health_publisher: ServiceHealthPublisher,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.market_data = market_data
        self.repository = repository
        self.health_publisher = health_publisher
        self.clock = clock
        now = format_utc(self.clock())
        self.health = ServiceHealthV1.starting(
            service=config.service_name,
            deployment_id=config.deployment_id,
            instance_id=config.instance_id,
            pid=os.getpid(),
            application_version=config.application_version,
            configuration_hash=config.service_configuration_hash,
            now_utc=now,
        )

    def publish_starting(self) -> ServiceHealthV1:
        self.health_publisher.publish(self.health)
        return self.health

    def validate_dependencies(self) -> None:
        self.market_data.validate_schema()
        self.repository.validate_schema()

    def _publish_health(
        self,
        *,
        observed_at_utc: str,
        readiness: Readiness,
        source_freshness_seconds: float | None,
        source_status: str,
        detail: str | None,
        last_success_at_utc: str | None,
    ) -> None:
        self.health = self.health.heartbeat(
            now_utc=observed_at_utc,
            liveness=Liveness.RUNNING,
            readiness=readiness,
            last_success_at_utc=last_success_at_utc,
            source_freshness_seconds=source_freshness_seconds,
            dependency_status=(
                DependencyStatusV1(
                    name="target_market_data",
                    status=source_status,
                    detail=detail,
                    observed_at_utc=observed_at_utc,
                ),
            ),
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)

    def refresh_health(
        self,
        *,
        observed_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> bool:
        observed = observed_at_utc or format_utc(self.clock())
        now = parse_utc(observed)
        try:
            latest = self.market_data.read_latest_source_bar()
        except Exception as exc:
            detail = (
                f"{error_text}; " if error_text else ""
            ) + f"market-data read failed: {type(exc).__name__}: {exc}"
            self._publish_health(
                observed_at_utc=observed,
                readiness=Readiness.BLOCKED,
                source_freshness_seconds=None,
                source_status="ERROR",
                detail=detail,
                last_success_at_utc=self.health.last_success_at_utc,
            )
            return False

        if latest is None:
            detail = error_text or "target market-data has no complete bar"
            self._publish_health(
                observed_at_utc=observed,
                readiness=Readiness.BLOCKED,
                source_freshness_seconds=None,
                source_status="NO_DATA",
                detail=detail,
                last_success_at_utc=self.health.last_success_at_utc,
            )
            return False

        age = max(
            0.0,
            (
                now
                - datetime.fromtimestamp(
                    latest.bar_end_ts,
                    tz=timezone.utc,
                )
            ).total_seconds(),
        )
        fresh = age <= self.config.max_complete_bar_lag_seconds
        if error_text is None and fresh:
            readiness = Readiness.READY
            detail = None
            source_status = "READY"
        elif fresh:
            readiness = Readiness.DEGRADED
            detail = error_text
            source_status = "ERROR"
        else:
            readiness = Readiness.BLOCKED
            detail = error_text or (
                "latest complete target market-data bar is stale: "
                f"age={age:g}s, "
                f"max={self.config.max_complete_bar_lag_seconds}s"
            )
            source_status = "STALE"
        latest_calculation = self.repository.read_latest_calculation(
            self.config.instrument_id
        )
        self._publish_health(
            observed_at_utc=observed,
            readiness=readiness,
            source_freshness_seconds=age,
            source_status=source_status,
            detail=detail,
            last_success_at_utc=(
                None
                if latest_calculation is None
                else latest_calculation.calculated_at_utc
            ),
        )
        return readiness in {Readiness.READY, Readiness.DEGRADED}

    def calculate_at(
        self,
        signal_bar_ts: int,
    ) -> SignalCalculationV1:
        window = build_signal_window(
            signal_bar_ts=int(signal_bar_ts),
            pattern_lookback_minutes=(
                self.config.pattern_lookback_minutes
            ),
            potential_horizon_minutes=(
                self.config.potential_horizon_minutes
            ),
        )
        inputs = self.market_data.load_pattern_inputs(
            window=window,
            bar_size_seconds=self.config.source_bar_size_seconds,
            historical_lookback_days=(
                self.config.historical_lookback_days
            ),
            candidate_hour_profile=(
                self.config.candidate_hour_profile
            ),
        )
        ranking = rank_candidates(
            current_values=inputs.current_values,
            candidates=inputs.candidates,
            candidate_matrix=inputs.candidate_matrix,
            pearson_minimum=self.config.pearson_minimum,
            minmax_hard_filter_max_ratio=(
                self.config.minmax_hard_filter_max_ratio
            ),
            score_pearson_weight=self.config.score_pearson_weight,
            score_end_delta_weight=(
                self.config.score_end_delta_weight
            ),
            score_minmax_weight=self.config.score_minmax_weight,
        )
        selected = tuple(
            item.candidate
            for item in ranking.ranked_candidates[
                : self.config.potential_candidate_max_count
            ]
        )
        full_values = self.market_data.load_full_candidate_values(
            candidates=selected,
            bar_size_seconds=self.config.source_bar_size_seconds,
        )
        potential = build_potential(
            current_values=inputs.current_values,
            ranking=ranking,
            full_values_by_signal_ts=full_values,
            bar_size_seconds=self.config.source_bar_size_seconds,
            potential_horizon_minutes=(
                self.config.potential_horizon_minutes
            ),
            minimum_candidate_count=(
                self.config.potential_candidate_min_count
            ),
            maximum_candidate_count=(
                self.config.potential_candidate_max_count
            ),
        )
        selected_direction = signal_direction(
            potential=potential,
            minimum_abs_end_delta_points=(
                self.config.minimum_abs_potential_end_delta_points
            ),
        )

        calculated_at = format_utc(self.clock())
        signal_bar_utc = format_utc(
            datetime.fromtimestamp(
                window.signal_bar_ts,
                tz=timezone.utc,
            )
        )
        calculation_id = new_id("signal_calculation")
        best_signal_pearson = (
            max(
                item.pearson_score
                for item in ranking.ranked_candidates
            )
            if ranking.ranked_candidates
            else None
        )
        best_candidate_score = (
            ranking.ranked_candidates[0].candidate_score
            if ranking.ranked_candidates
            else None
        )

        event = None
        if selected_direction in {"LONG", "SHORT"}:
            event = SignalEventV1(
                event_id=new_id("signal_event"),
                calculation_id=calculation_id,
                strategy_id=self.config.strategy_id,
                strategy_version=self.config.strategy_version,
                configuration_hash=(
                    self.config.signal_configuration_hash
                ),
                instrument_id=self.config.instrument_id,
                source_bar_id=inputs.source_bar.bar_id,
                source_bar_revision=inputs.source_bar.revision,
                source_con_id=inputs.source_bar.con_id,
                source_local_symbol=inputs.source_bar.local_symbol,
                signal_bar_utc=signal_bar_utc,
                created_at_utc=calculated_at,
                direction=SignalDirection(selected_direction),
                entry_price=inputs.source_bar.entry_price,
                best_pearson=float(best_signal_pearson or 0.0),
                candidate_score_best=best_candidate_score,
                potential_end_delta_points=(
                    potential.end_delta_points
                ),
                potential_max_profit_points=(
                    potential.max_profit_points
                ),
                potential_max_drawdown_points=(
                    potential.max_drawdown_points
                ),
                potential_used=potential.used_candidate_count,
            )
            status = SignalCalculationStatus.SIGNAL
            reason = None
        else:
            status = SignalCalculationStatus.NO_SIGNAL
            if not potential.available:
                reason = potential.reason or "potential_unavailable"
            elif potential.direction == "NONE":
                reason = "potential_direction_none"
            else:
                reason = (
                    "potential_below_threshold:"
                    f"{abs(potential.end_delta_points):g}<="
                    f"{self.config.minimum_abs_potential_end_delta_points:g}"
                )

        calculation = SignalCalculationV1(
            calculation_id=calculation_id,
            strategy_id=self.config.strategy_id,
            strategy_version=self.config.strategy_version,
            configuration_hash=self.config.signal_configuration_hash,
            instrument_id=self.config.instrument_id,
            source_bar_id=inputs.source_bar.bar_id,
            source_bar_revision=inputs.source_bar.revision,
            source_con_id=inputs.source_bar.con_id,
            source_local_symbol=inputs.source_bar.local_symbol,
            signal_bar_utc=signal_bar_utc,
            calculated_at_utc=calculated_at,
            status=status,
            entry_price=inputs.source_bar.entry_price,
            raw_candidate_count=inputs.raw_candidate_count,
            valid_pattern_count=ranking.valid_pattern_count,
            pearson_pass_count=ranking.pearson_pass_count,
            minmax_pass_count=ranking.minmax_pass_count,
            skipped_pattern_count=inputs.skipped_candidate_count,
            best_raw_pearson=ranking.best_raw_pearson,
            best_signal_pearson=best_signal_pearson,
            best_candidate_score=best_candidate_score,
            potential_direction=SignalDirection(
                potential.direction
            ),
            potential_end_delta_points=potential.end_delta_points,
            potential_max_profit_points=potential.max_profit_points,
            potential_max_drawdown_points=(
                potential.max_drawdown_points
            ),
            potential_used=potential.used_candidate_count,
            reason=reason,
            event=event,
        )
        published = self.repository.publish(calculation)
        self.refresh_health(observed_at_utc=calculated_at)
        return published

    async def run_forever(
        self,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stop = stop_event or asyncio.Event()
        self.validate_dependencies()
        latest_calculation = self.repository.read_latest_calculation(
            self.config.instrument_id
        )
        last_due = (
            None
            if latest_calculation is None
            else int(
                parse_utc(
                    latest_calculation.signal_bar_utc
                ).timestamp()
            )
        )

        while not stop.is_set():
            ready = self.refresh_health()
            latest = self.market_data.read_latest_source_bar()
            if latest is None:
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            if last_due is None:
                last_due = (
                    latest.bar_end_ts
                    - latest.bar_end_ts
                    % self.config.rolling_step_seconds
                )
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            if not ready:
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            due = get_due_signal_bar_ts(
                latest_complete_bar_end_ts=latest.bar_end_ts,
                rolling_step_seconds=self.config.rolling_step_seconds,
                last_calculated_bar_ts=last_due,
            )
            if due is None:
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            try:
                await asyncio.to_thread(self.calculate_at, due)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                self.refresh_health(error_text=detail)
            finally:
                # The current robot attempts one calculation per due point.
                # Repeating the same incomplete point every second does not
                # repair a historical gap and creates log storms.
                last_due = due

            await asyncio.sleep(self.config.poll_interval_seconds)

    def publish_blocked(self, reason: str) -> ServiceHealthV1:
        now = format_utc(self.clock())
        detail = str(reason or "signal service blocked")
        self._publish_health(
            observed_at_utc=now,
            readiness=Readiness.BLOCKED,
            source_freshness_seconds=None,
            source_status="BLOCKED",
            detail=detail,
            last_success_at_utc=self.health.last_success_at_utc,
        )
        return self.health

    def publish_failed(self, reason: str) -> ServiceHealthV1:
        now = format_utc(self.clock())
        detail = str(reason or "signal service failed")
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.FAILED,
            readiness=Readiness.BLOCKED,
            dependency_status=self.health.dependency_status,
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)
        return self.health

    def publish_stopped(self) -> ServiceHealthV1:
        now = format_utc(self.clock())
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.STOPPED,
            readiness=Readiness.NOT_READY,
            blocking_reason="service stopped",
        )
        self.health_publisher.publish(self.health)
        return self.health
