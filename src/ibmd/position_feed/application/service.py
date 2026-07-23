from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Callable
from datetime import datetime

from ibmd.foundation.identity import new_id
from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway.positions import IBPositionReader
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.position_feed.domain.snapshot import (
    build_complete_position_snapshot,
    build_failed_position_snapshot,
)

from .config import BrokerPositionFeedConfig
from .ports import (
    BrokerPositionSnapshotRepository,
    ServiceHealthPublisher,
)

logger = logging.getLogger(__name__)


class PositionFeedPollError(RuntimeError):
    pass


class BrokerPositionFeedService:
    def __init__(
        self,
        *,
        config: BrokerPositionFeedConfig,
        reader: IBPositionReader,
        repository: BrokerPositionSnapshotRepository,
        health_publisher: ServiceHealthPublisher,
        clock: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.reader = reader
        self.repository = repository
        self.health_publisher = health_publisher
        self.clock = clock
        self.monotonic = monotonic
        now = format_utc(self.clock())
        self.health = ServiceHealthV1.starting(
            service=config.service_name,
            deployment_id=config.deployment_id,
            instance_id=config.instance_id,
            pid=os.getpid(),
            application_version=config.application_version,
            configuration_hash=config.configuration_hash,
            now_utc=now,
        )

    def publish_starting(self) -> ServiceHealthV1:
        self.health_publisher.publish(self.health)
        return self.health

    def _publish_success_health(
        self,
        *,
        observed_at_utc: str,
    ) -> None:
        self.health = self.health.heartbeat(
            now_utc=observed_at_utc,
            liveness=Liveness.RUNNING,
            readiness=Readiness.READY,
            last_success_at_utc=observed_at_utc,
            source_freshness_seconds=0.0,
            dependency_status=(
                DependencyStatusV1(
                    name="ib_positions",
                    status="CONNECTED",
                    observed_at_utc=observed_at_utc,
                ),
            ),
            blocking_reason=None,
        )
        self.health_publisher.publish(self.health)

    def _publish_failure_health(
        self,
        *,
        observed_at_utc: str,
        error_text: str,
    ) -> None:
        source_freshness: float | None = None
        readiness = Readiness.BLOCKED
        try:
            latest = self.repository.read_latest_complete()
        except Exception as read_exc:
            latest = None
            error_text = (
                f"{error_text}; latest snapshot read failed: "
                f"{type(read_exc).__name__}: {read_exc}"
            )
        if latest is not None:
            freshness = latest.freshness(
                observed_at_utc=observed_at_utc,
                max_age_seconds=self.config.snapshot_max_age_seconds,
            )
            source_freshness = freshness.age_seconds
            readiness = (
                Readiness.DEGRADED
                if freshness.is_fresh
                else Readiness.BLOCKED
            )

        self.health = self.health.heartbeat(
            now_utc=observed_at_utc,
            liveness=Liveness.RUNNING,
            readiness=readiness,
            source_freshness_seconds=source_freshness,
            dependency_status=(
                DependencyStatusV1(
                    name="ib_positions",
                    status="ERROR",
                    detail=error_text,
                    observed_at_utc=observed_at_utc,
                ),
            ),
            blocking_reason=error_text,
        )
        self.health_publisher.publish(self.health)

    async def poll_once(self) -> BrokerPositionSnapshotV1:
        snapshot_id = new_id("position_snapshot")
        try:
            raw_positions = await asyncio.wait_for(
                self.reader.read_positions(
                    account_id=self.config.account_id,
                ),
                timeout=self.config.poll_timeout_seconds,
            )
            captured_at_utc = format_utc(self.clock())
            snapshot = build_complete_position_snapshot(
                rows=tuple(
                    item.to_public_row()
                    for item in raw_positions
                ),
                snapshot_id=snapshot_id,
                account_id=self.config.account_id,
                captured_at_utc=captured_at_utc,
                published_at_utc=captured_at_utc,
                source_session_id=self.reader.source_session_id,
            )
            self.repository.publish(snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            observed_at_utc = format_utc(self.clock())
            failure = build_failed_position_snapshot(
                snapshot_id=snapshot_id,
                account_id=self.config.account_id,
                captured_at_utc=observed_at_utc,
                published_at_utc=observed_at_utc,
                source_session_id=self.reader.source_session_id,
                error=exc,
            )
            failure_error = failure.error_text or "position poll failed"
            try:
                self.repository.publish(failure)
            except Exception as store_exc:
                failure_error = (
                    f"{failure_error}; failed to persist poll failure: "
                    f"{type(store_exc).__name__}: {store_exc}"
                )
            self._publish_failure_health(
                observed_at_utc=observed_at_utc,
                error_text=failure_error,
            )
            raise PositionFeedPollError(failure_error) from exc

        self._publish_success_health(
            observed_at_utc=captured_at_utc,
        )
        return snapshot

    async def run_forever(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        publish_initial: bool = True,
    ) -> None:
        stop = stop_event or asyncio.Event()
        if publish_initial:
            self.publish_starting()
        while not stop.is_set():
            started = self.monotonic()
            try:
                await self.poll_once()
            except PositionFeedPollError as exc:
                logger.warning("broker position poll failed: %s", exc)

            delay = max(
                0.0,
                self.config.poll_interval_seconds
                - (self.monotonic() - started),
            )
            if delay <= 0.0:
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def publish_blocked(self, reason: str) -> ServiceHealthV1:
        now = format_utc(self.clock())
        detail = str(reason or "service blocked")
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.RUNNING,
            readiness=Readiness.BLOCKED,
            dependency_status=(),
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)
        return self.health

    def publish_failed(self, reason: str) -> ServiceHealthV1:
        now = format_utc(self.clock())
        detail = str(reason or "service failed")
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.FAILED,
            readiness=Readiness.BLOCKED,
            dependency_status=(),
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)
        return self.health

    def publish_stopping(self) -> ServiceHealthV1:
        now = format_utc(self.clock())
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.STOPPING,
            readiness=Readiness.NOT_READY,
            blocking_reason="service stopping",
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
