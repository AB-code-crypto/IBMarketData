from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)
from ibmd.public_contracts.market_data import (
    MarketBarV1,
    MarketDataContractV1,
    MarketDataInstrumentState,
    MarketDataInstrumentStatusV1,
    MarketSideBarObservationV1,
)

from .config import MarketDataShadowConfig
from .ports import (
    MarketBarRepository,
    MarketDataSource,
    ServiceHealthPublisher,
)

logger = logging.getLogger(__name__)
_STOP = object()


class MarketDataServiceError(RuntimeError):
    pass


@dataclass(frozen=True)
class _QueuedFragments:
    fragments: tuple[MarketSideBarObservationV1, ...]
    future: asyncio.Future[tuple[MarketBarV1, ...]]


class MarketDataShadowService:
    def __init__(
        self,
        *,
        config: MarketDataShadowConfig,
        reader: MarketDataSource,
        repository: MarketBarRepository,
        health_publisher: ServiceHealthPublisher,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.reader = reader
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
            configuration_hash=config.configuration_hash,
            now_utc=now,
        )
        self._queue: asyncio.Queue[_QueuedFragments | object] = asyncio.Queue(
            maxsize=config.writer_queue_maxsize
        )
        self._writer_task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._writer_task is not None and not self._writer_task.done()

    def _require_running_writer(self) -> asyncio.Task[None]:
        task = self._writer_task
        if task is None:
            raise MarketDataServiceError("market-data writer is not started")
        if task.done():
            try:
                task.result()
            except Exception as exc:
                raise MarketDataServiceError(
                    "market-data writer terminated with an error: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            raise MarketDataServiceError(
                "market-data writer terminated unexpectedly"
            )
        return task

    def publish_starting(self) -> ServiceHealthV1:
        self.health_publisher.publish(self.health)
        return self.health

    async def start(self, *, publish_initial: bool = True) -> None:
        if self._writer_task is not None:
            raise MarketDataServiceError("market-data writer is already started")
        self.repository.validate_schema()
        if publish_initial:
            self.publish_starting()
        self._writer_task = asyncio.create_task(
            self._writer_loop(),
            name=f"market_data_writer_{self.config.instrument_id}",
        )

    async def _writer_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is _STOP:
                    return
                if not isinstance(item, _QueuedFragments):
                    raise MarketDataServiceError(
                        f"invalid market-data writer item: {type(item).__name__}"
                    )
                try:
                    result = await asyncio.to_thread(
                        self.repository.ingest_fragments,
                        item.fragments,
                    )
                except Exception as exc:
                    if not item.future.done():
                        item.future.set_exception(exc)
                else:
                    if not item.future.done():
                        item.future.set_result(result)
            finally:
                self._queue.task_done()

    async def submit_fragments(
        self,
        fragments: tuple[MarketSideBarObservationV1, ...],
    ) -> tuple[MarketBarV1, ...]:
        self._require_running_writer()
        values = tuple(fragments)
        if any(item.instrument_id != self.config.instrument_id for item in values):
            raise MarketDataServiceError(
                "market-data fragment belongs to another instrument"
            )
        if not values:
            return ()
        future: asyncio.Future[tuple[MarketBarV1, ...]] = (
            asyncio.get_running_loop().create_future()
        )
        await self._queue.put(_QueuedFragments(values, future))
        return await future

    def _current_status(
        self,
        *,
        observed_at_utc: str,
    ) -> MarketDataInstrumentStatusV1:
        latest = self.repository.read_latest_complete(self.config.instrument_id)
        return MarketDataInstrumentStatusV1.from_latest(
            instrument_id=self.config.instrument_id,
            latest=latest,
            observed_at_utc=observed_at_utc,
            max_age_seconds=self.config.bar_max_age_seconds,
        )

    def refresh_health(
        self,
        *,
        observed_at_utc: str | None = None,
        source_status: str = "CONNECTED",
        error_text: str | None = None,
    ) -> MarketDataInstrumentStatusV1:
        observed = observed_at_utc or format_utc(self.clock())
        try:
            status = self._current_status(observed_at_utc=observed)
        except Exception as exc:
            detail = (
                f"{error_text}; " if error_text else ""
            ) + f"latest market bar read failed: {type(exc).__name__}: {exc}"
            self._publish_health(
                observed_at_utc=observed,
                readiness=Readiness.BLOCKED,
                source_freshness_seconds=None,
                source_status="ERROR",
                detail=detail,
                last_success_at_utc=None,
            )
            raise

        if error_text is None and status.state == MarketDataInstrumentState.FRESH:
            readiness = Readiness.READY
            detail = None
        elif error_text is not None and status.state == MarketDataInstrumentState.FRESH:
            readiness = Readiness.DEGRADED
            detail = error_text
        else:
            readiness = Readiness.BLOCKED
            detail = error_text or (
                "no complete canonical market bar"
                if status.state == MarketDataInstrumentState.NO_DATA
                else (
                    "latest complete market bar is stale: "
                    f"age={status.age_seconds:g}s, "
                    f"max={status.max_age_seconds:g}s"
                )
            )
        latest = self.repository.read_latest_complete(self.config.instrument_id)
        self._publish_health(
            observed_at_utc=observed,
            readiness=readiness,
            source_freshness_seconds=status.age_seconds,
            source_status=source_status,
            detail=detail,
            last_success_at_utc=(
                None if latest is None else latest.published_at_utc
            ),
        )
        return status

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
                    name="ib_market_data",
                    status=source_status,
                    detail=detail,
                    observed_at_utc=observed_at_utc,
                ),
            ),
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)

    async def _record_source_failure(self, exc: Exception) -> None:
        observed = format_utc(self.clock())
        detail = f"{type(exc).__name__}: {exc}"
        try:
            await asyncio.to_thread(
                self.repository.record_source_failure,
                instrument_id=self.config.instrument_id,
                source_session_id=self.reader.source_session_id,
                observed_at_utc=observed,
                error_text=detail,
            )
        except Exception as store_exc:
            detail += (
                "; failed to persist source failure: "
                f"{type(store_exc).__name__}: {store_exc}"
            )
        self.refresh_health(
            observed_at_utc=observed,
            source_status="ERROR",
            error_text=detail,
        )

    async def bootstrap_recent_history(
        self,
        *,
        contract: MarketDataContractV1,
        end_at_utc: str | None = None,
    ) -> tuple[MarketBarV1, ...]:
        end = end_at_utc or format_utc(self.clock())
        try:
            fragments = await self.reader.fetch_recent_history(
                contract=contract,
                end_at_utc=end,
                lookback_seconds=self.config.history_lookback_seconds,
                bar_duration_seconds=self.config.bar_duration_seconds,
                use_rth=self.config.use_rth,
            )
            changed = await self.submit_fragments(fragments)
            self.refresh_health(observed_at_utc=format_utc(self.clock()))
            return changed
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._record_source_failure(exc)
            raise MarketDataServiceError(
                f"recent history bootstrap failed: {type(exc).__name__}: {exc}"
            ) from exc

    async def run_active_contract(
        self,
        *,
        contract: MarketDataContractV1,
        active_to_utc: str,
        stop_event: asyncio.Event | None = None,
        history_only: bool = False,
    ) -> tuple[MarketBarV1, ...]:
        stop = stop_event or asyncio.Event()
        if contract.instrument_id != self.config.instrument_id:
            raise MarketDataServiceError(
                "active contract belongs to another instrument: "
                f"expected={self.config.instrument_id}, "
                f"actual={contract.instrument_id}"
            )
        active_to = parse_utc(active_to_utc)
        if self.clock() >= active_to:
            raise MarketDataServiceError(
                "active contract interval has already ended: "
                f"contract={contract.local_symbol}, active_to={active_to_utc}"
            )
        changed = await self.bootstrap_recent_history(contract=contract)
        if history_only:
            return changed

        subscription = None
        try:
            subscription = await self.reader.open_realtime(
                contract=contract,
                bar_duration_seconds=self.config.bar_duration_seconds,
                use_rth=self.config.use_rth,
            )
            while not stop.is_set():
                now = self.clock()
                if now >= active_to:
                    break
                fragment = await subscription.next_bar(
                    timeout_seconds=self.config.realtime_read_timeout_seconds,
                )
                if fragment is None:
                    self.refresh_health(observed_at_utc=format_utc(now))
                    continue
                await self.submit_fragments((fragment,))
                self.refresh_health(observed_at_utc=format_utc(self.clock()))
            return changed
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._record_source_failure(exc)
            raise MarketDataServiceError(
                f"realtime market-data session failed: {type(exc).__name__}: {exc}"
            ) from exc
        finally:
            if subscription is not None:
                await subscription.close()

    def publish_blocked(self, reason: str) -> ServiceHealthV1:
        now = format_utc(self.clock())
        detail = str(reason or "market-data service blocked")
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
        detail = str(reason or "market-data service failed")
        self.health = self.health.heartbeat(
            now_utc=now,
            liveness=Liveness.FAILED,
            readiness=Readiness.BLOCKED,
            dependency_status=self.health.dependency_status,
            blocking_reason=detail,
        )
        self.health_publisher.publish(self.health)
        return self.health

    async def stop(self, *, publish_lifecycle: bool = True) -> None:
        task = self._writer_task
        if task is None:
            return
        if publish_lifecycle:
            now = format_utc(self.clock())
            self.health = self.health.heartbeat(
                now_utc=now,
                liveness=Liveness.STOPPING,
                readiness=Readiness.NOT_READY,
                blocking_reason="service stopping",
            )
            self.health_publisher.publish(self.health)
        if not task.done():
            await self._queue.put(_STOP)
        await task
        self._writer_task = None
        if publish_lifecycle:
            now = format_utc(self.clock())
            self.health = self.health.heartbeat(
                now_utc=now,
                liveness=Liveness.STOPPED,
                readiness=Readiness.NOT_READY,
                blocking_reason="service stopped",
            )
            self.health_publisher.publish(self.health)
