from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.public_contracts.health import (
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)


class TargetSupervisorError(RuntimeError):
    pass


class ChildProcess(Protocol):
    pid: int

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class ProcessLauncher(Protocol):
    def launch(self, spec: "SupervisorServiceSpecV1") -> tuple[ChildProcess, object]: ...


class HealthReader(Protocol):
    def __call__(
        self,
        path: str | Path,
        *,
        expected_service: str | None = None,
    ) -> ServiceHealthV1: ...


@dataclass(frozen=True)
class SupervisorServiceSpecV1:
    service_name: str
    argv: tuple[str, ...]
    health_file: Path
    log_file: Path

    def __post_init__(self) -> None:
        name = str(self.service_name or "").strip()
        if not name:
            raise TargetSupervisorError("service_name is required")
        argv = tuple(str(item) for item in self.argv)
        if not argv or not argv[0].strip():
            raise TargetSupervisorError(
                f"service {name!r} requires a non-empty argv"
            )
        object.__setattr__(self, "service_name", name)
        object.__setattr__(self, "argv", argv)
        object.__setattr__(self, "health_file", Path(self.health_file))
        object.__setattr__(self, "log_file", Path(self.log_file))


@dataclass(frozen=True)
class SupervisorPolicyV1:
    startup_timeout_seconds: float = 60.0
    heartbeat_max_age_seconds: float = 30.0
    poll_interval_seconds: float = 1.0
    shutdown_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        for field_name in (
            "startup_timeout_seconds",
            "heartbeat_max_age_seconds",
            "poll_interval_seconds",
            "shutdown_timeout_seconds",
        ):
            value = float(getattr(self, field_name))
            if value <= 0.0:
                raise TargetSupervisorError(
                    f"{field_name} must be positive"
                )
            object.__setattr__(self, field_name, value)


@dataclass
class ManagedServiceV1:
    spec: SupervisorServiceSpecV1
    process: ChildProcess
    log_handle: object
    started_at_utc: str


class SubprocessServiceLauncher:
    def __init__(
        self,
        *,
        working_directory: str | Path,
        environment: dict[str, str] | None = None,
    ) -> None:
        self.working_directory = Path(working_directory).resolve()
        self.environment = dict(os.environ if environment is None else environment)

    def launch(
        self,
        spec: SupervisorServiceSpecV1,
    ) -> tuple[subprocess.Popen, object]:
        spec.log_file.parent.mkdir(parents=True, exist_ok=True)
        handle = spec.log_file.open("ab", buffering=0)
        creationflags = 0
        if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        try:
            process = subprocess.Popen(
                list(spec.argv),
                cwd=str(self.working_directory),
                env=self.environment,
                stdin=subprocess.DEVNULL,
                stdout=handle,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
            )
        except Exception:
            handle.close()
            raise
        return process, handle


class TargetStackSupervisor:
    def __init__(
        self,
        *,
        deployment_id: str,
        application_version: str,
        configuration_hash: str,
        instance_id: str,
        specs: tuple[SupervisorServiceSpecV1, ...],
        policy: SupervisorPolicyV1,
        launcher: ProcessLauncher,
        health_reader: HealthReader,
        health_publisher,
        manifest_file: str | Path,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.deployment_id = str(deployment_id or "").strip()
        self.application_version = str(application_version or "").strip()
        self.configuration_hash = str(configuration_hash or "").strip()
        self.instance_id = str(instance_id or "").strip()
        if not all(
            (
                self.deployment_id,
                self.application_version,
                self.configuration_hash,
                self.instance_id,
            )
        ):
            raise TargetSupervisorError(
                "deployment/application/configuration/instance values are required"
            )
        names = tuple(item.service_name for item in specs)
        if not names or len(names) != len(set(names)):
            raise TargetSupervisorError(
                "supervisor service names must be non-empty and unique"
            )
        self.specs = tuple(specs)
        self.policy = policy
        self.launcher = launcher
        self.health_reader = health_reader
        self.health_publisher = health_publisher
        self.manifest_file = Path(manifest_file)
        self.sleep = sleep
        self.monotonic = monotonic
        self.managed: list[ManagedServiceV1] = []
        now = format_utc(utc_now())
        self.health = ServiceHealthV1.starting(
            service="supervisor",
            deployment_id=self.deployment_id,
            instance_id=self.instance_id,
            pid=os.getpid(),
            application_version=self.application_version,
            configuration_hash=self.configuration_hash,
            now_utc=now,
        )

    def _read_health(
        self,
        managed: ManagedServiceV1,
    ) -> ServiceHealthV1:
        try:
            health = self.health_reader(
                managed.spec.health_file,
                expected_service=managed.spec.service_name,
            )
        except Exception as exc:
            raise TargetSupervisorError(
                f"cannot read {managed.spec.service_name} health: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if health.deployment_id != self.deployment_id:
            raise TargetSupervisorError(
                f"{managed.spec.service_name} health deployment mismatch: "
                f"expected={self.deployment_id}, actual={health.deployment_id}"
            )
        if health.pid != managed.process.pid:
            raise TargetSupervisorError(
                f"{managed.spec.service_name} health pid mismatch: "
                f"expected={managed.process.pid}, actual={health.pid}"
            )
        return health

    def _wait_for_running(self, managed: ManagedServiceV1) -> ServiceHealthV1:
        deadline = self.monotonic() + self.policy.startup_timeout_seconds
        last_error: Exception | None = None
        while self.monotonic() < deadline:
            exit_code = managed.process.poll()
            if exit_code is not None:
                raise TargetSupervisorError(
                    f"{managed.spec.service_name} exited during startup: "
                    f"exit_code={exit_code}, log={managed.spec.log_file}"
                )
            if managed.spec.health_file.is_file():
                try:
                    health = self._read_health(managed)
                    if health.liveness == Liveness.RUNNING:
                        return health
                    if health.liveness in {Liveness.FAILED, Liveness.STOPPED}:
                        raise TargetSupervisorError(
                            f"{managed.spec.service_name} became "
                            f"{health.liveness.value} during startup: "
                            f"{health.blocking_reason}"
                        )
                except Exception as exc:
                    last_error = exc
            self.sleep(min(0.25, self.policy.poll_interval_seconds))
        detail = "" if last_error is None else f"; last_error={last_error}"
        raise TargetSupervisorError(
            f"{managed.spec.service_name} did not publish RUNNING health within "
            f"{self.policy.startup_timeout_seconds:.3f}s{detail}"
        )

    def _manifest_payload(self) -> dict:
        return {
            "deployment_id": self.deployment_id,
            "supervisor_instance_id": self.instance_id,
            "supervisor_pid": os.getpid(),
            "application_version": self.application_version,
            "configuration_hash": self.configuration_hash,
            "services": [
                {
                    "service": item.spec.service_name,
                    "pid": item.process.pid,
                    "argv": list(item.spec.argv),
                    "health_file": str(item.spec.health_file),
                    "log_file": str(item.spec.log_file),
                    "started_at_utc": item.started_at_utc,
                }
                for item in self.managed
            ],
            "automatic_restart_enabled": False,
        }

    def _publish_manifest(self) -> None:
        atomic_write_json(self.manifest_file, self._manifest_payload())

    def launch_all(self) -> tuple[ServiceHealthV1, ...]:
        health_values = []
        for spec in self.specs:
            try:
                spec.health_file.unlink()
            except FileNotFoundError:
                pass
            process, log_handle = self.launcher.launch(spec)
            managed = ManagedServiceV1(
                spec=spec,
                process=process,
                log_handle=log_handle,
                started_at_utc=format_utc(utc_now()),
            )
            self.managed.append(managed)
            self._publish_manifest()
            health_values.append(self._wait_for_running(managed))
        self._publish_manifest()
        return tuple(health_values)

    def monitor_once(self) -> tuple[ServiceHealthV1, ...]:
        observed = utc_now()
        values = []
        for managed in self.managed:
            exit_code = managed.process.poll()
            if exit_code is not None:
                raise TargetSupervisorError(
                    f"{managed.spec.service_name} exited unexpectedly: "
                    f"exit_code={exit_code}, log={managed.spec.log_file}"
                )
            health = self._read_health(managed)
            if health.liveness != Liveness.RUNNING:
                raise TargetSupervisorError(
                    f"{managed.spec.service_name} liveness is "
                    f"{health.liveness.value}: {health.blocking_reason}"
                )
            age = (observed - parse_utc(health.last_heartbeat_at_utc)).total_seconds()
            if age < 0.0:
                age = 0.0
            if age > self.policy.heartbeat_max_age_seconds:
                raise TargetSupervisorError(
                    f"{managed.spec.service_name} heartbeat is stale: "
                    f"age={age:.3f}s, max={self.policy.heartbeat_max_age_seconds:.3f}s"
                )
            values.append(health)
        return tuple(values)

    @staticmethod
    def _aggregate_health(
        values: tuple[ServiceHealthV1, ...],
    ) -> tuple[Readiness, str | None]:
        blocked = [
            item.service
            for item in values
            if item.readiness == Readiness.BLOCKED
        ]
        if blocked:
            return Readiness.BLOCKED, "blocked services: " + ",".join(blocked)
        degraded = [
            item.service
            for item in values
            if item.readiness != Readiness.READY
        ]
        if degraded:
            return Readiness.DEGRADED, "non-ready services: " + ",".join(degraded)
        return Readiness.READY, None

    def publish_running(
        self,
        values: tuple[ServiceHealthV1, ...],
    ) -> ServiceHealthV1:
        readiness, reason = self._aggregate_health(values)
        observed = format_utc(utc_now())
        self.health = self.health.heartbeat(
            now_utc=observed,
            liveness=Liveness.RUNNING,
            readiness=readiness,
            last_success_at_utc=observed,
            dependency_status=tuple(
                DependencyStatusV1(
                    name=item.service,
                    status=item.readiness.value,
                    detail=item.blocking_reason,
                    observed_at_utc=item.last_heartbeat_at_utc,
                )
                for item in values
            ),
            blocking_reason=reason,
        )
        self.health_publisher.publish(self.health)
        return self.health

    def publish_failed(self, reason: str) -> ServiceHealthV1:
        self.health = self.health.heartbeat(
            now_utc=format_utc(utc_now()),
            liveness=Liveness.FAILED,
            readiness=Readiness.BLOCKED,
            blocking_reason=str(reason or "target supervisor failed"),
        )
        self.health_publisher.publish(self.health)
        return self.health

    def shutdown(self) -> None:
        for managed in reversed(self.managed):
            if managed.process.poll() is None:
                try:
                    managed.process.terminate()
                except Exception:
                    pass
        deadline = self.monotonic() + self.policy.shutdown_timeout_seconds
        for managed in reversed(self.managed):
            if managed.process.poll() is not None:
                continue
            remaining = max(0.0, deadline - self.monotonic())
            try:
                managed.process.wait(timeout=remaining)
            except Exception:
                try:
                    managed.process.kill()
                except Exception:
                    pass
        for managed in self.managed:
            try:
                managed.log_handle.close()
            except Exception:
                pass

    def run_forever(self) -> None:
        self.health_publisher.publish(self.health)
        try:
            initial = self.launch_all()
            self.publish_running(initial)
            while True:
                self.sleep(self.policy.poll_interval_seconds)
                values = self.monitor_once()
                self.publish_running(values)
        except BaseException as exc:
            if not isinstance(exc, KeyboardInterrupt):
                try:
                    self.publish_failed(
                        f"{type(exc).__name__}: {exc}"
                    )
                except Exception:
                    pass
            raise
        finally:
            self.shutdown()
