from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ibmd.foundation.time import format_utc, utc_now
from ibmd.operations.supervisor import (
    SupervisorPolicyV1,
    SupervisorServiceSpecV1,
    TargetStackSupervisor,
    TargetSupervisorError,
)
from ibmd.public_contracts.health import (
    Liveness,
    Readiness,
    ServiceHealthV1,
)


class FakeProcess:
    def __init__(self, pid: int, events: list[str], name: str) -> None:
        self.pid = pid
        self.events = events
        self.name = name
        self.exit_code = None

    def poll(self):
        return self.exit_code

    def terminate(self):
        self.events.append(f"terminate:{self.name}")
        self.exit_code = 0

    def kill(self):
        self.events.append(f"kill:{self.name}")
        self.exit_code = -9

    def wait(self, timeout=None):
        if self.exit_code is None:
            raise TimeoutError(timeout)
        return self.exit_code


class FakeLog:
    def __init__(self, events: list[str], name: str) -> None:
        self.events = events
        self.name = name

    def close(self):
        self.events.append(f"close:{self.name}")


class FakeLauncher:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.processes = {}

    def launch(self, spec):
        self.events.append(f"launch:{spec.service_name}")
        spec.health_file.parent.mkdir(parents=True, exist_ok=True)
        spec.health_file.touch()
        process = FakeProcess(
            1_000 + len(self.processes),
            self.events,
            spec.service_name,
        )
        self.processes[spec.service_name] = process
        return process, FakeLog(self.events, spec.service_name)


class FakeHealthPublisher:
    def __init__(self) -> None:
        self.values = []

    def publish(self, health):
        self.values.append(health)


class FakeHealthReader:
    def __init__(self, values) -> None:
        self.values = values

    def __call__(self, path, *, expected_service=None):
        value = self.values[Path(path)]
        if expected_service is not None and value.service != expected_service:
            raise ValueError("service mismatch")
        return value


def running_health(service: str, pid: int, *, heartbeat: str | None = None):
    now = heartbeat or format_utc(utc_now())
    value = ServiceHealthV1.starting(
        service=service,
        deployment_id="account1",
        instance_id=f"instance_{service}",
        pid=pid,
        application_version="test",
        configuration_hash="a" * 64,
        now_utc=now,
    )
    return value.heartbeat(
        now_utc=now,
        liveness=Liveness.RUNNING,
        readiness=Readiness.READY,
        last_success_at_utc=now,
    )


class TargetSupervisorTest(unittest.TestCase):
    def build(self, root: Path, names=("market_data", "execution")):
        events = []
        launcher = FakeLauncher(events)
        specs = tuple(
            SupervisorServiceSpecV1(
                service_name=name,
                argv=("python", f"{name}.py"),
                health_file=root / "health" / f"{name}.json",
                log_file=root / "logs" / f"{name}.log",
            )
            for name in names
        )
        values = {}
        for index, spec in enumerate(specs):
            spec.health_file.parent.mkdir(parents=True, exist_ok=True)
            spec.health_file.touch()
            values[spec.health_file] = running_health(
                spec.service_name,
                1_000 + index,
            )
        publisher = FakeHealthPublisher()
        supervisor = TargetStackSupervisor(
            deployment_id="account1",
            application_version="test",
            configuration_hash="b" * 64,
            instance_id="instance_supervisor",
            specs=specs,
            policy=SupervisorPolicyV1(
                startup_timeout_seconds=1.0,
                heartbeat_max_age_seconds=30.0,
                poll_interval_seconds=0.01,
                shutdown_timeout_seconds=1.0,
            ),
            launcher=launcher,
            health_reader=FakeHealthReader(values),
            health_publisher=publisher,
            manifest_file=root / "runtime" / "supervisor.json",
            sleep=lambda _value: None,
        )
        return supervisor, launcher, values, events, publisher

    def test_launch_order_monitor_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            supervisor, launcher, _values, events, publisher = self.build(root)
            health = supervisor.launch_all()
            self.assertEqual(
                events[:2],
                ["launch:market_data", "launch:execution"],
            )
            self.assertEqual([item.service for item in health], ["market_data", "execution"])
            monitored = supervisor.monitor_once()
            published = supervisor.publish_running(monitored)
            self.assertEqual(published.readiness, Readiness.READY)
            self.assertTrue((root / "runtime" / "supervisor.json").is_file())
            self.assertEqual(len(publisher.values), 1)
            self.assertEqual(set(launcher.processes), {"market_data", "execution"})

    def test_unexpected_child_exit_stops_monitoring(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            supervisor, launcher, _values, _events, _publisher = self.build(
                Path(directory)
            )
            supervisor.launch_all()
            launcher.processes["execution"].exit_code = 7
            with self.assertRaisesRegex(
                TargetSupervisorError,
                "execution exited unexpectedly",
            ):
                supervisor.monitor_once()

    def test_stale_health_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            supervisor, _launcher, values, _events, _publisher = self.build(root)
            supervisor.launch_all()
            execution_file = root / "health" / "execution.json"
            values[execution_file] = running_health(
                "execution",
                1_001,
                heartbeat="2020-01-01T00:00:00Z",
            )
            with self.assertRaisesRegex(
                TargetSupervisorError,
                "heartbeat is stale",
            ):
                supervisor.monitor_once()

    def test_shutdown_is_reverse_order_without_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            supervisor, _launcher, _values, events, _publisher = self.build(
                Path(directory),
                names=("market_data", "signal", "execution"),
            )
            supervisor.launch_all()
            supervisor.shutdown()
            terminated = [item for item in events if item.startswith("terminate:")]
            self.assertEqual(
                terminated,
                [
                    "terminate:execution",
                    "terminate:signal",
                    "terminate:market_data",
                ],
            )
            self.assertNotIn("restart", " ".join(events))

    def test_blocked_child_degrades_supervisor_without_killing_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            supervisor, _launcher, values, _events, _publisher = self.build(root)
            supervisor.launch_all()
            execution_file = root / "health" / "execution.json"
            current = values[execution_file]
            values[execution_file] = current.heartbeat(
                now_utc=format_utc(utc_now()),
                liveness=Liveness.RUNNING,
                readiness=Readiness.BLOCKED,
                blocking_reason="paper mutation gate is disabled",
            )
            readiness, reason = supervisor._aggregate_health(
                supervisor.monitor_once()
            )
            self.assertEqual(readiness, Readiness.BLOCKED)
            self.assertIn("execution", reason)


if __name__ == "__main__":
    unittest.main()
