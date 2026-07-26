from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.operations.health import ServiceHealthFile, read_service_health
from ibmd.operations.supervisor import (
    SubprocessServiceLauncher,
    SupervisorPolicyV1,
    SupervisorServiceSpecV1,
    TargetStackSupervisor,
    TargetSupervisorError,
)

SERVICE_NAME = "supervisor"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Launch and monitor one complete target deployment. The supervisor "
            "reads health JSON only, never reads trading databases and never "
            "restarts a failed execution process automatically."
        )
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--startup-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--heartbeat-max-age-seconds", type=float, default=30.0)
    parser.add_argument("--shutdown-timeout-seconds", type=float, default=15.0)
    return parser


def _required_target_paths(data_root: Path) -> tuple[Path, ...]:
    return (
        data_root / "runtime" / "bootstrap.json",
        data_root / "catalog" / "instruments.v1.json",
        data_root / "catalog" / "contracts.mnq.v1.json",
        data_root / "catalog" / "sessions.v1.json",
        data_root / "catalog" / "strategy.IBMarketData.rolling.v1.json",
        data_root / "market_data" / "MNQ.sqlite3",
        data_root / "position_feed" / "broker_positions.sqlite3",
        data_root / "signal" / "signal.sqlite3",
        data_root / "decision" / "decision.sqlite3",
        data_root / "execution" / "execution.sqlite3",
    )


def _validate_target_root(data_root: Path) -> None:
    missing = [str(path) for path in _required_target_paths(data_root) if not path.is_file()]
    if missing:
        raise TargetSupervisorError(
            "target deployment is not bootstrapped; missing=" + repr(missing)
        )


def _service_specs(
    *,
    data_root: Path,
    environment: str,
) -> tuple[SupervisorServiceSpecV1, ...]:
    logs = data_root / "runtime" / "logs"
    health = data_root / "runtime" / "health"
    execution_argv = [
        sys.executable,
        str(ROOT / "apps" / "run_execution_runtime_v2.py"),
        "--continuous",
    ]
    session_override = environment in {"development", "test", "paper"}
    if session_override:
        execution_argv.append("--allow-unqualified-session")
    commands = (
        (
            "market_data",
            (
                sys.executable,
                str(ROOT / "apps" / "run_market_data_v2.py"),
            ),
        ),
        (
            "broker_position_feed",
            (
                sys.executable,
                str(ROOT / "apps" / "run_position_feed_v2.py"),
            ),
        ),
        (
            "signal",
            (
                sys.executable,
                str(ROOT / "apps" / "run_signal_v2.py"),
            ),
        ),
        (
            "decision",
            (
                sys.executable,
                str(ROOT / "apps" / "run_decision_runtime_v2.py"),
            ),
        ),
        ("execution", tuple(execution_argv)),
    )
    specs = tuple(
        SupervisorServiceSpecV1(
            service_name=name,
            argv=argv,
            health_file=health / f"{name}.json",
            log_file=logs / f"{name}.log",
        )
        for name, argv in commands
    )
    for spec in specs:
        executable = Path(spec.argv[1])
        if not executable.is_file():
            raise TargetSupervisorError(
                f"service entrypoint does not exist: {executable}"
            )
    return specs


def _configuration_hash(
    *,
    deployment_hash: str,
    specs: tuple[SupervisorServiceSpecV1, ...],
    policy: SupervisorPolicyV1,
) -> str:
    payload = {
        "deployment_hash": deployment_hash,
        "services": [
            {
                "service": item.service_name,
                "argv": list(item.argv),
                "health_file": str(item.health_file),
                "log_file": str(item.log_file),
            }
            for item in specs
        ],
        "policy": {
            "startup_timeout_seconds": policy.startup_timeout_seconds,
            "heartbeat_max_age_seconds": policy.heartbeat_max_age_seconds,
            "poll_interval_seconds": policy.poll_interval_seconds,
            "shutdown_timeout_seconds": policy.shutdown_timeout_seconds,
        },
        "automatic_restart_enabled": False,
        "database_access": False,
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _plan_payload(
    *,
    settings,
    specs: tuple[SupervisorServiceSpecV1, ...],
    policy: SupervisorPolicyV1,
) -> dict:
    return {
        "deployment_id": settings.deployment_id,
        "environment": settings.environment,
        "data_root": str(settings.data_root),
        "services": [
            {
                "service": item.service_name,
                "argv": list(item.argv),
                "health_file": str(item.health_file),
                "log_file": str(item.log_file),
            }
            for item in specs
        ],
        "policy": {
            "startup_timeout_seconds": policy.startup_timeout_seconds,
            "heartbeat_max_age_seconds": policy.heartbeat_max_age_seconds,
            "poll_interval_seconds": policy.poll_interval_seconds,
            "shutdown_timeout_seconds": policy.shutdown_timeout_seconds,
        },
        "automatic_restart_enabled": False,
        "trading_database_access": False,
        "continuous_broker_mutations_enabled": False,
    }


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    _validate_target_root(settings.data_root)
    policy = SupervisorPolicyV1(
        startup_timeout_seconds=arguments.startup_timeout_seconds,
        heartbeat_max_age_seconds=arguments.heartbeat_max_age_seconds,
        poll_interval_seconds=arguments.poll_interval_seconds,
        shutdown_timeout_seconds=arguments.shutdown_timeout_seconds,
    )
    specs = _service_specs(
        data_root=settings.data_root,
        environment=settings.environment,
    )
    plan = _plan_payload(settings=settings, specs=specs, policy=policy)
    if arguments.validate_only or arguments.print_plan:
        print(
            json.dumps(
                plan,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0

    configuration_hash = _configuration_hash(
        deployment_hash=settings.configuration_hash,
        specs=specs,
        policy=policy,
    )
    instance_id = new_id("instance")
    supervisor = TargetStackSupervisor(
        deployment_id=settings.deployment_id,
        application_version=settings.application_version,
        configuration_hash=configuration_hash,
        instance_id=instance_id,
        specs=specs,
        policy=policy,
        launcher=SubprocessServiceLauncher(working_directory=ROOT),
        health_reader=read_service_health,
        health_publisher=ServiceHealthFile(
            settings.paths_for(SERVICE_NAME).health_file,
            expected_service=SERVICE_NAME,
        ),
        manifest_file=settings.data_root / "runtime" / "supervisor.json",
    )
    with ServiceProcessLock(
        settings.paths_for(SERVICE_NAME).lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        supervisor.run_forever()
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = sum(
        int(value)
        for value in (
            arguments.validate_only,
            arguments.print_plan,
            arguments.continuous,
        )
    )
    if selected != 1:
        print(
            "target supervisor requires exactly one mode: "
            "--validate-only, --print-plan or --continuous",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except KeyboardInterrupt:
        return 130
    except (TargetSupervisorError, ValueError) as exc:
        print(
            f"target supervisor failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
