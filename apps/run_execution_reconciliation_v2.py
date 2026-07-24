from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.execution.adapters import (
    BrokerAttemptSchemaError,
    BrokerAttemptStoreError,
    SQLiteBrokerAttemptStore,
)
from ibmd.execution.adapters.sqlite_broker_reconciliation import (
    BrokerReconciliationSchemaError,
    BrokerReconciliationStoreError,
)
from ibmd.execution.adapters.sqlite_broker_reconciliation_store import (
    SQLiteBrokerReconciliationStore,
)
from ibmd.execution.application.read_only_reconciliation import (
    ReadOnlyBrokerReconciliationService,
    reconciliation_run_payload,
)
from ibmd.execution.domain import (
    BrokerAttemptDomainError,
    BrokerReconciliationDomainError,
)
from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.ib_gateway.broker_reconciliation import BrokerReconciliationReadError
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)

SERVICE_NAME = "execution"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read Interactive Brokers open/completed orders and executions, "
            "then reconcile existing target execution attempts without placing "
            "or cancelling any broker order."
        )
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate execution migration v2 and exit without connecting to IB",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="perform one read-only IB snapshot and reconciliation pass",
    )
    parser.add_argument("--execution-database", type=Path, default=None)
    parser.add_argument("--client-id-offset", type=int, default=100)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--commission-wait-seconds", type=float, default=2.0)
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {ROOT / 'migrations' / 'execution.v1.json'} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    execution_database = (
        arguments.execution_database.resolve()
        if arguments.execution_database is not None
        else settings.data_root / "execution" / "execution.sqlite3"
    )
    attempt_store = SQLiteBrokerAttemptStore(execution_database)
    reconciliation_store = SQLiteBrokerReconciliationStore(execution_database)
    try:
        attempt_store.validate_schema()
        reconciliation_store.validate_schema()
    except (BrokerAttemptSchemaError, BrokerReconciliationSchemaError) as exc:
        print(
            f"execution reconciliation store is not ready: {exc}",
            file=sys.stderr,
        )
        print(
            "Apply the explicit offline migration before startup:",
            file=sys.stderr,
        )
        print(
            migration_command(
                database_path=execution_database,
                application_version=settings.application_version,
            ),
            file=sys.stderr,
        )
        return 2

    if arguments.validate_store_only:
        print(
            "execution read-only reconciliation store is compatible: "
            f"execution={execution_database}"
        )
        return 0

    client_id = settings.ib_client_id + int(arguments.client_id_offset)
    if client_id < 0:
        raise ValueError("resolved reconciliation client id must be non-negative")
    timeout = float(arguments.request_timeout_seconds)
    reader = IBAsyncBrokerReconciliationReader(
        IBBrokerReconciliationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
            open_orders_timeout_seconds=timeout,
            completed_orders_timeout_seconds=timeout,
            executions_timeout_seconds=timeout,
            commission_wait_seconds=float(arguments.commission_wait_seconds),
        )
    )
    instance_id = new_id("instance")
    lock_file = settings.paths_for(SERVICE_NAME).lock_file
    try:
        with ServiceProcessLock(
            lock_file,
            service_name=SERVICE_NAME,
            deployment_id=settings.deployment_id,
            instance_id=instance_id,
        ):
            service = ReadOnlyBrokerReconciliationService(
                account_id=settings.ib_account_id,
                broker_source=reader,
                attempt_source=attempt_store,
                reconciliation_store=reconciliation_store,
            )
            result = await service.run_once()
            print(
                json.dumps(
                    reconciliation_run_payload(result),
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            return 0
    finally:
        await reader.close()


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = int(bool(arguments.validate_store_only)) + int(bool(arguments.once))
    if selected != 1:
        print(
            "read-only execution reconciliation requires exactly one of "
            "--validate-store-only or --once",
            file=sys.stderr,
        )
        return 2
    try:
        return asyncio.run(run(arguments))
    except (
        BrokerAttemptDomainError,
        BrokerAttemptStoreError,
        BrokerReconciliationDomainError,
        BrokerReconciliationReadError,
        BrokerReconciliationStoreError,
        ValueError,
    ) as exc:
        print(
            "read-only execution reconciliation failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
