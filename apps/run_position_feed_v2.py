from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.config import load_deployment_settings
from ibmd.foundation.identity import new_id
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.ib_gateway.ib_async_positions import (
    IBAsyncPositionReader,
    IBPositionConnectionSettings,
)
from ibmd.operations.health import ServiceHealthFile
from ibmd.position_feed import (
    BrokerPositionFeedConfig,
    BrokerPositionFeedService,
)
from ibmd.position_feed.adapters import (
    PositionSnapshotSchemaError,
    SQLiteBrokerPositionSnapshotStore,
)

SERVICE_NAME = "broker_position_feed"
DEFAULT_CLIENT_ID_OFFSET = 60


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the target raw broker position feed in shadow mode. "
            "This service never submits broker orders and never writes "
            "legacy positions_latest."
        )
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="publish one COMPLETE snapshot and exit",
    )
    parser.add_argument(
        "--validate-store-only",
        action="store_true",
        help="validate the pre-migrated SQLite store and exit without IB",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help=(
            "position-feed database path; default is "
            "<IBMD_DATA_ROOT>/position_feed/broker_positions.sqlite3"
        ),
    )
    parser.add_argument(
        "--client-id-offset",
        type=int,
        default=DEFAULT_CLIENT_ID_OFFSET,
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=float,
        default=20.0,
    )
    parser.add_argument(
        "--snapshot-max-age-seconds",
        type=float,
        default=10.0,
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=15.0,
    )
    parser.add_argument(
        "--account-timeout-seconds",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--position-timeout-seconds",
        type=float,
        default=15.0,
    )
    return parser


def migration_command(
    *,
    database_path: Path,
    application_version: str,
) -> str:
    manifest = ROOT / "migrations" / "position_feed.v1.json"
    return (
        f"{sys.executable} {ROOT / 'scripts' / 'run_target_migrations.py'} "
        f"--manifest {manifest} "
        f"--database {database_path} "
        f"--application-version {application_version} --apply"
    )


async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    paths = settings.paths_for(SERVICE_NAME)
    database_path = (
        arguments.database.resolve()
        if arguments.database is not None
        else (
            settings.data_root
            / "position_feed"
            / "broker_positions.sqlite3"
        )
    )
    instance_id = new_id("instance")
    config = BrokerPositionFeedConfig(
        account_id=settings.ib_account_id,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
        application_version=settings.application_version,
        configuration_hash=settings.configuration_hash,
        poll_interval_seconds=arguments.poll_interval_seconds,
        poll_timeout_seconds=arguments.poll_timeout_seconds,
        snapshot_max_age_seconds=arguments.snapshot_max_age_seconds,
    )
    repository = SQLiteBrokerPositionSnapshotStore(database_path)
    health_file = ServiceHealthFile(
        paths.health_file,
        expected_service=SERVICE_NAME,
    )

    client_id = settings.ib_client_id + int(arguments.client_id_offset)
    if client_id < 0:
        raise ValueError(
            f"resolved position-feed client id must be non-negative: {client_id}"
        )
    reader = IBAsyncPositionReader(
        IBPositionConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=arguments.connect_timeout_seconds,
            account_timeout_seconds=arguments.account_timeout_seconds,
            position_timeout_seconds=arguments.position_timeout_seconds,
        )
    )
    service = BrokerPositionFeedService(
        config=config,
        reader=reader,
        repository=repository,
        health_publisher=health_file,
    )

    with ServiceProcessLock(
        paths.lock_file,
        service_name=SERVICE_NAME,
        deployment_id=settings.deployment_id,
        instance_id=instance_id,
    ):
        service.publish_starting()
        try:
            repository.validate_schema()
        except PositionSnapshotSchemaError as exc:
            reason = f"position-feed store is not ready: {exc}"
            service.publish_blocked(reason)
            print(reason, file=sys.stderr)
            print(
                "Apply the explicit offline migration before startup:",
                file=sys.stderr,
            )
            print(
                migration_command(
                    database_path=database_path,
                    application_version=settings.application_version,
                ),
                file=sys.stderr,
            )
            return 2

        if arguments.validate_store_only:
            print(
                f"position-feed store is compatible: {database_path}"
            )
            service.publish_stopping()
            await reader.close()
            service.publish_stopped()
            return 0

        failed = False
        try:
            if arguments.once:
                snapshot = await service.poll_once()
                print(
                    "published broker position snapshot: "
                    f"snapshot_id={snapshot.snapshot_id}, "
                    f"account={snapshot.account_id}, "
                    f"rows={snapshot.row_count}, "
                    f"captured_at={snapshot.captured_at_utc}"
                )
            else:
                await service.run_forever(publish_initial=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failed = True
            try:
                service.publish_failed(
                    f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                pass
            raise
        finally:
            if not failed:
                try:
                    service.publish_stopping()
                except Exception:
                    pass
            await reader.close()
            if not failed:
                try:
                    service.publish_stopped()
                except Exception:
                    pass

    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        ),
    )
    arguments = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(arguments))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
