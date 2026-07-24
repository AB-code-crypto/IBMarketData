from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.identity import new_id
from ibmd.ib_gateway import (
    BrokerPositionReadError,
    RawBrokerPosition,
    ScriptedPositionReader,
)
from ibmd.ib_gateway.ib_async_positions import (
    IBAsyncPositionReader,
    IBPositionConnectionSettings,
    raw_position_from_ib,
)
from ibmd.operations.health import ServiceHealthFile
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.position_feed import (
    BrokerPositionFeedConfig,
    BrokerPositionFeedService,
    PositionFeedPollError,
)
from ibmd.position_feed.adapters import (
    PositionSnapshotSchemaError,
    PositionSnapshotStoreError,
    SQLiteBrokerPositionSnapshotStore,
)
from ibmd.public_contracts import (
    BrokerPositionContractError,
    BrokerPositionRowV1,
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)

ACCOUNT_ID = "U0000000"
MIGRATION_MANIFEST = ROOT / "migrations" / "position_feed.v1.json"


def raw_position(
    *,
    con_id: int = 793356225,
    local_symbol: str | None = "MNQU6",
    symbol: str = "MNQ",
    sec_type: str = "FUT",
    exchange: str | None = "CME",
    currency: str | None = "USD",
    signed_quantity: float = 1.0,
    average_cost: float | None = 20_000.0,
    account_id: str = ACCOUNT_ID,
) -> RawBrokerPosition:
    return RawBrokerPosition(
        account_id=account_id,
        con_id=con_id,
        local_symbol=local_symbol,
        symbol=symbol,
        sec_type=sec_type,
        exchange=exchange,
        currency=currency,
        signed_quantity=signed_quantity,
        average_cost=average_cost,
    )


def complete_snapshot(
    *,
    captured_at_utc: str = "2026-07-23T10:00:00.000000Z",
    rows: tuple[BrokerPositionRowV1, ...] | None = None,
) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=ACCOUNT_ID,
        captured_at_utc=captured_at_utc,
        published_at_utc=captured_at_utc,
        source_session_id=new_id("ib_session"),
        rows=(
            (raw_position().to_public_row(),)
            if rows is None
            else rows
        ),
    )


def apply_position_feed_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(
        MIGRATION_MANIFEST
    )
    self_check = SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()
    if not self_check.is_current:
        raise AssertionError("position-feed test schema is not current")


class CapturingHealthPublisher:
    def __init__(self) -> None:
        self.values: list[ServiceHealthV1] = []

    def publish(self, health: ServiceHealthV1) -> None:
        self.values.append(health)


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self.values = deque(values)
        if not self.values:
            raise ValueError("SequenceClock requires values")
        self.last = self.values[-1]

    def __call__(self) -> datetime:
        if self.values:
            self.last = self.values.popleft()
        return self.last


class PositionContractTest(unittest.TestCase):
    def test_complete_snapshot_round_trip_sorting_and_freshness(self):
        rows = (
            raw_position(
                con_id=9002,
                local_symbol=None,
                symbol="EUR",
                sec_type="CASH",
                exchange="IDEALPRO",
                signed_quantity=-25_000.0,
                average_cost=1.08,
            ).to_public_row(),
            raw_position(con_id=9001).to_public_row(),
        )
        snapshot = complete_snapshot(rows=rows)
        self.assertEqual(
            [item.con_id for item in snapshot.rows],
            [9001, 9002],
        )
        restored = BrokerPositionSnapshotV1.from_dict(
            snapshot.to_dict()
        )
        self.assertEqual(restored, snapshot)

        exact = snapshot.freshness(
            observed_at_utc="2026-07-23T10:00:10.000000Z",
            max_age_seconds=10.0,
        )
        late = snapshot.freshness(
            observed_at_utc="2026-07-23T10:00:10.000001Z",
            max_age_seconds=10.0,
        )
        self.assertTrue(exact.is_fresh)
        self.assertFalse(late.is_fresh)

    def test_failed_snapshot_and_invalid_rows_are_strict(self):
        failed = BrokerPositionSnapshotV1.failed(
            snapshot_id=new_id("position_snapshot"),
            account_id=ACCOUNT_ID,
            captured_at_utc="2026-07-23T10:00:00.000000Z",
            published_at_utc="2026-07-23T10:00:00.000000Z",
            source_session_id=new_id("ib_session"),
            error_text="ConnectionError: offline",
        )
        self.assertEqual(
            failed.status,
            BrokerPositionSnapshotStatus.FAILED,
        )
        self.assertEqual(failed.rows, ())

        with self.assertRaisesRegex(
            BrokerPositionContractError,
            "zero signed_quantity",
        ):
            BrokerPositionRowV1(
                con_id=1,
                local_symbol="MNQU6",
                symbol="MNQ",
                sec_type="FUT",
                exchange="CME",
                currency="USD",
                signed_quantity=0.0,
                average_cost=None,
            )

        duplicate = raw_position(con_id=1).to_public_row()
        with self.assertRaisesRegex(
            BrokerPositionContractError,
            "duplicate con_id",
        ):
            complete_snapshot(rows=(duplicate, duplicate))

        invalid = failed.to_dict()
        invalid["unexpected"] = True
        with self.assertRaisesRegex(
            BrokerPositionContractError,
            "unknown",
        ):
            BrokerPositionSnapshotV1.from_dict(invalid)


class IBPositionAdapterTest(unittest.IsolatedAsyncioTestCase):
    def test_raw_ib_mapping_preserves_broker_facts(self):
        contract = SimpleNamespace(
            conId=793356225,
            localSymbol="MNQU6",
            symbol="MNQ",
            secType="FUT",
            exchange="CME",
            primaryExchange="",
            currency="USD",
        )
        value = SimpleNamespace(
            account=ACCOUNT_ID,
            contract=contract,
            position=-2.0,
            avgCost=20_100.25,
        )
        mapped = raw_position_from_ib(
            value,
            expected_account_id=ACCOUNT_ID,
        )
        self.assertEqual(mapped.con_id, 793356225)
        self.assertEqual(mapped.local_symbol, "MNQU6")
        self.assertEqual(mapped.signed_quantity, -2.0)
        self.assertEqual(mapped.average_cost, 20_100.25)

        other = SimpleNamespace(
            account="U9999999",
            contract=contract,
            position=1.0,
            avgCost=20_000.0,
        )
        self.assertIsNone(
            raw_position_from_ib(
                other,
                expected_account_id=ACCOUNT_ID,
            )
        )
        zero = SimpleNamespace(
            account=ACCOUNT_ID,
            contract=contract,
            position=0.0,
            avgCost=20_000.0,
        )
        self.assertIsNone(
            raw_position_from_ib(
                zero,
                expected_account_id=ACCOUNT_ID,
            )
        )

    async def test_ib_async_reader_connects_validates_and_filters_account(self):
        contract = SimpleNamespace(
            conId=793356225,
            localSymbol="MNQU6",
            symbol="MNQ",
            secType="FUT",
            exchange="CME",
            primaryExchange="",
            currency="USD",
        )

        class FakeIB:
            def __init__(self):
                self.connected = False
                self.connect_calls: list[dict] = []
                self.disconnect_count = 0

            async def connectAsync(self, **kwargs):
                self.connect_calls.append(kwargs)
                self.connected = True

            def isConnected(self):
                return self.connected

            def managedAccounts(self):
                return [ACCOUNT_ID]

            async def reqPositionsAsync(self):
                return [
                    SimpleNamespace(
                        account=ACCOUNT_ID,
                        contract=contract,
                        position=1.0,
                        avgCost=20_000.0,
                    ),
                    SimpleNamespace(
                        account="U9999999",
                        contract=contract,
                        position=5.0,
                        avgCost=20_000.0,
                    ),
                ]

            def positions(self):
                raise AssertionError(
                    "positions cache must not replace a non-None response"
                )

            def disconnect(self):
                self.disconnect_count += 1
                self.connected = False

        fake = FakeIB()
        reader = IBAsyncPositionReader(
            IBPositionConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=260,
                account_id=ACCOUNT_ID,
            ),
            ib_factory=lambda: fake,
        )
        first_session = reader.source_session_id
        rows = await reader.read_positions(account_id=ACCOUNT_ID)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].account_id, ACCOUNT_ID)
        self.assertEqual(fake.connect_calls[0]["clientId"], 260)
        self.assertNotEqual(reader.source_session_id, first_session)

        with self.assertRaisesRegex(
            BrokerPositionReadError,
            "account mismatch",
        ):
            await reader.read_positions(account_id="U1111111")
        await reader.close()
        self.assertFalse(reader.connected)

    async def test_position_timeout_invalidates_connection(self):
        class SlowIB:
            def __init__(self):
                self.connected = False
                self.cancelled = False

            async def connectAsync(self, **kwargs):
                _ = kwargs
                self.connected = True

            def isConnected(self):
                return self.connected

            def managedAccounts(self):
                return [ACCOUNT_ID]

            async def reqPositionsAsync(self):
                await asyncio.sleep(1.0)
                return []

            def cancelPositions(self):
                self.cancelled = True

            def disconnect(self):
                self.connected = False

        fake = SlowIB()
        reader = IBAsyncPositionReader(
            IBPositionConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=260,
                account_id=ACCOUNT_ID,
                position_timeout_seconds=0.01,
            ),
            ib_factory=lambda: fake,
        )
        with self.assertRaisesRegex(
            BrokerPositionReadError,
            "timed out",
        ):
            await reader.read_positions(account_id=ACCOUNT_ID)
        self.assertTrue(fake.cancelled)
        self.assertFalse(reader.connected)


class PositionSnapshotStoreTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = (
            Path(self.temp_dir.name)
            / "position_feed"
            / "broker_positions.sqlite3"
        )
        apply_position_feed_schema(self.database)
        self.store = SQLiteBrokerPositionSnapshotStore(
            self.database
        )
        self.store.validate_schema()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_complete_failure_and_empty_complete_pointer_semantics(self):
        first = complete_snapshot()
        self.store.publish(first)
        self.assertEqual(
            self.store.read_latest_complete(),
            first,
        )

        failed = BrokerPositionSnapshotV1.failed(
            snapshot_id=new_id("position_snapshot"),
            account_id=ACCOUNT_ID,
            captured_at_utc="2026-07-23T10:00:02.000000Z",
            published_at_utc="2026-07-23T10:00:02.000000Z",
            source_session_id=new_id("ib_session"),
            error_text="ConnectionError: backend down",
        )
        self.store.publish(failed)
        self.assertEqual(
            self.store.read_attempt_status(failed.snapshot_id),
            "FAILED",
        )
        self.assertEqual(
            self.store.read_latest_complete().snapshot_id,
            first.snapshot_id,
        )

        empty = complete_snapshot(
            captured_at_utc="2026-07-23T10:00:04.000000Z",
            rows=(),
        )
        self.store.publish(empty)
        latest = self.store.read_latest_complete()
        self.assertEqual(latest.snapshot_id, empty.snapshot_id)
        self.assertEqual(latest.row_count, 0)
        self.assertEqual(latest.rows, ())
        self.assertEqual(self.store.count_attempts(), 3)

    def test_duplicate_snapshot_publish_rolls_back_without_pointer_change(self):
        first = complete_snapshot()
        self.store.publish(first)
        with self.assertRaises(PositionSnapshotStoreError):
            self.store.publish(first)
        latest = self.store.read_latest_complete()
        self.assertEqual(latest.snapshot_id, first.snapshot_id)
        self.assertEqual(self.store.count_attempts(), 1)

    def test_unmigrated_store_is_rejected_without_runtime_mutation(self):
        missing = (
            Path(self.temp_dir.name)
            / "missing"
            / "broker_positions.sqlite3"
        )
        store = SQLiteBrokerPositionSnapshotStore(missing)
        with self.assertRaises(PositionSnapshotSchemaError):
            store.validate_schema()
        self.assertFalse(missing.exists())


class PositionFeedServiceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = (
            Path(self.temp_dir.name)
            / "broker_positions.sqlite3"
        )
        apply_position_feed_schema(self.database)
        self.store = SQLiteBrokerPositionSnapshotStore(
            self.database
        )
        self.health = CapturingHealthPublisher()
        self.start = datetime(
            2026,
            7,
            23,
            10,
            0,
            0,
            tzinfo=timezone.utc,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def config(
        self,
        *,
        max_age: float = 10.0,
        timeout: float = 1.0,
    ) -> BrokerPositionFeedConfig:
        return BrokerPositionFeedConfig(
            account_id=ACCOUNT_ID,
            deployment_id="paper-mnq-01",
            instance_id=new_id("instance"),
            application_version="0.1.0-dev",
            configuration_hash="a" * 64,
            poll_interval_seconds=2.0,
            poll_timeout_seconds=timeout,
            snapshot_max_age_seconds=max_age,
        )

    async def test_success_publishes_all_raw_positions_without_robot_projection(self):
        rows = (
            raw_position(),
            raw_position(
                con_id=5001,
                local_symbol=None,
                symbol="EUR",
                sec_type="CASH",
                exchange="IDEALPRO",
                signed_quantity=-25_000.0,
                average_cost=1.08,
            ),
        )
        reader = ScriptedPositionReader([rows])
        service = BrokerPositionFeedService(
            config=self.config(),
            reader=reader,
            repository=self.store,
            health_publisher=self.health,
            clock=SequenceClock(
                self.start,
                self.start + timedelta(seconds=1),
            ),
        )
        service.publish_starting()
        snapshot = await service.poll_once()
        self.assertEqual(snapshot.row_count, 2)
        self.assertEqual(
            {item.sec_type for item in snapshot.rows},
            {"FUT", "CASH"},
        )
        self.assertFalse(
            any(abs(item.signed_quantity) <= 1e-12 for item in snapshot.rows)
        )
        self.assertEqual(
            self.store.read_latest_complete(),
            snapshot,
        )
        self.assertEqual(service.health.readiness, Readiness.READY)
        self.assertEqual(
            service.health.dependency_status[0].status,
            "CONNECTED",
        )

    async def test_failure_never_moves_latest_complete_or_fakes_flat(self):
        reader = ScriptedPositionReader(
            [
                (raw_position(),),
                BrokerPositionReadError("backend down"),
                BrokerPositionReadError("backend still down"),
            ]
        )
        service = BrokerPositionFeedService(
            config=self.config(max_age=10.0),
            reader=reader,
            repository=self.store,
            health_publisher=self.health,
            clock=SequenceClock(
                self.start,
                self.start + timedelta(seconds=1),
                self.start + timedelta(seconds=5),
                self.start + timedelta(seconds=20),
            ),
        )
        service.publish_starting()
        first = await service.poll_once()

        with self.assertRaises(PositionFeedPollError):
            await service.poll_once()
        self.assertEqual(service.health.readiness, Readiness.DEGRADED)
        self.assertEqual(
            self.store.read_latest_complete().snapshot_id,
            first.snapshot_id,
        )

        with self.assertRaises(PositionFeedPollError):
            await service.poll_once()
        self.assertEqual(service.health.readiness, Readiness.BLOCKED)
        self.assertEqual(
            self.store.read_latest_complete().snapshot_id,
            first.snapshot_id,
        )
        self.assertEqual(self.store.count_attempts(), 3)

    async def test_initial_failure_is_blocked_and_empty_success_is_complete(self):
        failed_reader = ScriptedPositionReader(
            [BrokerPositionReadError("offline")]
        )
        failed_service = BrokerPositionFeedService(
            config=self.config(),
            reader=failed_reader,
            repository=self.store,
            health_publisher=self.health,
            clock=SequenceClock(
                self.start,
                self.start + timedelta(seconds=1),
            ),
        )
        with self.assertRaises(PositionFeedPollError):
            await failed_service.poll_once()
        self.assertEqual(
            failed_service.health.readiness,
            Readiness.BLOCKED,
        )
        self.assertIsNone(self.store.read_latest_complete())

        empty_reader = ScriptedPositionReader([()])
        empty_service = BrokerPositionFeedService(
            config=self.config(),
            reader=empty_reader,
            repository=self.store,
            health_publisher=self.health,
            clock=SequenceClock(
                self.start + timedelta(seconds=2),
                self.start + timedelta(seconds=3),
            ),
        )
        empty = await empty_service.poll_once()
        self.assertEqual(empty.status, BrokerPositionSnapshotStatus.COMPLETE)
        self.assertEqual(empty.row_count, 0)
        self.assertEqual(empty_service.health.readiness, Readiness.READY)

    async def test_application_timeout_records_failed_attempt_only(self):
        class SlowReader:
            def __init__(self):
                self.source_session_id = new_id("ib_session")

            async def read_positions(self, *, account_id):
                _ = account_id
                await asyncio.sleep(1.0)
                return ()

            async def close(self):
                return None

        service = BrokerPositionFeedService(
            config=self.config(timeout=0.01),
            reader=SlowReader(),
            repository=self.store,
            health_publisher=self.health,
            clock=SequenceClock(
                self.start,
                self.start + timedelta(seconds=1),
            ),
        )
        with self.assertRaises(PositionFeedPollError):
            await service.poll_once()
        self.assertEqual(self.store.count_attempts(), 1)
        self.assertIsNone(self.store.read_latest_complete())


class PositionFeedEntrypointTest(unittest.TestCase):
    @staticmethod
    def environment(data_root: Path) -> dict[str, str]:
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONPATH": str(SRC),
                "IBMD_DEPLOYMENT_ID": "paper-mnq-01",
                "IBMD_DATA_ROOT": str(data_root),
                "IBMD_APPLICATION_VERSION": "0.1.0-dev",
                "IB_HOST": "127.0.0.1",
                "IB_PORT": "7497",
                "IB_CLIENT_ID": "200",
                "IB_ACCOUNT_ID": ACCOUNT_ID,
            }
        )
        return environment

    def test_help_does_not_require_production_environment(self):
        environment = dict(os.environ)
        for key in tuple(environment):
            if key.startswith("IBMD_") or key in {
                "IB_HOST",
                "IB_PORT",
                "IB_CLIENT_ID",
                "IB_ACCOUNT_ID",
            }:
                environment.pop(key, None)
        result = subprocess.run(
            [
                sys.executable,
                "apps/run_position_feed_v2.py",
                "--help",
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("shadow mode", result.stdout)

    def test_validate_store_only_uses_public_health_and_no_ib_connection(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database = (
                root
                / "position_feed"
                / "broker_positions.sqlite3"
            )
            apply_position_feed_schema(database)
            result = subprocess.run(
                [
                    sys.executable,
                    "apps/run_position_feed_v2.py",
                    "--validate-store-only",
                ],
                cwd=ROOT,
                env=self.environment(root),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("store is compatible", result.stdout)
            health = ServiceHealthFile(
                root
                / "runtime"
                / "health"
                / "broker_position_feed.json",
                expected_service="broker_position_feed",
            ).read()
            self.assertEqual(health.liveness, Liveness.STOPPED)
            self.assertEqual(health.readiness, Readiness.NOT_READY)


if __name__ == "__main__":
    unittest.main()
