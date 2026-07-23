from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.fake_market_data import ScriptedMarketDataReader
from ibmd.ib_gateway.ib_async_market_data import (
    IBAsyncMarketDataReader,
    IBMarketDataConnectionSettings,
)
from ibmd.ib_gateway.market_data import BrokerMarketDataReadError
from ibmd.market_data.adapters import (
    MarketDataSchemaError,
    SQLiteMarketBarStore,
)
from ibmd.market_data.application.config import MarketDataShadowConfig
from ibmd.market_data.application.service import (
    MarketDataServiceError,
    MarketDataShadowService,
)
from ibmd.market_data.domain import (
    MarketBarAssemblyError,
    assemble_complete_market_bar,
)
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts import Liveness, Readiness
from ibmd.public_contracts.market_data import (
    MarketBarV1,
    MarketDataContractError,
    MarketDataContractV1,
    MarketDataSourceKind,
    MarketSideBarObservationV1,
    QuoteSide,
)

ACCOUNT_ID = "U0000000"
INSTRUMENT = "MNQ"
CON_ID = 793356225
LOCAL_SYMBOL = "MNQU6"
MIGRATION_MANIFEST = ROOT / "migrations" / "market_data.v1.json"


def apply_market_data_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MIGRATION_MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def market_contract() -> MarketDataContractV1:
    return MarketDataContractV1(
        instrument_id=INSTRUMENT,
        sec_type="FUT",
        symbol="MNQ",
        exchange="CME",
        currency="USD",
        trading_class="MNQ",
        multiplier=2.0,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        last_trade_date="20260918",
    )


def side_bar(
    side: QuoteSide,
    *,
    bar_start_utc: str = "2026-07-23T10:00:00Z",
    observed_at_utc: str = "2026-07-23T10:00:06Z",
    close_price: float | None = None,
    source_kind: MarketDataSourceKind = MarketDataSourceKind.HISTORY,
    source_session_id: str | None = None,
    source_generation_id: str | None = None,
    con_id: int = CON_ID,
    local_symbol: str = LOCAL_SYMBOL,
) -> MarketSideBarObservationV1:
    base = 20_000.0 if side == QuoteSide.BID else 20_000.25
    close = base + 0.25 if close_price is None else float(close_price)
    return MarketSideBarObservationV1(
        instrument_id=INSTRUMENT,
        con_id=con_id,
        local_symbol=local_symbol,
        side=side,
        bar_start_utc=bar_start_utc,
        bar_duration_seconds=5,
        open_price=base,
        high_price=max(base, close) + 0.50,
        low_price=min(base, close) - 0.50,
        close_price=close,
        source_kind=source_kind,
        source_session_id=source_session_id or new_id("ib_session"),
        source_generation_id=(
            source_generation_id or new_id("md_generation")
        ),
        observed_at_utc=observed_at_utc,
    )


def side_pair(
    *,
    bar_start_utc: str = "2026-07-23T10:00:00Z",
    observed_at_utc: str = "2026-07-23T10:00:06Z",
    source_kind: MarketDataSourceKind = MarketDataSourceKind.HISTORY,
    source_session_id: str | None = None,
    source_generation_id: str | None = None,
    bid_close: float = 20_000.25,
    ask_close: float = 20_000.50,
    con_id: int = CON_ID,
    local_symbol: str = LOCAL_SYMBOL,
) -> tuple[MarketSideBarObservationV1, MarketSideBarObservationV1]:
    session = source_session_id or new_id("ib_session")
    generation = source_generation_id or new_id("md_generation")
    return (
        side_bar(
            QuoteSide.BID,
            bar_start_utc=bar_start_utc,
            observed_at_utc=observed_at_utc,
            close_price=bid_close,
            source_kind=source_kind,
            source_session_id=session,
            source_generation_id=generation,
            con_id=con_id,
            local_symbol=local_symbol,
        ),
        side_bar(
            QuoteSide.ASK,
            bar_start_utc=bar_start_utc,
            observed_at_utc=observed_at_utc,
            close_price=ask_close,
            source_kind=source_kind,
            source_session_id=session,
            source_generation_id=generation,
            con_id=con_id,
            local_symbol=local_symbol,
        ),
    )


class CapturingHealthPublisher:
    def __init__(self) -> None:
        self.values = []

    def publish(self, health):
        self.values.append(health)
        return health


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class AdvancingClock:
    def __init__(self, value: datetime, *, step_seconds: float = 1.0) -> None:
        self.value = value
        self.step = timedelta(seconds=step_seconds)

    def __call__(self) -> datetime:
        current = self.value
        self.value += self.step
        return current


class MarketDataContractTest(unittest.TestCase):
    def test_complete_bar_is_strict_round_trip_and_mid_is_derived(self):
        bid, ask = side_pair()
        bar = assemble_complete_market_bar(
            bid=bid,
            ask=ask,
            revision=1,
        )
        restored = MarketBarV1.from_dict(bar.to_dict())
        self.assertEqual(restored, bar)
        self.assertEqual(bar.mid_close, 20_000.375)
        self.assertEqual(bar.rounded_mid_close(3), 20_000.375)
        self.assertEqual(
            bar.freshness(
                observed_at_utc="2026-07-23T10:00:10Z",
                max_age_seconds=5,
            ).age_seconds,
            5.0,
        )

        invalid = bar.to_dict()
        invalid["complete"] = False
        with self.assertRaisesRegex(MarketDataContractError, "complete=True"):
            MarketBarV1.from_dict(invalid)

        unknown = bar.to_dict()
        unknown["unexpected"] = True
        with self.assertRaisesRegex(MarketDataContractError, "unknown"):
            MarketBarV1.from_dict(unknown)

    def test_observation_requires_alignment_positive_ohlc_and_stable_identity(self):
        payload = side_bar(QuoteSide.BID).to_dict()
        self.assertEqual(
            MarketSideBarObservationV1.from_dict(payload).to_dict(),
            payload,
        )
        with self.assertRaisesRegex(MarketDataContractError, "aligned"):
            side_bar(
                QuoteSide.BID,
                bar_start_utc="2026-07-23T10:00:01Z",
            )
        with self.assertRaisesRegex(MarketDataContractError, "positive"):
            MarketSideBarObservationV1(
                **{
                    **side_bar(QuoteSide.BID).to_dict(),
                    "side": QuoteSide.BID,
                    "source_kind": MarketDataSourceKind.HISTORY,
                    "close_price": 0.0,
                }
            )

    def test_different_generation_or_session_cannot_assemble(self):
        bid, ask = side_pair()
        different_generation = MarketSideBarObservationV1(
            **{
                **ask.to_dict(),
                "side": QuoteSide.ASK,
                "source_kind": ask.source_kind,
                "source_generation_id": new_id("md_generation"),
            }
        )
        with self.assertRaisesRegex(MarketBarAssemblyError, "generations"):
            assemble_complete_market_bar(
                bid=bid,
                ask=different_generation,
                revision=1,
            )

        different_session = MarketSideBarObservationV1(
            **{
                **ask.to_dict(),
                "side": QuoteSide.ASK,
                "source_kind": ask.source_kind,
                "source_session_id": new_id("ib_session"),
            }
        )
        with self.assertRaisesRegex(MarketBarAssemblyError, "IB sessions"):
            assemble_complete_market_bar(
                bid=bid,
                ask=different_session,
                revision=1,
            )


class MarketDataStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "market_data" / "MNQ.sqlite3"
        apply_market_data_schema(self.database)
        self.store = SQLiteMarketBarStore(
            self.database,
            instrument_id=INSTRUMENT,
        )
        self.store.validate_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_partial_sides_are_private_and_only_matching_generation_publishes(self):
        session = new_id("ib_session")
        generation_one = new_id("md_generation")
        generation_two = new_id("md_generation")
        bid_one = side_bar(
            QuoteSide.BID,
            source_session_id=session,
            source_generation_id=generation_one,
        )
        ask_two = side_bar(
            QuoteSide.ASK,
            source_session_id=session,
            source_generation_id=generation_two,
        )
        self.assertEqual(self.store.ingest_fragments((bid_one,)), ())
        self.assertEqual(self.store.ingest_fragments((ask_two,)), ())
        self.assertIsNone(self.store.read_latest_complete(INSTRUMENT))
        self.assertEqual(self.store.count_public_bars(), 0)
        self.assertEqual(self.store.count_fragments(), 2)

        ask_one = side_bar(
            QuoteSide.ASK,
            source_session_id=session,
            source_generation_id=generation_one,
        )
        published = self.store.ingest_fragments((ask_one,))
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0].revision, 1)
        self.assertEqual(self.store.count_public_bars(), 1)
        self.assertEqual(self.store.count_fragments(), 3)

        bid_two = side_bar(
            QuoteSide.BID,
            source_session_id=session,
            source_generation_id=generation_two,
            source_kind=MarketDataSourceKind.HISTORY,
            observed_at_utc="2026-07-23T10:00:07Z",
            close_price=20_001.0,
        )
        corrected = self.store.ingest_fragments((bid_two,))
        self.assertEqual(len(corrected), 1)
        self.assertEqual(corrected[0].revision, 2)

    def test_history_realtime_revision_and_delayed_pair_do_not_regress(self):
        history = side_pair(
            observed_at_utc="2026-07-23T10:00:06Z",
        )
        first = self.store.ingest_fragments(history)[0]
        self.assertEqual(first.revision, 1)

        realtime = side_pair(
            observed_at_utc="2026-07-23T10:00:08Z",
            source_kind=MarketDataSourceKind.REALTIME,
            bid_close=20_001.0,
            ask_close=20_001.25,
        )
        second = self.store.ingest_fragments(realtime)[0]
        self.assertEqual(second.revision, 2)
        self.assertEqual(second.source_kind, MarketDataSourceKind.REALTIME)
        self.assertEqual(second.first_published_at_utc, first.first_published_at_utc)

        delayed = side_pair(
            observed_at_utc="2026-07-23T10:00:07Z",
            source_kind=MarketDataSourceKind.RECENT_BACKFILL,
            bid_close=19_999.0,
            ask_close=19_999.25,
        )
        self.assertEqual(self.store.ingest_fragments(delayed), ())
        latest = self.store.read_latest_complete(INSTRUMENT)
        self.assertEqual(latest.revision, 2)
        self.assertEqual(latest.bid_close, 20_001.0)

        conn = sqlite3.connect(self.database)
        try:
            rejected = conn.execute(
                "SELECT COUNT(*) FROM internal_market_data_ingest_events "
                "WHERE result='REJECTED'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(rejected, 2)

    def test_old_backfill_never_moves_latest_pointer_backward(self):
        newer = side_pair(
            bar_start_utc="2026-07-23T10:00:10Z",
            observed_at_utc="2026-07-23T10:00:16Z",
        )
        older = side_pair(
            bar_start_utc="2026-07-23T09:59:55Z",
            observed_at_utc="2026-07-23T10:00:20Z",
            source_kind=MarketDataSourceKind.RECENT_BACKFILL,
        )
        new_bar = self.store.ingest_fragments(newer)[0]
        self.store.ingest_fragments(older)
        latest = self.store.read_latest_complete(INSTRUMENT)
        self.assertEqual(latest.bar_id, new_bar.bar_id)
        self.assertEqual(self.store.count_public_bars(), 2)

    def test_source_failure_is_audited_without_fake_bar_or_pointer_change(self):
        first = self.store.ingest_fragments(side_pair())[0]
        self.store.record_source_failure(
            instrument_id=INSTRUMENT,
            source_session_id=new_id("ib_session"),
            observed_at_utc="2026-07-23T10:00:20Z",
            error_text="ConnectionError: market data farm down",
        )
        self.assertEqual(
            self.store.read_latest_complete(INSTRUMENT).bar_id,
            first.bar_id,
        )
        self.assertEqual(self.store.count_source_failures(), 1)

    def test_unmigrated_store_is_rejected_without_runtime_creation(self):
        missing = Path(self.temp_dir.name) / "missing" / "MNQ.sqlite3"
        store = SQLiteMarketBarStore(missing, instrument_id=INSTRUMENT)
        with self.assertRaises(MarketDataSchemaError):
            store.validate_schema()
        self.assertFalse(missing.exists())


class MarketDataServiceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "MNQ.sqlite3"
        apply_market_data_schema(self.database)
        self.store = SQLiteMarketBarStore(
            self.database,
            instrument_id=INSTRUMENT,
        )
        self.health = CapturingHealthPublisher()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def config(self, *, max_age: float = 60.0) -> MarketDataShadowConfig:
        return MarketDataShadowConfig(
            instrument_id=INSTRUMENT,
            deployment_id="paper-mnq-01",
            instance_id=new_id("instance"),
            application_version="0.1.0-test",
            configuration_hash="a" * 64,
            history_lookback_seconds=3_600,
            bar_max_age_seconds=max_age,
            realtime_read_timeout_seconds=0.01,
        )

    async def test_serialized_writer_publishes_exactly_one_complete_pair(self):
        reader = ScriptedMarketDataReader()
        service = MarketDataShadowService(
            config=self.config(),
            reader=reader,
            repository=self.store,
            health_publisher=self.health,
            clock=MutableClock(
                datetime(2026, 7, 23, 10, 0, 10, tzinfo=timezone.utc)
            ),
        )
        await service.start()
        bid, ask = side_pair()
        try:
            results = await asyncio.gather(
                service.submit_fragments((bid,)),
                service.submit_fragments((ask,)),
            )
            self.assertEqual(sum(len(item) for item in results), 1)
            self.assertEqual(self.store.count_public_bars(), 1)
        finally:
            await service.stop()
        self.assertEqual(service.health.liveness, Liveness.STOPPED)

    async def test_history_realtime_seam_and_health(self):
        history = side_pair(
            bar_start_utc="2026-07-23T10:00:10Z",
            observed_at_utc="2026-07-23T10:00:16Z",
        )
        realtime = side_pair(
            bar_start_utc="2026-07-23T10:00:20Z",
            observed_at_utc="2026-07-23T10:00:26Z",
            source_kind=MarketDataSourceKind.REALTIME,
        )
        reader = ScriptedMarketDataReader(
            history=[history],
            realtime=[(*realtime, None, None, None)],
        )
        service = MarketDataShadowService(
            config=self.config(),
            reader=reader,
            repository=self.store,
            health_publisher=self.health,
            clock=AdvancingClock(
                datetime(2026, 7, 23, 10, 0, 20, tzinfo=timezone.utc)
            ),
        )
        await service.start()
        try:
            changed = await service.run_active_contract(
                contract=market_contract(),
                active_to_utc="2026-07-23T10:00:32Z",
            )
            self.assertEqual(len(changed), 1)
            latest = self.store.read_latest_complete(INSTRUMENT)
            self.assertEqual(latest.bar_start_utc, "2026-07-23T10:00:20.000000Z")
            self.assertEqual(latest.source_kind, MarketDataSourceKind.REALTIME)
            self.assertEqual(service.health.readiness, Readiness.READY)
            self.assertEqual(len(reader.history_calls), 1)
            self.assertEqual(len(reader.realtime_calls), 1)
        finally:
            await service.stop()

    async def test_source_error_is_degraded_while_fresh_then_blocked_when_stale(self):
        self.store.ingest_fragments(side_pair())
        clock = MutableClock(
            datetime(2026, 7, 23, 10, 0, 8, tzinfo=timezone.utc)
        )
        service = MarketDataShadowService(
            config=self.config(max_age=10.0),
            reader=ScriptedMarketDataReader(),
            repository=self.store,
            health_publisher=self.health,
            clock=clock,
        )
        service.refresh_health(
            source_status="ERROR",
            error_text="market data farm unavailable",
        )
        self.assertEqual(service.health.readiness, Readiness.DEGRADED)

        clock.value = datetime(2026, 7, 23, 10, 0, 20, tzinfo=timezone.utc)
        service.refresh_health(
            source_status="ERROR",
            error_text="market data farm still unavailable",
        )
        self.assertEqual(service.health.readiness, Readiness.BLOCKED)
        self.assertEqual(
            self.store.read_latest_complete(INSTRUMENT).bar_start_utc,
            "2026-07-23T10:00:00.000000Z",
        )

    async def test_initial_history_failure_records_error_without_bar(self):
        reader = ScriptedMarketDataReader(
            history=[BrokerMarketDataReadError("HMDS unavailable")]
        )
        clock = MutableClock(
            datetime(2026, 7, 23, 10, 0, 10, tzinfo=timezone.utc)
        )
        service = MarketDataShadowService(
            config=self.config(),
            reader=reader,
            repository=self.store,
            health_publisher=self.health,
            clock=clock,
        )
        await service.start()
        try:
            with self.assertRaises(MarketDataServiceError):
                await service.bootstrap_recent_history(
                    contract=market_contract()
                )
            self.assertIsNone(self.store.read_latest_complete(INSTRUMENT))
            self.assertEqual(self.store.count_source_failures(), 1)
            self.assertEqual(service.health.readiness, Readiness.BLOCKED)
        finally:
            await service.stop()


class FakeEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        if handler in self.handlers:
            self.handlers.remove(handler)
        return self

    def emit(self, *args) -> None:
        for handler in tuple(self.handlers):
            handler(*args)


class FakeBarList(list):
    def __init__(self) -> None:
        super().__init__()
        self.updateEvent = FakeEvent()


class FakeMarketDataIB:
    def __init__(self, *, managed_accounts=(ACCOUNT_ID,)) -> None:
        self.connected = False
        self.managed_accounts = list(managed_accounts)
        self.history_calls = []
        self.realtime_rows = {}
        self.cancelled = []

    async def connectAsync(self, **kwargs):
        self.connect_kwargs = kwargs
        self.connected = True

    def isConnected(self):
        return self.connected

    def managedAccounts(self):
        return list(self.managed_accounts)

    async def reqHistoricalDataAsync(self, contract, **kwargs):
        _ = contract
        self.history_calls.append(kwargs)
        side = kwargs["whatToShow"]
        base = 20_000.0 if side == "BID" else 20_000.25
        return [
            SimpleNamespace(
                date=datetime(
                    2026, 7, 23, 10, 0, 50, tzinfo=timezone.utc
                ),
                open=base,
                high=base + 1.0,
                low=base - 1.0,
                close=base + 0.25,
            )
        ]

    def reqRealTimeBars(self, contract, bar_size, what_to_show, use_rth):
        _ = contract, bar_size, use_rth
        rows = FakeBarList()
        self.realtime_rows[what_to_show] = rows
        return rows

    def cancelRealTimeBars(self, rows):
        self.cancelled.append(rows)

    def disconnect(self):
        self.connected = False


class IBAsyncMarketDataAdapterTest(unittest.IsolatedAsyncioTestCase):
    def settings(self, **overrides) -> IBMarketDataConnectionSettings:
        values = {
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 280,
            "account_id": ACCOUNT_ID,
            "historical_request_spacing_seconds": 0.0,
        }
        values.update(overrides)
        return IBMarketDataConnectionSettings(**values)

    async def test_historical_mapping_has_one_generation_for_bid_and_ask(self):
        fake = FakeMarketDataIB()
        reader = IBAsyncMarketDataReader(
            self.settings(),
            ib_factory=lambda: fake,
            clock=lambda: datetime(
                2026, 7, 23, 10, 1, 1, tzinfo=timezone.utc
            ),
        )
        rows = await reader.fetch_recent_history(
            contract=market_contract(),
            end_at_utc="2026-07-23T10:01:00Z",
            lookback_seconds=60,
            bar_duration_seconds=5,
            use_rth=False,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual({item.side for item in rows}, {QuoteSide.BID, QuoteSide.ASK})
        self.assertEqual(len({item.source_generation_id for item in rows}), 1)
        self.assertEqual(len({item.source_session_id for item in rows}), 1)
        self.assertEqual(
            [call["whatToShow"] for call in fake.history_calls],
            ["BID", "ASK"],
        )
        self.assertEqual(fake.connect_kwargs["account"], ACCOUNT_ID)
        await reader.close()

    async def test_wrong_account_fails_before_market_data_request(self):
        fake = FakeMarketDataIB(managed_accounts=("U9999999",))
        reader = IBAsyncMarketDataReader(
            self.settings(),
            ib_factory=lambda: fake,
        )
        with self.assertRaisesRegex(
            BrokerMarketDataReadError,
            "absent from IB session",
        ):
            await reader.fetch_recent_history(
                contract=market_contract(),
                end_at_utc="2026-07-23T10:01:00Z",
                lookback_seconds=60,
                bar_duration_seconds=5,
                use_rth=False,
            )
        self.assertEqual(fake.history_calls, [])

    async def test_realtime_bid_and_ask_share_generation_and_close_cancels_both(self):
        fake = FakeMarketDataIB()
        now = datetime(2026, 7, 23, 10, 0, 6, tzinfo=timezone.utc)
        reader = IBAsyncMarketDataReader(
            self.settings(),
            ib_factory=lambda: fake,
            clock=lambda: now,
        )
        subscription = await reader.open_realtime(
            contract=market_contract(),
            bar_duration_seconds=5,
            use_rth=False,
        )
        for side, base in (("BID", 20_000.0), ("ASK", 20_000.25)):
            rows = fake.realtime_rows[side]
            rows.append(
                SimpleNamespace(
                    time=datetime(
                        2026, 7, 23, 10, 0, 0, tzinfo=timezone.utc
                    ),
                    open_=base,
                    high=base + 1.0,
                    low=base - 1.0,
                    close=base + 0.25,
                )
            )
            rows.updateEvent.emit(rows, True)

        first = await subscription.next_bar(timeout_seconds=0.1)
        second = await subscription.next_bar(timeout_seconds=0.1)
        self.assertEqual({first.side, second.side}, {QuoteSide.BID, QuoteSide.ASK})
        self.assertEqual(first.source_generation_id, second.source_generation_id)
        self.assertEqual(first.source_session_id, second.source_session_id)
        await subscription.close()
        self.assertEqual(len(fake.cancelled), 2)
        await reader.close()

    async def test_historical_timeout_disconnects_session(self):
        class SlowIB(FakeMarketDataIB):
            async def reqHistoricalDataAsync(self, contract, **kwargs):
                _ = contract, kwargs
                await asyncio.sleep(1.0)
                return []

        fake = SlowIB()
        reader = IBAsyncMarketDataReader(
            self.settings(historical_timeout_seconds=0.01),
            ib_factory=lambda: fake,
        )
        with self.assertRaisesRegex(BrokerMarketDataReadError, "timed out"):
            await reader.fetch_recent_history(
                contract=market_contract(),
                end_at_utc="2026-07-23T10:01:00Z",
                lookback_seconds=60,
                bar_duration_seconds=5,
                use_rth=False,
            )
        self.assertFalse(reader.connected)


class MarketDataEntrypointTest(unittest.TestCase):
    @staticmethod
    def environment(data_root: Path) -> dict[str, str]:
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONPATH": str(SRC),
                "IBMD_DEPLOYMENT_ID": "paper-mnq-01",
                "IBMD_DATA_ROOT": str(data_root),
                "IBMD_APPLICATION_VERSION": "0.1.0-test",
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
            [sys.executable, str(ROOT / "apps/run_market_data_v2.py"), "--help"],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("shadow mode", result.stdout)

    def test_validate_store_only_uses_explicit_pre_migrated_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            database = data_root / "market_data" / "MNQ.sqlite3"
            apply_market_data_schema(database)
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "apps/run_market_data_v2.py"),
                    "--validate-store-only",
                    "--database",
                    str(database),
                ],
                cwd=ROOT,
                env=self.environment(data_root),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("store is compatible", result.stdout)
            health_path = data_root / "runtime/health/market_data.json"
            self.assertTrue(health_path.is_file())

    def test_unmigrated_database_is_not_created_by_runtime(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            database = data_root / "market_data" / "MNQ.sqlite3"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "apps/run_market_data_v2.py"),
                    "--validate-store-only",
                    "--database",
                    str(database),
                ],
                cwd=ROOT,
                env=self.environment(data_root),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 2)
            self.assertFalse(database.exists())
            self.assertIn("offline migration", result.stderr)


if __name__ == "__main__":
    unittest.main()
