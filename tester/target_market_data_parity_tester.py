from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
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
from ibmd.market_data.adapters import SQLiteMarketBarStore
from ibmd.market_data.application import (
    MarketDataServiceError,
    MarketDataShadowConfig,
    MarketDataShadowService,
    backfill_contract_history,
    iter_history_chunks,
)
from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest
from ibmd.operations.parity.market_data import compare_market_data_databases
from ibmd.public_contracts.market_data import (
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


def apply_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MIGRATION_MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def contract() -> MarketDataContractV1:
    return MarketDataContractV1(
        instrument_id=INSTRUMENT,
        sec_type="FUT",
        symbol=INSTRUMENT,
        exchange="CME",
        currency="USD",
        trading_class=INSTRUMENT,
        multiplier=2.0,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        last_trade_date="20260918",
    )


def pair(
    bar_start_utc: str,
    *,
    observed_at_utc: str,
    bid_close: float = 100.5,
    ask_close: float = 100.75,
    source_session_id: str | None = None,
    source_generation_id: str | None = None,
):
    session = source_session_id or new_id("ib_session")
    generation = source_generation_id or new_id("md_generation")
    return (
        MarketSideBarObservationV1(
            instrument_id=INSTRUMENT,
            con_id=CON_ID,
            local_symbol=LOCAL_SYMBOL,
            side=QuoteSide.BID,
            bar_start_utc=bar_start_utc,
            bar_duration_seconds=5,
            open_price=100.0,
            high_price=101.0,
            low_price=99.5,
            close_price=bid_close,
            source_kind=MarketDataSourceKind.HISTORY,
            source_session_id=session,
            source_generation_id=generation,
            observed_at_utc=observed_at_utc,
        ),
        MarketSideBarObservationV1(
            instrument_id=INSTRUMENT,
            con_id=CON_ID,
            local_symbol=LOCAL_SYMBOL,
            side=QuoteSide.ASK,
            bar_start_utc=bar_start_utc,
            bar_duration_seconds=5,
            open_price=100.25,
            high_price=101.25,
            low_price=99.75,
            close_price=ask_close,
            source_kind=MarketDataSourceKind.HISTORY,
            source_session_id=session,
            source_generation_id=generation,
            observed_at_utc=observed_at_utc,
        ),
    )


class CapturingHealth:
    def __init__(self) -> None:
        self.values = []

    def publish(self, value):
        self.values.append(value)
        return value


class MarketDataParityComparatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.legacy = root / "legacy.sqlite3"
        self.target = root / "target.sqlite3"
        apply_schema(self.target)
        connection = sqlite3.connect(self.legacy)
        try:
            connection.execute(
                """
                CREATE TABLE MNQ_5s (
                    bar_time_ts INTEGER PRIMARY KEY,
                    contract TEXT NOT NULL,
                    bid_open REAL,
                    bid_high REAL,
                    bid_low REAL,
                    bid_close REAL,
                    ask_open REAL,
                    ask_high REAL,
                    ask_low REAL,
                    ask_close REAL
                )
                """
            )
            connection.commit()
        finally:
            connection.close()
        self.store = SQLiteMarketBarStore(self.target, instrument_id=INSTRUMENT)

    def tearDown(self):
        self.temp.cleanup()

    def insert_legacy(
        self,
        ts: int,
        *,
        ask_close: float | None = 100.75,
    ) -> None:
        connection = sqlite3.connect(self.legacy)
        try:
            connection.execute(
                """
                INSERT INTO MNQ_5s (
                    bar_time_ts, contract,
                    bid_open, bid_high, bid_low, bid_close,
                    ask_open, ask_high, ask_low, ask_close
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    LOCAL_SYMBOL,
                    100.0,
                    101.0,
                    99.5,
                    100.5,
                    100.25,
                    101.25,
                    99.75,
                    ask_close,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def test_exact_complete_bars_match_and_legacy_half_bar_is_ignored(self):
        self.insert_legacy(1784800800)
        self.insert_legacy(1784800805, ask_close=None)
        self.store.ingest_fragments(
            pair(
                "2026-07-23T10:00:00Z",
                observed_at_utc="2026-07-23T10:00:06Z",
            )
        )
        report = compare_market_data_databases(
            legacy_database_path=self.legacy,
            legacy_table_name="MNQ_5s",
            target_database_path=self.target,
            instrument_id=INSTRUMENT,
            start_utc="2026-07-23T10:00:00Z",
            end_utc="2026-07-23T10:00:10Z",
        )
        self.assertTrue(report.is_match)
        self.assertEqual(report.legacy_count, 1)
        self.assertEqual(report.target_count, 1)
        self.assertEqual(report.matched_count, 1)

    def test_value_and_missing_bar_differences_are_separate(self):
        self.insert_legacy(1784800800)
        self.insert_legacy(1784800805)
        self.store.ingest_fragments(
            pair(
                "2026-07-23T10:00:00Z",
                observed_at_utc="2026-07-23T10:00:06Z",
                ask_close=101.0,
            )
        )
        report = compare_market_data_databases(
            legacy_database_path=self.legacy,
            legacy_table_name="MNQ_5s",
            target_database_path=self.target,
            instrument_id=INSTRUMENT,
            start_utc="2026-07-23T10:00:00Z",
            end_utc="2026-07-23T10:00:10Z",
        )
        self.assertFalse(report.is_match)
        self.assertEqual(report.value_mismatch_count, 1)
        self.assertEqual(report.legacy_only_count, 1)
        self.assertEqual(report.target_only_count, 0)
        self.assertIn(
            "ask_close",
            report.value_mismatch_samples[0]["differences"],
        )


class HistoryRangeBackfillTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "market.sqlite3"
        apply_schema(self.database)
        self.store = SQLiteMarketBarStore(self.database, instrument_id=INSTRUMENT)

    def tearDown(self):
        self.temp.cleanup()

    def config(self) -> MarketDataShadowConfig:
        return MarketDataShadowConfig(
            instrument_id=INSTRUMENT,
            deployment_id="paper-mnq-01",
            instance_id=new_id("instance"),
            application_version="0.1.0-test",
            configuration_hash="a" * 64,
            history_chunk_seconds=3_600,
            history_chunk_spacing_seconds=0.0,
        )

    async def test_explicit_range_is_split_into_exact_non_overlapping_chunks(self):
        history = (
            pair(
                "2026-07-23T10:00:00Z",
                observed_at_utc="2026-07-23T10:00:06Z",
            ),
            pair(
                "2026-07-23T11:00:00Z",
                observed_at_utc="2026-07-23T11:00:06Z",
            ),
            pair(
                "2026-07-23T12:00:00Z",
                observed_at_utc="2026-07-23T12:00:06Z",
            ),
        )
        reader = ScriptedMarketDataReader(history=history)
        service = MarketDataShadowService(
            config=self.config(),
            reader=reader,
            repository=self.store,
            health_publisher=CapturingHealth(),
            clock=lambda: datetime(
                2026, 7, 23, 12, 30, 10, tzinfo=timezone.utc
            ),
        )
        await service.start()
        try:
            result = await backfill_contract_history(
                service=service,
                contract=contract(),
                start_utc="2026-07-23T10:00:00Z",
                end_utc="2026-07-23T12:30:00Z",
            )
        finally:
            await service.stop()

        self.assertEqual(result.chunk_count, 3)
        self.assertEqual(result.fragment_count, 6)
        self.assertEqual(result.changed_bar_count, 3)
        self.assertEqual(
            [
                (
                    call["end_at_utc"],
                    call["lookback_seconds"],
                )
                for call in reader.history_calls
            ],
            [
                ("2026-07-23T11:00:00.000000Z", 3_600),
                ("2026-07-23T12:00:00.000000Z", 3_600),
                ("2026-07-23T12:30:00.000000Z", 1_800),
            ],
        )

    def test_chunk_boundaries_must_be_bar_aligned(self):
        with self.assertRaisesRegex(MarketDataServiceError, "aligned"):
            iter_history_chunks(
                start_utc="2026-07-23T10:00:01Z",
                end_utc="2026-07-23T11:00:00Z",
                chunk_seconds=3_600,
                bar_duration_seconds=5,
            )


class IBMarketDataReconnectTest(unittest.IsolatedAsyncioTestCase):
    async def test_failed_session_is_replaced_before_next_history_request(self):
        class FakeIB:
            def __init__(self):
                self.connected = False
                self.connect_calls = 0
                self.fail_first_request = True

            async def connectAsync(self, **kwargs):
                _ = kwargs
                self.connect_calls += 1
                self.connected = True

            def isConnected(self):
                return self.connected

            def managedAccounts(self):
                return [ACCOUNT_ID]

            async def reqHistoricalDataAsync(self, *args, **kwargs):
                _ = args
                if self.fail_first_request:
                    self.fail_first_request = False
                    self.connected = False
                    raise ConnectionError("simulated disconnect")
                return [
                    SimpleNamespace(
                        date=datetime(
                            2026, 7, 23, 10, 0, 0, tzinfo=timezone.utc
                        ),
                        open=100.0,
                        high=101.0,
                        low=99.5,
                        close=100.5
                        if kwargs["whatToShow"] == "BID"
                        else 100.75,
                    )
                ]

            def disconnect(self):
                self.connected = False

        fake = FakeIB()
        reader = IBAsyncMarketDataReader(
            IBMarketDataConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=280,
                account_id=ACCOUNT_ID,
                historical_request_spacing_seconds=0.0,
            ),
            ib_factory=lambda: fake,
            clock=lambda: datetime(
                2026, 7, 23, 10, 0, 6, tzinfo=timezone.utc
            ),
        )
        with self.assertRaises(BrokerMarketDataReadError):
            await reader.fetch_recent_history(
                contract=contract(),
                end_at_utc="2026-07-23T10:00:05Z",
                lookback_seconds=5,
                bar_duration_seconds=5,
                use_rth=False,
            )
        failed_session_id = reader.source_session_id

        fragments = await reader.fetch_recent_history(
            contract=contract(),
            end_at_utc="2026-07-23T10:00:05Z",
            lookback_seconds=5,
            bar_duration_seconds=5,
            use_rth=False,
        )
        self.assertEqual(fake.connect_calls, 2)
        self.assertNotEqual(reader.source_session_id, failed_session_id)
        self.assertEqual(len(fragments), 2)
        self.assertEqual(
            {item.source_session_id for item in fragments},
            {reader.source_session_id},
        )
        self.assertEqual(
            len({item.source_generation_id for item in fragments}),
            1,
        )


if __name__ == "__main__":
    unittest.main()
