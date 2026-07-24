from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.signal import (
    SignalCalculationStatus,
    SignalCalculationV1,
    SignalDirection,
    SignalEventV1,
)
from ibmd.signal.adapters import SQLiteSignalStore, SignalStoreError

MANIFEST = ROOT / "migrations" / "signal.v1.json"
SOURCE_BAR_ID = "market_bar_0123456789abcdef0123456789abcdef"


def apply_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def calculation(*, entry_price: float) -> SignalCalculationV1:
    calculation_id = new_id("signal_calculation")
    event = SignalEventV1(
        event_id=new_id("signal_event"),
        calculation_id=calculation_id,
        strategy_id="IBMarketData.rolling",
        strategy_version=1,
        configuration_hash="a" * 64,
        instrument_id="MNQ",
        source_bar_id=SOURCE_BAR_ID,
        source_bar_revision=1,
        source_con_id=793356225,
        source_local_symbol="MNQU6",
        signal_bar_utc="2026-07-23T10:00:00Z",
        created_at_utc="2026-07-23T10:00:02Z",
        direction=SignalDirection.LONG,
        entry_price=entry_price,
        best_pearson=0.9,
        candidate_score_best=0.8,
        potential_end_delta_points=40.0,
        potential_max_profit_points=50.0,
        potential_max_drawdown_points=-10.0,
        potential_used=7,
    )
    return SignalCalculationV1(
        calculation_id=calculation_id,
        strategy_id="IBMarketData.rolling",
        strategy_version=1,
        configuration_hash="a" * 64,
        instrument_id="MNQ",
        source_bar_id=SOURCE_BAR_ID,
        source_bar_revision=1,
        source_con_id=793356225,
        source_local_symbol="MNQU6",
        signal_bar_utc="2026-07-23T10:00:00Z",
        calculated_at_utc="2026-07-23T10:00:02Z",
        status=SignalCalculationStatus.SIGNAL,
        entry_price=entry_price,
        raw_candidate_count=10,
        valid_pattern_count=9,
        pearson_pass_count=8,
        minmax_pass_count=7,
        skipped_pattern_count=1,
        best_raw_pearson=0.95,
        best_signal_pearson=0.94,
        best_candidate_score=0.8,
        potential_direction=SignalDirection.LONG,
        potential_end_delta_points=40.0,
        potential_max_profit_points=50.0,
        potential_max_drawdown_points=-10.0,
        potential_used=7,
        reason=None,
        event=event,
    )


class SignalPublicationIdempotencyTest(unittest.TestCase):
    def test_recalculation_with_new_technical_ids_returns_stored_fact(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "signal.sqlite3"
            apply_schema(database)
            store = SQLiteSignalStore(database)

            first = calculation(entry_price=100.0)
            stored = store.publish(first)
            recomputed = calculation(entry_price=100.0)
            returned = store.publish(recomputed)

            self.assertEqual(returned, stored)
            self.assertNotEqual(
                recomputed.calculation_id,
                stored.calculation_id,
            )
            self.assertNotEqual(
                recomputed.event.event_id,
                stored.event.event_id,
            )

            conflicting = calculation(entry_price=101.0)
            with self.assertRaises(SignalStoreError):
                store.publish(conflicting)


if __name__ == "__main__":
    unittest.main()
