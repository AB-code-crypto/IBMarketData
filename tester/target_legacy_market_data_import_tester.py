from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.market_data.adapters import SQLiteMarketBarReader
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.operations.migrations.legacy_market_data import (
    LegacyContractMapping,
    LegacyMarketDataImportError,
    import_legacy_complete_bars,
)

INSTRUMENT = "MNQ"
CON_ID = 793356225
LOCAL_SYMBOL = "MNQU6"
START_UTC = "2026-07-23T10:00:00Z"
END_UTC = "2026-07-23T10:00:15Z"
START_TS = 1_784_800_800
MIGRATION_MANIFEST = ROOT / "migrations" / "market_data.v1.json"


def apply_target_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MIGRATION_MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def create_legacy_database(path: Path) -> None:
    connection = sqlite3.connect(path)
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
        connection.executemany(
            """
            INSERT INTO MNQ_5s (
                bar_time_ts,
                contract,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    START_TS,
                    LOCAL_SYMBOL,
                    20_000.0,
                    20_001.0,
                    19_999.5,
                    20_000.5,
                    20_000.25,
                    20_001.25,
                    19_999.75,
                    20_000.75,
                ),
                (
                    START_TS + 5,
                    LOCAL_SYMBOL,
                    20_000.5,
                    20_001.5,
                    20_000.0,
                    20_001.0,
                    20_000.75,
                    20_001.75,
                    20_000.25,
                    20_001.25,
                ),
                (
                    START_TS + 10,
                    LOCAL_SYMBOL,
                    20_001.0,
                    20_002.0,
                    20_000.5,
                    20_001.5,
                    20_001.25,
                    20_002.25,
                    20_000.75,
                    None,
                ),
            ),
        )
        connection.commit()
    finally:
        connection.close()


class LegacyMarketDataImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.legacy = root / "legacy.sqlite3"
        self.target = root / "target.sqlite3"
        create_legacy_database(self.legacy)
        apply_target_schema(self.target)
        self.mappings = (
            LegacyContractMapping(
                local_symbol=LOCAL_SYMBOL,
                con_id=CON_ID,
            ),
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def execute(self, *, apply: bool):
        return import_legacy_complete_bars(
            legacy_database_path=self.legacy,
            legacy_table_name="MNQ_5s",
            target_database_path=self.target,
            instrument_id=INSTRUMENT,
            mappings=self.mappings,
            start_utc=START_UTC,
            end_utc=END_UTC,
            bar_duration_seconds=5,
            apply=apply,
            applied_at_utc="2026-07-23T12:00:00Z",
        )

    def test_dry_run_apply_and_second_apply_are_idempotent(self) -> None:
        dry_run = self.execute(apply=False)
        self.assertFalse(dry_run.applied)
        self.assertEqual(dry_run.complete_source_count, 2)
        self.assertEqual(dry_run.existing_exact_count, 0)
        self.assertEqual(dry_run.imported_count, 2)
        self.assertEqual(dry_run.incomplete_source_count, 1)
        self.assertEqual(
            SQLiteMarketBarReader(self.target).read_range(
                instrument_id=INSTRUMENT,
                start_utc=START_UTC,
                end_utc=END_UTC,
            ),
            (),
        )

        applied = self.execute(apply=True)
        self.assertTrue(applied.applied)
        self.assertEqual(applied.imported_count, 2)
        bars = SQLiteMarketBarReader(self.target).read_range(
            instrument_id=INSTRUMENT,
            start_utc=START_UTC,
            end_utc=END_UTC,
        )
        self.assertEqual(len(bars), 2)
        self.assertEqual([item.con_id for item in bars], [CON_ID, CON_ID])
        self.assertEqual(
            [item.local_symbol for item in bars],
            [LOCAL_SYMBOL, LOCAL_SYMBOL],
        )
        self.assertEqual(
            [item.bid_close for item in bars],
            [20_000.5, 20_001.0],
        )
        self.assertEqual(
            [item.ask_close for item in bars],
            [20_000.75, 20_001.25],
        )

        second = self.execute(apply=True)
        self.assertTrue(second.applied)
        self.assertEqual(second.complete_source_count, 2)
        self.assertEqual(second.existing_exact_count, 2)
        self.assertEqual(second.imported_count, 0)

        connection = sqlite3.connect(self.target)
        try:
            latest = connection.execute(
                """
                SELECT latest_bar_start_ts, latest_con_id, latest_local_symbol
                FROM internal_market_data_state
                WHERE instrument_id = ?
                """,
                (INSTRUMENT,),
            ).fetchone()
            event_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM internal_market_data_source_events
                WHERE instrument_id = ? AND status = 'INFO'
                """,
                (INSTRUMENT,),
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(latest, (START_TS + 5, CON_ID, LOCAL_SYMBOL))
        self.assertEqual(event_count, 2)

    def test_existing_value_conflict_is_rejected_without_mutation(self) -> None:
        self.execute(apply=True)
        connection = sqlite3.connect(self.target)
        try:
            connection.execute(
                """
                UPDATE internal_market_bars
                SET ask_close = ask_close + 1.0
                WHERE instrument_id = ? AND bar_start_ts = ?
                """,
                (INSTRUMENT, START_TS),
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(
            LegacyMarketDataImportError,
            "conflicting canonical bars",
        ):
            self.execute(apply=True)

        connection = sqlite3.connect(self.target)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM internal_market_bars"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 2)

    def test_unregistered_complete_contract_is_rejected(self) -> None:
        connection = sqlite3.connect(self.legacy)
        try:
            connection.execute(
                """
                UPDATE MNQ_5s
                SET contract = 'UNKNOWN'
                WHERE bar_time_ts = ?
                """,
                (START_TS,),
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(
            LegacyMarketDataImportError,
            "absent from target catalog",
        ):
            self.execute(apply=False)

    def test_command_help_does_not_require_deployment_environment(self) -> None:
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
                str(ROOT / "scripts/import_legacy_market_data.py"),
                "--help",
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Without --apply", result.stdout)

    def test_command_dry_run_is_read_only(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/import_legacy_market_data.py"),
                "--legacy-database",
                str(self.legacy),
                "--legacy-table",
                "MNQ_5s",
                "--target-database",
                str(self.target),
                "--catalog-root",
                str(ROOT / "catalog"),
                "--instrument",
                INSTRUMENT,
                "--start-utc",
                START_UTC,
                "--end-utc",
                END_UTC,
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertFalse(payload["applied"])
        self.assertEqual(payload["imported_count"], 2)
        self.assertEqual(
            SQLiteMarketBarReader(self.target).read_range(
                instrument_id=INSTRUMENT,
                start_utc=START_UTC,
                end_utc=END_UTC,
            ),
            (),
        )


if __name__ == "__main__":
    unittest.main()
