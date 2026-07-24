from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ib_signal.pearson import (
    calculate_centered_pearson_batch as legacy_pearson_batch,
)
from ib_signal.signal_candidate_potential import (
    build_candidate_potential_result as legacy_build_potential,
)
from ib_signal.signal_candidate_rank_features import (
    filter_candidates_by_minmax_ratio as legacy_minmax_filter,
    rank_candidates_by_score as legacy_rank,
)
from ib_signal.signal_candidates import (
    CandidateWindow as LegacyCandidateWindow,
)
from ib_signal.signal_window import SignalWindow as LegacySignalWindow
from ibmd.foundation.identity import new_id
from ibmd.foundation.time import format_utc
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.signal import (
    SignalCalculationStatus,
    SignalCalculationV1,
    SignalContractError,
    SignalDirection,
    SignalEventV1,
)
from ibmd.signal.adapters import (
    SQLiteSignalMarketDataReader,
    SQLiteSignalStore,
    SignalStoreError,
)
from ibmd.signal.application.config import SignalShadowConfig
from ibmd.signal.application.service import SignalShadowService
from ibmd.signal.domain.calculation import (
    CandidateWindow,
    build_potential,
    calculate_centered_pearson_batch,
    rank_candidates,
)

INSTRUMENT = "MNQ"
CON_ID = 793356225
LOCAL_SYMBOL = "MNQU6"
MARKET_MANIFEST = ROOT / "migrations" / "market_data.v1.json"
SIGNAL_MANIFEST = ROOT / "migrations" / "signal.v1.json"


def apply_schema(path: Path, manifest: Path) -> None:
    store_name, migrations = load_migration_manifest(manifest)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def target_candidate(signal_ts: int) -> CandidateWindow:
    return CandidateWindow(
        signal_bar_ts=signal_ts,
        signal_time_ct="2026-07-23 05:00:00",
        pattern_start_ts=signal_ts - 20,
        pattern_end_ts=signal_ts,
        trade_start_ts=signal_ts,
        trade_end_ts=signal_ts + 60,
    )


def legacy_candidate(signal_ts: int) -> LegacyCandidateWindow:
    return LegacyCandidateWindow(
        signal_bar_ts=signal_ts,
        signal_bar_time_ct="2026-07-23 05:00:00",
        hour_ct=5,
        pattern_start_ts=signal_ts - 20,
        pattern_end_ts=signal_ts,
        trade_start_ts=signal_ts,
        trade_end_ts=signal_ts + 60,
    )


def signal_event(
    *,
    calculation_id: str,
    source_bar_id: str,
    entry_price: float,
) -> SignalEventV1:
    return SignalEventV1(
        event_id=new_id("signal_event"),
        calculation_id=calculation_id,
        strategy_id="IBMarketData.rolling",
        strategy_version=1,
        configuration_hash="a" * 64,
        instrument_id=INSTRUMENT,
        source_bar_id=source_bar_id,
        source_bar_revision=1,
        source_con_id=CON_ID,
        source_local_symbol=LOCAL_SYMBOL,
        signal_bar_utc="2026-07-23T10:00:00Z",
        created_at_utc="2026-07-23T10:00:01Z",
        direction=SignalDirection.LONG,
        entry_price=entry_price,
        best_pearson=0.9,
        candidate_score_best=0.8,
        potential_end_delta_points=2.0,
        potential_max_profit_points=3.0,
        potential_max_drawdown_points=-1.0,
        potential_used=2,
    )


def signal_calculation(
    *,
    source_bar_id: str | None = None,
    calculation_id: str | None = None,
    with_event: bool = True,
    entry_price: float = 100.0,
) -> SignalCalculationV1:
    calculation = calculation_id or new_id("signal_calculation")
    source = source_bar_id or new_id("market_bar")
    event = (
        signal_event(
            calculation_id=calculation,
            source_bar_id=source,
            entry_price=entry_price,
        )
        if with_event
        else None
    )
    return SignalCalculationV1(
        calculation_id=calculation,
        strategy_id="IBMarketData.rolling",
        strategy_version=1,
        configuration_hash="a" * 64,
        instrument_id=INSTRUMENT,
        source_bar_id=source,
        source_bar_revision=1,
        source_con_id=CON_ID,
        source_local_symbol=LOCAL_SYMBOL,
        signal_bar_utc="2026-07-23T10:00:00Z",
        calculated_at_utc="2026-07-23T10:00:01Z",
        status=(
            SignalCalculationStatus.SIGNAL
            if with_event
            else SignalCalculationStatus.NO_SIGNAL
        ),
        entry_price=entry_price,
        raw_candidate_count=3,
        valid_pattern_count=3,
        pearson_pass_count=2,
        minmax_pass_count=2,
        skipped_pattern_count=0,
        best_raw_pearson=0.95,
        best_signal_pearson=0.95 if with_event else None,
        best_candidate_score=0.8 if with_event else None,
        potential_direction=(
            SignalDirection.LONG
            if with_event
            else SignalDirection.NONE
        ),
        potential_end_delta_points=2.0 if with_event else 0.0,
        potential_max_profit_points=3.0 if with_event else 0.0,
        potential_max_drawdown_points=-1.0 if with_event else 0.0,
        potential_used=2 if with_event else 0,
        reason=None if with_event else "no_candidates",
        event=event,
    )


class SignalContractAndStoreTest(unittest.TestCase):
    def test_contract_round_trip_and_status_invariants(self) -> None:
        calculation = signal_calculation()
        restored = SignalCalculationV1.from_dict(
            calculation.to_dict()
        )
        self.assertEqual(restored, calculation)

        invalid = calculation.to_dict()
        invalid["status"] = "NO_SIGNAL"
        with self.assertRaises(SignalContractError):
            SignalCalculationV1.from_dict(invalid)

    def test_store_is_idempotent_and_rejects_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "signal.sqlite3"
            apply_schema(database, SIGNAL_MANIFEST)
            store = SQLiteSignalStore(database)
            store.validate_schema()
            calculation = signal_calculation()
            self.assertEqual(store.publish(calculation), calculation)
            self.assertEqual(store.publish(calculation), calculation)
            latest = store.read_latest_calculation(INSTRUMENT)
            self.assertEqual(latest, calculation)

            conflict = signal_calculation(
                source_bar_id=calculation.source_bar_id,
                entry_price=101.0,
            )
            with self.assertRaises(SignalStoreError):
                store.publish(conflict)


class SignalDomainParityTest(unittest.TestCase):
    def test_pearson_filter_and_rank_match_legacy_functions(self) -> None:
        current = np.asarray([100.0, 101.0, 102.0, 103.0])
        matrix = np.asarray(
            [
                [200.0, 201.0, 202.0, 203.0],
                [300.0, 301.0, 300.0, 299.0],
                [400.0, 401.0, 402.5, 404.0],
            ],
            dtype=float,
        )
        candidates = tuple(
            target_candidate(1_000 + index * 100)
            for index in range(3)
        )
        legacy_candidates = [
            legacy_candidate(item.signal_bar_ts)
            for item in candidates
        ]

        target_pearson = calculate_centered_pearson_batch(
            current,
            matrix,
        )
        legacy_pearson = legacy_pearson_batch(current, matrix)
        np.testing.assert_allclose(
            target_pearson,
            legacy_pearson,
            rtol=0.0,
            atol=1e-12,
        )

        target = rank_candidates(
            current_values=current,
            candidates=candidates,
            candidate_matrix=matrix,
            pearson_minimum=0.7,
            minmax_hard_filter_max_ratio=1.5,
            score_pearson_weight=1.0,
            score_end_delta_weight=1.0,
            score_minmax_weight=1.0,
        )
        passed = np.flatnonzero(legacy_pearson >= 0.7)
        legacy_filtered = legacy_minmax_filter(
            current_values=current,
            candidates=[
                legacy_candidates[int(index)] for index in passed
            ],
            candidate_matrix=matrix[passed, :],
            pearson_scores=legacy_pearson[passed],
            max_ratio=1.5,
        )
        legacy_ranked = legacy_rank(
            current_values=current,
            candidates=legacy_filtered.valid_candidates,
            candidate_matrix=legacy_filtered.candidate_matrix,
            pearson_scores=legacy_filtered.pearson_scores,
            pearson_weight=1.0,
            end_delta_weight=1.0,
            minmax_weight=1.0,
        )
        self.assertEqual(
            [
                item.candidate.signal_bar_ts
                for item in target.ranked_candidates
            ],
            [
                item.signal_bar_ts
                for item in legacy_ranked.valid_candidates
            ],
        )
        np.testing.assert_allclose(
            [
                item.candidate_score
                for item in target.ranked_candidates
            ],
            legacy_ranked.candidate_scores,
            rtol=0.0,
            atol=1e-12,
        )

    def test_potential_matches_legacy_for_same_full_series(self) -> None:
        current = np.asarray([100.0, 101.0, 102.0, 103.0])
        matrix = np.asarray(
            [
                [200.0, 201.0, 202.0, 203.0],
                [300.0, 301.0, 302.0, 303.0],
                [400.0, 401.0, 402.0, 403.0],
            ]
        )
        candidates = tuple(
            target_candidate(10_000 + index * 100)
            for index in range(3)
        )
        ranking = rank_candidates(
            current_values=current,
            candidates=candidates,
            candidate_matrix=matrix,
            pearson_minimum=0.7,
            minmax_hard_filter_max_ratio=1.5,
            score_pearson_weight=1.0,
            score_end_delta_weight=1.0,
            score_minmax_weight=1.0,
        )
        full_series = np.asarray(
            [
                100.0,
                101.0,
                102.0,
                103.0,
                103.1,
                103.2,
                103.3,
                103.4,
                103.5,
                103.6,
                103.7,
                103.8,
                103.9,
                104.0,
                104.1,
                104.2,
            ]
        )
        full = {
            candidate.signal_bar_ts: full_series
            for candidate in candidates
        }
        target = build_potential(
            current_values=current,
            ranking=ranking,
            full_values_by_signal_ts=full,
            bar_size_seconds=5,
            potential_horizon_minutes=1,
            minimum_candidate_count=2,
            maximum_candidate_count=3,
        )

        legacy_candidates = [
            legacy_candidate(item.candidate.signal_bar_ts)
            for item in ranking.ranked_candidates
        ]
        legacy_window = LegacySignalWindow(
            signal_bar_ts=20_000,
            pattern_start_ts=19_980,
            pattern_end_ts=20_000,
            pattern_seconds=20,
            trade_start_ts=20_000,
            trade_end_ts=20_060,
            trade_seconds=60,
        )
        with patch(
            "ib_signal.signal_candidate_potential."
            "read_candidate_full_values",
            side_effect=lambda instrument_code, candidate, **kwargs: (
                full[candidate.signal_bar_ts]
            ),
        ):
            legacy = legacy_build_potential(
                instrument_code=INSTRUMENT,
                signal_window=legacy_window,
                current_values=current,
                candidates=legacy_candidates,
                candidate_scores=np.asarray(
                    [
                        item.candidate_score
                        for item in ranking.ranked_candidates
                    ]
                ),
                min_count=2,
                max_count=3,
            )
        self.assertEqual(target.direction, legacy.direction)
        self.assertEqual(
            target.used_candidate_count,
            legacy.used_candidates_count,
        )
        self.assertAlmostEqual(
            target.end_delta_points,
            legacy.end_delta_points,
            places=12,
        )
        self.assertAlmostEqual(
            target.max_profit_points,
            legacy.max_profit_points,
            places=12,
        )
        self.assertAlmostEqual(
            target.max_drawdown_points,
            legacy.max_drawdown_points,
            places=12,
        )


class CapturingHealth:
    def __init__(self) -> None:
        self.values = []

    def publish(self, value) -> None:
        self.values.append(value)


def insert_series(
    connection: sqlite3.Connection,
    *,
    start: datetime,
    count: int,
    base: float,
) -> None:
    for index in range(count):
        bar_start = start + timedelta(seconds=index * 5)
        start_ts = int(bar_start.timestamp())
        end = bar_start + timedelta(seconds=5)
        close = base + index * 0.1
        connection.execute(
            """
            INSERT INTO internal_market_bars (
                bar_id,
                instrument_id,
                con_id,
                local_symbol,
                bar_start_ts,
                bar_start_utc,
                bar_end_utc,
                bar_duration_seconds,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close,
                source_kind,
                first_published_at_utc,
                published_at_utc,
                revision,
                complete,
                volume,
                average,
                bar_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 5, ?, ?, ?, ?, ?, ?, ?, ?,
                      'HISTORY', ?, ?, 1, 1, NULL, NULL, NULL)
            """,
            (
                new_id("market_bar"),
                INSTRUMENT,
                CON_ID,
                LOCAL_SYMBOL,
                start_ts,
                format_utc(bar_start),
                format_utc(end),
                close,
                close + 0.5,
                close - 0.5,
                close,
                close + 0.25,
                close + 0.75,
                close - 0.25,
                close + 0.25,
                format_utc(end),
                format_utc(end),
            ),
        )


class TargetSignalEndToEndTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.market_database = root / "market.sqlite3"
        self.signal_database = root / "signal.sqlite3"
        apply_schema(self.market_database, MARKET_MANIFEST)
        apply_schema(self.signal_database, SIGNAL_MANIFEST)

        connection = sqlite3.connect(self.market_database)
        try:
            for hour, base in ((7, 390.0), (8, 395.0), (9, 398.0)):
                insert_series(
                    connection,
                    start=datetime(
                        2026,
                        7,
                        23,
                        hour - 1,
                        59,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    count=24,
                    base=base,
                )
            insert_series(
                connection,
                start=datetime(
                    2026,
                    7,
                    23,
                    9,
                    59,
                    0,
                    tzinfo=timezone.utc,
                ),
                count=12,
                base=400.0,
            )
            latest = connection.execute(
                """
                SELECT *
                FROM internal_market_bars
                WHERE bar_start_ts = ?
                """,
                (
                    int(
                        datetime(
                            2026,
                            7,
                            23,
                            9,
                            59,
                            55,
                            tzinfo=timezone.utc,
                        ).timestamp()
                    ),
                ),
            ).fetchone()
            connection.execute(
                """
                INSERT INTO internal_market_data_state (
                    instrument_id,
                    latest_complete_bar_id,
                    latest_bar_start_ts,
                    latest_bar_end_utc,
                    latest_con_id,
                    latest_local_symbol,
                    last_ingest_at_utc,
                    last_source_status,
                    last_error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OK', NULL)
                """,
                (
                    INSTRUMENT,
                    latest[0],
                    latest[4],
                    latest[6],
                    CON_ID,
                    LOCAL_SYMBOL,
                    latest[18],
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reader_and_service_publish_shadow_signal_without_commands(self) -> None:
        reader = SQLiteSignalMarketDataReader(
            self.market_database,
            instrument_id=INSTRUMENT,
            price_precision=3,
        )
        store = SQLiteSignalStore(self.signal_database)
        config = SignalShadowConfig(
            deployment_id="paper-mnq-01",
            instance_id=new_id("instance"),
            application_version="0.1.0-test",
            service_configuration_hash="b" * 64,
            strategy_id="IBMarketData.rolling",
            strategy_version=1,
            signal_configuration_hash="a" * 64,
            instrument_id=INSTRUMENT,
            price_precision=3,
            source_bar_size_seconds=5,
            max_complete_bar_lag_seconds=60,
            rolling_step_seconds=60,
            pattern_lookback_minutes=1,
            potential_horizon_minutes=1,
            historical_lookback_days=1,
            pearson_minimum=0.7,
            minmax_hard_filter_max_ratio=1.5,
            score_pearson_weight=1.0,
            score_end_delta_weight=1.0,
            score_minmax_weight=1.0,
            potential_candidate_min_count=2,
            potential_candidate_max_count=3,
            minimum_abs_potential_end_delta_points=0.1,
            candidate_hour_profile="current_v1",
        )
        service = SignalShadowService(
            config=config,
            market_data=reader,
            repository=store,
            health_publisher=CapturingHealth(),
            clock=lambda: datetime(
                2026,
                7,
                23,
                10,
                0,
                1,
                tzinfo=timezone.utc,
            ),
        )
        service.validate_dependencies()
        calculation = service.calculate_at(
            int(
                datetime(
                    2026,
                    7,
                    23,
                    10,
                    0,
                    0,
                    tzinfo=timezone.utc,
                ).timestamp()
            )
        )
        self.assertEqual(
            calculation.status,
            SignalCalculationStatus.SIGNAL,
        )
        self.assertIsNotNone(calculation.event)
        self.assertEqual(
            calculation.event.direction,
            SignalDirection.LONG,
        )
        self.assertEqual(calculation.raw_candidate_count, 3)
        self.assertEqual(calculation.valid_pattern_count, 3)
        self.assertEqual(
            store.read_latest_calculation(INSTRUMENT),
            calculation,
        )
        connection = sqlite3.connect(self.signal_database)
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        finally:
            connection.close()
        self.assertNotIn("trade_intents", tables)
        self.assertNotIn("strategy_commands", tables)


if __name__ == "__main__":
    unittest.main()
