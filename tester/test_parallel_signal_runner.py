from __future__ import annotations

import gc
import unittest

import numpy as np

from tester.in_memory_signal_data import InMemorySignalDataSource
from tester.models import SignalVariant, TesterSignal
from tester.parallel_signal_runner import (
    SharedSignalDataOwner,
    SignalChunkResult,
    attach_shared_signal_source,
    build_signal_chunk_tasks,
    count_signal_points,
    merge_signal_chunk_results,
)


class ParallelSignalRunnerTest(unittest.TestCase):
    def test_shared_signal_arrays_roundtrip(self) -> None:
        bar_time_ts = np.arange(0, 600, 5, dtype=np.int64)
        mid_close = np.linspace(100.0, 101.0, bar_time_ts.size)
        candidate_signal_ts = np.arange(60, 601, 60, dtype=np.int64)
        candidate_time_ct = np.asarray(
            [f"2026-08-04 {index:02d}:00:00" for index in range(10)],
            dtype="U19",
        )
        candidate_hour_ct = np.arange(10, dtype=np.int8)
        source = InMemorySignalDataSource(
            instrument_code="MNQ",
            bar_size_seconds=5,
            bar_time_ts=bar_time_ts,
            mid_close=mid_close,
            candidate_signal_ts=candidate_signal_ts,
            candidate_signal_time_ct=candidate_time_ct,
            candidate_hour_ct=candidate_hour_ct,
        )

        with SharedSignalDataOwner(source) as owner:
            attached, segments = attach_shared_signal_source(owner.descriptor)
            try:
                np.testing.assert_array_equal(attached.bar_time_ts, bar_time_ts)
                np.testing.assert_allclose(attached.mid_close, mid_close)
                np.testing.assert_array_equal(
                    attached.candidate_signal_ts,
                    candidate_signal_ts,
                )
                np.testing.assert_array_equal(
                    attached.candidate_signal_time_ct,
                    candidate_time_ct,
                )
                np.testing.assert_array_equal(
                    attached.candidate_hour_ct,
                    candidate_hour_ct,
                )
                self.assertFalse(attached.bar_time_ts.flags.writeable)
                self.assertFalse(attached.mid_close.flags.writeable)
            finally:
                attached = None
                gc.collect()
                for segment in segments:
                    segment.close()

    def test_chunks_cover_every_signal_point_once(self) -> None:
        variant = SignalVariant(90, 30, 0.7, 1.5, 3, 9, 10.0)
        start_ts = 1
        end_ts = 6 * 60 * 60 + 17
        tasks = build_signal_chunk_tasks(
            variant_index=1,
            variant=variant,
            start_ts=start_ts,
            end_ts=end_ts,
            worker_count=20,
        )

        actual_points: list[int] = []
        for task in tasks:
            actual_points.extend(
                range(task.start_ts, task.end_ts + 1, 60)
            )
        expected_points = list(range(60, end_ts + 1, 60))

        self.assertEqual(actual_points, expected_points)
        self.assertEqual(len(actual_points), len(set(actual_points)))
        self.assertEqual(
            len(actual_points),
            count_signal_points(start_ts=start_ts, end_ts=end_ts),
        )
        self.assertLessEqual(len(tasks), 20 * 4)

    def test_merge_is_ordered_and_sums_counters(self) -> None:
        later = SignalChunkResult(
            variant_index=1,
            chunk_index=1,
            signals=[self._signal(180)],
            calculation_points=2,
            skipped_points=1,
            no_signal_points=0,
            stage_timings_seconds={"total": 2.0, "pearson": 0.5},
            elapsed_seconds=3.0,
        )
        earlier = SignalChunkResult(
            variant_index=1,
            chunk_index=0,
            signals=[self._signal(60), self._signal(120)],
            calculation_points=2,
            skipped_points=0,
            no_signal_points=1,
            stage_timings_seconds={"total": 1.0, "pearson": 0.25},
            elapsed_seconds=2.0,
        )

        merged = merge_signal_chunk_results([later, earlier])

        self.assertEqual(
            [row.signal_bar_ts for row in merged.signal_batch.signals],
            [60, 120, 180],
        )
        self.assertEqual(merged.signal_batch.calculation_points, 4)
        self.assertEqual(merged.signal_batch.skipped_points, 1)
        self.assertEqual(merged.signal_batch.no_signal_points, 1)
        self.assertEqual(merged.stage_timings_seconds["total"], 3.0)
        self.assertEqual(merged.stage_timings_seconds["pearson"], 0.75)
        self.assertEqual(merged.worker_elapsed_seconds, 5.0)
        self.assertEqual(merged.chunk_count, 2)

    @staticmethod
    def _signal(signal_bar_ts: int) -> TesterSignal:
        return TesterSignal(
            signal_bar_ts=signal_bar_ts,
            signal_time_msk="2026-08-04 00:00:00",
            signal_time_ct="2026-08-03 16:00:00",
            direction="LONG",
            reference_price=100.0,
            best_pearson=0.8,
            best_candidate_score=0.7,
            potential_end_delta_points=10.0,
            potential_max_profit_points=15.0,
            potential_max_drawdown_points=-5.0,
            potential_used=3,
            raw_candidates_count=10,
            valid_candidates_count=9,
            pearson_passed_count=5,
            minmax_passed_count=4,
        )


if __name__ == "__main__":
    unittest.main()
