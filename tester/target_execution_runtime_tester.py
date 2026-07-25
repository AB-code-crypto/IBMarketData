from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass

from ibmd.execution.application.runtime import (
    EXECUTION_RUNTIME_STAGE_ORDER,
    ExecutionRuntimeCoordinator,
    ExecutionRuntimeError,
    ExecutionRuntimeStage,
    ExecutionRuntimeStageResultV1,
    ExecutionRuntimeStageStatus,
    ExecutionRuntimeTickStatus,
)

T0 = "2026-07-27T10:00:00Z"


@dataclass
class ScriptedStage:
    stage: ExecutionRuntimeStage
    result: ExecutionRuntimeStageResultV1 | None = None
    error: Exception | None = None
    calls: int = 0

    async def run_once(
        self,
        *,
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        self.calls += 1
        if self.error is not None:
            raise self.error
        if self.result is not None:
            return self.result
        return ExecutionRuntimeStageResultV1.no_action(
            self.stage,
            observed_at_utc=observed_at_utc,
        )


def stages(
    overrides: dict[ExecutionRuntimeStage, ExecutionRuntimeStageResultV1] | None = None,
    errors: dict[ExecutionRuntimeStage, Exception] | None = None,
) -> tuple[ScriptedStage, ...]:
    overrides = overrides or {}
    errors = errors or {}
    return tuple(
        ScriptedStage(
            stage=stage,
            result=overrides.get(stage),
            error=errors.get(stage),
        )
        for stage in EXECUTION_RUNTIME_STAGE_ORDER
    )


class ExecutionRuntimeKernelTest(unittest.TestCase):
    def test_canonical_order_finalizes_before_generic_projection(self) -> None:
        self.assertLess(
            EXECUTION_RUNTIME_STAGE_ORDER.index(
                ExecutionRuntimeStage.POSITION_FINALIZATION
            ),
            EXECUTION_RUNTIME_STAGE_ORDER.index(
                ExecutionRuntimeStage.POSITION_PROJECTION
            ),
        )
        self.assertLess(
            EXECUTION_RUNTIME_STAGE_ORDER.index(
                ExecutionRuntimeStage.PROTECTIVE_SUBMISSION
            ),
            EXECUTION_RUNTIME_STAGE_ORDER.index(
                ExecutionRuntimeStage.COMMAND_ADMISSION
            ),
        )
        self.assertEqual(
            EXECUTION_RUNTIME_STAGE_ORDER[-1],
            ExecutionRuntimeStage.STRATEGIC_SUBMISSION,
        )

    def test_all_no_action_runs_complete_priority_chain(self) -> None:
        values = stages()
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=False,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.IDLE)
        self.assertEqual(tick.broker_mutation_count, 0)
        self.assertIsNone(tick.stopped_after_stage)
        self.assertEqual(
            tuple(item.stage for item in tick.results),
            EXECUTION_RUNTIME_STAGE_ORDER,
        )
        self.assertTrue(all(item.calls == 1 for item in values))

    def test_broker_free_updates_continue_through_lower_stages(self) -> None:
        values = stages(
            {
                ExecutionRuntimeStage.STRATEGIC_RECONCILIATION: (
                    ExecutionRuntimeStageResultV1.updated(
                        ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
                        observed_at_utc=T0,
                        subject_id="broker_operation_a",
                    )
                ),
                ExecutionRuntimeStage.DAILY_RISK: (
                    ExecutionRuntimeStageResultV1.updated(
                        ExecutionRuntimeStage.DAILY_RISK,
                        observed_at_utc=T0,
                        subject_id="daily_risk_calculation_a",
                    )
                ),
            }
        )
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=False,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.PROGRESSED)
        self.assertEqual(len(tick.results), len(EXECUTION_RUNTIME_STAGE_ORDER))
        self.assertTrue(all(item.calls == 1 for item in values))

    def test_blocked_stage_stops_all_lower_priority_work(self) -> None:
        blocked = ExecutionRuntimeStage.LIQUIDATION_ADVANCE
        values = stages(
            {
                blocked: ExecutionRuntimeStageResultV1.blocked(
                    blocked,
                    observed_at_utc=T0,
                    subject_id="liquidation_operation_a",
                    detail="broker mutation is disabled",
                )
            }
        )
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=False,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.BLOCKED)
        self.assertEqual(tick.stopped_after_stage, blocked)
        self.assertEqual(
            len(tick.results),
            EXECUTION_RUNTIME_STAGE_ORDER.index(blocked) + 1,
        )
        for runner in values[: len(tick.results)]:
            self.assertEqual(runner.calls, 1)
        for runner in values[len(tick.results) :]:
            self.assertEqual(runner.calls, 0)

    def test_one_broker_mutation_ends_tick(self) -> None:
        mutated = ExecutionRuntimeStage.PROTECTIVE_SUBMISSION
        values = stages(
            {
                mutated: ExecutionRuntimeStageResultV1.mutated(
                    mutated,
                    observed_at_utc=T0,
                    subject_id="protective_order_a",
                )
            }
        )
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=True,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.MUTATED)
        self.assertEqual(tick.broker_mutation_count, 1)
        self.assertEqual(tick.stopped_after_stage, mutated)
        self.assertEqual(
            len(tick.results),
            EXECUTION_RUNTIME_STAGE_ORDER.index(mutated) + 1,
        )
        self.assertEqual(
            values[EXECUTION_RUNTIME_STAGE_ORDER.index(mutated) + 1].calls,
            0,
        )

    def test_mutation_while_disabled_fails_closed(self) -> None:
        mutated = ExecutionRuntimeStage.REVERSE_HANDOFF
        values = stages(
            {
                mutated: ExecutionRuntimeStageResultV1.mutated(
                    mutated,
                    observed_at_utc=T0,
                    subject_id="strategy_command_a",
                )
            }
        )
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=False,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.FAILED)
        self.assertEqual(tick.broker_mutation_count, 0)
        self.assertEqual(tick.stopped_after_stage, mutated)
        self.assertIn("mutations are disabled", tick.results[-1].detail)

    def test_stage_exception_is_a_fail_closed_tick_result(self) -> None:
        failed = ExecutionRuntimeStage.POSITION_FINALIZATION
        values = stages(errors={failed: RuntimeError("broken evidence")})
        coordinator = ExecutionRuntimeCoordinator(
            stages=values,
            broker_mutations_enabled=False,
        )
        tick = asyncio.run(coordinator.run_tick(observed_at_utc=T0))
        self.assertEqual(tick.status, ExecutionRuntimeTickStatus.FAILED)
        self.assertEqual(tick.stopped_after_stage, failed)
        self.assertIn("broken evidence", tick.results[-1].detail)
        self.assertEqual(
            len(tick.results),
            EXECUTION_RUNTIME_STAGE_ORDER.index(failed) + 1,
        )

    def test_stage_status_flags_are_strict(self) -> None:
        with self.assertRaises(ExecutionRuntimeError):
            ExecutionRuntimeStageResultV1(
                stage=ExecutionRuntimeStage.DAILY_RISK,
                status=ExecutionRuntimeStageStatus.UPDATED,
                observed_at_utc=T0,
                state_changed=False,
            )
        with self.assertRaises(ExecutionRuntimeError):
            ExecutionRuntimeStageResultV1.blocked(
                ExecutionRuntimeStage.DAILY_RISK,
                observed_at_utc=T0,
                detail="",
            )

    def test_missing_or_reordered_stages_are_rejected(self) -> None:
        values = stages()
        with self.assertRaises(ExecutionRuntimeError):
            ExecutionRuntimeCoordinator(
                stages=values[:-1],
                broker_mutations_enabled=False,
            )
        reordered = list(values)
        reordered[0], reordered[1] = reordered[1], reordered[0]
        with self.assertRaises(ExecutionRuntimeError):
            ExecutionRuntimeCoordinator(
                stages=tuple(reordered),
                broker_mutations_enabled=False,
            )


if __name__ == "__main__":
    unittest.main()
