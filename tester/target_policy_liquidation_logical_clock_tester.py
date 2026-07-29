from __future__ import annotations

from dataclasses import dataclass
import unittest

from apps.prepare_execution_policy_liquidation_paper_drill_v2 import (
    _record_request_lifecycle_at,
)


@dataclass(frozen=True)
class FakeOperation:
    created_at_utc: str
    updated_at_utc: str


@dataclass(frozen=True)
class FakeTrigger:
    triggered_at_utc: str


@dataclass(frozen=True)
class FakeSnapshot:
    operation: FakeOperation
    triggers: tuple[FakeTrigger, ...]


@dataclass(frozen=True)
class FakeReadiness:
    updated_at_utc: str


@dataclass(frozen=True)
class FakeRequest:
    snapshot: FakeSnapshot
    trigger: FakeTrigger
    execution_readiness: FakeReadiness


class PolicyLiquidationLogicalClockTest(unittest.TestCase):
    def test_future_logical_trigger_does_not_future_date_lifecycle(self) -> None:
        logical = "2026-09-16T22:00:01.000000Z"
        recorded = "2026-07-29T10:18:24.000000Z"
        trigger = FakeTrigger(triggered_at_utc=logical)
        request = FakeRequest(
            snapshot=FakeSnapshot(
                operation=FakeOperation(
                    created_at_utc=logical,
                    updated_at_utc=logical,
                ),
                triggers=(trigger,),
            ),
            trigger=trigger,
            execution_readiness=FakeReadiness(updated_at_utc=logical),
        )

        result = _record_request_lifecycle_at(
            request,
            recorded_at_utc=recorded,
        )

        self.assertIsNot(result, request)
        self.assertEqual(result.snapshot.operation.created_at_utc, recorded)
        self.assertEqual(result.snapshot.operation.updated_at_utc, recorded)
        self.assertEqual(result.execution_readiness.updated_at_utc, recorded)
        self.assertEqual(result.trigger.triggered_at_utc, logical)
        self.assertEqual(result.snapshot.triggers[0].triggered_at_utc, logical)
        self.assertEqual(request.snapshot.operation.created_at_utc, logical)


if __name__ == "__main__":
    unittest.main()
