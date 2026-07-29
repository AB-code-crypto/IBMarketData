from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


app_path = Path("apps/prepare_execution_policy_liquidation_paper_drill_v2.py")
app = app_path.read_text(encoding="utf-8")

app = replace_once(
    app,
    "from __future__ import annotations\n\nimport argparse\n",
    "from __future__ import annotations\n\nfrom dataclasses import replace\n\nimport argparse\n",
    label="dataclasses.replace import",
)

app = replace_once(
    app,
    "from ibmd.execution.domain.liquidation import (\n"
    "    LiquidationDomainError,\n"
    "    request_liquidation,\n"
    ")\n",
    "from ibmd.execution.domain.liquidation import (\n"
    "    LiquidationDomainError,\n"
    "    LiquidationRequestResult,\n"
    "    request_liquidation,\n"
    ")\n",
    label="LiquidationRequestResult import",
)

app = replace_once(
    app,
    "from ibmd.foundation.time import format_utc, parse_utc\n",
    "from ibmd.foundation.time import format_utc, parse_utc, utc_now\n",
    label="utc_now import",
)

app = replace_once(
    app,
    "class PolicyLiquidationDrillError(RuntimeError):\n"
    "    pass\n\n\n"
    "def build_parser() -> argparse.ArgumentParser:\n",
    "class PolicyLiquidationDrillError(RuntimeError):\n"
    "    pass\n\n\n"
    "def _record_request_lifecycle_at(\n"
    "    request: LiquidationRequestResult,\n"
    "    *,\n"
    "    recorded_at_utc: str,\n"
    ") -> LiquidationRequestResult:\n"
    "    recorded = format_utc(parse_utc(recorded_at_utc))\n"
    "    operation = replace(\n"
    "        request.snapshot.operation,\n"
    "        created_at_utc=recorded,\n"
    "        updated_at_utc=recorded,\n"
    "    )\n"
    "    snapshot = replace(request.snapshot, operation=operation)\n"
    "    readiness = replace(\n"
    "        request.execution_readiness,\n"
    "        updated_at_utc=recorded,\n"
    "    )\n"
    "    return replace(\n"
    "        request,\n"
    "        snapshot=snapshot,\n"
    "        execution_readiness=readiness,\n"
    "    )\n\n\n"
    "def build_parser() -> argparse.ArgumentParser:\n",
    label="lifecycle clock helper",
)

app = replace_once(
    app,
    "    request = request_liquidation(\n"
    "        episode=episode,\n"
    "        position=position,\n"
    "        readiness=readiness,\n"
    "        reason=candidate.reason,\n"
    "        source_ref=candidate.source_ref,\n"
    "        observed_at_utc=observed,\n"
    "        existing=None,\n"
    "    )\n"
    "    with ServiceProcessLock(\n",
    "    request = request_liquidation(\n"
    "        episode=episode,\n"
    "        position=position,\n"
    "        readiness=readiness,\n"
    "        reason=candidate.reason,\n"
    "        source_ref=candidate.source_ref,\n"
    "        observed_at_utc=observed,\n"
    "        existing=None,\n"
    "    )\n"
    "    recorded_at = format_utc(utc_now())\n"
    "    request = _record_request_lifecycle_at(\n"
    "        request,\n"
    "        recorded_at_utc=recorded_at,\n"
    "    )\n"
    "    with ServiceProcessLock(\n",
    label="request lifecycle restamp",
)

app = replace_once(
    app,
    "        \"observed_at_utc\": observed,\n"
    "        \"selected_reason\": candidate.reason.value,\n",
    "        \"observed_at_utc\": observed,\n"
    "        \"operation_recorded_at_utc\": recorded_at,\n"
    "        \"logical_trigger_time_decoupled\": True,\n"
    "        \"selected_reason\": candidate.reason.value,\n",
    label="diagnostic payload fields",
)

app_path.write_text(app, encoding="utf-8")


test_path = Path("tester/target_policy_liquidation_logical_clock_tester.py")
if test_path.exists():
    raise RuntimeError(f"test already exists: {test_path}")

test_path.write_text(
    '''from __future__ import annotations

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
''',
    encoding="utf-8",
)

print("Patched policy liquidation logical clock handling and added regression coverage.")
