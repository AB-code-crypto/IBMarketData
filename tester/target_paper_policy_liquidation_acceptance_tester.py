from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactStore
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptanceRunner,
)
from ibmd.operations.paper_policy_liquidation_acceptance import (
    PaperPolicyLiquidationAcceptanceRunner,
)
from ibmd.public_contracts.liquidation import LiquidationReason
from tester.target_paper_liquidation_acceptance_tester import (
    EPISODE_ID,
    OPERATION_ID,
    FakeExecutor,
    FakeStateSource,
    closed_state,
    paper_payload,
    policy,
    state,
)

TRIGGER_ID = "liquidation_trigger_00000000000000000000000000000001"


def write_source_summary(path: Path, *, schema_name: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_name": schema_name,
                "schema_version": 1,
                "drill_id": "source-policy-test",
                "position_episode_id": EPISODE_ID,
                "position_proof": {"accepted": True},
                "protection": {
                    "fully_live": True,
                    "stop_state": "LIVE",
                    "take_profit_state": "LIVE",
                },
                "live_position_left_protected": True,
            }
        ),
        encoding="utf-8",
    )


def trigger_payload(reason: str, observed_at: str) -> dict:
    source_ref = (
        "daily-flat:CME_EQUITY_INDEX:2026-07-27"
        if reason == "DAILY_FLAT"
        else "rollover:MNQU6:793356225"
    )
    return {
        "position_episode_id": EPISODE_ID,
        "observed_at_utc": observed_at,
        "selected_reason": reason,
        "selected_source_ref": source_ref,
        "selected_detail": f"test {reason.lower()} trigger",
        "all_candidates": [
            {
                "reason": reason,
                "source_ref": source_ref,
                "detail": f"test {reason.lower()} trigger",
            }
        ],
        "blocked_reasons": [],
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
        },
        "liquidation_trigger": {
            "trigger_id": TRIGGER_ID,
            "liquidation_operation_id": OPERATION_ID,
            "reason": reason,
            "source_ref": source_ref,
            "triggered_at_utc": observed_at,
        },
        "operation_created": True,
        "trigger_created": True,
        "broker_mutations_performed": False,
        "automatic_retry_enabled": False,
    }


def successful_payloads(reason: str, observed_at: str) -> list[dict]:
    return [
        trigger_payload(reason, observed_at),
        paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),
        paper_payload(action="CANCEL_STOP", mutation=True),
        paper_payload(
            action="SUBMIT_MARKET_CLOSE",
            mutation=True,
            operation_state="RECONCILING",
            attempt_state="FILLED",
        ),
        paper_payload(
            action="WAIT_FOR_FLAT",
            mutation=False,
            operation_state="SUCCEEDED",
            attempt_state="FILLED",
            episode_closed=True,
        ),
        paper_payload(
            action="NONE",
            mutation=False,
            operation_state="SUCCEEDED",
            attempt_state="FILLED",
            episode_closed=True,
        ),
    ]


def successful_states():
    return [
        state(),
        state(next_action="CANCEL_STOP", exposed=1),
        state(
            operation_state="PREPARING",
            next_action="SUBMIT_MARKET_CLOSE",
            exposed=0,
        ),
        state(
            operation_state="RECONCILING",
            next_action="WAIT_FOR_FLAT",
            attempt_state="FILLED",
            exposed=0,
        ),
        closed_state(),
        closed_state(),
    ]


class PaperPolicyLiquidationAcceptanceTest(unittest.TestCase):
    def run_scenario(self, reason: LiquidationReason, schema_name: str):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(summary, schema_name=schema_name)
            observed = (
                "2026-07-27T19:59:51Z"
                if reason == LiquidationReason.DAILY_FLAT
                else "2026-09-16T22:00:01Z"
            )
            executor = FakeExecutor(
                successful_payloads(reason.value, observed)
            )
            source = FakeStateSource(successful_states())
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            runner = PaperPolicyLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                scenario=reason,
                logical_trigger_at_utc=observed,
                allow_unqualified_session=(
                    reason == LiquidationReason.DAILY_FLAT
                ),
                command_executor=executor,
                state_source=source,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            )
            result = runner.run()
            self.assertTrue(source.validated)
            self.assertEqual(result.scenario, reason)
            self.assertEqual(result.trigger_id, TRIGGER_ID)
            self.assertEqual(
                result.trigger_candidate_reasons,
                (reason.value,),
            )
            payload = result.to_dict()
            self.assertTrue(payload["policy_trigger_proven"])
            self.assertEqual(payload["broker_mutation_count"], 3)
            self.assertTrue(payload["state"]["fully_closed"])
            self.assertTrue(payload["flat_proof"]["accepted"])
            self.assertEqual(executor.calls[0][0], "liquidation-request")
            self.assertEqual(
                executor.calls[0][1],
                "prepare_execution_policy_liquidation_paper_drill_v2.py",
            )
            self.assertEqual(
                executor.calls[-1][0],
                "liquidation-idempotency",
            )
            self.assertTrue((artifacts.directory / "summary.json").is_file())

    def test_daily_flat_trigger_closes_normal_entry(self) -> None:
        self.run_scenario(
            LiquidationReason.DAILY_FLAT,
            "PaperAcceptanceResult",
        )

    def test_rollover_trigger_closes_reverse_position(self) -> None:
        self.run_scenario(
            LiquidationReason.ROLLOVER,
            "PaperReverseAcceptanceResult",
        )

    def test_trigger_reason_mismatch_stops_before_broker_actions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(
                summary,
                schema_name="PaperRestartAcceptanceResult",
            )
            observed = "2026-07-27T19:59:51Z"
            payload = trigger_payload("ROLLOVER", observed)
            executor = FakeExecutor([payload])
            runner = PaperPolicyLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                scenario=LiquidationReason.DAILY_FLAT,
                logical_trigger_at_utc=observed,
                allow_unqualified_session=True,
                command_executor=executor,
                state_source=FakeStateSource([state()]),
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "selected another reason",
            ):
                runner.run()
            self.assertEqual(len(executor.calls), 1)

    def test_regular_liquidation_accepts_reverse_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(
                summary,
                schema_name="PaperReverseAcceptanceResult",
            )
            runner = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=FakeExecutor([]),
                state_source=FakeStateSource([state()]),
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            source_drill, episode = runner._load_entry_summary()
            self.assertEqual(source_drill, "source-policy-test")
            self.assertEqual(episode, EPISODE_ID)


if __name__ == "__main__":
    unittest.main()
