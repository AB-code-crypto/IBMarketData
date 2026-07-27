from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactStore
from ibmd.operations.paper_liquidation_acceptance import (
    FlatPositionProofV1,
    LiquidationStateObservationV1,
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePathsV1,
    PaperLiquidationAcceptancePolicyV1,
    PaperLiquidationAcceptanceRunner,
)


OPERATION_ID = "liquidation_operation_00000000000000000000000000000001"
ATTEMPT_ID = "liquidation_attempt_00000000000000000000000000000001"
EPISODE_ID = "position_episode_00000000000000000000000000000001"
ORDER_REF = f"IBMD:{OPERATION_ID}:1"


def state(
    *,
    operation_state: str = "REQUESTED",
    next_action: str = "CANCEL_TAKE_PROFIT",
    attempt_state: str | None = None,
    episode_status: str = "OPEN",
    protection_status: str = "PROTECTED",
    exposed: int = 2,
    position_status: str = "OPEN",
    position_side: str = "LONG",
    position_quantity: int = 1,
) -> LiquidationStateObservationV1:
    return LiquidationStateObservationV1(
        liquidation_operation_id=OPERATION_ID,
        operation_state=operation_state,
        next_action=next_action,
        liquidation_attempt_id=(None if attempt_state is None else ATTEMPT_ID),
        attempt_no=None if attempt_state is None else 1,
        attempt_state=attempt_state,
        order_ref=None if attempt_state is None else ORDER_REF,
        trigger_count=1,
        episode_status=episode_status,
        protection_status=protection_status,
        exposed_protective_order_count=exposed,
        strategy_position_status=position_status,
        strategy_position_side=position_side,
        strategy_position_quantity=position_quantity,
    )


def closed_state() -> LiquidationStateObservationV1:
    return state(
        operation_state="SUCCEEDED",
        next_action="NONE",
        attempt_state="FILLED",
        episode_status="CLOSED",
        protection_status="CLOSED",
        exposed=0,
        position_status="FLAT",
        position_side="FLAT",
        position_quantity=0,
    )


def request_payload(*, created: bool = True) -> dict:
    return {
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
        },
        "liquidation_attempt": None,
        "triggers": [{"reason": "MANUAL_EMERGENCY"}],
        "operation_created": created,
        "trigger_created": created,
        "broker_mutations_performed": False,
    }


def paper_payload(
    *,
    action: str,
    mutation: bool,
    operation_state: str = "CANCELING_EXITS",
    attempt_state: str | None = None,
    episode_closed: bool = False,
    mutation_error: str | None = None,
) -> dict:
    attempt = None
    if attempt_state is not None:
        attempt = {
            "liquidation_attempt_id": ATTEMPT_ID,
            "attempt_no": 1,
            "order_ref": ORDER_REF,
            "state": attempt_state,
        }
    return {
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": operation_state,
            "next_action": action,
        },
        "liquidation_attempt": attempt,
        "action": action,
        "episode_closed": episode_closed,
        "broker_mutation_performed": mutation,
        "mutation_error": mutation_error,
    }


class FakeExecutor:
    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = list(payloads)
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []

    def run_json(self, *, step_name, script, arguments):
        self.calls.append((step_name, Path(script).name, tuple(arguments)))
        if not self.payloads:
            raise AssertionError("unexpected child invocation")
        return self.payloads.pop(0)


class FakeStateSource:
    def __init__(
        self,
        states: list[LiquidationStateObservationV1],
        *,
        flat: FlatPositionProofV1 | None = None,
    ) -> None:
        self.states = list(states)
        self.flat = flat or FlatPositionProofV1(
            accepted=True,
            reason="accepted",
            snapshot_id="position_snapshot_00000000000000000000000000000001",
            captured_at_utc="2026-07-26T22:01:00Z",
            source_freshness_seconds=1.0,
            open_contract_count=0,
        )
        self.validated = False

    def validate_schema(self) -> None:
        self.validated = True

    def read_state(self, **_kwargs) -> LiquidationStateObservationV1:
        if not self.states:
            raise AssertionError("unexpected state read")
        if len(self.states) == 1:
            return self.states[0]
        return self.states.pop(0)

    def read_flat_proof(self, **_kwargs) -> FlatPositionProofV1:
        return self.flat


def write_entry_summary(path: Path, *, fully_live: bool = True) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_name": "PaperAcceptanceResult",
                "schema_version": 1,
                "drill_id": "paper-acceptance-test",
                "position_episode_id": EPISODE_ID,
                "position_proof": {"accepted": True},
                "protection": {
                    "fully_live": fully_live,
                    "stop_state": "LIVE",
                    "take_profit_state": "LIVE",
                },
                "live_position_left_protected": True,
            }
        ),
        encoding="utf-8",
    )


def policy(root: Path, summary: Path) -> PaperLiquidationAcceptancePolicyV1:
    return PaperLiquidationAcceptancePolicyV1(
        environment="paper",
        account_id="DU000000",
        deployment_id="paper-drill-test",
        strategy_id="IBMarketData.rolling",
        instrument_id="MNQ",
        max_invocations=8,
        poll_seconds=0.0,
        position_max_age_seconds=30.0,
        reconciliation_read_attempts=5,
        reconciliation_poll_seconds=0.0,
        commission_wait_seconds=0.0,
        cancel_client_id_offset=140,
        submit_client_id_offset=160,
        reconciliation_client_id_offset=100,
        paths=PaperLiquidationAcceptancePathsV1(
            repo_root=root,
            execution_database=root / "execution.sqlite3",
            position_feed_database=root / "positions.sqlite3",
            catalog_root=root / "catalog",
            entry_summary=summary,
        ),
    )


class PaperLiquidationAcceptanceTest(unittest.TestCase):
    def test_fresh_protected_position_closes_once_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            executor = FakeExecutor(
                [
                    request_payload(),
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
            )
            source = FakeStateSource(
                [
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
            )
            result = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                state_source=source,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            ).run()
            self.assertTrue(source.validated)
            self.assertEqual(result.take_profit_cancel_count, 1)
            self.assertEqual(result.stop_cancel_count, 1)
            self.assertEqual(result.market_close_submission_count, 1)
            self.assertEqual(result.to_dict()["broker_mutation_count"], 3)
            self.assertTrue(result.state.fully_closed)
            self.assertTrue(result.flat_proof.accepted)
            self.assertTrue((artifacts.directory / "summary.json").is_file())
            self.assertEqual(executor.calls[-1][0], "liquidation-idempotency")

    def test_oca_auto_cancelled_stop_is_valid_fresh_liquidation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            executor = FakeExecutor(
                [
                    request_payload(),
                    paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),
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
            )
            source = FakeStateSource(
                [
                    state(),
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
            )
            result = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                state_source=source,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            ).run()
            self.assertEqual(result.take_profit_cancel_count, 1)
            self.assertEqual(result.stop_cancel_count, 0)
            self.assertEqual(result.market_close_submission_count, 1)
            self.assertEqual(result.durable_market_close_attempt_count, 1)
            self.assertEqual(
                result.protective_cancel_mode,
                "OCA_AUTO_CANCELLED_STOP",
            )
            self.assertFalse(result.recovered_from_durable_state)
            self.assertEqual(result.to_dict()["broker_mutation_count"], 2)

    def test_closed_operation_can_recover_summary_without_broker_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            executor = FakeExecutor(
                [
                    request_payload(created=False),
                    paper_payload(
                        action="NONE",
                        mutation=False,
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                        episode_closed=True,
                    ),
                ]
            )
            source = FakeStateSource([closed_state(), closed_state()])
            result = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                state_source=source,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            ).run()
            self.assertEqual(result.invocation_count, 0)
            self.assertEqual(result.take_profit_cancel_count, 0)
            self.assertEqual(result.stop_cancel_count, 0)
            self.assertEqual(result.market_close_submission_count, 0)
            self.assertEqual(result.durable_market_close_attempt_count, 1)
            self.assertTrue(result.recovered_from_durable_state)
            self.assertEqual(
                result.protective_cancel_mode,
                "RECOVERED_DURABLE_CLOSED_STATE",
            )
            self.assertEqual(result.to_dict()["broker_mutation_count"], 0)
            self.assertEqual(executor.calls[-1][0], "liquidation-idempotency")
            self.assertTrue((artifacts.directory / "summary.json").is_file())

    def test_unknown_market_close_outcome_stops_without_new_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            executor = FakeExecutor(
                [
                    request_payload(),
                    paper_payload(
                        action="RECONCILE_MARKET_CLOSE",
                        mutation=False,
                        operation_state="RECONCILING",
                        attempt_state="UNKNOWN_OUTCOME",
                    ),
                ]
            )
            source = FakeStateSource([state()])
            runner = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                state_source=source,
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "UNKNOWN_OUTCOME",
            ):
                runner.run()
            self.assertEqual(len(executor.calls), 2)

    def test_duplicate_cancel_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            executor = FakeExecutor(
                [
                    request_payload(),
                    paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),
                    paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),
                ]
            )
            source = FakeStateSource(
                [
                    state(),
                    state(),
                ]
            )
            runner = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                state_source=source,
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "action repeated",
            ):
                runner.run()

    def test_entry_summary_must_prove_live_protection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary, fully_live=False)
            runner = PaperLiquidationAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=FakeExecutor([]),
                state_source=FakeStateSource([state()]),
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "live protective orders",
            ):
                runner.run()

    def test_policy_rejects_live_environment_and_duplicate_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary)
            base = policy(root, summary)
            values = dict(base.__dict__)
            values["environment"] = "live"
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "IBMD_ENVIRONMENT=paper",
            ):
                PaperLiquidationAcceptancePolicyV1(**values)
            values = dict(base.__dict__)
            values["submit_client_id_offset"] = values[
                "cancel_client_id_offset"
            ]
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "must be distinct",
            ):
                PaperLiquidationAcceptancePolicyV1(**values)


if __name__ == "__main__":
    unittest.main()
