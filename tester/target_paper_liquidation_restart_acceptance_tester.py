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

from ibmd.ib_gateway.paper_cancellations import (
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
)
from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactStore
from ibmd.operations.paper_liquidation_acceptance import (
    FlatPositionProofV1,
    LiquidationStateObservationV1,
    PaperLiquidationAcceptanceError,
    PaperLiquidationAcceptancePathsV1,
    PaperLiquidationAcceptancePolicyV1,
)
from ibmd.operations.paper_liquidation_restart_acceptance import (
    LiquidationRestartCheckpointV1,
    PaperLiquidationRestartAcceptanceRunner,
    ProtectiveCancelIdentityV1,
    RestartCancelCheckpointV1,
)
from ibmd.operations.paper_restart_acceptance import (
    RestartSubmitCheckpointV1,
)
from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulCancelGateway,
    RESTART_PROBE_EXIT_CODE,
)

OPERATION_ID = "liquidation_operation_00000000000000000000000000000001"
ATTEMPT_ID = "liquidation_attempt_00000000000000000000000000000001"
EPISODE_ID = "position_episode_00000000000000000000000000000001"
CLOSE_REF = f"IBMD:{OPERATION_ID}:1"
TP_REF = "IBMD:protective_order_tp"
STOP_REF = "IBMD:protective_order_stop"


class ProbeExit(BaseException):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


class FakeCancellationGateway:
    def __init__(self) -> None:
        self.requests = []

    async def cancel_order(self, request):
        self.requests.append(request)
        return PaperOrderCancelReceipt(
            broker_order_id=request.broker_order_id,
            order_ref=request.order_ref,
            cancel_requested_at_utc="2026-07-26T12:00:00Z",
        )

    async def close(self) -> None:
        return None


class CancellationRestartProbeTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def terminate(code: int):
        raise ProbeExit(code)

    async def test_cancel_checkpoint_is_written_before_probe_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            checkpoint = Path(temporary) / "cancel.json"
            inner = FakeCancellationGateway()
            gateway = CrashAfterSuccessfulCancelGateway(
                inner=inner,
                checkpoint_file=checkpoint,
                terminate=self.terminate,
            )
            request = PaperOrderCancelRequest(
                account_id="DU000000",
                broker_order_id=22,
                order_ref=TP_REF,
            )
            with self.assertRaises(ProbeExit) as captured:
                await gateway.cancel_order(request)
            self.assertEqual(captured.exception.code, RESTART_PROBE_EXIT_CODE)
            value = json.loads(checkpoint.read_text(encoding="utf-8"))
            self.assertEqual(value["schema_name"], "PaperRestartCancelCheckpoint")
            self.assertEqual(value["mutation_kind"], "CANCEL_ORDER")
            self.assertEqual(value["receipt"]["broker_order_id"], 22)
            self.assertEqual(value["receipt"]["order_ref"], TP_REF)
            self.assertFalse(value["reconciliation_started"])
            self.assertEqual(len(inner.requests), 1)


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
        order_ref=None if attempt_state is None else CLOSE_REF,
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


def request_payload() -> dict:
    return {
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
            "next_action": "RECONCILE_EXITS",
        },
        "liquidation_attempt": None,
        "triggers": [{"reason": "MANUAL_EMERGENCY"}],
        "operation_created": True,
        "trigger_created": True,
        "broker_mutations_performed": False,
    }


def advance_payload(*, action: str = "CANCEL_TAKE_PROFIT") -> dict:
    return {
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "CANCELING_EXITS",
            "next_action": action,
            "blocking_reason": None,
        },
        "liquidation_attempt": None,
        "operation_created": False,
        "trigger_created": False,
        "broker_mutations_performed": False,
    }


def resume_payload(
    *,
    action: str,
    attempt_state: str | None = None,
    operation_state: str = "CANCELING_EXITS",
    mutation: bool = False,
) -> dict:
    attempt = None
    if attempt_state is not None:
        attempt = {
            "liquidation_attempt_id": ATTEMPT_ID,
            "attempt_no": 1,
            "order_ref": CLOSE_REF,
            "state": attempt_state,
        }
    return {
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": operation_state,
            "next_action": action,
            "blocking_reason": None,
        },
        "liquidation_attempt": attempt,
        "action": action,
        "broker_mutation_performed": mutation,
        "mutation_error": None,
    }


def write_entry_summary(path: Path, *, schema_name: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_name": schema_name,
                "schema_version": 1,
                "drill_id": "paper-restart-entry-test",
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


def policy(root: Path, summary: Path) -> PaperLiquidationAcceptancePolicyV1:
    return PaperLiquidationAcceptancePolicyV1(
        environment="paper",
        account_id="DU000000",
        deployment_id="paper-drill-liquidation-restart-test",
        strategy_id="IBMarketData.rolling",
        instrument_id="MNQ",
        max_invocations=16,
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


class FakeStateSource:
    def __init__(self, states: list[LiquidationStateObservationV1]) -> None:
        self.states = list(states)
        self.validated = False
        self.identities = {
            "CANCEL_TAKE_PROFIT": ProtectiveCancelIdentityV1(
                action="CANCEL_TAKE_PROFIT",
                state="LIVE",
                broker_order_id=31,
                order_ref=TP_REF,
            ),
            "CANCEL_STOP": ProtectiveCancelIdentityV1(
                action="CANCEL_STOP",
                state="LIVE",
                broker_order_id=32,
                order_ref=STOP_REF,
            ),
        }

    def validate_schema(self) -> None:
        self.validated = True

    def read_state(self, **_kwargs) -> LiquidationStateObservationV1:
        if not self.states:
            raise AssertionError("unexpected state read")
        if len(self.states) == 1:
            return self.states[0]
        return self.states.pop(0)

    def read_flat_proof(self, **_kwargs) -> FlatPositionProofV1:
        return FlatPositionProofV1(
            accepted=True,
            reason="accepted",
            snapshot_id="position_snapshot_00000000000000000000000000000001",
            captured_at_utc="2026-07-26T12:01:00Z",
            source_freshness_seconds=1.0,
            open_contract_count=0,
        )

    def read_protective_cancel_identity(self, *, action: str, **_kwargs):
        return self.identities[action]


class FakeNormalExecutor:
    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = list(payloads)
        self.calls = []

    def run_json(self, *, step_name, script, arguments):
        self.calls.append((step_name, Path(script).name, tuple(arguments)))
        if not self.payloads:
            raise AssertionError(f"unexpected child invocation: {step_name}")
        return self.payloads.pop(0)


class FakeCrashExecutor:
    def __init__(self) -> None:
        self.actions = []

    @staticmethod
    def cancel_checkpoint(action: str, broker_order_id: int, order_ref: str):
        raw = {
            "schema_name": "PaperRestartCancelCheckpoint",
            "schema_version": 1,
            "mutation_kind": "CANCEL_ORDER",
            "checkpoint_at_utc": "2026-07-26T12:00:00Z",
            "process_id": 123,
            "expected_exit_code": RESTART_PROBE_EXIT_CODE,
            "request": {
                "account_id": "DU000000",
                "broker_order_id": broker_order_id,
                "order_ref": order_ref,
            },
            "receipt": {
                "broker_order_id": broker_order_id,
                "order_ref": order_ref,
                "cancel_requested_at_utc": "2026-07-26T12:00:01Z",
            },
            "reconciliation_started": False,
            "automatic_retry_enabled": False,
        }
        parsed = RestartCancelCheckpointV1.from_mapping(raw)
        return LiquidationRestartCheckpointV1(
            action=action,
            broker_order_id=parsed.broker_order_id,
            order_ref=parsed.order_ref,
            acknowledged_at_utc=parsed.cancel_requested_at_utc,
            raw=parsed.to_dict(),
        )

    @staticmethod
    def close_checkpoint():
        raw = {
            "schema_name": "PaperRestartSubmitCheckpoint",
            "schema_version": 1,
            "mutation_kind": "LIQUIDATION_MARKET_CLOSE",
            "checkpoint_at_utc": "2026-07-26T12:00:02Z",
            "process_id": 124,
            "expected_exit_code": RESTART_PROBE_EXIT_CODE,
            "request": {
                "broker_order_id": 41,
                "order_ref": CLOSE_REF,
            },
            "receipt": {
                "broker_order_id": 41,
                "order_ref": CLOSE_REF,
                "submitted_at_utc": "2026-07-26T12:00:03Z",
            },
            "reconciliation_started": False,
            "automatic_retry_enabled": False,
        }
        parsed = RestartSubmitCheckpointV1.from_mapping(
            raw,
            expected_kind="LIQUIDATION_MARKET_CLOSE",
        )
        return LiquidationRestartCheckpointV1(
            action="SUBMIT_MARKET_CLOSE",
            broker_order_id=parsed.broker_order_id,
            order_ref=parsed.order_ref,
            acknowledged_at_utc=parsed.submitted_at_utc,
            raw=parsed.to_dict(),
        )

    def run_expected_crash(
        self,
        *,
        expected_action: str,
        expected_order_ref=None,
        expected_broker_order_id=None,
        **_kwargs,
    ):
        self.actions.append(expected_action)
        if expected_action == "CANCEL_TAKE_PROFIT":
            checkpoint = self.cancel_checkpoint(expected_action, 31, TP_REF)
        elif expected_action == "CANCEL_STOP":
            checkpoint = self.cancel_checkpoint(expected_action, 32, STOP_REF)
        else:
            checkpoint = self.close_checkpoint()
        if expected_order_ref is not None:
            assert checkpoint.order_ref == expected_order_ref
        if expected_broker_order_id is not None:
            assert checkpoint.broker_order_id == expected_broker_order_id
        return checkpoint


class PaperLiquidationRestartAcceptanceTest(unittest.TestCase):
    def test_cancel_and_close_are_adopted_without_repeat_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(
                summary,
                schema_name="PaperRestartAcceptanceResult",
            )
            source = FakeStateSource(
                [
                    state(),
                    state(next_action="RECONCILE_EXITS"),
                    state(next_action="CANCEL_STOP", exposed=1),
                    state(next_action="RECONCILE_EXITS", exposed=1),
                    state(
                        operation_state="PREPARING",
                        next_action="SUBMIT_MARKET_CLOSE",
                        exposed=0,
                    ),
                    state(
                        operation_state="SUBMITTING",
                        next_action="RECONCILE_MARKET_CLOSE",
                        attempt_state="SUBMITTING",
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
            executor = FakeNormalExecutor(
                [
                    request_payload(),
                    advance_payload(),
                    resume_payload(action="CANCEL_STOP"),
                    resume_payload(
                        action="SUBMIT_MARKET_CLOSE",
                        operation_state="PREPARING",
                    ),
                    resume_payload(
                        action="WAIT_FOR_FLAT",
                        operation_state="RECONCILING",
                        attempt_state="FILLED",
                    ),
                    resume_payload(
                        action="NONE",
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                    ),
                    resume_payload(
                        action="NONE",
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                    ),
                ]
            )
            crash = FakeCrashExecutor()
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            result = PaperLiquidationRestartAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                crash_executor=crash,
                state_source=source,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            ).run()
            payload = result.to_dict()
            self.assertTrue(source.validated)
            self.assertEqual(
                crash.actions,
                [
                    "CANCEL_TAKE_PROFIT",
                    "CANCEL_STOP",
                    "SUBMIT_MARKET_CLOSE",
                ],
            )
            self.assertEqual(payload["intentional_process_terminations"], 3)
            self.assertEqual(payload["broker_mutation_count"], 3)
            self.assertEqual(payload["protective_cancel_mode"], "EXPLICIT_BOTH")
            self.assertTrue(payload["initial_advance_broker_free"])
            self.assertTrue(payload["all_resume_mutations_false"])
            self.assertEqual(executor.calls[1][0], "liquidation-initial-advance")
            self.assertEqual(
                executor.calls[1][2][:2],
                ("--advance-position-episode-id", EPISODE_ID),
            )
            self.assertNotIn(
                "--once-paper-position-episode-id",
                executor.calls[1][2],
            )
            self.assertTrue(payload["restart_adoption_proven"])
            self.assertEqual(payload["attempt_no"], 1)
            self.assertTrue(payload["state"]["fully_closed"])
            self.assertTrue(payload["flat_proof"]["accepted"])
            self.assertTrue((artifacts.directory / "summary.json").is_file())

    def test_oca_auto_cancelled_stop_is_adopted_without_repeat_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(
                summary,
                schema_name="PaperRestartAcceptanceResult",
            )
            source = FakeStateSource(
                [
                    state(),
                    state(next_action="RECONCILE_EXITS"),
                    state(
                        operation_state="PREPARING",
                        next_action="SUBMIT_MARKET_CLOSE",
                        exposed=0,
                    ),
                    state(
                        operation_state="SUBMITTING",
                        next_action="RECONCILE_MARKET_CLOSE",
                        attempt_state="SUBMITTING",
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
            executor = FakeNormalExecutor(
                [
                    request_payload(),
                    advance_payload(),
                    resume_payload(
                        action="SUBMIT_MARKET_CLOSE",
                        operation_state="PREPARING",
                    ),
                    resume_payload(
                        action="WAIT_FOR_FLAT",
                        operation_state="RECONCILING",
                        attempt_state="FILLED",
                    ),
                    resume_payload(
                        action="NONE",
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                    ),
                    resume_payload(
                        action="NONE",
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                    ),
                ]
            )
            crash = FakeCrashExecutor()
            result = PaperLiquidationRestartAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                crash_executor=crash,
                state_source=source,
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            ).run()
            payload = result.to_dict()
            self.assertEqual(
                crash.actions,
                ["CANCEL_TAKE_PROFIT", "SUBMIT_MARKET_CLOSE"],
            )
            self.assertEqual(payload["intentional_process_terminations"], 2)
            self.assertEqual(payload["broker_mutation_count"], 2)
            self.assertEqual(
                payload["protective_cancel_mode"],
                "OCA_AUTO_CANCELLED_STOP",
            )
            self.assertTrue(payload["initial_advance_broker_free"])
            self.assertTrue(payload["all_resume_mutations_false"])
            self.assertTrue(payload["restart_adoption_proven"])
            self.assertTrue(payload["state"]["fully_closed"])
            self.assertTrue(payload["flat_proof"]["accepted"])

    def test_resume_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "entry-summary.json"
            write_entry_summary(summary, schema_name="PaperAcceptanceResult")
            source = FakeStateSource(
                [
                    state(),
                    state(next_action="RECONCILE_EXITS"),
                ]
            )
            executor = FakeNormalExecutor(
                [
                    request_payload(),
                    advance_payload(),
                    resume_payload(
                        action="CANCEL_STOP",
                        mutation=True,
                    ),
                ]
            )
            runner = PaperLiquidationRestartAcceptanceRunner(
                policy=policy(root, summary),
                command_executor=executor,
                crash_executor=FakeCrashExecutor(),
                state_source=source,
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "another broker mutation",
            ):
                runner.run()


if __name__ == "__main__":
    unittest.main()
