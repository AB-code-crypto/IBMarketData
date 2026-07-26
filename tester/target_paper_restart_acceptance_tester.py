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

from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway.paper_orders import (
    PaperMarketOrderRequest,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
    PaperProtectiveOrderRequest,
)
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactStore,
    PaperAcceptanceError,
    PaperAcceptancePathsV1,
    PaperAcceptancePolicyV1,
    PositionProofV1,
    ProtectionObservationV1,
)
from ibmd.operations.paper_restart_acceptance import (
    PaperRestartAcceptanceRunner,
    RestartSubmitCheckpointV1,
)
from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulSubmitGateway,
    RESTART_PROBE_EXIT_CODE,
    RestartProbeError,
    require_restart_probe_checkpoint,
)
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.protection import (
    ProtectiveOrderKind,
    ProtectiveOrderType,
)


class ProbeExit(BaseException):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


class FakeGateway:
    def __init__(self) -> None:
        self.market_requests = []
        self.protective_requests = []
        self.closed = False

    async def allocate_order_id(self, *, account_id: str) -> int:
        return 17

    async def submit_market_order(self, request):
        self.market_requests.append(request)
        return PaperOrderSubmissionReceipt(
            broker_order_id=request.broker_order_id,
            order_ref=request.order_ref,
            submitted_at_utc="2026-07-26T12:00:00Z",
        )

    async def submit_protective_order(self, request):
        self.protective_requests.append(request)
        return PaperOrderSubmissionReceipt(
            broker_order_id=request.broker_order_id,
            order_ref=request.order_ref,
            submitted_at_utc="2026-07-26T12:00:01Z",
        )

    async def close(self) -> None:
        self.closed = True


class RestartProbeGatewayTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.route = PaperOrderRoute(
            instrument_id="MNQ",
            con_id=793356225,
            local_symbol="MNQU6",
            last_trade_date="20260918",
            sec_type="FUT",
            exchange="CME",
            currency="USD",
            trading_class="MNQ",
            multiplier=2,
        )

    @staticmethod
    def terminator(code: int):
        raise ProbeExit(code)

    async def test_market_checkpoint_is_written_before_probe_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "market.json"
            inner = FakeGateway()
            gateway = CrashAfterSuccessfulSubmitGateway(
                inner=inner,
                checkpoint_file=target,
                terminate=self.terminator,
            )
            request = PaperMarketOrderRequest(
                account_id="DU000000",
                broker_order_id=17,
                order_ref="IBMD:broker_operation_abc:1",
                side=BrokerOrderSide.BUY,
                quantity=1,
                route=self.route,
            )
            with self.assertRaises(ProbeExit) as captured:
                await gateway.submit_market_order(request)
            self.assertEqual(captured.exception.code, RESTART_PROBE_EXIT_CODE)
            value = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(value["mutation_kind"], "MARKET_ENTRY")
            self.assertEqual(value["receipt"]["broker_order_id"], 17)
            self.assertEqual(value["request"]["order_type"], "MARKET")
            self.assertFalse(value["reconciliation_started"])
            self.assertEqual(len(inner.market_requests), 1)

    async def test_protective_checkpoint_preserves_stop_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "stop.json"
            inner = FakeGateway()
            gateway = CrashAfterSuccessfulSubmitGateway(
                inner=inner,
                checkpoint_file=target,
                terminate=self.terminator,
            )
            request = PaperProtectiveOrderRequest(
                account_id="DU000000",
                broker_order_id=18,
                order_ref="IBMD:protective_order_abc",
                kind=ProtectiveOrderKind.STOP_LOSS,
                side=BrokerOrderSide.SELL,
                order_type=ProtectiveOrderType.STOP,
                quantity=1,
                route=self.route,
                stop_price=20_000.0,
                limit_price=None,
                time_in_force="DAY",
                outside_rth=True,
                oca_group="IBMD-OCA-1",
            )
            with self.assertRaises(ProbeExit):
                await gateway.submit_protective_order(request)
            value = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(value["mutation_kind"], "STOP_LOSS")
            self.assertEqual(value["request"]["stop_price"], 20_000.0)
            self.assertEqual(value["request"]["time_in_force"], "DAY")
            self.assertTrue(value["request"]["outside_rth"])
            self.assertEqual(len(inner.protective_requests), 1)

    def test_checkpoint_path_is_paper_drill_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            valid = root / "runtime" / "paper_restart_acceptance" / "x.json"
            resolved = require_restart_probe_checkpoint(
                environment="paper",
                deployment_id="paper-drill-account1",
                data_root=root,
                checkpoint_file=valid,
            )
            self.assertEqual(resolved, valid.resolve())
            with self.assertRaises(RestartProbeError):
                require_restart_probe_checkpoint(
                    environment="live",
                    deployment_id="paper-drill-account1",
                    data_root=root,
                    checkpoint_file=valid,
                )
            with self.assertRaises(RestartProbeError):
                require_restart_probe_checkpoint(
                    environment="paper",
                    deployment_id="production-account1",
                    data_root=root,
                    checkpoint_file=valid,
                )
            with self.assertRaises(RestartProbeError):
                require_restart_probe_checkpoint(
                    environment="paper",
                    deployment_id="paper-drill-account1",
                    data_root=root,
                    checkpoint_file=root / "outside.json",
                )


class FakeStateSource:
    def __init__(self) -> None:
        self.protection = ProtectionObservationV1(
            position_episode_id="position_episode_test",
            protection_status="PLANNED",
            stop_state="PLANNED",
            stop_order_ref="IBMD:stop",
            stop_broker_order_id=None,
            take_profit_state="PLANNED",
            take_profit_order_ref="IBMD:tp",
            take_profit_broker_order_id=None,
            blocking_reason="protection:stop_not_proven",
        )

    def validate_schema(self) -> None:
        return None

    def read_position_proof(self, **_kwargs) -> PositionProofV1:
        return PositionProofV1(
            accepted=True,
            reason="accepted",
            snapshot_id="position_snapshot_test",
            captured_at_utc="2026-07-26T12:00:03Z",
            source_freshness_seconds=0.5,
            con_id=793356225,
            local_symbol="MNQU6",
            signed_quantity=1.0,
            competing_contract_count=0,
        )

    def read_protection(self, position_episode_id: str) -> ProtectionObservationV1:
        if position_episode_id != self.protection.position_episode_id:
            raise AssertionError(position_episode_id)
        return self.protection


class FakeCrashExecutor:
    def __init__(self, state: FakeStateSource) -> None:
        self.state = state
        self.kinds = []

    @staticmethod
    def checkpoint(kind: str, broker_order_id: int, order_ref: str):
        raw = {
            "schema_name": "PaperRestartSubmitCheckpoint",
            "schema_version": 1,
            "mutation_kind": kind,
            "checkpoint_at_utc": "2026-07-26T12:00:00Z",
            "process_id": 123,
            "expected_exit_code": RESTART_PROBE_EXIT_CODE,
            "request": {
                "broker_order_id": broker_order_id,
                "order_ref": order_ref,
            },
            "receipt": {
                "broker_order_id": broker_order_id,
                "order_ref": order_ref,
                "submitted_at_utc": "2026-07-26T12:00:01Z",
            },
            "reconciliation_started": False,
            "automatic_retry_enabled": False,
        }
        return RestartSubmitCheckpointV1.from_mapping(
            raw,
            expected_kind=kind,
        )

    def run_expected_crash(self, *, expected_kind: str, **_kwargs):
        self.kinds.append(expected_kind)
        if expected_kind == "MARKET_ENTRY":
            return self.checkpoint(
                expected_kind,
                101,
                "IBMD:broker_operation_test:1",
            )
        if expected_kind == "STOP_LOSS":
            self.state.protection = ProtectionObservationV1(
                position_episode_id="position_episode_test",
                protection_status="STOP_SUBMITTING",
                stop_state="SUBMITTING",
                stop_order_ref="IBMD:stop",
                stop_broker_order_id=201,
                take_profit_state="PLANNED",
                take_profit_order_ref="IBMD:tp",
                take_profit_broker_order_id=None,
                blocking_reason="protection:stop_not_proven",
            )
            return self.checkpoint(expected_kind, 201, "IBMD:stop")
        self.state.protection = ProtectionObservationV1(
            position_episode_id="position_episode_test",
            protection_status="STOP_LIVE",
            stop_state="LIVE",
            stop_order_ref="IBMD:stop",
            stop_broker_order_id=201,
            take_profit_state="SUBMITTING",
            take_profit_order_ref="IBMD:tp",
            take_profit_broker_order_id=202,
            blocking_reason="protection:take_profit_outcome_unknown",
        )
        return self.checkpoint(expected_kind, 202, "IBMD:tp")


class FakeNormalExecutor:
    def __init__(self, state: FakeStateSource) -> None:
        self.state = state
        self.steps = []

    @staticmethod
    def entry_payload(submission_performed: bool = False) -> dict:
        return {
            "command_id": "strategy_command_test",
            "operation_id": "broker_operation_test",
            "attempt_id": "broker_attempt_test",
            "attempt_no": 1,
            "order_ref": "IBMD:broker_operation_test:1",
            "broker_order_id": 101,
            "submission_performed": submission_performed,
            "operation_state": "SUCCEEDED",
            "attempt_state": "FILLED",
            "filled_qty": 1,
            "remaining_qty": 0,
        }

    def run_json(self, *, step_name: str, script, arguments):
        self.steps.append(step_name)
        if step_name == "prepare":
            return {
                "ready_for_submit": True,
                "broker_mutations_performed": False,
                "reused_existing_command": False,
                "submit_before_utc": "2099-01-01T00:00:00Z",
                "command": {
                    "command_id": "strategy_command_test",
                    "command_kind": "OPEN",
                    "desired_target_side": "LONG",
                    "desired_target_quantity": 1,
                },
                "command_state": {
                    "command_id": "strategy_command_test",
                    "state": "ADMITTED",
                },
                "execution_fixture": {
                    "position": {"projection_status": "FLAT"},
                    "readiness": {
                        "status": "READY",
                        "broker_actions_enabled": True,
                        "reconciliation_complete": True,
                        "clock_healthy": True,
                    },
                },
                "active_contract": {
                    "con_id": 793356225,
                    "local_symbol": "MNQU6",
                    "contract_is_active": True,
                },
                "session": {"phase": "TRADING"},
            }
        if step_name.startswith("entry-"):
            return self.entry_payload()
        if step_name == "protection-plan":
            self.state.protection = ProtectionObservationV1(
                position_episode_id="position_episode_test",
                protection_status="PLANNED",
                stop_state="PLANNED",
                stop_order_ref="IBMD:stop",
                stop_broker_order_id=None,
                take_profit_state="PLANNED",
                take_profit_order_ref="IBMD:tp",
                take_profit_broker_order_id=None,
                blocking_reason="protection:stop_not_proven",
            )
            return {
                "broker_mutations_performed": False,
                "position_episode": {
                    "position_episode_id": "position_episode_test",
                    "status": "OPEN",
                    "source_operation_id": "broker_operation_test",
                    "side": "LONG",
                    "quantity": 1,
                    "con_id": 793356225,
                    "local_symbol": "MNQU6",
                },
                "strategy_position": {
                    "position_episode_id": "position_episode_test",
                    "projection_status": "OPEN",
                    "side": "LONG",
                    "quantity": 1,
                },
                "protection": {
                    "orders": [
                        {
                            "kind": "STOP_LOSS",
                            "state": "PLANNED",
                            "planned_sequence": 1,
                        },
                        {
                            "kind": "TAKE_PROFIT",
                            "state": "PLANNED",
                            "planned_sequence": 2,
                        },
                    ]
                },
            }
        if step_name.startswith("stop_loss-resume"):
            self.state.protection = ProtectionObservationV1(
                position_episode_id="position_episode_test",
                protection_status="STOP_LIVE",
                stop_state="LIVE",
                stop_order_ref="IBMD:stop",
                stop_broker_order_id=201,
                take_profit_state="PLANNED",
                take_profit_order_ref="IBMD:tp",
                take_profit_broker_order_id=None,
                blocking_reason="protection:take_profit_not_live",
            )
            return {"submission_performed": False}
        if step_name.startswith("take_profit-resume"):
            self.state.protection = ProtectionObservationV1(
                position_episode_id="position_episode_test",
                protection_status="PROTECTED",
                stop_state="LIVE",
                stop_order_ref="IBMD:stop",
                stop_broker_order_id=201,
                take_profit_state="LIVE",
                take_profit_order_ref="IBMD:tp",
                take_profit_broker_order_id=202,
                blocking_reason=None,
            )
            return {"submission_performed": False}
        if step_name == "protective-restart-idempotency":
            return {"submission_performed": False}
        raise AssertionError((step_name, script, arguments))


class PaperRestartAcceptanceRunnerTest(unittest.TestCase):
    def test_market_stop_and_take_profit_are_adopted_without_resubmit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            paths = PaperAcceptancePathsV1(
                repo_root=ROOT,
                decision_database=root / "decision.sqlite3",
                execution_database=root / "execution.sqlite3",
                position_feed_database=root / "positions.sqlite3",
                catalog_root=ROOT / "catalog",
            )
            policy = PaperAcceptancePolicyV1(
                environment="paper",
                account_id="DU000000",
                deployment_id="paper-drill-restart-test",
                instrument_id="MNQ",
                drill_id="restart-test",
                target_side="LONG",
                command_ttl_seconds=600,
                position_max_age_seconds=30,
                entry_max_invocations=3,
                entry_poll_seconds=0,
                position_wait_seconds=10,
                position_poll_seconds=0,
                protective_max_invocations=3,
                protective_poll_seconds=0,
                reconciliation_read_attempts=2,
                reconciliation_poll_seconds=0,
                commission_wait_seconds=0,
                submit_client_id_offset=120,
                protective_submit_client_id_offset=140,
                reconciliation_client_id_offset=100,
                paths=paths,
            )
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            state = FakeStateSource()
            normal = FakeNormalExecutor(state)
            crash = FakeCrashExecutor(state)
            runner = PaperRestartAcceptanceRunner(
                policy=policy,
                command_executor=normal,
                crash_executor=crash,
                state_source=state,
                artifacts=artifacts,
                sleeper=lambda _seconds: None,
            )
            result = runner.run().to_dict()
            self.assertEqual(
                crash.kinds,
                ["MARKET_ENTRY", "STOP_LOSS", "TAKE_PROFIT"],
            )
            self.assertEqual(result["intentional_process_terminations"], 3)
            self.assertEqual(result["broker_mutation_count"], 3)
            self.assertTrue(result["restart_adoption_proven"])
            self.assertTrue(result["all_resume_submissions_false"])
            self.assertEqual(result["attempt_no"], 1)
            self.assertTrue(result["protection"]["fully_live"])

    def test_duplicate_submission_during_resume_is_rejected(self) -> None:
        state = FakeStateSource()
        executor = FakeNormalExecutor(state)
        executor.entry_payload = lambda submission_performed=False: {
            **FakeNormalExecutor.entry_payload(),
            "submission_performed": True,
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            paths = PaperAcceptancePathsV1(
                repo_root=ROOT,
                decision_database=root / "decision.sqlite3",
                execution_database=root / "execution.sqlite3",
                position_feed_database=root / "positions.sqlite3",
                catalog_root=ROOT / "catalog",
            )
            policy = PaperAcceptancePolicyV1(
                environment="paper",
                account_id="DU000000",
                deployment_id="paper-drill-restart-test",
                instrument_id="MNQ",
                drill_id="restart-duplicate-test",
                target_side="LONG",
                command_ttl_seconds=600,
                position_max_age_seconds=30,
                entry_max_invocations=1,
                entry_poll_seconds=0,
                position_wait_seconds=10,
                position_poll_seconds=0,
                protective_max_invocations=1,
                protective_poll_seconds=0,
                reconciliation_read_attempts=1,
                reconciliation_poll_seconds=0,
                commission_wait_seconds=0,
                submit_client_id_offset=120,
                protective_submit_client_id_offset=140,
                reconciliation_client_id_offset=100,
                paths=paths,
            )
            runner = PaperRestartAcceptanceRunner(
                policy=policy,
                command_executor=executor,
                crash_executor=FakeCrashExecutor(state),
                state_source=state,
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperAcceptanceError,
                "another MARKET submission",
            ):
                runner.run()


if __name__ == "__main__":
    unittest.main()
