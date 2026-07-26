from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.execution.application.paper_drill import (
    PaperDrillDecisionEvaluation,
)
from ibmd.execution.application.paper_reverse_drill import (
    PaperReverseDrillPolicyV1,
    PaperReverseDrillPreparationError,
    PaperReverseExecutionDrillPreparer,
)
from ibmd.execution.domain import RegisteredFuturesContractV1
from ibmd.execution.domain.protective_uncertainty import (
    readiness_for_protection,
)
from ibmd.foundation.identity import new_id
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceError,
    PaperAcceptancePathsV1,
    PaperAcceptancePolicyV1,
    PositionProofV1,
    ProtectionObservationV1,
)
from ibmd.operations.paper_reverse_acceptance import (
    PaperReverseAcceptanceRunner,
)
from ibmd.public_contracts.decision import DesiredTargetSide
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)
from tester.target_execution_liquidation_tester import live_protection
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    CON_ID,
    DEPLOYMENT,
    INSTRUMENT,
    LOCAL_SYMBOL,
    STRATEGY,
    T2,
    blocked_readiness,
    position_snapshot,
    strategy_position,
)

T0 = datetime(2026, 7, 27, 10, 0, 0, tzinfo=timezone.utc)
SOURCE_EPISODE = "position_episode_source"
TARGET_EPISODE = "position_episode_target"
COMMAND = "strategy_command_reverse_acceptance"
OPERATION = "broker_operation_reverse_acceptance"
ATTEMPT = "broker_attempt_reverse_acceptance"
ORDER_REF = "IBMD:broker_operation_reverse_acceptance:1"
TP_REF = "IBMD:source:TP"
STOP_REF = "IBMD:source:SL"


class MemoryDecisionRepository:
    def __init__(self) -> None:
        self.evaluation = None

    def read_record(self, decision_id):
        if self.evaluation is None:
            return None
        return (
            self.evaluation.record
            if self.evaluation.record.decision_id == decision_id
            else None
        )

    def read_command(self, command_id):
        if self.evaluation is None:
            return None
        command = self.evaluation.command
        return command if command is not None and command.command_id == command_id else None

    def publish(self, evaluation: PaperDrillDecisionEvaluation):
        if self.evaluation is None:
            self.evaluation = evaluation
        elif self.evaluation != evaluation:
            raise AssertionError("decision changed")
        return self.evaluation


class MemoryExecutionRepository:
    def __init__(self, position, readiness, risk) -> None:
        self.position = position
        self.readiness = readiness
        self.risk = risk
        self.command_state = None
        self.fixture = None

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def read_latest_daily_risk(self, **_scope):
        return self.risk

    def read_command_state(self, command_id):
        if self.command_state is None:
            return None
        return self.command_state if self.command_state.command_id == command_id else None

    def publish_fixture(self, fixture):
        self.fixture = fixture
        self.position = fixture.position
        self.readiness = fixture.readiness
        self.risk = fixture.daily_risk
        return self.readiness, self.position, self.risk

    def publish_admission(self, admission):
        self.command_state = admission.command_state
        return self.command_state


class MemoryProtectionSource:
    def __init__(self, episode, protection) -> None:
        self.episode = episode
        self.protection = protection

    def read_episode(self, position_episode_id):
        return self.episode if position_episode_id == self.episode.position_episode_id else None

    def read_protection_by_episode(self, position_episode_id):
        return self.protection if position_episode_id == self.episode.position_episode_id else None


class SnapshotSource:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot

    def read_latest_complete(self):
        return self.snapshot


class EmptyAttempts:
    def read_unresolved(self):
        return ()


class EmptyLiquidation:
    def read_snapshot_by_episode(self, _position_episode_id):
        return None


class PaperReverseDrillPreparerTest(unittest.TestCase):
    def make_context(self):
        episode, protection = live_protection()
        position = strategy_position(episode)
        readiness = readiness_for_protection(
            blocked_readiness(),
            protection=protection,
            observed_at_utc=T2,
        )
        risk = DailyRiskStateV1(
            account_id=ACCOUNT,
            strategy_id=STRATEGY,
            deployment_id=DEPLOYMENT,
            trading_day="2026-07-27",
            status=DailyRiskStatus.MONITORING,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_pnl=0.0,
            target_pnl=500.0,
            pnl_ready=True,
            cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
            updated_at_utc=T2,
        )
        execution = MemoryExecutionRepository(position, readiness, risk)
        decision = MemoryDecisionRepository()
        source = MemoryProtectionSource(episode, protection)
        policy = PaperReverseDrillPolicyV1(
            drill_id="reverse-preparer-test",
            account_id=ACCOUNT,
            environment="paper",
            confirmed_paper_account_id=ACCOUNT,
            strategy_id=STRATEGY,
            strategy_version=1,
            deployment_id=DEPLOYMENT,
            instrument_id=INSTRUMENT,
            policy_hash="f" * 64,
            target_side=DesiredTargetSide.SHORT,
            target_quantity=1,
            command_ttl_seconds=600,
            position_max_age_seconds=30.0,
            active_contract=RegisteredFuturesContractV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                contract_is_active=True,
            ),
        )
        preparer = PaperReverseExecutionDrillPreparer(
            policy=policy,
            decision_repository=decision,
            execution_repository=execution,
            execution_state_source=execution,
            protection_source=source,
            position_snapshot_source=SnapshotSource(position_snapshot()),
            broker_attempt_source=EmptyAttempts(),
            liquidation_source=EmptyLiquidation(),
            contract_registry=(policy.active_contract,),
        )
        return preparer, decision, execution, source, policy

    def test_long_one_prepares_stable_short_reverse_two(self) -> None:
        preparer, decision, execution, _source, _policy = self.make_context()
        first = preparer.prepare(observed_at_utc="2026-07-27T10:00:03Z")
        second = preparer.prepare(observed_at_utc="2026-07-27T10:00:04Z")
        self.assertTrue(first.ready_for_handoff)
        self.assertEqual(first.command.command_kind.value, "REVERSE")
        self.assertEqual(first.command.desired_target_side.value, "SHORT")
        self.assertEqual(first.reverse_order_quantity, 2)
        self.assertEqual(first.source_episode.side.value, "LONG")
        self.assertTrue(second.reused_existing_command)
        self.assertEqual(second.command.command_id, first.command.command_id)
        self.assertEqual(execution.command_state.state.value, "ADMITTED")
        self.assertIsNotNone(decision.evaluation)

    def test_same_side_or_missing_live_stop_is_rejected(self) -> None:
        preparer, _decision, _execution, source, policy = self.make_context()
        same_side = PaperReverseExecutionDrillPreparer(
            policy=replace(policy, target_side=DesiredTargetSide.LONG),
            decision_repository=preparer.decision_repository,
            execution_repository=preparer.execution_repository,
            execution_state_source=preparer.execution_state_source,
            protection_source=source,
            position_snapshot_source=preparer.position_snapshot_source,
            broker_attempt_source=EmptyAttempts(),
            liquidation_source=EmptyLiquidation(),
            contract_registry=(policy.active_contract,),
        )
        with self.assertRaisesRegex(
            PaperReverseDrillPreparationError,
            "not opposite",
        ):
            same_side.prepare(observed_at_utc="2026-07-27T10:00:03Z")
        source.protection = replace(
            source.protection,
            orders=tuple(
                replace(order, state=order.state.PLANNED)
                if order.kind.value == "STOP_LOSS"
                else order
                for order in source.protection.orders
            ),
        )
        with self.assertRaisesRegex(
            PaperReverseDrillPreparationError,
            "STOP is not LIVE",
        ):
            preparer.prepare(observed_at_utc="2026-07-27T10:00:03Z")


class MemoryArtifacts:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self.values = {}

    @property
    def directory(self) -> Path:
        return self._directory

    def write_json(self, name: str, value: object) -> Path:
        self.values[name] = value
        return self._directory / f"{name}.json"


class ScriptedCommand:
    def __init__(
        self,
        step_name: str,
        payload: Mapping[str, Any],
        callback: Callable[[], None] | None = None,
    ) -> None:
        self.step_name = step_name
        self.payload = dict(payload)
        self.callback = callback


class ScriptedExecutor:
    def __init__(self, commands: Sequence[ScriptedCommand]) -> None:
        self.commands = list(commands)
        self.calls = []

    def run_json(self, *, step_name, script, arguments):
        if not self.commands:
            raise AssertionError(f"unexpected command: {step_name}")
        expected = self.commands.pop(0)
        if expected.step_name != step_name:
            raise AssertionError(
                f"expected {expected.step_name!r}, got {step_name!r}"
            )
        self.calls.append((step_name, Path(script).name, tuple(arguments)))
        if expected.callback is not None:
            expected.callback()
        return dict(expected.payload)


class RunnerState:
    def __init__(self) -> None:
        self.validated = False
        self.position = PositionProofV1(
            accepted=True,
            reason="accepted",
            snapshot_id="position_snapshot_reverse_acceptance",
            captured_at_utc="2026-07-27T10:00:10Z",
            source_freshness_seconds=1.0,
            con_id=CON_ID,
            local_symbol=LOCAL_SYMBOL,
            signed_quantity=-1.0,
            competing_contract_count=0,
        )
        self.protection = source_observation()

    def validate_schema(self) -> None:
        self.validated = True

    def read_position_proof(self, **values):
        self.asserted_signed = values["signed_quantity"]
        return self.position

    def read_protection(self, position_episode_id: str):
        if position_episode_id != self.protection.position_episode_id:
            raise AssertionError(position_episode_id)
        return self.protection


def source_observation() -> ProtectionObservationV1:
    return ProtectionObservationV1(
        position_episode_id=SOURCE_EPISODE,
        protection_status="PROTECTED",
        stop_state="LIVE",
        stop_order_ref=STOP_REF,
        stop_broker_order_id=7001,
        take_profit_state="LIVE",
        take_profit_order_ref=TP_REF,
        take_profit_broker_order_id=7002,
        blocking_reason=None,
    )


def target_observation(
    *,
    stop: str,
    take_profit: str,
) -> ProtectionObservationV1:
    status = (
        "PROTECTED"
        if stop == "LIVE" and take_profit == "LIVE"
        else "STOP_LIVE"
        if stop == "LIVE"
        else "PLANNED"
    )
    return ProtectionObservationV1(
        position_episode_id=TARGET_EPISODE,
        protection_status=status,
        stop_state=stop,
        stop_order_ref="IBMD:target:SL",
        stop_broker_order_id=8001 if stop == "LIVE" else None,
        take_profit_state=take_profit,
        take_profit_order_ref="IBMD:target:TP",
        take_profit_broker_order_id=8002 if take_profit == "LIVE" else None,
        blocking_reason=None,
    )


def source_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_name": "PaperAcceptanceResult",
                "schema_version": 1,
                "drill_id": "source-entry-drill",
                "position_episode_id": SOURCE_EPISODE,
                "position_proof": {
                    "accepted": True,
                    "signed_quantity": 1.0,
                },
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


def prepared_reverse() -> dict:
    return {
        "ready_for_handoff": True,
        "reused_existing_command": False,
        "broker_mutations_performed": False,
        "submit_before_utc": "2026-07-27T10:05:00Z",
        "reverse_order_quantity": 2,
        "command": {
            "command_id": COMMAND,
            "command_kind": "REVERSE",
            "desired_target_side": "SHORT",
            "desired_target_quantity": 1,
        },
        "command_state": {"command_id": COMMAND, "state": "ADMITTED"},
        "execution_fixture": {
            "position": {
                "projection_status": "OPEN",
                "position_episode_id": SOURCE_EPISODE,
                "side": "LONG",
                "quantity": 1,
            }
        },
        "source_episode": {"position_episode_id": SOURCE_EPISODE},
        "source_protection": {
            "orders": [
                {
                    "kind": "STOP_LOSS",
                    "state": "LIVE",
                    "order_ref": STOP_REF,
                },
                {
                    "kind": "TAKE_PROFIT",
                    "state": "LIVE",
                    "order_ref": TP_REF,
                },
            ]
        },
        "active_contract": {
            "con_id": CON_ID,
            "local_symbol": LOCAL_SYMBOL,
            "contract_is_active": True,
        },
        "session": {"phase": "TRADING"},
    }


def handoff(*, action: str, mutated: bool, order_ref: str | None = None) -> dict:
    return {
        "command_id": COMMAND,
        "action": action,
        "ready_for_reverse_submit": action == "READY_TO_SUBMIT",
        "broker_mutation_performed": mutated,
        "mutation_error": None,
        "cancel_receipt": (
            None
            if order_ref is None
            else {
                "broker_order_id": 7002 if order_ref == TP_REF else 7001,
                "order_ref": order_ref,
                "cancel_requested_at_utc": "2026-07-27T10:00:02Z",
            }
        ),
        "blocking_reason": None,
    }


def reverse_entry(*, submitted: bool) -> dict:
    return {
        "command_id": COMMAND,
        "operation_id": OPERATION,
        "attempt_id": ATTEMPT,
        "attempt_no": 1,
        "order_ref": ORDER_REF,
        "operation_state": "SUCCEEDED",
        "attempt_state": "FILLED",
        "submission_performed": submitted,
        "filled_qty": 2,
        "remaining_qty": 0,
        "blocking_reason": None,
    }


def finalization(*, created: bool) -> dict:
    return {
        "source_operation_id": OPERATION,
        "source_attempt_id": ATTEMPT,
        "closing_position_episode_id": SOURCE_EPISODE,
        "opening_position_episode_id": TARGET_EPISODE,
        "opening_side": "SHORT",
        "opening_quantity": 1,
        "opening_entry_average_price": 28_600.25,
        "allocations": [
            {
                "exec_id": "reverse-exec-1",
                "close_quantity": 1,
                "open_quantity": 1,
            }
        ],
        "finalization_created": created,
        "commission_completion_refreshed": False,
        "broker_mutations_performed": False,
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


def protective(kind: str, submitted: bool) -> dict:
    return {
        "order_kind": kind,
        "submission_performed": submitted,
        "order_state": "LIVE",
    }


def policy(root: Path) -> PaperAcceptancePolicyV1:
    return PaperAcceptancePolicyV1(
        environment="paper",
        account_id="DU000000",
        deployment_id="paper-drill-reverse-acceptance",
        instrument_id="MNQ",
        drill_id="reverse-acceptance-test",
        target_side="SHORT",
        command_ttl_seconds=600,
        position_max_age_seconds=30.0,
        entry_max_invocations=4,
        entry_poll_seconds=0.0,
        position_wait_seconds=1.0,
        position_poll_seconds=0.0,
        protective_max_invocations=4,
        protective_poll_seconds=0.0,
        reconciliation_read_attempts=5,
        reconciliation_poll_seconds=0.0,
        commission_wait_seconds=0.0,
        submit_client_id_offset=120,
        protective_submit_client_id_offset=140,
        reconciliation_client_id_offset=100,
        paths=PaperAcceptancePathsV1(
            repo_root=root,
            decision_database=root / "decision.sqlite3",
            execution_database=root / "execution.sqlite3",
            position_feed_database=root / "position.sqlite3",
            catalog_root=root / "catalog",
        ),
    )


class PaperReverseAcceptanceRunnerTest(unittest.TestCase):
    def test_full_reverse_closes_one_opens_one_and_reprotects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / "source-summary.json"
            source_summary(summary)
            state = RunnerState()

            def planned_target() -> None:
                state.protection = target_observation(
                    stop="PLANNED",
                    take_profit="PLANNED",
                )

            def stop_live() -> None:
                state.protection = target_observation(
                    stop="LIVE",
                    take_profit="PLANNED",
                )

            def both_live() -> None:
                state.protection = target_observation(
                    stop="LIVE",
                    take_profit="LIVE",
                )

            executor = ScriptedExecutor(
                (
                    ScriptedCommand("reverse-prepare", prepared_reverse()),
                    ScriptedCommand(
                        "reverse-handoff-01",
                        handoff(
                            action="CANCEL_STOP",
                            mutated=True,
                            order_ref=TP_REF,
                        ),
                    ),
                    ScriptedCommand(
                        "reverse-handoff-02",
                        handoff(
                            action="READY_TO_SUBMIT",
                            mutated=True,
                            order_ref=STOP_REF,
                        ),
                    ),
                    ScriptedCommand(
                        "reverse-handoff-idempotency",
                        handoff(action="READY_TO_SUBMIT", mutated=False),
                    ),
                    ScriptedCommand("entry-01", reverse_entry(submitted=True)),
                    ScriptedCommand(
                        "entry-idempotency",
                        reverse_entry(submitted=False),
                    ),
                    ScriptedCommand(
                        "reverse-finalization",
                        finalization(created=True),
                        planned_target,
                    ),
                    ScriptedCommand(
                        "reverse-finalization-idempotency",
                        finalization(created=False),
                    ),
                    ScriptedCommand(
                        "protective-01",
                        protective("STOP_LOSS", True),
                        stop_live,
                    ),
                    ScriptedCommand(
                        "protective-02",
                        protective("TAKE_PROFIT", True),
                        both_live,
                    ),
                    ScriptedCommand(
                        "protective-idempotency",
                        protective("TAKE_PROFIT", False),
                    ),
                )
            )
            artifacts = MemoryArtifacts(root / "artifacts")
            result = PaperReverseAcceptanceRunner(
                policy=policy(root),
                entry_summary=summary,
                command_executor=executor,
                state_source=state,
                artifacts=artifacts,
                handoff_max_invocations=4,
                handoff_poll_seconds=0.0,
                clock=lambda: T0,
                sleeper=lambda _seconds: None,
            ).run()
            payload = result.to_dict()
            self.assertTrue(state.validated)
            self.assertEqual(state.asserted_signed, -1)
            self.assertEqual(payload["reverse_order_quantity"], 2)
            self.assertEqual(
                payload["handoff_cancel_actions"],
                ["TAKE_PROFIT", "STOP_LOSS"],
            )
            self.assertEqual(payload["reverse_submission_count"], 1)
            self.assertEqual(payload["broker_mutation_count"], 5)
            self.assertEqual(payload["source_position_episode_id"], SOURCE_EPISODE)
            self.assertEqual(payload["target_position_episode_id"], TARGET_EPISODE)
            self.assertEqual(payload["allocations"][0]["close_quantity"], 1)
            self.assertEqual(payload["allocations"][0]["open_quantity"], 1)
            self.assertTrue(payload["protection"]["fully_live"])
            self.assertEqual(executor.commands, [])

    def test_duplicate_handoff_cancel_is_critical_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / "source-summary.json"
            source_summary(summary)
            executor = ScriptedExecutor(
                (
                    ScriptedCommand("reverse-prepare", prepared_reverse()),
                    ScriptedCommand(
                        "reverse-handoff-01",
                        handoff(
                            action="CANCEL_STOP",
                            mutated=True,
                            order_ref=TP_REF,
                        ),
                    ),
                    ScriptedCommand(
                        "reverse-handoff-02",
                        handoff(
                            action="CANCEL_STOP",
                            mutated=True,
                            order_ref=TP_REF,
                        ),
                    ),
                )
            )
            with self.assertRaisesRegex(
                PaperAcceptanceError,
                "cancelled TAKE_PROFIT twice",
            ):
                PaperReverseAcceptanceRunner(
                    policy=policy(root),
                    entry_summary=summary,
                    command_executor=executor,
                    state_source=RunnerState(),
                    artifacts=MemoryArtifacts(root / "artifacts"),
                    handoff_max_invocations=2,
                    handoff_poll_seconds=0.0,
                    clock=lambda: T0,
                    sleeper=lambda _seconds: None,
                ).run()


if __name__ == "__main__":
    unittest.main()
