from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.operations.paper_acceptance import (
    PaperAcceptanceDrillRunner,
    PaperAcceptanceError,
    PaperAcceptancePathsV1,
    PaperAcceptancePolicyV1,
    PositionProofV1,
    ProtectionObservationV1,
)

T0 = datetime(2026, 7, 27, 10, 0, 0, tzinfo=timezone.utc)
COMMAND = "strategy_command_acceptance"
OPERATION = "broker_operation_acceptance"
ATTEMPT = "broker_attempt_acceptance"
ORDER_REF = "IBMD:broker_operation_acceptance:1"
EPISODE = "position_episode_acceptance"


class MemoryArtifacts:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self.values: dict[str, object] = {}

    @property
    def directory(self) -> Path:
        return self._directory

    def write_json(self, name: str, value: object) -> Path:
        self.values[name] = value
        return self._directory / f"{name}.json"


@dataclass
class ScriptedCommand:
    step_name: str
    payload: Mapping[str, Any]
    callback: Callable[[], None] | None = None


class ScriptedExecutor:
    def __init__(self, commands: Sequence[ScriptedCommand]) -> None:
        self.commands = list(commands)
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []

    def run_json(self, *, step_name, script, arguments):
        if not self.commands:
            raise AssertionError(f"unexpected command: {step_name}")
        expected = self.commands.pop(0)
        if step_name != expected.step_name:
            raise AssertionError(
                f"expected step {expected.step_name!r}, got {step_name!r}"
            )
        self.calls.append((step_name, Path(script).name, tuple(arguments)))
        if expected.callback is not None:
            expected.callback()
        return dict(expected.payload)


class MemoryState:
    def __init__(self) -> None:
        self.validated = False
        self.position = PositionProofV1(
            accepted=True,
            reason="accepted",
            snapshot_id="position_snapshot_acceptance",
            captured_at_utc="2026-07-27T10:00:01Z",
            source_freshness_seconds=1.0,
            con_id=793356225,
            local_symbol="MNQU6",
            signed_quantity=1.0,
            competing_contract_count=0,
        )
        self.protection = observation(stop="PLANNED", take_profit="PLANNED")

    def validate_schema(self) -> None:
        self.validated = True

    def read_position_proof(self, **_values) -> PositionProofV1:
        return self.position

    def read_protection(self, position_episode_id: str) -> ProtectionObservationV1:
        if position_episode_id != EPISODE:
            raise AssertionError(position_episode_id)
        return self.protection


def observation(
    *,
    stop: str,
    take_profit: str | None,
    status: str | None = None,
) -> ProtectionObservationV1:
    if status is None:
        if stop == "LIVE" and take_profit == "LIVE":
            status = "PROTECTED"
        elif stop == "LIVE":
            status = "STOP_LIVE"
        elif stop == "UNKNOWN_OUTCOME":
            status = "UNPROTECTED"
        else:
            status = "PLANNED"
    return ProtectionObservationV1(
        position_episode_id=EPISODE,
        protection_status=status,
        stop_state=stop,
        stop_order_ref="IBMD:STOP:acceptance",
        stop_broker_order_id=1001 if stop == "LIVE" else None,
        take_profit_state=take_profit,
        take_profit_order_ref="IBMD:TP:acceptance",
        take_profit_broker_order_id=(
            1002 if take_profit == "LIVE" else None
        ),
        blocking_reason=None,
    )


def prepared(*, reused: bool = False) -> dict:
    return {
        "ready_for_submit": True,
        "reused_existing_command": reused,
        "broker_mutations_performed": False,
        "submit_before_utc": "2026-07-27T10:05:00Z",
        "command": {
            "command_id": COMMAND,
            "command_kind": "OPEN",
            "desired_target_side": "LONG",
            "desired_target_quantity": 1,
        },
        "command_state": {
            "command_id": COMMAND,
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


def entry(
    *,
    operation_state: str,
    attempt_state: str,
    submitted: bool,
    filled: int,
    remaining: int,
) -> dict:
    return {
        "command_id": COMMAND,
        "operation_id": OPERATION,
        "attempt_id": ATTEMPT,
        "attempt_no": 1,
        "order_ref": ORDER_REF,
        "operation_state": operation_state,
        "attempt_state": attempt_state,
        "submission_performed": submitted,
        "filled_qty": filled,
        "remaining_qty": remaining,
        "blocking_reason": None,
    }


def plan() -> dict:
    return {
        "broker_mutations_performed": False,
        "position_episode": {
            "position_episode_id": EPISODE,
            "status": "OPEN",
            "source_operation_id": OPERATION,
            "side": "LONG",
            "quantity": 1,
            "con_id": 793356225,
            "local_symbol": "MNQU6",
        },
        "strategy_position": {
            "position_episode_id": EPISODE,
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


def protective(*, kind: str, submitted: bool, state: str) -> dict:
    return {
        "order_kind": kind,
        "submission_performed": submitted,
        "order_state": state,
    }


def policy(root: Path, *, entry_max: int = 8) -> PaperAcceptancePolicyV1:
    return PaperAcceptancePolicyV1(
        environment="paper",
        account_id="DU000000",
        deployment_id="paper-drill-acceptance",
        instrument_id="MNQ",
        drill_id="acceptance-test",
        target_side="LONG",
        command_ttl_seconds=600,
        position_max_age_seconds=30.0,
        entry_max_invocations=entry_max,
        entry_poll_seconds=0.0,
        position_wait_seconds=1.0,
        position_poll_seconds=0.0,
        protective_max_invocations=6,
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


class PaperAcceptanceRunnerTest(unittest.TestCase):
    def test_full_success_has_exactly_three_mutations_and_no_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = MemoryState()

            def stop_live() -> None:
                state.protection = observation(
                    stop="LIVE",
                    take_profit="PLANNED",
                )

            def both_live() -> None:
                state.protection = observation(
                    stop="LIVE",
                    take_profit="LIVE",
                )

            executor = ScriptedExecutor(
                (
                    ScriptedCommand("prepare", prepared()),
                    ScriptedCommand(
                        "entry-01",
                        entry(
                            operation_state="SUCCEEDED",
                            attempt_state="FILLED",
                            submitted=True,
                            filled=1,
                            remaining=0,
                        ),
                    ),
                    ScriptedCommand(
                        "entry-idempotency",
                        entry(
                            operation_state="SUCCEEDED",
                            attempt_state="FILLED",
                            submitted=False,
                            filled=1,
                            remaining=0,
                        ),
                    ),
                    ScriptedCommand("protection-plan", plan()),
                    ScriptedCommand(
                        "protective-01",
                        protective(
                            kind="STOP_LOSS",
                            submitted=True,
                            state="LIVE",
                        ),
                        stop_live,
                    ),
                    ScriptedCommand(
                        "protective-02",
                        protective(
                            kind="TAKE_PROFIT",
                            submitted=True,
                            state="LIVE",
                        ),
                        both_live,
                    ),
                    ScriptedCommand(
                        "protective-idempotency",
                        protective(
                            kind="TAKE_PROFIT",
                            submitted=False,
                            state="LIVE",
                        ),
                    ),
                )
            )
            artifacts = MemoryArtifacts(root / "artifacts")
            result = PaperAcceptanceDrillRunner(
                policy=policy(root),
                command_executor=executor,
                state_source=state,
                artifacts=artifacts,
                clock=lambda: T0,
                sleeper=lambda _seconds: None,
            ).run()
            self.assertTrue(state.validated)
            self.assertEqual(result.entry_submission_count, 1)
            self.assertEqual(result.stop_submission_count, 1)
            self.assertEqual(result.take_profit_submission_count, 1)
            self.assertTrue(result.protection.fully_live)
            self.assertEqual(result.to_dict()["broker_mutation_count"], 3)
            self.assertEqual(executor.commands, [])
            flattened = " ".join(
                " ".join(call[2]) for call in executor.calls
            )
            self.assertNotIn("Read-Host", flattened)

    def test_entry_unknown_outcome_never_retries_place_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executor = ScriptedExecutor(
                (
                    ScriptedCommand("prepare", prepared()),
                    ScriptedCommand(
                        "entry-01",
                        entry(
                            operation_state="UNKNOWN_OUTCOME",
                            attempt_state="UNKNOWN_OUTCOME",
                            submitted=True,
                            filled=0,
                            remaining=1,
                        ),
                    ),
                    ScriptedCommand(
                        "entry-02",
                        entry(
                            operation_state="UNKNOWN_OUTCOME",
                            attempt_state="UNKNOWN_OUTCOME",
                            submitted=False,
                            filled=0,
                            remaining=1,
                        ),
                    ),
                )
            )
            with self.assertRaisesRegex(
                PaperAcceptanceError,
                "remained unproven",
            ) as context:
                PaperAcceptanceDrillRunner(
                    policy=policy(root, entry_max=2),
                    command_executor=executor,
                    state_source=MemoryState(),
                    artifacts=MemoryArtifacts(root / "artifacts"),
                    clock=lambda: T0,
                    sleeper=lambda _seconds: None,
                ).run()
            self.assertTrue(context.exception.position_may_be_open)
            self.assertEqual(
                [call[0] for call in executor.calls],
                ["prepare", "entry-01", "entry-02"],
            )
            self.assertEqual(executor.commands, [])

    def test_duplicate_entry_submission_is_critical_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executor = ScriptedExecutor(
                (
                    ScriptedCommand("prepare", prepared()),
                    ScriptedCommand(
                        "entry-01",
                        entry(
                            operation_state="LIVE",
                            attempt_state="LIVE",
                            submitted=True,
                            filled=0,
                            remaining=1,
                        ),
                    ),
                    ScriptedCommand(
                        "entry-02",
                        entry(
                            operation_state="SUCCEEDED",
                            attempt_state="FILLED",
                            submitted=True,
                            filled=1,
                            remaining=0,
                        ),
                    ),
                )
            )
            with self.assertRaisesRegex(PaperAcceptanceError, "more than once"):
                PaperAcceptanceDrillRunner(
                    policy=policy(root),
                    command_executor=executor,
                    state_source=MemoryState(),
                    artifacts=MemoryArtifacts(root / "artifacts"),
                    clock=lambda: T0,
                    sleeper=lambda _seconds: None,
                ).run()

    def test_unknown_stop_stops_before_take_profit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = MemoryState()

            def stop_unknown() -> None:
                state.protection = observation(
                    stop="UNKNOWN_OUTCOME",
                    take_profit="PLANNED",
                )

            executor = ScriptedExecutor(
                (
                    ScriptedCommand("prepare", prepared()),
                    ScriptedCommand(
                        "entry-01",
                        entry(
                            operation_state="SUCCEEDED",
                            attempt_state="FILLED",
                            submitted=True,
                            filled=1,
                            remaining=0,
                        ),
                    ),
                    ScriptedCommand(
                        "entry-idempotency",
                        entry(
                            operation_state="SUCCEEDED",
                            attempt_state="FILLED",
                            submitted=False,
                            filled=1,
                            remaining=0,
                        ),
                    ),
                    ScriptedCommand("protection-plan", plan()),
                    ScriptedCommand(
                        "protective-01",
                        protective(
                            kind="STOP_LOSS",
                            submitted=True,
                            state="UNKNOWN_OUTCOME",
                        ),
                        stop_unknown,
                    ),
                )
            )
            with self.assertRaisesRegex(PaperAcceptanceError, "unsafe state"):
                PaperAcceptanceDrillRunner(
                    policy=policy(root),
                    command_executor=executor,
                    state_source=state,
                    artifacts=MemoryArtifacts(root / "artifacts"),
                    clock=lambda: T0,
                    sleeper=lambda _seconds: None,
                ).run()
            self.assertEqual(executor.commands, [])


if __name__ == "__main__":
    unittest.main()
