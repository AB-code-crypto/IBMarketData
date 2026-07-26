from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ibmd.foundation.atomic_json import read_json_object
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.operations.paper_acceptance import (
    PaperAcceptanceArtifactSink,
    PaperAcceptanceDrillRunner,
    PaperAcceptanceError,
    PaperAcceptancePolicyV1,
    PaperAcceptanceStateSource,
    PositionProofV1,
    ProtectionObservationV1,
)


@dataclass(frozen=True)
class PaperReverseAcceptanceResultV1:
    drill_id: str
    source_drill_id: str
    started_at_utc: str
    finished_at_utc: str
    source_position_episode_id: str
    target_position_episode_id: str
    command_id: str
    operation_id: str
    attempt_id: str
    order_ref: str
    reverse_order_quantity: int
    handoff_cancel_actions: tuple[str, ...]
    handoff_invocation_count: int
    reverse_submission_count: int
    reverse_invocation_count: int
    position_proof: PositionProofV1
    allocations: tuple[Mapping[str, Any], ...]
    opening_entry_average_price: float
    stop_submission_count: int
    take_profit_submission_count: int
    protection: ProtectionObservationV1
    artifact_directory: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": "PaperReverseAcceptanceResult",
            "schema_version": 1,
            "drill_id": self.drill_id,
            "source_drill_id": self.source_drill_id,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "source_position_episode_id": self.source_position_episode_id,
            "position_episode_id": self.target_position_episode_id,
            "target_position_episode_id": self.target_position_episode_id,
            "command_id": self.command_id,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
            "order_ref": self.order_ref,
            "reverse_order_quantity": self.reverse_order_quantity,
            "handoff_cancel_actions": list(self.handoff_cancel_actions),
            "handoff_invocation_count": self.handoff_invocation_count,
            "reverse_submission_count": self.reverse_submission_count,
            "reverse_invocation_count": self.reverse_invocation_count,
            "position_proof": self.position_proof.to_dict(),
            "allocations": [dict(item) for item in self.allocations],
            "opening_entry_average_price": self.opening_entry_average_price,
            "stop_submission_count": self.stop_submission_count,
            "take_profit_submission_count": self.take_profit_submission_count,
            "protection": self.protection.to_dict(),
            "broker_mutation_count": (
                len(self.handoff_cancel_actions)
                + self.reverse_submission_count
                + self.stop_submission_count
                + self.take_profit_submission_count
            ),
            "automatic_retry_enabled": False,
            "manual_cleanup_required": True,
            "live_position_left_protected": True,
            "legacy_database_compatibility_required": False,
            "artifact_directory": self.artifact_directory,
        }


class PaperReverseAcceptanceRunner(PaperAcceptanceDrillRunner):
    def __init__(
        self,
        *,
        policy: PaperAcceptancePolicyV1,
        entry_summary: str | Path,
        command_executor,
        state_source: PaperAcceptanceStateSource,
        artifacts: PaperAcceptanceArtifactSink,
        handoff_max_invocations: int = 12,
        handoff_poll_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None],
    ) -> None:
        super().__init__(
            policy=policy,
            command_executor=command_executor,
            state_source=state_source,
            artifacts=artifacts,
            clock=clock,
            sleeper=sleeper,
        )
        self.entry_summary = Path(entry_summary).resolve()
        self.handoff_max_invocations = int(handoff_max_invocations)
        self.handoff_poll_seconds = float(handoff_poll_seconds)
        if self.handoff_max_invocations <= 0:
            raise PaperAcceptanceError(
                "handoff_max_invocations must be positive",
                stage="configuration",
            )
        if self.handoff_poll_seconds < 0.0:
            raise PaperAcceptanceError(
                "handoff_poll_seconds must be non-negative",
                stage="configuration",
            )

    def _load_source_summary(self) -> tuple[str, str, Mapping[str, Any]]:
        try:
            value = read_json_object(self.entry_summary)
        except Exception as exc:
            raise PaperAcceptanceError(
                f"cannot read source acceptance summary: {exc}",
                stage="source-summary",
            ) from exc
        if value.get("schema_name") not in {
            "PaperAcceptanceResult",
            "PaperRestartAcceptanceResult",
        } or int(value.get("schema_version") or 0) != 1:
            raise PaperAcceptanceError(
                "source summary is not a protected-position acceptance result",
                stage="source-summary",
            )
        proof = self._mapping(
            value.get("position_proof"),
            field_name="position_proof",
            stage="source-summary",
        )
        protection = self._mapping(
            value.get("protection"),
            field_name="protection",
            stage="source-summary",
        )
        if proof.get("accepted") is not True:
            raise PaperAcceptanceError(
                "source summary does not prove the broker position",
                stage="source-summary",
            )
        if (
            protection.get("fully_live") is not True
            or protection.get("stop_state") != "LIVE"
            or protection.get("take_profit_state")
            not in {"LIVE", "NOT_REQUIRED"}
        ):
            raise PaperAcceptanceError(
                "source summary does not prove live protection",
                stage="source-summary",
            )
        if value.get("live_position_left_protected") is not True:
            raise PaperAcceptanceError(
                "source summary does not declare a live protected position",
                stage="source-summary",
            )
        return (
            self._text(
                value.get("drill_id"),
                field_name="drill_id",
                stage="source-summary",
            ),
            self._text(
                value.get("position_episode_id"),
                field_name="position_episode_id",
                stage="source-summary",
            ),
            value,
        )

    def _prepare_reverse(
        self,
        *,
        source_episode_id: str,
    ) -> tuple[Mapping[str, Any], str, int, str, int, bool]:
        paths = self.policy.paths
        payload = self._run_json(
            step_name="reverse-prepare",
            script_name="prepare_execution_reverse_paper_drill_v2.py",
            arguments=(
                "--prepare",
                "--drill-id",
                self.policy.drill_id,
                "--target-side",
                self.policy.target_side,
                "--confirm-paper-account",
                self.policy.account_id,
                "--command-ttl-seconds",
                str(self.policy.command_ttl_seconds),
                "--position-max-age-seconds",
                str(self.policy.position_max_age_seconds),
                "--decision-database",
                str(paths.decision_database),
                "--execution-database",
                str(paths.execution_database),
                "--position-feed-database",
                str(paths.position_feed_database),
                "--catalog-root",
                str(paths.catalog_root),
                "--instrument",
                self.policy.instrument_id,
            ),
        )
        if payload.get("ready_for_handoff") is not True:
            raise PaperAcceptanceError(
                "paper reverse preparation did not become ready_for_handoff",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperAcceptanceError(
                "paper reverse preparation unexpectedly mutated broker state",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        command = self._mapping(
            payload.get("command"),
            field_name="command",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        state = self._mapping(
            payload.get("command_state"),
            field_name="command_state",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        fixture = self._mapping(
            payload.get("execution_fixture"),
            field_name="execution_fixture",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        position = self._mapping(
            fixture.get("position"),
            field_name="execution_fixture.position",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        source_episode = self._mapping(
            payload.get("source_episode"),
            field_name="source_episode",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        active = self._mapping(
            payload.get("active_contract"),
            field_name="active_contract",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        session = self._mapping(
            payload.get("session"),
            field_name="session",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        if command.get("command_kind") != "REVERSE":
            raise PaperAcceptanceError(
                "paper reverse preparation did not create REVERSE",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if command.get("desired_target_side") != self.policy.target_side:
            raise PaperAcceptanceError(
                "paper reverse target side differs from the acceptance policy",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        target_quantity = int(command.get("desired_target_quantity") or 0)
        if target_quantity != 1:
            raise PaperAcceptanceError(
                "paper reverse acceptance requires target quantity 1",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if (
            state.get("state") != "ADMITTED"
            or state.get("command_id") != command.get("command_id")
        ):
            raise PaperAcceptanceError(
                "paper reverse command is not ADMITTED",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if (
            position.get("projection_status") != "OPEN"
            or position.get("position_episode_id") != source_episode_id
            or source_episode.get("position_episode_id") != source_episode_id
        ):
            raise PaperAcceptanceError(
                "paper reverse preparation refers to another source episode",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if session.get("phase") != "TRADING":
            raise PaperAcceptanceError(
                "paper reverse acceptance requires a TRADING session",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        if active.get("contract_is_active") is not True:
            raise PaperAcceptanceError(
                "paper reverse held contract is not active",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        submit_before = parse_utc(
            self._text(
                payload.get("submit_before_utc"),
                field_name="submit_before_utc",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        )
        if self.clock() >= submit_before:
            raise PaperAcceptanceError(
                "paper reverse position proof expired before handoff",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        reverse_order_quantity = int(
            payload.get("reverse_order_quantity") or 0
        )
        source_quantity = int(position.get("quantity") or 0)
        if reverse_order_quantity != source_quantity + target_quantity:
            raise PaperAcceptanceError(
                "paper reverse order quantity is not source + target quantity",
                stage="reverse-prepare",
                position_may_be_open=True,
            )
        return (
            payload,
            self._text(
                command.get("command_id"),
                field_name="command_id",
                stage="reverse-prepare",
                position_may_be_open=True,
            ),
            int(active.get("con_id") or 0),
            self._text(
                active.get("local_symbol"),
                field_name="local_symbol",
                stage="reverse-prepare",
                position_may_be_open=True,
            ),
            reverse_order_quantity,
            bool(payload.get("reused_existing_command")),
        )

    def _handoff_arguments(self, command_id: str) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--once-command-id",
            command_id,
            "--confirm-paper-account",
            self.policy.account_id,
            "--decision-database",
            str(paths.decision_database),
            "--execution-database",
            str(paths.execution_database),
            "--position-feed-database",
            str(paths.position_feed_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
            "--position-max-age-seconds",
            str(self.policy.position_max_age_seconds),
            "--cancel-client-id-offset",
            "180",
            "--reconciliation-client-id-offset",
            str(self.policy.reconciliation_client_id_offset),
            "--commission-wait-seconds",
            str(self.policy.commission_wait_seconds),
        )

    def _complete_handoff(
        self,
        *,
        command_id: str,
        source_protection: Mapping[str, Any],
    ) -> tuple[tuple[str, ...], int]:
        orders = source_protection.get("orders")
        if not isinstance(orders, list):
            raise PaperAcceptanceError(
                "source protection orders must be a list",
                stage="reverse-handoff",
                position_may_be_open=True,
            )
        by_ref = {
            str(item.get("order_ref")): str(item.get("kind"))
            for item in orders
            if isinstance(item, Mapping)
        }
        expected = []
        for kind in ("TAKE_PROFIT", "STOP_LOSS"):
            matching = [
                item
                for item in orders
                if isinstance(item, Mapping)
                and item.get("kind") == kind
                and item.get("state") == "LIVE"
            ]
            if matching:
                expected.append(kind)
        actions: list[str] = []
        invocations = 0
        ready = False
        for index in range(1, self.handoff_max_invocations + 1):
            payload = self._run_json(
                step_name=f"reverse-handoff-{index:02d}",
                script_name="run_execution_reverse_handoff_v2.py",
                arguments=self._handoff_arguments(command_id),
            )
            invocations += 1
            if payload.get("command_id") != command_id:
                raise PaperAcceptanceError(
                    "reverse handoff response belongs to another command",
                    stage="reverse-handoff",
                    position_may_be_open=True,
                )
            if payload.get("mutation_error"):
                raise PaperAcceptanceError(
                    "reverse handoff mutation/reconciliation failed: "
                    f"{payload.get('mutation_error')}",
                    stage="reverse-handoff",
                    position_may_be_open=True,
                )
            if payload.get("broker_mutation_performed") is True:
                receipt = self._mapping(
                    payload.get("cancel_receipt"),
                    field_name="cancel_receipt",
                    stage="reverse-handoff",
                    position_may_be_open=True,
                )
                order_ref = self._text(
                    receipt.get("order_ref"),
                    field_name="cancel_receipt.order_ref",
                    stage="reverse-handoff",
                    position_may_be_open=True,
                )
                kind = by_ref.get(order_ref)
                if kind not in {"TAKE_PROFIT", "STOP_LOSS"}:
                    raise PaperAcceptanceError(
                        "reverse handoff cancelled an unknown protective order",
                        stage="reverse-handoff",
                        position_may_be_open=True,
                    )
                if kind in actions:
                    raise PaperAcceptanceError(
                        f"CRITICAL: reverse handoff cancelled {kind} twice",
                        stage="reverse-handoff",
                        position_may_be_open=True,
                    )
                actions.append(kind)
            if payload.get("ready_for_reverse_submit") is True:
                ready = True
                break
            if payload.get("action") == "OPERATOR_REQUIRED":
                raise PaperAcceptanceError(
                    "reverse handoff requires operator intervention: "
                    f"{payload.get('blocking_reason')}",
                    stage="reverse-handoff",
                    position_may_be_open=True,
                )
            if index < self.handoff_max_invocations and self.handoff_poll_seconds:
                self.sleeper(self.handoff_poll_seconds)
        if not ready:
            raise PaperAcceptanceError(
                "reverse handoff did not become ready within bounded invocations",
                stage="reverse-handoff",
                position_may_be_open=True,
            )
        if actions != expected:
            raise PaperAcceptanceError(
                "reverse handoff cancellation order differs from STOP-first safety "
                f"contract: expected={expected}, actual={actions}",
                stage="reverse-handoff",
                position_may_be_open=True,
            )
        repeat = self._run_json(
            step_name="reverse-handoff-idempotency",
            script_name="run_execution_reverse_handoff_v2.py",
            arguments=self._handoff_arguments(command_id),
        )
        invocations += 1
        if (
            repeat.get("broker_mutation_performed") is not False
            or repeat.get("ready_for_reverse_submit") is not True
        ):
            raise PaperAcceptanceError(
                "reverse handoff idempotency proof failed",
                stage="reverse-handoff-idempotency",
                position_may_be_open=True,
            )
        return tuple(actions), invocations

    def _finalize_reverse(
        self,
        *,
        operation_id: str,
        source_episode_id: str,
        target_quantity: int,
    ) -> tuple[str, tuple[Mapping[str, Any], ...], float]:
        paths = self.policy.paths
        arguments = (
            "--finalize-operation-id",
            operation_id,
            "--execution-database",
            str(paths.execution_database),
            "--position-feed-database",
            str(paths.position_feed_database),
            "--catalog-root",
            str(paths.catalog_root),
            "--instrument",
            self.policy.instrument_id,
            "--position-max-age-seconds",
            str(self.policy.position_max_age_seconds),
        )
        payload = self._run_json(
            step_name="reverse-finalization",
            script_name="run_execution_reverse_finalization_v2.py",
            arguments=arguments,
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperAcceptanceError(
                "reverse finalization unexpectedly mutated broker state",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        if payload.get("finalization_created") is not True:
            raise PaperAcceptanceError(
                "fresh reverse finalization was not created",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        if (
            payload.get("source_operation_id") != operation_id
            or payload.get("closing_position_episode_id") != source_episode_id
            or payload.get("opening_side") != self.policy.target_side
            or int(payload.get("opening_quantity") or 0) != target_quantity
        ):
            raise PaperAcceptanceError(
                "reverse finalization scope differs from the filled reverse",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        new_episode_id = self._text(
            payload.get("opening_position_episode_id"),
            field_name="opening_position_episode_id",
            stage="reverse-finalization",
            position_may_be_open=True,
        )
        raw_allocations = payload.get("allocations")
        if not isinstance(raw_allocations, list) or not raw_allocations:
            raise PaperAcceptanceError(
                "reverse finalization has no fill allocations",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        allocations = tuple(
            self._mapping(
                item,
                field_name="allocation",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
            for item in raw_allocations
        )
        close_total = sum(
            int(item.get("close_quantity") or 0) for item in allocations
        )
        open_total = sum(
            int(item.get("open_quantity") or 0) for item in allocations
        )
        if close_total <= 0 or open_total != target_quantity:
            raise PaperAcceptanceError(
                "reverse fill allocation does not close/open the expected quantities",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        price = float(payload.get("opening_entry_average_price") or 0.0)
        if price <= 0.0:
            raise PaperAcceptanceError(
                "reverse opening entry price is invalid",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        protection = self._mapping(
            payload.get("protection"),
            field_name="protection",
            stage="reverse-finalization",
            position_may_be_open=True,
        )
        orders = protection.get("orders")
        if not isinstance(orders, list):
            raise PaperAcceptanceError(
                "new reverse protection orders are missing",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        states = {
            str(item.get("kind")): str(item.get("state"))
            for item in orders
            if isinstance(item, Mapping)
        }
        if states.get("STOP_LOSS") != "PLANNED" or states.get(
            "TAKE_PROFIT"
        ) not in {"PLANNED", "NOT_REQUIRED"}:
            raise PaperAcceptanceError(
                "reverse finalization did not create a STOP-first protection plan",
                stage="reverse-finalization",
                position_may_be_open=True,
            )
        repeated = self._run_json(
            step_name="reverse-finalization-idempotency",
            script_name="run_execution_reverse_finalization_v2.py",
            arguments=arguments,
        )
        if (
            repeated.get("finalization_created") is not False
            or repeated.get("opening_position_episode_id") != new_episode_id
            or repeated.get("source_operation_id") != operation_id
        ):
            raise PaperAcceptanceError(
                "reverse finalization idempotency proof failed",
                stage="reverse-finalization-idempotency",
                position_may_be_open=True,
            )
        return new_episode_id, allocations, price

    def run(self) -> PaperReverseAcceptanceResultV1:
        started = format_utc(self.clock())
        self.state_source.validate_schema()
        self.artifacts.write_json("configuration", self.policy.to_dict())
        source_drill_id, source_episode_id, source_summary = (
            self._load_source_summary()
        )
        (
            prepared,
            command_id,
            con_id,
            local_symbol,
            reverse_order_quantity,
            resumed,
        ) = self._prepare_reverse(source_episode_id=source_episode_id)
        source_protection = self._mapping(
            prepared.get("source_protection"),
            field_name="source_protection",
            stage="reverse-prepare",
            position_may_be_open=True,
        )
        cancel_actions, handoff_invocations = self._complete_handoff(
            command_id=command_id,
            source_protection=source_protection,
        )
        identity, reverse_submissions, reverse_invocations = (
            self._complete_entry(
                command_id=command_id,
                quantity=reverse_order_quantity,
                resumed_existing_command=resumed,
            )
        )
        target_quantity = 1
        position_proof = self._wait_for_position(
            con_id=con_id,
            local_symbol=local_symbol,
            quantity=target_quantity,
        )
        new_episode_id, allocations, entry_price = self._finalize_reverse(
            operation_id=identity[1],
            source_episode_id=source_episode_id,
            target_quantity=target_quantity,
        )
        stop_count, tp_count, protection = self._complete_protection(
            position_episode_id=new_episode_id,
            resumed_existing_command=False,
        )
        result = PaperReverseAcceptanceResultV1(
            drill_id=self.policy.drill_id,
            source_drill_id=source_drill_id,
            started_at_utc=started,
            finished_at_utc=format_utc(self.clock()),
            source_position_episode_id=source_episode_id,
            target_position_episode_id=new_episode_id,
            command_id=identity[0],
            operation_id=identity[1],
            attempt_id=identity[2],
            order_ref=identity[4],
            reverse_order_quantity=reverse_order_quantity,
            handoff_cancel_actions=cancel_actions,
            handoff_invocation_count=handoff_invocations,
            reverse_submission_count=reverse_submissions,
            reverse_invocation_count=reverse_invocations,
            position_proof=position_proof,
            allocations=allocations,
            opening_entry_average_price=entry_price,
            stop_submission_count=stop_count,
            take_profit_submission_count=tp_count,
            protection=protection,
            artifact_directory=str(self.artifacts.directory),
        )
        self.artifacts.write_json("source-summary", source_summary)
        self.artifacts.write_json("summary", result.to_dict())
        return result
