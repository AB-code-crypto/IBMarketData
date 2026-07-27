from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/operations/paper_liquidation_acceptance.py",
    '''        request = self._run_json(
            step_name="liquidation-request",
            arguments=self._request_arguments(
                position_episode_id=position_episode_id,
                source_drill_id=source_drill_id,
            ),
        )
        if request.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "liquidation request unexpectedly performed broker mutation",
                stage="liquidation-request",
            )
        operation = self._mapping(
            request.get("liquidation_operation"),
            field_name="liquidation_operation",
            stage="liquidation-request",
        )
        operation_id = self._text(
            operation.get("liquidation_operation_id"),
            field_name="liquidation_operation_id",
            stage="liquidation-request",
        )
        resumed = request.get("operation_created") is False
        counts = {
            "CANCEL_TAKE_PROFIT": 0,
            "CANCEL_STOP": 0,
            "SUBMIT_MARKET_CLOSE": 0,
        }
        invocation_count = 0
        state = self._state(position_episode_id)
        attempt_id = state.liquidation_attempt_id
        order_ref = state.order_ref
        recovered_from_durable_state = bool(resumed and state.fully_closed)
        self.artifacts.write_json("liquidation-state-00", state.to_dict())
''',
    '''        initial_state: LiquidationStateObservationV1 | None = None
        try:
            initial_state = self._state(position_episode_id)
        except PaperLiquidationAcceptanceError as exc:
            if (
                exc.stage != "state-proof"
                or "liquidation acceptance state is incomplete" not in str(exc)
            ):
                raise

        counts = {
            "CANCEL_TAKE_PROFIT": 0,
            "CANCEL_STOP": 0,
            "SUBMIT_MARKET_CLOSE": 0,
        }
        invocation_count = 0
        if initial_state is not None and initial_state.fully_closed:
            operation_id = initial_state.liquidation_operation_id
            resumed = True
            state = initial_state
            attempt_id = state.liquidation_attempt_id
            order_ref = state.order_ref
            recovered_from_durable_state = True
        else:
            request = self._run_json(
                step_name="liquidation-request",
                arguments=self._request_arguments(
                    position_episode_id=position_episode_id,
                    source_drill_id=source_drill_id,
                ),
            )
            if request.get("broker_mutations_performed") is not False:
                raise PaperLiquidationAcceptanceError(
                    "liquidation request unexpectedly performed broker mutation",
                    stage="liquidation-request",
                )
            operation = self._mapping(
                request.get("liquidation_operation"),
                field_name="liquidation_operation",
                stage="liquidation-request",
            )
            operation_id = self._text(
                operation.get("liquidation_operation_id"),
                field_name="liquidation_operation_id",
                stage="liquidation-request",
            )
            resumed = request.get("operation_created") is False
            state = initial_state or self._state(position_episode_id)
            attempt_id = state.liquidation_attempt_id
            order_ref = state.order_ref
            recovered_from_durable_state = bool(resumed and state.fully_closed)
        self.artifacts.write_json("liquidation-state-00", state.to_dict())
''',
)

replace_once(
    "tester/target_paper_liquidation_acceptance_tester.py",
    '''            executor = FakeExecutor(
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
''',
    '''            executor = FakeExecutor(
                [
                    paper_payload(
                        action="NONE",
                        mutation=False,
                        operation_state="SUCCEEDED",
                        attempt_state="FILLED",
                        episode_closed=True,
                    ),
                ]
            )
''',
)

replace_once(
    "tester/target_paper_liquidation_acceptance_tester.py",
    '''            self.assertEqual(executor.calls[-1][0], "liquidation-idempotency")
            self.assertTrue((artifacts.directory / "summary.json").is_file())
''',
    '''            self.assertEqual(
                [item[0] for item in executor.calls],
                ["liquidation-idempotency"],
            )
            self.assertTrue((artifacts.directory / "summary.json").is_file())
''',
)
