from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"expected exactly one match in {path}: count={count}, anchor={old[:80]!r}"
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def patch_runner() -> None:
    path = ROOT / "src" / "ibmd" / "operations" / "paper_liquidation_restart_acceptance.py"

    replace_once(
        path,
        """    resume_invocation_count: int
    checkpoints: tuple[LiquidationRestartCheckpointV1, ...]
""",
        """    resume_invocation_count: int
    protective_cancel_mode: str
    checkpoints: tuple[LiquidationRestartCheckpointV1, ...]
""",
    )

    replace_once(
        path,
        """            "resume_invocation_count": self.resume_invocation_count,
            "checkpoints": [item.to_dict() for item in self.checkpoints],
""",
        """            "resume_invocation_count": self.resume_invocation_count,
            "protective_cancel_mode": self.protective_cancel_mode,
            "initial_advance_broker_free": True,
            "checkpoints": [item.to_dict() for item in self.checkpoints],
""",
    )

    replace_once(
        path,
        """        return payload

    def run(self) -> PaperLiquidationRestartAcceptanceResultV1:
""",
        """        return payload

    def _initial_advance_arguments(
        self,
        position_episode_id: str,
    ) -> tuple[str, ...]:
        paths = self.policy.paths
        return (
            "--advance-position-episode-id",
            position_episode_id,
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

    def _initial_broker_free_advance(
        self,
        *,
        position_episode_id: str,
        operation_id: str,
    ) -> Mapping[str, Any]:
        payload = self._run_json(
            step_name="liquidation-initial-advance",
            arguments=self._initial_advance_arguments(position_episode_id),
        )
        if payload.get("broker_mutations_performed") is not False:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance unexpectedly performed a broker "
                "mutation",
                stage="liquidation-initial-advance",
            )
        if payload.get("operation_created") is not False:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance unexpectedly created an operation",
                stage="liquidation-initial-advance",
            )
        operation = self._mapping(
            payload.get("liquidation_operation"),
            field_name="liquidation_operation",
            stage="liquidation-initial-advance",
        )
        if operation.get("liquidation_operation_id") != operation_id:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance changed the operation identity",
                stage="liquidation-initial-advance",
            )
        action = self._text(
            operation.get("next_action"),
            field_name="next_action",
            stage="liquidation-initial-advance",
        )
        if action not in {"CANCEL_TAKE_PROFIT", "CANCEL_STOP"}:
            raise PaperLiquidationAcceptanceError(
                "initial liquidation advance did not select a protective cancel: "
                f"{action}",
                stage="liquidation-initial-advance",
            )
        return payload

    def run(self) -> PaperLiquidationRestartAcceptanceResultV1:
""",
    )

    replace_once(
        path,
        """        if request.get("operation_created") is not True:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart acceptance requires a fresh operation",
                stage="liquidation-request",
            )

        checkpoints: list[LiquidationRestartCheckpointV1] = []
""",
        """        if request.get("operation_created") is not True:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart acceptance requires a fresh operation",
                stage="liquidation-request",
            )
        if operation.get("next_action") != "RECONCILE_EXITS":
            raise PaperLiquidationAcceptanceError(
                "fresh liquidation operation did not start at RECONCILE_EXITS",
                stage="liquidation-request",
            )
        self._initial_broker_free_advance(
            position_episode_id=position_episode_id,
            operation_id=operation_id,
        )

        checkpoints: list[LiquidationRestartCheckpointV1] = []
""",
    )

    replace_once(
        path,
        """        expected_actions = {
            "CANCEL_STOP",
            "SUBMIT_MARKET_CLOSE",
        }
        entry_summary = read_json_object(self.policy.paths.entry_summary)
        protection = self._mapping(
            entry_summary.get("protection"),
            field_name="protection",
            stage="entry-summary",
        )
        if protection.get("take_profit_state") == "LIVE":
            expected_actions.add("CANCEL_TAKE_PROFIT")
        actual_actions = {item.action for item in checkpoints}
        if actual_actions != expected_actions:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart checkpoints differ from the expected "
                f"actions: expected={sorted(expected_actions)}, "
                f"actual={sorted(actual_actions)}",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
""",
        """        entry_summary = read_json_object(self.policy.paths.entry_summary)
        protection = self._mapping(
            entry_summary.get("protection"),
            field_name="protection",
            stage="entry-summary",
        )
        actual_actions = {item.action for item in checkpoints}
        if protection.get("take_profit_state") == "LIVE":
            if "CANCEL_TAKE_PROFIT" not in actual_actions:
                raise PaperLiquidationAcceptanceError(
                    "liquidation restart omitted the TAKE PROFIT checkpoint",
                    stage="liquidation-restart",
                    broker_exposure_possible=True,
                )
            if "CANCEL_STOP" in actual_actions:
                expected_actions = {
                    "CANCEL_TAKE_PROFIT",
                    "CANCEL_STOP",
                    "SUBMIT_MARKET_CLOSE",
                }
                protective_cancel_mode = "EXPLICIT_BOTH"
            else:
                expected_actions = {
                    "CANCEL_TAKE_PROFIT",
                    "SUBMIT_MARKET_CLOSE",
                }
                protective_cancel_mode = "OCA_AUTO_CANCELLED_STOP"
        else:
            expected_actions = {
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            }
            protective_cancel_mode = "STOP_ONLY"
        if actual_actions != expected_actions:
            raise PaperLiquidationAcceptanceError(
                "liquidation restart checkpoints differ from the expected "
                f"actions: expected={sorted(expected_actions)}, "
                f"actual={sorted(actual_actions)}",
                stage="liquidation-restart",
                broker_exposure_possible=True,
            )
""",
    )

    replace_once(
        path,
        """            resume_invocation_count=resume_count + 1,
            checkpoints=tuple(checkpoints),
""",
        """            resume_invocation_count=resume_count + 1,
            protective_cancel_mode=protective_cancel_mode,
            checkpoints=tuple(checkpoints),
""",
    )


def patch_tests() -> None:
    path = ROOT / "tester" / "target_paper_liquidation_restart_acceptance_tester.py"

    replace_once(
        path,
        """        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
        },
""",
        """        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
            "next_action": "RECONCILE_EXITS",
        },
""",
    )

    replace_once(
        path,
        """def resume_payload(
""",
        """def advance_payload(*, action: str = "CANCEL_TAKE_PROFIT") -> dict:
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
""",
    )

    replace_once(
        path,
        """                    request_payload(),
                    resume_payload(action="CANCEL_STOP"),
""",
        """                    request_payload(),
                    advance_payload(),
                    resume_payload(action="CANCEL_STOP"),
""",
    )

    replace_once(
        path,
        """            self.assertTrue(payload["all_resume_mutations_false"])
            self.assertTrue(payload["restart_adoption_proven"])
""",
        """            self.assertTrue(payload["all_resume_mutations_false"])
            self.assertTrue(payload["initial_advance_broker_free"])
            self.assertEqual(payload["protective_cancel_mode"], "EXPLICIT_BOTH")
            self.assertTrue(payload["restart_adoption_proven"])
""",
    )

    replace_once(
        path,
        """            self.assertTrue((artifacts.directory / "summary.json").is_file())

    def test_resume_mutation_is_rejected(self) -> None:
""",
        """            self.assertTrue((artifacts.directory / "summary.json").is_file())
            self.assertEqual(executor.calls[1][0], "liquidation-initial-advance")
            self.assertEqual(
                executor.calls[1][2][:2],
                ("--advance-position-episode-id", EPISODE_ID),
            )
            self.assertNotIn(
                "--once-paper-position-episode-id",
                executor.calls[1][2],
            )

    def test_oca_auto_cancelled_stop_skips_second_cancel_checkpoint(self) -> None:
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
            self.assertEqual(payload["broker_mutation_count"], 2)
            self.assertEqual(
                payload["protective_cancel_mode"],
                "OCA_AUTO_CANCELLED_STOP",
            )
            self.assertTrue(payload["state"]["fully_closed"])

    def test_resume_mutation_is_rejected(self) -> None:
""",
    )

    replace_once(
        path,
        """                    request_payload(),
                    resume_payload(
                        action="CANCEL_STOP",
                        mutation=True,
                    ),
""",
        """                    request_payload(),
                    advance_payload(),
                    resume_payload(
                        action="CANCEL_STOP",
                        mutation=True,
                    ),
""",
    )


def patch_entrypoint() -> None:
    path = ROOT / "scripts" / "run_paper_liquidation_restart_acceptance.py"

    replace_once(
        path,
        """            "Run one deterministic paper liquidation restart drill. The TP "
            "cancel, STOP cancel and MARKET-close child processes terminate "
            "immediately after their confirmed broker action; ordinary "
""",
        """            "Run one deterministic paper liquidation restart drill. The TP "
            "cancel, optional explicit STOP cancel and MARKET-close child "
            "processes terminate immediately after their confirmed broker action; "
            "ordinary "
""",
    )

    replace_once(
        path,
        """        "automatic_retry_enabled": False,
        "paper_account_left_flat_after_success": True,
""",
        """        "automatic_retry_enabled": False,
        "oca_auto_cancelled_stop_supported": True,
        "restart_action_sequences": [
            [
                "CANCEL_TAKE_PROFIT",
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            ],
            [
                "CANCEL_TAKE_PROFIT",
                "SUBMIT_MARKET_CLOSE",
            ],
            [
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            ],
        ],
        "paper_account_left_flat_after_success": True,
""",
    )


def patch_runbook() -> None:
    path = ROOT / "docs" / "paper-liquidation-restart-acceptance-runbook.md"

    replace_once(
        path,
        """This drill proves restart adoption across the three broker mutations used to close
one protected position:

```text
cancel TAKE PROFIT
cancel STOP
submit liquidation MARKET close
```
""",
        """This drill proves restart adoption across the broker mutations used to close one
protected position. The explicit path has three mutations; IB may also remove the
STOP as the OCA sibling while reconciling the TAKE PROFIT cancellation:

```text
explicit: cancel TAKE PROFIT -> cancel STOP -> submit liquidation MARKET close
OCA:      cancel TAKE PROFIT -> submit liquidation MARKET close
```
""",
    )

    replace_once(
        path,
        """liquidation request, broker-free
→ cancel TAKE PROFIT and terminate child
→ reconcile TAKE PROFIT cancellation without another cancelOrder
→ cancel STOP and terminate child
→ reconcile STOP cancellation without another cancelOrder
→ submit one MARKET close and terminate child
""",
        """liquidation request, broker-free
→ broker-free advance selects the first protective cancellation
→ cancel TAKE PROFIT and terminate child
→ reconcile TAKE PROFIT cancellation without another cancelOrder
→ if STOP remains LIVE: cancel STOP and terminate child
→ otherwise accept broker-proven OCA sibling cancellation
→ reconcile explicit STOP cancellation without another cancelOrder, when used
→ submit one MARKET close and terminate child
""",
    )

    replace_once(
        path,
        """restart_actions =
  CANCEL_TAKE_PROFIT
  CANCEL_STOP
  SUBMIT_MARKET_CLOSE

intentional_process_terminations = 3
broker_mutation_count             = 3
all_resume_mutations_false        = true
""",
        """restart_actions =
  CANCEL_TAKE_PROFIT
  [CANCEL_STOP when STOP remains LIVE]
  SUBMIT_MARKET_CLOSE

protective_cancel_mode =
  EXPLICIT_BOTH or OCA_AUTO_CANCELLED_STOP or STOP_ONLY

intentional_process_terminations = 2 or 3
broker_mutation_count             = 2 or 3
initial_advance_broker_free       = true
all_resume_mutations_false        = true
""",
    )


def main() -> None:
    patch_runner()
    patch_tests()
    patch_entrypoint()
    patch_runbook()


if __name__ == "__main__":
    main()
