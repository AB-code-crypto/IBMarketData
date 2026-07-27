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
    "src/ibmd/operations/paper_liquidation_restart_acceptance.py",
    '''    checkpoints: tuple[LiquidationRestartCheckpointV1, ...]\n    state: LiquidationStateObservationV1\n''',
    '''    checkpoints: tuple[LiquidationRestartCheckpointV1, ...]\n    protective_cancel_mode: str\n    state: LiquidationStateObservationV1\n''',
)
replace_once(
    "src/ibmd/operations/paper_liquidation_restart_acceptance.py",
    '''            "restart_actions": actions,\n            "all_resume_mutations_false": True,\n''',
    '''            "restart_actions": actions,\n            "protective_cancel_mode": self.protective_cancel_mode,\n            "all_resume_mutations_false": True,\n''',
)
replace_once(
    "src/ibmd/operations/paper_liquidation_restart_acceptance.py",
    '''        expected_actions = {\n            "CANCEL_STOP",\n            "SUBMIT_MARKET_CLOSE",\n        }\n        entry_summary = read_json_object(self.policy.paths.entry_summary)\n        protection = self._mapping(\n            entry_summary.get("protection"),\n            field_name="protection",\n            stage="entry-summary",\n        )\n        if protection.get("take_profit_state") == "LIVE":\n            expected_actions.add("CANCEL_TAKE_PROFIT")\n        actual_actions = {item.action for item in checkpoints}\n        if actual_actions != expected_actions:\n            raise PaperLiquidationAcceptanceError(\n                "liquidation restart checkpoints differ from the expected "\n                f"actions: expected={sorted(expected_actions)}, "\n                f"actual={sorted(actual_actions)}",\n                stage="liquidation-restart",\n                broker_exposure_possible=True,\n            )\n''',
    '''        entry_summary = read_json_object(self.policy.paths.entry_summary)\n        protection = self._mapping(\n            entry_summary.get("protection"),\n            field_name="protection",\n            stage="entry-summary",\n        )\n        take_profit_live = protection.get("take_profit_state") == "LIVE"\n        actual_actions = [item.action for item in checkpoints]\n        if take_profit_live:\n            explicit_actions = [\n                "CANCEL_TAKE_PROFIT",\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ]\n            oca_actions = [\n                "CANCEL_TAKE_PROFIT",\n                "SUBMIT_MARKET_CLOSE",\n            ]\n            if actual_actions == explicit_actions:\n                protective_cancel_mode = "EXPLICIT_BOTH"\n            elif actual_actions == oca_actions:\n                protective_cancel_mode = "OCA_AUTO_CANCELLED_STOP"\n            else:\n                raise PaperLiquidationAcceptanceError(\n                    "liquidation restart checkpoints do not prove an explicit "\n                    "or OCA protective cancellation path: "\n                    f"actual={actual_actions}",\n                    stage="liquidation-restart",\n                    broker_exposure_possible=True,\n                )\n        else:\n            expected_actions = [\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ]\n            if actual_actions != expected_actions:\n                raise PaperLiquidationAcceptanceError(\n                    "STOP-only liquidation restart checkpoints differ from the "\n                    f"expected actions: actual={actual_actions}",\n                    stage="liquidation-restart",\n                    broker_exposure_possible=True,\n                )\n            protective_cancel_mode = "STOP_ONLY"\n''',
)
replace_once(
    "src/ibmd/operations/paper_liquidation_restart_acceptance.py",
    '''            checkpoints=tuple(checkpoints),\n            state=repeated,\n''',
    '''            checkpoints=tuple(checkpoints),\n            protective_cancel_mode=protective_cancel_mode,\n            state=repeated,\n''',
)
replace_once(
    "src/ibmd/operations/acceptance_manifest.py",
    '''    actions = value.get("restart_actions")\n    if not isinstance(actions, list) or actions != [\n        "CANCEL_TAKE_PROFIT",\n        "CANCEL_STOP",\n        "SUBMIT_MARKET_CLOSE",\n    ]:\n        raise TargetAcceptanceError(\n            "liquidation restart actions must prove TP cancel, STOP cancel and close"\n        )\n    facts.update(\n        {\n            "restart_adoption_proven": True,\n            "attempt_no": 1,\n            "restart_actions": list(actions),\n        }\n    )\n''',
    '''    actions = value.get("restart_actions")\n    if not isinstance(actions, list):\n        raise TargetAcceptanceError(\n            "liquidation restart actions must be a list"\n        )\n    mode = _required_text(\n        value.get("protective_cancel_mode"),\n        field_name="protective_cancel_mode",\n    )\n    expected_by_mode = {\n        "EXPLICIT_BOTH": [\n            "CANCEL_TAKE_PROFIT",\n            "CANCEL_STOP",\n            "SUBMIT_MARKET_CLOSE",\n        ],\n        "OCA_AUTO_CANCELLED_STOP": [\n            "CANCEL_TAKE_PROFIT",\n            "SUBMIT_MARKET_CLOSE",\n        ],\n        "STOP_ONLY": [\n            "CANCEL_STOP",\n            "SUBMIT_MARKET_CLOSE",\n        ],\n    }\n    expected_actions = expected_by_mode.get(mode)\n    if expected_actions is None or actions != expected_actions:\n        raise TargetAcceptanceError(\n            "liquidation restart actions differ from protective_cancel_mode"\n        )\n    terminations = _positive_int(\n        value.get("intentional_process_terminations"),\n        field_name="intentional_process_terminations",\n    )\n    mutations = _positive_int(\n        value.get("broker_mutation_count"),\n        field_name="broker_mutation_count",\n    )\n    if terminations != len(actions) or mutations != len(actions):\n        raise TargetAcceptanceError(\n            "liquidation restart mutation counts must equal restart actions"\n        )\n    facts.update(\n        {\n            "restart_adoption_proven": True,\n            "attempt_no": 1,\n            "restart_actions": list(actions),\n            "protective_cancel_mode": mode,\n            "intentional_process_terminations": terminations,\n        }\n    )\n''',
)
replace_once(
    "scripts/run_paper_liquidation_restart_acceptance.py",
    '''        "restart_actions": [\n            "CANCEL_TAKE_PROFIT",\n            "CANCEL_STOP",\n            "SUBMIT_MARKET_CLOSE",\n        ],\n        "interactive_confirmation_required": False,\n''',
    '''        "restart_actions": [\n            "CANCEL_TAKE_PROFIT",\n            "CANCEL_STOP",\n            "SUBMIT_MARKET_CLOSE",\n        ],\n        "oca_sibling_auto_cancel_supported": True,\n        "restart_action_variants": [\n            [\n                "CANCEL_TAKE_PROFIT",\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ],\n            [\n                "CANCEL_TAKE_PROFIT",\n                "SUBMIT_MARKET_CLOSE",\n            ],\n            [\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ],\n        ],\n        "interactive_confirmation_required": False,\n''',
)
replace_once(
    "tester/target_paper_liquidation_restart_acceptance_tester.py",
    '''            self.assertEqual(payload["broker_mutation_count"], 3)\n            self.assertTrue(payload["all_resume_mutations_false"])\n''',
    '''            self.assertEqual(payload["broker_mutation_count"], 3)\n            self.assertEqual(payload["protective_cancel_mode"], "EXPLICIT_BOTH")\n            self.assertTrue(payload["all_resume_mutations_false"])\n''',
)
insert_marker = '''    def test_resume_mutation_is_rejected(self) -> None:\n'''
oca_test = '''    def test_oca_auto_cancelled_stop_is_adopted_without_repeat_mutation(self) -> None:\n        with tempfile.TemporaryDirectory() as temporary:\n            root = Path(temporary)\n            summary = root / "entry-summary.json"\n            write_entry_summary(\n                summary,\n                schema_name="PaperRestartAcceptanceResult",\n            )\n            source = FakeStateSource(\n                [\n                    state(),\n                    state(next_action="RECONCILE_EXITS"),\n                    state(\n                        operation_state="PREPARING",\n                        next_action="SUBMIT_MARKET_CLOSE",\n                        exposed=0,\n                    ),\n                    state(\n                        operation_state="SUBMITTING",\n                        next_action="RECONCILE_MARKET_CLOSE",\n                        attempt_state="SUBMITTING",\n                        exposed=0,\n                    ),\n                    state(\n                        operation_state="RECONCILING",\n                        next_action="WAIT_FOR_FLAT",\n                        attempt_state="FILLED",\n                        exposed=0,\n                    ),\n                    closed_state(),\n                    closed_state(),\n                ]\n            )\n            executor = FakeNormalExecutor(\n                [\n                    request_payload(),\n                    resume_payload(\n                        action="SUBMIT_MARKET_CLOSE",\n                        operation_state="PREPARING",\n                    ),\n                    resume_payload(\n                        action="WAIT_FOR_FLAT",\n                        operation_state="RECONCILING",\n                        attempt_state="FILLED",\n                    ),\n                    resume_payload(\n                        action="NONE",\n                        operation_state="SUCCEEDED",\n                        attempt_state="FILLED",\n                    ),\n                    resume_payload(\n                        action="NONE",\n                        operation_state="SUCCEEDED",\n                        attempt_state="FILLED",\n                    ),\n                ]\n            )\n            crash = FakeCrashExecutor()\n            result = PaperLiquidationRestartAcceptanceRunner(\n                policy=policy(root, summary),\n                command_executor=executor,\n                crash_executor=crash,\n                state_source=source,\n                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),\n                sleeper=lambda _seconds: None,\n            ).run()\n            payload = result.to_dict()\n            self.assertEqual(\n                crash.actions,\n                ["CANCEL_TAKE_PROFIT", "SUBMIT_MARKET_CLOSE"],\n            )\n            self.assertEqual(payload["intentional_process_terminations"], 2)\n            self.assertEqual(payload["broker_mutation_count"], 2)\n            self.assertEqual(\n                payload["protective_cancel_mode"],\n                "OCA_AUTO_CANCELLED_STOP",\n            )\n            self.assertTrue(payload["all_resume_mutations_false"])\n            self.assertTrue(payload["restart_adoption_proven"])\n            self.assertTrue(payload["state"]["fully_closed"])\n            self.assertTrue(payload["flat_proof"]["accepted"])\n\n'''
replace_once(
    "tester/target_paper_liquidation_restart_acceptance_tester.py",
    insert_marker,
    oca_test + insert_marker,
)
replace_once(
    "docs/paper-liquidation-restart-acceptance-runbook.md",
    '''cancel TAKE PROFIT\ncancel STOP\nsubmit liquidation MARKET close\n''',
    '''cancel TAKE PROFIT\ncancel STOP when it remains LIVE after reconciliation\nsubmit liquidation MARKET close\n''',
)
replace_once(
    "docs/paper-liquidation-restart-acceptance-runbook.md",
    '''→ cancel STOP and terminate child\n→ reconcile STOP cancellation without another cancelOrder\n→ submit one MARKET close and terminate child\n''',
    '''→ if STOP remains LIVE, cancel it and terminate child\n→ otherwise accept broker-confirmed OCA sibling cancellation\n→ reconcile any explicit STOP cancellation without another cancelOrder\n→ submit one MARKET close and terminate child\n''',
)
replace_once(
    "docs/paper-liquidation-restart-acceptance-runbook.md",
    '''restart_actions =\n  CANCEL_TAKE_PROFIT\n  CANCEL_STOP\n  SUBMIT_MARKET_CLOSE\n\nintentional_process_terminations = 3\nbroker_mutation_count             = 3\n''',
    '''restart_actions = either\n  CANCEL_TAKE_PROFIT, CANCEL_STOP, SUBMIT_MARKET_CLOSE\nor\n  CANCEL_TAKE_PROFIT, SUBMIT_MARKET_CLOSE\nwhen TWS auto-cancels the OCA sibling STOP\n\nintentional_process_terminations = 2 or 3\nbroker_mutation_count             = 2 or 3\nprotective_cancel_mode             = EXPLICIT_BOTH or OCA_AUTO_CANCELLED_STOP\n''',
)
