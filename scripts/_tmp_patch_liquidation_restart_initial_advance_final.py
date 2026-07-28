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


SOURCE = "src/ibmd/operations/paper_liquidation_restart_acceptance.py"
TEST = "tester/target_paper_liquidation_restart_acceptance_tester.py"
SCRIPT = "scripts/run_paper_liquidation_restart_acceptance.py"
RUNBOOK = "docs/paper-liquidation-restart-acceptance-runbook.md"

replace_once(
    SOURCE,
    '''            "protective_cancel_mode": self.protective_cancel_mode,\n            "all_resume_mutations_false": True,\n''',
    '''            "protective_cancel_mode": self.protective_cancel_mode,\n            "initial_advance_broker_free": True,\n            "all_resume_mutations_false": True,\n''',
)

replace_once(
    SOURCE,
    '''    def _normal_resume(\n        self,\n        *,\n        position_episode_id: str,\n        step_name: str,\n    ) -> Mapping[str, Any]:\n        payload = self._run_json(\n            step_name=step_name,\n            arguments=self._paper_arguments(position_episode_id),\n        )\n        self._assert_not_unsafe_payload(payload)\n        if payload.get("broker_mutation_performed") is not False:\n            raise PaperLiquidationAcceptanceError(\n                "CRITICAL: liquidation restart resume performed another broker "\n                "mutation",\n                stage=step_name,\n                broker_exposure_possible=True,\n            )\n        return payload\n\n    def run(self) -> PaperLiquidationRestartAcceptanceResultV1:\n''',
    '''    def _normal_resume(\n        self,\n        *,\n        position_episode_id: str,\n        step_name: str,\n    ) -> Mapping[str, Any]:\n        payload = self._run_json(\n            step_name=step_name,\n            arguments=self._paper_arguments(position_episode_id),\n        )\n        self._assert_not_unsafe_payload(payload)\n        if payload.get("broker_mutation_performed") is not False:\n            raise PaperLiquidationAcceptanceError(\n                "CRITICAL: liquidation restart resume performed another broker "\n                "mutation",\n                stage=step_name,\n                broker_exposure_possible=True,\n            )\n        return payload\n\n    def _initial_advance_arguments(\n        self,\n        position_episode_id: str,\n    ) -> tuple[str, ...]:\n        paths = self.policy.paths\n        return (\n            "--advance-position-episode-id",\n            position_episode_id,\n            "--execution-database",\n            str(paths.execution_database),\n            "--position-feed-database",\n            str(paths.position_feed_database),\n            "--catalog-root",\n            str(paths.catalog_root),\n            "--instrument",\n            self.policy.instrument_id,\n            "--position-max-age-seconds",\n            str(self.policy.position_max_age_seconds),\n        )\n\n    def _initial_broker_free_advance(\n        self,\n        *,\n        position_episode_id: str,\n        operation_id: str,\n    ) -> Mapping[str, Any]:\n        payload = self._run_json(\n            step_name="liquidation-initial-advance",\n            arguments=self._initial_advance_arguments(position_episode_id),\n        )\n        if payload.get("broker_mutations_performed") is not False:\n            raise PaperLiquidationAcceptanceError(\n                "initial liquidation advance unexpectedly performed a broker "\n                "mutation",\n                stage="liquidation-initial-advance",\n            )\n        if payload.get("operation_created") is not False:\n            raise PaperLiquidationAcceptanceError(\n                "initial liquidation advance unexpectedly created an operation",\n                stage="liquidation-initial-advance",\n            )\n        operation = self._mapping(\n            payload.get("liquidation_operation"),\n            field_name="liquidation_operation",\n            stage="liquidation-initial-advance",\n        )\n        if operation.get("liquidation_operation_id") != operation_id:\n            raise PaperLiquidationAcceptanceError(\n                "initial liquidation advance changed the operation identity",\n                stage="liquidation-initial-advance",\n            )\n        action = self._text(\n            operation.get("next_action"),\n            field_name="next_action",\n            stage="liquidation-initial-advance",\n        )\n        if action not in {"CANCEL_TAKE_PROFIT", "CANCEL_STOP"}:\n            raise PaperLiquidationAcceptanceError(\n                "initial liquidation advance did not select a protective cancel: "\n                f"{action}",\n                stage="liquidation-initial-advance",\n            )\n        return payload\n\n    def run(self) -> PaperLiquidationRestartAcceptanceResultV1:\n''',
)

replace_once(
    SOURCE,
    '''        if request.get("operation_created") is not True:\n            raise PaperLiquidationAcceptanceError(\n                "liquidation restart acceptance requires a fresh operation",\n                stage="liquidation-request",\n            )\n\n        checkpoints: list[LiquidationRestartCheckpointV1] = []\n''',
    '''        if request.get("operation_created") is not True:\n            raise PaperLiquidationAcceptanceError(\n                "liquidation restart acceptance requires a fresh operation",\n                stage="liquidation-request",\n            )\n        if operation.get("next_action") != "RECONCILE_EXITS":\n            raise PaperLiquidationAcceptanceError(\n                "fresh liquidation operation did not start at RECONCILE_EXITS",\n                stage="liquidation-request",\n            )\n        self._initial_broker_free_advance(\n            position_episode_id=position_episode_id,\n            operation_id=operation_id,\n        )\n\n        checkpoints: list[LiquidationRestartCheckpointV1] = []\n''',
)

replace_once(
    SCRIPT,
    '''        "interactive_confirmation_required": False,\n        "automatic_retry_enabled": False,\n        "paper_account_left_flat_after_success": True,\n''',
    '''        "interactive_confirmation_required": False,\n        "automatic_retry_enabled": False,\n        "initial_advance_broker_free": True,\n        "paper_account_left_flat_after_success": True,\n''',
)

replace_once(
    TEST,
    '''        "liquidation_operation": {\n            "liquidation_operation_id": OPERATION_ID,\n            "state": "REQUESTED",\n        },\n''',
    '''        "liquidation_operation": {\n            "liquidation_operation_id": OPERATION_ID,\n            "state": "REQUESTED",\n            "next_action": "RECONCILE_EXITS",\n        },\n''',
)

replace_once(
    TEST,
    '''def resume_payload(\n''',
    '''def advance_payload(*, action: str = "CANCEL_TAKE_PROFIT") -> dict:\n    return {\n        "liquidation_operation": {\n            "liquidation_operation_id": OPERATION_ID,\n            "state": "CANCELING_EXITS",\n            "next_action": action,\n            "blocking_reason": None,\n        },\n        "liquidation_attempt": None,\n        "operation_created": False,\n        "trigger_created": False,\n        "broker_mutations_performed": False,\n    }\n\n\ndef resume_payload(\n''',
)

replace_once(
    TEST,
    '''                [\n                    request_payload(),\n                    resume_payload(action="CANCEL_STOP"),\n''',
    '''                [\n                    request_payload(),\n                    advance_payload(),\n                    resume_payload(action="CANCEL_STOP"),\n''',
)

replace_once(
    TEST,
    '''                [\n                    request_payload(),\n                    resume_payload(\n                        action="SUBMIT_MARKET_CLOSE",\n''',
    '''                [\n                    request_payload(),\n                    advance_payload(),\n                    resume_payload(\n                        action="SUBMIT_MARKET_CLOSE",\n''',
)

replace_once(
    TEST,
    '''                [\n                    request_payload(),\n                    resume_payload(\n                        action="CANCEL_STOP",\n                        mutation=True,\n''',
    '''                [\n                    request_payload(),\n                    advance_payload(),\n                    resume_payload(\n                        action="CANCEL_STOP",\n                        mutation=True,\n''',
)

replace_once(
    TEST,
    '''            self.assertEqual(payload["protective_cancel_mode"], "EXPLICIT_BOTH")\n            self.assertTrue(payload["all_resume_mutations_false"])\n''',
    '''            self.assertEqual(payload["protective_cancel_mode"], "EXPLICIT_BOTH")\n            self.assertTrue(payload["initial_advance_broker_free"])\n            self.assertTrue(payload["all_resume_mutations_false"])\n            self.assertEqual(executor.calls[1][0], "liquidation-initial-advance")\n            self.assertEqual(\n                executor.calls[1][2][:2],\n                ("--advance-position-episode-id", EPISODE_ID),\n            )\n            self.assertNotIn(\n                "--once-paper-position-episode-id",\n                executor.calls[1][2],\n            )\n''',
)

replace_once(
    TEST,
    '''            self.assertEqual(\n                payload["protective_cancel_mode"],\n                "OCA_AUTO_CANCELLED_STOP",\n            )\n            self.assertTrue(payload["all_resume_mutations_false"])\n''',
    '''            self.assertEqual(\n                payload["protective_cancel_mode"],\n                "OCA_AUTO_CANCELLED_STOP",\n            )\n            self.assertTrue(payload["initial_advance_broker_free"])\n            self.assertTrue(payload["all_resume_mutations_false"])\n''',
)

replace_once(
    RUNBOOK,
    '''```text\nliquidation request, broker-free\n→ cancel TAKE PROFIT and terminate child\n''',
    '''```text\nliquidation request, broker-free\n→ broker-free advance selects the first protective cancellation\n→ cancel TAKE PROFIT and terminate child\n''',
)

replace_once(
    RUNBOOK,
    '''```\n\nIf TAKE PROFIT is `NOT_REQUIRED`, that cancellation checkpoint is omitted.\n''',
    '''```\n\nThe initial advance uses `--advance-position-episode-id`; that mode has no broker\ngateway and must report `broker_mutations_performed=false` before any crash probe.\n\nIf TAKE PROFIT is `NOT_REQUIRED`, that cancellation checkpoint is omitted.\n''',
)

replace_once(
    RUNBOOK,
    '''protective_cancel_mode             = EXPLICIT_BOTH or OCA_AUTO_CANCELLED_STOP\nall_resume_mutations_false        = true\n''',
    '''protective_cancel_mode             = EXPLICIT_BOTH or OCA_AUTO_CANCELLED_STOP\ninitial_advance_broker_free       = true\nall_resume_mutations_false        = true\n''',
)

validation = ROOT / "docs/validation/paper-liquidation-restart-acceptance-2026-07-28.md"
if validation.exists():
    raise SystemExit(f"validation document already exists: {validation}")
validation.write_text(
    '''# Paper liquidation restart acceptance — 2026-07-28\n\n## Scope\n\nA real Interactive Brokers paper-account drill validated deterministic restart\nadoption for entry, protective orders and liquidation of one MNQ position.\n\n```text\naccount:       DUQ895165\nenvironment:   paper\ndeployment:    paper-drill-restart-20260728-03\ninstrument:    MNQ / MNQU6\nquantity:      1\ncandidate SHA: b40eb2d8afcc1ecc5040a830848055ae6c550598\n```\n\nThe independent broker-position feed remained active throughout the sequence.\nThe paper account was FLAT with no open orders before the drill.\n\n## Entry and protection restart proof\n\nThe entry restart acceptance completed with exit code 0 and proved:\n\n```text\nMARKET entry checkpoint       = present\nSTOP checkpoint               = present\nTAKE PROFIT checkpoint        = present\nintentional process exits     = 3\nbroker mutations              = 3\nall resume submissions false  = true\nentry attempt_no              = 1\nposition proof accepted       = true\nSTOP state                    = LIVE\nTAKE PROFIT state             = LIVE\nlive position left protected  = true\nrestart adoption proven       = true\n```\n\nEntry artifact directory:\n\n```text\nC:\\IBMarketData-shadow\\data_target\\paper-drill-restart-20260728-03\\runtime\\paper_restart_acceptance\\paper-restart-20260728T165156Z\\run-20260728T165156095913Z\n```\n\n## Liquidation restart proof\n\nThe liquidation restart acceptance completed with exit code 0. The initial\nliquidation transition was performed through the broker-free advance entrypoint.\nInteractive Brokers then auto-cancelled the STOP as the OCA sibling after the\nTAKE PROFIT cancellation.\n\n```text\ninitial_advance_broker_free  = true\nrestart actions              = CANCEL_TAKE_PROFIT, SUBMIT_MARKET_CLOSE\nprotective_cancel_mode       = OCA_AUTO_CANCELLED_STOP\nintentional process exits    = 2\nbroker mutations             = 2\nall resume mutations false   = true\nliquidation attempt_no       = 1\nliquidation attempt state    = FILLED\nliquidation operation state  = SUCCEEDED\nposition episode state       = CLOSED\nprotection state             = CLOSED\nstrategy position state      = FLAT\nexposed protective orders    = 0\nindependent FLAT proof       = accepted\nopen contract count          = 0\npaper account left flat      = true\nmanual cleanup required      = false\nrestart adoption proven      = true\n```\n\nLiquidation artifact directory:\n\n```text\nC:\\IBMarketData-shadow\\data_target\\paper-drill-restart-20260728-03\\runtime\\paper_restart_acceptance\\liquidation\\run-20260728T165240061995Z\n```\n\n## Result\n\nThe liquidation restart acceptance gate passed on the real paper account. The\noriginal defect—using a broker-capable resume invocation to select the first\nprotective cancellation—was not reproduced after the broker-free initial advance\nfix. No manual cleanup remained.\n''',
    encoding="utf-8",
)
