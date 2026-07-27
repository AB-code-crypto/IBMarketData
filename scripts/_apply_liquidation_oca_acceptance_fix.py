from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"{path}: expected one occurrence, found {count}: {old!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


acceptance = "src/ibmd/operations/paper_liquidation_acceptance.py"
replace_once(
    acceptance,
    "    market_close_submission_count: int\n"
    "    state: LiquidationStateObservationV1\n",
    "    market_close_submission_count: int\n"
    "    durable_market_close_attempt_count: int\n"
    "    protective_cancel_mode: str\n"
    "    recovered_from_durable_state: bool\n"
    "    state: LiquidationStateObservationV1\n",
)
replace_once(
    acceptance,
    "            \"market_close_submission_count\": (\n"
    "                self.market_close_submission_count\n"
    "            ),\n"
    "            \"broker_mutation_count\": (\n",
    "            \"market_close_submission_count\": (\n"
    "                self.market_close_submission_count\n"
    "            ),\n"
    "            \"durable_market_close_attempt_count\": (\n"
    "                self.durable_market_close_attempt_count\n"
    "            ),\n"
    "            \"protective_cancel_mode\": self.protective_cancel_mode,\n"
    "            \"recovered_from_durable_state\": (\n"
    "                self.recovered_from_durable_state\n"
    "            ),\n"
    "            \"broker_mutation_count\": (\n",
)
replace_once(
    acceptance,
    "    def _load_entry_summary(self) -> tuple[str, str]:\n",
    "    def _load_entry_summary(self) -> tuple[str, str, str]:\n",
)
replace_once(
    acceptance,
    "        return (\n"
    "            self._text(\n"
    "                value.get(\"drill_id\"),\n"
    "                field_name=\"drill_id\",\n"
    "                stage=\"entry-summary\",\n"
    "            ),\n"
    "            self._text(\n"
    "                value.get(\"position_episode_id\"),\n"
    "                field_name=\"position_episode_id\",\n"
    "                stage=\"entry-summary\",\n"
    "            ),\n"
    "        )\n",
    "        return (\n"
    "            self._text(\n"
    "                value.get(\"drill_id\"),\n"
    "                field_name=\"drill_id\",\n"
    "                stage=\"entry-summary\",\n"
    "            ),\n"
    "            self._text(\n"
    "                value.get(\"position_episode_id\"),\n"
    "                field_name=\"position_episode_id\",\n"
    "                stage=\"entry-summary\",\n"
    "            ),\n"
    "            str(protection.get(\"take_profit_state\")),\n"
    "        )\n",
)
replace_once(
    acceptance,
    "        source_drill_id, position_episode_id = self._load_entry_summary()\n",
    "        (\n"
    "            source_drill_id,\n"
    "            position_episode_id,\n"
    "            entry_take_profit_state,\n"
    "        ) = self._load_entry_summary()\n",
)
replace_once(
    acceptance,
    "        invocation_count = 0\n"
    "        attempt_id: str | None = None\n"
    "        order_ref: str | None = None\n"
    "        state = self._state(position_episode_id)\n",
    "        invocation_count = 0\n"
    "        state = self._state(position_episode_id)\n"
    "        attempt_id = state.liquidation_attempt_id\n"
    "        order_ref = state.order_ref\n"
    "        recovered_from_durable_state = bool(resumed and state.fully_closed)\n",
)
replace_once(
    acceptance,
    "        if not resumed and any(value != 1 for value in counts.values()):\n"
    "            raise PaperLiquidationAcceptanceError(\n"
    "                \"fresh protected liquidation did not report exactly one TP cancel, \"\n"
    "                \"one STOP cancel and one MARKET close\",\n"
    "                stage=\"liquidation\",\n"
    "                broker_exposure_possible=True,\n"
    "            )\n",
    "        take_profit_cancel_count = counts[\"CANCEL_TAKE_PROFIT\"]\n"
    "        stop_cancel_count = counts[\"CANCEL_STOP\"]\n"
    "        market_close_submission_count = counts[\"SUBMIT_MARKET_CLOSE\"]\n"
    "        if not resumed:\n"
    "            if market_close_submission_count != 1:\n"
    "                raise PaperLiquidationAcceptanceError(\n"
    "                    \"fresh liquidation did not report exactly one MARKET close\",\n"
    "                    stage=\"liquidation\",\n"
    "                    broker_exposure_possible=True,\n"
    "                )\n"
    "            if entry_take_profit_state == \"LIVE\":\n"
    "                if (\n"
    "                    take_profit_cancel_count != 1\n"
    "                    or stop_cancel_count not in {0, 1}\n"
    "                ):\n"
    "                    raise PaperLiquidationAcceptanceError(\n"
    "                        \"fresh protected liquidation did not report one TP \"\n"
    "                        \"cancel and zero or one STOP cancel\",\n"
    "                        stage=\"liquidation\",\n"
    "                        broker_exposure_possible=True,\n"
    "                    )\n"
    "                protective_cancel_mode = (\n"
    "                    \"OCA_AUTO_CANCELLED_STOP\"\n"
    "                    if stop_cancel_count == 0\n"
    "                    else \"EXPLICIT_BOTH\"\n"
    "                )\n"
    "            else:\n"
    "                if take_profit_cancel_count != 0 or stop_cancel_count != 1:\n"
    "                    raise PaperLiquidationAcceptanceError(\n"
    "                        \"STOP-only liquidation did not report one STOP cancel\",\n"
    "                        stage=\"liquidation\",\n"
    "                        broker_exposure_possible=True,\n"
    "                    )\n"
    "                protective_cancel_mode = \"STOP_ONLY\"\n"
    "        else:\n"
    "            protective_cancel_mode = (\n"
    "                \"RECOVERED_DURABLE_CLOSED_STATE\"\n"
    "                if recovered_from_durable_state\n"
    "                else \"RESUMED_OPERATION\"\n"
    "            )\n"
    "        if state.attempt_no != 1 or state.attempt_state != \"FILLED\":\n"
    "            raise PaperLiquidationAcceptanceError(\n"
    "                \"closed liquidation does not prove one FILLED durable attempt\",\n"
    "                stage=\"liquidation\",\n"
    "                broker_exposure_possible=True,\n"
    "            )\n"
    "        durable_market_close_attempt_count = 1\n",
)
replace_once(
    acceptance,
    "            take_profit_cancel_count=counts[\"CANCEL_TAKE_PROFIT\"],\n"
    "            stop_cancel_count=counts[\"CANCEL_STOP\"],\n"
    "            market_close_submission_count=counts[\"SUBMIT_MARKET_CLOSE\"],\n"
    "            state=repeated,\n",
    "            take_profit_cancel_count=take_profit_cancel_count,\n"
    "            stop_cancel_count=stop_cancel_count,\n"
    "            market_close_submission_count=market_close_submission_count,\n"
    "            durable_market_close_attempt_count=(\n"
    "                durable_market_close_attempt_count\n"
    "            ),\n"
    "            protective_cancel_mode=protective_cancel_mode,\n"
    "            recovered_from_durable_state=recovered_from_durable_state,\n"
    "            state=repeated,\n",
)

manifest = "src/ibmd/operations/acceptance_manifest.py"
replace_once(
    manifest,
    "    if _positive_int(\n"
    "        value.get(\"market_close_submission_count\"),\n"
    "        field_name=\"market_close_submission_count\",\n"
    "    ) != 1:\n"
    "        raise TargetAcceptanceError(\n"
    "            \"market_close_submission_count must equal 1\"\n"
    "        )\n"
    "    facts[\"market_close_submission_count\"] = 1\n",
    "    explicit_market_close_count = _non_negative_int(\n"
    "        value.get(\"market_close_submission_count\"),\n"
    "        field_name=\"market_close_submission_count\",\n"
    "    )\n"
    "    durable_market_close_count = _positive_int(\n"
    "        value.get(\n"
    "            \"durable_market_close_attempt_count\",\n"
    "            explicit_market_close_count,\n"
    "        ),\n"
    "        field_name=\"durable_market_close_attempt_count\",\n"
    "    )\n"
    "    recovered = value.get(\"recovered_from_durable_state\") is True\n"
    "    if durable_market_close_count != 1:\n"
    "        raise TargetAcceptanceError(\n"
    "            \"durable_market_close_attempt_count must equal 1\"\n"
    "        )\n"
    "    if recovered:\n"
    "        if explicit_market_close_count != 0:\n"
    "            raise TargetAcceptanceError(\n"
    "                \"recovered liquidation must not report a new MARKET close\"\n"
    "            )\n"
    "    elif explicit_market_close_count != 1:\n"
    "        raise TargetAcceptanceError(\n"
    "            \"market_close_submission_count must equal 1\"\n"
    "        )\n"
    "    facts.update(\n"
    "        {\n"
    "            \"market_close_submission_count\": explicit_market_close_count,\n"
    "            \"durable_market_close_attempt_count\": 1,\n"
    "            \"recovered_from_durable_state\": recovered,\n"
    "        }\n"
    "    )\n",
)

test_path = "tester/target_paper_liquidation_acceptance_tester.py"
test_marker = "    def test_unknown_market_close_outcome_stops_without_new_attempt(self) -> None:\n"
new_tests = '''    def test_oca_auto_cancelled_stop_is_valid_fresh_liquidation(self) -> None:\n        with tempfile.TemporaryDirectory() as temporary:\n            root = Path(temporary)\n            summary = root / "entry-summary.json"\n            write_entry_summary(summary)\n            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")\n            executor = FakeExecutor(\n                [\n                    request_payload(),\n                    paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),\n                    paper_payload(\n                        action="SUBMIT_MARKET_CLOSE",\n                        mutation=True,\n                        operation_state="RECONCILING",\n                        attempt_state="FILLED",\n                    ),\n                    paper_payload(\n                        action="WAIT_FOR_FLAT",\n                        mutation=False,\n                        operation_state="SUCCEEDED",\n                        attempt_state="FILLED",\n                        episode_closed=True,\n                    ),\n                    paper_payload(\n                        action="NONE",\n                        mutation=False,\n                        operation_state="SUCCEEDED",\n                        attempt_state="FILLED",\n                        episode_closed=True,\n                    ),\n                ]\n            )\n            source = FakeStateSource(\n                [\n                    state(),\n                    state(\n                        operation_state="PREPARING",\n                        next_action="SUBMIT_MARKET_CLOSE",\n                        exposed=0,\n                    ),\n                    state(\n                        operation_state="RECONCILING",\n                        next_action="WAIT_FOR_FLAT",\n                        attempt_state="FILLED",\n                        exposed=0,\n                    ),\n                    closed_state(),\n                    closed_state(),\n                ]\n            )\n            result = PaperLiquidationAcceptanceRunner(\n                policy=policy(root, summary),\n                command_executor=executor,\n                state_source=source,\n                artifacts=artifacts,\n                sleeper=lambda _seconds: None,\n            ).run()\n            self.assertEqual(result.take_profit_cancel_count, 1)\n            self.assertEqual(result.stop_cancel_count, 0)\n            self.assertEqual(result.market_close_submission_count, 1)\n            self.assertEqual(result.durable_market_close_attempt_count, 1)\n            self.assertEqual(\n                result.protective_cancel_mode,\n                "OCA_AUTO_CANCELLED_STOP",\n            )\n            self.assertFalse(result.recovered_from_durable_state)\n            self.assertEqual(result.to_dict()["broker_mutation_count"], 2)\n\n    def test_closed_operation_can_recover_summary_without_broker_mutation(self) -> None:\n        with tempfile.TemporaryDirectory() as temporary:\n            root = Path(temporary)\n            summary = root / "entry-summary.json"\n            write_entry_summary(summary)\n            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")\n            executor = FakeExecutor(\n                [\n                    request_payload(created=False),\n                    paper_payload(\n                        action="NONE",\n                        mutation=False,\n                        operation_state="SUCCEEDED",\n                        attempt_state="FILLED",\n                        episode_closed=True,\n                    ),\n                ]\n            )\n            source = FakeStateSource([closed_state(), closed_state()])\n            result = PaperLiquidationAcceptanceRunner(\n                policy=policy(root, summary),\n                command_executor=executor,\n                state_source=source,\n                artifacts=artifacts,\n                sleeper=lambda _seconds: None,\n            ).run()\n            self.assertEqual(result.invocation_count, 0)\n            self.assertEqual(result.take_profit_cancel_count, 0)\n            self.assertEqual(result.stop_cancel_count, 0)\n            self.assertEqual(result.market_close_submission_count, 0)\n            self.assertEqual(result.durable_market_close_attempt_count, 1)\n            self.assertTrue(result.recovered_from_durable_state)\n            self.assertEqual(\n                result.protective_cancel_mode,\n                "RECOVERED_DURABLE_CLOSED_STATE",\n            )\n            self.assertEqual(result.to_dict()["broker_mutation_count"], 0)\n            self.assertEqual(executor.calls[-1][0], "liquidation-idempotency")\n            self.assertTrue((artifacts.directory / "summary.json").is_file())\n\n'''
replace_once(test_path, test_marker, new_tests + test_marker)

runbook = "docs/paper-liquidation-acceptance-runbook.md"
replace_once(
    runbook,
    "one durable MANUAL_EMERGENCY liquidation trigger\n"
    "→ cancel TAKE PROFIT once\n"
    "→ prove terminal TAKE PROFIT state\n"
    "→ cancel STOP once\n"
    "→ prove terminal STOP state\n",
    "one durable MANUAL_EMERGENCY liquidation trigger\n"
    "→ cancel TAKE PROFIT once when it is LIVE\n"
    "→ prove terminal TAKE PROFIT state\n"
    "→ accept an OCA-auto-cancelled STOP or cancel STOP once\n"
    "→ prove terminal STOP state\n",
)
replace_once(
    runbook,
    "One child invocation performs at most one broker mutation. A fresh protected\n"
    "position must report exactly:\n\n"
    "```text\n"
    "CANCEL_TAKE_PROFIT      = 1\n"
    "CANCEL_STOP             = 1\n"
    "SUBMIT_MARKET_CLOSE     = 1\n"
    "broker_mutation_count   = 3\n"
    "liquidation attempt_no  = 1\n"
    "```\n\n"
    "A resumed operation may report fewer mutations because already persisted actions\n"
    "are reconciled rather than repeated.\n",
    "One child invocation performs at most one broker mutation. A fresh protected\n"
    "position must report one MARKET close and one of these protective paths:\n\n"
    "```text\n"
    "EXPLICIT_BOTH:\n"
    "  CANCEL_TAKE_PROFIT    = 1\n"
    "  CANCEL_STOP           = 1\n"
    "  broker_mutation_count = 3 including MARKET close\n\n"
    "OCA_AUTO_CANCELLED_STOP:\n"
    "  CANCEL_TAKE_PROFIT    = 1\n"
    "  CANCEL_STOP           = 0\n"
    "  broker_mutation_count = 2 including MARKET close\n"
    "```\n\n"
    "Both paths require terminal evidence for both protective orders, one FILLED\n"
    "durable liquidation attempt and independent FLAT proof. A resumed completed\n"
    "operation may recover its summary with zero new broker mutations.\n",
)
