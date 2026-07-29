from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


manifest_path = Path("src/ibmd/operations/acceptance_manifest.py")
manifest = manifest_path.read_text(encoding="utf-8")
manifest = replace_once(
    manifest,
    '''def _closed_position_facts(value: Mapping[str, Any]) -> dict[str, Any]:
    state = _mapping(value.get("state"), field_name="state")
    proof = _mapping(value.get("flat_proof"), field_name="flat_proof")
    _required_true(state.get("fully_closed"), field_name="state.fully_closed")
''',
    '''def _closed_position_facts(
    value: Mapping[str, Any],
    *,
    state_field: str = "state",
) -> dict[str, Any]:
    state = _mapping(value.get(state_field), field_name=state_field)
    proof = _mapping(value.get("flat_proof"), field_name="flat_proof")
    _required_true(
        state.get("fully_closed"),
        field_name=f"{state_field}.fully_closed",
    )
''',
    label="closed-position state field",
)
manifest = replace_once(
    manifest,
    '''    _required_false(
        value.get("command_intake_enabled"),
        field_name="command_intake_enabled",
    )
    facts = _closed_position_facts(value)
    state = _mapping(
''',
    '''    _required_false(
        value.get("command_intake_enabled"),
        field_name="command_intake_enabled",
    )
    facts = _closed_position_facts(
        value,
        state_field="liquidation_state",
    )
    state = _mapping(
''',
    label="daily halt liquidation state",
)
manifest_path.write_text(manifest, encoding="utf-8")


test_path = Path("tester/target_cutover_preflight_tester.py")
test = test_path.read_text(encoding="utf-8")
test = replace_once(
    test,
    '''            "cleanup_status_complete": True,
            "command_intake_enabled": False,
            **closed(),
            "synthetic_trigger": {
''',
    '''            "cleanup_status_complete": True,
            "command_intake_enabled": False,
            "position_episode_id": EPISODE,
            "liquidation_operation_id": LIQUIDATION,
            "liquidation_state": {"fully_closed": True},
            "flat_proof": {"accepted": True},
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "synthetic_trigger": {
''',
    label="daily halt fixture shape",
)
test = replace_once(
    test,
    '''    def test_rollover_accepts_only_daily_flat_qualification_blocker(self) -> None:
''',
    '''    def test_daily_halt_uses_liquidation_state_contract(self) -> None:
        value = summary(AcceptanceGate.DAILY_HALT)
        self.assertNotIn("state", value)
        self.assertTrue(value["liquidation_state"]["fully_closed"])

        _finished, _primary_id, facts = validate_acceptance_summary(
            AcceptanceGate.DAILY_HALT,
            value,
        )
        self.assertTrue(facts["fully_closed"])
        self.assertTrue(facts["flat_proof_accepted"])

        invalid = dict(value)
        invalid.pop("liquidation_state")
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "liquidation_state must be a JSON object",
        ):
            validate_acceptance_summary(
                AcceptanceGate.DAILY_HALT,
                invalid,
            )

    def test_rollover_accepts_only_daily_flat_qualification_blocker(self) -> None:
''',
    label="daily halt regression test",
)
test_path.write_text(test, encoding="utf-8")

print(
    "Patched DAILY_HALT manifest validation to use liquidation_state and "
    "added realistic regression coverage."
)
