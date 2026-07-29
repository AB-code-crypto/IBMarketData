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
    '''    blocked = value.get("blocked_reasons")
    if not isinstance(blocked, list) or blocked:
        raise TargetAcceptanceError("policy liquidation blocked_reasons must be empty")
''',
    '''    blocked = value.get("blocked_reasons")
    if (
        not isinstance(blocked, list)
        or any(
            not isinstance(item, str) or not item.strip()
            for item in blocked
        )
    ):
        raise TargetAcceptanceError(
            "policy liquidation blocked_reasons must be a list of non-empty strings"
        )
    if scenario == "DAILY_FLAT":
        if blocked:
            raise TargetAcceptanceError(
                "DAILY_FLAT blocked_reasons must be empty"
            )
    elif scenario == "ROLLOVER":
        allowed_prefix = "daily_flat_session_not_production_qualified:"
        unexpected = [
            item for item in blocked
            if not item.startswith(allowed_prefix)
        ]
        if unexpected:
            raise TargetAcceptanceError(
                "ROLLOVER contains unexpected blockers: "
                + repr(unexpected)
            )
    else:
        raise TargetAcceptanceError(
            f"unsupported policy liquidation scenario: {scenario}"
        )
''',
    label="policy blocker validation",
)
manifest = replace_once(
    manifest,
    '''            "policy_trigger_proven": True,
        }
''',
    '''            "policy_trigger_proven": True,
            "blocked_reasons": list(blocked),
        }
''',
    label="policy blocker evidence",
)
manifest_path.write_text(manifest, encoding="utf-8")


test_path = Path("tester/target_cutover_preflight_tester.py")
test = test_path.read_text(encoding="utf-8")
test = replace_once(
    test,
    '''    def test_manifest_roundtrip_and_tamper_detection(self) -> None:
''',
    '''    def test_rollover_accepts_only_daily_flat_qualification_blocker(self) -> None:
        value = summary(AcceptanceGate.ROLLOVER)
        value["blocked_reasons"] = [
            "daily_flat_session_not_production_qualified:CME_EQUITY_INDEX"
        ]
        _finished, _primary_id, facts = validate_acceptance_summary(
            AcceptanceGate.ROLLOVER,
            value,
        )
        self.assertEqual(
            facts["blocked_reasons"],
            value["blocked_reasons"],
        )

        invalid_rollover = summary(AcceptanceGate.ROLLOVER)
        invalid_rollover["blocked_reasons"] = [
            "rollover_contract_still_active:MNQU6"
        ]
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "ROLLOVER contains unexpected blockers",
        ):
            validate_acceptance_summary(
                AcceptanceGate.ROLLOVER,
                invalid_rollover,
            )

        invalid_daily_flat = summary(AcceptanceGate.DAILY_FLAT)
        invalid_daily_flat["blocked_reasons"] = [
            "daily_flat_session_not_production_qualified:CME_EQUITY_INDEX"
        ]
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "DAILY_FLAT blocked_reasons must be empty",
        ):
            validate_acceptance_summary(
                AcceptanceGate.DAILY_FLAT,
                invalid_daily_flat,
            )

    def test_manifest_roundtrip_and_tamper_detection(self) -> None:
''',
    label="rollover blocker regression test",
)
test_path.write_text(test, encoding="utf-8")

print("Patched acceptance manifest ROLLOVER blocker validation and added regression coverage.")
