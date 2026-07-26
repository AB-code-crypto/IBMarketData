from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(
            f"expected one patch target in {path}: {old[:100]!r}"
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


preparer = Path(
    "apps/prepare_execution_policy_liquidation_paper_drill_v2.py"
)
replace_once(
    preparer,
    '''_SUPPORTED_REASONS = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.ROLLOVER,
}
''',
    '''_SUPPORTED_REASONS = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.DAILY_HALT,
    LiquidationReason.ROLLOVER,
}
''',
)
replace_once(
    preparer,
    '''            "Evaluate one explicit DAILY_FLAT or ROLLOVER condition at a "
''',
    '''            "Evaluate one explicit DAILY_FLAT, DAILY_HALT or ROLLOVER "
            "condition at a "
''',
)

runner = Path(
    "src/ibmd/operations/paper_policy_liquidation_acceptance.py"
)
replace_once(
    runner,
    '''_SUPPORTED = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.ROLLOVER,
}
''',
    '''_SUPPORTED = {
    LiquidationReason.DAILY_FLAT,
    LiquidationReason.DAILY_HALT,
    LiquidationReason.ROLLOVER,
}
''',
)
