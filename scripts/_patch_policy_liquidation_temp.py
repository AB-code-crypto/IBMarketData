from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(
            f"expected exactly one patch target in {path}: {old[:100]!r}"
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


policy = Path("src/ibmd/operations/paper_policy_liquidation_acceptance.py")
replace_once(
    policy,
    "from __future__ import annotations\n\nfrom dataclasses import dataclass\n",
    "from __future__ import annotations\n\nimport time\nfrom dataclasses import dataclass\n",
)
replace_once(
    policy,
    '''        artifacts: PaperAcceptanceArtifactSink,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None],
    ) -> None:
''',
    '''        artifacts: PaperAcceptanceArtifactSink,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
''',
)

base = Path("src/ibmd/operations/paper_liquidation_acceptance.py")
replace_once(
    base,
    '''        if (
            value.get("schema_name") != "PaperAcceptanceResult"
            or value.get("schema_version") != 1
        ):
            raise PaperLiquidationAcceptanceError(
                "entry summary is not PaperAcceptanceResult v1",
                stage="entry-summary",
            )
''',
    '''        if (
            value.get("schema_name")
            not in {
                "PaperAcceptanceResult",
                "PaperRestartAcceptanceResult",
                "PaperReverseAcceptanceResult",
            }
            or value.get("schema_version") != 1
        ):
            raise PaperLiquidationAcceptanceError(
                "entry summary is not a supported protected-position result v1",
                stage="entry-summary",
            )
''',
)

restart = Path(
    "src/ibmd/operations/paper_liquidation_restart_acceptance.py"
)
replace_once(
    restart,
    '''        if value.get("schema_name") not in {
            "PaperAcceptanceResult",
            "PaperRestartAcceptanceResult",
        } or int(value.get("schema_version") or 0) != 1:
''',
    '''        if value.get("schema_name") not in {
            "PaperAcceptanceResult",
            "PaperRestartAcceptanceResult",
            "PaperReverseAcceptanceResult",
        } or int(value.get("schema_version") or 0) != 1:
''',
)
