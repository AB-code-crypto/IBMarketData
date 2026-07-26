from pathlib import Path

path = Path("src/ibmd/operations/paper_reverse_acceptance.py")
text = path.read_text(encoding="utf-8")
text = text.replace(
    "from __future__ import annotations\n\nfrom dataclasses import dataclass\n",
    "from __future__ import annotations\n\nimport time\nfrom dataclasses import dataclass\n",
    1,
)
old = '''        artifacts: PaperAcceptanceArtifactSink,
        handoff_max_invocations: int = 12,
        handoff_poll_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None],
    ) -> None:
'''
new = '''        artifacts: PaperAcceptanceArtifactSink,
        handoff_max_invocations: int = 12,
        handoff_poll_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
'''
if text.count(old) != 1:
    raise SystemExit("expected paper reverse runner constructor block")
text = text.replace(old, new, 1)
text = text.replace(
    '"reverse handoff cancellation order differs from STOP-first safety "\n',
    '"reverse handoff cancellation order differs from the TP-then-STOP "\n',
    1,
)
path.write_text(text, encoding="utf-8")
