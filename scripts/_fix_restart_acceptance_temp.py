from pathlib import Path

path = Path("src/ibmd/operations/paper_restart_acceptance.py")
text = path.read_text(encoding="utf-8")
text = text.replace(
    "import json\nimport subprocess\n",
    "import json\nimport subprocess\nimport time\n",
    1,
)
old = '''        artifacts: PaperAcceptanceArtifactStore,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None],
    ) -> None:
'''
new = '''        artifacts: PaperAcceptanceArtifactStore,
        clock: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
'''
if text.count(old) != 1:
    raise SystemExit("expected restart runner constructor block")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
