from pathlib import Path

path = Path("src/ibmd/execution/adapters/sqlite_reverse_finalization.py")
text = path.read_text(encoding="utf-8")
old = "from pathlib import Path\n\n"
if text.count(old) != 1:
    raise SystemExit("expected one unused pathlib import")
path.write_text(text.replace(old, "", 1), encoding="utf-8")
