from pathlib import Path

path = Path("src/ibmd/operations/supervisor.py")
text = path.read_text(encoding="utf-8")
old = "import subprocess\nimport sys\nimport time\n"
new = "import subprocess\nimport time\n"
if text.count(old) != 1:
    raise SystemExit("expected one unused supervisor sys import")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
