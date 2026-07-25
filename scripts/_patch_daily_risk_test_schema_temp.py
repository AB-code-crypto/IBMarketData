from pathlib import Path

path = Path("tester/target_execution_daily_risk_tester.py")
text = path.read_text(encoding="utf-8")
old = '''    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        for statement in manifest["statements"]:
'''
new = '''    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS execution_target_schema_components ("
            "component_name TEXT PRIMARY KEY, "
            "component_version INTEGER NOT NULL CHECK (component_version > 0), "
            "checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL, "
            "application_version TEXT NOT NULL)"
        )
        for statement in manifest["statements"]:
'''
if text.count(old) != 1:
    raise SystemExit("expected one daily-risk component helper")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
