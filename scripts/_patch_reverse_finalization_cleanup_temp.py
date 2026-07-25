from pathlib import Path

path = Path("src/ibmd/execution/adapters/sqlite_reverse_finalization.py")
text = path.read_text(encoding="utf-8")
old = '''                stored = _finalization(str(row["payload_json"]))
                if stored.to_dict() if hasattr(stored, "to_dict") else False:
                    pass
                if canonical_json_text(_payload(stored)) != canonical_json_text(
'''
new = '''                stored = _finalization(str(row["payload_json"]))
                if canonical_json_text(_payload(stored)) != canonical_json_text(
'''
if text.count(old) != 1:
    raise SystemExit("expected one reverse finalization cleanup target")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
