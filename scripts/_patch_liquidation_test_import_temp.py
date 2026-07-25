from __future__ import annotations

from pathlib import Path

path = Path("tester/target_execution_paper_liquidation_tester.py")
text = path.read_text(encoding="utf-8")
old = '''from tester.target_execution_protective_lifecycle_tester import (\n    broker_snapshot,\n    completed_order,\n    open_order,\n)\n'''
new = '''from tester.target_execution_protective_lifecycle_tester import (\n    _broker_snapshot as broker_snapshot,\n    _completed_order as completed_order,\n    _open_order as open_order,\n)\n'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"expected one helper import block, got {count}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
