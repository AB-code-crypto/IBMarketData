from __future__ import annotations

from pathlib import Path

path = Path("tester/target_execution_paper_liquidation_tester.py")
text = path.read_text(encoding="utf-8")
replacements = [
    (
        '''from ibmd.public_contracts.execution import (\n    ExecutionReadinessStatus,\n    ExecutionReadinessV1,\n)\n''',
        '''from ibmd.public_contracts.execution import ExecutionReadinessStatus\n''',
    ),
    (
        '''    STRATEGY,\n    T1,\n    T2,\n''',
        '''    STRATEGY,\n    T2,\n''',
    ),
]
for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one lint replacement, got {count}: {old!r}")
    text = text.replace(old, new, 1)
path.write_text(text, encoding="utf-8")
