from __future__ import annotations

from pathlib import Path

path = Path("tester/target_execution_reverse_handoff_tester.py")
text = path.read_text(encoding="utf-8")
replacements = [
    (
        "from datetime import datetime, timezone\n",
        "from datetime import datetime\n",
    ),
    (
        "from ibmd.foundation.identity import new_id\n",
        "",
    ),
    (
        '''from ibmd.public_contracts.execution import (\n    ExecutionCommandState,\n    ExecutionCommandStateV1,\n    StrategyPositionSide,\n)\n''',
        '''from ibmd.public_contracts.execution import (\n    ExecutionCommandState,\n    ExecutionCommandStateV1,\n)\n''',
    ),
    (
        '''    T0,\n    T1,\n    T2,\n''',
        '''    T0,\n    T2,\n''',
    ),
]
for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one lint replacement, got {count}: {old!r}")
    text = text.replace(old, new, 1)
path.write_text(text, encoding="utf-8")
