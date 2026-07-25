from __future__ import annotations

from pathlib import Path

PATH = Path("tester/target_execution_paper_liquidation_tester.py")
text = PATH.read_text(encoding="utf-8")

old_import = '''from ibmd.execution.domain.liquidation import request_liquidation\n'''
new_import = '''from ibmd.execution.domain.liquidation import (\n    mark_close_submitting,\n    plan_close_attempt,\n    request_liquidation,\n)\n'''
if text.count(old_import) != 1:
    raise SystemExit("liquidation import block did not match once")
text = text.replace(old_import, new_import, 1)

old_block = '''        planned = replace(\n            repository.liquidation,\n            operation=replace(\n                repository.liquidation.operation,\n                state=LiquidationOperationState.PREPARING,\n                next_action=LiquidationNextAction.SUBMIT_MARKET_CLOSE,\n                updated_at_utc=T2,\n                blocking_reason="market_close_preparation_required",\n            ),\n        )\n        from ibmd.execution.domain.liquidation import (\n            mark_close_submitting,\n            plan_close_attempt,\n        )\n\n'''
if text.count(old_block) != 1:
    raise SystemExit("redundant recovery setup block did not match once")
text = text.replace(old_block, "", 1)

PATH.write_text(text, encoding="utf-8")
