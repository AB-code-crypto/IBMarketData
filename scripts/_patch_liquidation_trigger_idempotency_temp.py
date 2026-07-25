from __future__ import annotations

from pathlib import Path

path = Path("src/ibmd/execution/domain/liquidation.py")
text = path.read_text(encoding="utf-8")
old = '''        trigger_created = all(\n            item.trigger_id != trigger.trigger_id for item in existing.triggers\n        )\n        triggers = (\n            existing.triggers + (trigger,)\n            if trigger_created\n            else existing.triggers\n        )\n        reasons = tuple(\n            sorted(\n                {*existing.operation.trigger_reasons, reason},\n                key=lambda item: item.value,\n            )\n        )\n        operation = replace(\n            existing.operation,\n            trigger_reasons=reasons,\n            updated_at_utc=observed,\n        )\n'''
new = '''        existing_trigger = next(\n            (\n                item\n                for item in existing.triggers\n                if item.trigger_id == trigger.trigger_id\n            ),\n            None,\n        )\n        trigger_created = existing_trigger is None\n        if trigger_created:\n            triggers = existing.triggers + (trigger,)\n            reasons = tuple(\n                sorted(\n                    {*existing.operation.trigger_reasons, reason},\n                    key=lambda item: item.value,\n                )\n            )\n            operation = replace(\n                existing.operation,\n                trigger_reasons=reasons,\n                updated_at_utc=observed,\n            )\n        else:\n            trigger = existing_trigger\n            triggers = existing.triggers\n            operation = existing.operation\n'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"expected one trigger block, got {count}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
