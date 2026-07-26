from pathlib import Path

path = Path("tester/target_paper_reverse_acceptance_tester.py")
text = path.read_text(encoding="utf-8")
text = text.replace(
    '''from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)
''',
    '''from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)
from ibmd.public_contracts.protection import (
    ProtectionSetStatus,
    ProtectiveOrderState,
)
''',
    1,
)
old = '''        source.protection = replace(
            source.protection,
            orders=tuple(
                replace(order, state=order.state.PLANNED)
                if order.kind.value == "STOP_LOSS"
                else order
                for order in source.protection.orders
            ),
        )
'''
new = '''        source.protection = replace(
            source.protection,
            orders=tuple(
                replace(
                    order,
                    state=ProtectiveOrderState.UNKNOWN_OUTCOME,
                    updated_at_utc="2026-07-27T10:00:03Z",
                    failure_reason="test_stop_unknown",
                )
                if order.kind.value == "STOP_LOSS"
                else order
                for order in source.protection.orders
            ),
            status=ProtectionSetStatus.UNPROTECTED,
            updated_at_utc="2026-07-27T10:00:03Z",
            blocking_reason="test_stop_unknown",
        )
'''
if text.count(old) != 1:
    raise SystemExit("expected paper reverse stop mutation test block")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
