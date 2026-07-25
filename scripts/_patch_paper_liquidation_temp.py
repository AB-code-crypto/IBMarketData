from __future__ import annotations

from pathlib import Path

PATH = Path("src/ibmd/execution/application/paper_liquidation.py")
text = PATH.read_text(encoding="utf-8")

replacements = [
    (
        "from ibmd.foundation.time import format_utc, parse_utc, utc_now",
        "from ibmd.foundation.time import format_utc, utc_now",
    ),
    (
        "    PositionEpisodeStatus,\n",
        "",
    ),
    (
        '''        if (\n            not readiness.broker_actions_enabled\n            or not readiness.reconciliation_complete\n            or not readiness.clock_healthy\n        ):\n            raise PaperLiquidationError(\n                "liquidation requires broker actions, reconciliation and clock health"\n            )\n''',
        '''        if (\n            not readiness.reconciliation_complete\n            or not readiness.clock_healthy\n        ):\n            raise PaperLiquidationError(\n                "liquidation recovery requires reconciliation and clock health"\n            )\n''',
    ),
    (
        '''    async def _read_broker_snapshot(self) -> BrokerReconciliationSnapshotV1:\n        return await self.broker_snapshot_source.read_snapshot(\n            account_id=self.policy.account_id\n        )\n''',
        '''    @staticmethod\n    def _require_broker_mutation(\n        readiness: ExecutionReadinessV1,\n    ) -> None:\n        if not readiness.broker_actions_enabled:\n            raise PaperLiquidationError(\n                "liquidation broker mutation requires broker_actions_enabled=true"\n            )\n\n    async def _read_broker_snapshot(self) -> BrokerReconciliationSnapshotV1:\n        return await self.broker_snapshot_source.read_snapshot(\n            account_id=self.policy.account_id\n        )\n''',
    ),
    (
        '''    ) -> PaperLiquidationRun:\n        before = liquidation\n        order = (\n''',
        '''    ) -> PaperLiquidationRun:\n        self._require_broker_mutation(readiness)\n        before = liquidation\n        order = (\n''',
    ),
    (
        '''    ) -> PaperLiquidationRun:\n        before = liquidation\n        working = liquidation\n        working_readiness = readiness\n''',
        '''    ) -> PaperLiquidationRun:\n        self._require_broker_mutation(readiness)\n        before = liquidation\n        working = liquidation\n        working_readiness = readiness\n''',
    ),
    (
        '''        assessed = assess_next_action(\n            snapshot=liquidation,\n''',
        '''        unresolved_protection = any(\n            item.state\n            in {\n                ProtectiveOrderState.SUBMITTING,\n                ProtectiveOrderState.LIVE,\n                ProtectiveOrderState.CANCEL_REQUESTED,\n                ProtectiveOrderState.UNKNOWN_OUTCOME,\n            }\n            for item in protection.orders\n        )\n        if proof.state == "FLAT" and not unresolved_protection:\n            return self._complete_flat(\n                episode=episode,\n                protection=protection,\n                position=position,\n                readiness=readiness,\n                liquidation=liquidation,\n                proof=proof,\n            )\n\n        assessed = assess_next_action(\n            snapshot=liquidation,\n''',
    ),
]

for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected exactly one replacement match, got {count}: {old[:80]!r}"
        )
    text = text.replace(old, new, 1)

PATH.write_text(text, encoding="utf-8")
