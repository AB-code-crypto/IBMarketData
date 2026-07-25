from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters.sqlite_liquidation import SQLiteLiquidationStore
from ibmd.execution.adapters.sqlite_protection import SQLiteProtectionStore
from ibmd.execution.domain.liquidation import request_liquidation
from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.public_contracts.liquidation import LiquidationReason
from tester.target_execution_liquidation_tester import apply_schema
from tester.target_execution_protective_submit_tester import (
    T1,
    T3,
    blocked_readiness,
    episode_and_protection,
    strategy_position,
)


class LiquidationTriggerPersistenceTest(unittest.TestCase):
    def test_duplicate_stable_trigger_is_a_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            episode, protection = episode_and_protection()
            position = strategy_position(episode)
            readiness = blocked_readiness()
            SQLiteProtectionStore(database).publish_plan(
                PositionEpisodeProtectionPlan(
                    episode=episode,
                    strategy_position=position,
                    execution_readiness=readiness,
                    protection=protection,
                )
            )
            store = SQLiteLiquidationStore(database)
            first = request_liquidation(
                episode=episode,
                position=position,
                readiness=readiness,
                reason=LiquidationReason.DAILY_FLAT,
                source_ref="daily-flat:2026-07-27",
                observed_at_utc=T1,
            )
            stored = store.publish_request(current=None, result=first)
            repeated = request_liquidation(
                episode=episode,
                position=position,
                readiness=first.execution_readiness,
                reason=LiquidationReason.DAILY_FLAT,
                source_ref="daily-flat:2026-07-27",
                observed_at_utc=T3,
                existing=stored,
            )
            self.assertFalse(repeated.trigger_created)
            self.assertEqual(repeated.snapshot.operation, stored.operation)
            stored_again = store.publish_request(
                current=stored,
                result=repeated,
            )
            self.assertEqual(stored_again.operation, stored.operation)
            connection = sqlite3.connect(str(database))
            try:
                count = connection.execute(
                    "SELECT COUNT(*) FROM internal_liquidation_triggers"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
