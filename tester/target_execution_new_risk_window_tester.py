from __future__ import annotations

import unittest

from ibmd.execution.application.new_risk_window import (
    NewRiskWindowError,
    NewRiskWindowV1,
)


class NewRiskWindowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.window = NewRiskWindowV1(
            enabled=True,
            timezone_name="America/Chicago",
            liquidation_start_local="14:59:50",
            risk_blocked_until_local="16:00:00",
        )

    def test_normal_session_time_is_allowed(self) -> None:
        self.window.require_allows_new_risk(
            observed_at_utc="2026-07-24T18:00:00Z",
            lead_seconds=60,
        )

    def test_liquidation_window_is_blocked(self) -> None:
        with self.assertRaisesRegex(
            NewRiskWindowError,
            "daily-flat window",
        ):
            self.window.require_allows_new_risk(
                observed_at_utc="2026-07-24T20:00:00Z",
                lead_seconds=0,
            )

    def test_lead_time_blocks_submission_near_liquidation_start(self) -> None:
        with self.assertRaisesRegex(
            NewRiskWindowError,
            "lead_seconds=60",
        ):
            self.window.require_allows_new_risk(
                observed_at_utc="2026-07-24T19:59:00Z",
                lead_seconds=60,
            )

    def test_post_window_time_is_not_claimed_blocked(self) -> None:
        self.window.require_allows_new_risk(
            observed_at_utc="2026-07-24T21:01:00Z",
            lead_seconds=60,
        )

    def test_disabled_policy_is_noop(self) -> None:
        disabled = NewRiskWindowV1(
            enabled=False,
            timezone_name="America/Chicago",
            liquidation_start_local="14:59:50",
            risk_blocked_until_local="16:00:00",
        )
        disabled.require_allows_new_risk(
            observed_at_utc="2026-07-24T20:30:00Z",
            lead_seconds=60,
        )


if __name__ == "__main__":
    unittest.main()
