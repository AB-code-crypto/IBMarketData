from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


reverse_app_path = Path("apps/run_execution_reverse_handoff_v2.py")
reverse_app = reverse_app_path.read_text(encoding="utf-8")
reverse_app = replace_once(
    reverse_app,
    '    parser.add_argument("--cancel-client-id-offset", type=int, default=180)\n',
    '    parser.add_argument("--cancel-client-id-offset", type=int, default=140)\n',
    label="reverse handoff cancellation client default",
)
reverse_app_path.write_text(reverse_app, encoding="utf-8")


acceptance_path = Path("src/ibmd/operations/paper_reverse_acceptance.py")
acceptance = acceptance_path.read_text(encoding="utf-8")
acceptance = replace_once(
    acceptance,
    '            "--cancel-client-id-offset",\n            "180",\n',
    '            "--cancel-client-id-offset",\n            str(self.policy.protective_submit_client_id_offset),\n',
    label="reverse acceptance cancellation client ownership",
)
acceptance_path.write_text(acceptance, encoding="utf-8")


test_path = Path("tester/target_reverse_cancel_client_ownership_tester.py")
if test_path.exists():
    raise RuntimeError(f"test already exists: {test_path}")

test_path.write_text(
    '''from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.run_execution_protective_submit_v2 import (
    build_parser as build_protective_submit_parser,
)
from apps.run_execution_reverse_handoff_v2 import (
    build_parser as build_reverse_handoff_parser,
)
from ibmd.operations.paper_acceptance import (
    PaperAcceptancePathsV1,
    PaperAcceptancePolicyV1,
)
from ibmd.operations.paper_reverse_acceptance import (
    PaperReverseAcceptanceRunner,
)


class ReverseCancelClientOwnershipTest(unittest.TestCase):
    def test_reverse_default_matches_protective_submit_default(self) -> None:
        protective = build_protective_submit_parser().parse_args([])
        reverse = build_reverse_handoff_parser().parse_args([])
        self.assertEqual(protective.submit_client_id_offset, 140)
        self.assertEqual(reverse.cancel_client_id_offset, 140)
        self.assertEqual(
            reverse.cancel_client_id_offset,
            protective.submit_client_id_offset,
        )

    def test_acceptance_uses_protective_submit_client_for_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = PaperAcceptancePolicyV1(
                environment="paper",
                account_id="DU000000",
                deployment_id="paper-drill-reverse-client-ownership",
                instrument_id="MNQ",
                drill_id="reverse-client-ownership-test",
                target_side="SHORT",
                command_ttl_seconds=600,
                position_max_age_seconds=30.0,
                entry_max_invocations=4,
                entry_poll_seconds=0.0,
                position_wait_seconds=1.0,
                position_poll_seconds=0.0,
                protective_max_invocations=4,
                protective_poll_seconds=0.0,
                reconciliation_read_attempts=5,
                reconciliation_poll_seconds=0.0,
                commission_wait_seconds=0.0,
                submit_client_id_offset=120,
                protective_submit_client_id_offset=140,
                reconciliation_client_id_offset=100,
                paths=PaperAcceptancePathsV1(
                    repo_root=root,
                    decision_database=root / "decision.sqlite3",
                    execution_database=root / "execution.sqlite3",
                    position_feed_database=root / "position.sqlite3",
                    catalog_root=root / "catalog",
                ),
            )
            runner = PaperReverseAcceptanceRunner(
                policy=policy,
                entry_summary=root / "entry-summary.json",
                command_executor=object(),
                state_source=object(),
                artifacts=object(),
                handoff_max_invocations=1,
                handoff_poll_seconds=0.0,
            )
            arguments = runner._handoff_arguments("strategy_command_test")
            index = arguments.index("--cancel-client-id-offset")
            self.assertEqual(
                arguments[index + 1],
                str(policy.protective_submit_client_id_offset),
            )


if __name__ == "__main__":
    unittest.main()
''',
    encoding="utf-8",
)

print("Patched reverse cancellation client ownership and added regression test.")
