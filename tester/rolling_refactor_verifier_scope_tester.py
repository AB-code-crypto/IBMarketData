from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.verify_rolling_refactor import (
    is_legacy_scan_path,
    iter_source_files,
    verify_forbidden_text,
    verify_python_syntax,
)


class RollingRefactorVerifierScopeTest(unittest.TestCase):
    def test_target_only_trees_are_not_legacy_scanned(self) -> None:
        skipped = (
            Path("src/ibmd/example.py"),
            Path("apps/run_example.py"),
            Path("bootstrap/target.json"),
            Path("catalog/policy.json"),
            Path("docs/target-runbook.md"),
            Path("migrations/execution.json"),
            Path("scripts/target_tool.py"),
            Path("tester/target_example_tester.py"),
            Path(".github/workflows/target.yml"),
        )
        for relative in skipped:
            with self.subTest(relative=relative):
                self.assertFalse(is_legacy_scan_path(relative))

    def test_legacy_runtime_and_legacy_tests_remain_scanned(self) -> None:
        included = (
            Path("ib_signal/signal_config.py"),
            Path("ib_execution/execution_loop.py"),
            Path("core/price_source.py"),
            Path("run_signal.py"),
            Path("tester/rolling_signal_smoke_tester.py"),
        )
        for relative in included:
            with self.subTest(relative=relative):
                self.assertTrue(is_legacy_scan_path(relative))

    def test_forbidden_target_terms_do_not_hide_legacy_violation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = {
                "src/ibmd/target.py": "mid_open = 1\n",
                "apps/target.py": "job_data = 1\n",
                "scripts/target.py": "spread_close = 1\n",
                "tester/target_example_tester.py": "slot_loss_extension = 1\n",
                "ib_signal/legacy.py": "candidate_funnel_store = 1\n",
            }
            for relative, text in values.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            errors: list[str] = []
            verify_forbidden_text(root, errors)
            self.assertEqual(
                errors,
                [
                    "forbidden token 'candidate_funnel_store' in "
                    "ib_signal/legacy.py"
                ],
            )
            scanned = {
                path.relative_to(root).as_posix()
                for path in iter_source_files(root)
            }
            self.assertEqual(scanned, {"ib_signal/legacy.py"})

    def test_target_syntax_is_owned_by_target_ci_but_legacy_syntax_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "src" / "ibmd" / "broken.py"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("def broken(:\n", encoding="utf-8")
            legacy = root / "ib_signal" / "broken.py"
            legacy.parent.mkdir(parents=True, exist_ok=True)
            legacy.write_text("def broken(:\n", encoding="utf-8")
            errors: list[str] = []
            verify_python_syntax(root, errors)
            self.assertEqual(len(errors), 1)
            self.assertIn("ib_signal/broken.py", errors[0])
            self.assertNotIn("src/ibmd/broken.py", errors[0])


if __name__ == "__main__":
    unittest.main()
