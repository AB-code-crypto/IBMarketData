from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.operations.architecture import verify_target_architecture


class TargetArchitectureBoundaryTest(unittest.TestCase):
    def test_current_target_tree_passes_strict_gateway_rules(self):
        self.assertEqual(verify_target_architecture(ROOT), [])

    def test_gateway_service_import_and_domain_gateway_import_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            gateway = root / "src/ibmd/ib_gateway"
            domain = root / "src/ibmd/signal/domain"
            execution = root / "src/ibmd/execution"
            gateway.mkdir(parents=True)
            domain.mkdir(parents=True)
            execution.mkdir(parents=True)

            (gateway / "bad.py").write_text(
                "from ibmd.execution.application import execute\n",
                encoding="utf-8",
            )
            (domain / "bad.py").write_text(
                "from ibmd.ib_gateway.ib_async_positions import "
                "IBAsyncPositionReader\n",
                encoding="utf-8",
            )
            (execution / "application.py").write_text(
                "def execute():\n    return None\n",
                encoding="utf-8",
            )

            violations = verify_target_architecture(root)
            rule_ids = {item.rule_id for item in violations}
            self.assertIn("TGT-202", rule_ids)
            self.assertIn("TGT-301", rule_ids)


if __name__ == "__main__":
    unittest.main()
