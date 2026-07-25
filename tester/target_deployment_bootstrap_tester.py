from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ibmd.operations.bootstrap import (
    TargetBootstrapError,
    TargetDeploymentBootstrapper,
    load_target_bootstrap_manifest,
)

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_MANIFEST = ROOT / "bootstrap" / "target.v1.json"


class TargetBootstrapManifestTest(unittest.TestCase):
    def test_manifest_defines_only_target_stores(self) -> None:
        manifest = load_target_bootstrap_manifest(BOOTSTRAP_MANIFEST)
        self.assertEqual(manifest.bootstrap_name, "IBMarketData.target")
        self.assertEqual(manifest.bootstrap_version, 1)
        self.assertEqual(
            tuple(item.store_name for item in manifest.stores),
            (
                "market_data",
                "position_feed",
                "signal",
                "decision",
                "execution",
            ),
        )
        execution = next(
            item for item in manifest.stores if item.store_name == "execution"
        )
        self.assertEqual(len(execution.component_manifests), 4)
        self.assertNotIn("legacy", json.dumps(manifest.to_dict()).lower())

    def test_invalid_store_set_is_rejected(self) -> None:
        raw = json.loads(BOOTSTRAP_MANIFEST.read_text(encoding="utf-8"))
        raw["stores"] = raw["stores"][:-1]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(TargetBootstrapError, "store set"):
                load_target_bootstrap_manifest(path)


class TargetDeploymentBootstrapperTest(unittest.TestCase):
    def bootstrapper(
        self,
        target: Path,
        *,
        source_root: Path = ROOT,
        manifest: Path = BOOTSTRAP_MANIFEST,
        require_production_sessions: bool = False,
    ) -> TargetDeploymentBootstrapper:
        return TargetDeploymentBootstrapper(
            source_root=source_root,
            bootstrap_manifest_path=manifest,
            target_root=target,
            application_version="bootstrap-test",
            require_production_sessions=require_production_sessions,
        )

    def test_plan_has_no_legacy_import_and_does_not_create_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "account1"
            plan = self.bootstrapper(target).plan()
            self.assertFalse(target.exists())
            self.assertFalse(plan["historical_price_import_included"])
            self.assertFalse(plan["legacy_database_compatibility_required"])
            self.assertEqual(len(plan["artifact_hashes"]), 11)
            self.assertEqual(
                {item["store_name"] for item in plan["stores"]},
                {
                    "market_data",
                    "position_feed",
                    "signal",
                    "decision",
                    "execution",
                },
            )

    def test_apply_is_atomic_and_creates_all_fresh_stores(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "account1"
            bootstrapper = self.bootstrapper(target)
            result = bootstrapper.apply()
            self.assertTrue(target.is_dir())
            self.assertEqual(result.target_root, str(target.resolve()))
            self.assertFalse(result.to_dict()["historical_price_import_included"])
            self.assertTrue((target / "catalog" / "sessions.v1.json").is_file())
            self.assertTrue((target / "runtime" / "bootstrap.json").is_file())

            expected_paths = {
                "market_data": target / "market_data" / "MNQ.sqlite3",
                "position_feed": (
                    target / "position_feed" / "broker_positions.sqlite3"
                ),
                "signal": target / "signal" / "signal.sqlite3",
                "decision": target / "decision" / "decision.sqlite3",
                "execution": target / "execution" / "execution.sqlite3",
            }
            self.assertEqual(
                {item.store_name for item in result.stores},
                set(expected_paths),
            )
            for path in expected_paths.values():
                self.assertTrue(path.is_file(), path)

            execution = expected_paths["execution"]
            connection = sqlite3.connect(str(execution))
            try:
                components = connection.execute(
                    "SELECT component_name FROM "
                    "execution_target_schema_components "
                    "ORDER BY component_name"
                ).fetchall()
                objects = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type IN ('table', 'view')"
                    ).fetchall()
                }
            finally:
                connection.close()
            self.assertEqual(
                tuple(row[0] for row in components),
                (
                    "execution_daily_risk",
                    "execution_liquidation",
                    "execution_protective_lifecycle",
                    "execution_reverse_finalization",
                ),
            )
            self.assertIn("public_daily_risk_calculations_v1", objects)
            self.assertIn("public_liquidation_operations_v1", objects)
            self.assertIn("public_protective_fills_v1", objects)
            self.assertIn("public_reverse_finalizations_v1", objects)
            forbidden = {
                name
                for name in objects
                if "legacy" in name.lower()
                or "trade_intent" in name.lower()
                or "job_data" in name.lower()
            }
            self.assertEqual(forbidden, set())

            validated = bootstrapper.validate_target()
            self.assertEqual(validated.bootstrap_hash, result.bootstrap_hash)
            self.assertEqual(
                validated.catalog_bundle_hash,
                result.catalog_bundle_hash,
            )
            self.assertEqual(
                validated.to_dict()["artifact_hashes"],
                result.to_dict()["artifact_hashes"],
            )

    def test_existing_target_root_is_never_reused_or_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "account1"
            target.mkdir()
            sentinel = target / "do-not-delete.txt"
            sentinel.write_text("user data", encoding="utf-8")
            with self.assertRaisesRegex(TargetBootstrapError, "already exists"):
                self.bootstrapper(target).apply()
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "user data")

    def test_unqualified_catalog_cannot_be_bootstrapped_for_production(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "account1"
            with self.assertRaisesRegex(
                TargetBootstrapError,
                "production-qualified",
            ):
                self.bootstrapper(
                    target,
                    require_production_sessions=True,
                ).apply()
            self.assertFalse(target.exists())

    def test_component_failure_discards_staging_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            source = temporary / "source"
            source.mkdir()
            shutil.copytree(ROOT / "catalog", source / "catalog")
            shutil.copytree(ROOT / "migrations", source / "migrations")
            shutil.copytree(ROOT / "bootstrap", source / "bootstrap")

            broken = source / "migrations" / "execution.daily_risk.v1.json"
            raw = json.loads(broken.read_text(encoding="utf-8"))
            raw["statements"] = ["CREATE TABLE broken ("]
            broken.write_text(json.dumps(raw), encoding="utf-8")

            target = temporary / "account1"
            with self.assertRaises(Exception):
                self.bootstrapper(
                    target,
                    source_root=source,
                    manifest=source / "bootstrap" / "target.v1.json",
                ).apply()
            self.assertFalse(target.exists())
            leftovers = list(temporary.glob(".account1.bootstrap-*"))
            self.assertEqual(leftovers, [])

    def test_validation_detects_manifest_artifact_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            source = temporary / "source"
            source.mkdir()
            shutil.copytree(ROOT / "catalog", source / "catalog")
            shutil.copytree(ROOT / "migrations", source / "migrations")
            shutil.copytree(ROOT / "bootstrap", source / "bootstrap")
            target = temporary / "account1"
            bootstrapper = self.bootstrapper(
                target,
                source_root=source,
                manifest=source / "bootstrap" / "target.v1.json",
            )
            bootstrapper.apply()
            manifest = source / "bootstrap" / "target.v1.json"
            manifest.write_text(
                manifest.read_text(encoding="utf-8") + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(TargetBootstrapError, "artifact hashes"):
                bootstrapper.validate_target()


if __name__ == "__main__":
    unittest.main()
