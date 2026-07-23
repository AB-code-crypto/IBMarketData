from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import JsonDataError, atomic_write_json, read_json_object
from ibmd.foundation.config import ConfigurationError, DeploymentSettings
from ibmd.foundation.identity import new_id, validate_id
from ibmd.foundation.process_lock import (
    ServiceAlreadyRunningError,
    ServiceProcessLock,
    read_process_lock_metadata,
)
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.operations.architecture import verify_target_architecture
from ibmd.operations.health import ServiceHealthFile
from ibmd.operations.migrations import (
    MigrationChecksumMismatch,
    MigrationError,
    SQLiteMigrationRunner,
    SqlMigration,
    load_migration_manifest,
)
from ibmd.public_contracts import (
    ContractEnvelopeV1,
    DependencyStatusV1,
    Liveness,
    Readiness,
    ServiceHealthV1,
)


class TargetImportAndConfigTest(unittest.TestCase):
    @staticmethod
    def mapping(**overrides: str) -> dict[str, str]:
        value = {
            "IBMD_DEPLOYMENT_ID": "paper-mnq-01",
            "IBMD_DATA_ROOT": "var/ibmd",
            "IBMD_APPLICATION_VERSION": "0.1.0-dev",
            "IB_HOST": "127.0.0.1",
            "IB_PORT": "7497",
            "IB_CLIENT_ID": "200",
            "IB_ACCOUNT_ID": "U0000000",
        }
        value.update(overrides)
        return value

    def test_target_package_imports_without_production_environment(self):
        environment = dict(os.environ)
        for key in tuple(environment):
            if key.startswith("IBMD_") or key in {
                "IB_HOST",
                "IB_PORT",
                "IB_CLIENT_ID",
                "IB_ACCOUNT_ID",
                "TELEGRAM_BOT_TOKEN",
                "TELEGRAM_CHAT_ID",
            }:
                environment.pop(key, None)
        environment["PYTHONPATH"] = str(SRC)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import ibmd; "
                    "import ibmd.foundation.config; "
                    "import ibmd.public_contracts.health; "
                    "print(ibmd.__version__)"
                ),
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("0.1.0-dev", result.stdout)

    def test_typed_config_and_paths_are_deterministic(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = DeploymentSettings.from_mapping(
                self.mapping(),
                base_dir=temp_dir,
            )
            same = DeploymentSettings.from_mapping(
                dict(reversed(list(self.mapping().items()))),
                base_dir=temp_dir,
            )
            self.assertEqual(settings.strategy_id, "IBMarketData.rolling")
            self.assertEqual(settings.strategy_version, 1)
            self.assertEqual(settings.environment, "paper")
            self.assertEqual(settings.data_root, (Path(temp_dir) / "var/ibmd").resolve())
            self.assertEqual(settings.configuration_hash, same.configuration_hash)
            self.assertEqual(len(settings.configuration_hash), 64)

            paths = settings.paths_for("execution")
            self.assertEqual(paths.data_dir, settings.data_root / "execution")
            self.assertEqual(
                paths.health_file,
                settings.data_root / "runtime/health/execution.json",
            )
            self.assertEqual(
                paths.lock_file,
                settings.data_root / "runtime/locks/execution.lock",
            )

            changed = DeploymentSettings.from_mapping(
                self.mapping(IB_PORT="4002"),
                base_dir=temp_dir,
            )
            self.assertNotEqual(settings.configuration_hash, changed.configuration_hash)

    def test_config_rejects_missing_or_invalid_values(self):
        with self.assertRaisesRegex(ConfigurationError, "IBMD_DEPLOYMENT_ID"):
            DeploymentSettings.from_mapping(
                {key: value for key, value in self.mapping().items() if key != "IBMD_DEPLOYMENT_ID"}
            )
        with self.assertRaisesRegex(ConfigurationError, "IB_PORT"):
            DeploymentSettings.from_mapping(self.mapping(IB_PORT="70000"))
        with self.assertRaisesRegex(ConfigurationError, "IB_CLIENT_ID"):
            DeploymentSettings.from_mapping(self.mapping(IB_CLIENT_ID="-1"))
        settings = DeploymentSettings.from_mapping(self.mapping())
        with self.assertRaisesRegex(ConfigurationError, "service_name"):
            settings.paths_for("Execution Service")

    def test_stable_identity_validation(self):
        value = new_id("operation")
        self.assertEqual(validate_id(value, expected_kind="operation"), value)
        with self.assertRaises(ValueError):
            validate_id(value, expected_kind="attempt")


class ContractAndHealthTest(unittest.TestCase):
    def test_contract_envelope_round_trip_is_strict(self):
        envelope = ContractEnvelopeV1.create(
            schema_name="StrategyCommandRequest",
            producer_version="0.1.0-dev",
            correlation_id="decision_01",
            created_at_utc="2026-07-23T10:00:00.000000Z",
            payload={"command_id": "command_01", "quantity": 1},
        )
        restored = ContractEnvelopeV1.from_dict(envelope.to_dict())
        self.assertEqual(restored, envelope)
        invalid = envelope.to_dict()
        invalid["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unknown"):
            ContractEnvelopeV1.from_dict(invalid)

    def test_utc_helpers_reject_naive_values(self):
        parsed = parse_utc("2026-07-23T10:00:00Z")
        self.assertEqual(format_utc(parsed), "2026-07-23T10:00:00.000000Z")
        with self.assertRaises(ValueError):
            parse_utc("2026-07-23 10:00:00")

    def test_service_health_atomic_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runtime/health/execution.json"
            health = ServiceHealthV1.starting(
                service="execution",
                deployment_id="paper-mnq-01",
                instance_id="instance_01",
                pid=1234,
                application_version="0.1.0-dev",
                configuration_hash="a" * 64,
                now_utc="2026-07-23T10:00:00.000000Z",
            ).heartbeat(
                now_utc="2026-07-23T10:00:01.000000Z",
                liveness=Liveness.RUNNING,
                readiness=Readiness.DEGRADED,
                last_success_at_utc="2026-07-23T10:00:00.500000Z",
                source_freshness_seconds=1.25,
                dependency_status=(
                    DependencyStatusV1(
                        name="broker",
                        status="CONNECTED",
                        observed_at_utc="2026-07-23T10:00:01.000000Z",
                    ),
                ),
                blocking_reason="reconciliation pending",
            )
            file = ServiceHealthFile(path, expected_service="execution")
            file.publish(health)
            self.assertEqual(file.read(), health)
            self.assertEqual(
                list(path.parent.glob(f".{path.name}.*.tmp")),
                [],
            )
            payload = read_json_object(path)
            self.assertEqual(payload["schema_name"], "ServiceHealth")
            self.assertEqual(payload["readiness"], "DEGRADED")

            payload["unknown"] = "bad"
            atomic_write_json(path, payload)
            with self.assertRaisesRegex(ValueError, "unknown"):
                file.read()

    def test_atomic_json_rejects_non_finite_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(JsonDataError):
                atomic_write_json(
                    Path(temp_dir) / "bad.json",
                    {"value": float("nan")},
                )


class ProcessLockTest(unittest.TestCase):
    def test_service_owned_lock_is_exclusive_and_metadata_is_visible(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "execution.lock"
            first = ServiceProcessLock(
                path,
                service_name="execution",
                deployment_id="paper-mnq-01",
                instance_id="instance_first",
            ).acquire()
            try:
                metadata = read_process_lock_metadata(path)
                self.assertIsNotNone(metadata)
                self.assertEqual(metadata.service_name, "execution")
                self.assertEqual(metadata.deployment_id, "paper-mnq-01")
                self.assertEqual(metadata.instance_id, "instance_first")
                self.assertEqual(metadata.pid, os.getpid())

                second = ServiceProcessLock(
                    path,
                    service_name="execution",
                    deployment_id="paper-mnq-01",
                    instance_id="instance_second",
                )
                with self.assertRaises(ServiceAlreadyRunningError) as context:
                    second.acquire()
                self.assertIsNotNone(context.exception.metadata)
                self.assertEqual(
                    context.exception.metadata.instance_id,
                    "instance_first",
                )
            finally:
                first.release()

            with ServiceProcessLock(
                path,
                service_name="execution",
                deployment_id="paper-mnq-01",
                instance_id="instance_third",
            ) as third:
                self.assertTrue(third.acquired)
                self.assertEqual(
                    read_process_lock_metadata(path).instance_id,
                    "instance_third",
                )


class MigrationRunnerTest(unittest.TestCase):
    @staticmethod
    def migrations() -> tuple[SqlMigration, ...]:
        return (
            SqlMigration(
                version=1,
                name="create_items",
                statements=(
                    "CREATE TABLE items (item_id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                ),
            ),
            SqlMigration(
                version=2,
                name="add_created_at",
                statements=(
                    "ALTER TABLE items ADD COLUMN created_at_utc TEXT",
                ),
            ),
        )

    def runner(self, path: Path, migrations=None) -> SQLiteMigrationRunner:
        return SQLiteMigrationRunner(
            database_path=path,
            store_name="execution",
            migrations=self.migrations() if migrations is None else migrations,
            application_version="0.1.0-dev",
        )

    def test_dry_run_does_not_create_database_and_apply_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "execution.sqlite3"
            runner = self.runner(path)
            plan = runner.inspect()
            self.assertFalse(path.exists())
            self.assertEqual([item.version for item in plan.pending], [1, 2])

            applied = runner.apply()
            self.assertTrue(path.exists())
            self.assertTrue(applied.is_current)
            self.assertEqual(applied.current_version, 2)
            self.assertTrue(runner.apply().is_current)

            conn = sqlite3.connect(path)
            try:
                columns = [
                    row[1]
                    for row in conn.execute("PRAGMA table_info(items)").fetchall()
                ]
                ledger = conn.execute(
                    "SELECT version, name FROM schema_migrations ORDER BY version"
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(columns, ["item_id", "value", "created_at_utc"])
            self.assertEqual(ledger, [(1, "create_items"), (2, "add_created_at")])

    def test_checksum_change_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "execution.sqlite3"
            self.runner(path).apply()
            changed = (
                SqlMigration(
                    version=1,
                    name="create_items",
                    statements=(
                        "CREATE TABLE items (item_id INTEGER PRIMARY KEY, value BLOB NOT NULL)",
                    ),
                ),
                self.migrations()[1],
            )
            with self.assertRaises(MigrationChecksumMismatch):
                self.runner(path, migrations=changed).inspect()

    def test_failed_migration_rolls_back_its_version(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "execution.sqlite3"
            migrations = (
                self.migrations()[0],
                SqlMigration(
                    version=2,
                    name="broken",
                    statements=(
                        "CREATE TABLE temporary_change (value TEXT)",
                        "INSERT INTO missing_table(value) VALUES ('fail')",
                    ),
                ),
            )
            with self.assertRaisesRegex(MigrationError, "rolled back"):
                self.runner(path, migrations=migrations).apply()

            conn = sqlite3.connect(path)
            try:
                temporary_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='temporary_change'"
                ).fetchone()
                versions = conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
            finally:
                conn.close()
            self.assertIsNone(temporary_exists)
            self.assertEqual(versions, [(1,)])

    def test_manifest_loader_and_cli_default_to_read_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "migrations.json"
            database = root / "execution.sqlite3"
            manifest.write_text(
                json.dumps(
                    {
                        "store_name": "execution",
                        "migrations": [
                            {
                                "version": 1,
                                "name": "create_example",
                                "statements": [
                                    "CREATE TABLE example (example_id INTEGER PRIMARY KEY)"
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            store, migrations = load_migration_manifest(manifest)
            self.assertEqual(store, "execution")
            self.assertEqual(migrations[0].version, 1)

            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/run_target_migrations.py"),
                    "--manifest",
                    str(manifest),
                    "--database",
                    str(database),
                    "--application-version",
                    "0.1.0-dev",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(database.exists())
            self.assertEqual(json.loads(result.stdout)["pending_versions"], [1])


class TargetArchitectureVerifierTest(unittest.TestCase):
    def test_current_target_tree_passes(self):
        self.assertEqual(verify_target_architecture(ROOT), [])

    def test_forbidden_imports_and_module_scope_environment_are_detected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            domain = root / "src/ibmd/signal/domain"
            other = root / "src/ibmd/decision"
            domain.mkdir(parents=True)
            other.mkdir(parents=True)
            (domain / "bad.py").write_text(
                "import sqlite3\n"
                "from ib_execution.execution_store import get_trade_db_connection\n"
                "from ibmd.decision.application import decide\n"
                "import os\n"
                "ACCOUNT = os.environ['IB_ACCOUNT_ID']\n",
                encoding="utf-8",
            )
            (other / "application.py").write_text(
                "def decide():\n    return None\n",
                encoding="utf-8",
            )
            violations = verify_target_architecture(root)
            rule_ids = {violation.rule_id for violation in violations}
            self.assertTrue({"TGT-100", "TGT-200", "TGT-300", "TGT-400"} <= rule_ids)


if __name__ == "__main__":
    unittest.main()
