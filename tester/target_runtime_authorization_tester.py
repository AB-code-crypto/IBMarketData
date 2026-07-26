from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from apps.run_execution_authorized_runtime_v2 import (
    _forwarded_runtime_args,
    build_parser,
)
from apps.run_target_supervisor import _service_specs
from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.config import DeploymentSettings
from ibmd.operations.acceptance_manifest import (
    AcceptanceGate,
    build_target_acceptance_manifest,
)
from ibmd.operations.bootstrap import TargetDeploymentBootstrapper
from ibmd.operations.cutover_preflight import (
    CutoverMode,
    TargetRuntimeAuthorizationV1,
)
from ibmd.operations.runtime_authorization import (
    RuntimeAuthorizationError,
    verify_runtime_start_authorization,
)
from tester.target_cutover_preflight_tester import summary

ROOT = Path(__file__).resolve().parents[1]
T0 = "2026-07-27T10:00:00Z"
T1 = "2026-07-27T10:01:00Z"
T2 = "2026-07-27T11:00:00Z"
ACCOUNT = "DU000000"
DEPLOYMENT = "paper-soak-authorization-test"
APPLICATION = "authorization-test"


def settings(root: Path) -> DeploymentSettings:
    return DeploymentSettings.from_mapping(
        {
            "IBMD_ENVIRONMENT": "paper",
            "IBMD_DEPLOYMENT_ID": DEPLOYMENT,
            "IBMD_DATA_ROOT": str(root),
            "IBMD_APPLICATION_VERSION": APPLICATION,
            "IB_HOST": "127.0.0.1",
            "IB_PORT": "7497",
            "IB_CLIENT_ID": "200",
            "IB_ACCOUNT_ID": ACCOUNT,
        }
    )


def build_target(root: Path):
    deployment = settings(root)
    bootstrap = TargetDeploymentBootstrapper(
        source_root=ROOT,
        bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
        target_root=root,
        application_version=APPLICATION,
        require_production_sessions=False,
    ).apply()
    evidence_root = root / "runtime" / "acceptance" / "evidence"
    evidence_root.mkdir(parents=True, exist_ok=True)
    summaries = {}
    for gate in AcceptanceGate:
        path = evidence_root / f"{gate.value.lower()}.summary.json"
        atomic_write_json(path, summary(gate))
        summaries[gate] = path
    manifest = build_target_acceptance_manifest(
        settings=deployment,
        summaries=summaries,
        created_at_utc=T0,
    )
    manifest_path = root / "runtime" / "acceptance" / "manifest.json"
    atomic_write_json(manifest_path, manifest.to_dict())
    authorization = TargetRuntimeAuthorizationV1(
        authorization_id="runtime_authorization_test",
        mode=CutoverMode.PAPER_SOAK,
        environment="paper",
        account_id=ACCOUNT,
        deployment_id=DEPLOYMENT,
        application_version=APPLICATION,
        data_root=str(root),
        acceptance_manifest_hash=manifest.content_hash,
        bootstrap_hash=bootstrap.bootstrap_hash,
        catalog_bundle_hash=bootstrap.catalog_bundle_hash,
        issued_at_utc=T0,
        expires_at_utc=T2,
        allow_unqualified_session=True,
    )
    authorization_path = root / "runtime" / "authorization.json"
    atomic_write_json(authorization_path, authorization.to_dict())
    return (
        deployment,
        bootstrap,
        manifest,
        manifest_path,
        authorization,
        authorization_path,
        summaries,
    )


class RuntimeAuthorizationVerificationTest(unittest.TestCase):
    def test_valid_fresh_target_authorization_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "target"
            (
                deployment,
                bootstrap,
                manifest,
                manifest_path,
                authorization,
                authorization_path,
                _summaries,
            ) = build_target(root)
            proof = verify_runtime_start_authorization(
                settings=deployment,
                source_root=ROOT,
                authorization_path=authorization_path,
                acceptance_manifest_path=manifest_path,
                bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
                catalog_root=root / "catalog",
                observed_at_utc=T1,
            )
            self.assertEqual(
                proof.authorization.content_hash,
                authorization.content_hash,
            )
            self.assertEqual(proof.acceptance_gate_count, len(AcceptanceGate))
            self.assertEqual(
                proof.authorization.acceptance_manifest_hash,
                manifest.content_hash,
            )
            self.assertEqual(
                proof.authorization.bootstrap_hash,
                bootstrap.bootstrap_hash,
            )
            self.assertFalse(proof.session_production_qualified)
            payload = proof.to_dict()
            self.assertTrue(payload["continuous_broker_mutations_authorized"])
            self.assertFalse(
                payload["continuous_broker_mutation_adapters_enabled"]
            )
            self.assertFalse(payload["live_account_enablement"])

    def test_tampered_acceptance_summary_blocks_startup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "target"
            (
                deployment,
                _bootstrap,
                _manifest,
                manifest_path,
                _authorization,
                authorization_path,
                summaries,
            ) = build_target(root)
            value = summary(AcceptanceGate.ENTRY_PROTECTION)
            value["entry_submission_count"] = 2
            atomic_write_json(
                summaries[AcceptanceGate.ENTRY_PROTECTION],
                value,
            )
            with self.assertRaisesRegex(
                RuntimeAuthorizationError,
                "acceptance evidence changed",
            ):
                verify_runtime_start_authorization(
                    settings=deployment,
                    source_root=ROOT,
                    authorization_path=authorization_path,
                    acceptance_manifest_path=manifest_path,
                    bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
                    catalog_root=root / "catalog",
                    observed_at_utc=T1,
                )

    def test_expired_authorization_blocks_startup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "target"
            (
                deployment,
                _bootstrap,
                _manifest,
                manifest_path,
                _authorization,
                authorization_path,
                _summaries,
            ) = build_target(root)
            with self.assertRaisesRegex(RuntimeAuthorizationError, "expired"):
                verify_runtime_start_authorization(
                    settings=deployment,
                    source_root=ROOT,
                    authorization_path=authorization_path,
                    acceptance_manifest_path=manifest_path,
                    bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
                    catalog_root=root / "catalog",
                    observed_at_utc=T2,
                )


class AuthorizedWrapperTest(unittest.TestCase):
    def test_wrapper_forwards_runtime_mode_and_target_catalog(self) -> None:
        parser = build_parser()
        arguments, remainder = parser.parse_known_args(
            [
                "--runtime-authorization",
                "authorization.json",
                "--continuous",
                "--poll-interval-seconds",
                "2",
            ]
        )
        forwarded = _forwarded_runtime_args(
            arguments,
            remainder,
            catalog_root=Path("/target/catalog"),
        )
        self.assertEqual(
            forwarded,
            [
                "--continuous",
                "--poll-interval-seconds",
                "2",
                "--catalog-root",
                "/target/catalog",
            ],
        )

    def test_validation_only_rejects_inner_runtime_arguments(self) -> None:
        parser = build_parser()
        arguments, remainder = parser.parse_known_args(
            [
                "--runtime-authorization",
                "authorization.json",
                "--validate-authorization-only",
                "--once",
            ]
        )
        with self.assertRaisesRegex(
            RuntimeAuthorizationError,
            "forbidden",
        ):
            _forwarded_runtime_args(
                arguments,
                remainder,
                catalog_root=Path("/target/catalog"),
            )


class AuthorizedSupervisorPlanTest(unittest.TestCase):
    def test_authorized_plan_selects_wrapper_but_keeps_adapters_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "target"
            (
                deployment,
                _bootstrap,
                _manifest,
                manifest_path,
                _authorization,
                authorization_path,
                _summaries,
            ) = build_target(root)
            proof = verify_runtime_start_authorization(
                settings=deployment,
                source_root=ROOT,
                authorization_path=authorization_path,
                acceptance_manifest_path=manifest_path,
                bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
                catalog_root=root / "catalog",
                observed_at_utc=T1,
            )
            specs = _service_specs(
                data_root=root,
                environment="paper",
                authorization_path=authorization_path,
                authorization_proof=proof,
            )
            execution = next(
                item for item in specs if item.service_name == "execution"
            )
            self.assertTrue(
                execution.argv[1].endswith(
                    "apps/run_execution_authorized_runtime_v2.py"
                )
            )
            self.assertIn("--runtime-authorization", execution.argv)
            self.assertIn("--allow-unqualified-session", execution.argv)

    def test_read_only_plan_keeps_canonical_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            specs = _service_specs(
                data_root=root,
                environment="paper",
                authorization_path=None,
                authorization_proof=None,
            )
            execution = next(
                item for item in specs if item.service_name == "execution"
            )
            self.assertTrue(
                execution.argv[1].endswith("apps/run_execution_runtime_v2.py")
            )
            self.assertNotIn("--runtime-authorization", execution.argv)


if __name__ == "__main__":
    unittest.main()
