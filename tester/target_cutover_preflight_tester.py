from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.config import DeploymentSettings
from ibmd.foundation.process_lock import ServiceProcessLock
from ibmd.operations.acceptance_manifest import (
    AcceptanceGate,
    TargetAcceptanceError,
    TargetAcceptanceManifestV1,
    build_target_acceptance_manifest,
    validate_acceptance_summary,
    verify_acceptance_manifest,
)
from ibmd.operations.cutover_preflight import (
    CutoverMode,
    CutoverPreflightError,
    TargetRuntimeAuthorizationV1,
    _active_legacy_services,
    _target_lock_check,
    verify_runtime_authorization,
)

T0 = "2026-07-27T10:00:00Z"
T1 = "2026-07-27T10:05:00.000000Z"
ACCOUNT = "DU000000"
DEPLOYMENT = "paper-soak-account1"
APPLICATION = "cutover-test"
EPISODE = "position_episode_test"
LIQUIDATION = "liquidation_operation_test"
ATTEMPT = "liquidation_attempt_test"
TRIGGER = "liquidation_trigger_test"


def protected() -> dict:
    return {
        "position_episode_id": EPISODE,
        "position_proof": {"accepted": True},
        "protection": {
            "fully_live": True,
            "stop_state": "LIVE",
            "take_profit_state": "LIVE",
        },
        "live_position_left_protected": True,
    }


def closed() -> dict:
    return {
        "position_episode_id": EPISODE,
        "liquidation_operation_id": LIQUIDATION,
        "state": {"fully_closed": True},
        "flat_proof": {"accepted": True},
        "paper_account_left_flat": True,
        "manual_cleanup_required": False,
    }


def summary(gate: AcceptanceGate) -> dict:
    common = {
        "schema_version": 1,
        "automatic_retry_enabled": False,
    }
    if gate == AcceptanceGate.ENTRY_PROTECTION:
        return {
            **common,
            "schema_name": "PaperAcceptanceResult",
            "drill_id": "entry-test",
            "finished_at_utc": T1,
            **protected(),
            "entry_submission_count": 1,
            "stop_submission_count": 1,
            "take_profit_submission_count": 1,
            "manual_cleanup_required": True,
        }
    if gate == AcceptanceGate.LIQUIDATION:
        return {
            **common,
            "schema_name": "PaperLiquidationAcceptanceResult",
            "finished_at_utc": T1,
            **closed(),
            "market_close_submission_count": 1,
        }
    if gate == AcceptanceGate.RESTART:
        return {
            **common,
            "schema_name": "PaperRestartAcceptanceResult",
            "drill_id": "restart-test",
            "finished_at_utc": T1,
            **protected(),
            "restart_adoption_proven": True,
            "all_resume_submissions_false": True,
            "attempt_no": 1,
            "intentional_process_terminations": 3,
            "broker_mutation_count": 3,
            "manual_cleanup_required": True,
        }
    if gate == AcceptanceGate.LIQUIDATION_RESTART:
        return {
            **common,
            "schema_name": "PaperLiquidationRestartAcceptanceResult",
            "finished_at_utc": T1,
            **closed(),
            "restart_adoption_proven": True,
            "all_resume_mutations_false": True,
            "attempt_no": 1,
            "restart_actions": [
                "CANCEL_TAKE_PROFIT",
                "CANCEL_STOP",
                "SUBMIT_MARKET_CLOSE",
            ],
            "protective_cancel_mode": "EXPLICIT_BOTH",
            "intentional_process_terminations": 3,
            "broker_mutation_count": 3,
        }
    if gate == AcceptanceGate.REVERSE:
        return {
            **common,
            "schema_name": "PaperReverseAcceptanceResult",
            "drill_id": "reverse-test",
            "finished_at_utc": T1,
            **protected(),
            "source_position_episode_id": "position_episode_source",
            "target_position_episode_id": EPISODE,
            "reverse_submission_count": 1,
            "reverse_order_quantity": 2,
            "allocations": [
                {"close_quantity": 1, "open_quantity": 1}
            ],
            "manual_cleanup_required": True,
        }
    if gate == AcceptanceGate.DAILY_HALT:
        return {
            **common,
            "schema_name": "PaperDailyHaltAcceptanceResult",
            "drill_id": "daily-halt-test",
            "scenario": "DAILY_HALT",
            "synthetic_market_mark_only": True,
            "real_owned_fill_evidence_only": True,
            "daily_halt_sticky": True,
            "cleanup_status_complete": True,
            "command_intake_enabled": False,
            "position_episode_id": EPISODE,
            "liquidation_operation_id": LIQUIDATION,
            "liquidation_state": {"fully_closed": True},
            "flat_proof": {"accepted": True},
            "paper_account_left_flat": True,
            "manual_cleanup_required": False,
            "synthetic_trigger": {
                "triggered_calculation": {"calculated_at_utc": T1}
            },
            "final_daily_risk_state": {
                "status": "HALTED",
                "cleanup_status": "COMPLETE",
            },
            "final_execution_readiness": {
                "command_intake_enabled": False
            },
        }
    if gate in {AcceptanceGate.DAILY_FLAT, AcceptanceGate.ROLLOVER}:
        scenario = gate.value
        return {
            **common,
            "schema_name": "PaperPolicyLiquidationAcceptanceResult",
            "scenario": scenario,
            "finished_at_utc": T1,
            **closed(),
            "policy_trigger_proven": True,
            "blocked_reasons": [],
            "trigger_candidate_reasons": [scenario],
            "trigger_source_ref": f"{scenario.lower()}:test",
            "trigger_id": TRIGGER + "_" + scenario.lower(),
        }
    raise AssertionError(gate)


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


def write_summaries(root: Path) -> dict[AcceptanceGate, Path]:
    values = {}
    evidence = root / "runtime" / "acceptance" / "evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    for gate in AcceptanceGate:
        path = evidence / f"{gate.value.lower()}.summary.json"
        atomic_write_json(path, summary(gate))
        values[gate] = path
    return values


class AcceptanceManifestTest(unittest.TestCase):
    def test_every_gate_validator_accepts_exact_success_contract(self) -> None:
        for gate in AcceptanceGate:
            with self.subTest(gate=gate):
                finished, primary_id, facts = validate_acceptance_summary(
                    gate,
                    summary(gate),
                )
                self.assertEqual(finished, T1)
                self.assertTrue(primary_id)
                self.assertTrue(facts)

    def test_daily_halt_uses_liquidation_state_contract(self) -> None:
        value = summary(AcceptanceGate.DAILY_HALT)
        self.assertNotIn("state", value)
        self.assertTrue(value["liquidation_state"]["fully_closed"])

        _finished, _primary_id, facts = validate_acceptance_summary(
            AcceptanceGate.DAILY_HALT,
            value,
        )
        self.assertTrue(facts["fully_closed"])
        self.assertTrue(facts["flat_proof_accepted"])

        invalid = dict(value)
        invalid.pop("liquidation_state")
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "liquidation_state must be a JSON object",
        ):
            validate_acceptance_summary(
                AcceptanceGate.DAILY_HALT,
                invalid,
            )

    def test_rollover_accepts_only_daily_flat_qualification_blocker(self) -> None:
        value = summary(AcceptanceGate.ROLLOVER)
        value["blocked_reasons"] = [
            "daily_flat_session_not_production_qualified:CME_EQUITY_INDEX"
        ]
        _finished, _primary_id, facts = validate_acceptance_summary(
            AcceptanceGate.ROLLOVER,
            value,
        )
        self.assertEqual(
            facts["blocked_reasons"],
            value["blocked_reasons"],
        )

        invalid_rollover = summary(AcceptanceGate.ROLLOVER)
        invalid_rollover["blocked_reasons"] = [
            "rollover_contract_still_active:MNQU6"
        ]
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "ROLLOVER contains unexpected blockers",
        ):
            validate_acceptance_summary(
                AcceptanceGate.ROLLOVER,
                invalid_rollover,
            )

        invalid_daily_flat = summary(AcceptanceGate.DAILY_FLAT)
        invalid_daily_flat["blocked_reasons"] = [
            "daily_flat_session_not_production_qualified:CME_EQUITY_INDEX"
        ]
        with self.assertRaisesRegex(
            TargetAcceptanceError,
            "DAILY_FLAT blocked_reasons must be empty",
        ):
            validate_acceptance_summary(
                AcceptanceGate.DAILY_FLAT,
                invalid_daily_flat,
            )

    def test_manifest_roundtrip_and_tamper_detection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            deployment = settings(root)
            summaries = write_summaries(root)
            manifest = build_target_acceptance_manifest(
                settings=deployment,
                summaries=summaries,
                created_at_utc=T0,
            )
            restored = TargetAcceptanceManifestV1.from_dict(manifest.to_dict())
            self.assertEqual(restored, manifest)
            verified = verify_acceptance_manifest(
                restored,
                settings=deployment,
            )
            self.assertEqual(len(verified), len(AcceptanceGate))
            payload = summary(AcceptanceGate.ENTRY_PROTECTION)
            payload["entry_submission_count"] = 2
            atomic_write_json(
                summaries[AcceptanceGate.ENTRY_PROTECTION],
                payload,
            )
            with self.assertRaises(TargetAcceptanceError):
                verify_acceptance_manifest(restored, settings=deployment)

    def test_wrong_scope_and_missing_gate_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            deployment = settings(root)
            summaries = write_summaries(root)
            missing = dict(summaries)
            missing.pop(AcceptanceGate.ROLLOVER)
            with self.assertRaisesRegex(TargetAcceptanceError, "missing=.*ROLLOVER"):
                build_target_acceptance_manifest(
                    settings=deployment,
                    summaries=missing,
                    created_at_utc=T0,
                )


class CutoverSafetyTest(unittest.TestCase):
    def test_target_lock_conflict_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            deployment = settings(Path(temporary).resolve())
            lock = ServiceProcessLock(
                deployment.paths_for("execution").lock_file,
                service_name="execution",
                deployment_id=deployment.deployment_id,
            )
            with lock:
                free, conflicts = _target_lock_check(deployment)
            self.assertFalse(free)
            self.assertEqual(conflicts[0]["service"], "execution")

    def test_live_legacy_status_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary)
            (runtime / "execution.json").write_text(
                json.dumps(
                    {
                        "service_key": "execution",
                        "script": "run_execution.py",
                        "state": "running",
                        "pid": os.getpid(),
                    }
                ),
                encoding="utf-8",
            )
            active = _active_legacy_services(runtime)
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0]["service_key"], "execution")

    def test_authorization_roundtrip_scope_and_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            deployment = settings(root)
            manifest = build_target_acceptance_manifest(
                settings=deployment,
                summaries=write_summaries(root),
                created_at_utc=T0,
            )
            authorization = TargetRuntimeAuthorizationV1(
                authorization_id="runtime_authorization_test",
                mode=CutoverMode.PAPER_SOAK,
                environment="paper",
                account_id=ACCOUNT,
                deployment_id=DEPLOYMENT,
                application_version=APPLICATION,
                data_root=str(root),
                acceptance_manifest_hash=manifest.content_hash,
                bootstrap_hash="a" * 64,
                catalog_bundle_hash="b" * 64,
                issued_at_utc=T0,
                expires_at_utc=T1,
                allow_unqualified_session=True,
            )
            restored = TargetRuntimeAuthorizationV1.from_dict(
                authorization.to_dict()
            )
            verify_runtime_authorization(
                restored,
                settings=deployment,
                acceptance_manifest=manifest,
                bootstrap_hash="a" * 64,
                catalog_bundle_hash="b" * 64,
                observed_at_utc="2026-07-27T10:01:00Z",
            )
            with self.assertRaisesRegex(
                CutoverPreflightError,
                "expired",
            ):
                verify_runtime_authorization(
                    restored,
                    settings=deployment,
                    acceptance_manifest=manifest,
                    bootstrap_hash="a" * 64,
                    catalog_bundle_hash="b" * 64,
                    observed_at_utc=T1,
                )


if __name__ == "__main__":
    unittest.main()
