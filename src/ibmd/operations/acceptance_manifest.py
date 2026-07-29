from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from ibmd.foundation.atomic_json import (
    canonical_json_text,
    read_json_object,
)
from ibmd.foundation.config import DeploymentSettings
from ibmd.foundation.time import format_utc, parse_utc


class TargetAcceptanceError(RuntimeError):
    pass


class AcceptanceGate(str, Enum):
    ENTRY_PROTECTION = "ENTRY_PROTECTION"
    LIQUIDATION = "LIQUIDATION"
    RESTART = "RESTART"
    LIQUIDATION_RESTART = "LIQUIDATION_RESTART"
    REVERSE = "REVERSE"
    DAILY_HALT = "DAILY_HALT"
    DAILY_FLAT = "DAILY_FLAT"
    ROLLOVER = "ROLLOVER"


REQUIRED_PAPER_SOAK_GATES = tuple(AcceptanceGate)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_payload(value: object) -> str:
    return hashlib.sha256(
        canonical_json_text(value).encode("utf-8")
    ).hexdigest()


def _mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TargetAcceptanceError(f"{field_name} must be a JSON object")
    return value


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TargetAcceptanceError(f"{field_name} is required")
    return text


def _required_true(value: object, *, field_name: str) -> None:
    if value is not True:
        raise TargetAcceptanceError(f"{field_name} must be true")


def _required_false(value: object, *, field_name: str) -> None:
    if value is not False:
        raise TargetAcceptanceError(f"{field_name} must be false")


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TargetAcceptanceError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise TargetAcceptanceError(f"{field_name} must be an integer") from exc
    if parsed <= 0 or float(parsed) != exact:
        raise TargetAcceptanceError(f"{field_name} must be a positive integer")
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TargetAcceptanceError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise TargetAcceptanceError(f"{field_name} must be an integer") from exc
    if parsed < 0 or float(parsed) != exact:
        raise TargetAcceptanceError(f"{field_name} must be a non-negative integer")
    return parsed


def _schema(
    value: Mapping[str, Any],
    expected: str,
) -> None:
    if (
        value.get("schema_name") != expected
        or int(value.get("schema_version") or 0) != 1
    ):
        raise TargetAcceptanceError(
            f"acceptance summary must be {expected} v1"
        )
    _required_false(
        value.get("automatic_retry_enabled"),
        field_name=f"{expected}.automatic_retry_enabled",
    )


def _finished_at(value: Mapping[str, Any]) -> str:
    raw = _required_text(
        value.get("finished_at_utc"),
        field_name="finished_at_utc",
    )
    return format_utc(parse_utc(raw))


def _protected_position_facts(value: Mapping[str, Any]) -> dict[str, Any]:
    proof = _mapping(value.get("position_proof"), field_name="position_proof")
    protection = _mapping(value.get("protection"), field_name="protection")
    _required_true(proof.get("accepted"), field_name="position_proof.accepted")
    _required_true(
        protection.get("fully_live"),
        field_name="protection.fully_live",
    )
    if protection.get("stop_state") != "LIVE":
        raise TargetAcceptanceError("protection.stop_state must be LIVE")
    take_profit = str(protection.get("take_profit_state") or "")
    if take_profit not in {"LIVE", "NOT_REQUIRED"}:
        raise TargetAcceptanceError(
            "protection.take_profit_state must be LIVE or NOT_REQUIRED"
        )
    _required_true(
        value.get("live_position_left_protected"),
        field_name="live_position_left_protected",
    )
    return {
        "position_episode_id": _required_text(
            value.get("position_episode_id"),
            field_name="position_episode_id",
        ),
        "stop_state": "LIVE",
        "take_profit_state": take_profit,
        "position_proof_accepted": True,
    }


def _closed_position_facts(value: Mapping[str, Any]) -> dict[str, Any]:
    state = _mapping(value.get("state"), field_name="state")
    proof = _mapping(value.get("flat_proof"), field_name="flat_proof")
    _required_true(state.get("fully_closed"), field_name="state.fully_closed")
    _required_true(proof.get("accepted"), field_name="flat_proof.accepted")
    _required_true(
        value.get("paper_account_left_flat"),
        field_name="paper_account_left_flat",
    )
    _required_false(
        value.get("manual_cleanup_required"),
        field_name="manual_cleanup_required",
    )
    return {
        "position_episode_id": _required_text(
            value.get("position_episode_id"),
            field_name="position_episode_id",
        ),
        "liquidation_operation_id": _required_text(
            value.get("liquidation_operation_id"),
            field_name="liquidation_operation_id",
        ),
        "flat_proof_accepted": True,
        "fully_closed": True,
    }


def _validate_entry(value: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperAcceptanceResult")
    facts = _protected_position_facts(value)
    if _positive_int(
        value.get("entry_submission_count"),
        field_name="entry_submission_count",
    ) != 1:
        raise TargetAcceptanceError("entry_submission_count must equal 1")
    if _positive_int(
        value.get("stop_submission_count"),
        field_name="stop_submission_count",
    ) != 1:
        raise TargetAcceptanceError("stop_submission_count must equal 1")
    expected_tp = 0 if facts["take_profit_state"] == "NOT_REQUIRED" else 1
    actual_tp = _non_negative_int(
        value.get("take_profit_submission_count"),
        field_name="take_profit_submission_count",
    )
    if actual_tp != expected_tp:
        raise TargetAcceptanceError(
            "take_profit_submission_count differs from protection policy"
        )
    _required_true(
        value.get("manual_cleanup_required"),
        field_name="manual_cleanup_required",
    )
    facts.update(
        {
            "entry_submission_count": 1,
            "stop_submission_count": 1,
            "take_profit_submission_count": actual_tp,
        }
    )
    return _finished_at(value), _required_text(value.get("drill_id"), field_name="drill_id"), facts


def _validate_liquidation(
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperLiquidationAcceptanceResult")
    facts = _closed_position_facts(value)
    explicit_market_close_count = _non_negative_int(
        value.get("market_close_submission_count"),
        field_name="market_close_submission_count",
    )
    durable_market_close_count = _positive_int(
        value.get(
            "durable_market_close_attempt_count",
            explicit_market_close_count,
        ),
        field_name="durable_market_close_attempt_count",
    )
    recovered = value.get("recovered_from_durable_state") is True
    if durable_market_close_count != 1:
        raise TargetAcceptanceError(
            "durable_market_close_attempt_count must equal 1"
        )
    if recovered:
        if explicit_market_close_count != 0:
            raise TargetAcceptanceError(
                "recovered liquidation must not report a new MARKET close"
            )
    elif explicit_market_close_count != 1:
        raise TargetAcceptanceError(
            "market_close_submission_count must equal 1"
        )
    facts.update(
        {
            "market_close_submission_count": explicit_market_close_count,
            "durable_market_close_attempt_count": 1,
            "recovered_from_durable_state": recovered,
        }
    )
    return (
        _finished_at(value),
        _required_text(
            value.get("liquidation_operation_id"),
            field_name="liquidation_operation_id",
        ),
        facts,
    )


def _validate_restart(
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperRestartAcceptanceResult")
    facts = _protected_position_facts(value)
    _required_true(
        value.get("restart_adoption_proven"),
        field_name="restart_adoption_proven",
    )
    _required_true(
        value.get("all_resume_submissions_false"),
        field_name="all_resume_submissions_false",
    )
    if _positive_int(value.get("attempt_no"), field_name="attempt_no") != 1:
        raise TargetAcceptanceError("restart attempt_no must equal 1")
    terminations = _positive_int(
        value.get("intentional_process_terminations"),
        field_name="intentional_process_terminations",
    )
    mutations = _positive_int(
        value.get("broker_mutation_count"),
        field_name="broker_mutation_count",
    )
    if terminations != mutations:
        raise TargetAcceptanceError(
            "restart broker mutation count must equal intentional terminations"
        )
    facts.update(
        {
            "restart_adoption_proven": True,
            "attempt_no": 1,
            "intentional_process_terminations": terminations,
        }
    )
    return _finished_at(value), _required_text(value.get("drill_id"), field_name="drill_id"), facts


def _validate_liquidation_restart(
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperLiquidationRestartAcceptanceResult")
    facts = _closed_position_facts(value)
    _required_true(
        value.get("restart_adoption_proven"),
        field_name="restart_adoption_proven",
    )
    _required_true(
        value.get("all_resume_mutations_false"),
        field_name="all_resume_mutations_false",
    )
    if _positive_int(value.get("attempt_no"), field_name="attempt_no") != 1:
        raise TargetAcceptanceError("liquidation restart attempt_no must equal 1")
    actions = value.get("restart_actions")
    if not isinstance(actions, list):
        raise TargetAcceptanceError(
            "liquidation restart actions must be a list"
        )
    mode = _required_text(
        value.get("protective_cancel_mode"),
        field_name="protective_cancel_mode",
    )
    expected_by_mode = {
        "EXPLICIT_BOTH": [
            "CANCEL_TAKE_PROFIT",
            "CANCEL_STOP",
            "SUBMIT_MARKET_CLOSE",
        ],
        "OCA_AUTO_CANCELLED_STOP": [
            "CANCEL_TAKE_PROFIT",
            "SUBMIT_MARKET_CLOSE",
        ],
        "STOP_ONLY": [
            "CANCEL_STOP",
            "SUBMIT_MARKET_CLOSE",
        ],
    }
    expected_actions = expected_by_mode.get(mode)
    if expected_actions is None or actions != expected_actions:
        raise TargetAcceptanceError(
            "liquidation restart actions differ from protective_cancel_mode"
        )
    terminations = _positive_int(
        value.get("intentional_process_terminations"),
        field_name="intentional_process_terminations",
    )
    mutations = _positive_int(
        value.get("broker_mutation_count"),
        field_name="broker_mutation_count",
    )
    if terminations != len(actions) or mutations != len(actions):
        raise TargetAcceptanceError(
            "liquidation restart mutation counts must equal restart actions"
        )
    facts.update(
        {
            "restart_adoption_proven": True,
            "attempt_no": 1,
            "restart_actions": list(actions),
            "protective_cancel_mode": mode,
            "intentional_process_terminations": terminations,
        }
    )
    return (
        _finished_at(value),
        _required_text(
            value.get("liquidation_operation_id"),
            field_name="liquidation_operation_id",
        ),
        facts,
    )


def _validate_reverse(
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperReverseAcceptanceResult")
    facts = _protected_position_facts(value)
    source_episode = _required_text(
        value.get("source_position_episode_id"),
        field_name="source_position_episode_id",
    )
    target_episode = _required_text(
        value.get("target_position_episode_id"),
        field_name="target_position_episode_id",
    )
    if source_episode == target_episode:
        raise TargetAcceptanceError("REVERSE source and target episodes must differ")
    if _positive_int(
        value.get("reverse_submission_count"),
        field_name="reverse_submission_count",
    ) != 1:
        raise TargetAcceptanceError("reverse_submission_count must equal 1")
    reverse_quantity = _positive_int(
        value.get("reverse_order_quantity"),
        field_name="reverse_order_quantity",
    )
    allocations = value.get("allocations")
    if not isinstance(allocations, list) or not allocations:
        raise TargetAcceptanceError("REVERSE allocations must be a non-empty list")
    close_quantity = 0
    open_quantity = 0
    for index, allocation in enumerate(allocations):
        item = _mapping(allocation, field_name=f"allocations[{index}]")
        close_quantity += _non_negative_int(
            item.get("close_quantity"),
            field_name=f"allocations[{index}].close_quantity",
        )
        open_quantity += _non_negative_int(
            item.get("open_quantity"),
            field_name=f"allocations[{index}].open_quantity",
        )
    if close_quantity <= 0 or open_quantity <= 0:
        raise TargetAcceptanceError(
            "REVERSE allocations must contain closing and opening quantity"
        )
    if close_quantity + open_quantity != reverse_quantity:
        raise TargetAcceptanceError(
            "REVERSE allocation quantity must equal reverse order quantity"
        )
    facts.update(
        {
            "source_position_episode_id": source_episode,
            "target_position_episode_id": target_episode,
            "reverse_order_quantity": reverse_quantity,
            "close_quantity": close_quantity,
            "open_quantity": open_quantity,
        }
    )
    return _finished_at(value), _required_text(value.get("drill_id"), field_name="drill_id"), facts


def _validate_daily_halt(
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperDailyHaltAcceptanceResult")
    if value.get("scenario") != "DAILY_HALT":
        raise TargetAcceptanceError("daily-halt scenario must be DAILY_HALT")
    _required_true(
        value.get("synthetic_market_mark_only"),
        field_name="synthetic_market_mark_only",
    )
    _required_true(
        value.get("real_owned_fill_evidence_only"),
        field_name="real_owned_fill_evidence_only",
    )
    _required_true(value.get("daily_halt_sticky"), field_name="daily_halt_sticky")
    _required_true(
        value.get("cleanup_status_complete"),
        field_name="cleanup_status_complete",
    )
    _required_false(
        value.get("command_intake_enabled"),
        field_name="command_intake_enabled",
    )
    facts = _closed_position_facts(value)
    state = _mapping(
        value.get("final_daily_risk_state"),
        field_name="final_daily_risk_state",
    )
    readiness = _mapping(
        value.get("final_execution_readiness"),
        field_name="final_execution_readiness",
    )
    if state.get("status") != "HALTED" or state.get("cleanup_status") != "COMPLETE":
        raise TargetAcceptanceError(
            "final daily-risk state must be HALTED / COMPLETE"
        )
    if readiness.get("command_intake_enabled") is not False:
        raise TargetAcceptanceError(
            "final execution readiness must keep command intake disabled"
        )
    facts.update(
        {
            "scenario": "DAILY_HALT",
            "daily_halt_sticky": True,
            "synthetic_market_mark_only": True,
        }
    )
    return (
        _finished_at_from_nested(value),
        _required_text(value.get("drill_id"), field_name="drill_id"),
        facts,
    )


def _finished_at_from_nested(value: Mapping[str, Any]) -> str:
    raw = value.get("finished_at_utc")
    if raw is None:
        synthetic = _mapping(value.get("synthetic_trigger"), field_name="synthetic_trigger")
        calculation = _mapping(
            synthetic.get("triggered_calculation"),
            field_name="synthetic_trigger.triggered_calculation",
        )
        raw = calculation.get("calculated_at_utc")
    return format_utc(
        parse_utc(_required_text(raw, field_name="finished_at_utc"))
    )


def _validate_policy(
    value: Mapping[str, Any],
    *,
    scenario: str,
) -> tuple[str, str, dict[str, Any]]:
    _schema(value, "PaperPolicyLiquidationAcceptanceResult")
    if value.get("scenario") != scenario:
        raise TargetAcceptanceError(
            f"policy liquidation scenario must be {scenario}"
        )
    _required_true(
        value.get("policy_trigger_proven"),
        field_name="policy_trigger_proven",
    )
    blocked = value.get("blocked_reasons")
    if (
        not isinstance(blocked, list)
        or any(
            not isinstance(item, str) or not item.strip()
            for item in blocked
        )
    ):
        raise TargetAcceptanceError(
            "policy liquidation blocked_reasons must be a list of non-empty strings"
        )
    if scenario == "DAILY_FLAT":
        if blocked:
            raise TargetAcceptanceError(
                "DAILY_FLAT blocked_reasons must be empty"
            )
    elif scenario == "ROLLOVER":
        allowed_prefix = "daily_flat_session_not_production_qualified:"
        unexpected = [
            item for item in blocked
            if not item.startswith(allowed_prefix)
        ]
        if unexpected:
            raise TargetAcceptanceError(
                "ROLLOVER contains unexpected blockers: "
                + repr(unexpected)
            )
    else:
        raise TargetAcceptanceError(
            f"unsupported policy liquidation scenario: {scenario}"
        )
    candidates = value.get("trigger_candidate_reasons")
    if not isinstance(candidates, list) or scenario not in candidates:
        raise TargetAcceptanceError(
            f"policy trigger candidates must contain {scenario}"
        )
    facts = _closed_position_facts(value)
    facts.update(
        {
            "scenario": scenario,
            "trigger_source_ref": _required_text(
                value.get("trigger_source_ref"),
                field_name="trigger_source_ref",
            ),
            "policy_trigger_proven": True,
            "blocked_reasons": list(blocked),
        }
    )
    return (
        _finished_at(value),
        _required_text(value.get("trigger_id"), field_name="trigger_id"),
        facts,
    )


def validate_acceptance_summary(
    gate: AcceptanceGate,
    value: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    if gate == AcceptanceGate.ENTRY_PROTECTION:
        return _validate_entry(value)
    if gate == AcceptanceGate.LIQUIDATION:
        return _validate_liquidation(value)
    if gate == AcceptanceGate.RESTART:
        return _validate_restart(value)
    if gate == AcceptanceGate.LIQUIDATION_RESTART:
        return _validate_liquidation_restart(value)
    if gate == AcceptanceGate.REVERSE:
        return _validate_reverse(value)
    if gate == AcceptanceGate.DAILY_HALT:
        return _validate_daily_halt(value)
    if gate == AcceptanceGate.DAILY_FLAT:
        return _validate_policy(value, scenario="DAILY_FLAT")
    if gate == AcceptanceGate.ROLLOVER:
        return _validate_policy(value, scenario="ROLLOVER")
    raise TargetAcceptanceError(f"unsupported acceptance gate: {gate}")


@dataclass(frozen=True)
class AcceptanceEvidenceV1:
    gate: AcceptanceGate
    summary_schema: str
    summary_version: int
    relative_summary_path: str
    summary_sha256: str
    finished_at_utc: str
    primary_id: str
    facts: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.gate, AcceptanceGate):
            raise TargetAcceptanceError("evidence gate must be AcceptanceGate")
        schema = _required_text(self.summary_schema, field_name="summary_schema")
        path = Path(_required_text(
            self.relative_summary_path,
            field_name="relative_summary_path",
        ))
        if path.is_absolute() or ".." in path.parts:
            raise TargetAcceptanceError(
                "relative_summary_path must be a safe relative path"
            )
        digest = str(self.summary_sha256 or "").strip().lower()
        if len(digest) != 64 or any(item not in "0123456789abcdef" for item in digest):
            raise TargetAcceptanceError("summary_sha256 must be lowercase SHA-256 hex")
        if int(self.summary_version) != 1:
            raise TargetAcceptanceError("summary_version must equal 1")
        if not isinstance(self.facts, Mapping):
            raise TargetAcceptanceError("evidence facts must be a mapping")
        object.__setattr__(self, "summary_schema", schema)
        object.__setattr__(self, "summary_version", 1)
        object.__setattr__(self, "relative_summary_path", path.as_posix())
        object.__setattr__(self, "summary_sha256", digest)
        object.__setattr__(
            self,
            "finished_at_utc",
            format_utc(parse_utc(self.finished_at_utc)),
        )
        object.__setattr__(
            self,
            "primary_id",
            _required_text(self.primary_id, field_name="primary_id"),
        )
        object.__setattr__(self, "facts", dict(self.facts))

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate.value,
            "summary_schema": self.summary_schema,
            "summary_version": self.summary_version,
            "relative_summary_path": self.relative_summary_path,
            "summary_sha256": self.summary_sha256,
            "finished_at_utc": self.finished_at_utc,
            "primary_id": self.primary_id,
            "facts": dict(self.facts),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AcceptanceEvidenceV1":
        expected = {
            "gate",
            "summary_schema",
            "summary_version",
            "relative_summary_path",
            "summary_sha256",
            "finished_at_utc",
            "primary_id",
            "facts",
        }
        if set(value) != expected:
            raise TargetAcceptanceError(
                "acceptance evidence fields mismatch: "
                f"missing={sorted(expected - set(value))}, "
                f"unknown={sorted(set(value) - expected)}"
            )
        facts = value["facts"]
        if not isinstance(facts, Mapping):
            raise TargetAcceptanceError("acceptance evidence facts must be an object")
        try:
            gate = AcceptanceGate(str(value["gate"]))
        except ValueError as exc:
            raise TargetAcceptanceError(
                f"unknown acceptance gate: {value['gate']!r}"
            ) from exc
        return cls(
            gate=gate,
            summary_schema=str(value["summary_schema"]),
            summary_version=int(value["summary_version"]),
            relative_summary_path=str(value["relative_summary_path"]),
            summary_sha256=str(value["summary_sha256"]),
            finished_at_utc=str(value["finished_at_utc"]),
            primary_id=str(value["primary_id"]),
            facts=dict(facts),
        )


@dataclass(frozen=True)
class TargetAcceptanceManifestV1:
    environment: str
    account_id: str
    deployment_id: str
    application_version: str
    data_root: str
    created_at_utc: str
    evidence: tuple[AcceptanceEvidenceV1, ...]

    SCHEMA_NAME = "TargetAcceptanceManifest"
    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        environment = _required_text(self.environment, field_name="environment").lower()
        if environment != "paper":
            raise TargetAcceptanceError(
                "target acceptance manifest currently supports paper only"
            )
        account = _required_text(self.account_id, field_name="account_id")
        if not account.upper().startswith("D"):
            raise TargetAcceptanceError(
                "target acceptance manifest requires an IB paper account"
            )
        root = Path(_required_text(self.data_root, field_name="data_root"))
        if not root.is_absolute():
            raise TargetAcceptanceError("manifest data_root must be absolute")
        evidence = tuple(self.evidence)
        gates = [item.gate for item in evidence]
        if len(gates) != len(set(gates)):
            raise TargetAcceptanceError("acceptance manifest gates must be unique")
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "account_id", account)
        object.__setattr__(
            self,
            "deployment_id",
            _required_text(self.deployment_id, field_name="deployment_id"),
        )
        object.__setattr__(
            self,
            "application_version",
            _required_text(
                self.application_version,
                field_name="application_version",
            ),
        )
        object.__setattr__(self, "data_root", str(root.resolve()))
        object.__setattr__(
            self,
            "created_at_utc",
            format_utc(parse_utc(self.created_at_utc)),
        )
        object.__setattr__(
            self,
            "evidence",
            tuple(sorted(evidence, key=lambda item: item.gate.value)),
        )

    @property
    def evidence_by_gate(self) -> dict[AcceptanceGate, AcceptanceEvidenceV1]:
        return {item.gate: item for item in self.evidence}

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "environment": self.environment,
            "account_id": self.account_id,
            "deployment_id": self.deployment_id,
            "application_version": self.application_version,
            "data_root": self.data_root,
            "created_at_utc": self.created_at_utc,
            "evidence": [item.to_dict() for item in self.evidence],
            "automatic_retry_enabled": False,
            "legacy_database_compatibility_required": False,
        }

    @property
    def content_hash(self) -> str:
        return _hash_payload(self.unsigned_payload())

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.unsigned_payload(),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TargetAcceptanceManifestV1":
        expected = {
            "schema_name",
            "schema_version",
            "environment",
            "account_id",
            "deployment_id",
            "application_version",
            "data_root",
            "created_at_utc",
            "evidence",
            "automatic_retry_enabled",
            "legacy_database_compatibility_required",
            "content_hash",
        }
        if set(value) != expected:
            raise TargetAcceptanceError(
                "acceptance manifest fields mismatch: "
                f"missing={sorted(expected - set(value))}, "
                f"unknown={sorted(set(value) - expected)}"
            )
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or int(value["schema_version"]) != cls.SCHEMA_VERSION
        ):
            raise TargetAcceptanceError("unsupported acceptance manifest schema")
        _required_false(
            value["automatic_retry_enabled"],
            field_name="automatic_retry_enabled",
        )
        _required_false(
            value["legacy_database_compatibility_required"],
            field_name="legacy_database_compatibility_required",
        )
        raw_evidence = value["evidence"]
        if not isinstance(raw_evidence, list):
            raise TargetAcceptanceError("manifest evidence must be a list")
        manifest = cls(
            environment=str(value["environment"]),
            account_id=str(value["account_id"]),
            deployment_id=str(value["deployment_id"]),
            application_version=str(value["application_version"]),
            data_root=str(value["data_root"]),
            created_at_utc=str(value["created_at_utc"]),
            evidence=tuple(
                AcceptanceEvidenceV1.from_dict(item)
                for item in raw_evidence
            ),
        )
        expected_hash = str(value["content_hash"] or "").strip().lower()
        if expected_hash != manifest.content_hash:
            raise TargetAcceptanceError(
                "acceptance manifest content_hash does not match its payload"
            )
        return manifest


def build_acceptance_evidence(
    *,
    gate: AcceptanceGate,
    summary_path: str | Path,
    data_root: str | Path,
) -> AcceptanceEvidenceV1:
    root = Path(data_root).resolve()
    path = Path(summary_path).resolve()
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise TargetAcceptanceError(
            f"acceptance summary must be inside data_root: {path}"
        ) from exc
    if not path.is_file():
        raise TargetAcceptanceError(f"acceptance summary does not exist: {path}")
    try:
        value = read_json_object(path)
    except Exception as exc:
        raise TargetAcceptanceError(
            f"cannot read acceptance summary {path}: {exc}"
        ) from exc
    finished, primary_id, facts = validate_acceptance_summary(gate, value)
    return AcceptanceEvidenceV1(
        gate=gate,
        summary_schema=str(value["schema_name"]),
        summary_version=int(value["schema_version"]),
        relative_summary_path=relative.as_posix(),
        summary_sha256=_sha256_file(path),
        finished_at_utc=finished,
        primary_id=primary_id,
        facts=facts,
    )


def build_target_acceptance_manifest(
    *,
    settings: DeploymentSettings,
    summaries: Mapping[AcceptanceGate, str | Path],
    created_at_utc: str,
) -> TargetAcceptanceManifestV1:
    required = set(REQUIRED_PAPER_SOAK_GATES)
    actual = set(summaries)
    if actual != required:
        raise TargetAcceptanceError(
            "paper-soak acceptance gate set mismatch: "
            f"missing={sorted(item.value for item in required - actual)}, "
            f"unknown={sorted(item.value for item in actual - required)}"
        )
    evidence = tuple(
        build_acceptance_evidence(
            gate=gate,
            summary_path=summaries[gate],
            data_root=settings.data_root,
        )
        for gate in REQUIRED_PAPER_SOAK_GATES
    )
    relative_paths = [item.relative_summary_path for item in evidence]
    if len(relative_paths) != len(set(relative_paths)):
        raise TargetAcceptanceError(
            "each acceptance gate must use a distinct summary file"
        )
    return TargetAcceptanceManifestV1(
        environment=settings.environment,
        account_id=settings.ib_account_id,
        deployment_id=settings.deployment_id,
        application_version=settings.application_version,
        data_root=str(settings.data_root),
        created_at_utc=created_at_utc,
        evidence=evidence,
    )


def load_target_acceptance_manifest(
    path: str | Path,
) -> TargetAcceptanceManifestV1:
    source = Path(path)
    try:
        value = read_json_object(source)
    except Exception as exc:
        raise TargetAcceptanceError(
            f"cannot read target acceptance manifest {source}: {exc}"
        ) from exc
    return TargetAcceptanceManifestV1.from_dict(value)


def verify_acceptance_manifest(
    manifest: TargetAcceptanceManifestV1,
    *,
    settings: DeploymentSettings,
    required_gates: tuple[AcceptanceGate, ...] = REQUIRED_PAPER_SOAK_GATES,
) -> tuple[AcceptanceEvidenceV1, ...]:
    expected_scope = (
        settings.environment,
        settings.ib_account_id,
        settings.deployment_id,
        settings.application_version,
        str(settings.data_root.resolve()),
    )
    actual_scope = (
        manifest.environment,
        manifest.account_id,
        manifest.deployment_id,
        manifest.application_version,
        str(Path(manifest.data_root).resolve()),
    )
    if actual_scope != expected_scope:
        raise TargetAcceptanceError(
            "acceptance manifest scope differs from deployment settings: "
            f"expected={expected_scope}, actual={actual_scope}"
        )
    by_gate = manifest.evidence_by_gate
    missing = [item.value for item in required_gates if item not in by_gate]
    if missing:
        raise TargetAcceptanceError(
            f"acceptance manifest is missing required gates: {missing}"
        )
    verified = []
    for gate in required_gates:
        stored = by_gate[gate]
        current = build_acceptance_evidence(
            gate=gate,
            summary_path=settings.data_root / stored.relative_summary_path,
            data_root=settings.data_root,
        )
        if current != stored:
            raise TargetAcceptanceError(
                f"acceptance evidence changed after manifest creation: {gate.value}"
            )
        verified.append(current)
    return tuple(verified)


__all__ = [
    "AcceptanceEvidenceV1",
    "AcceptanceGate",
    "REQUIRED_PAPER_SOAK_GATES",
    "TargetAcceptanceError",
    "TargetAcceptanceManifestV1",
    "build_acceptance_evidence",
    "build_target_acceptance_manifest",
    "load_target_acceptance_manifest",
    "validate_acceptance_summary",
    "verify_acceptance_manifest",
]
