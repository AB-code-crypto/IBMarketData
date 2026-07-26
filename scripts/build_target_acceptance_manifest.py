from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now_text
from ibmd.operations.acceptance_manifest import (
    AcceptanceGate,
    TargetAcceptanceError,
    build_target_acceptance_manifest,
    load_target_acceptance_manifest,
    verify_acceptance_manifest,
)


_GATE_ARGUMENTS = {
    AcceptanceGate.ENTRY_PROTECTION: "entry_summary",
    AcceptanceGate.LIQUIDATION: "liquidation_summary",
    AcceptanceGate.RESTART: "restart_summary",
    AcceptanceGate.LIQUIDATION_RESTART: "liquidation_restart_summary",
    AcceptanceGate.REVERSE: "reverse_summary",
    AcceptanceGate.DAILY_HALT: "daily_halt_summary",
    AcceptanceGate.DAILY_FLAT: "daily_flat_summary",
    AcceptanceGate.ROLLOVER: "rollover_summary",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build or validate one immutable paper acceptance manifest. Every "
            "summary must be inside IBMD_DATA_ROOT and must prove its exact broker "
            "safety gate."
        )
    )
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--entry-summary", type=Path, default=None)
    parser.add_argument("--liquidation-summary", type=Path, default=None)
    parser.add_argument("--restart-summary", type=Path, default=None)
    parser.add_argument("--liquidation-restart-summary", type=Path, default=None)
    parser.add_argument("--reverse-summary", type=Path, default=None)
    parser.add_argument("--daily-halt-summary", type=Path, default=None)
    parser.add_argument("--daily-flat-summary", type=Path, default=None)
    parser.add_argument("--rollover-summary", type=Path, default=None)
    return parser


def _manifest_path(arguments: argparse.Namespace, data_root: Path) -> Path:
    return (
        arguments.manifest.resolve()
        if arguments.manifest is not None
        else data_root / "runtime" / "acceptance" / "manifest.json"
    )


def _summary_mapping(arguments: argparse.Namespace) -> dict[AcceptanceGate, Path]:
    values: dict[AcceptanceGate, Path] = {}
    missing = []
    for gate, attribute in _GATE_ARGUMENTS.items():
        path = getattr(arguments, attribute)
        if path is None:
            missing.append("--" + attribute.replace("_", "-"))
        else:
            values[gate] = path.resolve()
    if missing:
        raise TargetAcceptanceError(
            "--build requires every acceptance summary: " + ", ".join(missing)
        )
    return values


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    target = _manifest_path(arguments, settings.data_root)
    if arguments.build:
        if target.exists():
            raise TargetAcceptanceError(
                f"acceptance manifest already exists and is immutable: {target}"
            )
        manifest = build_target_acceptance_manifest(
            settings=settings,
            summaries=_summary_mapping(arguments),
            created_at_utc=utc_now_text(),
        )
        atomic_write_json(target, manifest.to_dict())
        mode = "built"
    else:
        if any(getattr(arguments, item) is not None for item in _GATE_ARGUMENTS.values()):
            raise TargetAcceptanceError(
                "summary arguments are valid only with --build"
            )
        manifest = load_target_acceptance_manifest(target)
        verify_acceptance_manifest(manifest, settings=settings)
        mode = "validated"
    payload = {
        "mode": mode,
        "manifest_path": str(target),
        "content_hash": manifest.content_hash,
        "environment": manifest.environment,
        "account_id": manifest.account_id,
        "deployment_id": manifest.deployment_id,
        "application_version": manifest.application_version,
        "gate_count": len(manifest.evidence),
        "gates": [item.gate.value for item in manifest.evidence],
        "all_summary_hashes_verified": True,
        "automatic_retry_enabled": False,
        "legacy_database_compatibility_required": False,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if int(bool(arguments.build)) + int(bool(arguments.validate)) != 1:
        print(
            "acceptance manifest requires exactly one mode: --build or --validate",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (ConfigurationError, OSError, TargetAcceptanceError, ValueError) as exc:
        print(
            f"target acceptance manifest failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
