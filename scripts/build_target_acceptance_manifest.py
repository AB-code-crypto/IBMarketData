from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import (
    atomic_write_json,
    read_json_object,
)
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now_text
from ibmd.operations.acceptance_manifest import (
    AcceptanceGate,
    TargetAcceptanceError,
    build_target_acceptance_manifest,
    load_target_acceptance_manifest,
    validate_acceptance_summary,
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
            "Build or validate one immutable paper acceptance manifest. Build mode "
            "validates external runner summaries, copies them into IBMD_DATA_ROOT "
            "and records content hashes for later cutover verification."
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
    paths = list(values.values())
    if len(paths) != len(set(paths)):
        raise TargetAcceptanceError(
            "each acceptance gate must use a distinct source summary file"
        )
    return values


def _validate_external_summaries(
    sources: dict[AcceptanceGate, Path],
) -> None:
    for gate, source in sources.items():
        if not source.is_file():
            raise TargetAcceptanceError(
                f"acceptance source summary does not exist: {source}"
            )
        try:
            value = read_json_object(source)
        except Exception as exc:
            raise TargetAcceptanceError(
                f"cannot read acceptance source summary {source}: {exc}"
            ) from exc
        validate_acceptance_summary(gate, value)


def _fsync_copied_file(path: Path) -> None:
    # Windows os.fsync() delegates to _commit(), which rejects a read-only
    # descriptor with EBADF. The copied file is already closed by copyfile;
    # reopen it read/write solely to commit its contents durably.
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _stage_summaries(
    *,
    sources: dict[AcceptanceGate, Path],
    data_root: Path,
) -> tuple[Path, dict[AcceptanceGate, Path]]:
    acceptance_root = data_root / "runtime" / "acceptance"
    evidence = acceptance_root / "evidence"
    if evidence.exists():
        raise TargetAcceptanceError(
            f"acceptance evidence directory already exists and is immutable: {evidence}"
        )
    acceptance_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=".evidence-stage-", dir=str(acceptance_root))
    )
    staged: dict[AcceptanceGate, Path] = {}
    try:
        for gate, source in sources.items():
            target = temporary / f"{gate.value.lower()}.summary.json"
            shutil.copyfile(source, target)
            _fsync_copied_file(target)
            staged[gate] = target
        os.replace(temporary, evidence)
        return evidence, {
            gate: evidence / path.name for gate, path in staged.items()
        }
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        if evidence.is_dir():
            shutil.rmtree(evidence, ignore_errors=True)
        raise


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    target = _manifest_path(arguments, settings.data_root)
    if arguments.build:
        if target.exists():
            raise TargetAcceptanceError(
                f"acceptance manifest already exists and is immutable: {target}"
            )
        sources = _summary_mapping(arguments)
        _validate_external_summaries(sources)
        evidence_directory, staged = _stage_summaries(
            sources=sources,
            data_root=settings.data_root,
        )
        try:
            manifest = build_target_acceptance_manifest(
                settings=settings,
                summaries=staged,
                created_at_utc=utc_now_text(),
            )
            atomic_write_json(target, manifest.to_dict())
        except Exception:
            shutil.rmtree(evidence_directory, ignore_errors=True)
            raise
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
        "summaries_staged_inside_target_root": True,
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
