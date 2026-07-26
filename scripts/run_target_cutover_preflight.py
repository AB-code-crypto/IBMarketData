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
from ibmd.operations.acceptance_manifest import TargetAcceptanceError
from ibmd.operations.cutover_preflight import (
    CutoverMode,
    CutoverPreflightError,
    TargetCutoverPreflight,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a fail-closed target cutover preflight and optionally issue one "
            "short-lived PAPER_SOAK runtime authorization. The preflight performs "
            "no broker calls and starts no services."
        )
    )
    parser.add_argument(
        "--mode",
        choices=[item.value for item in CutoverMode],
        default=CutoverMode.PAPER_SOAK.value,
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--issue-authorization", action="store_true")
    parser.add_argument(
        "--bootstrap-manifest",
        type=Path,
        default=ROOT / "bootstrap" / "target.v1.json",
    )
    parser.add_argument("--acceptance-manifest", type=Path, default=None)
    parser.add_argument("--legacy-runtime-dir", type=Path, default=None)
    parser.add_argument("--authorization-file", type=Path, default=None)
    parser.add_argument("--authorization-valid-hours", type=float, default=24.0)
    parser.add_argument("--allow-unqualified-session", action="store_true")
    return parser


def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
    acceptance = (
        arguments.acceptance_manifest.resolve()
        if arguments.acceptance_manifest is not None
        else settings.data_root / "runtime" / "acceptance" / "manifest.json"
    )
    legacy_runtime = (
        arguments.legacy_runtime_dir.resolve()
        if arguments.legacy_runtime_dir is not None
        else ROOT / "data" / "runtime" / "wt_run"
    )
    authorization_file = (
        arguments.authorization_file.resolve()
        if arguments.authorization_file is not None
        else settings.data_root / "runtime" / "authorization.json"
    )
    issue = bool(arguments.issue_authorization)
    if issue and authorization_file.exists():
        raise CutoverPreflightError(
            "runtime authorization already exists and must not be overwritten: "
            f"{authorization_file}"
        )
    service = TargetCutoverPreflight(
        settings=settings,
        source_root=ROOT,
        bootstrap_manifest_path=arguments.bootstrap_manifest,
        acceptance_manifest_path=acceptance,
        legacy_runtime_dir=legacy_runtime,
        mode=CutoverMode(arguments.mode),
        allow_unqualified_session=bool(arguments.allow_unqualified_session),
        authorization_valid_hours=float(arguments.authorization_valid_hours),
    )
    result = service.run(
        observed_at_utc=utc_now_text(),
        issue_authorization=issue,
    )
    if result.authorization is not None:
        atomic_write_json(authorization_file, result.authorization.to_dict())
    payload = result.to_dict()
    payload["acceptance_manifest_path"] = str(acceptance)
    payload["legacy_runtime_dir"] = str(legacy_runtime)
    payload["authorization_file"] = (
        str(authorization_file)
        if result.authorization is not None
        else None
    )
    payload["services_started"] = False
    payload["broker_connections_opened"] = False
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result.ready else 2


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if int(bool(arguments.check)) + int(bool(arguments.issue_authorization)) != 1:
        print(
            "cutover preflight requires exactly one mode: "
            "--check or --issue-authorization",
            file=sys.stderr,
        )
        return 2
    if (
        arguments.mode == CutoverMode.LIVE_CUTOVER.value
        and arguments.allow_unqualified_session
    ):
        print(
            "--allow-unqualified-session is forbidden for LIVE_CUTOVER",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (
        ConfigurationError,
        CutoverPreflightError,
        OSError,
        TargetAcceptanceError,
        ValueError,
    ) as exc:
        print(
            f"target cutover preflight failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
