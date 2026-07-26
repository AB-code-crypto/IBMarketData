from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apps import run_execution_runtime_v2
from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.config import ConfigurationError, load_deployment_settings
from ibmd.foundation.time import utc_now_text
from ibmd.operations.runtime_authorization import (
    RuntimeAuthorizationError,
    verify_runtime_start_authorization,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify one immutable PAPER_SOAK runtime authorization and then invoke "
            "the canonical execution runtime in the same process. The current "
            "continuous broker-mutation adapters remain disabled."
        ),
        add_help=False,
    )
    parser.add_argument("--runtime-authorization", type=Path, required=True)
    parser.add_argument("--acceptance-manifest", type=Path, default=None)
    parser.add_argument(
        "--bootstrap-manifest",
        type=Path,
        default=ROOT / "bootstrap" / "target.v1.json",
    )
    parser.add_argument("--catalog-root", type=Path, default=None)
    parser.add_argument("--authorization-proof-file", type=Path, default=None)
    parser.add_argument("--validate-authorization-only", action="store_true")
    parser.add_argument("-h", "--help", action="store_true")
    return parser


def _forwarded_runtime_args(
    wrapper_arguments: argparse.Namespace,
    remainder: list[str],
    *,
    catalog_root: Path,
) -> list[str]:
    if wrapper_arguments.validate_authorization_only:
        if remainder:
            raise RuntimeAuthorizationError(
                "runtime arguments are forbidden with --validate-authorization-only"
            )
        return []
    if not remainder:
        raise RuntimeAuthorizationError(
            "authorized wrapper requires canonical execution runtime arguments"
        )
    if "--catalog-root" in remainder:
        raise RuntimeAuthorizationError(
            "pass --catalog-root to the authorized wrapper, not the inner runtime"
        )
    return [*remainder, "--catalog-root", str(catalog_root)]


def run(
    wrapper_arguments: argparse.Namespace,
    remainder: list[str],
) -> int:
    settings = load_deployment_settings()
    acceptance_manifest = (
        wrapper_arguments.acceptance_manifest.resolve()
        if wrapper_arguments.acceptance_manifest is not None
        else settings.data_root / "runtime" / "acceptance" / "manifest.json"
    )
    catalog_root = (
        wrapper_arguments.catalog_root.resolve()
        if wrapper_arguments.catalog_root is not None
        else settings.data_root / "catalog"
    )
    proof_file = (
        wrapper_arguments.authorization_proof_file.resolve()
        if wrapper_arguments.authorization_proof_file is not None
        else settings.data_root / "runtime" / "authorization-active.json"
    )
    forwarded = _forwarded_runtime_args(
        wrapper_arguments,
        remainder,
        catalog_root=catalog_root,
    )
    proof = verify_runtime_start_authorization(
        settings=settings,
        source_root=ROOT,
        authorization_path=wrapper_arguments.runtime_authorization,
        acceptance_manifest_path=acceptance_manifest,
        bootstrap_manifest_path=wrapper_arguments.bootstrap_manifest,
        catalog_root=catalog_root,
        observed_at_utc=utc_now_text(),
    )
    atomic_write_json(proof_file, proof.to_dict())
    if wrapper_arguments.validate_authorization_only:
        print(
            json.dumps(
                {
                    **proof.to_dict(),
                    "authorization_proof_file": str(proof_file),
                    "execution_runtime_started": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    return run_execution_runtime_v2.main(forwarded)


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    wrapper_arguments, remainder = parser.parse_known_args(values)
    if wrapper_arguments.help:
        parser.print_help()
        print("\nCanonical execution runtime arguments:")
        run_execution_runtime_v2.build_parser().print_help()
        return 0
    try:
        return run(wrapper_arguments, remainder)
    except (
        ConfigurationError,
        OSError,
        RuntimeAuthorizationError,
        ValueError,
    ) as exc:
        print(
            "authorized execution runtime failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
