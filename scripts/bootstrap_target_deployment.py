from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.operations.bootstrap import (
    TargetBootstrapError,
    TargetDeploymentBootstrapper,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create or validate one clean target IBMarketData deployment root. "
            "The bootstrap is atomic at the directory level, refuses to reuse "
            "an existing root, contains no legacy trade/state migration and "
            "does not import historical prices."
        )
    )
    parser.add_argument(
        "--bootstrap-manifest",
        type=Path,
        default=ROOT / "bootstrap" / "target.v1.json",
    )
    parser.add_argument("--target-root", required=True, type=Path)
    parser.add_argument("--application-version", required=True)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--validate-target", action="store_true")
    parser.add_argument(
        "--require-production-sessions",
        action="store_true",
        help=(
            "Reject a bootstrap whose copied catalog does not contain a "
            "production-qualified daily-flat session artifact."
        ),
    )
    return parser


def run(arguments: argparse.Namespace) -> int:
    bootstrapper = TargetDeploymentBootstrapper(
        source_root=ROOT,
        bootstrap_manifest_path=arguments.bootstrap_manifest,
        target_root=arguments.target_root,
        application_version=arguments.application_version,
        require_production_sessions=bool(
            arguments.require_production_sessions
        ),
    )
    if arguments.plan:
        payload = bootstrapper.plan()
    elif arguments.apply:
        payload = bootstrapper.apply().to_dict()
    else:
        payload = bootstrapper.validate_target().to_dict()
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    selected = sum(
        int(value)
        for value in (
            arguments.plan,
            arguments.apply,
            arguments.validate_target,
        )
    )
    if selected != 1:
        print(
            "target bootstrap requires exactly one mode: "
            "--plan, --apply or --validate-target",
            file=sys.stderr,
        )
        return 2
    try:
        return run(arguments)
    except (TargetBootstrapError, OSError, ValueError) as exc:
        print(
            f"target deployment bootstrap failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
