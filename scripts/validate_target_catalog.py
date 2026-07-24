from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import load_catalog_bundle, resolve_active_contract


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate versioned target IBMarketData catalog artifacts."
    )
    parser.add_argument(
        "catalog_root",
        nargs="?",
        default=str(ROOT / "catalog"),
    )
    parser.add_argument(
        "--require-production-sessions",
        action="store_true",
        help="Fail when an enabled daily-flat session is not production-qualified.",
    )
    parser.add_argument(
        "--resolve-active-at",
        help="Optional UTC timestamp used to print active MNQ contract resolution.",
    )
    arguments = parser.parse_args(argv)

    bundle = load_catalog_bundle(
        arguments.catalog_root,
        require_production_sessions=arguments.require_production_sessions,
    )
    print(f"catalog bundle valid: hash={bundle.bundle_hash}")
    print(
        "artifacts: "
        f"instrument={bundle.instrument_master.content_hash}, "
        f"contracts={bundle.contract_calendar.content_hash}, "
        f"sessions={bundle.session_calendar.content_hash}, "
        f"strategy={bundle.strategy_policy.content_hash}"
    )
    if arguments.resolve_active_at:
        resolution = resolve_active_contract(
            bundle.contract_calendar,
            arguments.resolve_active_at,
        )
        symbol = (
            None
            if resolution.contract is None
            else resolution.contract.local_symbol
        )
        print(
            "active contract resolution: "
            f"status={resolution.status.value}, local_symbol={symbol}, "
            f"at={resolution.observed_at_utc}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
