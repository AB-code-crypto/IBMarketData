from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or explicitly apply target SQLite migrations. "
            "The default is dry-run and never mutates the database."
        )
    )
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--application-version", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply pending migrations. Without this flag the command is read-only.",
    )
    arguments = parser.parse_args(argv)

    store_name, migrations = load_migration_manifest(arguments.manifest)
    runner = SQLiteMigrationRunner(
        database_path=arguments.database,
        store_name=store_name,
        migrations=migrations,
        application_version=arguments.application_version,
    )
    plan = runner.apply() if arguments.apply else runner.inspect()
    print(json.dumps(plan.to_dict(), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
