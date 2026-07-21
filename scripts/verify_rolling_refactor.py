#!/usr/bin/env python3
"""Static verifier for the rolling-only IBMarketData refactor."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path

DELETED_PATHS = (
    "ib_job_data",
    "run_job_data.py",
    "ib_execution/slot_close_recovery.py",
    "ib_execution/slot_loss_extension.py",
    "ib_execution/slot_loss_extension_runner.py",
    "ib_execution/slot_loss_extension_store.py",
    "ib_signal/candidate_funnel_store.py",
    "ib_signal/job_reader.py",
    "ib_signal/signal_candidate_regime_filter.py",
    "ib_signal/signal_interpretation.py",
    "ib_signal/signal_ma_zone_reader.py",
    "ib_signal/signal_regime_reader.py",
    "ib_signal/signal_regression.py",
    "ib_signal/signal_regression_relation.py",
    "ib_signal/signal_regression_threshold.py",
    "ib_signal/signal_rules_config.py",
    "ib_signal/signal_sma_reader.py",
)

FORBIDDEN_TEXT = (
    "ib_job_data",
    "run_job_data",
    "SignalWindowMode",
    "MarketRegimeFilterMode",
    "slot_loss_extension",
    "slot_close_recovery",
    "signal_candidate_regime_filter",
    "signal_interpretation",
    "signal_ma_zone_reader",
    "signal_regime_reader",
    "signal_sma_reader",
    "candidate_funnel_store",
    "regression_flat_delta_threshold",
    "regime_flat_delta_threshold",
    "mid_open",
    "mid_high",
    "mid_low",
    "spread_open",
    "spread_high",
    "spread_low",
    "spread_close",
)

TEXT_SUFFIXES = {".py", ".md", ".txt", ".bat", ".ps1", ".toml", ".yaml", ".yml"}
SKIP_DIR_NAMES = {
    ".git",
    ".idea",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "data",
    "logs",
    ".venv",
    "venv",
}

# The signal-event migration must recognize old column names while copying rows.
TEXT_ALLOWLIST: dict[str, set[str]] = {
    "ib_signal/signal_event_store.py": {
        "signal_window_mode",
        "market_regime_filter_mode",
    },
    "ib_execution/execution_store.py": {
        "slot_loss_extension",
    },
    "tester/rolling_signal_smoke_tester.py": {
        "slot_loss_extension",
    },
}


def iter_source_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        if any(part in SKIP_DIR_NAMES for part in path.relative_to(root).parts):
            continue
        yield path


def verify_deleted_paths(root: Path, errors: list[str]) -> None:
    for relative in DELETED_PATHS:
        if (root / relative).exists():
            errors.append(f"obsolete path still exists: {relative}")


def verify_forbidden_text(root: Path, errors: list[str]) -> None:
    for path in iter_source_files(root):
        relative = path.relative_to(root).as_posix()
        if relative == "scripts/verify_rolling_refactor.py":
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        allowed = TEXT_ALLOWLIST.get(relative, set())
        for token in FORBIDDEN_TEXT:
            if token in allowed:
                continue
            if token in text:
                errors.append(f"forbidden token {token!r} in {relative}")

        # Broader service terms are checked separately to avoid allowing stale
        # launcher/docs wording while still permitting the legacy DB migration.
        lowered = text.lower()
        if relative != "ib_signal/signal_event_store.py":
            for token in ("job_data", "job-data", "job db", "job_db"):
                if token in lowered:
                    errors.append(f"obsolete job-data term {token!r} in {relative}")


def verify_python_syntax(root: Path, errors: list[str]) -> None:
    for path in iter_source_files(root):
        if path.suffix != ".py":
            continue
        relative = path.relative_to(root).as_posix()
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"syntax error in {relative}: {exc}")


def require_contains(
    root: Path,
    relative: str,
    required: tuple[str, ...],
    forbidden: tuple[str, ...],
    errors: list[str],
) -> None:
    path = root / relative
    if not path.is_file():
        errors.append(f"required file missing: {relative}")
        return
    text = path.read_text(encoding="utf-8")
    for token in required:
        if token not in text:
            errors.append(f"{relative}: required token missing: {token!r}")
    for token in forbidden:
        if token in text:
            errors.append(f"{relative}: forbidden token present: {token!r}")


def verify_structure(root: Path, errors: list[str]) -> None:
    require_contains(
        root,
        "wt_run/common.py",
        required=(
            'ServiceSpec("market_data"',
            'ServiceSpec("signal"',
            'ServiceSpec("trader"',
        ),
        forbidden=("job_data", "run_job_data"),
        errors=errors,
    )
    require_contains(
        root,
        "ib_signal/signal_config.py",
        required=("rolling_back_minutes", "rolling_trade_minutes"),
        forbidden=("signal_window_mode", "second_chance", "market_regime"),
        errors=errors,
    )
    require_contains(
        root,
        "run_signal.py",
        required=("wait_for_fresh_price_bars",),
        forbidden=("job", "feature_db"),
        errors=errors,
    )
    require_contains(
        root,
        "core/price_source.py",
        required=("def mid_close_sql", "bid_close", "ask_close"),
        forbidden=("ALTER TABLE", "ADD COLUMN"),
        errors=errors,
    )
    require_contains(
        root,
        "ib_signal/signal_candidates.py",
        required=(
            "def build_candidate_signal_rows_query",
            "bar_time_ts >= ?",
            "bar_time_ts <= ?",
        ),
        forbidden=("ib_job_data",),
        errors=errors,
    )
    for relative in (
        "ib_signal/signal_pattern_matrix.py",
        "ib_signal/signal_candidate_potential.py",
    ):
        require_contains(
            root,
            relative,
            required=("mid_close_sql", "get_price_db_target"),
            forbidden=("ib_job_data",),
            errors=errors,
        )
    require_contains(
        root,
        "ib_execution/execution_loop.py",
        required=("next_execution_stats_reconcile_ts",),
        forbidden=("slot_loss", "slot_close"),
        errors=errors,
    )
    require_contains(
        root,
        "ib_execution/emergency_close.py",
        required=("close_market_safely", "cancel_exit_orders_for_instrument"),
        forbidden=("slot_loss", "slot_close"),
        errors=errors,
    )
    require_contains(
        root,
        "ib_trader/trade_decision_service.py",
        required=("flat_position_open_by_rolling_signal", "MARKET"),
        forbidden=("SignalWindowMode", "second_chance", "ma_zone"),
        errors=errors,
    )

    # mid_close remains a read-time expression; canonical price-table writers
    # must not acquire a duplicate physical column.
    for relative in ("core/db_sql.py", "core/price_db.py", "core/realtime_db.py"):
        path = root / relative
        if path.is_file() and "mid_close" in path.read_text(encoding="utf-8"):
            errors.append(f"physical mid_close leaked into price writer/schema: {relative}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("repo", nargs="?", default=".")
    args = parser.parse_args(argv)
    root = Path(args.repo).resolve()
    errors: list[str] = []
    verify_deleted_paths(root, errors)
    verify_forbidden_text(root, errors)
    verify_python_syntax(root, errors)
    verify_structure(root, errors)

    if errors:
        print("Rolling refactor verification FAILED:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("Rolling refactor verification OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
