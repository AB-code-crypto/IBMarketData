from __future__ import annotations

import argparse
import codecs
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.time import format_utc, parse_utc


class SignalParityError(RuntimeError):
    pass


_INT_FIELDS = (
    "raw_candidate_count",
    "valid_pattern_count",
    "pearson_pass_count",
    "minmax_pass_count",
    "skipped_pattern_count",
    "potential_used",
)
_FLOAT_FIELDS = (
    "entry_price",
    "best_raw_pearson",
    "best_signal_pearson",
    "best_candidate_score",
    "potential_end_delta_points",
    "potential_max_profit_points",
    "potential_max_drawdown_points",
)
_TEXT_FIELDS = (
    "status",
    "reason",
    "potential_direction",
)
_REQUIRED_TARGET_FIELDS = {
    "signal_bar_utc",
    *_INT_FIELDS,
    *_FLOAT_FIELDS,
    *_TEXT_FIELDS,
}


@dataclass(frozen=True)
class SignalParityReport:
    signal_bar_utc: str
    tolerance: float
    is_match: bool
    legacy_metrics: dict[str, Any]
    target_metrics: dict[str, Any]
    mismatches: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_bar_utc": self.signal_bar_utc,
            "tolerance": self.tolerance,
            "is_match": self.is_match,
            "legacy_metrics": self.legacy_metrics,
            "target_metrics": self.target_metrics,
            "mismatches": list(self.mismatches),
        }


def _decode_json_text(raw: bytes, *, path: Path) -> str:
    if raw.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
        encoding = "utf-32"
    elif raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        # Windows PowerShell 5.1 Tee-Object/Out-File writes UTF-16 with BOM.
        encoding = "utf-16"
    elif raw.startswith(codecs.BOM_UTF8):
        encoding = "utf-8-sig"
    else:
        encoding = "utf-8"

    try:
        return raw.decode(encoding)
    except UnicodeDecodeError as exc:
        raise SignalParityError(
            "cannot decode target calculation JSON "
            f"{path}: expected UTF-8 or BOM-marked UTF-16/UTF-32; {exc}"
        ) from exc


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise SignalParityError(
            f"target calculation JSON does not exist: {path}"
        )
    try:
        text = _decode_json_text(path.read_bytes(), path=path)
        value = json.loads(text)
    except OSError as exc:
        raise SignalParityError(
            f"cannot read target calculation JSON {path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise SignalParityError(
            f"target calculation JSON is invalid {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SignalParityError(
            "target calculation JSON must contain an object"
        )
    missing = sorted(_REQUIRED_TARGET_FIELDS - set(value))
    if missing:
        raise SignalParityError(
            "target calculation JSON is missing required fields: "
            f"{missing}"
        )
    return value


def _normalize_optional_float(
    value: Any,
    *,
    field_name: str,
) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SignalParityError(
            f"target field {field_name} must be numeric or null: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise SignalParityError(
            f"target field {field_name} must be finite: {value!r}"
        )
    return number


def target_metrics(value: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "signal_bar_utc": format_utc(
            parse_utc(str(value["signal_bar_utc"]))
        ),
    }
    for field_name in _INT_FIELDS:
        try:
            metrics[field_name] = int(value[field_name])
        except (TypeError, ValueError) as exc:
            raise SignalParityError(
                f"target field {field_name} must be an integer: "
                f"{value[field_name]!r}"
            ) from exc
    for field_name in _FLOAT_FIELDS:
        metrics[field_name] = _normalize_optional_float(
            value[field_name],
            field_name=field_name,
        )
    for field_name in _TEXT_FIELDS:
        metrics[field_name] = (
            None
            if value[field_name] is None
            else str(value[field_name])
        )
    return metrics


def _read_only_connection(db_path, **kwargs) -> sqlite3.Connection:
    _ = kwargs
    source = Path(db_path)
    if not source.is_file():
        raise FileNotFoundError(f"Price DB not found: {source}")
    uri = f"file:{source.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA temp_store=MEMORY")
    return connection


def legacy_metrics(
    *,
    legacy_price_dir: Path,
    instrument_id: str,
    signal_bar_utc: str,
) -> dict[str, Any]:
    signal_time = parse_utc(signal_bar_utc)
    signal_ts = int(signal_time.timestamp())

    try:
        from config import settings_live
        import ib_signal.signal_candidate_potential as potential_module
        import ib_signal.signal_candidates as candidates_module
        import ib_signal.signal_pattern_matrix as pattern_module
        from ib_signal.pearson import calculate_centered_pearson_batch
        from ib_signal.signal_candidate_potential import (
            build_candidate_potential_result,
        )
        from ib_signal.signal_candidate_rank_features import (
            filter_candidates_by_minmax_ratio,
            rank_candidates_by_score,
        )
        from ib_signal.signal_candidates import find_candidate_windows
        from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
        from ib_signal.signal_pattern_matrix import build_pattern_matrix
        from ib_signal.signal_window import build_current_signal_window
    except Exception as exc:
        raise SignalParityError(
            "cannot import current rolling implementation; ensure the legacy "
            f"environment is available: {type(exc).__name__}: {exc}"
        ) from exc

    source_dir = legacy_price_dir.resolve()
    if not source_dir.is_dir():
        raise SignalParityError(
            f"legacy price directory does not exist: {source_dir}"
        )
    settings_live.price_db_dir = str(source_dir)

    # Legacy matrix construction uses TEMP tables. The main price database stays
    # read-only while SQLite is still allowed to create TEMP objects in memory.
    candidates_module.open_sqlite_connection = _read_only_connection
    pattern_module.open_sqlite_connection = _read_only_connection
    potential_module.open_sqlite_connection = _read_only_connection

    settings = DEFAULT_SIGNAL_CONFIG
    try:
        window = build_current_signal_window(
            signal_bar_ts=signal_ts,
            settings=settings,
        )
        candidate_search = find_candidate_windows(
            instrument_code=instrument_id,
            current_window=window,
            settings=settings,
        )
        pattern = build_pattern_matrix(
            instrument_code=instrument_id,
            window=window,
            candidates=candidate_search.candidates,
        )
        all_pearson = calculate_centered_pearson_batch(
            pattern.current_values,
            pattern.candidate_matrix,
        )
        best_raw_pearson = (
            float(all_pearson.max()) if all_pearson.size else 0.0
        )
        passed_indices = np.flatnonzero(
            all_pearson >= float(settings.pearson_min)
        )
        passed_candidates = [
            pattern.valid_candidates[int(index)]
            for index in passed_indices
        ]
        minmax = filter_candidates_by_minmax_ratio(
            current_values=pattern.current_values,
            candidates=passed_candidates,
            candidate_matrix=pattern.candidate_matrix[
                passed_indices,
                :,
            ],
            pearson_scores=all_pearson[passed_indices],
            max_ratio=settings.candidate_minmax_hard_filter_max_ratio,
        )
        ranked = rank_candidates_by_score(
            current_values=pattern.current_values,
            candidates=minmax.valid_candidates,
            candidate_matrix=minmax.candidate_matrix,
            pearson_scores=minmax.pearson_scores,
            pearson_weight=settings.candidate_score_pearson_weight,
            end_delta_weight=settings.candidate_score_end_delta_weight,
            minmax_weight=settings.candidate_score_minmax_weight,
        )
        potential = build_candidate_potential_result(
            instrument_code=instrument_id,
            signal_window=window,
            current_values=pattern.current_values,
            candidates=ranked.valid_candidates,
            candidate_scores=ranked.candidate_scores,
            min_count=settings.candidate_potential_min_count,
            max_count=settings.candidate_potential_max_count,
        )
    except SignalParityError:
        raise
    except Exception as exc:
        raise SignalParityError(
            "current rolling calculation failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    threshold = abs(
        float(settings.candidate_potential_min_abs_end_delta_points)
    )
    if (
        potential.is_available
        and potential.direction in {"LONG", "SHORT"}
        and abs(float(potential.end_delta_points)) > threshold
    ):
        status = "SIGNAL"
        reason = None
    elif not potential.is_available:
        status = "NO_SIGNAL"
        reason = potential.unavailable_reason or "potential_unavailable"
    elif potential.direction == "NONE":
        status = "NO_SIGNAL"
        reason = "potential_direction_none"
    else:
        status = "NO_SIGNAL"
        reason = (
            "potential_below_threshold:"
            f"{abs(float(potential.end_delta_points)):g}<={threshold:g}"
        )

    return {
        "signal_bar_utc": format_utc(signal_time),
        "status": status,
        "reason": reason,
        "entry_price": float(pattern.current_values[-1]),
        "raw_candidate_count": int(
            candidate_search.raw_candidate_rows_count
        ),
        "valid_pattern_count": len(pattern.valid_candidates),
        "pearson_pass_count": len(passed_candidates),
        "minmax_pass_count": int(minmax.kept_candidates_count),
        "skipped_pattern_count": int(
            pattern.skipped_candidates_count
        ),
        "best_raw_pearson": best_raw_pearson,
        "best_signal_pearson": (
            float(ranked.pearson_scores.max())
            if ranked.pearson_scores.size
            else None
        ),
        "best_candidate_score": (
            float(ranked.candidate_scores.max())
            if ranked.candidate_scores.size
            else None
        ),
        "potential_direction": str(potential.direction),
        "potential_end_delta_points": float(
            potential.end_delta_points
        ),
        "potential_max_profit_points": float(
            potential.max_profit_points
        ),
        "potential_max_drawdown_points": float(
            potential.max_drawdown_points
        ),
        "potential_used": int(potential.used_candidates_count),
    }


def compare_metrics(
    *,
    legacy: dict[str, Any],
    target: dict[str, Any],
    tolerance: float,
) -> SignalParityReport:
    numeric_tolerance = float(tolerance)
    if (
        not math.isfinite(numeric_tolerance)
        or numeric_tolerance < 0.0
    ):
        raise SignalParityError(
            "tolerance must be finite and non-negative: "
            f"{tolerance!r}"
        )
    if legacy["signal_bar_utc"] != target["signal_bar_utc"]:
        raise SignalParityError(
            "legacy and target signal timestamps differ: "
            f"legacy={legacy['signal_bar_utc']}, "
            f"target={target['signal_bar_utc']}"
        )

    mismatches: list[dict[str, Any]] = []
    for field_name in (*_TEXT_FIELDS, *_INT_FIELDS):
        if legacy[field_name] != target[field_name]:
            mismatches.append(
                {
                    "field": field_name,
                    "legacy": legacy[field_name],
                    "target": target[field_name],
                }
            )

    for field_name in _FLOAT_FIELDS:
        legacy_value = legacy[field_name]
        target_value = target[field_name]
        if legacy_value is None or target_value is None:
            if legacy_value != target_value:
                mismatches.append(
                    {
                        "field": field_name,
                        "legacy": legacy_value,
                        "target": target_value,
                    }
                )
            continue
        absolute_error = abs(
            float(legacy_value) - float(target_value)
        )
        if absolute_error > numeric_tolerance:
            mismatches.append(
                {
                    "field": field_name,
                    "legacy": float(legacy_value),
                    "target": float(target_value),
                    "absolute_error": absolute_error,
                }
            )

    return SignalParityReport(
        signal_bar_utc=legacy["signal_bar_utc"],
        tolerance=numeric_tolerance,
        is_match=not mismatches,
        legacy_metrics=legacy,
        target_metrics=target,
        mismatches=tuple(mismatches),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare one stored target signal calculation with the current "
            "rolling implementation on the same explicit timestamp. The "
            "legacy price databases are opened with SQLite mode=ro."
        )
    )
    parser.add_argument("--legacy-price-dir", type=Path, required=True)
    parser.add_argument("--target-json", type=Path, required=True)
    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        target_value = _read_json_object(
            arguments.target_json.resolve()
        )
        target = target_metrics(target_value)
        legacy = legacy_metrics(
            legacy_price_dir=arguments.legacy_price_dir.resolve(),
            instrument_id=str(arguments.instrument or "").strip(),
            signal_bar_utc=target["signal_bar_utc"],
        )
        report = compare_metrics(
            legacy=legacy,
            target=target,
            tolerance=arguments.tolerance,
        )
    except SignalParityError as exc:
        print(
            json.dumps(
                {"is_match": False, "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    payload = report.to_dict()
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    if arguments.output_json is not None:
        atomic_write_json(arguments.output_json.resolve(), payload)
    return 0 if report.is_match else 1


if __name__ == "__main__":
    raise SystemExit(main())
