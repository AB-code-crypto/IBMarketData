from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from scripts.compare_signal_shadow import (
    SignalParityError,
    _read_json_object,
    compare_metrics,
    target_metrics,
)


BASE_METRICS = {
    "signal_bar_utc": "2026-07-23T17:24:00.000000Z",
    "status": "NO_SIGNAL",
    "reason": "potential_below_threshold:18.1062<=30",
    "potential_direction": "LONG",
    "raw_candidate_count": 5912,
    "valid_pattern_count": 5393,
    "pearson_pass_count": 104,
    "minmax_pass_count": 28,
    "skipped_pattern_count": 519,
    "potential_used": 9,
    "entry_price": 28659.75,
    "best_raw_pearson": 0.8530980901417716,
    "best_signal_pearson": 0.8107114932943953,
    "best_candidate_score": 0.8773834163247242,
    "potential_end_delta_points": 18.106193633446342,
    "potential_max_profit_points": 18.453382611892096,
    "potential_max_drawdown_points": -2.087049306650171,
}


class SignalParityComparisonTest(unittest.TestCase):
    def test_exact_metrics_match(self) -> None:
        target = target_metrics(dict(BASE_METRICS))
        report = compare_metrics(
            legacy=dict(target),
            target=target,
            tolerance=1e-9,
        )
        self.assertTrue(report.is_match)
        self.assertEqual(report.mismatches, ())

    def test_material_count_and_float_differences_are_reported(self) -> None:
        target = target_metrics(dict(BASE_METRICS))
        legacy = dict(target)
        legacy["raw_candidate_count"] = 5911
        legacy["potential_end_delta_points"] = (
            float(target["potential_end_delta_points"]) + 0.01
        )
        report = compare_metrics(
            legacy=legacy,
            target=target,
            tolerance=1e-9,
        )
        self.assertFalse(report.is_match)
        by_field = {
            item["field"]: item for item in report.mismatches
        }
        self.assertIn("raw_candidate_count", by_field)
        self.assertIn("potential_end_delta_points", by_field)
        self.assertAlmostEqual(
            by_field["potential_end_delta_points"]["absolute_error"],
            0.01,
            places=12,
        )

    def test_timestamp_mismatch_is_rejected(self) -> None:
        target = target_metrics(dict(BASE_METRICS))
        legacy = dict(target)
        legacy["signal_bar_utc"] = "2026-07-23T17:25:00.000000Z"
        with self.assertRaisesRegex(
            SignalParityError,
            "timestamps differ",
        ):
            compare_metrics(
                legacy=legacy,
                target=target,
                tolerance=1e-9,
            )

    def test_windows_powershell_utf16_json_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "target.json"
            path.write_bytes(
                json.dumps(BASE_METRICS, indent=2).encode("utf-16")
            )
            restored = _read_json_object(path)
        self.assertEqual(restored, BASE_METRICS)

    def test_utf8_bom_json_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "target.json"
            path.write_text(
                json.dumps(BASE_METRICS),
                encoding="utf-8-sig",
            )
            restored = _read_json_object(path)
        self.assertEqual(restored, BASE_METRICS)

    def test_invalid_text_encoding_is_reported_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "target.json"
            path.write_bytes(b"\x80\x81\x82")
            with self.assertRaisesRegex(
                SignalParityError,
                "cannot decode target calculation JSON",
            ):
                _read_json_object(path)

    def test_target_metrics_reject_missing_required_field(self) -> None:
        value = dict(BASE_METRICS)
        value.pop("entry_price")
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "target.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/compare_signal_shadow.py"),
                    "--legacy-price-dir",
                    str(Path(temp)),
                    "--target-json",
                    str(path),
                ],
                cwd=ROOT,
                env=dict(os.environ),
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("missing required fields", result.stderr)

    def test_help_does_not_import_legacy_runtime(self) -> None:
        environment = dict(os.environ)
        for name in (
            "IB_HOST",
            "IB_PORT",
            "IB_CLIENT_ID",
            "IB_ACCOUNT_ID",
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
        ):
            environment.pop(name, None)
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/compare_signal_shadow.py"),
                "--help",
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "Compare one stored target signal calculation",
            result.stdout,
        )
        self.assertIn(
            "implementation on the same explicit timestamp",
            result.stdout,
        )


if __name__ == "__main__":
    unittest.main()
