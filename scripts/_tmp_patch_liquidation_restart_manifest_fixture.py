from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "tester/target_cutover_preflight_tester.py"
text = path.read_text(encoding="utf-8")
old = '''            "restart_actions": [\n                "CANCEL_TAKE_PROFIT",\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ],\n'''
new = '''            "restart_actions": [\n                "CANCEL_TAKE_PROFIT",\n                "CANCEL_STOP",\n                "SUBMIT_MARKET_CLOSE",\n            ],\n            "protective_cancel_mode": "EXPLICIT_BOTH",\n            "intentional_process_terminations": 3,\n            "broker_mutation_count": 3,\n'''
if text.count(old) != 1:
    raise SystemExit(f"expected one fixture match, found {text.count(old)}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
