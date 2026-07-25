from pathlib import Path

path = Path("tester/target_execution_daily_risk_tester.py")
text = path.read_text(encoding="utf-8")
for value in (
    "    StrategyPositionSide,\n",
    "    StrategyPositionStatus,\n",
    "    StrategyPositionV1,\n",
    "    T1,\n",
):
    if text.count(value) != 1:
        raise SystemExit(f"expected one unused import: {value!r}")
    text = text.replace(value, "", 1)
path.write_text(text, encoding="utf-8")
