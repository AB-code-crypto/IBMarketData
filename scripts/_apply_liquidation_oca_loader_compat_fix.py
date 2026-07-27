from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "src/ibmd/operations/paper_liquidation_acceptance.py"
text = PATH.read_text(encoding="utf-8")


def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one occurrence, found {count}: {old!r}")
    text = text.replace(old, new, 1)


replace_once(
    "    def _load_entry_summary(self) -> tuple[str, str, str]:\n",
    "    def _load_entry_summary(self) -> tuple[str, str]:\n",
)
replace_once(
    "            ),\n"
    "            str(protection.get(\"take_profit_state\")),\n"
    "        )\n\n"
    "    def _request_arguments(\n",
    "            ),\n"
    "        )\n\n"
    "    def _entry_take_profit_state(self) -> str:\n"
    "        try:\n"
    "            value = read_json_object(self.policy.paths.entry_summary)\n"
    "        except Exception as exc:\n"
    "            raise PaperLiquidationAcceptanceError(\n"
    "                f\"cannot re-read entry acceptance summary: {exc}\",\n"
    "                stage=\"entry-summary\",\n"
    "            ) from exc\n"
    "        protection = self._mapping(\n"
    "            value.get(\"protection\"),\n"
    "            field_name=\"protection\",\n"
    "            stage=\"entry-summary\",\n"
    "        )\n"
    "        state = str(protection.get(\"take_profit_state\") or \"\").strip()\n"
    "        if state not in {\"LIVE\", \"NOT_REQUIRED\"}:\n"
    "            raise PaperLiquidationAcceptanceError(\n"
    "                \"entry summary has invalid TAKE PROFIT state\",\n"
    "                stage=\"entry-summary\",\n"
    "            )\n"
    "        return state\n\n"
    "    def _request_arguments(\n",
)
replace_once(
    "        (\n"
    "            source_drill_id,\n"
    "            position_episode_id,\n"
    "            entry_take_profit_state,\n"
    "        ) = self._load_entry_summary()\n",
    "        source_drill_id, position_episode_id = self._load_entry_summary()\n"
    "        entry_take_profit_state = self._entry_take_profit_state()\n",
)
PATH.write_text(text, encoding="utf-8")
