from pathlib import Path

path = Path("src/ibmd/operations/paper_policy_liquidation_acceptance.py")
text = path.read_text(encoding="utf-8")
old = '''        if payload.get("observed_at_utc") != self.logical_trigger_at_utc:
            raise PaperLiquidationAcceptanceError(
                "policy trigger observation time changed",
                stage="liquidation-request",
            )
'''
new = '''        payload_observed = str(payload.get("observed_at_utc") or "").strip()
        try:
            same_observation = (
                parse_utc(payload_observed)
                == parse_utc(self.logical_trigger_at_utc)
            )
        except ValueError:
            same_observation = False
        if not same_observation:
            raise PaperLiquidationAcceptanceError(
                "policy trigger observation time changed",
                stage="liquidation-request",
            )
'''
if text.count(old) != 1:
    raise SystemExit("expected policy trigger observation comparison")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
