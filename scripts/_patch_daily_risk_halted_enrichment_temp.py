from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected one replacement in {path}: {old[:120]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/execution/domain/daily_risk.py",
    '''    if same_day and prior_status == DailyRiskStatus.HALTED:
        return current_state

    if sticky or carryover:
''',
    '''    if sticky or carryover:
''',
)

replace_once(
    "tester/target_execution_daily_risk_tester.py",
    '''    def test_flat_after_closing_becomes_halted(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
''',
    '''    def test_halted_state_accepts_late_commission_enrichment(self) -> None:
        halted_not_ready = calculate_daily_risk(
            policy=policy(),
            owned_fills=(owned_fill(commission_fact=None),),
            position=flat_position(),
            episode=None,
            market_mark=None,
            current_state=None,
            current_readiness=ready_readiness(),
            liquidation=None,
            observed_at_utc=T4,
        )
        halted_not_ready = replace(
            halted_not_ready.state,
            status=DailyRiskStatus.HALTED,
            cleanup_status=DailyRiskCleanupStatus.COMPLETE,
            updated_at_utc=T4,
        )
        enriched = calculate_daily_risk(
            policy=policy(),
            owned_fills=(
                owned_fill(
                    commission_fact=commission("daily-risk-exec-1")
                ),
            ),
            position=flat_position(),
            episode=None,
            market_mark=None,
            current_state=halted_not_ready,
            current_readiness=ready_readiness(at=T4),
            liquidation=None,
            observed_at_utc=T5,
        )
        self.assertEqual(enriched.state.status, DailyRiskStatus.HALTED)
        self.assertEqual(
            enriched.state.cleanup_status,
            DailyRiskCleanupStatus.COMPLETE,
        )
        self.assertTrue(enriched.state.pnl_ready)
        self.assertEqual(enriched.state.realized_pnl, -1.25)
        self.assertEqual(enriched.state.total_pnl, -1.25)

    def test_flat_after_closing_becomes_halted(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
''',
)
