from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected one replacement in {path}: {old[:120]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/execution/domain/daily_risk.py",
    '''def _realized_pnl(
    fills: tuple[DailyRiskOwnedFillV1, ...],
) -> tuple[float | None, tuple[str, ...], str | None]:
''',
    '''def _realized_pnl(
    fills: tuple[DailyRiskOwnedFillV1, ...],
    *,
    expected_currency: str,
) -> tuple[float | None, tuple[str, ...], str | None]:
''',
)

replace_once(
    "src/ibmd/execution/domain/daily_risk.py",
    '''        if commission is None:
            missing.append(fill.exec_id)
            continue
        if item.kind == DailyRiskFillKind.STRATEGIC_OPEN:
''',
    '''        if commission is None:
            missing.append(fill.exec_id)
            continue
        if commission.currency != expected_currency:
            return (
                None,
                (),
                "owned execution commission currency differs from daily-risk "
                f"currency: exec_id={fill.exec_id}, expected={expected_currency}, "
                f"actual={commission.currency}",
            )
        if item.kind == DailyRiskFillKind.STRATEGIC_OPEN:
''',
)

replace_once(
    "src/ibmd/execution/domain/daily_risk.py",
    '''    realized, missing_exec_ids, realized_error = _realized_pnl(daily_fills)
''',
    '''    realized, missing_exec_ids, realized_error = _realized_pnl(
        daily_fills,
        expected_currency="USD",
    )
''',
)

replace_once(
    "tester/target_execution_daily_risk_tester.py",
    '''    def test_trigger_is_sticky_even_when_mark_retraces(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
''',
    '''    def test_non_usd_commission_is_not_ready(self) -> None:
        value = plan()
        wrong_currency = BrokerCommissionFactV1(
            exec_id="daily-risk-exec-1",
            commission=1.25,
            currency="EUR",
            realized_pnl=0.0,
            reported_at_utc=T4,
        )
        update = calculate_daily_risk(
            policy=policy(),
            owned_fills=(owned_fill(commission_fact=wrong_currency),),
            position=value.strategy_position,
            episode=value.episode,
            market_mark=mark(28_610.0),
            current_state=None,
            current_readiness=ready_readiness(),
            liquidation=None,
            observed_at_utc=T5,
        )
        self.assertEqual(update.state.status, DailyRiskStatus.NOT_READY)
        self.assertEqual(
            update.calculation.reason_code,
            "EXECUTION_EVIDENCE_INCOMPLETE",
        )
        self.assertIn(
            "currency",
            update.calculation.reason_detail,
        )

    def test_trigger_is_sticky_even_when_mark_retraces(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
''',
)
