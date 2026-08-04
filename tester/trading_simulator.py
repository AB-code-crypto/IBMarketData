from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, time, timezone
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from tester.models import (
    CompletedTrade,
    DailyResult,
    ExecutionVariant,
    PriceBar,
    SimulationResult,
    TesterSignal,
)


MSK_TIMEZONE = ZoneInfo("Europe/Moscow")
CT_TIMEZONE = ZoneInfo("America/Chicago")
FUTURES_DAILY_FLAT_START_CT = time(14, 59, 50)
FUTURES_CLEARING_END_CT = time(16, 0, 0)


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def format_ts_msk(ts: int) -> str:
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .astimezone(MSK_TIMEZONE)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def get_moscow_day(ts: int) -> str:
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .astimezone(MSK_TIMEZONE)
        .strftime("%Y-%m-%d")
    )


def is_futures_daily_flat_blocked(ts: int) -> bool:
    current_ct = (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .astimezone(CT_TIMEZONE)
        .time()
        .replace(tzinfo=None)
    )
    return FUTURES_DAILY_FLAT_START_CT <= current_ct < FUTURES_CLEARING_END_CT


def load_price_bars(
        *,
        db_path: Path,
        table_name: str,
        start_ts: int,
        end_ts: int,
) -> list[PriceBar]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close
            FROM {quote_identifier(table_name)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
              AND bid_open IS NOT NULL
              AND bid_high IS NOT NULL
              AND bid_low IS NOT NULL
              AND bid_close IS NOT NULL
              AND ask_open IS NOT NULL
              AND ask_high IS NOT NULL
              AND ask_low IS NOT NULL
              AND ask_close IS NOT NULL
            ORDER BY bar_time_ts
            """,
            (int(start_ts), int(end_ts)),
        ).fetchall()
    finally:
        conn.close()

    return [
        PriceBar(
            bar_time_ts=int(row[0]),
            bid_open=float(row[1]),
            bid_high=float(row[2]),
            bid_low=float(row[3]),
            bid_close=float(row[4]),
            ask_open=float(row[5]),
            ask_high=float(row[6]),
            ask_low=float(row[7]),
            ask_close=float(row[8]),
        )
        for row in rows
    ]


def calculate_max_drawdown(net_results: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in net_results:
        equity += float(value)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return float(max_drawdown)


def calculate_max_consecutive_losses(net_results: list[float]) -> int:
    current = 0
    maximum = 0
    for value in net_results:
        if float(value) < 0.0:
            current += 1
            maximum = max(maximum, current)
        else:
            current = 0
    return maximum


def build_metrics(
        *,
        trades: list[CompletedTrade],
        signals_count: int,
        counters: Counter,
        total_commission_usd: float,
) -> dict[str, float | int | None]:
    net_results = [float(trade.net_pnl_usd) for trade in trades]
    gross_results = [float(trade.gross_pnl_usd) for trade in trades]
    winners = [value for value in net_results if value > 0.0]
    losers = [value for value in net_results if value < 0.0]
    gross_profit = sum(value for value in net_results if value > 0.0)
    gross_loss = sum(value for value in net_results if value < 0.0)
    exit_counts = Counter(trade.exit_reason for trade in trades)
    long_results = [
        trade.net_pnl_usd for trade in trades if trade.direction == "LONG"
    ]
    short_results = [
        trade.net_pnl_usd for trade in trades if trade.direction == "SHORT"
    ]

    return {
        "net_profit_usd": float(sum(net_results)),
        "gross_trade_pnl_usd": float(sum(gross_results)),
        "gross_profit_usd": float(gross_profit),
        "gross_loss_usd": float(gross_loss),
        "total_commission_usd": float(total_commission_usd),
        "trades_count": len(trades),
        "winning_trades_count": len(winners),
        "losing_trades_count": len(losers),
        "win_rate": (len(winners) / len(trades)) if trades else 0.0,
        "profit_factor": (
            float(gross_profit / abs(gross_loss)) if gross_loss < 0.0 else None
        ),
        "average_trade_usd": (
            float(sum(net_results) / len(net_results)) if net_results else 0.0
        ),
        "median_trade_usd": float(median(net_results)) if net_results else 0.0,
        "max_drawdown_usd": calculate_max_drawdown(net_results),
        "max_consecutive_losses": calculate_max_consecutive_losses(net_results),
        "long_trades_count": len(long_results),
        "long_net_profit_usd": float(sum(long_results)),
        "short_trades_count": len(short_results),
        "short_net_profit_usd": float(sum(short_results)),
        "signals_count": int(signals_count),
        "executed_signals_count": int(counters["executed_signals"]),
        "ignored_same_direction_count": int(counters["ignored_same_direction"]),
        "ignored_daily_flat_count": int(counters["ignored_daily_flat"]),
        "ignored_daily_take_profit_count": int(
            counters["ignored_daily_take_profit"]
        ),
        "ignored_missing_execution_bar_count": int(
            counters["ignored_missing_execution_bar"]
        ),
        "ambiguous_tp_sl_bars_count": int(counters["ambiguous_tp_sl"]),
        "take_profit_exits_count": int(exit_counts["TAKE_PROFIT"]),
        "stop_loss_exits_count": int(exit_counts["STOP_LOSS"]),
        "reverse_exits_count": int(exit_counts["REVERSE"]),
        "daily_flat_exits_count": int(exit_counts["DAILY_FLAT"]),
        "daily_take_profit_exits_count": int(
            exit_counts["DAILY_TAKE_PROFIT"]
        ),
        "test_end_exits_count": int(exit_counts["TEST_END"]),
        "average_mfe_points": (
            float(sum(trade.mfe_points for trade in trades) / len(trades))
            if trades
            else 0.0
        ),
        "average_mae_points": (
            float(sum(trade.mae_points for trade in trades) / len(trades))
            if trades
            else 0.0
        ),
    }


def simulate_trading(
        *,
        bars: list[PriceBar],
        signals: list[TesterSignal],
        execution: ExecutionVariant,
        commission_per_contract_side_usd: float,
        multiplier_usd_per_point: float,
) -> SimulationResult:
    signals_by_execution_ts: dict[int, list[TesterSignal]] = defaultdict(list)
    for signal in signals:
        signals_by_execution_ts[
            int(signal.signal_bar_ts) + int(execution.delay_seconds)
        ].append(signal)

    available_bar_ts = {bar.bar_time_ts for bar in bars}
    counters: Counter = Counter()
    for execution_ts, rows in signals_by_execution_ts.items():
        if execution_ts not in available_bar_ts:
            counters["ignored_missing_execution_bar"] += len(rows)

    trades: list[CompletedTrade] = []
    daily_results: list[DailyResult] = []
    total_commission_usd = 0.0

    position_side = "FLAT"
    entry_price = 0.0
    entry_ts = 0
    entry_signal_bar_ts = 0
    entry_commission_usd = 0.0
    mfe_points = 0.0
    mae_points = 0.0

    current_day: str | None = None
    day_net_realized_usd = 0.0
    day_commission_usd = 0.0
    day_closed_trades_count = 0
    day_executed_signals_count = 0
    day_take_profit_triggered = False
    daily_take_profit_halted = False

    def finalize_day() -> None:
        if current_day is None:
            return
        daily_results.append(
            DailyResult(
                moscow_day=current_day,
                net_realized_pnl_usd=float(day_net_realized_usd),
                commission_usd=float(day_commission_usd),
                closed_trades_count=int(day_closed_trades_count),
                executed_signals_count=int(day_executed_signals_count),
                daily_take_profit_triggered=bool(day_take_profit_triggered),
            )
        )

    def executable_open_price(side: str, bar: PriceBar) -> float:
        return float(bar.ask_open if side == "LONG" else bar.bid_open)

    def executable_close_price(side: str, bar: PriceBar, *, at_close: bool) -> float:
        if side == "LONG":
            return float(bar.bid_close if at_close else bar.bid_open)
        return float(bar.ask_close if at_close else bar.ask_open)

    def open_position(side: str, bar: PriceBar, signal: TesterSignal) -> None:
        nonlocal position_side
        nonlocal entry_price
        nonlocal entry_ts
        nonlocal entry_signal_bar_ts
        nonlocal entry_commission_usd
        nonlocal mfe_points
        nonlocal mae_points
        nonlocal day_net_realized_usd
        nonlocal day_commission_usd
        nonlocal total_commission_usd

        commission = float(commission_per_contract_side_usd)
        position_side = str(side)
        entry_price = executable_open_price(position_side, bar)
        entry_ts = int(bar.bar_time_ts)
        entry_signal_bar_ts = int(signal.signal_bar_ts)
        entry_commission_usd = commission
        mfe_points = 0.0
        mae_points = 0.0
        day_net_realized_usd -= commission
        day_commission_usd += commission
        total_commission_usd += commission

    def close_position(
            *,
            exit_ts: int,
            exit_price: float,
            exit_reason: str,
            exit_signal_bar_ts: int | None,
    ) -> None:
        nonlocal position_side
        nonlocal entry_price
        nonlocal entry_ts
        nonlocal entry_signal_bar_ts
        nonlocal entry_commission_usd
        nonlocal mfe_points
        nonlocal mae_points
        nonlocal day_net_realized_usd
        nonlocal day_commission_usd
        nonlocal day_closed_trades_count
        nonlocal total_commission_usd

        if position_side == "FLAT":
            return

        if position_side == "LONG":
            gross_points = float(exit_price) - entry_price
        else:
            gross_points = entry_price - float(exit_price)
        gross_pnl_usd = gross_points * float(multiplier_usd_per_point)
        exit_commission_usd = float(commission_per_contract_side_usd)
        net_pnl_usd = (
            gross_pnl_usd - entry_commission_usd - exit_commission_usd
        )

        day_net_realized_usd += gross_pnl_usd - exit_commission_usd
        day_commission_usd += exit_commission_usd
        day_closed_trades_count += 1
        total_commission_usd += exit_commission_usd

        trades.append(
            CompletedTrade(
                direction=position_side,
                entry_ts=entry_ts,
                exit_ts=int(exit_ts),
                entry_time_msk=format_ts_msk(entry_ts),
                exit_time_msk=format_ts_msk(exit_ts),
                entry_price=float(entry_price),
                exit_price=float(exit_price),
                entry_signal_bar_ts=entry_signal_bar_ts,
                exit_signal_bar_ts=exit_signal_bar_ts,
                exit_reason=str(exit_reason),
                gross_points=float(gross_points),
                gross_pnl_usd=float(gross_pnl_usd),
                entry_commission_usd=float(entry_commission_usd),
                exit_commission_usd=float(exit_commission_usd),
                net_pnl_usd=float(net_pnl_usd),
                mfe_points=float(mfe_points),
                mae_points=float(mae_points),
                holding_seconds=max(0, int(exit_ts) - entry_ts),
            )
        )

        position_side = "FLAT"
        entry_price = 0.0
        entry_ts = 0
        entry_signal_bar_ts = 0
        entry_commission_usd = 0.0
        mfe_points = 0.0
        mae_points = 0.0

    def update_excursions(bar: PriceBar) -> None:
        nonlocal mfe_points
        nonlocal mae_points
        if position_side == "LONG":
            mfe_points = max(mfe_points, float(bar.bid_high) - entry_price)
            mae_points = max(mae_points, entry_price - float(bar.bid_low))
        elif position_side == "SHORT":
            mfe_points = max(mfe_points, entry_price - float(bar.ask_low))
            mae_points = max(mae_points, float(bar.ask_high) - entry_price)

    for bar in bars:
        bar_day = get_moscow_day(bar.bar_time_ts)
        if current_day != bar_day:
            finalize_day()
            current_day = bar_day
            day_net_realized_usd = 0.0
            day_commission_usd = 0.0
            day_closed_trades_count = 0
            day_executed_signals_count = 0
            day_take_profit_triggered = False
            daily_take_profit_halted = False

        daily_flat_blocked = is_futures_daily_flat_blocked(bar.bar_time_ts)
        if daily_flat_blocked and position_side != "FLAT":
            close_position(
                exit_ts=bar.bar_time_ts,
                exit_price=executable_close_price(
                    position_side,
                    bar,
                    at_close=False,
                ),
                exit_reason="DAILY_FLAT",
                exit_signal_bar_ts=None,
            )

        due_signals = signals_by_execution_ts.get(bar.bar_time_ts, [])
        for signal in due_signals:
            if daily_flat_blocked:
                counters["ignored_daily_flat"] += 1
                continue
            if daily_take_profit_halted:
                counters["ignored_daily_take_profit"] += 1
                continue
            if position_side == signal.direction:
                counters["ignored_same_direction"] += 1
                continue

            counters["executed_signals"] += 1
            day_executed_signals_count += 1
            if position_side != "FLAT":
                close_position(
                    exit_ts=bar.bar_time_ts,
                    exit_price=executable_close_price(
                        position_side,
                        bar,
                        at_close=False,
                    ),
                    exit_reason="REVERSE",
                    exit_signal_bar_ts=signal.signal_bar_ts,
                )
            open_position(signal.direction, bar, signal)

        if position_side != "FLAT":
            update_excursions(bar)
            take_profit = float(execution.take_profit_points)
            stop_loss = float(execution.stop_loss_points)

            if position_side == "LONG":
                take_profit_price = entry_price + take_profit
                stop_loss_price = entry_price - stop_loss
                take_profit_hit = (
                    take_profit > 0.0 and bar.bid_high >= take_profit_price
                )
                stop_loss_hit = stop_loss > 0.0 and bar.bid_low <= stop_loss_price
            else:
                take_profit_price = entry_price - take_profit
                stop_loss_price = entry_price + stop_loss
                take_profit_hit = (
                    take_profit > 0.0 and bar.ask_low <= take_profit_price
                )
                stop_loss_hit = stop_loss > 0.0 and bar.ask_high >= stop_loss_price

            if take_profit_hit and stop_loss_hit:
                counters["ambiguous_tp_sl"] += 1
                take_profit_hit = False

            if stop_loss_hit:
                close_position(
                    exit_ts=bar.bar_time_ts,
                    exit_price=float(stop_loss_price),
                    exit_reason="STOP_LOSS",
                    exit_signal_bar_ts=None,
                )
            elif take_profit_hit:
                close_position(
                    exit_ts=bar.bar_time_ts,
                    exit_price=float(take_profit_price),
                    exit_reason="TAKE_PROFIT",
                    exit_signal_bar_ts=None,
                )

        daily_target = float(execution.daily_take_profit_usd)
        if daily_target > 0.0 and not daily_take_profit_halted:
            unrealized_usd = 0.0
            if position_side == "LONG":
                unrealized_usd = (
                    float(bar.bid_close) - entry_price
                ) * float(multiplier_usd_per_point)
            elif position_side == "SHORT":
                unrealized_usd = (
                    entry_price - float(bar.ask_close)
                ) * float(multiplier_usd_per_point)

            if day_net_realized_usd + unrealized_usd >= daily_target:
                if position_side != "FLAT":
                    close_position(
                        exit_ts=bar.bar_time_ts + 5,
                        exit_price=executable_close_price(
                            position_side,
                            bar,
                            at_close=True,
                        ),
                        exit_reason="DAILY_TAKE_PROFIT",
                        exit_signal_bar_ts=None,
                    )
                daily_take_profit_halted = True
                day_take_profit_triggered = True

    if bars and position_side != "FLAT":
        last_bar = bars[-1]
        close_position(
            exit_ts=last_bar.bar_time_ts + 5,
            exit_price=executable_close_price(
                position_side,
                last_bar,
                at_close=True,
            ),
            exit_reason="TEST_END",
            exit_signal_bar_ts=None,
        )

    finalize_day()
    metrics = build_metrics(
        trades=trades,
        signals_count=len(signals),
        counters=counters,
        total_commission_usd=total_commission_usd,
    )
    return SimulationResult(
        trades=trades,
        daily_results=daily_results,
        metrics=metrics,
    )


__all__ = [
    "load_price_bars",
    "simulate_trading",
    "is_futures_daily_flat_blocked",
]
