"""
stop_loss_replay.py

Простой офлайн-тестер фиксированного стоп-лосса по уже исполненным сделкам из trade.sqlite3.

Как использовать:
1. Положи файл в корень проекта IBMarketData.
2. В блоке НАСТРОЙКИ ниже выставь нужные стопы и фильтры.
3. Запусти файл из IDE / PyCharm / двойным кликом / python stop_loss_replay.py.
   Аргументы командной строки не используются.

Что делает:
- читает EXECUTED trade_intents;
- собирает сделки entry -> exit;
- берёт 5-секундные mid OHLC из job DB: data/features/<instrument>_job.sqlite3, table mid_price_5s;
- проверяет, был бы задет фиксированный стоп между входом и реальным выходом;
- сохраняет два CSV:
  1) summary по стопам;
  2) детали по каждой сделке и каждому стопу.

Важно:
- Внутри 5-секундной свечи порядок high/low неизвестен.
  Если свеча коснулась stop-loss уровня до реального exit time, скрипт считает стоп сработавшим.
  Это консервативная оценка.
"""

import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BASE_DIR

# ============================================================
# НАСТРОЙКИ
# ============================================================

# БД с заявками/сделками.
TRADE_DB_PATH = BASE_DIR / 'data' / "trade.sqlite3"

# Какие инструменты анализировать.
# None = все инструменты, найденные в trade_intents.
INSTRUMENTS: list[str] | None = ["MNQ", "MES"]

# Диапазон trade_intent_id.
# Удобно, если хочешь проверить только свежий кусок истории.
MIN_TRADE_INTENT_ID: int | None = None
MAX_TRADE_INTENT_ID: int | None = None

# Стопы в пунктах.
# Можно задать один стоп: [30]
# Или сетку: [10, 15, 20, 25, 30, 35, 40, 50, 60, 75, 100]
STOP_LOSS_POINTS_LIST: list[float] = [0,50, 100, 150, 200]

# Учитывать ли комиссии в USD-итогах.
# Комиссии берутся из trade_intents.total_commission entry + exit, если есть.
INCLUDE_COMMISSIONS_IN_USD: bool = True

# Источник цен для проверки стопа.
# Рекомендуемый вариант: feature/job DB с mid_price_5s.
# Скрипт сам строит путь: Path(settings_live.price_db_dir).parent / "features" / "<db_stem>_job.sqlite3"
USE_FEATURE_DB_MID_PRICE: bool = True

# Если True, при отсутствии feature DB попробует прочитать raw price DB и посчитать mid high/low из bid/ask.
ALLOW_RAW_PRICE_DB_FALLBACK: bool = True

# Отступ по времени для поиска цен вокруг сделки.
# Нужен на случай, если fill time слегка не совпал с bar_time_ts.
PRICE_LOOKBACK_SECONDS_BEFORE_ENTRY: int = 10
PRICE_LOOKAHEAD_SECONDS_AFTER_EXIT: int = 10

# Куда писать отчёты.
OUTPUT_DIR = Path("data/stop_loss_reports")

# Имена файлов отчётов.
SUMMARY_CSV_NAME = "stop_loss_summary.csv"
DETAILS_CSV_NAME = "stop_loss_trade_details.csv"

# Печатать топ результатов в консоль.
PRINT_TOP_N: int = 10


# ============================================================
# МОДЕЛИ
# ============================================================

@dataclass(frozen=True)
class IntentRow:
    trade_intent_id: int
    source_signal_id: int
    instrument_code: str
    signal_bar_ts: int
    intent_source: str
    action: str
    action_reason: str
    target_side: str
    target_qty: float
    position_before_side: str
    position_before_qty: float
    order_type: str
    status: str
    avg_fill_price: float
    total_commission: float | None
    realized_pnl: float | None
    created_at_ts: int
    sent_at_ts: int | None
    finished_at_ts: int
    order_ref: str | None


@dataclass(frozen=True)
class Trade:
    instrument_code: str
    side: str
    qty: float

    entry_trade_intent_id: int
    exit_trade_intent_id: int

    entry_ts: int
    exit_ts: int

    entry_price: float
    exit_price: float

    entry_source: str
    exit_source: str
    exit_action: str
    exit_reason: str

    entry_commission: float | None
    exit_commission: float | None
    exit_realized_pnl: float | None

    multiplier: float


@dataclass(frozen=True)
class PriceBar:
    ts: int
    high: float
    low: float


@dataclass(frozen=True)
class StopResult:
    stop_points: float
    trade: Trade
    stop_hit: bool
    stop_hit_ts: int | None
    simulated_exit_price: float
    actual_points: float
    simulated_points: float
    actual_usd: float
    simulated_usd: float


# ============================================================
# ОБЩИЕ УТИЛИТЫ
# ============================================================

def ts_to_utc(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def side_sign(side: str) -> int:
    side = str(side).upper()
    if side == "LONG":
        return 1
    if side == "SHORT":
        return -1
    raise ValueError(f"Unknown side: {side!r}")


def points_for_trade(side: str, entry_price: float, exit_price: float) -> float:
    return (float(exit_price) - float(entry_price)) * side_sign(side)


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


# ============================================================
# PROJECT IMPORTS
# ============================================================

def load_project_context():
    """
    Импорты спрятаны в функцию, чтобы ошибка была понятнее, если скрипт запущен не из корня проекта.
    """
    try:
        from config import settings_live as settings
        from contracts import Instrument
        from core.instrument_db import get_instrument_db_path, get_instrument_table_name
    except Exception as exc:
        raise RuntimeError(
            "Не удалось импортировать project modules. Запускай скрипт из корня IBMarketData. "
            f"Original error: {type(exc).__name__}: {exc}"
        ) from exc

    return settings, Instrument, get_instrument_db_path, get_instrument_table_name


# ============================================================
# ЧТЕНИЕ TRADE DB
# ============================================================

def read_executed_position_intents(trade_db_path: Path) -> list[IntentRow]:
    conn = sqlite3.connect(str(trade_db_path))
    try:
        where = [
            "status = 'EXECUTED'",
            "action IN ('OPEN_POSITION', 'CLOSE_POSITION', 'REVERSE_POSITION')",
            "avg_fill_price IS NOT NULL",
            "finished_at_ts IS NOT NULL",
        ]
        params: list[Any] = []

        if INSTRUMENTS is not None:
            placeholders = ",".join("?" for _ in INSTRUMENTS)
            where.append(f"instrument_code IN ({placeholders})")
            params.extend(INSTRUMENTS)

        if MIN_TRADE_INTENT_ID is not None:
            where.append("trade_intent_id >= ?")
            params.append(int(MIN_TRADE_INTENT_ID))

        if MAX_TRADE_INTENT_ID is not None:
            where.append("trade_intent_id <= ?")
            params.append(int(MAX_TRADE_INTENT_ID))

        sql = f"""
            SELECT
                trade_intent_id,
                source_signal_id,
                instrument_code,
                signal_bar_ts,
                intent_source,
                action,
                action_reason,
                target_side,
                target_qty,
                position_before_side,
                position_before_qty,
                order_type,
                status,
                avg_fill_price,
                total_commission,
                realized_pnl,
                created_at_ts,
                sent_at_ts,
                finished_at_ts,
                order_ref
            FROM trade_intents
            WHERE {" AND ".join(where)}
            ORDER BY finished_at_ts ASC, trade_intent_id ASC
        """

        rows = conn.execute(sql, params).fetchall()

        result: list[IntentRow] = []
        for row in rows:
            result.append(IntentRow(
                trade_intent_id=int(row[0]),
                source_signal_id=int(row[1]),
                instrument_code=str(row[2]),
                signal_bar_ts=int(row[3]),
                intent_source=str(row[4]),
                action=str(row[5]).upper(),
                action_reason=str(row[6]),
                target_side=str(row[7]).upper(),
                target_qty=float(row[8]),
                position_before_side=str(row[9]).upper(),
                position_before_qty=float(row[10]),
                order_type=str(row[11]).upper(),
                status=str(row[12]).upper(),
                avg_fill_price=float(row[13]),
                total_commission=safe_float(row[14]),
                realized_pnl=safe_float(row[15]),
                created_at_ts=int(row[16]),
                sent_at_ts=None if row[17] is None else int(row[17]),
                finished_at_ts=int(row[18]),
                order_ref=None if row[19] is None else str(row[19]),
            ))

        return result

    finally:
        conn.close()


def reconstruct_trades(intents: list[IntentRow], instrument_registry: dict[str, dict[str, Any]]) -> list[Trade]:
    """
    Собирает сделки из последовательности EXECUTED position-intents.

    Логика:
    - OPEN_POSITION открывает новую сделку.
    - CLOSE_POSITION закрывает текущую сделку.
    - REVERSE_POSITION закрывает текущую сделку и сразу открывает новую в target_side.
    """
    trades: list[Trade] = []
    open_by_instrument: dict[str, IntentRow] = {}

    for intent in intents:
        instrument_code = intent.instrument_code
        multiplier = float(instrument_registry.get(instrument_code, {}).get("multiplier", 1.0))

        if intent.action == "OPEN_POSITION":
            if intent.target_side not in {"LONG", "SHORT"} or intent.target_qty <= 0:
                continue

            if instrument_code in open_by_instrument:
                # Некорректная история: новая позиция без закрытия старой.
                # Не закрываем принудительно, просто заменяем open state свежей записью.
                print(
                    f"WARNING: {instrument_code}: OPEN_POSITION while previous trade is still open. "
                    f"previous_entry_id={open_by_instrument[instrument_code].trade_intent_id}, "
                    f"new_entry_id={intent.trade_intent_id}. Previous trade skipped."
                )

            open_by_instrument[instrument_code] = intent
            continue

        if intent.action == "CLOSE_POSITION":
            entry = open_by_instrument.pop(instrument_code, None)

            if entry is None:
                print(
                    f"WARNING: {instrument_code}: CLOSE_POSITION without open trade. "
                    f"close_id={intent.trade_intent_id}. Skipped."
                )
                continue

            trades.append(build_trade(entry, intent, multiplier))
            continue

        if intent.action == "REVERSE_POSITION":
            entry = open_by_instrument.pop(instrument_code, None)

            if entry is not None:
                trades.append(build_trade(entry, intent, multiplier))

            if intent.target_side in {"LONG", "SHORT"} and intent.target_qty > 0:
                open_by_instrument[instrument_code] = intent

            continue

    for instrument_code, entry in open_by_instrument.items():
        print(
            f"WARNING: {instrument_code}: open trade has no exit yet. "
            f"entry_id={entry.trade_intent_id}. Skipped."
        )

    return trades


def build_trade(entry: IntentRow, exit_intent: IntentRow, multiplier: float) -> Trade:
    return Trade(
        instrument_code=entry.instrument_code,
        side=entry.target_side,
        qty=float(entry.target_qty),
        entry_trade_intent_id=entry.trade_intent_id,
        exit_trade_intent_id=exit_intent.trade_intent_id,
        entry_ts=entry.finished_at_ts,
        exit_ts=exit_intent.finished_at_ts,
        entry_price=float(entry.avg_fill_price),
        exit_price=float(exit_intent.avg_fill_price),
        entry_source=entry.intent_source,
        exit_source=exit_intent.intent_source,
        exit_action=exit_intent.action,
        exit_reason=exit_intent.action_reason,
        entry_commission=entry.total_commission,
        exit_commission=exit_intent.total_commission,
        exit_realized_pnl=exit_intent.realized_pnl,
        multiplier=float(multiplier),
    )


# ============================================================
# ЧТЕНИЕ ЦЕН
# ============================================================

def get_feature_db_path(settings, instrument_registry: dict[str, dict[str, Any]], instrument_code: str) -> Path:
    row = instrument_registry[instrument_code]
    price_db_filename = row["db_filename"]
    feature_db_stem = Path(price_db_filename).stem.lower()
    return Path(settings.price_db_dir).parent / "features" / f"{feature_db_stem}_job.sqlite3"


def get_price_source_for_instrument(settings, instrument_registry, get_instrument_db_path, get_instrument_table_name, instrument_code: str):
    """
    Возвращает (db_path, table_name, mode)
    mode:
      - feature_mid: таблица mid_price_5s с mid_high/mid_low
      - raw_bid_ask: raw price DB с bid_high/ask_high/bid_low/ask_low
      - raw_ohlc: raw таблица с high/low
    """
    if instrument_code not in instrument_registry:
        raise ValueError(f"Instrument {instrument_code!r} is not found in contracts.py")

    if USE_FEATURE_DB_MID_PRICE:
        feature_db_path = get_feature_db_path(settings, instrument_registry, instrument_code)
        if feature_db_path.is_file():
            return feature_db_path, "mid_price_5s", "feature_mid"

        if not ALLOW_RAW_PRICE_DB_FALLBACK:
            raise FileNotFoundError(f"Feature DB not found: {feature_db_path}")

    row = instrument_registry[instrument_code]
    price_db_path = Path(get_instrument_db_path(settings, instrument_code, row))
    table_name = get_instrument_table_name(instrument_code, row)

    if not price_db_path.is_file():
        raise FileNotFoundError(
            f"Neither feature DB nor raw price DB exists for {instrument_code}: "
            f"feature={get_feature_db_path(settings, instrument_registry, instrument_code)}, raw={price_db_path}"
        )

    return price_db_path, table_name, "raw_auto"


def read_price_bars(
        settings,
        instrument_registry,
        get_instrument_db_path,
        get_instrument_table_name,
        instrument_code: str,
        start_ts: int,
        end_ts: int,
) -> list[PriceBar]:
    db_path, table_name, mode = get_price_source_for_instrument(
        settings,
        instrument_registry,
        get_instrument_db_path,
        get_instrument_table_name,
        instrument_code,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()}

        if mode == "feature_mid" or {"mid_high", "mid_low"}.issubset(columns):
            sql = f"""
                SELECT bar_time_ts, mid_high, mid_low
                FROM {quote_identifier(table_name)}
                WHERE bar_time_ts BETWEEN ? AND ?
                ORDER BY bar_time_ts ASC
            """

        elif {"bid_high", "ask_high", "bid_low", "ask_low"}.issubset(columns):
            sql = f"""
                SELECT
                    bar_time_ts,
                    (bid_high + ask_high) / 2.0 AS mid_high,
                    (bid_low + ask_low) / 2.0 AS mid_low
                FROM {quote_identifier(table_name)}
                WHERE bar_time_ts BETWEEN ? AND ?
                  AND bid_high IS NOT NULL
                  AND ask_high IS NOT NULL
                  AND bid_low IS NOT NULL
                  AND ask_low IS NOT NULL
                ORDER BY bar_time_ts ASC
            """

        elif {"high", "low"}.issubset(columns):
            sql = f"""
                SELECT bar_time_ts, high, low
                FROM {quote_identifier(table_name)}
                WHERE bar_time_ts BETWEEN ? AND ?
                ORDER BY bar_time_ts ASC
            """

        else:
            raise RuntimeError(
                f"Cannot detect high/low columns in {db_path}::{table_name}. "
                f"Columns: {sorted(columns)}"
            )

        rows = conn.execute(sql, (int(start_ts), int(end_ts))).fetchall()

        return [
            PriceBar(ts=int(row[0]), high=float(row[1]), low=float(row[2]))
            for row in rows
        ]

    finally:
        conn.close()


# ============================================================
# STOP LOSS SIMULATION
# ============================================================

def find_stop_hit(trade: Trade, stop_points: float, bars: list[PriceBar]) -> tuple[bool, int | None, float]:
    """
    Возвращает: stop_hit, stop_hit_ts, simulated_exit_price.
    Если stop_points <= 0 — режим без стопа: выход по фактическому exit.
    Если стоп не задет — simulated_exit_price = actual exit.
    """
    if float(stop_points) <= 0.0:
        return False, None, trade.exit_price

    sign = side_sign(trade.side)

    if sign > 0:
        stop_price = trade.entry_price - float(stop_points)
        for bar in bars:
            if bar.low <= stop_price:
                return True, bar.ts, stop_price

    else:
        stop_price = trade.entry_price + float(stop_points)
        for bar in bars:
            if bar.high >= stop_price:
                return True, bar.ts, stop_price

    return False, None, trade.exit_price


def simulate_trade_stop(trade: Trade, stop_points: float, bars: list[PriceBar]) -> StopResult:
    stop_hit, stop_hit_ts, simulated_exit_price = find_stop_hit(trade, stop_points, bars)

    actual_points = points_for_trade(trade.side, trade.entry_price, trade.exit_price)
    simulated_points = points_for_trade(trade.side, trade.entry_price, simulated_exit_price)

    gross_actual_usd = actual_points * trade.qty * trade.multiplier
    gross_simulated_usd = simulated_points * trade.qty * trade.multiplier

    commission = 0.0
    if INCLUDE_COMMISSIONS_IN_USD:
        commission += float(trade.entry_commission or 0.0)
        commission += float(trade.exit_commission or 0.0)

    actual_usd = gross_actual_usd - commission
    simulated_usd = gross_simulated_usd - commission

    return StopResult(
        stop_points=float(stop_points),
        trade=trade,
        stop_hit=stop_hit,
        stop_hit_ts=stop_hit_ts,
        simulated_exit_price=simulated_exit_price,
        actual_points=actual_points,
        simulated_points=simulated_points,
        actual_usd=actual_usd,
        simulated_usd=simulated_usd,
    )


def simulate_all(trades: list[Trade]) -> list[StopResult]:
    settings, instrument_registry, get_instrument_db_path, get_instrument_table_name = load_project_context()

    results: list[StopResult] = []
    trades_by_instrument: dict[str, list[Trade]] = {}

    for trade in trades:
        trades_by_instrument.setdefault(trade.instrument_code, []).append(trade)

    for instrument_code, instrument_trades in sorted(trades_by_instrument.items()):
        if not instrument_trades:
            continue

        min_ts = min(t.entry_ts for t in instrument_trades) - int(PRICE_LOOKBACK_SECONDS_BEFORE_ENTRY)
        max_ts = max(t.exit_ts for t in instrument_trades) + int(PRICE_LOOKAHEAD_SECONDS_AFTER_EXIT)

        bars = read_price_bars(
            settings=settings,
            instrument_registry=instrument_registry,
            get_instrument_db_path=get_instrument_db_path,
            get_instrument_table_name=get_instrument_table_name,
            instrument_code=instrument_code,
            start_ts=min_ts,
            end_ts=max_ts,
        )

        if not bars:
            print(f"WARNING: {instrument_code}: no price bars found for interval {ts_to_utc(min_ts)}..{ts_to_utc(max_ts)}")
            continue

        # Для быстрого отбора баров по сделке.
        bar_by_ts = bars
        for trade in instrument_trades:
            trade_start = trade.entry_ts - int(PRICE_LOOKBACK_SECONDS_BEFORE_ENTRY)
            trade_end = trade.exit_ts + int(PRICE_LOOKAHEAD_SECONDS_AFTER_EXIT)
            trade_bars = [b for b in bar_by_ts if trade_start <= b.ts <= trade_end]

            if not trade_bars:
                print(
                    f"WARNING: {instrument_code}: no price bars for trade "
                    f"{trade.entry_trade_intent_id}->{trade.exit_trade_intent_id}"
                )
                continue

            for stop_points in STOP_LOSS_POINTS_LIST:
                results.append(simulate_trade_stop(trade, float(stop_points), trade_bars))

    return results


# ============================================================
# ОТЧЁТЫ
# ============================================================

def build_summary(results: list[StopResult]) -> list[dict[str, Any]]:
    by_stop: dict[float, list[StopResult]] = {}

    for result in results:
        by_stop.setdefault(float(result.stop_points), []).append(result)

    rows: list[dict[str, Any]] = []

    for stop_points, group in sorted(by_stop.items()):
        trade_count = len(group)
        stop_hits = sum(1 for r in group if r.stop_hit)
        actual_points_sum = sum(r.actual_points * r.trade.qty for r in group)
        simulated_points_sum = sum(r.simulated_points * r.trade.qty for r in group)
        actual_usd_sum = sum(r.actual_usd for r in group)
        simulated_usd_sum = sum(r.simulated_usd for r in group)
        delta_points = simulated_points_sum - actual_points_sum
        delta_usd = simulated_usd_sum - actual_usd_sum

        wins = sum(1 for r in group if r.simulated_points > 0)
        losses = sum(1 for r in group if r.simulated_points < 0)
        flats = trade_count - wins - losses

        rows.append({
            "stop_points": stop_points,
            "trade_count": trade_count,
            "stop_hits": stop_hits,
            "stop_hit_rate_pct": round(stop_hits / trade_count * 100.0, 2) if trade_count else 0.0,
            "sim_wins": wins,
            "sim_losses": losses,
            "sim_flats": flats,
            "actual_points_sum": round(actual_points_sum, 2),
            "simulated_points_sum": round(simulated_points_sum, 2),
            "delta_points": round(delta_points, 2),
            "actual_usd_sum": round(actual_usd_sum, 2),
            "simulated_usd_sum": round(simulated_usd_sum, 2),
            "delta_usd": round(delta_usd, 2),
            "avg_simulated_points": round(simulated_points_sum / trade_count, 2) if trade_count else 0.0,
            "avg_simulated_usd": round(simulated_usd_sum / trade_count, 2) if trade_count else 0.0,
        })

    rows.sort(key=lambda x: (x["simulated_usd_sum"], x["simulated_points_sum"]), reverse=True)
    return rows


def build_detail_rows(results: list[StopResult]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for r in results:
        t = r.trade
        rows.append({
            "stop_points": r.stop_points,
            "instrument_code": t.instrument_code,
            "side": t.side,
            "qty": t.qty,
            "entry_trade_intent_id": t.entry_trade_intent_id,
            "exit_trade_intent_id": t.exit_trade_intent_id,
            "entry_time_utc": ts_to_utc(t.entry_ts),
            "exit_time_utc": ts_to_utc(t.exit_ts),
            "entry_ts": t.entry_ts,
            "exit_ts": t.exit_ts,
            "entry_price": t.entry_price,
            "actual_exit_price": t.exit_price,
            "simulated_exit_price": r.simulated_exit_price,
            "stop_hit": int(r.stop_hit),
            "stop_hit_time_utc": ts_to_utc(r.stop_hit_ts),
            "stop_hit_ts": r.stop_hit_ts or "",
            "actual_points": round(r.actual_points, 2),
            "simulated_points": round(r.simulated_points, 2),
            "delta_points": round(r.simulated_points - r.actual_points, 2),
            "actual_usd": round(r.actual_usd, 2),
            "simulated_usd": round(r.simulated_usd, 2),
            "delta_usd": round(r.simulated_usd - r.actual_usd, 2),
            "entry_source": t.entry_source,
            "exit_source": t.exit_source,
            "exit_action": t.exit_action,
            "exit_reason": t.exit_reason,
            "exit_realized_pnl_from_db": "" if t.exit_realized_pnl is None else round(t.exit_realized_pnl, 2),
        })

    rows.sort(key=lambda x: (x["stop_points"], x["entry_ts"], x["entry_trade_intent_id"]))
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    settings, instrument_registry, _, _ = load_project_context()

    if not TRADE_DB_PATH.is_file():
        raise FileNotFoundError(f"Trade DB not found: {TRADE_DB_PATH.resolve()}")

    print("=" * 80)
    print("STOP LOSS REPLAY")
    print("=" * 80)
    print(f"Trade DB: {TRADE_DB_PATH.resolve()}")
    print(f"Price DB dir: {Path(settings.price_db_dir).resolve()}")
    print(f"Feature DB dir: {(Path(settings.price_db_dir).parent / 'features').resolve()}")
    print(f"Instruments: {INSTRUMENTS if INSTRUMENTS is not None else 'ALL'}")
    print(f"Stop list: {STOP_LOSS_POINTS_LIST}")
    print(f"Intent ID range: {MIN_TRADE_INTENT_ID}..{MAX_TRADE_INTENT_ID}")
    print()

    intents = read_executed_position_intents(TRADE_DB_PATH)
    trades = reconstruct_trades(intents, instrument_registry)

    print(f"Executed position intents: {len(intents)}")
    print(f"Reconstructed closed trades: {len(trades)}")

    if not trades:
        print("Нет закрытых сделок для анализа.")
        return

    results = simulate_all(trades)
    summary_rows = build_summary(results)
    detail_rows = build_detail_rows(results)

    summary_path = OUTPUT_DIR / SUMMARY_CSV_NAME
    details_path = OUTPUT_DIR / DETAILS_CSV_NAME

    write_csv(summary_path, summary_rows)
    write_csv(details_path, detail_rows)

    print()
    print("TOP STOP LOSS RESULTS")
    print("-" * 80)

    for row in summary_rows[: int(PRINT_TOP_N)]:
        print(
            f"SL={row['stop_points']:>7g} pt | "
            f"sim_usd={row['simulated_usd_sum']:>10.2f} | "
            f"delta_usd={row['delta_usd']:>10.2f} | "
            f"sim_points={row['simulated_points_sum']:>9.2f} | "
            f"hits={row['stop_hits']:>3}/{row['trade_count']} "
            f"({row['stop_hit_rate_pct']:>5.2f}%)"
        )

    print()
    print(f"Summary CSV: {summary_path.resolve()}")
    print(f"Details CSV: {details_path.resolve()}")
    print("=" * 80)


if __name__ == "__main__":
    main()
