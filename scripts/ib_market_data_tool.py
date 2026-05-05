"""
Единый ручной инструмент для проверки работы с рыночными данными Interactive Brokers.

Назначение
----------
Скрипт нужен для ручной диагностики IB:

- найти conId и проверить, как IB распознаёт конкретный контракт;
- массово пройтись по заглушкам conId=PLACEHOLDER_CON_ID в contracts.py;
- проверить historical request по выбранному инструменту и интервалу;
- получить historical bars в компактном виде;
- проверить realtime bars по разным режимам whatToShow.

Скрипт не пишет данные в SQLite-БД и не используется основным роботом.

Актуальная версия рассчитана на текущую архитектуру проекта:
- contracts.py содержит FUT/CASH/CRYPTO инструменты;
- conId=111 считается временной заглушкой и не передаётся в IB Contract;
- построение контрактов делается через core.contract_utils;
- FUT использует конкретный localSymbol;
- CASH/CRYPTO используют логический код инструмента.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from ib_async import IB, Contract

from contracts import Instrument, PLACEHOLDER_CON_ID
from core.contract_utils import (
    build_instrument_contract,
    get_contract_row_by_local_symbol,
    get_contract_storage_name,
)

# ==========================================================
# ОБЩИЙ РЕЖИМ РАБОТЫ
# ==========================================================
# Варианты:
# - "contract_lookup"
# - "registry_conid_lookup"
# - "historical_probe"
# - "historical_fetch"
# - "realtime_probe"
MODE = "contract_lookup"

# ==========================================================
# НАСТРОЙКИ ПОДКЛЮЧЕНИЯ К IB
# ==========================================================
IB_HOST = "127.0.0.1"
IB_PORT = 7496
IB_CLIENT_ID = 201

# ==========================================================
# НАСТРОЙКИ ИНСТРУМЕНТА ИЗ contracts.py
# ==========================================================
# Для FUT обязательно указываем CONTRACT_LOCAL_SYMBOL.
# Для CASH/CRYPTO CONTRACT_LOCAL_SYMBOL не используется.
INSTRUMENT_CODE = "MES"
CONTRACT_LOCAL_SYMBOL = "MESH6"

# ==========================================================
# НАСТРОЙКИ CONTRACT LOOKUP
# ==========================================================
# LOOKUP_SOURCE:
# - "registry" — строить lookup-контракт из contracts.py;
# - "manual"   — строить lookup-контракт из LOOKUP_* полей ниже.
LOOKUP_SOURCE = "registry"

LOOKUP_INSTRUMENT_CODE = "MES"
LOOKUP_CONTRACT_LOCAL_SYMBOL = "MESH6"

LOOKUP_CON_ID = None
LOOKUP_LOCAL_SYMBOL = "MESH6"
LOOKUP_SEC_TYPE = "FUT"
LOOKUP_INCLUDE_EXPIRED = True
LOOKUP_SYMBOL = ""
LOOKUP_EXCHANGE = "CME"
LOOKUP_CURRENCY = ""
LOOKUP_TRADING_CLASS = ""
LOOKUP_MULTIPLIER = ""
LOOKUP_EXPIRY = ""
LOOKUP_PRINT_JSON = False

# ==========================================================
# НАСТРОЙКИ МАССОВОГО ПОИСКА conId
# ==========================================================
# Пустой список означает: пройти все FUT-инструменты из contracts.py.
CONID_LOOKUP_INSTRUMENT_CODES = ["MES", "ES"]

# Если True — ищем только строки, где conId отсутствует или равен PLACEHOLDER_CON_ID.
CONID_LOOKUP_ONLY_MISSING_OR_PLACEHOLDER = True

# Пауза между запросами contract details, чтобы не спамить IB.
CONID_LOOKUP_DELAY_SECONDS = 0.5

# ==========================================================
# НАСТРОЙКИ HISTORICAL РЕЖИМОВ
# ==========================================================
# Интервал задаётся в UTC.
HISTORICAL_START_UTC = "2026-03-12 17:00:00"
HISTORICAL_END_UTC = "2026-03-12 17:05:00"

HISTORICAL_BAR_SIZE_SETTING = "5 secs"
HISTORICAL_USE_RTH = False

# Для historical_probe проверяем несколько whatToShow подряд.
# Для FX/CRYPTO иногда полезнее начинать с MIDPOINT/TRADES,
# но BID/ASK тоже оставлены для проверки совместимости с основной БД.
HISTORICAL_PROBE_WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK"]

# Для historical_fetch берём один whatToShow.
HISTORICAL_FETCH_WHAT_TO_SHOW = "MIDPOINT"

# Печатать все бары или только начало/конец.
PRINT_ALL_HISTORICAL_BARS = False
HEAD_BARS = 3
TAIL_BARS = 10

# Для probe пустой результат считаем ошибкой.
FAIL_IF_NO_BARS = True

# Сколько секунд дать IB на доставку errorEvent после historical request.
ERROR_FLUSH_DELAY_SECONDS = 0.2

# Коды ошибок IB, которые считаем фатальными для historical request.
FATAL_HISTORICAL_ERROR_CODES = {
    162,
    165,
    166,
    200,
    321,
    366,
}

# ==========================================================
# НАСТРОЙКИ REALTIME РЕЖИМА
# ==========================================================
REALTIME_WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK"]
REALTIME_BAR_SIZE = 5
REALTIME_USE_RTH = False
SECONDS_PER_MODE = 15
PRINT_UTC_TIME = True


# ==========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

def parse_utc(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def normalize_ib_message(text):
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def format_server_time(dt):
    return str(dt).split("+")[0]


def format_utc(dt, for_ib=False):
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt, bar_size_setting):
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("HISTORICAL_END_UTC должно быть строго больше HISTORICAL_START_UTC")

    if bar_size_setting == "5 secs" and total_seconds > 3600:
        raise ValueError(
            "Для ручного теста с 5-секундными барами держи окно не больше 1 часа."
        )

    return f"{total_seconds} S"


def get_instrument_row(instrument_code):
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code} не найден в contracts.py")

    return Instrument[instrument_code]


def get_registry_contract_context(
        instrument_code,
        contract_local_symbol: Optional[str] = None,
):
    instrument_row = get_instrument_row(instrument_code)
    sec_type = instrument_row["secType"]

    if sec_type == "FUT":
        if not contract_local_symbol:
            raise ValueError(
                f"Для FUT-инструмента {instrument_code} нужно указать CONTRACT_LOCAL_SYMBOL"
            )
        contract_row = get_contract_row_by_local_symbol(
            instrument_row,
            contract_local_symbol,
        )
    elif sec_type in ("CASH", "CRYPTO"):
        contract_row = None
    else:
        raise ValueError(
            f"Неподдерживаемый secType={sec_type} для инструмента {instrument_code}"
        )

    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    contract_name = get_contract_storage_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )

    return instrument_row, contract_row, contract, contract_name


def build_registry_contract(instrument_code, contract_local_symbol=None):
    _, _, contract, _ = get_registry_contract_context(
        instrument_code=instrument_code,
        contract_local_symbol=contract_local_symbol,
    )
    return contract


def build_lookup_contract_from_registry():
    _, _, contract, _ = get_registry_contract_context(
        instrument_code=LOOKUP_INSTRUMENT_CODE,
        contract_local_symbol=LOOKUP_CONTRACT_LOCAL_SYMBOL,
    )

    # Для lookup включаем includeExpired, чтобы удобно искать старые квартальные фьючерсы.
    contract.includeExpired = LOOKUP_INCLUDE_EXPIRED
    return contract


def build_manual_lookup_contract():
    kwargs = {
        "secType": LOOKUP_SEC_TYPE,
        "includeExpired": LOOKUP_INCLUDE_EXPIRED,
    }

    if LOOKUP_CON_ID is not None:
        kwargs["conId"] = int(LOOKUP_CON_ID)

    if LOOKUP_LOCAL_SYMBOL:
        kwargs["localSymbol"] = LOOKUP_LOCAL_SYMBOL

    if LOOKUP_SYMBOL:
        kwargs["symbol"] = LOOKUP_SYMBOL

    if LOOKUP_EXCHANGE:
        kwargs["exchange"] = LOOKUP_EXCHANGE

    if LOOKUP_CURRENCY:
        kwargs["currency"] = LOOKUP_CURRENCY

    if LOOKUP_TRADING_CLASS:
        kwargs["tradingClass"] = LOOKUP_TRADING_CLASS

    if LOOKUP_MULTIPLIER:
        kwargs["multiplier"] = LOOKUP_MULTIPLIER

    if LOOKUP_EXPIRY:
        kwargs["lastTradeDateOrContractMonth"] = LOOKUP_EXPIRY

    return Contract(**kwargs)


def build_lookup_contract():
    if LOOKUP_SOURCE == "registry":
        return build_lookup_contract_from_registry()

    if LOOKUP_SOURCE == "manual":
        return build_manual_lookup_contract()

    raise ValueError(f"Неподдерживаемый LOOKUP_SOURCE: {LOOKUP_SOURCE}")


def contract_to_dict(contract):
    return {
        "conId": getattr(contract, "conId", None),
        "secType": getattr(contract, "secType", None),
        "symbol": getattr(contract, "symbol", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "tradingClass": getattr(contract, "tradingClass", None),
        "multiplier": getattr(contract, "multiplier", None),
        "exchange": getattr(contract, "exchange", None),
        "primaryExchange": getattr(contract, "primaryExchange", None),
        "currency": getattr(contract, "currency", None),
    }


def print_contract(contract, index=None):
    if index is not None:
        print(f"Инструмент #{index}")

    print(f"  conId       : {getattr(contract, 'conId', None)}")
    print(f"  secType     : {getattr(contract, 'secType', None)}")
    print(f"  symbol      : {getattr(contract, 'symbol', None)}")
    print(f"  localSymbol : {getattr(contract, 'localSymbol', None)}")
    print(f"  expiry      : {getattr(contract, 'lastTradeDateOrContractMonth', None)}")
    print(f"  tradingClass: {getattr(contract, 'tradingClass', None)}")
    print(f"  multiplier  : {getattr(contract, 'multiplier', None)}")
    print(f"  exchange    : {getattr(contract, 'exchange', None)}")
    print(f"  primaryExch : {getattr(contract, 'primaryExchange', None)}")
    print(f"  currency    : {getattr(contract, 'currency', None)}")


def print_registry_contract_info(contract, instrument_code, contract_name, bar_size=None, use_rth=None, what_to_show_list=None):
    print("=" * 100)
    print("ПАРАМЕТРЫ ТЕСТА")
    print("=" * 100)
    print(f"instrument_code             : {instrument_code}")
    print(f"contract_name               : {contract_name}")
    print(f"secType                     : {getattr(contract, 'secType', None)}")
    print(f"symbol                      : {getattr(contract, 'symbol', None)}")
    print(f"localSymbol                 : {getattr(contract, 'localSymbol', None)}")
    print(f"conId                       : {getattr(contract, 'conId', None)}")
    print(f"lastTradeDateOrContractMonth: {getattr(contract, 'lastTradeDateOrContractMonth', None)}")
    print(f"exchange                    : {getattr(contract, 'exchange', None)}")
    print(f"currency                    : {getattr(contract, 'currency', None)}")
    print(f"tradingClass                : {getattr(contract, 'tradingClass', None)}")
    print(f"multiplier                  : {getattr(contract, 'multiplier', None)}")

    if bar_size is not None:
        print(f"barSize                     : {bar_size}")

    if use_rth is not None:
        print(f"useRTH                      : {use_rth}")

    if what_to_show_list is not None:
        print(f"whatToShow list             : {what_to_show_list}")

    print()


def print_historical_request_info(contract, instrument_code, contract_name, start_dt, end_dt, duration_str, what_to_show_list):
    print_registry_contract_info(
        contract=contract,
        instrument_code=instrument_code,
        contract_name=contract_name,
        bar_size=HISTORICAL_BAR_SIZE_SETTING,
        use_rth=HISTORICAL_USE_RTH,
        what_to_show_list=what_to_show_list,
    )
    print(f"start_utc                  : {start_dt}")
    print(f"end_utc                    : {end_dt}")
    print(f"durationStr                : {duration_str}")
    print()


def print_bar(bar, index):
    print(
        f"{index:04d} | "
        f"date={str(bar.date).split('+')[0]} | "
        f"open={bar.open} | "
        f"high={bar.high} | "
        f"low={bar.low} | "
        f"close={bar.close} | "
        f"volume={bar.volume} | "
        f"average={bar.average} | "
        f"barCount={bar.barCount}"
    )


def print_historical_bars_result(what_to_show, bars):
    print("=" * 100)
    print(f"whatToShow = {what_to_show}")
    print(f"bars count  = {len(bars)}")
    print("=" * 100)

    if PRINT_ALL_HISTORICAL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    if len(bars) <= HEAD_BARS + TAIL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    for i, bar in enumerate(bars[:HEAD_BARS], start=1):
        print_bar(bar, i)

    print("...")

    tail_start_index = len(bars) - TAIL_BARS + 1
    for i, bar in enumerate(bars[-TAIL_BARS:], start=tail_start_index):
        print_bar(bar, i)

    print()


def raise_if_historical_errors(error_records, what_to_show):
    if not error_records:
        return

    lines = [f"[{what_to_show}] IB вернул ошибки historical data:"]

    for code, message in error_records:
        lines.append(f"code={code}, message={message}")

    raise RuntimeError("\n".join(lines))


def format_realtime_bar_time(bar):
    value = bar.time

    if not isinstance(value, datetime):
        raise TypeError(
            f"Ожидался datetime в bar.time, получено: {type(value).__name__}"
        )

    if value.tzinfo is None:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if PRINT_UTC_TIME:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def print_realtime_bar(bar, what_to_show, index):
    print(
        f"[{what_to_show:<8}] "
        f"#{index:03d} | "
        f"time={format_realtime_bar_time(bar)} | "
        f"open={bar.open_} | "
        f"high={bar.high} | "
        f"low={bar.low} | "
        f"close={bar.close} | "
        f"volume={bar.volume} | "
        f"wap={bar.wap} | "
        f"count={bar.count}"
    )


def historical_bars_to_dicts(bars):
    result = []

    for bar in bars:
        bar_date = bar.date

        if isinstance(bar_date, datetime):
            if bar_date.tzinfo is None:
                dt_utc = bar_date.replace(tzinfo=timezone.utc)
            else:
                dt_utc = bar_date.astimezone(timezone.utc)
        else:
            ts_utc = int(bar_date)
            dt_utc = datetime.fromtimestamp(ts_utc, tz=timezone.utc)

        result.append(
            {
                "ts_utc": int(dt_utc.timestamp()),
                "dt_utc": dt_utc,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": float(bar.volume),
                "barCount": bar.barCount,
                "wap": bar.average,
            }
        )

    return result


async def connect_ib_async():
    ib_async_logger = logging.getLogger("ib_async")
    ib_async_logger.handlers.clear()
    ib_async_logger.addHandler(logging.NullHandler())
    ib_async_logger.propagate = False

    ib = IB()

    await ib.connectAsync(
        host=IB_HOST,
        port=IB_PORT,
        clientId=IB_CLIENT_ID,
    )

    if not ib.isConnected():
        raise RuntimeError("Не удалось установить соединение с IB")

    return ib


async def request_historical_once(
        ib,
        contract,
        start_dt,
        end_dt,
        what_to_show,
        bar_size_setting,
        use_rth,
):
    duration_str = build_duration_str(start_dt, end_dt, bar_size_setting)

    return await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=format_utc(end_dt, for_ib=True),
        durationStr=duration_str,
        barSizeSetting=bar_size_setting,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=False,
    )


async def print_new_realtime_bars_for_period(bars, what_to_show, seconds_to_watch):
    printed_count = 0

    for _ in range(seconds_to_watch):
        current_count = len(bars)

        if current_count > printed_count:
            new_bars = bars[printed_count:current_count]

            for i, bar in enumerate(new_bars, start=printed_count + 1):
                print_realtime_bar(bar, what_to_show, i)

            printed_count = current_count

        await asyncio.sleep(1)

    return printed_count


def should_lookup_contract_row(contract_row):
    if not CONID_LOOKUP_ONLY_MISSING_OR_PLACEHOLDER:
        return True

    con_id = contract_row.get("conId")
    return con_id is None or con_id == PLACEHOLDER_CON_ID


def build_contract_for_conid_lookup(instrument_code, instrument_row, contract_row):
    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    contract.includeExpired = LOOKUP_INCLUDE_EXPIRED
    return contract


# ==========================================================
# РЕЖИМЫ
# ==========================================================

async def run_contract_lookup_mode(ib):
    contract = build_lookup_contract()
    details = await ib.reqContractDetailsAsync(contract)

    if not details:
        print("[NOT FOUND] Ничего не найдено.")
        print()
        print("Текущие параметры поиска:")
        print(f"  LOOKUP_SOURCE={LOOKUP_SOURCE}")
        print(f"  LOOKUP_INSTRUMENT_CODE={LOOKUP_INSTRUMENT_CODE}")
        print(f"  LOOKUP_CONTRACT_LOCAL_SYMBOL={LOOKUP_CONTRACT_LOCAL_SYMBOL}")
        print(f"  LOOKUP_CON_ID={LOOKUP_CON_ID}")
        print(f"  LOOKUP_LOCAL_SYMBOL={LOOKUP_LOCAL_SYMBOL}")
        print(f"  LOOKUP_SEC_TYPE={LOOKUP_SEC_TYPE}")
        print(f"  LOOKUP_INCLUDE_EXPIRED={LOOKUP_INCLUDE_EXPIRED}")
        print(f"  LOOKUP_SYMBOL={LOOKUP_SYMBOL}")
        print(f"  LOOKUP_EXCHANGE={LOOKUP_EXCHANGE}")
        print(f"  LOOKUP_CURRENCY={LOOKUP_CURRENCY}")
        print(f"  LOOKUP_TRADING_CLASS={LOOKUP_TRADING_CLASS}")
        print(f"  LOOKUP_MULTIPLIER={LOOKUP_MULTIPLIER}")
        print(f"  LOOKUP_EXPIRY={LOOKUP_EXPIRY}")
        return

    contracts = [item.contract for item in details]

    if LOOKUP_PRINT_JSON:
        data = [contract_to_dict(c) for c in contracts]
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    print(f"Найдено инструментов: {len(contracts)}")
    print()

    for i, c in enumerate(contracts, start=1):
        print_contract(c, index=i)
        print()

    if len(contracts) == 1:
        print("Найден единственный контракт.")
        print("Готовая строка для contracts.py:")
        c = contracts[0]
        print(
            f'{{"conId": {c.conId}, "localSymbol": "{c.localSymbol}", '
            f'"lastTradeDateOrContractMonth": "{c.lastTradeDateOrContractMonth}"}}'
        )
    else:
        print("Выше выведены все найденные варианты. Можно выбрать нужный conId вручную.")


async def run_registry_conid_lookup_mode(ib):
    target_codes = CONID_LOOKUP_INSTRUMENT_CODES or list(Instrument.keys())
    printed_any = False

    for instrument_code in target_codes:
        instrument_row = get_instrument_row(instrument_code)

        if instrument_row["secType"] != "FUT":
            continue

        print("=" * 100)
        print(f"ПОИСК conId ДЛЯ {instrument_code}")
        print("=" * 100)

        for contract_row in instrument_row["contracts"]:
            if not should_lookup_contract_row(contract_row):
                continue

            printed_any = True
            local_symbol = contract_row["localSymbol"]
            lookup_contract = build_contract_for_conid_lookup(
                instrument_code=instrument_code,
                instrument_row=instrument_row,
                contract_row=contract_row,
            )

            print()
            print(f"[{instrument_code}] {local_symbol}: ищу contract details...")
            details = await ib.reqContractDetailsAsync(lookup_contract)

            if not details:
                print("  [NOT FOUND]")
                await asyncio.sleep(CONID_LOOKUP_DELAY_SECONDS)
                continue

            contracts = [item.contract for item in details]

            if len(contracts) == 1:
                c = contracts[0]
                print(
                    f'  OK: {local_symbol} -> conId={c.conId}, '
                    f'expiry={c.lastTradeDateOrContractMonth}, '
                    f'tradingClass={c.tradingClass}, multiplier={c.multiplier}'
                )
                print(
                    f'  Для contracts.py: {{"conId": {c.conId}, '
                    f'"localSymbol": "{c.localSymbol}", '
                    f'"lastTradeDateOrContractMonth": "{c.lastTradeDateOrContractMonth}"}}'
                )
            else:
                print(f"  Найдено вариантов: {len(contracts)}")
                for i, c in enumerate(contracts, start=1):
                    print_contract(c, index=i)
                    print()

            await asyncio.sleep(CONID_LOOKUP_DELAY_SECONDS)

    if not printed_any:
        print("Нет FUT-контрактов, подходящих под условия CONID_LOOKUP_*.")


async def run_historical_probe_mode(ib):
    start_dt = parse_utc(HISTORICAL_START_UTC)
    end_dt = parse_utc(HISTORICAL_END_UTC)
    duration_str = build_duration_str(start_dt, end_dt, HISTORICAL_BAR_SIZE_SETTING)
    _, _, contract, contract_name = get_registry_contract_context(
        INSTRUMENT_CODE,
        CONTRACT_LOCAL_SYMBOL,
    )

    print_historical_request_info(
        contract=contract,
        instrument_code=INSTRUMENT_CODE,
        contract_name=contract_name,
        start_dt=start_dt,
        end_dt=end_dt,
        duration_str=duration_str,
        what_to_show_list=HISTORICAL_PROBE_WHAT_TO_SHOW_LIST,
    )

    current_request_errors = []

    def on_ib_error(req_id, error_code, error_string, contract_obj):
        if error_code not in FATAL_HISTORICAL_ERROR_CODES:
            return

        message = normalize_ib_message(error_string)
        current_request_errors.append((error_code, message))

    ib.errorEvent += on_ib_error

    try:
        server_time = await ib.reqCurrentTimeAsync()
        print("Соединение с IB установлено")
        print(f"Время сервера IB: {format_server_time(server_time)}")
        print()

        if end_dt > server_time.astimezone(timezone.utc):
            raise ValueError(
                f"HISTORICAL_END_UTC={end_dt} лежит в будущем относительно времени сервера IB={server_time}"
            )

        for what_to_show in HISTORICAL_PROBE_WHAT_TO_SHOW_LIST:
            current_request_errors.clear()

            bars = await request_historical_once(
                ib=ib,
                contract=contract,
                start_dt=start_dt,
                end_dt=end_dt,
                what_to_show=what_to_show,
                bar_size_setting=HISTORICAL_BAR_SIZE_SETTING,
                use_rth=HISTORICAL_USE_RTH,
            )

            await asyncio.sleep(ERROR_FLUSH_DELAY_SECONDS)
            raise_if_historical_errors(current_request_errors, what_to_show)

            if FAIL_IF_NO_BARS and len(bars) == 0:
                raise RuntimeError(
                    f"[{what_to_show}] IB не вернул ни одного historical bar"
                )

            print_historical_bars_result(what_to_show, bars)
            await asyncio.sleep(1)

    finally:
        try:
            ib.errorEvent -= on_ib_error
        except Exception:
            pass


async def run_historical_fetch_mode(ib):
    start_dt = parse_utc(HISTORICAL_START_UTC)
    end_dt = parse_utc(HISTORICAL_END_UTC)
    duration_str = build_duration_str(start_dt, end_dt, HISTORICAL_BAR_SIZE_SETTING)
    _, _, contract, contract_name = get_registry_contract_context(
        INSTRUMENT_CODE,
        CONTRACT_LOCAL_SYMBOL,
    )

    print_historical_request_info(
        contract=contract,
        instrument_code=INSTRUMENT_CODE,
        contract_name=contract_name,
        start_dt=start_dt,
        end_dt=end_dt,
        duration_str=duration_str,
        what_to_show_list=[HISTORICAL_FETCH_WHAT_TO_SHOW],
    )

    server_time = await ib.reqCurrentTimeAsync()
    print("Соединение с IB установлено")
    print(f"Время сервера IB: {format_server_time(server_time)}")
    print()

    if end_dt > server_time.astimezone(timezone.utc):
        raise ValueError(
            f"HISTORICAL_END_UTC={end_dt} лежит в будущем относительно времени сервера IB={server_time}"
        )

    bars = await request_historical_once(
        ib=ib,
        contract=contract,
        start_dt=start_dt,
        end_dt=end_dt,
        what_to_show=HISTORICAL_FETCH_WHAT_TO_SHOW,
        bar_size_setting=HISTORICAL_BAR_SIZE_SETTING,
        use_rth=HISTORICAL_USE_RTH,
    )

    rows = historical_bars_to_dicts(bars)

    print(f"Загружено баров: {len(rows)}")

    if not rows:
        print("Пустой результат.")
        return

    print("Первые 3 бара:")
    for row in rows[:3]:
        print(row)

    if len(rows) > 3:
        print("Последние 3 бара:")
        for row in rows[-3:]:
            print(row)


async def run_realtime_probe_mode(ib):
    _, _, contract, contract_name = get_registry_contract_context(
        INSTRUMENT_CODE,
        CONTRACT_LOCAL_SYMBOL,
    )

    print_registry_contract_info(
        contract=contract,
        instrument_code=INSTRUMENT_CODE,
        contract_name=contract_name,
        bar_size=REALTIME_BAR_SIZE,
        use_rth=REALTIME_USE_RTH,
        what_to_show_list=REALTIME_WHAT_TO_SHOW_LIST,
    )

    server_time = await ib.reqCurrentTimeAsync()
    print("Соединение с IB установлено")
    print(f"Время сервера IB: {format_server_time(server_time)}")
    print()

    for what_to_show in REALTIME_WHAT_TO_SHOW_LIST:
        print("=" * 100)
        print(f"СТАРТ ПОДПИСКИ: whatToShow = {what_to_show}")
        print("=" * 100)

        bars = ib.reqRealTimeBars(
            contract,
            REALTIME_BAR_SIZE,
            what_to_show,
            REALTIME_USE_RTH,
            [],
        )

        try:
            printed_count = await print_new_realtime_bars_for_period(
                bars,
                what_to_show,
                SECONDS_PER_MODE,
            )

            print(f"[{what_to_show}] Получено баров   : {len(bars)}")
            print(f"[{what_to_show}] Напечатано баров : {printed_count}")

        finally:
            ib.cancelRealTimeBars(bars)
            print(f"СТОП ПОДПИСКИ: whatToShow = {what_to_show}")
            print()
            await asyncio.sleep(2)


async def main():
    supported_modes = {
        "contract_lookup",
        "registry_conid_lookup",
        "historical_probe",
        "historical_fetch",
        "realtime_probe",
    }

    if MODE not in supported_modes:
        raise ValueError(f"Неподдерживаемый MODE: {MODE}")

    ib = await connect_ib_async()

    try:
        if MODE == "contract_lookup":
            await run_contract_lookup_mode(ib)
        elif MODE == "registry_conid_lookup":
            await run_registry_conid_lookup_mode(ib)
        elif MODE == "historical_probe":
            await run_historical_probe_mode(ib)
        elif MODE == "historical_fetch":
            await run_historical_fetch_mode(ib)
        elif MODE == "realtime_probe":
            await run_realtime_probe_mode(ib)
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("Соединение с IB закрыто")


if __name__ == "__main__":
    asyncio.run(main())
