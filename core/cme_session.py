from datetime import datetime, timezone

from core.time_utils import CT_TIMEZONE


def should_load_futures_hour_chunk(chunk_start_ts, chunk_end_ts):
    # Проверяем, попадает ли часовой chunk в гарантированное weekend-окно CME
    # для equity index futures в UTC.
    #
    # Логика специально консервативная:
    # пропускаем только гарантированно закрытое окно,
    # чтобы случайно не отрезать торговые бары на переходах летнего/зимнего времени.
    #
    # Функция возвращает:
    # - True  -> chunk надо качать;
    # - False -> chunk гарантированно попал на выходные, пропускаем.
    chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
    chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

    # Для safety-логики функция рассчитана именно на часовые chunk-и.
    # Если когда-нибудь сюда начнут передавать другой размер,
    # лучше упасть сразу и явно.
    if int((chunk_end_dt - chunk_start_dt).total_seconds()) > 3600:
        raise ValueError(
            f"Функция should_load_futures_hour_chunk рассчитана только на часовые интервалы. "
            f"Получен интервал: {chunk_start_dt}-{chunk_end_dt}"
        )

    start_weekday = chunk_start_dt.weekday()
    start_hour = chunk_start_dt.hour

    # Пятница после 22:00 UTC и до конца суток — гарантированно выходные.
    if start_weekday == 4 and start_hour >= 22:
        return False

    # Вся суббота целиком гарантированно попадает в weekend-окно.
    if start_weekday == 5:
        return False

    # Воскресенье до 22:00 UTC не торгуется.
    if start_weekday == 6 and start_hour < 22:
        return False

    return True


def should_load_fx_hour_chunk(chunk_start_ts, chunk_end_ts):
    # Грубая 24/5-фильтрация для FX.
    #
    # Логика консервативная: пропускаем субботу и раннее воскресенье UTC.
    # Если IB где-то отдаёт редкие служебные котировки на границе сессии,
    # лучше немного докачать лишнее, чем случайно отрезать торговые бары.
    chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
    start_weekday = chunk_start_dt.weekday()
    start_hour = chunk_start_dt.hour

    if start_weekday == 5:
        return False

    if start_weekday == 6 and start_hour < 22:
        return False

    return True


def should_load_history_chunk(session_model, chunk_start_ts, chunk_end_ts):
    # Единая точка принятия решения, стоит ли запрашивать historical chunk.
    if session_model == "CME_EQUITY_INDEX":
        return should_load_futures_hour_chunk(chunk_start_ts, chunk_end_ts)

    if session_model == "FX_24_5":
        return should_load_fx_hour_chunk(chunk_start_ts, chunk_end_ts)

    if session_model == "CRYPTO_24_7":
        return True

    # Для неизвестной модели ничего не пропускаем.
    # Это безопаснее, чем случайно потерять историю.
    return True


def is_expected_cme_realtime_flow_now():
    # Грубая проверка, должен ли сейчас вообще идти поток 5-секундных баров для CME.
    # Используем Chicago time, потому что расписание CME естественнее проверять в CT.
    now_ct = datetime.now(CT_TIMEZONE)
    weekday = now_ct.weekday()  # Mon=0 ... Sun=6
    hour = now_ct.hour

    # Суббота полностью закрыта.
    if weekday == 5:
        return False

    # Воскресенье: открытие с 17:00 CT.
    if weekday == 6:
        return hour >= 17

    # Пятница: торговля до 16:00 CT.
    if weekday == 4:
        return hour < 16

    # Пн-Чт: ежедневный клиринг/maintenance 16:00-17:00 CT.
    return hour != 16


def is_expected_fx_realtime_flow_now():
    # Грубая проверка 24/5 для FX по UTC.
    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()
    hour = now_utc.hour

    if weekday == 5:
        return False

    if weekday == 6 and hour < 22:
        return False

    return True


def is_expected_realtime_flow_now(session_model="CME_EQUITY_INDEX"):
    # Единая точка проверки ожидаемого realtime-потока.
    if session_model == "CME_EQUITY_INDEX":
        return is_expected_cme_realtime_flow_now()

    if session_model == "FX_24_5":
        return is_expected_fx_realtime_flow_now()

    if session_model == "CRYPTO_24_7":
        return True

    return True
