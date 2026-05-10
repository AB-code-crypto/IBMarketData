from core.price_validation import validate_positive_price


def validate_history_price_value(value, field_name, stream_name, contract_name, interval_text, bar_index):
    """Что делает: проверяет одно price-поле historical-бара и добавляет контекст ошибки. Зачем нужна: плохие цены от IB должны быть обнаружены до записи в SQLite."""
    context = (
        f"{stream_name} для {contract_name}, "
        f"interval={interval_text}, bar_index={bar_index}"
    )
    return validate_positive_price(
        value,
        field_name=field_name,
        context=context,
    )


def validate_history_bar_stream(*, bars, stream_name, contract_name, interval_text):
    """Что делает: проверяет OHLC-поля всех баров одного BID или ASK historical-stream. Зачем нужна: один битый бар делает весь chunk непригодным для записи."""
    for index, bar in enumerate(bars):
        for field_name, field_value in (
                ("open", bar.open),
                ("high", bar.high),
                ("low", bar.low),
                ("close", bar.close),
        ):
            validation_error = validate_history_price_value(
                value=field_value,
                field_name=field_name,
                stream_name=stream_name,
                contract_name=contract_name,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                return validation_error

    return None


def validate_history_bid_ask_bars(*, bid_bars, ask_bars, contract_name, interval_text):
    """Что делает: проверяет оба historical-stream: BID и ASK. Зачем нужна: price DB должна получать только валидные пары BID/ASK."""
    validation_error = validate_history_bar_stream(
        bars=bid_bars,
        stream_name="BID",
        contract_name=contract_name,
        interval_text=interval_text,
    )

    if validation_error is not None:
        return validation_error

    return validate_history_bar_stream(
        bars=ask_bars,
        stream_name="ASK",
        contract_name=contract_name,
        interval_text=interval_text,
    )
