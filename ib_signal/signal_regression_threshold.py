from contracts import Instrument


def get_regression_flat_delta_threshold(instrument_code: str) -> float:
    """Что делает: возвращает flat-порог для regression delta из contracts.py.
    Зачем нужна: порог — характеристика инструмента, а не настройка signal-сервиса."""
    instrument_row = Instrument[instrument_code]

    if "regression_flat_delta_threshold" not in instrument_row:
        raise ValueError(
            f"{instrument_code}: в contracts.py не задан regression_flat_delta_threshold"
        )

    threshold = float(instrument_row["regression_flat_delta_threshold"])

    if threshold < 0.0:
        raise ValueError(
            f"{instrument_code}: regression_flat_delta_threshold не может быть отрицательным: {threshold}"
        )

    return threshold
