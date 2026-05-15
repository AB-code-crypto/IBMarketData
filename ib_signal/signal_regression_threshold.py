from contracts import Instrument


def get_regression_flat_delta_threshold_bps(instrument_code: str) -> float:
    """Что делает: возвращает flat-порог для regression delta в базисных пунктах из contracts.py.
    Зачем нужна: порог — характеристика инструмента, а не настройка signal-сервиса."""
    instrument_row = Instrument[instrument_code]

    if "regression_flat_delta_threshold_bps" not in instrument_row:
        raise ValueError(
            f"{instrument_code}: в contracts.py не задан regression_flat_delta_threshold_bps"
        )

    threshold_bps = float(instrument_row["regression_flat_delta_threshold_bps"])

    if threshold_bps < 0.0:
        raise ValueError(
            f"{instrument_code}: regression_flat_delta_threshold_bps не может быть отрицательным: {threshold_bps}"
        )

    return threshold_bps
