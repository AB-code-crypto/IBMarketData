from contracts import Instrument


# Стартовые диагностические пороги flat для fitted_delta regression line.
# Единицы измерения — ценовые пункты инструмента за всё pattern-window.
# Если в contracts.py у конкретного инструмента задан regression_flat_delta_threshold,
# то значение из инструмента перекрывает этот дефолт по secType.
REGRESSION_FLAT_DELTA_THRESHOLD_BY_SEC_TYPE: dict[str, float] = {
    "FUT": 10.0,
    "CASH": 0.0002,
    "CRYPTO": 100.0,
}


def get_regression_flat_delta_threshold(instrument_code: str) -> float:
    """Что делает: возвращает flat-порог для regression delta по инструменту.
    Зачем нужна: фьючи, валюты и крипта имеют разные масштабы цены, поэтому один общий порог — мусор."""
    instrument_row = Instrument[instrument_code]

    threshold_value = instrument_row.get("regression_flat_delta_threshold")

    if threshold_value is None:
        sec_type = str(instrument_row["secType"])
        if sec_type not in REGRESSION_FLAT_DELTA_THRESHOLD_BY_SEC_TYPE:
            raise ValueError(
                f"{instrument_code}: нет regression_flat_delta_threshold "
                f"и нет дефолта для secType={sec_type}"
            )

        threshold_value = REGRESSION_FLAT_DELTA_THRESHOLD_BY_SEC_TYPE[sec_type]

    threshold = float(threshold_value)

    if threshold < 0.0:
        raise ValueError(
            f"{instrument_code}: regression_flat_delta_threshold не может быть отрицательным: {threshold}"
        )

    return threshold
