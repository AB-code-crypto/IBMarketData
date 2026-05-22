# Общие параметры расчёта job-data признаков.
# Это не настройки signal-сервиса и не торговые правила.
# Здесь лежат параметры индикаторных признаков, которые job_data считает заранее.

# Рабочая SMA для режима. Сейчас в sma_5s уже есть sma_600,
# поэтому regime строится именно по ней.
REGIME_SMA_PERIOD_BARS = 600

# Окно регрессии по sma_600.
# 60 баров * 5 секунд = 5 минут.
REGIME_REGRESSION_BARS = 60

# Имя итоговой колонки в sma_5s.
# Значения:
#   1  = режим вверх / LONG
#   0  = флет
#  -1  = режим вниз / SHORT
REGIME_COLUMN_NAME = "regime"

# Имя instrument-level параметра в contracts.py.
# Сам порог хранится в пунктах, потому что масштаб зависит от инструмента.
REGIME_FLAT_DELTA_THRESHOLD_POINTS_KEY = "regime_flat_delta_threshold_points"
