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


# MA-zone строится вокруг рабочей SMA600.
# Логика адаптирована из IBP_MA_Zones.mq5:
# rolling percentile отклонений high/low от SMA, затем деление диапазона на 3 зоны.
MA_ZONE_SMA_PERIOD_BARS = 600

# Аналог InpRangeLookbackBars=200 на M1:
# 200 минут * 60 секунд / 5 секунд = 2400 5-секундных баров.
MA_ZONE_RANGE_LOOKBACK_BARS = 2400

# Аналог InpRangePercentile.
# 95.0 означает: игнорируем редкие экстремальные выносы.
MA_ZONE_RANGE_PERCENTILE = 95.0

# Аналог InpLevel1Percent / InpLevel2Percent.
MA_ZONE_LEVEL1_PERCENT = 33.333333
MA_ZONE_LEVEL2_PERCENT = 66.666667

# Поддерживаются:
#   "HIGH_LOW" — верхняя зона по mid_high, нижняя по mid_low;
#   "CLOSE"    — обе стороны по mid_close.
MA_ZONE_RANGE_SOURCE = "HIGH_LOW"

# Значения:
#   3  upper_far
#   2  upper_middle
#   1  upper_near
#   0  нет данных / около MA
#  -1  lower_near
#  -2  lower_middle
#  -3  lower_far
MA_ZONE_COLUMN_NAME = "ma_zone"
