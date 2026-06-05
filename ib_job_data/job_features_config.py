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
# Диапазон зон симметричный: один общий rolling percentile положительных отклонений
# high/low от SMA, затем этот диапазон зеркально откладывается вверх и вниз от SMA.
# Это убирает слипание одной стороны зон в затяжном тренде.

# Аналог InpRangeLookbackBars=200 на M1:
# 200 минут * 60 секунд / 5 секунд = 2400 5-секундных баров.
MA_ZONE_RANGE_LOOKBACK_BARS = 2400

# Аналог InpRangePercentile.
# 95.0 означает: ширина зон задаётся типичным диапазоном отклонений,
# а редкие экстремальные выносы не раздувают шкалу.
MA_ZONE_RANGE_PERCENTILE = 95.0

# Аналог InpLevel1Percent / InpLevel2Percent.
MA_ZONE_LEVEL1_PERCENT = 33.333333
MA_ZONE_LEVEL2_PERCENT = 66.666667

# Значения:
#   4  above upper_far boundary / повышенный вынос
#   3  upper_far
#   2  upper_middle
#   1  upper_near
#   0  нет данных / около MA
#  -1  lower_near
#  -2  lower_middle
#  -3  lower_far
#  -4  below lower_far boundary / повышенный вынос
MA_ZONE_COLUMN_NAME = "ma_zone"

# Эти две колонки исторически использовались для раздельных upper/lower диапазонов на PNG.
# Сейчас диапазон один и симметричный, поэтому ma_zone_features пишет одинаковое значение в обе колонки.
# Колонки оставлены, чтобы не ломать существующую схему job DB и signal_plot-reader.
MA_ZONE_UPPER_RANGE_COLUMN_NAME = "ma_zone_upper_range_points"
MA_ZONE_LOWER_RANGE_COLUMN_NAME = "ma_zone_lower_range_points"
