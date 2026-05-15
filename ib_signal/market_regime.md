# Market Regime: расчёт, классификация и фильтрация кандидатов

## 1. Назначение

Market regime нужен для того, чтобы исторические кандидаты сравнивались с текущим паттерном не только по форме движения цены через Pearson, но и по рыночному контексту.

Базовая идея:

```text
кандидат должен быть похож по форме
и находиться в таком же режиме относительно SMA 600
```

Сейчас regime строится вокруг трёх признаков:

```text
1. направление price-regression
2. направление SMA 600 regression
3. взаимное расположение price-regression и SMA 600 regression
```

---

## 2. Какие данные используются

Для текущего паттерна используются только данные текущего `pattern-window`.

Для исторического кандидата используются только данные его `pattern-window`.

Trade-window кандидата для расчёта режима не используется. Это важно: иначе появится lookahead bias — утечка будущего.

Правильно:

```text
candidate.pattern_start_ts -> candidate.pattern_end_ts
```

Неправильно:

```text
candidate.pattern_start_ts -> candidate.trade_end_ts
```

---

## 3. Regression line

Для price pattern и SMA 600 строится обычная линейная регрессия:

```text
y = slope * x + intercept
```

где:

```text
x = 0, 1, 2, ..., points_count - 1
y = значения price или SMA 600
```

Результат регрессии хранит:

```text
slope
intercept
line_values
fitted_start
fitted_end
fitted_delta
points_count
```

Главный показатель для режима:

```text
fitted_delta = fitted_end - fitted_start
```

Это изменение именно regression line, а не разница между первой и последней фактической ценой.

---

## 4. Базисные пункты

Чтобы не задавать пороги в абсолютных пунктах цены, режим считается в базисных пунктах.

```text
1 bps = 0.01%
100 bps = 1%
```

Для regression delta:

```text
delta_bps = fitted_delta / abs(fitted_start) * 10000
```

Пример:

```text
fitted_start = 29000
fitted_delta = 10 пунктов

delta_bps = 10 / 29000 * 10000 = 3.45 bps
```

На PNG мы показываем обе величины:

```text
bps / pt
```

Пример:

```text
price delta : 26.53 / 76.93
```

Это означает:

```text
26.53 bps / 76.93 пунктов
```

---

## 5. Порог flat

Порог задаётся в `contracts.py` как характеристика инструмента:

```python
"regression_flat_delta_threshold_bps": ...
```

Общий fallback в DEFAULTS:

```python
FUT_DEFAULTS["regression_flat_delta_threshold_bps"] = 1.0
FX_DEFAULTS["regression_flat_delta_threshold_bps"] = 1.0
CRYPTO_DEFAULTS["regression_flat_delta_threshold_bps"] = 1.0
```

Для конкретных инструментов задаются актуальные значения, например:

```python
"MNQ": {
    ...
    "regression_flat_delta_threshold_bps": 3.5,
}
```

Порог читается через:

```python
get_regression_flat_delta_threshold_bps(instrument_code)
```

---

## 6. Direction: направление регрессии

Для price-regression и SMA 600 regression направление считается одинаково:

```text
delta_bps > +threshold_bps  => up
delta_bps < -threshold_bps  => down
иначе                      => flat
```

Пример:

```text
threshold = 3.5 bps

price_delta_bps = 0.93
price_direction = flat

sma600_delta_bps = 31.77
sma600_direction = up
```

Важно: `slope` внутри regression result остаётся, но в режимной диагностике не используется, потому что при фиксированной длине окна он дублирует `delta`.

---

## 7. Relation: взаимное расположение price-regression и SMA 600 regression

Сравниваются две линии:

```text
price_regression.line_values
sma600_regression.line_values
```

Считается разница:

```text
diff = price_regression - sma600_regression
```

Из неё берутся две точки:

```text
start_diff = diff[0]
end_diff   = diff[-1]
```

Для каждой точки считаются значения в пунктах и в bps.

Порядок на PNG:

```text
bps / pt
```

Пример:

```text
start_diff = 20.51 / 60.11
end_diff   = -10.26 / -30.06
```

Это значит:

```text
в начале price-regression была выше SMA 600 regression на 20.51 bps / 60.11 пунктов
в конце price-regression стала ниже SMA 600 regression на 10.26 bps / 30.06 пунктов
```

---

## 8. Relation-классы

Классификация relation строится только по `start_diff_bps`, `end_diff_bps` и `near_threshold_bps`.

Сейчас `near_threshold_bps` используется тот же, что и `regression_flat_delta_threshold_bps`.

### 8.1 `above_sma`

```text
start_diff_bps > threshold
end_diff_bps   > threshold
```

Цена была выше SMA 600 в начале окна и осталась выше в конце.

Смысл:

```text
price-regression уверенно выше SMA 600 regression
```

---

### 8.2 `below_sma`

```text
start_diff_bps < -threshold
end_diff_bps   < -threshold
```

Цена была ниже SMA 600 в начале окна и осталась ниже в конце.

Смысл:

```text
price-regression уверенно ниже SMA 600 regression
```

---

### 8.3 `cross_up_sma`

```text
start_diff_bps < -threshold
end_diff_bps   > threshold
```

Цена была ниже SMA 600 в начале окна и стала выше в конце.

Смысл:

```text
пересечение SMA 600 снизу вверх
```

---

### 8.4 `cross_down_sma`

```text
start_diff_bps > threshold
end_diff_bps   < -threshold
```

Цена была выше SMA 600 в начале окна и стала ниже в конце.

Смысл:

```text
пересечение SMA 600 сверху вниз
```

---

### 8.5 `near_sma`

```text
abs(start_diff_bps) <= threshold
abs(end_diff_bps)   <= threshold
```

Цена была рядом с SMA 600 в начале и осталась рядом в конце.

Смысл:

```text
price-regression и SMA 600 regression почти слиты
```

---

### 8.6 `mixed_sma`

Всё, что не попало в чистые классы:

```text
above_sma
below_sma
cross_up_sma
cross_down_sma
near_sma
```

Примеры:

```text
start_diff = +8 bps
end_diff   = +2 bps
```

Это не `above_sma`, потому что в конце price уже слишком близко к SMA.  
Это не `near_sma`, потому что в начале было далеко.  
Значит `mixed_sma`.

Другой пример:

```text
start_diff = -2 bps
end_diff   = +9 bps
```

Старт был около SMA, конец выше SMA. Это тоже `mixed_sma`.

Смысл:

```text
переходный / нечистый / неопределённый relation
```

Важно: при включённом SOFT/HARD-фильтре текущий `mixed_sma` пока пропускается, потому что фильтровать кандидатов по мусорной корзине нельзя.

---

## 9. Market regime

Полноценный regime состоит из трёх компонентов:

```text
price_direction
sma600_direction
price_vs_sma600_relation
```

Примеры:

```text
flat / up / cross_down_sma
down / up / cross_down_sma
up / up / cross_down_sma
```

Это разные рыночные режимы, даже если relation одинаковый.

Почему это важно:

```text
cross_down_sma может произойти:
- когда price падает
- когда price стоит, а SMA растёт
- когда price растёт, но SMA растёт быстрее
```

Поэтому relation — это только часть режима.

---

## 10. Режимный фильтр кандидатов

В `SignalConfig` используется enum:

```python
class MarketRegimeFilterMode(Enum):
    OFF = "OFF"
    SOFT = "SOFT"
    HARD = "HARD"
```

Настройка:

```python
market_regime_filter_mode: MarketRegimeFilterMode = MarketRegimeFilterMode.HARD
```

По умолчанию сейчас включён `HARD`.

---

## 11. OFF / SOFT / HARD

### 11.1 OFF

Фильтр по режиму выключен.

Кандидат проходит, если:

```text
pearson >= pearson_min
```

Relation и directions могут считаться диагностически, но не участвуют в финальном отборе.

---

### 11.2 SOFT

Мягкий режим.

Кандидат проходит, если:

```text
pearson >= pearson_min
candidate_relation == current_relation
```

Например:

```text
current_relation = cross_down_sma
candidate_relation = cross_down_sma
```

Кандидат остаётся даже если направления price/SMA 600 у него отличаются от текущих.

---

### 11.3 HARD

Жёсткий режим.

Кандидат проходит, если:

```text
pearson >= pearson_min
candidate_relation == current_relation
candidate_price_direction == current_price_direction
candidate_sma600_direction == current_sma600_direction
```

То есть должен совпасть весь текущий market regime:

```text
price_direction + sma600_direction + relation
```

---

## 12. Диагностические счётчики фильтра

Даже если активен `HARD`, лог показывает, что было бы при `SOFT`.

В логе:

```text
regime_filter: mode=HARD, current=flat/up/cross_down_sma, soft_kept=1, hard_kept=0, final_kept=0, skipped_sma=0, relation_mismatch=0, direction_mismatch=1
```

Расшифровка:

```text
mode=HARD
```

Активный режим фильтра.

```text
current=flat/up/cross_down_sma
```

Текущий market regime:

```text
price_direction = flat
sma600_direction = up
relation = cross_down_sma
```

```text
soft_kept=1
```

Сколько кандидатов осталось бы при SOFT-фильтре.

```text
hard_kept=0
```

Сколько кандидатов осталось бы при HARD-фильтре.

```text
final_kept=0
```

Сколько кандидатов реально передано дальше по активному режиму.

```text
skipped_sma=0
```

Сколько Pearson-прошедших кандидатов отброшено из-за отсутствующей или неполной SMA 600.

```text
relation_mismatch=0
```

Сколько Pearson-прошедших кандидатов отброшено из-за несовпадения relation.

```text
direction_mismatch=1
```

Сколько SOFT-прошедших кандидатов отброшено в HARD из-за несовпадения `price_direction` или `sma600_direction`.

---

## 13. Текущий итоговый лог

Финальный runtime-лог специально сгруппирован по секциям:

```text
MNQ: signal_calc
  time: latest_job_row=...
  window: signal_bar=..., pattern=..., trade=...
  candidates: hour=..., allowed=..., found=..., first=..., last=...
  pearson: min=0.70, best=..., passed=.../...
  regression: threshold=... bps; price: delta=... bps / ... pt, dir=...; sma600: delta=... bps / ... pt, dir=...
  relation: price_vs_sma600: ..., start=... bps / ... pt, end=... bps / ... pt
  regime_filter: mode=..., current=..., soft_kept=..., hard_kept=..., final_kept=..., skipped_sma=..., relation_mismatch=..., direction_mismatch=...
```

Строка `matrix: points=...` убрана как лишняя для постоянного просмотра.

---

## 14. PNG-диагностика

На PNG справа есть блок:

```text
REGRESSION (bps / pt)
```

В нём порядок чисел всегда:

```text
bps / pt
```

Пример:

```text
flat_threshold : 3.50 / 10.12
price delta    : 26.53 / 76.93
sma 600 delta  : 43.87 / 127.21
```

Есть блок:

```text
RELATION (bps / pt)
```

Пример:

```text
price/sma 600 : cross_down_sma
start_diff    : 20.51 / 60.11
end_diff      : -10.26 / -30.06
```

`mean_diff` удалён, потому что для двух прямых он не даёт новой информации: разница двух прямых тоже прямая, а значит start/end достаточно.

---

## 15. Что не делаем сейчас

Пока не добавлены:

```text
SMA 120 / 600 / 1200 stack
волатильность
pattern efficiency
future MFE / MAE
статистика будущего движения
```

Это следующие возможные слои, но они не входят в текущий market-regime filter.

Текущий regime-фильтр ограничен:

```text
price-regression
SMA 600 regression
price-vs-SMA600 relation
price/SMA600 direction
```

---

## 16. Главные риски и ограничения

### 16.1 HARD может сильно душить выборку

`HARD` требует совпадения:

```text
relation
price_direction
sma600_direction
```

Если Pearson и так пропускает мало кандидатов, HARD может часто давать `final_kept=0`.

Поэтому в логе есть диагностика:

```text
soft_kept
hard_kept
```

Она нужна, чтобы сравнивать силу отсечения.

---

### 16.2 `mixed_sma` пока не торгуем

`mixed_sma` — это остаточная корзина. Там могут быть разные переходные состояния.

Поэтому при активном SOFT/HARD текущий `mixed_sma` пока пропускается.

---

### 16.3 Один и тот же relation не означает один и тот же режим

Например, `cross_down_sma` может возникнуть в разных сценариях:

```text
price down / sma up
price flat / sma up
price up / sma up
price down / sma down
```

Поэтому SOFT — это только relation-фильтр, а HARD — полноценнее.

---

## 17. Минимальная схема принятия кандидата сейчас

```text
1. Найти historical candidate windows.
2. Собрать price pattern matrix.
3. Посчитать Pearson.
4. Посчитать текущий regime:
   - price_direction
   - sma600_direction
   - price_vs_sma600_relation
5. Для Pearson-прошедших кандидатов:
   - прочитать SMA 600 на candidate pattern-window
   - посчитать candidate price_direction
   - посчитать candidate sma600_direction
   - посчитать candidate relation
6. Применить active mode:
   OFF  -> только Pearson
   SOFT -> Pearson + relation
   HARD -> Pearson + relation + directions
7. Передать дальше только final candidates.
```

---

## 18. Практическая интерпретация

Пример:

```text
current = flat/up/cross_down_sma
```

Это значит:

```text
price-regression почти flat
SMA600-regression уверенно up
price-regression пересекла SMA600-regression сверху вниз
```

Если активен `SOFT`, подойдут кандидаты:

```text
*/ */ cross_down_sma
```

Если активен `HARD`, подойдут только кандидаты:

```text
flat / up / cross_down_sma
```

где `*` означает любое направление.

---

## 19. Краткий словарь

```text
bps
    базисные пункты, 1 bps = 0.01%

pt
    ценовые пункты инструмента

price_direction
    направление price-regression: up / down / flat

sma600_direction
    направление SMA 600 regression: up / down / flat

relation
    взаимное расположение price-regression относительно SMA 600 regression

above_sma
    price-regression выше SMA 600 regression

below_sma
    price-regression ниже SMA 600 regression

cross_up_sma
    пересечение SMA 600 снизу вверх

cross_down_sma
    пересечение SMA 600 сверху вниз

near_sma
    price-regression рядом с SMA 600 regression в начале и конце окна

mixed_sma
    нечистый / переходный relation, пока не используем для фильтрации

OFF
    без regime-фильтра

SOFT
    фильтр только по relation

HARD
    фильтр по relation + price_direction + sma600_direction
```
