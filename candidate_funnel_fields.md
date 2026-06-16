# candidate_funnel_events — краткая расшифровка полей отсева кандидатов

Таблица `candidate_funnel_events` нужна, чтобы видеть, на каком этапе `ib_signal` теряет исторических кандидатов. Главная логика чтения: смотри значения слева направо как воронку.

```text
raw → slot_offset → matrix → pearson → regime → minmax → score → potential
```

Если на каком-то шаге количество резко падает, значит именно этот фильтр является узким местом.

## Идентификация расчёта

| Поле | Что означает |
|---|---|
| `funnel_id` | Внутренний ID записи диагностики. |
| `instrument_code` | Инструмент: `MNQ`, `MES` и т.д. |
| `signal_bar_ts` | Unix timestamp точки расчёта сигнала. |
| `signal_time_ct` | Время расчёта сигнала в Chicago Time. |
| `signal_window_mode` | Режим окна: `SLOT` или `ROLLING`. |
| `market_regime_filter_mode` | Режим regime-фильтра: `OFF`, `SOFT`, `HARD`. |
| `created_at_ts` | Когда запись создана. |
| `updated_at_ts` | Когда запись обновлена. |

## Ранний поиск кандидатов

Это самый первый этап. Он показывает, сколько исторических окон вообще попало в первичный поиск.

| Поле | Что означает |
|---|---|
| `current_hour_ct` | CT-час текущего сигнала. По нему выбирается группа разрешённых исторических часов. |
| `allowed_hours_ct_json` | Список CT-часов, среди которых ищутся исторические кандидаты. |
| `min_candidate_signal_ts` | Левая граница истории для поиска кандидатов. Зависит от `history_lookback_days`. |
| `max_candidate_signal_ts` | Правая граница поиска. Кандидат должен закончить своё future/trade-window до текущего сигнала. |
| `signal_phase_seconds` | Фаза сигнала внутри часа: `signal_bar_ts % 3600`. Например, `1800` = ровно 30-я минута часа. |
| `slot_offset_seconds` | Offset текущего сигнала внутри SLOT. Например, `1800` = 30 минут от начала слота. |
| `raw_candidate_rows_count` | Сколько кандидатов прошло time-range + CT-hour + phase внутри часа. Если тут мало, проблема в истории, hour groups или phase matching. |
| `slot_offset_kept_count` | Сколько кандидатов осталось после проверки того же offset внутри SLOT. |
| `slot_offset_dropped_count` | Сколько кандидатов отброшено из-за несовпадения SLOT-offset. |

### Как читать

```text
raw_candidate_rows_count = 3
```

Кандидатов мало уже на самом раннем поиске. Смотреть `history_lookback_days`, `allowed_hours_ct_json`, `signal_phase_seconds`, SLOT-offset.

```text
raw_candidate_rows_count = 700
slot_offset_kept_count = 700
```

Ранний поиск не проблема. Смотреть следующие фильтры.

## Pattern matrix

Этот этап проверяет, можно ли для кандидатов собрать полный исторический pattern-window без дыр и NaN.

| Поле | Что означает |
|---|---|
| `matrix_source_candidates_count` | Сколько кандидатов пришло на сборку pattern matrix. Обычно равно `slot_offset_kept_count`. |
| `matrix_valid_candidates_count` | Сколько кандидатов имеют полный корректный pattern-window. |
| `matrix_skipped_candidates_count` | Сколько кандидатов отброшено из-за неполных данных, дыр, NaN или отсутствующих баров. |

### Как читать

```text
matrix_source_candidates_count = 700
matrix_valid_candidates_count = 690
```

Нормально, потери маленькие.

```text
matrix_source_candidates_count = 700
matrix_valid_candidates_count = 30
```

Проблема в данных: дыры в job DB / price DB или недостаточно истории для окон.

## Pearson

Этот этап ищет похожую форму текущего паттерна среди исторических кандидатов.

| Поле | Что означает |
|---|---|
| `pearson_min` | Минимальный Pearson threshold из настроек. |
| `pearson_passed_count` | Сколько кандидатов прошло `pearson >= pearson_min`. |
| `best_pearson` | Лучший Pearson среди кандидатов. |

### Как читать

```text
matrix_valid_candidates_count = 757
pearson_passed_count = 5
```

Основной отсев происходит на Pearson. Возможные действия для тестов: снизить `pearson_min` или менять длину pattern-window.

```text
best_pearson = 0.62
pearson_min = 0.70
pearson_passed_count = 0
```

Похожих паттернов по текущему threshold нет.

## Regime filter

Этот этап сравнивает текущий рынок и кандидатов относительно SMA/regime. Особенно важен при `market_regime_filter_mode = SOFT` или `HARD`.

| Поле | Что означает |
|---|---|
| `current_relation` | Текущее отношение price-regression к SMA600: например `above_sma`, `below_sma`, `mixed_sma`. |
| `current_price_direction` | Направление price-regression текущего паттерна: `up`, `down`, `flat`. |
| `current_sma600_direction` | Направление SMA600-regression: `up`, `down`, `flat`. |
| `regime_source_candidates_count` | Сколько кандидатов пришло в regime-фильтр. |
| `regime_pearson_passed_count` | Сколько кандидатов прошло Pearson перед regime-фильтром. |
| `regime_soft_kept_count` | Сколько кандидатов осталось бы в режиме `SOFT`. SOFT обычно требует совпадения relation. |
| `regime_hard_kept_count` | Сколько кандидатов осталось бы в режиме `HARD`. HARD требует SOFT + совпадение направлений. |
| `regime_final_kept_count` | Итоговое количество кандидатов после активного режима `OFF/SOFT/HARD`. |
| `regime_skipped_sma_count` | Сколько кандидатов отброшено, потому что не удалось прочитать SMA для candidate-window. |
| `regime_relation_mismatch_count` | Сколько Pearson-кандидатов отброшено из-за другого relation. |
| `regime_direction_mismatch_count` | Сколько SOFT-кандидатов отброшено из-за несовпадения direction в HARD. |
| `regime_skip_reason` | Причина полного пропуска regime-фильтра, например `current_relation=mixed_sma` или `SMA 600 relation не рассчитан`. |

### Как читать

```text
pearson_passed_count = 20
regime_final_kept_count = 3
```

Regime-фильтр слишком сильно режет. Проверять `SOFT` вместо `HARD` или instrument-specific threshold.

```text
regime_soft_kept_count = 3
regime_hard_kept_count = 3
```

В этом расчёте `HARD` не режет сильнее, чем `SOFT`.

```text
regime_relation_mismatch_count = 2
regime_direction_mismatch_count = 0
```

Кандидаты отпали по relation, а не по direction.

## Minmax filter

Этот этап отбрасывает кандидатов, у которых форма похожа, но масштаб движения сильно отличается от текущего паттерна.

| Поле | Что означает |
|---|---|
| `minmax_source_candidates_count` | Сколько кандидатов пришло в minmax-фильтр. |
| `minmax_kept_candidates_count` | Сколько осталось после minmax-фильтра. |
| `minmax_dropped_candidates_count` | Сколько отброшено по слишком отличающемуся размаху движения. |
| `minmax_disabled_reason` | Если фильтр был выключен или не работал, тут причина. |

### Как читать

```text
regime_final_kept_count = 3
minmax_kept_candidates_count = 2
```

Minmax отрезал одного кандидата. Если из-за этого ломается potential, стоит тестировать более мягкий `candidate_minmax_hard_filter_max_ratio`.

## Score

После фильтров кандидаты ранжируются по итоговому score.

| Поле | Что означает |
|---|---|
| `score_candidates_count` | Сколько кандидатов дошло до scoring и сортировки. |

### Как читать

Если `score_candidates_count` мало, значит кандидатов уже мало после предыдущих фильтров. Сам score обычно не является главным местом отсева, он в основном сортирует.

## Potential

Этот этап строит прогнозный путь на основе top-score кандидатов. Именно он в итоге даёт направление `LONG/SHORT/NONE` и expected movement.

| Поле | Что означает |
|---|---|
| `potential_available` | `1`, если potential удалось рассчитать; `0`, если нет. |
| `potential_used_candidates_count` | Сколько кандидатов реально использовано для расчёта potential. |
| `potential_source_candidates_count` | Сколько кандидатов было доступно для potential до отбора top-N. |
| `potential_direction` | Направление potential: `LONG`, `SHORT`, `NONE`. |
| `potential_unavailable_reason` | Почему potential не рассчитался. Частый пример: `not_enough_valid_candidates:2<3`. |
| `potential_end_delta_points` | Ожидаемая конечная дельта в пунктах по итоговому weighted future path. |

### Как читать

```text
potential_unavailable_reason = not_enough_valid_candidates:2<3
```

До potential дошло только 2 кандидата, а минимум в настройках — 3. Не обязательно сразу снижать minimum. Сначала лучше понять, кто съел кандидатов: Pearson, regime или minmax.

## PNG / итоговая визуализация

| Поле | Что означает |
|---|---|
| `final_plot_candidates_count` | Сколько кандидатов дошло до финального набора, переданного в PNG. |
| `png_saved` | `1`, если PNG была сохранена; `0`, если нет; `NULL`, если до этого этапа не дошли. |
| `skip_reason` | Общая причина пропуска расчёта или неполного funnel, например `DATA_NOT_READY: ...`. |

### Как читать

```text
png_saved = 0
```

PNG не было, но строка в `candidate_funnel_events` всё равно показывает, где расчёт остановился.

```text
skip_reason = DATA_NOT_READY: ...
```

Данных для окна не хватило, но если `candidate_search_result` уже был получен, ранний funnel всё равно записан.

## Типовые паттерны диагностики

### 1. Мало кандидатов на самом старте

```text
raw_candidate_rows_count маленький
slot_offset_kept_count маленький
```

Смотреть:

- `history_lookback_days`
- `allowed_hours_ct_json`
- `signal_phase_seconds`
- `slot_offset_seconds`
- наличие истории в job DB

### 2. Кандидатов много, но Pearson почти всё режет

```text
matrix_valid_candidates_count большой
pearson_passed_count маленький
```

Смотреть:

- `pearson_min`
- длину pattern-window
- качество текущей формы паттерна

### 3. Pearson нормальный, но regime всё режет

```text
pearson_passed_count нормальный
regime_final_kept_count маленький
```

Смотреть:

- `market_regime_filter_mode`
- `current_relation`
- `regime_relation_mismatch_count`
- `regime_direction_mismatch_count`
- instrument-specific regression thresholds

### 4. Regime нормальный, но minmax добивает

```text
regime_final_kept_count нормальный
minmax_kept_candidates_count маленький
```

Смотреть:

- `candidate_minmax_hard_filter_max_ratio`
- текущий размах паттерна
- отличается ли масштаб MES/MNQ

### 5. До potential дошло меньше минимума

```text
potential_unavailable_reason = not_enough_valid_candidates:X<Y
```

Сначала смотреть, где потерялись кандидаты до potential. Снижать `candidate_potential_min_count` — последнее действие, потому что potential по 1–2 кандидатам статистически слабый.

## Минимальный SQL для анализа

Последние MES-расчёты:

```sql
SELECT
    signal_time_ct,
    raw_candidate_rows_count AS raw,
    slot_offset_kept_count AS slot_kept,
    matrix_valid_candidates_count AS matrix_valid,
    pearson_passed_count AS pearson,
    current_relation,
    current_price_direction,
    current_sma600_direction,
    regime_soft_kept_count AS soft,
    regime_hard_kept_count AS hard,
    regime_final_kept_count AS regime_final,
    regime_relation_mismatch_count AS rel_drop,
    regime_direction_mismatch_count AS dir_drop,
    minmax_kept_candidates_count AS minmax,
    minmax_dropped_candidates_count AS minmax_drop,
    score_candidates_count AS score,
    potential_available,
    potential_unavailable_reason,
    png_saved,
    skip_reason
FROM candidate_funnel_events
WHERE instrument_code = 'MES'
ORDER BY signal_bar_ts DESC
LIMIT 100;
```

Агрегация по инструментам:

```sql
SELECT
    instrument_code,
    COUNT(*) AS calcs,
    ROUND(AVG(raw_candidate_rows_count), 2) AS avg_raw,
    ROUND(AVG(slot_offset_kept_count), 2) AS avg_slot_kept,
    ROUND(AVG(matrix_valid_candidates_count), 2) AS avg_matrix_valid,
    ROUND(AVG(pearson_passed_count), 2) AS avg_pearson,
    ROUND(AVG(regime_final_kept_count), 2) AS avg_regime_final,
    ROUND(AVG(minmax_kept_candidates_count), 2) AS avg_minmax_kept,
    ROUND(AVG(score_candidates_count), 2) AS avg_score,
    SUM(CASE
        WHEN potential_unavailable_reason LIKE 'not_enough_valid_candidates%'
        THEN 1 ELSE 0
    END) AS not_enough_potential_count,
    SUM(CASE WHEN potential_available = 1 THEN 1 ELSE 0 END) AS potential_available_count
FROM candidate_funnel_events
GROUP BY instrument_code;
```
