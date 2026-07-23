# Shadow-runbook: target `market_data`

**Статус:** shadow only; не подключён к legacy signal/trader/execution  
**Ветка:** `agent/architecture-rewrite`  
**Исходный runtime:** `5073dbea3f96740b4390e113d06578a514918f00`

## 1. Назначение

Новый сервис получает недавнюю историю и realtime 5-second bars по BID и ASK для активного MNQ futures contract и пишет их только в собственную target-БД.

Он нужен для проверки новой границы ответственности:

```text
IB BID/ASK facts
→ internal side staging
→ complete canonical MarketBarV1
→ public read-only views
```

Сервис не:

- отправляет broker orders;
- пишет `data/prices/MNQ.sqlite3` старого робота;
- пишет `state.sqlite3`, `trade.sqlite3` или `positions_latest`;
- публикует signal events;
- влияет на legacy trader/execution;
- импортирует старые `contracts.py`, `core` или `ib_signal` packages.

## 2. Текущие ограничения

Это не готовая замена production market-data.

1. Реализован только futures-профиль MNQ и только 5-second BID/ASK bars.
2. Исторический bootstrap загружает короткое recent-window, а не всю годовую историю.
3. Active contract берётся из target catalog, но текущий session calendar не production-qualified: holidays и early closes ещё не заполнены.
4. Переключение контракта в этом slice реактивное: старая подписка закрывается на `active_to`, затем resolver ждёт следующий active interval и запускает history + realtime нового контракта. Двухфазная предварительная подписка следующего контракта будет отдельным slice.
5. CI использует fake IB adapter. Реальная TWS/Gateway и реальные market-data permissions должны быть проверены отдельным shadow run.
6. `volume`, `average` и `bar_count` в `MarketBarV1` сейчас `NULL`: BID/ASK quote bars не используются как источник торгового объёма.

Эти ограничения запрещают live cutover, но не мешают безопасному параллельному наблюдению.

## 3. Изоляция deployment

Каждый account/компьютер запускает собственную копию с отдельными:

```text
IBMD_DEPLOYMENT_ID
IBMD_DATA_ROOT
IB_HOST / IB_PORT / IB_CLIENT_ID / IB_ACCOUNT_ID
```

Target market-data использует `IB_CLIENT_ID + 80` по умолчанию.

Файлы сервиса:

```text
<IBMD_DATA_ROOT>/market_data/MNQ.sqlite3
<IBMD_DATA_ROOT>/runtime/health/market_data.json
<IBMD_DATA_ROOT>/runtime/locks/market_data.lock
```

## 4. Подготовка environment

Пример для paper deployment:

```bash
export IBMD_DEPLOYMENT_ID=paper-mnq-shadow-01
export IBMD_DATA_ROOT=/absolute/path/to/ibmd-shadow
export IBMD_APPLICATION_VERSION=0.1.0-dev

export IB_HOST=127.0.0.1
export IB_PORT=7497
export IB_CLIENT_ID=200
export IB_ACCOUNT_ID=U0000000
```

На Windows те же значения задаются в environment выбранного deployment. `.env` старого робота автоматически не используется target package.

## 5. Явная offline migration

Runtime не создаёт и не перестраивает schema.

```bash
python scripts/run_target_migrations.py \
  --manifest migrations/market_data.v1.json \
  --database "$IBMD_DATA_ROOT/market_data/MNQ.sqlite3" \
  --application-version "$IBMD_APPLICATION_VERSION" \
  --apply
```

Проверка без IB:

```bash
python apps/run_market_data_v2.py --validate-store-only
```

Ожидаемый результат:

```text
market-data store is compatible: .../market_data/MNQ.sqlite3
```

Если migration не выполнена, entrypoint завершится с code `2`, опубликует `BLOCKED` health и не создаст БД самостоятельно.

## 6. Одноразовый recent-history shadow

```bash
python apps/run_market_data_v2.py \
  --history-only \
  --history-lookback-seconds 3600
```

Процесс:

1. проверяет expected IB account через `managedAccounts`;
2. разрешает active contract из target calendar;
3. запрашивает BID и ASK с одним `source_generation_id`;
4. публикует только совпавшие полные пары;
5. завершает работу без realtime subscription.

Во время declared rollover gap команда завершается code `3`, а не угадывает contract.

## 7. Непрерывный shadow-run

```bash
python apps/run_market_data_v2.py
```

Полезные параметры:

```text
--client-id-offset 80
--history-lookback-seconds 3600
--bar-max-age-seconds 60
--realtime-read-timeout-seconds 1
--reconnect-delay-seconds 5
--historical-timeout-seconds 30
--historical-request-spacing-seconds 1
--writer-queue-maxsize 10000
```

`--use-rth` по умолчанию выключен, что сохраняет baseline `useRTH=false`.

## 8. Семантика complete bar

BID и ASK могут приходить в любом порядке, но публичный bar создаётся только если совпадают:

```text
instrument_id
con_id
bar_start_utc
bar_duration_seconds
source_session_id
source_generation_id
source_kind
```

Следовательно, нельзя склеить:

- BID старой IB-session с ASK новой session;
- historical BID с realtime ASK;
- старую сторону до reconnect с новой стороной после reconnect;
- исправление одного поколения со стороной другого поколения.

Неполные стороны доступны только во внутренних staging/audit tables.

## 9. Public read-only products

```text
public_market_bars_v1
public_market_data_latest_v1
public_market_data_status_v1
```

Проверка latest bar:

```bash
python - <<'PY'
from pathlib import Path
import os, sqlite3

path = Path(os.environ['IBMD_DATA_ROOT']) / 'market_data' / 'MNQ.sqlite3'
uri = f"file:{path.resolve().as_posix()}?mode=ro"
conn = sqlite3.connect(uri, uri=True)
conn.row_factory = sqlite3.Row
conn.execute('PRAGMA query_only = ON')
row = conn.execute(
    'SELECT * FROM public_market_data_latest_v1 WHERE instrument_id=?',
    ('MNQ',),
).fetchone()
print(None if row is None else dict(row))
conn.close()
PY
```

`mid_close` не хранится. Он вычисляется consumer-ом:

```text
(bid_close + ask_close) / 2
```

с округлением согласно instrument policy.

## 10. History/realtime seam

Один и тот же natural bar может быть опубликован повторно полным набором данных:

```text
history revision 1
→ realtime/recent-backfill correction revision 2
```

Правила:

- идентичная полная пара не увеличивает revision;
- изменившаяся полная пара увеличивает revision;
- `first_published_at_utc` сохраняется;
- запоздавшая пара с более старым `published_at_utc` не откатывает новую canonical revision;
- backfill старого timestamp не двигает latest pointer назад.

## 11. Health

Файл:

```text
<IBMD_DATA_ROOT>/runtime/health/market_data.json
```

Состояния:

```text
READY     — latest complete bar fresh, источник работает
DEGRADED  — источник ошибся, но последний complete bar ещё fresh
BLOCKED   — complete bar отсутствует/устарел или contract недоступен
```

Ошибка источника не создаёт bar и не обновляет freshness старого bar.

## 12. Что сравнивать со старым роботом

На одинаковом active contract и timestamp:

- наличие обеих сторон BID/ASK;
- OHLC каждой стороны;
- число полных 5-second bars;
- contract `conId/localSymbol`;
- отсутствие потери в history/realtime seam;
- поведение при TWS reconnect;
- задержку latest complete bar;
- отсутствие публичных half-bars;
- revision при backfill/correction.

Допустимые различия должны быть объяснены. Фраза «примерно совпадает» для price pipeline неприемлема.

## 13. Безопасная остановка

`Ctrl+C`:

1. прекращает active reader loop;
2. закрывает BID/ASK subscriptions;
3. дожидается serialized writer queue;
4. закрывает IB session;
5. публикует `STOPPED` health;
6. освобождает service lock.

Остановка shadow market-data не затрагивает работающий legacy robot.

## 14. Gate следующего этапа

Перед подключением target signal требуется:

- фактический shadow-run через TWS/Gateway;
- совпадение complete BID/ASK bars на контрольном окне;
- проверка reconnect;
- проверка history/realtime seam;
- отдельный двухфазный contract hot-switch;
- production-qualified session calendar;
- достаточное target history coverage для rolling profile.
