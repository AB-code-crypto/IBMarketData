# IBMarketData

Робот для загрузки BID/ASK-данных из Interactive Brokers, построения rolling-сигналов и исполнения сделок через TWS / IB Gateway.

## Рабочий контур

```text
market_data -> position_sync -> execution -> signal -> trader
```

- `market_data` определяет активный контракт, загружает историю, закрывает свежие разрывы и пишет realtime BID/ASK-бары в отдельную SQLite-БД каждого инструмента;
- `position_sync` синхронизирует фактические брокерские позиции;
- `execution` исполняет trade intents, ставит обычные TP/SL и выполняет reconciliation;
- `signal` читает основную price DB, вычисляет `mid_close` как среднее `bid_close` и `ask_close`, строит только rolling-окна и ищет похожие исторические участки по Pearson;
- `trader` превращает свежий LONG/SHORT-сигнал в MARKET open/reverse intent с учётом позиции и защитных ограничений.

Отдельного сервиса подготовки признаков нет. Рыночные режимы, moving averages, MA-зоны, SLOT-окна и повторное удержание убыточной позиции удалены.

## Основные точки запуска

```text
run_market_data.py     История и realtime BID/ASK
run_position_sync.py   Позиции Interactive Brokers
run_execution.py       Исполнение и защитные ордера
run_signal.py          Rolling-only signal pipeline
run_trader.py          Решения по свежим сигналам
run_wt.py              Управляемый запуск всех сервисов в Windows Terminal
```

## Данные

Цены каждого логического инструмента хранятся отдельно:

```text
data/prices/MNQ.sqlite3
data/prices/MES.sqlite3
data/prices/EURUSD.sqlite3
data/prices/BTCUSD.sqlite3
```

Каноническая таблица содержит время, контракт и BID/ASK OHLC. `mid_close` не дублируется физическим столбцом: signal-сервис вычисляет его при чтении с округлением из `contracts.py`.

Операционное состояние находится в:

```text
data/state.sqlite3
data/trade.sqlite3
```

При первом запуске новой версии старые signal/trade metadata автоматически сужаются до актуальной rolling-only схемы. Execution также проверяет и подтверждённо отменяет оставшиеся у брокера `_EXT_TP`/`_EXT_SL` ордера старого механизма. Перед обновлением всё равно требуется резервная копия обеих БД.

## Проверка после обновления

```bash
python -m compileall -q .
python scripts/verify_rolling_refactor.py .
python -m unittest tester.rolling_signal_smoke_tester -v
python -m unittest tester.rolling_price_pipeline_tester -v
```

Первый запуск после глобального рефакторинга следует выполнять на paper account в состоянии FLAT.
