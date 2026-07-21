# Rolling-only deployment runbook

Этот документ описывает безопасное обновление робота после merge PR с rolling-only рефакторингом.

## Предварительные условия

Перед обновлением:

- робот полностью остановлен;
- на брокерском счёте нет открытых позиций;
- отсутствуют незавершённые заявки, которые вы хотите сохранить;
- сохранены резервные копии `data/state.sqlite3`, `data/trade.sqlite3` и каталога `data/prices`;
- локальный Git working tree чистый.

Проверка:

```powershell
git status --short
git branch --show-current
git rev-parse HEAD
```

Перед merge ожидается старый `master` commit:

```text
f7bfe1b18b9fd264a1b064e19defcfcca851e165
```

Если `git status --short` выводит файлы, обновление необходимо остановить и сначала сохранить локальные изменения.

## Обновление кода после merge

```powershell
git switch master
git fetch origin
git pull --ff-only origin master
```

Проверьте, что локальный `master` совпадает с удалённым:

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/master
```

Два SHA должны совпадать, а `git status --short` не должен ничего выводить.

## Зависимости и статические проверки

Активируйте штатное виртуальное окружение робота, затем выполните:

```powershell
python -m pip install -r requirements.txt
python -m compileall -q .
python scripts\verify_rolling_refactor.py .
python -m unittest tester.rolling_signal_smoke_tester -v
python -m unittest tester.rolling_price_pipeline_tester -v
```

Все команды должны завершиться с кодом `0`.

## Что не удалять

Не удаляйте:

- `data/state.sqlite3`;
- `data/trade.sqlite3`;
- `data/prices`;
- `.env` и локальные секреты;
- резервные копии БД.

Старые job/features DB больше не используются. Их можно удалить только после успешного paper-запуска и проверки новых сервисов.

## Первый запуск

Первый запуск выполняется на paper account.

Перед запуском проверьте в `.env`:

- адрес и порт paper TWS / IB Gateway;
- paper account ID;
- уникальные IB client IDs;
- Telegram-настройки при их использовании.

Запуск:

```powershell
python run_wt.py
```

Новый launcher должен открыть только пять сервисов:

```text
market_data
position_sync
execution
signal
trader
```

Сервиса `job_data` быть не должно.

## Контроль первого запуска

Проверяйте сервисы по порядку.

### 1. market_data

- IB API подключён;
- выбран активный фьючерсный контракт;
- история готова;
- realtime BID/ASK обновляется;
- нет циклических reconnect или ошибок SQLite.

### 2. position_sync

- подключён правильный paper account;
- позиция отображается как `FLAT`;
- snapshot свежий;
- нет неизвестного контракта или stale snapshot.

### 3. execution

- миграция `trade.sqlite3` завершилась без ошибки;
- проверка открытых ордеров IB прошла;
- старые `_EXT_TP`/`_EXT_SL` отсутствуют либо подтверждённо отменены;
- daily PnL monitor имеет статус `READY`;
- protective reconciliation и watchdog не сообщают ошибок.

Если execution завершается из-за невозможности проверить или отменить legacy exit orders, робот не запускать. Сначала вручную проверить заявки в TWS.

### 4. signal

- читает основную price DB;
- latest price bar свежий;
- `mid_close` рассчитывается из BID/ASK close;
- используются только rolling windows;
- отсутствуют сообщения про job DB, SLOT, regime, SMA или MA-zone.

### 5. trader

- видит свежий position snapshot;
- новые сигналы создают только MARKET intents;
- нет предупреждений IB clock guard;
- нет блокировки из-за pending execution statistics.

## Безопасный тест

Не создавайте искусственную реальную сделку только ради проверки.

Минимально допустимая проверка:

1. оставить paper account в состоянии FLAT;
2. дать сервисам отработать несколько циклов;
3. убедиться, что нет новых ERROR/CRITICAL сообщений;
4. проверить, что новые signal events имеют rolling-only формат;
5. при естественном сигнале проконтролировать создание MARKET intent и обычных TP/SL.

## Откат

При критической ошибке:

1. остановить все сервисы;
2. не удалять новые логи;
3. сохранить текущие `state.sqlite3` и `trade.sqlite3` отдельно для анализа;
4. вернуть предыдущий commit кода в отдельной ветке или каталоге;
5. восстановить резервные копии БД только при подтверждённой необходимости и при отсутствии новых брокерских исполнений после обновления.

Нельзя слепо восстанавливать старую trade DB, если после обновления были отправлены или исполнены ордера: сначала требуется reconciliation с фактическим состоянием IB.
