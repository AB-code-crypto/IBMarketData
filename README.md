# IBMarketData

IBMarketData — отдельный сервис для получения рыночных данных из Interactive Brokers.

Сервис подключается к TWS / IB Gateway, докачивает недостающую историю, получает realtime BID/ASK-котировки, сохраняет данные в SQLite и поддерживает prepared DB для дальнейшего анализа.

## Что делает сервис

- подключается к Interactive Brokers TWS / IB Gateway;
- определяет активный фьючерсный контракт;
- докачивает недостающую историческую BID/ASK-историю;
- получает realtime BID/ASK-бары;
- сохраняет данные в `data/price.sqlite3`;
- поддерживает `data/prepared.sqlite3`;
- закрывает свежие дырки после старта или reconnect;
- следит за состоянием IB-соединения;
- восстанавливает соединение при временных сбоях;
- пишет технические логи в консоль и, при наличии настроек, в Telegram.

## Основные файлы

```text
main.py                     Главная точка запуска сервиса
config.py                   Настройки подключения, путей БД и Telegram
contracts.py                Реестр инструментов и контрактов

core/ib_connector.py        Подключение к IB, heartbeat, reconnect
core/load_history.py        Загрузка исторических данных
core/load_realtime.py       Получение realtime BID/ASK
core/recent_gaps_service.py Добор свежих пропусков после старта/reconnect
core/db_initializer.py      Создание таблиц SQLite
core/db_sql.py              SQL-схемы и SQL-запросы
core/logger.py              Логирование
core/telegram_sender.py     Отправка технических сообщений в Telegram

ts/prepared_task.py         Фоновая задача синхронизации prepared DB
ts/prepared_sync.py         Синхронизация prepared DB
ts/prepared_builder.py      Построение prepared-строк