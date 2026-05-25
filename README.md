# KafkaItmoProject

Учебный проект с четырьмя отдельными лабораторными работами:

- `lab1`: `CSV -> Kafka -> Spark Structured Streaming -> Parquet`
- `lab2`: `CSV -> Kafka -> Flink -> Parquet`
- `lab3`: `Event Generator -> Kafka -> Flink (DataStream API, Event Time Windows) -> Console`
- `lab4`: `Events + Rules -> Kafka -> Flink (Keyed State, Blocking, TTL) -> Console`

## Конфигурация

Общие переменные лежат в `.env.local`,
а чтение и валидация вынесены в
[shared/config.py](/home/elina/PycharmProjects/KafkaItmoProject/shared/config.py).

## Lab 1

- `lab1/producer.py` читает данные из CSV и отправляет события в Kafka в формате Avro.
- `lab1/kafka_to_parquet.py` читает сообщения из Kafka через Spark, декодирует их и сохраняет в Parquet.
- `shared/schema.py` хранит общую Avro-схему и SQL-описание полей для Flink.

Запуск:

```bash
make install
make kafka-up
make lab1-run
```

## Lab 2

- `lab2/producer.py` читает тот же CSV и отправляет сообщения в Kafka в формате JSON.
- `lab2/flink_job.py` читает сообщения из Kafka и записывает их в Parquet.
- `lab2/verify_parquet.py` считает количество строк в Parquet.
- `shared/schema.py` используется как общая схема проекта.
- checkpoint и savepoint разделены в `data/output/lab2`.

Схема работы приложения:

```text
make flink-up
-> docker compose up -d kafka jobmanager taskmanager
-> поднимается Kafka и кластер Flink

make lab2-submit
-> docker compose exec jobmanager flink run -d -py /workspace/lab2/flink_job.py
-> Flink запускает Python job внутри кластера

make lab2-producer
-> lab2/producer.py читает CSV
-> отправляет JSON-сообщения в Kafka topic e-commerce-data-lab2

lab2/flink_job.py
-> читает данные из Kafka
-> обрабатывает их в PyFlink
-> сохраняет результат в data/output/lab2/parquet
```

Поток данных:

```text
CSV
-> lab2/producer.py
-> Kafka topic
-> lab2/flink_job.py
-> Flink JobManager / TaskManager
-> data/output/lab2/parquet
```

Запуск:

```bash
make flink-up
make lab2-submit
make lab2-producer
```

Для замедления обработки в Python UDF можно выставить в `.env.local`:

```bash
FLINK_MAP_DELAY_MS=5
```

Полезные команды:

```bash
make lab2-list-jobs
make lab2-stop-savepoint JOB_ID=<job_id>
make lab2-savepoint-status JOB_ID=3e94630228d7b26bdd5e1f0e71ce8f99 TRIGGER_ID=1251d34e305d569187fb77b21f55a179
make lab2-run-from-savepoint SAVEPOINT=file:///workspace/data/output/lab2/savepoints/<savepoint_dir>
make lab2-verify
```

Flink Web UI после запуска доступен на `http://localhost:8081`.

## Lab 3

ТЗ 3 практического занятия:

- написать producer, который генерирует события и отправляет их в Kafka;
- Flink читает события, обрабатывает по event time и считает оконные агрегаты;
- агрегат: общее количество сообщений в окне (tumbling window);
- результаты окон выводятся в консоль по мере срабатывания окон.

Формат события:

- `event_id`
- `user_id`
- `event_type`
- `event_time`

Producer поддерживает 3 режима:

- `normal`: большая часть событий отправляется по порядку;
- `out_of_order`: часть событий (по умолчанию около 25%) отправляется не по порядку из буфера;
- `late`: часть событий (по умолчанию около 15%) отправляется с задержкой позже новых событий.

Реализация в проекте:

- `lab3/producer.py` генерирует синтетические события;
- `lab3/event_types.py` содержит банк `event_type`;
- `lab3/flink_job.py` реализован через DataStream API:
  - чтение из Kafka;
  - извлечение `event_time`;
  - watermarks для out-of-order;
  - `allowed_lateness` для поздних событий;
  - tumbling event-time window и подсчет количества сообщений.

Запуск:

```bash
make flink-up
make lab3-create-topic
make lab3-submit
PRODUCER_MODE=normal make lab3-producer
PRODUCER_MODE=out_of_order make lab3-producer
PRODUCER_MODE=late make lab3-producer

## Lab 4

ТЗ 4 практического занятия:

- реализовать поток `events` с полями `userId`, `eventType`, `value`, `timestamp`;
- `userId` должен идти последовательно по кругу от `1` до `100`;
- реализовать поток `rules` с полем `blockedUser`;
- поток `rules` должен нечасто публиковать случайного заблокированного пользователя;
- основной поток `events` должен обрабатываться во Flink по ключу `userId`;
- для каждого пользователя нужно хранить `ListState<Double>` со значениями `value`;
- при обработке события нужно считать сумму по всему списку значений пользователя;
- при поступлении нового `blockedUser` накопленная статистика этого пользователя больше не должна использоваться;
- для состояния должен быть настроен `StateTtlConfig` с TTL `10` секунд;
- в конце обработки нужно периодически выводить текущую сумму `value` по пользователям.

Запуск:

```bash
make flink-up
make lab4-create-topics
make lab4-submit
make lab4-producer
```