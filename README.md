# KafkaItmoProject

Учебный проект с двумя отдельными лабораторными работами:

- `lab1`: `CSV -> Kafka -> Spark Structured Streaming -> Parquet`
- `lab2`: `CSV -> Kafka -> Flink -> Parquet`

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

Запуск:

```bash
make flink-up
make lab2-submit
make lab2-producer
```

Для замедления обработки в Python UDF можно выставить в `.env.local`:

```bash
FLINK_MAP_DELAY_MS=200
```

Полезные команды:

```bash
make lab2-list-jobs
make lab2-stop-savepoint JOB_ID=<job_id>
make lab2-run-from-savepoint SAVEPOINT=file:///workspace/data/output/lab2/savepoints/<savepoint_dir>
make lab2-verify
```

Flink Web UI после запуска доступен на `http://localhost:8081`.
