# KafkaItmoProject

Небольшой учебный проект для `lab1` с пайплайном:
`CSV -> Kafka -> Spark Structured Streaming -> Parquet`.

## Что делает проект

- `lab1/producer.py` читает данные из CSV и отправляет события в Kafka в формате Avro.
- `lab1/kafka_to_parquet.py` читает сообщения из Kafka через Spark, декодирует их и сохраняет в Parquet.
- `lab1/schema.py` хранит Avro-схему событий.

## Стек

- Kafka
- PySpark
- Avro
- Pandas
- Docker Compose

## Запуск

1. Установить зависимости:

```bash
make install
```

2. Поднять Kafka:

```bash
make kafka-up
```

3. Запустить полный сценарий:

```bash
make lab1-run
```

Команда:
- отправит данные из CSV в Kafka,
- считает сообщения через Spark,
- сохранит результат в `data/output/parquet`,
- покажет содержимое итогового Parquet.
