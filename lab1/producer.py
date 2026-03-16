#Реализовать продюсера для  Kafka, данные для отправки csv.
#Формат отправки в Kafka рекомендую использовать форматы protobuf или avro

import io
import os
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

from schema import AVRO_SCHEMA

schema = parse_schema(AVRO_SCHEMA)


def to_avro_bytes(record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "e-commerce-data")
    csv_path_env = os.getenv("CSV_PATH")
    csv_path = Path(csv_path_env or "data/E-Commerce Data.csv")

    if csv_path_env is None and not csv_path.exists():
        fallback_paths = [
            Path("data/E-Commerce Data.csv"),
        ]
        csv_path = next((path for path in fallback_paths if path.exists()), csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file was not found: {csv_path}. "
            "Place the dataset there or override CSV_PATH."
        )

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    df = pd.read_csv(csv_path, encoding="ISO-8859-1")

    for _, row in df.iterrows():
        record = {
            "InvoiceNo": str(row["InvoiceNo"]),
            "StockCode": str(row["StockCode"]),
            "Description": None if pd.isna(row["Description"]) else str(row["Description"]),
            "Quantity": int(row["Quantity"]),
            "InvoiceDate": str(row["InvoiceDate"]),
            "UnitPrice": float(row["UnitPrice"]),
            "CustomerID": None if pd.isna(row["CustomerID"]) else int(row["CustomerID"]),
            "Country": str(row["Country"]),
        }

        key = record["CustomerID"] if record["CustomerID"] is not None else record["InvoiceNo"]

        producer.produce(
            topic=kafka_topic,
            key=str(key).encode("utf-8"),
            value=to_avro_bytes(record),
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush() #все буферизированные собития будут перенесены в kafka


if __name__ == "__main__":
    main()
