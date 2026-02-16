#Реализовать продюсера для  Kafka, данные для отправки csv.
#Формат отправки в Kafka рекомендую использовать форматы protobuf или avro
import pandas as pd
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
import io

AVRO_SCHEMA = {
  "type": "record",
  "name": "EcommerceEvent",
  "namespace": "itmo.kafka",
  "fields": [
    {"name": "InvoiceNo", "type": "string"},
    {"name": "StockCode", "type": "string"},
    {"name": "Description", "type": ["null","string"], "default": None},
    {"name": "Quantity", "type": "int"},
    {"name": "InvoiceDate", "type": "string"},
    {"name": "UnitPrice", "type": "double"},
    {"name": "CustomerID", "type": ["null","int"], "default": None},
    {"name": "Country", "type": "string"}
  ]
}
schema = parse_schema(AVRO_SCHEMA)

def to_avro_bytes(record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")


producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

path = 'data/e_commerce_data.csv'
df = pd.read_csv(path, encoding="ISO-8859-1")

for _, row in df.iterrows():
    record = {
        "InvoiceNo": str(row["InvoiceNo"]),
        "StockCode": str(row["StockCode"]),
        "Description": None if pd.isna(row["Description"]) else str(row["Description"]),
        "Quantity": int(row["Quantity"]),
        "InvoiceDate": str(row["InvoiceDate"]),
        "UnitPrice": float(row["UnitPrice"]),
        "CustomerID": None if pd.isna(row["CustomerID"]) else int(row["CustomerID"]),
        "Country": str(row["Country"])
    }

    key = record["CustomerID"] if record["CustomerID"] is not None else record["InvoiceNo"]

    producer.produce(
        topic="e-commerce-data",
        key=str(key).encode("utf-8"),
        value=to_avro_bytes(record),
        on_delivery=delivery_report
    )
    producer.poll(0)

producer.flush() #все буферизированные собития будут перенесены в kafka
