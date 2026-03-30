import json
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from shared.config import require_env, require_path_env


def delivery_report(err, msg) -> None:
    if err:
        print(f"Message delivery failed: {err}")


def csv_path() -> Path:
    path = require_path_env("CSV_PATH")
    if path.exists():
        return path

    raise FileNotFoundError(
        f"CSV file was not found: {path}. "
        "Place the dataset there or set CSV_PATH in .env.local."
    )


def main() -> None:
    producer = Producer({"bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS")})
    df = pd.read_csv(csv_path(), encoding="ISO-8859-1")


    for _, row in df.iterrows():
        payload = {
            "invoice_no": str(row["InvoiceNo"]),
            "stock_code": str(row["StockCode"]),
            "description": None if pd.isna(row["Description"]) else str(row["Description"]),
            "quantity": int(row["Quantity"]),
            "invoice_date": str(row["InvoiceDate"]),
            "unit_price": float(row["UnitPrice"]),
            "customer_id": None if pd.isna(row["CustomerID"]) else int(row["CustomerID"]),
            "country": str(row["Country"]),
        }
        key = payload["customer_id"] if payload["customer_id"] is not None else payload["invoice_no"]

        producer.produce(
            topic=require_env("KAFKA_TOPIC"),
            key=str(key).encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush()


if __name__ == "__main__":
    main()
