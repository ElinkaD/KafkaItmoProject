import json

AVRO_SCHEMA = {
    "type": "record",
    "name": "EcommerceEvent",
    "namespace": "itmo.kafka",
    "fields": [
        {"name": "InvoiceNo", "type": "string"},
        {"name": "StockCode", "type": "string"},
        {"name": "Description", "type": ["null", "string"], "default": None},
        {"name": "Quantity", "type": "int"},
        {"name": "InvoiceDate", "type": "string"},
        {"name": "UnitPrice", "type": "double"},
        {"name": "CustomerID", "type": ["null", "int"], "default": None},
        {"name": "Country", "type": "string"},
    ],
}

AVRO_SCHEMA_JSON = json.dumps(AVRO_SCHEMA)

FLINK_FIELDS = [
    ("invoice_no", "STRING"),
    ("stock_code", "STRING"),
    ("description", "STRING"),
    ("quantity", "INT"),
    ("invoice_date", "STRING"),
    ("unit_price", "DOUBLE"),
    ("customer_id", "INT"),
    ("country", "STRING"),
]


def sql_columns() -> str:
    return ",\n    ".join(f"{name} {dtype}" for name, dtype in FLINK_FIELDS)
