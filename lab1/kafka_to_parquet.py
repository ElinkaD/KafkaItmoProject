import os

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

from schema import AVRO_SCHEMA_JSON


def is_truthy(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "on"}


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "e-commerce-data")
    output_path = os.getenv("OUTPUT_PATH", "output/parquet")
    checkpoint_path = os.getenv(
        "CHECKPOINT_PATH",
        "output/checkpoints/kafka_to_parquet",
    )
    available_now = is_truthy(os.getenv("SPARK_AVAILABLE_NOW"))

    spark = (
        SparkSession.builder.appName("kafka-to-parquet")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    decoded_df = kafka_df.select(
        col("timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("key"),
        from_avro(col("value"), AVRO_SCHEMA_JSON).alias("event"),
    )

    result_df = decoded_df.select(
        col("timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("key"),
        col("event.InvoiceNo").alias("invoice_no"),
        col("event.StockCode").alias("stock_code"),
        col("event.Description").alias("description"),
        col("event.Quantity").alias("quantity"),
        col("event.InvoiceDate").alias("invoice_date"),
        col("event.UnitPrice").alias("unit_price"),
        col("event.CustomerID").alias("customer_id"),
        col("event.Country").alias("country"),
    )

    writer = (
        result_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
