# source .venv/bin/activate
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
#   kafka_to_parquet.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main() -> None:
    spark = (
        SparkSession.builder.appName("kafka-to-parquet")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap = "localhost:9092"
    kafka_topic = "e-commerce-data"
    output_path = "output/parquet"
    checkpoint_path = "output/checkpoints/kafka_to_parquet"

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    result_df = kafka_df.select(
        col("timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("key"),
        col("value"),
    )

    query = (
        result_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
