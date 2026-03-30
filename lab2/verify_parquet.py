import os

from pyspark.sql import SparkSession


def main() -> None:
    output_path = os.getenv("OUTPUT_PATH", "data/output/lab2/parquet")
    show_rows = int(os.getenv("SHOW_ROWS", "20"))

    spark = SparkSession.builder.appName("lab2-verify-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(output_path)
    print(f"rows={df.count()}")
    df.show(show_rows, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
