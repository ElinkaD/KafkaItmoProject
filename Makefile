PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
SPARK_SUBMIT := $(VENV)/bin/spark-submit
SPARK_HOME_CMD = $$(cd $(VENV)/lib/python*/site-packages/pyspark && pwd)
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.spark:spark-avro_2.13:4.1.1
LAB1_DIR := lab1
KAFKA_TOPIC ?= e-commerce-data
CSV_PATH ?= $(CURDIR)/data/E-Commerce Data.csv
OUTPUT_DIR ?= data/output
OUTPUT_PATH ?= $(OUTPUT_DIR)/parquet
CHECKPOINT_PATH ?= $(OUTPUT_DIR)/checkpoints/kafka_to_parquet
SHOW_ROWS ?= 20

.DEFAULT_GOAL := lab1-run

.PHONY: venv install kafka-up producer spark-once show-parquet lab1-run clean-output

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(VENV_PYTHON) -m pip install -r requirements.txt

kafka-up:
	docker compose up -d

producer:
	env KAFKA_TOPIC="$(KAFKA_TOPIC)" CSV_PATH="$(CSV_PATH)" $(VENV_PYTHON) $(LAB1_DIR)/producer.py

spark-once:
	env SPARK_HOME="$(SPARK_HOME_CMD)" SPARK_LOCAL_IP=127.0.0.1 SPARK_AVAILABLE_NOW=1 KAFKA_TOPIC="$(KAFKA_TOPIC)" OUTPUT_PATH="$(OUTPUT_PATH)" CHECKPOINT_PATH="$(CHECKPOINT_PATH)" \
	$(SPARK_SUBMIT) --packages $(SPARK_PACKAGES) $(LAB1_DIR)/kafka_to_parquet.py

show-parquet:
	env SPARK_HOME="$(SPARK_HOME_CMD)" SPARK_LOCAL_IP=127.0.0.1 OUTPUT_PATH="$(OUTPUT_PATH)" SHOW_ROWS="$(SHOW_ROWS)" $(VENV_PYTHON) -c "from pyspark.sql import SparkSession; import os; spark = SparkSession.builder.appName('show-parquet').getOrCreate(); df = spark.read.parquet(os.environ['OUTPUT_PATH']); print('rows=', df.count()); df.show(int(os.environ['SHOW_ROWS']), truncate=False); spark.stop()"

lab1-run:
	$(MAKE) producer CSV_PATH="$(CSV_PATH)" KAFKA_TOPIC="$(KAFKA_TOPIC)"
	$(MAKE) spark-once KAFKA_TOPIC="$(KAFKA_TOPIC)" OUTPUT_PATH="$(OUTPUT_PATH)" CHECKPOINT_PATH="$(CHECKPOINT_PATH)"
	$(MAKE) show-parquet OUTPUT_PATH="$(OUTPUT_PATH)" SHOW_ROWS="$(SHOW_ROWS)"

clean-output:
	rm -rf $(OUTPUT_DIR)
