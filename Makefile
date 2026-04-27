ifneq (,$(wildcard .env.local))
include .env.local
export
endif

PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
SPARK_SUBMIT := $(VENV)/bin/spark-submit
SPARK_HOME_CMD = $$(cd $(VENV)/lib/python*/site-packages/pyspark && pwd)
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.spark:spark-avro_2.13:4.1.1
DOCKER_COMPOSE ?= docker compose
LAB1_DIR := lab1
LAB2_DIR := lab2
LAB3_DIR := lab3
LAB1_OUTPUT_DIR ?= data/output/lab1
LAB2_CONTAINER_ROOT ?= /workspace
LAB2_OUTPUT_DIR ?= $(CURDIR)/data/output/lab2
FLINK_REST_URL ?= http://localhost:8081
SHOW_ROWS ?= 20
JOB_ID ?=
SAVEPOINT ?=
TRIGGER_ID ?=

.DEFAULT_GOAL := lab1-run

.PHONY: venv install kafka-up flink-up producer spark-once show-parquet lab1-run clean-output lab2-prepare-output lab2-create-topic lab2-producer lab2-submit lab2-run lab2-list-jobs lab2-stop-savepoint lab2-savepoint-status lab2-run-from-savepoint lab2-verify lab2-clean lab3-create-topic lab3-submit lab3-producer lab3-run

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(VENV_PYTHON) -m pip install -r requirements.txt

kafka-up:
	$(DOCKER_COMPOSE) up -d kafka

flink-up:
	$(DOCKER_COMPOSE) up -d kafka jobmanager taskmanager

producer:
	env PYTHONPATH="$(CURDIR)" KAFKA_BOOTSTRAP_SERVERS="$(KAFKA_BOOTSTRAP_SERVERS)" KAFKA_TOPIC="$(KAFKA_TOPIC)" CSV_PATH="$(CSV_PATH)" $(VENV_PYTHON) -m $(LAB1_DIR).producer

spark-once:
	env PYTHONPATH="$(CURDIR)" SPARK_HOME="$(SPARK_HOME_CMD)" SPARK_LOCAL_IP=127.0.0.1 SPARK_AVAILABLE_NOW=1 KAFKA_TOPIC="$(KAFKA_TOPIC)" OUTPUT_PATH="$(OUTPUT_PATH)" CHECKPOINT_PATH="$(CHECKPOINT_PATH)" \
	KAFKA_BOOTSTRAP_SERVERS="$(KAFKA_BOOTSTRAP_SERVERS)" $(SPARK_SUBMIT) --packages $(SPARK_PACKAGES) $(LAB1_DIR)/kafka_to_parquet.py

show-parquet:
	env SPARK_HOME="$(SPARK_HOME_CMD)" SPARK_LOCAL_IP=127.0.0.1 OUTPUT_PATH="$(OUTPUT_PATH)" SHOW_ROWS="$(SHOW_ROWS)" $(VENV_PYTHON) -c "from pyspark.sql import SparkSession; import os; spark = SparkSession.builder.appName('show-parquet').getOrCreate(); df = spark.read.parquet(os.environ['OUTPUT_PATH']); print('rows=', df.count()); df.show(int(os.environ['SHOW_ROWS']), truncate=False); spark.stop()"

lab1-run:
	$(MAKE) producer CSV_PATH="$(CSV_PATH)" KAFKA_TOPIC="$(KAFKA_TOPIC)"
	$(MAKE) spark-once KAFKA_TOPIC="$(KAFKA_TOPIC)" OUTPUT_PATH="$(OUTPUT_PATH)" CHECKPOINT_PATH="$(CHECKPOINT_PATH)"
	$(MAKE) show-parquet OUTPUT_PATH="$(OUTPUT_PATH)" SHOW_ROWS="$(SHOW_ROWS)"

clean-output:
	rm -rf $(LAB1_OUTPUT_DIR)

lab2-producer:
	env PYTHONPATH="$(CURDIR)" KAFKA_BOOTSTRAP_SERVERS="$(LAB2_PRODUCER_BOOTSTRAP_SERVERS)" KAFKA_TOPIC="$(LAB2_KAFKA_TOPIC)" KAFKA_GROUP_ID="$(KAFKA_GROUP_ID)" CSV_PATH="$(CSV_PATH)" OUTPUT_PATH="$(LAB2_OUTPUT_PATH)" CHECKPOINT_PATH="$(LAB2_CHECKPOINT_PATH)" SAVEPOINT_PATH="$(LAB2_SAVEPOINT_PATH)" FLINK_CHECKPOINT_INTERVAL_MS="$(FLINK_CHECKPOINT_INTERVAL_MS)" FLINK_PIPELINE_NAME="$(FLINK_PIPELINE_NAME)" FLINK_MAP_DELAY_MS="$(FLINK_MAP_DELAY_MS)" $(VENV_PYTHON) -m $(LAB2_DIR).producer

lab2-create-topic:
	$(DOCKER_COMPOSE) exec -T kafka sh -lc 'kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic "$(LAB2_KAFKA_TOPIC)" --partitions 1 --replication-factor 1'

lab2-prepare-output:
	$(DOCKER_COMPOSE) exec -T jobmanager sh -lc 'mkdir -p /workspace/data/output/lab2/parquet /workspace/data/output/lab2/checkpoints /workspace/data/output/lab2/savepoints && chmod -R 777 /workspace/data/output/lab2'

lab2-submit: lab2-create-topic lab2-prepare-output
	$(DOCKER_COMPOSE) exec -T \
		-e PYTHONPATH="$(LAB2_CONTAINER_ROOT)" \
		-e PYFLINK_CLIENT_EXECUTABLE="python3" \
		-e PYFLINK_PYTHON="python3" \
		-e KAFKA_BOOTSTRAP_SERVERS="$(LAB2_FLINK_BOOTSTRAP_SERVERS)" \
		-e KAFKA_TOPIC="$(LAB2_KAFKA_TOPIC)" \
		-e KAFKA_GROUP_ID="$(KAFKA_GROUP_ID)" \
		-e OUTPUT_PATH="$(LAB2_OUTPUT_PATH)" \
		-e CHECKPOINT_PATH="$(LAB2_CHECKPOINT_PATH)" \
		-e SAVEPOINT_PATH="$(LAB2_SAVEPOINT_PATH)" \
		-e FLINK_CHECKPOINT_INTERVAL_MS="$(FLINK_CHECKPOINT_INTERVAL_MS)" \
		-e FLINK_PIPELINE_NAME="$(FLINK_PIPELINE_NAME)" \
		-e FLINK_MAP_DELAY_MS="$(FLINK_MAP_DELAY_MS)" \
		jobmanager \
		flink run -d -py $(LAB2_CONTAINER_ROOT)/$(LAB2_DIR)/flink_job.py

lab2-run:
	$(MAKE) flink-up
	$(MAKE) lab2-producer LAB2_KAFKA_TOPIC="$(LAB2_KAFKA_TOPIC)" CSV_PATH="$(CSV_PATH)"
	$(MAKE) lab2-submit LAB2_KAFKA_TOPIC="$(LAB2_KAFKA_TOPIC)"

lab2-list-jobs:
	$(DOCKER_COMPOSE) exec -T jobmanager flink list

lab2-stop-savepoint:
	curl -sS -X POST "$(FLINK_REST_URL)/jobs/$(JOB_ID)/stop" \
		-H "Content-Type: application/json" \
		-d '{"drain": false, "targetDirectory": "$(LAB2_SAVEPOINT_PATH)", "formatType": "CANONICAL"}'

lab2-savepoint-status:
	curl -sS "$(FLINK_REST_URL)/jobs/$(JOB_ID)/savepoints/$(TRIGGER_ID)"

lab2-run-from-savepoint:
	$(MAKE) lab2-prepare-output
	$(DOCKER_COMPOSE) exec -T \
		-e PYTHONPATH="$(LAB2_CONTAINER_ROOT)" \
		-e PYFLINK_CLIENT_EXECUTABLE="python3" \
		-e PYFLINK_PYTHON="python3" \
		-e KAFKA_BOOTSTRAP_SERVERS="$(LAB2_FLINK_BOOTSTRAP_SERVERS)" \
		-e KAFKA_TOPIC="$(LAB2_KAFKA_TOPIC)" \
		-e KAFKA_GROUP_ID="$(KAFKA_GROUP_ID)" \
		-e OUTPUT_PATH="$(LAB2_OUTPUT_PATH)" \
		-e CHECKPOINT_PATH="$(LAB2_CHECKPOINT_PATH)" \
		-e SAVEPOINT_PATH="$(LAB2_SAVEPOINT_PATH)" \
		-e FLINK_CHECKPOINT_INTERVAL_MS="$(FLINK_CHECKPOINT_INTERVAL_MS)" \
		-e FLINK_PIPELINE_NAME="$(FLINK_PIPELINE_NAME)" \
		-e FLINK_MAP_DELAY_MS="$(FLINK_MAP_DELAY_MS)" \
		jobmanager \
		flink run -d -Dexecution.savepoint.path="$(SAVEPOINT)" -py $(LAB2_CONTAINER_ROOT)/$(LAB2_DIR)/flink_job.py

lab2-verify:
	env OUTPUT_PATH="$(LAB2_VERIFY_OUTPUT_PATH)" SHOW_ROWS="$(SHOW_ROWS)" SPARK_HOME="$(SPARK_HOME_CMD)" SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) $(LAB2_DIR)/verify_parquet.py

lab2-clean:
	$(DOCKER_COMPOSE) exec -T jobmanager sh -lc 'rm -rf /workspace/data/output/lab2' || true
	mkdir -p $(LAB2_OUTPUT_DIR)/parquet $(LAB2_OUTPUT_DIR)/checkpoints $(LAB2_OUTPUT_DIR)/savepoints

lab3-create-topic:
	$(DOCKER_COMPOSE) exec -T kafka sh -lc 'kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic "$(LAB3_KAFKA_TOPIC)" --partitions 1 --replication-factor 1'

lab3-submit:
	$(DOCKER_COMPOSE) exec -T \
		-e PYTHONPATH="$(LAB2_CONTAINER_ROOT)" \
		-e PYFLINK_CLIENT_EXECUTABLE="python3" \
		-e PYFLINK_PYTHON="python3" \
		-e KAFKA_BOOTSTRAP_SERVERS="$(LAB3_FLINK_BOOTSTRAP_SERVERS)" \
		-e KAFKA_TOPIC="$(LAB3_KAFKA_TOPIC)" \
		-e KAFKA_GROUP_ID="$(LAB3_KAFKA_GROUP_ID)" \
		-e FLINK_CHECKPOINT_INTERVAL_MS="$(FLINK_CHECKPOINT_INTERVAL_MS)" \
		-e FLINK_PIPELINE_NAME="$(LAB3_FLINK_PIPELINE_NAME)" \
		-e LAB3_WINDOW_SIZE_SEC="$(LAB3_WINDOW_SIZE_SEC)" \
		-e LAB3_WATERMARK_OUT_OF_ORDER_SEC="$(LAB3_WATERMARK_OUT_OF_ORDER_SEC)" \
		-e LAB3_ALLOWED_LATENESS_SEC="$(LAB3_ALLOWED_LATENESS_SEC)" \
		jobmanager \
		flink run -d -py $(LAB2_CONTAINER_ROOT)/$(LAB3_DIR)/flink_job.py

lab3-producer:
	env PYTHONPATH="$(CURDIR)" KAFKA_BOOTSTRAP_SERVERS="$(LAB3_PRODUCER_BOOTSTRAP_SERVERS)" KAFKA_TOPIC="$(LAB3_KAFKA_TOPIC)" PRODUCER_MODE="$(PRODUCER_MODE)" LAB3_EVENTS_COUNT="$(LAB3_EVENTS_COUNT)" LAB3_SEND_INTERVAL_MS="$(LAB3_SEND_INTERVAL_MS)" LAB3_RANDOM_SEED="$(LAB3_RANDOM_SEED)" LAB3_OUT_OF_ORDER_RATIO="$(LAB3_OUT_OF_ORDER_RATIO)" LAB3_LATE_RATIO="$(LAB3_LATE_RATIO)" LAB3_LATE_SHIFT_MIN_SEC="$(LAB3_LATE_SHIFT_MIN_SEC)" LAB3_LATE_SHIFT_MAX_SEC="$(LAB3_LATE_SHIFT_MAX_SEC)" LAB3_DELAY_RELEASE_MIN_EVENTS="$(LAB3_DELAY_RELEASE_MIN_EVENTS)" LAB3_DELAY_RELEASE_MAX_EVENTS="$(LAB3_DELAY_RELEASE_MAX_EVENTS)" $(VENV_PYTHON) -m $(LAB3_DIR).producer

lab3-run:
	$(MAKE) flink-up
	$(MAKE) lab3-create-topic LAB3_KAFKA_TOPIC="$(LAB3_KAFKA_TOPIC)"
	$(MAKE) lab3-submit LAB3_KAFKA_TOPIC="$(LAB3_KAFKA_TOPIC)"
