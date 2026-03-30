import time

from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import JobManagerCheckpointStorage
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udf

from shared.config import require_env, require_int_env
from shared.schema import sql_columns


def build_source_ddl() -> str:
    kafka_topic = require_env("KAFKA_TOPIC")
    kafka_bootstrap_servers = require_env("KAFKA_BOOTSTRAP_SERVERS")
    kafka_group_id = require_env("KAFKA_GROUP_ID")
    return f"""
    CREATE TABLE kafka_source (
        {sql_columns()}
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{kafka_topic}',
        'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
        'properties.group.id' = '{kafka_group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """


def build_sink_ddl() -> str:
    output_path = require_env("OUTPUT_PATH")
    return f"""
    CREATE TABLE parquet_sink (
        {sql_columns()}
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'parquet'
    )
    """


@udf(result_type=DataTypes.STRING())
def identity_with_optional_delay(value: str | None) -> str | None:
    map_delay_ms = require_int_env("FLINK_MAP_DELAY_MS")
    if map_delay_ms > 0:
        time.sleep(map_delay_ms / 1000)
    return value


def main() -> None:
    checkpoint_interval_ms = require_int_env("FLINK_CHECKPOINT_INTERVAL_MS")
    map_delay_ms = require_int_env("FLINK_MAP_DELAY_MS")
    pipeline_name = require_env("FLINK_PIPELINE_NAME")

    env = StreamExecutionEnvironment.get_execution_environment() # среда выполнения Flink streaming job
    #включаем checkpoint
    env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE) # режим EXACTLY_ONCE нужен, чтобы восстановление было корректным без потери/дублирования состояния на уровне механизма checkpoint

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(3000) # мин пазуа между checkpoints
    checkpoint_config.set_checkpoint_timeout(60_000)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3) # допускается несколько неудач
    #где хранить checkpoint state.
    #Сейчас он хранится у JobManager, поэтому checkpoint видны в UI, но не появляются как файлы в папке.
    checkpoint_config.set_checkpoint_storage(JobManagerCheckpointStorage())

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    table_env.get_config().set("pipeline.name", pipeline_name)
    # Дублируем интервал checkpoint уже на уровне Table API конфигурации, чтобы SQL/Table execution жил с той же настройкой.
    table_env.get_config().set("execution.checkpointing.interval", f"{checkpoint_interval_ms} ms") 
    # Говорим Flink не выкидывать checkpoint metadata сразу после отмены job
    table_env.get_config().set("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION")
    table_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")

    table_env.execute_sql(build_source_ddl())
    table_env.execute_sql(build_sink_ddl())

    # Если искусственная задержка выключена, убираем Python UDF из пайплайна:
    # переход в Python-процесс на каждую строку заметно замедляет обработку.
    description_expr = "description"
    if map_delay_ms > 0:
        table_env.create_temporary_system_function("identity_with_optional_delay", identity_with_optional_delay)
        description_expr = "identity_with_optional_delay(description)"

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(
        f"""
        INSERT INTO parquet_sink
        SELECT
            invoice_no,
            stock_code,
            {description_expr} AS description,
            quantity,
            invoice_date,
            unit_price,
            customer_id,
            country
        FROM kafka_source
        """
    )
    result = statement_set.execute()
    result.wait()


if __name__ == "__main__":
    main()
