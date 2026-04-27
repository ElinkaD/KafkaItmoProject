import json
from datetime import datetime

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import JobManagerCheckpointStorage
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import FlatMapFunction, KeySelector, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from shared.config import require_env, require_int_env


# (
#   event_id: string,
#   user_id: int,
#   event_type: string,
#   event_time: string,
#   event_ts_ms: long нужен Flink'у для watermark и event-time окон
# )
EVENT_TYPE_INFO = Types.TUPLE(
    [Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()]
)

# Flink работает с event-time timestamp'ами именно в мс
def parse_iso_to_epoch_ms(value: str) -> int:
    normalized = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalized).timestamp() * 1000)


class ParseEventJson(FlatMapFunction):
    def flat_map(self, value: str):
        try:
            raw = json.loads(value)
            event_id = str(raw["event_id"])
            user_id = int(raw["user_id"])
            event_type = str(raw["event_type"])
            event_time = str(raw["event_time"])
            event_ts_ms = parse_iso_to_epoch_ms(event_time) #отдельно считаем event timestamp в мс
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return

        # для одного корректного JSON возвращаем одно событие
        yield event_id, user_id, event_type, event_time, event_ts_ms


# 4: event_ts_ms
# окна будут строиться не по времени прихода в Kafka/Flink,
# а по времени, которое лежит внутри самого события
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value[4]


class ConstantKeySelector(KeySelector):
    def get_key(self, value):
        return "all"


class CountWindowByKey(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = 0
        for _ in elements:
            count += 1

        window_start = datetime.utcfromtimestamp(context.window().start / 1000).isoformat() + "Z"
        window_end = datetime.utcfromtimestamp(context.window().end / 1000).isoformat() + "Z"
        current_watermark = context.current_watermark()
        if current_watermark < 0:
            watermark = "MIN_WATERMARK"
        else:
            watermark = datetime.utcfromtimestamp(current_watermark / 1000).isoformat() + "Z"
        yield (
            "window_start="
            f"{window_start} window_end={window_end} total_messages={count} watermark={watermark}"
        )


def main() -> None:
    kafka_topic = require_env("KAFKA_TOPIC")
    kafka_bootstrap_servers = require_env("KAFKA_BOOTSTRAP_SERVERS")
    kafka_group_id = require_env("KAFKA_GROUP_ID")
    pipeline_name = require_env("FLINK_PIPELINE_NAME")
    checkpoint_interval_ms = require_int_env("FLINK_CHECKPOINT_INTERVAL_MS")
    # Размер tumbling window в секундах
    window_size_sec = require_int_env("LAB3_WINDOW_SIZE_SEC")
    # Насколько события могут приходить не по порядку
    watermark_out_of_order_sec = require_int_env("LAB3_WATERMARK_OUT_OF_ORDER_SEC")
    # Сколько времени окно остаётся открытым для late events
    allowed_lateness_sec = require_int_env("LAB3_ALLOWED_LATENESS_SEC")

    env = StreamExecutionEnvironment.get_execution_environment()
    checkpoint_config = env.get_checkpoint_config()
    env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(3000)
    checkpoint_config.set_checkpoint_timeout(60_000)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    checkpoint_config.set_checkpoint_storage(JobManagerCheckpointStorage())
    env.get_config().set_auto_watermark_interval(1000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap_servers)
        .set_topics(kafka_topic)
        .set_group_id(kafka_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # создаём stream из Kafka
    raw_events = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-source",
    )

    parsed_events = raw_events.flat_map(ParseEventJson(), output_type=EVENT_TYPE_INFO)

    # Watermark допускает ограниченный out-of-order по event time
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(watermark_out_of_order_sec)
    ).with_timestamp_assigner(EventTimestampAssigner())

    events_with_watermarks = parsed_events.assign_timestamps_and_watermarks(watermark_strategy)

    # строим оконную агрегацию
    window_counts = (
        events_with_watermarks.key_by(ConstantKeySelector(), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_size_sec)))
        .allowed_lateness(allowed_lateness_sec * 1000)
        .process(CountWindowByKey(), output_type=Types.STRING())
    )

    window_counts.print()
    env.execute(pipeline_name)


if __name__ == "__main__":
    main()
