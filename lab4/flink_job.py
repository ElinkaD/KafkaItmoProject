import json
from typing import Iterable

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import JobManagerCheckpointStorage
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    FlatMapFunction,
    KeySelector,
    KeyedBroadcastProcessFunction,
)
from pyflink.datastream.state import (
    ListStateDescriptor,
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)

from shared.config import require_env, require_int_env

EVENT_TYPE_INFO = Types.TUPLE(
    [Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING()]
)
RULE_TYPE_INFO = Types.TUPLE([Types.INT()])
OUTPUT_TYPE_INFO = Types.STRING()

BLOCKED_USERS_STATE_DESCRIPTOR = MapStateDescriptor(
    "blocked-users",
    Types.INT(),
    Types.BOOLEAN(),
)


class ParseEventJson(FlatMapFunction):
    def flat_map(self, value: str):
        try:
            raw = json.loads(value)
            user_id = int(raw["userId"])
            event_type = str(raw["eventType"])
            numeric_value = float(raw["value"])
            timestamp = str(raw["timestamp"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return

        yield user_id, event_type, numeric_value, timestamp


class ParseRuleJson(FlatMapFunction):
    def flat_map(self, value: str):
        try:
            raw = json.loads(value)
            blocked_user = int(raw["blockedUser"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return

        yield (blocked_user,)


class EventUserIdSelector(KeySelector):
    def get_key(self, value):
        return value[0]


class UserStatsWithBroadcastRules(KeyedBroadcastProcessFunction):
    def __init__(self, emit_interval_ms: int):
        self.emit_interval_ms = emit_interval_ms
        self.values_state = None
        self.next_timer_state = None

    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(10))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )

        values_descriptor = ListStateDescriptor("user-values", Types.DOUBLE())
        values_descriptor.enable_time_to_live(ttl_config)
        self.values_state = runtime_context.get_list_state(values_descriptor)

        timer_descriptor = ValueStateDescriptor("next-emit-timer", Types.LONG())
        self.next_timer_state = runtime_context.get_state(timer_descriptor)

    def process_element(self, value, ctx):
        user_id, event_type, numeric_value, event_timestamp = value
        if self._is_blocked(user_id, ctx):
            self._clear_user_state(ctx)
            yield (
                f"ignored_event userId={user_id} eventType={event_type} value={numeric_value:.2f} "
                f"reason=blocked timestamp={event_timestamp}"
            )
            return

        self.values_state.add(numeric_value)
        current_sum, values_count = self._calculate_sum()
        self._ensure_timer(ctx)

        yield (
            f"event userId={user_id} eventType={event_type} value={numeric_value:.2f} "
            f"valuesCount={values_count} currentSum={current_sum:.2f} timestamp={event_timestamp}"
        )

    def process_broadcast_element(self, value, ctx):
        blocked_user = value[0]
        broadcast_state = ctx.get_broadcast_state(BLOCKED_USERS_STATE_DESCRIPTOR)
        broadcast_state.put(blocked_user, True)
        yield f"rule blockedUser={blocked_user} action=blocked_forever"

    def on_timer(self, timestamp: int, ctx):
        current_user_id = ctx.get_current_key()
        if self._is_blocked(current_user_id, ctx):
            self._clear_user_state()
            return

        current_sum, values_count = self._calculate_sum()
        if values_count == 0:
            self.next_timer_state.clear()
            return

        next_timestamp = timestamp + self.emit_interval_ms
        ctx.timer_service().register_processing_time_timer(next_timestamp)
        self.next_timer_state.update(next_timestamp)

        yield (
            f"userId={current_user_id} "
            f"valuesCount={values_count} currentSum={current_sum:.2f}"
        )

    def _ensure_timer(self, ctx) -> None:
        if self.next_timer_state.value() is not None:
            return

        now = ctx.timer_service().current_processing_time()
        next_timestamp = ((now + self.emit_interval_ms) // 1000) * 1000
        if next_timestamp <= now:
            next_timestamp += 1000

        ctx.timer_service().register_processing_time_timer(next_timestamp)
        self.next_timer_state.update(next_timestamp)

    def _clear_user_state(self, ctx=None) -> None:
        self.values_state.clear()
        if ctx is None:
            self.next_timer_state.clear()
            return
        self._clear_timer(ctx)

    def _clear_timer(self, ctx) -> None:
        scheduled_timer = self.next_timer_state.value()
        if scheduled_timer is None:
            return

        ctx.timer_service().delete_processing_time_timer(scheduled_timer)
        self.next_timer_state.clear()

    def _calculate_sum(self) -> tuple[float, int]:
        values: Iterable[float] = self.values_state.get()
        total = 0.0
        count = 0
        for item in values:
            total += float(item)
            count += 1
        return total, count

    def _is_blocked(self, user_id: int, ctx) -> bool:
        broadcast_state = ctx.get_broadcast_state(BLOCKED_USERS_STATE_DESCRIPTOR)
        return broadcast_state.get(user_id) is True


def build_kafka_source(topic_env_name: str, group_env_name: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(require_env("LAB4_FLINK_BOOTSTRAP_SERVERS"))
        .set_topics(require_env(topic_env_name))
        .set_group_id(require_env(group_env_name))
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main() -> None:
    pipeline_name = require_env("LAB4_FLINK_PIPELINE_NAME")
    checkpoint_interval_ms = require_int_env("FLINK_CHECKPOINT_INTERVAL_MS")
    emit_interval_ms = require_int_env("LAB4_EMIT_INTERVAL_MS")

    env = StreamExecutionEnvironment.get_execution_environment()
    checkpoint_config = env.get_checkpoint_config()
    env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(3000)
    checkpoint_config.set_checkpoint_timeout(60_000)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    checkpoint_config.set_checkpoint_storage(JobManagerCheckpointStorage())

    events_stream = env.from_source(
        source=build_kafka_source("LAB4_EVENTS_TOPIC", "LAB4_EVENTS_GROUP_ID"),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="lab4-events-source",
    ).flat_map(ParseEventJson(), output_type=EVENT_TYPE_INFO)

    rules_stream = env.from_source(
        source=build_kafka_source("LAB4_RULES_TOPIC", "LAB4_RULES_GROUP_ID"),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="lab4-rules-source",
    ).flat_map(ParseRuleJson(), output_type=RULE_TYPE_INFO)

    broadcast_rules = rules_stream.broadcast(BLOCKED_USERS_STATE_DESCRIPTOR)

    processed = (
        events_stream.key_by(EventUserIdSelector(), key_type=Types.INT())
        .connect(broadcast_rules)
        .process(UserStatsWithBroadcastRules(emit_interval_ms), output_type=OUTPUT_TYPE_INFO)
    )

    processed.print()
    env.execute(pipeline_name)


if __name__ == "__main__":
    main()
