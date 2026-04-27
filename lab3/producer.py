import json
import random
import time
import uuid
from collections import deque
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from lab3.event_types import EVENT_TYPES
from shared.config import require_env, require_float_env, require_int_env


def delivery_report(err, msg) -> None:
    if err:
        print(f"Message delivery failed: {err}")


def normalize_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    supported = {"normal", "out_of_order", "late"}
    if normalized not in supported:
        raise RuntimeError(f"PRODUCER_MODE must be one of {sorted(supported)}, got {mode!r}")
    return normalized


def build_event(index: int, started_at: datetime, late_shift_seconds: int) -> dict[str, str | int]:
    event_time = started_at + timedelta(seconds=index)
    if late_shift_seconds > 0:
        event_time -= timedelta(seconds=late_shift_seconds)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "event_type": random.choice(EVENT_TYPES),
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
    }


def send_event(producer: Producer, topic: str, event: dict[str, str | int]) -> None:
    producer.produce(
        topic=topic,
        key=str(event["user_id"]).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_report,
    )
    producer.poll(0)


def main() -> None:
    kafka_topic = require_env("KAFKA_TOPIC")
    producer_mode = normalize_mode(require_env("PRODUCER_MODE"))
    events_count = require_int_env("LAB3_EVENTS_COUNT")
    send_interval_ms = require_int_env("LAB3_SEND_INTERVAL_MS")
    random_seed = require_int_env("LAB3_RANDOM_SEED")

    out_of_order_ratio = require_float_env("LAB3_OUT_OF_ORDER_RATIO")
    late_ratio = require_float_env("LAB3_LATE_RATIO")
    late_shift_min_sec = require_int_env("LAB3_LATE_SHIFT_MIN_SEC")
    late_shift_max_sec = require_int_env("LAB3_LATE_SHIFT_MAX_SEC")
    delayed_release_min = require_int_env("LAB3_DELAY_RELEASE_MIN_EVENTS")
    delayed_release_max = require_int_env("LAB3_DELAY_RELEASE_MAX_EVENTS")

    random.seed(random_seed)
    producer = Producer({"bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS")})

    buffer_size = random.randint(5, 10)
    reorder_ratio = out_of_order_ratio if producer_mode == "out_of_order" else 0.0
    delayed_ratio = late_ratio if producer_mode == "late" else 0.0
    started_at = datetime.now(UTC)

    send_buffer: list[dict[str, str | int]] = []
    # очередь отложенных событий
    # 1. количество отправленных сообщений после которого можно выпустить событие ниже
    # 2. само событие 
    delayed_events: deque[tuple[int, dict[str, str | int]]] = deque()
    sent_messages = 0
    created_events = 0

    print(
        f"Producer mode={producer_mode}, events={events_count}, buffer_size={buffer_size}, "
        f"reorder_ratio={reorder_ratio:.2f}, delayed_ratio={delayed_ratio:.2f}"
    )

    def emit_from_buffer() -> bool:
        nonlocal sent_messages
        if not send_buffer:
            return False
        # для out_of_order часть записей отправляем не с головы буфера, а случайно
        if reorder_ratio > 0 and random.random() < reorder_ratio:
            idx = random.randrange(len(send_buffer))
            event = send_buffer.pop(idx)
        else:
            event = send_buffer.pop(0)
        send_event(producer, kafka_topic, event)
        sent_messages += 1
        return True

    def emit_released_delayed() -> bool:
        nonlocal sent_messages
        emitted = False
        while delayed_events and delayed_events[0][0] <= sent_messages:
            _, event = delayed_events.popleft()
            send_event(producer, kafka_topic, event)
            sent_messages += 1
            emitted = True
        return emitted

    while created_events < events_count:
        late_shift_seconds = 0
        if producer_mode == "late":
            # late режиме event_time специально сдвигаем назад
            late_shift_seconds = random.randint(late_shift_min_sec, late_shift_max_sec)
        event = build_event(created_events, started_at, late_shift_seconds)
        created_events += 1

        if delayed_ratio > 0 and random.random() < delayed_ratio:
            # Событие отправится позже, чтобы попасть в allowed lateness во Flink job.
            release_after = sent_messages + random.randint(delayed_release_min, delayed_release_max)
            delayed_events.append((release_after, event))
        else:
            send_buffer.append(event)

        if len(send_buffer) >= buffer_size:
            emit_from_buffer()
            emit_released_delayed()
            time.sleep(send_interval_ms / 1000)

    while send_buffer:
        emit_from_buffer()
        emit_released_delayed()
        time.sleep(send_interval_ms / 1000)

    while delayed_events:
        _, event = delayed_events.popleft()
        send_event(producer, kafka_topic, event)
        sent_messages += 1
        time.sleep(send_interval_ms / 1000)

    producer.flush()
    print(f"Produced {sent_messages} events to topic={kafka_topic}")


if __name__ == "__main__":
    main()
