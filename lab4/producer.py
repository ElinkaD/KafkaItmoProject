import json
import random
import time
from datetime import UTC, datetime

from confluent_kafka import Producer

from lab3.event_types import EVENT_TYPES
from shared.config import require_env, require_float_env, require_int_env

USER_ID_MIN = 1
USER_ID_MAX = 100


def delivery_report(err, msg) -> None:
    if err:
        print(f"Message delivery failed: {err}")


def build_event(
    user_id: int,
    min_value: float,
    max_value: float,
) -> dict[str, int | float | str]:
    return {
        "userId": user_id,
        "eventType": random.choice(EVENT_TYPES),
        "value": round(random.uniform(min_value, max_value), 2),
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def build_rule() -> dict[str, int]:
    return {"blockedUser": random.randint(USER_ID_MIN, USER_ID_MAX)}


def send_event(producer: Producer, topic: str, event: dict[str, int | float | str]) -> None:
    producer.produce(
        topic=topic,
        key=str(event["userId"]).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_report,
    )
    producer.poll(0)


def send_rule(producer: Producer, topic: str, rule: dict[str, int]) -> None:
    producer.produce(
        topic=topic,
        key=str(rule["blockedUser"]).encode("utf-8"),
        value=json.dumps(rule).encode("utf-8"),
        on_delivery=delivery_report,
    )
    producer.poll(0)


def main() -> None:
    kafka_bootstrap_servers = require_env("LAB4_PRODUCER_BOOTSTRAP_SERVERS")
    events_topic = require_env("LAB4_EVENTS_TOPIC")
    rules_topic = require_env("LAB4_RULES_TOPIC")
    events_count = require_int_env("LAB4_EVENTS_COUNT")
    send_interval_ms = require_int_env("LAB4_SEND_INTERVAL_MS")
    rule_frequency_events = require_int_env("LAB4_RULE_FREQUENCY_EVENTS")
    random_seed = require_int_env("LAB4_RANDOM_SEED")
    min_value = require_float_env("LAB4_MIN_VALUE")
    max_value = require_float_env("LAB4_MAX_VALUE")

    if rule_frequency_events <= 0:
        raise RuntimeError("LAB4_RULE_FREQUENCY_EVENTS must be > 0")
    if min_value > max_value:
        raise RuntimeError("LAB4_MIN_VALUE must be <= LAB4_MAX_VALUE")

    random.seed(random_seed)
    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})

    current_user_id = USER_ID_MIN
    sent_events = 0
    rules_sent = 0

    print(
        f"Producing lab4 events={events_count}, events_topic={events_topic}, "
        f"rules_topic={rules_topic}, rule_frequency={rule_frequency_events}, "
        f"value_range=[{min_value:.2f}, {max_value:.2f}]"
    )

    for event_index in range(1, events_count + 1):
        event = build_event(current_user_id, min_value, max_value)
        send_event(producer, events_topic, event)
        sent_events += 1

        if event_index % rule_frequency_events == 0:
            rule = build_rule()
            send_rule(producer, rules_topic, rule)
            rules_sent += 1

        current_user_id += 1
        if current_user_id > USER_ID_MAX:
            current_user_id = USER_ID_MIN

        time.sleep(send_interval_ms / 1000)

    producer.flush()
    print(f"Produced {sent_events} events and {rules_sent} blocking rules")


if __name__ == "__main__":
    main()
