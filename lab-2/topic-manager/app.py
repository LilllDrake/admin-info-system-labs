import os
import time

from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    ResourceType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

TOPIC_NAME = os.getenv(
    "TOPIC_NAME",
    "orders",
)

TOPIC_PARTITIONS = int(
    os.getenv("TOPIC_PARTITIONS", "1")
)

TOPIC_REPLICATION_FACTOR = int(
    os.getenv("TOPIC_REPLICATION_FACTOR", "1")
)

TOPIC_RETENTION_MS = os.getenv(
    "TOPIC_RETENTION_MS",
    "604800000",
)


admin = AdminClient(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
)


def wait_for_kafka():
    for attempt in range(30):
        try:
            admin.list_topics(timeout=5)
            print("Kafka is ready")
            return
        except Exception as error:
            print(
                f"Waiting for Kafka: "
                f"attempt {attempt + 1}/30: {error}"
            )
            time.sleep(2)

    raise RuntimeError("Kafka did not become ready")


def update_retention():
    topic_resource = ConfigResource(
        ResourceType.TOPIC,
        TOPIC_NAME,
    )

    describe_result = admin.describe_configs(
        [topic_resource]
    )

    configs = describe_result[topic_resource].result(
        timeout=30
    )

    current_retention = configs["retention.ms"].value

    print(
        f"Current retention.ms={current_retention}, "
        f"desired retention.ms={TOPIC_RETENTION_MS}"
    )

    if current_retention == TOPIC_RETENTION_MS:
        print("Retention is already configured")
        return

    retention_entry = ConfigEntry(
        name="retention.ms",
        value=TOPIC_RETENTION_MS,
        incremental_operation=AlterConfigOpType.SET,
    )

    topic_resource.add_incremental_config(
        retention_entry
    )

    update_result = admin.incremental_alter_configs(
        [topic_resource]
    )

    update_result[topic_resource].result(timeout=30)

    print(
        f"Updated topic {TOPIC_NAME}: "
        f"retention.ms={TOPIC_RETENTION_MS}"
    )


def increase_partitions(current_partitions):
    if TOPIC_PARTITIONS <= current_partitions:
        return

    new_partitions = NewPartitions(
        TOPIC_NAME,
        TOPIC_PARTITIONS,
    )

    result = admin.create_partitions(
        [new_partitions]
    )

    result[TOPIC_NAME].result(timeout=30)

    print(
        f"Increased partitions for {TOPIC_NAME}: "
        f"{current_partitions} -> {TOPIC_PARTITIONS}"
    )


def create_or_update_topic():
    metadata = admin.list_topics(timeout=10)

    if TOPIC_NAME in metadata.topics:
        current_partitions = len(
            metadata.topics[TOPIC_NAME].partitions
        )

        print(
            f"Topic {TOPIC_NAME} already exists: "
            f"partitions={current_partitions}"
        )

        increase_partitions(current_partitions)
        update_retention()
        return

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICATION_FACTOR,
        config={
            "retention.ms": TOPIC_RETENTION_MS,
        },
    )

    result = admin.create_topics([topic])
    result[TOPIC_NAME].result(timeout=30)

    print(
        f"Created topic {TOPIC_NAME}: "
        f"partitions={TOPIC_PARTITIONS}, "
        f"replication_factor="
        f"{TOPIC_REPLICATION_FACTOR}, "
        f"retention_ms={TOPIC_RETENTION_MS}"
    )


wait_for_kafka()
create_or_update_topic()