import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


BROKER = "localhost:9092"


def get_topics():
    admin = AdminClient({
        "bootstrap.servers": BROKER
    })

    metadata = admin.list_topics(timeout=5)

    ignored_topics = {
        "__consumer_offsets"
    }

    return sorted(
        topic
        for topic in metadata.topics.keys()
        if topic not in ignored_topics
    )


def get_topic_partitions(topic):
    admin = AdminClient({
        "bootstrap.servers": BROKER
    })

    metadata = admin.list_topics(
        topic=topic,
        timeout=5
    )

    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None:
        return []

    return sorted(
        topic_metadata.partitions.keys()
    )


def get_latest_offsets(topic):
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "dashboard-monitor",
        "enable.auto.commit": False,
    })

    try:
        partitions = get_topic_partitions(topic)

        offsets = {}

        for partition in partitions:

            topic_partition = TopicPartition(
                topic,
                partition
            )

            low, high = consumer.get_watermark_offsets(
                topic_partition
            )

            offsets[partition] = {
                "low": low,
                "high": high,
            }

        return offsets

    finally:
        consumer.close()


def get_consumer_lag(topic, group_id):

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group_id,
        "enable.auto.commit": False,
    })

    try:

        partitions = get_topic_partitions(topic)

        lag_by_partition = {}

        for partition in partitions:

            topic_partition = TopicPartition(
                topic,
                partition
            )

            low, high = consumer.get_watermark_offsets(
                topic_partition
            )

            committed = consumer.committed(
                [topic_partition],
                timeout=5
            )

            committed_offset = committed[0].offset

            if committed_offset < 0:
                lag = high
            else:
                lag = max(
                    high - committed_offset,
                    0
                )

            lag_by_partition[partition] = {
                "latest_offset": high,
                "committed_offset": committed_offset,
                "lag": lag,
            }

        return lag_by_partition

    finally:
        consumer.close()


def get_total_lag(topic, group_id):

    partition_lag = get_consumer_lag(
        topic,
        group_id
    )

    return sum(
        data["lag"]
        for data in partition_lag.values()
    )


def get_total_latest_offset(topic):

    offsets = get_latest_offsets(topic)

    return sum(
        data["high"]
        for data in offsets.values()
    )


def get_throughput(topic, previous_offset, previous_time):

    current_offset = get_total_latest_offset(topic)

    current_time = time.time()

    elapsed_seconds = current_time - previous_time

    if elapsed_seconds <= 0:
        return 0, current_offset, current_time

    events_processed = max(
        current_offset - previous_offset,
        0
    )

    events_per_second = (
        events_processed / elapsed_seconds
    )

    return (
        events_per_second,
        current_offset,
        current_time,
    )