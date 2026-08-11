
from confluent_kafka import Consumer, Producer
import json
import uuid
from datetime import datetime, timezone

from src.agents.scheduled_report.agent import (
    ScheduledReportAgent,
)


# ============================================================
# Redpanda Configuration
# ============================================================

BROKER = "localhost:9092"

INPUT_TOPIC = "report.scheduled"
OUTPUT_TOPIC = "report.completed"

GROUP_ID = "scheduled-report-group"


# ============================================================
# Kafka Consumer
# ============================================================

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})


# ============================================================
# Kafka Producer
# ============================================================

producer = Producer({
    "bootstrap.servers": BROKER,
})


# ============================================================
# Scheduled Report Agent
# ============================================================

agent = ScheduledReportAgent()


# ============================================================
# Subscribe to Input Topic
# ============================================================

consumer.subscribe([INPUT_TOPIC])


print(
    "Scheduled Report Agent started...",
    flush=True,
)

print(
    f"Listening to topic: {INPUT_TOPIC}",
    flush=True,
)

print(
    f"Consumer group: {GROUP_ID}",
    flush=True,
)


# ============================================================
# Event Processing Loop
# ============================================================

try:

    while True:

        # Wait for an event from Redpanda
        message = consumer.poll(1.0)

        # No event available
        if message is None:
            continue

        # Kafka/Redpanda error
        if message.error():

            print(
                f"Consumer error: {message.error()}",
                flush=True,
            )

            continue

        # ====================================================
        # Decode Event
        # ====================================================

        event = json.loads(
            message.value().decode("utf-8")
        )

        print(
            "\nEvent received:",
            flush=True,
        )

        print(
            json.dumps(
                event,
                indent=2,
            ),
            flush=True,
        )

        print(
            f"Topic: {message.topic()}",
            flush=True,
        )

        print(
            f"Partition: {message.partition()}",
            flush=True,
        )

        print(
            f"Offset: {message.offset()}",
            flush=True,
        )

        # ====================================================
        # Generate Report
        # ====================================================

        print(
            "\nGenerating report...",
            flush=True,
        )

        report = agent.process(event)

        print(
            "\nReport generated:",
            flush=True,
        )

        print(
            json.dumps(
                report,
                indent=2,
            ),
            flush=True,
        )

        # ====================================================
        # Create report.completed Event
        # ====================================================

        completed_event = {
            "metadata": {
                "event_id": str(uuid.uuid4()),
                "event_type": "report.completed",
                "event_version": 1,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "correlation_id": event[
                    "metadata"
                ]["correlation_id"],
                "producer": "scheduled-report-agent",
            },
            "payload": report,
        }

        # ====================================================
        # Publish Completed Event
        # ====================================================

        producer.produce(
            OUTPUT_TOPIC,
            value=json.dumps(
                completed_event
            ).encode("utf-8"),
        )

        producer.flush()

        print(
            "\nReport completed event published:",
            flush=True,
        )

        print(
            json.dumps(
                completed_event,
                indent=2,
            ),
            flush=True,
        )

        print(
            "\nReport processing completed successfully.",
            flush=True,
        )


# ============================================================
# Shutdown
# ============================================================

except KeyboardInterrupt:

    print(
        "\nStopping Scheduled Report Agent...",
        flush=True,
    )


finally:

    consumer.close()

    print(
        "Report consumer closed.",
        flush=True,
    )

