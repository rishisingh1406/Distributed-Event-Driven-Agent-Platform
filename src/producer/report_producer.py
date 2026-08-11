from confluent_kafka import Producer
import json
import uuid
from datetime import datetime, timezone


BROKER = "localhost:9092"
TOPIC = "report.scheduled"


producer = Producer({
    "bootstrap.servers": BROKER,
})


event = {
    "metadata": {
        "event_id": str(uuid.uuid4()),
        "event_type": "report.scheduled",
        "event_version": 1,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "correlation_id": str(uuid.uuid4()),
        "producer": "report-scheduler",
    },
    "payload": {
        "report_id": "REPORT-1001",
        "report_type": "daily_operations",
    },
}


def delivery_report(error, message):

    if error:

        print(
            f"Delivery failed: {error}",
            flush=True,
        )

    else:

        print(
            f"Event delivered to "
            f"{message.topic()} "
            f"[partition {message.partition()}] "
            f"at offset {message.offset()}",
            flush=True,
        )


print(
    "Publishing report.scheduled event...",
    flush=True,
)


producer.produce(
    TOPIC,
    value=json.dumps(event).encode("utf-8"),
    callback=delivery_report,
)

producer.flush()


print(
    "\nEvent published successfully:",
    flush=True,
)

print(
    json.dumps(
        event,
        indent=2,
    ),
    flush=True,
)