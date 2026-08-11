from celery import Celery
from confluent_kafka import Producer

import json
import uuid
from datetime import datetime, timezone


REDIS_BROKER = "redis://localhost:6379/0"
REDPANDA_BROKER = "localhost:9092"

celery_app = Celery(
    "report_scheduler",
    broker=REDIS_BROKER,
)

producer = Producer({
    "bootstrap.servers": REDPANDA_BROKER,
})


@celery_app.task
def publish_report_scheduled():
    """Create and publish a report.scheduled event."""

    event = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "report.scheduled",
            "event_version": 1,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "correlation_id": str(uuid.uuid4()),
            "producer": "celery-beat",
        },
        "payload": {
            "report_id": str(uuid.uuid4()),
            "report_type": "daily_operations",
        },
    }

    producer.produce(
        "report.scheduled",
        value=json.dumps(event).encode("utf-8"),
    )

    producer.flush()

    print(
        "Published report.scheduled event:",
        flush=True,
    )

    print(
        json.dumps(event, indent=2),
        flush=True,
    )


# Run every 60 seconds during development.
celery_app.conf.beat_schedule = {
    "schedule-report-every-minute": {
        "task": (
            "src.scheduler.celery_app."
            "publish_report_scheduled"
        ),
        "schedule": 60.0,
    },
}