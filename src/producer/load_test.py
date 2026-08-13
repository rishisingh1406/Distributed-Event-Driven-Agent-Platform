from confluent_kafka import Producer
import json
import uuid
import time
from datetime import datetime, timezone


BROKER = "localhost:9092"
TOPIC = "ticket.created"

TOTAL_EVENTS = 500


producer = Producer({
    "bootstrap.servers": BROKER,
})


def delivery_report(err, message):

    if err is not None:
        print(
            f"Delivery failed: {err}",
            flush=True
        )
        return

    if message.offset() % 50 == 0:
        print(
            f"Delivered offset={message.offset()} "
            f"partition={message.partition()}",
            flush=True
        )


def create_ticket(ticket_number):

    ticket_id = f"LOAD-{ticket_number:04d}"

    return {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "ticket.created",
            "event_version": 1,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "correlation_id": str(uuid.uuid4()),
            "producer": "load-test",
        },

        "payload": {
            "ticket_id": ticket_id,
            "customer_id": f"C-{ticket_number:04d}",
            "title": "Load test ticket",
            "description": (
                "This ticket was generated "
                "for Day 68 load testing."
            ),
        },
    }


print(
    f"Starting load test: {TOTAL_EVENTS} events",
    flush=True
)

start_time = time.time()


for i in range(1, TOTAL_EVENTS + 1):

    event = create_ticket(i)

    ticket_id = event["payload"]["ticket_id"]

    producer.produce(
        TOPIC,
        key=ticket_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


producer.flush()


elapsed = time.time() - start_time


print(
    f"\nPublished {TOTAL_EVENTS} events.",
    flush=True
)

print(
    f"Producer time: {elapsed:.2f} seconds",
    flush=True
)

print(
    f"Producer rate: "
    f"{TOTAL_EVENTS / elapsed:.2f} events/sec",
    flush=True
)