from confluent_kafka import Producer
import json
import uuid
from datetime import datetime, timezone

BROKER = "localhost:9092"
TOPIC = "ticket.created"

producer = Producer({
    "bootstrap.servers": BROKER,
})


def delivery_report(err, message):
    if err is not None:
        print(f"Delivery failed: {err}", flush=True)
        return

    print(
        f"Event delivered to {message.topic()} "
        f"[partition {message.partition()}] "
        f"at offset {message.offset()}",
        flush=True,
    )


def create_ticket(ticket_id, title, description):
    return {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "ticket.created",
            "event_version": 1,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": str(uuid.uuid4()),
            "producer": "support-api",
        },
        "payload": {
            "ticket_id": ticket_id,
            "customer_id": f"C-{ticket_id.split('-')[1]}",
            "title": title,
            "description": description,
        },
    }


tickets = [
    create_ticket(
        "T-1001",
        "Unable to login",
        "Customer cannot access account",
    ),
    create_ticket(
        "T-1002",
        "Payment failed",
        "Customer payment is failing",
    ),
    create_ticket(
        "T-1003",
        "Password reset issue",
        "Customer cannot reset password",
    ),
    create_ticket(
        "T-1004",
        "Account locked",
        "Customer account is locked",
    ),
    create_ticket(
        "T-1005",
        "Billing problem",
        "Customer was charged incorrectly",
    ),
    create_ticket(
        "T-1006",
        "Email not received",
        "Customer did not receive verification email",
    ),
]


print("Publishing ticket.created events...", flush=True)

for event in tickets:
    ticket_id = event["payload"]["ticket_id"]

    print(
        f"\nPublishing {ticket_id}...",
        flush=True,
    )

    producer.produce(
        TOPIC,
        key=ticket_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)

producer.flush()

print("\nAll ticket events published.", flush=True)