from confluent_kafka import Producer
import json
import uuid
from datetime import datetime, timezone

from src.common.logging_config import setup_logger


# ============================================================
# Redpanda configuration
# ============================================================

BROKER = "localhost:9092"
TOPIC = "ticket.created"


# ============================================================
# Structured logger
# ============================================================

logger = setup_logger(
    "support-api-producer"
)


# ============================================================
# Kafka / Redpanda Producer
# ============================================================

producer = Producer({
    "bootstrap.servers": BROKER,
})


# ============================================================
# Delivery callback
# ============================================================

def delivery_report(err, message):

    if err is not None:

        logger.error(
            f"Event delivery failed: {err}",
            extra={
                "service": "support-api-producer",
                "event": "event_delivery_failed",
            },
        )

        return

    logger.info(
        "Event delivered to Redpanda",
        extra={
            "service": "support-api-producer",
            "event": "event_delivered",
        },
    )


# ============================================================
# Create ticket event
# ============================================================

def create_ticket(
    ticket_id,
    title,
    description,
):

    event_id = str(uuid.uuid4())

    correlation_id = str(uuid.uuid4())

    return {
        "metadata": {
            "event_id": event_id,
            "event_type": "ticket.created",
            "event_version": 1,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "correlation_id": correlation_id,
            "producer": "support-api",
        },

        "payload": {
            "ticket_id": ticket_id,
            "customer_id": (
                f"C-{ticket_id.split('-')[1]}"
            ),
            "title": title,
            "description": description,
        },
    }


# ============================================================
# Create ticket events
# ============================================================

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
        "FAIL-001",
        "Simulated failure",
        "This ticket is intentionally designed to fail processing",
    ),

    create_ticket(
        "T-1006",
        "Email not received",
        "Customer did not receive verification email",
    ),
]


# ============================================================
# Start publishing
# ============================================================

logger.info(
    "Publishing ticket.created events",
    extra={
        "service": "support-api-producer",
        "event": "publishing_started",
    },
)


# ============================================================
# Publish events
# ============================================================

for event in tickets:

    metadata = event["metadata"]

    event_id = metadata["event_id"]

    correlation_id = metadata["correlation_id"]

    ticket_id = event["payload"]["ticket_id"]


    logger.info(
        f"Publishing ticket {ticket_id}",
        extra={
            "service": "support-api-producer",
            "event": "event_publish_started",
            "event_id": event_id,
            "correlation_id": correlation_id,
        },
    )


    producer.produce(
        TOPIC,
        key=ticket_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )


    # Trigger delivery callbacks
    producer.poll(0)


# ============================================================
# Wait for all messages to be delivered
# ============================================================

producer.flush()


# ============================================================
# Publishing completed
# ============================================================

logger.info(
    "All ticket events published",
    extra={
        "service": "support-api-producer",
        "event": "publishing_completed",
    },
)