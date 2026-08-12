from datetime import datetime, timezone
import json
import uuid

from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer

from src.api.models import CreateTicketRequest


BROKER = "localhost:9092"
TOPIC = "ticket.created"


app = FastAPI(
    title="Event-Driven Agent Platform",
    version="1.0.0",
)


producer = Producer({
    "bootstrap.servers": BROKER,
})


def delivery_report(err, message):
    if err is not None:
        print(
            f"Event delivery failed: {err}",
            flush=True,
        )
        return

    print(
        f"Event delivered to "
        f"{message.topic()} "
        f"[partition {message.partition()}] "
        f"at offset {message.offset()}",
        flush=True,
    )


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


@app.post("/tickets")
def create_ticket(request: CreateTicketRequest):

    ticket_id = f"T-{uuid.uuid4().hex[:8]}"

    event = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "ticket.created",
            "event_version": 1,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "correlation_id": str(uuid.uuid4()),
            "producer": "support-api",
        },
        "payload": {
            "ticket_id": ticket_id,
            "customer_id": request.customer_id,
            "title": request.title,
            "description": request.description,
        },
    }

    try:

        producer.produce(
            TOPIC,
            key=ticket_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )

        producer.flush()

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish event: {exc}",
        )

    print(
        "\nTicket event published:",
        flush=True,
    )

    print(
        json.dumps(
            event,
            indent=2,
        ),
        flush=True,
    )

    return {
        "status": "accepted",
        "ticket_id": ticket_id,
        "event_type": "ticket.created",
        "message": "Ticket accepted for processing.",
    }