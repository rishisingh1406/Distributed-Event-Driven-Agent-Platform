from confluent_kafka import Consumer
import json

from src.agents.ticket_triage.agent import TicketTriageAgent


# Redpanda configuration

BROKER = "localhost:9092"
TOPIC = "ticket.created"
GROUP_ID = "ticket-triage-group"


# Create Kafka/Redpanda consumer

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})


# Create Ticket Triage Agent

agent = TicketTriageAgent()


# Subscribe to ticket.created topic

consumer.subscribe([TOPIC])


print("Ticket Triage Agent started...", flush=True)
print(f"Listening to topic: {TOPIC}", flush=True)
print(f"Consumer group: {GROUP_ID}", flush=True)


try:

    while True:

        # Wait for an event
        message = consumer.poll(1.0)

        # No event available yet
        if message is None:
            continue

        # Kafka/Redpanda error
        if message.error():
            print(
                f"Consumer error: {message.error()}",
                flush=True
            )
            continue

        # Decode event from Kafka/Redpanda
        event = json.loads(
            message.value().decode("utf-8")
        )

        print("\nEvent received:", flush=True)

        print(
            json.dumps(event, indent=2),
            flush=True
        )

        # Kafka/Redpanda metadata

        print(
            f"Topic: {message.topic()}",
            flush=True
        )

        print(
            f"Partition: {message.partition()}",
            flush=True
        )

        print(
            f"Offset: {message.offset()}",
            flush=True
        )

        # Process event with Ticket Triage Agent

        print(
            "\nProcessing ticket...",
            flush=True
        )

        triage_result = agent.process(event)

        # Display triage result

        print(
            "\nTriage result:",
            flush=True
        )

        print(
            json.dumps(
                triage_result,
                indent=2
            ),
            flush=True
        )

        print(
            f"\nTicket "
            f"{triage_result['ticket_id']} "
            f"successfully triaged.",
            flush=True
        )


except KeyboardInterrupt:

    print(
        "\nStopping Ticket Triage Agent...",
        flush=True
    )


finally:

    consumer.close()

    print(
        "Consumer closed.",
        flush=True
    )