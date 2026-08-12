from confluent_kafka import Consumer, Producer
import json

from src.agents.ticket_triage.agent import TicketTriageAgent


# ============================================================
# Redpanda configuration
# ============================================================

BROKER = "localhost:9092"

INPUT_TOPIC = "ticket.created"
DLQ_TOPIC = "ticket.created.dlq"

GROUP_ID = "ticket-triage-group"

MAX_RETRIES = 3


# ============================================================
# Kafka / Redpanda consumer
# ============================================================

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",

    # We manually commit offsets.
    # This is important for retry + DLQ handling.
    "enable.auto.commit": False,
})


# ============================================================
# Kafka / Redpanda producer
# Used for publishing failed events to the DLQ.
# ============================================================

producer = Producer({
    "bootstrap.servers": BROKER,
})


# ============================================================
# Ticket Triage Agent
# Business logic remains outside the consumer.
# ============================================================

agent = TicketTriageAgent()


# ============================================================
# Subscribe to input topic
# ============================================================

consumer.subscribe([INPUT_TOPIC])


print(
    "Ticket Triage Agent started...",
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

print(
    f"Maximum retries: {MAX_RETRIES}",
    flush=True,
)

print(
    f"DLQ topic: {DLQ_TOPIC}",
    flush=True,
)


# ============================================================
# Process events
# ============================================================

try:

    while True:

        # ----------------------------------------------------
        # Wait for an event
        # ----------------------------------------------------

        message = consumer.poll(1.0)

        if message is None:
            continue


        # ----------------------------------------------------
        # Kafka / Redpanda error
        # ----------------------------------------------------

        if message.error():

            print(
                f"Consumer error: {message.error()}",
                flush=True,
            )

            continue


        # ----------------------------------------------------
        # Decode event
        # ----------------------------------------------------

        try:

            event = json.loads(
                message.value().decode("utf-8")
            )

        except json.JSONDecodeError as error:

            print(
                f"Invalid JSON event: {error}",
                flush=True,
            )

            # Invalid messages cannot be processed.
            # Commit so the consumer does not get stuck forever.
            consumer.commit(
                message=message,
                asynchronous=False,
            )

            continue


        # ----------------------------------------------------
        # Display event metadata
        # ----------------------------------------------------

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
        # Retry loop
        # ====================================================

        processing_successful = False

        for attempt in range(1, MAX_RETRIES + 1):

            print(
                f"\nProcessing ticket "
                f"(attempt {attempt}/{MAX_RETRIES})...",
                flush=True,
            )

            try:

                # ------------------------------------------------
                # Business logic
                # ------------------------------------------------

                triage_result = agent.process(event)


                # ------------------------------------------------
                # Processing succeeded
                # ------------------------------------------------

                print(
                    "\nTriage result:",
                    flush=True,
                )

                print(
                    json.dumps(
                        triage_result,
                        indent=2,
                    ),
                    flush=True,
                )

                print(
                    f"\nTicket "
                    f"{triage_result['ticket_id']} "
                    f"successfully triaged.",
                    flush=True,
                )


                # ------------------------------------------------
                # Commit offset ONLY after successful processing
                # ------------------------------------------------

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    "Kafka offset committed.",
                    flush=True,
                )

                processing_successful = True

                break


            except Exception as error:

                print(
                    f"\nProcessing failed "
                    f"on attempt {attempt}: {error}",
                    flush=True,
                )


                # ------------------------------------------------
                # If retries remain, try again.
                # ------------------------------------------------

                if attempt < MAX_RETRIES:

                    print(
                        f"Retrying ticket... "
                        f"({attempt + 1}/{MAX_RETRIES})",
                        flush=True,
                    )

                    continue


                # =================================================
                # All retries exhausted
                # =================================================

                print(
                    "\nMaximum retries reached.",
                    flush=True,
                )

                print(
                    "Sending event to Dead Letter Queue...",
                    flush=True,
                )


                # ------------------------------------------------
                # Add failure metadata
                # ------------------------------------------------

                dlq_event = {
                    "original_event": event,

                    "failure": {
                        "error": str(error),
                        "retry_count": MAX_RETRIES,
                        "source_topic": message.topic(),
                        "source_partition": message.partition(),
                        "source_offset": message.offset(),
                    },
                }


                # ------------------------------------------------
                # Publish failed event to DLQ
                # ------------------------------------------------

                try:

                    producer.produce(
                        DLQ_TOPIC,
                        value=json.dumps(
                            dlq_event
                        ).encode("utf-8"),
                    )

                    # Make sure the message is actually delivered
                    # before committing the original event.
                    producer.flush()


                    print(
                        "\nEvent successfully published "
                        "to DLQ.",
                        flush=True,
                    )

                    print(
                        json.dumps(
                            dlq_event,
                            indent=2,
                        ),
                        flush=True,
                    )


                    # ------------------------------------------------
                    # Commit original Kafka offset
                    # ------------------------------------------------

                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )

                    print(
                        "\nOriginal event offset committed "
                        "after DLQ publication.",
                        flush=True,
                    )

                    processing_successful = True


                except Exception as dlq_error:

                    print(
                        "\nCRITICAL: Failed to publish "
                        "event to DLQ.",
                        flush=True,
                    )

                    print(
                        f"DLQ error: {dlq_error}",
                        flush=True,
                    )

                    print(
                        "Offset will NOT be committed.",
                        flush=True,
                    )


        # --------------------------------------------------------
        # Event processing finished
        # --------------------------------------------------------

        if processing_successful:

            print(
                "\nEvent processing completed.",
                flush=True,
            )

        else:

            print(
                "\nEvent processing failed. "
                "Message remains uncommitted.",
                flush=True,
            )


except KeyboardInterrupt:

    print(
        "\nStopping Ticket Triage Agent...",
        flush=True,
    )


finally:

    consumer.close()

    print(
        "Consumer closed.",
        flush=True,
    )