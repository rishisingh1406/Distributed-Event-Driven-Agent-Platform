from confluent_kafka import Consumer, Producer
import json

from src.agents.ticket_triage.agent import TicketTriageAgent
from src.common.logging_config import setup_logger
from src.database.ticket_store import TicketStore


# ============================================================
# Redpanda configuration
# ============================================================

BROKER = "localhost:9092"

INPUT_TOPIC = "ticket.created"
DLQ_TOPIC = "ticket.created.dlq"

GROUP_ID = "ticket-triage-group"

MAX_RETRIES = 3


# ============================================================
# Structured logger
# ============================================================

logger = setup_logger(
    "ticket-triage-consumer"
)


# ============================================================
# Kafka / Redpanda consumer
# ============================================================

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",

    # Manual offset commits.
    # Important for retry + DLQ handling.
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
# ============================================================

agent = TicketTriageAgent()


# ============================================================
# PostgreSQL Ticket Store
# ============================================================

ticket_store = TicketStore()

ticket_store.initialize()


# ============================================================
# Subscribe to input topic
# ============================================================

consumer.subscribe([INPUT_TOPIC])


logger.info(
    "Ticket Triage Agent started",
    extra={
        "service": "ticket-triage-consumer",
        "event": "consumer_started",
    },
)


logger.info(
    "Listening to topic",
    extra={
        "service": "ticket-triage-consumer",
        "event": "consumer_subscribed",
    },
)


logger.info(
    f"Consumer configuration: group={GROUP_ID}, "
    f"max_retries={MAX_RETRIES}, "
    f"dlq_topic={DLQ_TOPIC}",
    extra={
        "service": "ticket-triage-consumer",
        "event": "consumer_configuration",
    },
)


logger.info(
    "PostgreSQL ticket store initialized",
    extra={
        "service": "ticket-triage-consumer",
        "event": "database_initialized",
    },
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

            logger.error(
                "Consumer error",
                extra={
                    "service": "ticket-triage-consumer",
                    "event": "consumer_error",
                },
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

            logger.error(
                f"Invalid JSON event: {error}",
                extra={
                    "service": "ticket-triage-consumer",
                    "event": "invalid_json",
                },
            )

            # Invalid messages cannot be processed.
            # Commit so consumer does not get stuck forever.

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            continue


        # ====================================================
        # Extract event metadata
        # ====================================================

        metadata = event.get(
            "metadata",
            {},
        )

        event_id = metadata.get(
            "event_id"
        )

        correlation_id = metadata.get(
            "correlation_id"
        )

        event_type = metadata.get(
            "event_type"
        )

        payload = event.get(
            "payload",
            {},
        )

        ticket_id = payload.get(
            "ticket_id"
        )


        # ====================================================
        # Event received
        # ====================================================

        logger.info(
            "Event received",
            extra={
                "service": "ticket-triage-consumer",
                "event": "event_received",
                "event_id": event_id,
                "correlation_id": correlation_id,
            },
        )


        # ====================================================
        # Event location
        # ====================================================

        logger.info(
            "Event location",
            extra={
                "service": "ticket-triage-consumer",
                "event": "event_location",
                "event_id": event_id,
                "correlation_id": correlation_id,
            },
        )


        # ====================================================
        # Retry loop
        # ====================================================

        processing_successful = False

        for attempt in range(
            1,
            MAX_RETRIES + 1
        ):

            logger.info(
                f"Processing ticket "
                f"(attempt {attempt}/{MAX_RETRIES})",
                extra={
                    "service": "ticket-triage-consumer",
                    "event": "processing_started",
                    "event_id": event_id,
                    "correlation_id": correlation_id,
                },
            )

            try:

                # ------------------------------------------------
                # Business logic
                # ------------------------------------------------

                triage_result = agent.process(
                    event
                )


                # ------------------------------------------------
                # Processing succeeded
                # ------------------------------------------------

                logger.info(
                    "Ticket triage completed",
                    extra={
                        "service": "ticket-triage-consumer",
                        "event": "processing_completed",
                        "event_id": event_id,
                        "correlation_id": correlation_id,
                    },
                )


                # ------------------------------------------------
                # Save processing result to PostgreSQL
                #
                # IMPORTANT:
                # Database persistence happens BEFORE the
                # Redpanda offset is committed.
                # ------------------------------------------------

                ticket_store.save_ticket_result(
                    event_id=event_id,
                    correlation_id=correlation_id,
                    ticket_id=ticket_id,
                    status="completed",
                    triage_result=triage_result,
                )


                logger.info(
                    "Ticket result persisted to PostgreSQL",
                    extra={
                        "service": "ticket-triage-consumer",
                        "event": "database_write_completed",
                        "event_id": event_id,
                        "correlation_id": correlation_id,
                    },
                )


                # ------------------------------------------------
                # Commit offset ONLY after:
                #
                # 1. Agent processing succeeded
                # 2. PostgreSQL persistence succeeded
                # ------------------------------------------------

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )


                logger.info(
                    "Kafka offset committed",
                    extra={
                        "service": "ticket-triage-consumer",
                        "event": "offset_committed",
                        "event_id": event_id,
                        "correlation_id": correlation_id,
                    },
                )


                processing_successful = True

                break


            except Exception as error:

                logger.error(
                    f"Processing failed on attempt "
                    f"{attempt}: {error}",
                    extra={
                        "service": "ticket-triage-consumer",
                        "event": "processing_failed",
                        "event_id": event_id,
                        "correlation_id": correlation_id,
                    },
                )


                # ------------------------------------------------
                # Retry
                # ------------------------------------------------

                if attempt < MAX_RETRIES:

                    logger.warning(
                        f"Retrying ticket "
                        f"(next attempt: {attempt + 1})",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "processing_retry",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )

                    continue


                # =================================================
                # All retries exhausted
                # =================================================

                logger.error(
                    "Maximum retries reached",
                    extra={
                        "service": "ticket-triage-consumer",
                        "event": "max_retries_reached",
                        "event_id": event_id,
                        "correlation_id": correlation_id,
                    },
                )


                # ------------------------------------------------
                # Create DLQ event
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

                    logger.warning(
                        "Publishing event to DLQ",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "dlq_publish_started",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )


                    producer.produce(
                        DLQ_TOPIC,
                        value=json.dumps(
                            dlq_event
                        ).encode("utf-8"),
                    )


                    # Make sure the DLQ message is delivered
                    # before committing the original event.

                    producer.flush()


                    logger.error(
                        "Event successfully published to DLQ",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "dlq_publish_completed",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )


                    # ------------------------------------------------
                    # Commit original Kafka offset
                    # ------------------------------------------------

                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )


                    logger.info(
                        "Original event offset committed after DLQ",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "dlq_offset_committed",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )


                    processing_successful = True


                except Exception as dlq_error:

                    logger.critical(
                        f"Failed to publish event to DLQ: "
                        f"{dlq_error}",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "dlq_publish_failed",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )


                    logger.error(
                        "Offset will NOT be committed",
                        extra={
                            "service": "ticket-triage-consumer",
                            "event": "offset_not_committed",
                            "event_id": event_id,
                            "correlation_id": correlation_id,
                        },
                    )


        # --------------------------------------------------------
        # Event processing finished
        # --------------------------------------------------------

        if processing_successful:

            logger.info(
                "Event processing completed",
                extra={
                    "service": "ticket-triage-consumer",
                    "event": "event_completed",
                    "event_id": event_id,
                    "correlation_id": correlation_id,
                },
            )

        else:

            logger.error(
                "Event processing failed and remains uncommitted",
                extra={
                    "service": "ticket-triage-consumer",
                    "event": "event_failed",
                    "event_id": event_id,
                    "correlation_id": correlation_id,
                },
            )


except KeyboardInterrupt:

    logger.info(
        "Stopping Ticket Triage Agent",
        extra={
            "service": "ticket-triage-consumer",
            "event": "consumer_stopping",
        },
    )


finally:

    consumer.close()

    logger.info(
        "Consumer closed",
        extra={
            "service": "ticket-triage-consumer",
            "event": "consumer_closed",
        },
    )