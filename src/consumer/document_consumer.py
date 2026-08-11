from confluent_kafka import Consumer
import json

from src.agents.document_processing.agent import (
    DocumentProcessingAgent,
)


BROKER = "localhost:9092"
TOPIC = "document.uploaded"
GROUP_ID = "document-processing-group"


consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})


agent = DocumentProcessingAgent()


consumer.subscribe([TOPIC])


print(
    "Document Processing Agent started...",
    flush=True,
)

print(
    f"Listening to topic: {TOPIC}",
    flush=True,
)

print(
    f"Consumer group: {GROUP_ID}",
    flush=True,
)


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():

            print(
                f"Consumer error: {message.error()}",
                flush=True,
            )

            continue

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
            "\nProcessing document...",
            flush=True,
        )

        result = agent.process(event)

        print(
            "\nDocument processing result:",
            flush=True,
        )

        print(
            json.dumps(
                result,
                indent=2,
            ),
            flush=True,
        )

        print(
            f"\nDocument "
            f"{result['document_id']} "
            f"successfully processed.",
            flush=True,
        )

        print(
            f"Chunks created: "
            f"{result['chunk_count']}",
            flush=True,
        )


except KeyboardInterrupt:

    print(
        "\nStopping Document Processing Agent...",
        flush=True,
    )


finally:

    consumer.close()

    print(
        "Document consumer closed.",
        flush=True,
    )