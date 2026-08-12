from .classifier import TicketClassifier


class TicketTriageAgent:

    def __init__(self):
        self.classifier = TicketClassifier()

    def process(self, event: dict) -> dict:
        payload = event["payload"]

        ticket_id = payload["ticket_id"]
        title = payload["title"]
        description = payload["description"]

        # Controlled failure for Day 65 DLQ testing.
        # This allows us to simulate an event that
        # consistently fails processing.
        if ticket_id == "FAIL-001":
            raise RuntimeError(
                "Simulated ticket processing failure"
            )

        # Normal ticket processing
        result = self.classifier.classify(
            title=title,
            description=description,
        )

        return {
            "ticket_id": ticket_id,
            "category": result.category,
            "priority": result.priority,
            "reasoning": result.reasoning,
            "status": "triaged",
        }