from .classifier import TicketClassifier


class TicketTriageAgent:

    def __init__(self):
        self.classifier = TicketClassifier()

    def process(self, event: dict) -> dict:
        payload = event["payload"]

        ticket_id = payload["ticket_id"]
        title = payload["title"]
        description = payload["description"]

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