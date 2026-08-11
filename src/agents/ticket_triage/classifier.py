from dataclasses import dataclass


@dataclass
class TriageResult:
    category: str
    priority: str
    reasoning: str


class TicketClassifier:

    def classify(self, title: str, description: str) -> TriageResult:
        text = f"{title} {description}".lower()

        if any(
            word in text
            for word in ["login", "password", "account", "authentication"]
        ):
            category = "account"

        elif any(
            word in text
            for word in ["payment", "billing", "invoice", "charge"]
        ):
            category = "billing"

        elif any(
            word in text
            for word in ["bug", "error", "crash", "exception", "broken"]
        ):
            category = "technical"

        else:
            category = "general"

        if any(
            word in text
            for word in ["urgent", "critical", "down", "cannot", "unable"]
        ):
            priority = "high"

        elif any(
            word in text
            for word in ["slow", "problem", "issue"]
        ):
            priority = "medium"

        else:
            priority = "low"

        reasoning = (
            f"Classified as {category} "
            f"with {priority} priority."
        )

        return TriageResult(
            category=category,
            priority=priority,
            reasoning=reasoning,
        )