from src.agents.ticket_triage.agent import TicketTriageAgent


def test_login_ticket():

    agent = TicketTriageAgent()

    event = {
        "payload": {
            "ticket_id": "T-1001",
            "customer_id": "C-42",
            "title": "Unable to login",
            "description": "Customer cannot access account",
        }
    }

    result = agent.process(event)

    assert result["ticket_id"] == "T-1001"
    assert result["category"] == "account"
    assert result["priority"] == "high"
    assert result["status"] == "triaged"


def test_billing_ticket():

    agent = TicketTriageAgent()

    event = {
        "payload": {
            "ticket_id": "T-1002",
            "customer_id": "C-43",
            "title": "Payment issue",
            "description": "I was charged twice",
        }
    }

    result = agent.process(event)

    assert result["category"] == "billing"
    assert result["priority"] == "medium"
    assert result["status"] == "triaged"