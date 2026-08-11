import pytest

from src.events import (
    TicketCreatedEvent,
)


def test_ticket_created_event():

    event = TicketCreatedEvent(
        metadata={
            "event_type": "ticket.created",
            "event_version": 1,
            "producer": "test",
        },
        payload={
            "ticket_id": "T-1001",
            "customer_id": "C-42",
            "title": "Login problem",
            "description": "Cannot login",
        },
    )

    assert event.metadata.event_type == "ticket.created"
    assert event.payload.ticket_id == "T-1001"


def test_invalid_ticket_event():

    with pytest.raises(Exception):

        TicketCreatedEvent(
            metadata={
                "event_type": "ticket.created",
                "event_version": 1,
                "producer": "test",
            },
            payload={
                "customer_id": "C-42",
                "title": "Login problem",
            },
        )