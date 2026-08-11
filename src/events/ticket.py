from pydantic import BaseModel

from .base import Event, EventMetadata


class TicketCreatedPayload(BaseModel):
    ticket_id: str
    customer_id: str
    title: str
    description: str


class TicketCreatedEvent(Event):
    metadata: EventMetadata
    payload: TicketCreatedPayload