from pydantic import BaseModel

from .base import Event, EventMetadata


class ReportScheduledPayload(BaseModel):
    report_id: str
    report_type: str
    scheduled_for: str


class ReportScheduledEvent(Event):
    metadata: EventMetadata
    payload: ReportScheduledPayload