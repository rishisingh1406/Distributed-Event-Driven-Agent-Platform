from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventMetadata(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_version: int = 1
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    correlation_id: UUID = Field(default_factory=uuid4)
    producer: str


class Event(BaseModel):
    metadata: EventMetadata
    payload: dict[str, Any]