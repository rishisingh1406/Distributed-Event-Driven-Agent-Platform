from pydantic import BaseModel

from .base import Event, EventMetadata


class DocumentUploadedPayload(BaseModel):
    document_id: str
    filename: str
    storage_path: str
    uploaded_by: str


class DocumentUploadedEvent(Event):
    metadata: EventMetadata
    payload: DocumentUploadedPayload