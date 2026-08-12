from pydantic import BaseModel, Field


class CreateTicketRequest(BaseModel):
    customer_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)