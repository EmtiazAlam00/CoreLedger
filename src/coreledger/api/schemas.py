import uuid

from pydantic import BaseModel, Field


class PaymentRequest(BaseModel):
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount_minor: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)


class PaymentResponse(BaseModel):
    transaction_id: uuid.UUID
    status: str
