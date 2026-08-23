import uuid

from pydantic import BaseModel

PAYMENT_EVENTS_TOPIC = "payment.events"


class PaymentInitiatedEvent(BaseModel):
    transaction_id: uuid.UUID
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount_minor: int
    currency: str
