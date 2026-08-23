import os
from contextlib import asynccontextmanager

from confluent_kafka import Producer
from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy.orm import Session

from coreledger.api.auth import require_access_token, require_mtls_client
from coreledger.api.deps import get_db, get_producer
from coreledger.api.jws_auth import require_valid_jws
from coreledger.api.schemas import PaymentRequest, PaymentResponse
from coreledger.db.ledger import create_pending_transaction
from coreledger.db.models import Account, Token, Transaction
from coreledger.events import PAYMENT_EVENTS_TOPIC, PaymentInitiatedEvent
from coreledger.oauth.routes import router as oauth_router

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One producer for the app's lifetime — librdkafka batches/pools
    # connections internally, so a per-request producer would throw that away.
    app.state.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    yield
    app.state.producer.flush()


app = FastAPI(title="CoreLedger", lifespan=lifespan)
app.include_router(oauth_router)


@app.post("/payments", response_model=PaymentResponse, status_code=202)
def create_payment(
    payload: PaymentRequest,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    db: Session = Depends(get_db),
    producer: Producer = Depends(get_producer),
    _mtls: str = Depends(require_mtls_client),
    _jws: None = Depends(require_valid_jws),
    _token: Token = Depends(require_access_token),
) -> PaymentResponse:
    for account_id in (payload.from_account_id, payload.to_account_id):
        if db.get(Account, account_id) is None:
            raise HTTPException(status_code=404, detail=f"account {account_id} not found")

    transaction_id, is_new = create_pending_transaction(db, idempotency_key=idempotency_key)
    db.commit()

    if not is_new:
        # Already handled by an earlier call with this idempotency key — do
        # not re-publish, and report whatever its current status actually is
        # (the consumer may have already settled it by now).
        existing = db.get(Transaction, transaction_id)
        return PaymentResponse(transaction_id=transaction_id, status=existing.status.value)

    event = PaymentInitiatedEvent(
        transaction_id=transaction_id,
        from_account_id=payload.from_account_id,
        to_account_id=payload.to_account_id,
        amount_minor=payload.amount_minor,
        currency=payload.currency,
    )
    # Partitioned by from_account_id: all events debiting the same account
    # land on the same partition, so the consumer processes them in order.
    producer.produce(
        topic=PAYMENT_EVENTS_TOPIC,
        key=str(payload.from_account_id),
        value=event.model_dump_json(),
    )
    producer.flush(timeout=5)  # block until the broker acks it — the API's
    # "durably queued" promise to the client would be a lie otherwise.

    return PaymentResponse(transaction_id=transaction_id, status="pending")
