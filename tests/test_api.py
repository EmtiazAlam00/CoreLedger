import json
import uuid

from confluent_kafka import Consumer
from sqlalchemy import select

from coreledger.client_sdk import generate_signing_keypair, sign_request_body
from coreledger.db.models import LedgerEntry, Transaction, TransactionStatus
from coreledger.db.session import SessionLocal
from coreledger.events import PAYMENT_EVENTS_TOPIC, PaymentInitiatedEvent
from coreledger.ledger_writer.consumer import handle_event

# Requests through TestClient bypass nginx entirely, so tests that exercise
# the normal path have to simulate the header nginx would have added after a
# successful mTLS handshake. The DN's CN must match conftest.py's
# TEST_CLIENT_ID, since that's how the API looks up the registered JWK.
MTLS_OK = {"X-SSL-Client-Verify": "SUCCESS", "X-SSL-Client-DN": "CN=coreledger-pytest-client"}


def _payment_body(from_id, to_id, amount_minor: int, currency: str = "USD") -> bytes:
    # json.dumps with default separators — the exact bytes we sign must be
    # the exact bytes sent, so we build the body ourselves rather than
    # trusting client.post(json=...) to serialize identically.
    return json.dumps(
        {
            "from_account_id": str(from_id),
            "to_account_id": str(to_id),
            "amount_minor": amount_minor,
            "currency": currency,
        }
    ).encode()


def _post_payment(
    client,
    body: bytes,
    *,
    idempotency_key: str,
    jws_header: str | None,
    access_token: str | None,
):
    headers = {**MTLS_OK, "Idempotency-Key": idempotency_key, "Content-Type": "application/json"}
    if jws_header is not None:
        headers["X-JWS-Signature"] = jws_header
    if access_token is not None:
        headers["Authorization"] = f"Bearer {access_token}"
    return client.post("/payments", content=body, headers=headers)


def _drain_event_for(transaction_id: uuid.UUID, group_id: str, max_polls: int = 20) -> PaymentInitiatedEvent:
    """Test-only helper standing in for the real ledger_writer loop, but
    searching for a specific transaction rather than trusting the first
    message seen — a fresh group with auto.offset.reset=earliest would
    otherwise happily return a stale message left in the topic by an
    earlier test run."""
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([PAYMENT_EVENTS_TOPIC])
    try:
        for _ in range(max_polls):
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            assert msg.error() is None, msg.error()
            event = PaymentInitiatedEvent.model_validate_json(msg.value())
            consumer.commit(msg)
            if event.transaction_id == transaction_id:
                return event
        raise AssertionError(f"transaction {transaction_id} not found on {PAYMENT_EVENTS_TOPIC}")
    finally:
        consumer.close()


def test_create_payment_returns_202_pending(client, two_accounts, sign, access_token):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 5000)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=sign(body), access_token=access_token
    )

    assert response.status_code == 202
    resp_body = response.json()
    assert resp_body["status"] == "pending"
    assert uuid.UUID(resp_body["transaction_id"])


def test_duplicate_idempotency_key_returns_same_transaction(client, two_accounts, sign, access_token):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 2500)
    key = str(uuid.uuid4())

    # Each attempt needs its own valid signature (own jti) — a client
    # retrying a timed-out request would re-sign, not resend the same bytes.
    first = _post_payment(client, body, idempotency_key=key, jws_header=sign(body), access_token=access_token)
    second = _post_payment(client, body, idempotency_key=key, jws_header=sign(body), access_token=access_token)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["transaction_id"] == second.json()["transaction_id"]


def test_missing_idempotency_key_is_rejected(client, two_accounts, sign, access_token):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)

    headers = {
        **MTLS_OK,
        "X-JWS-Signature": sign(body),
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = client.post("/payments", content=body, headers=headers)

    assert response.status_code == 422


def test_missing_mtls_header_is_rejected(client, two_accounts, sign, access_token):
    """Simulates a request sent directly to :8000, bypassing nginx (and
    therefore mTLS) entirely."""
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)

    headers = {
        "Idempotency-Key": str(uuid.uuid4()),
        "X-JWS-Signature": sign(body),
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    response = client.post("/payments", content=body, headers=headers)

    assert response.status_code == 401


def test_missing_jws_signature_is_rejected(client, two_accounts, access_token):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=None, access_token=access_token
    )

    assert response.status_code == 422  # X-JWS-Signature is a required header


def test_tampered_payload_is_rejected(client, two_accounts, sign, access_token):
    """Signs one payload, sends a different one — the classic 'attacker
    modifies the amount in transit' scenario."""
    from_id, to_id = two_accounts
    signed_body = _payment_body(from_id, to_id, 1000)
    signature = sign(signed_body)

    tampered_body = _payment_body(from_id, to_id, 999999)
    response = _post_payment(
        client, tampered_body, idempotency_key=str(uuid.uuid4()), jws_header=signature, access_token=access_token
    )

    assert response.status_code == 401
    assert "invalid JWS signature" in response.json()["detail"]


def test_replayed_jws_is_rejected(client, two_accounts, sign, access_token):
    """Same signed request (same jti) sent twice — a captured request
    resubmitted verbatim, as opposed to a legitimate retry (which would
    carry a fresh signature/jti, covered by the duplicate-idempotency test)."""
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)
    signature = sign(body)

    first = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=signature, access_token=access_token
    )
    replay = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=signature, access_token=access_token
    )

    assert first.status_code == 202
    assert replay.status_code == 401
    assert "replay" in replay.json()["detail"].lower()


def test_unregistered_client_key_is_rejected(client, two_accounts, access_token):
    """A signature from a key that was never registered for this client_id
    — e.g. a forged or stale credential."""
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)
    rogue_key = generate_signing_keypair()
    signature = sign_request_body(body, rogue_key, "coreledger-pytest-client")

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=signature, access_token=access_token
    )

    assert response.status_code == 401


def test_missing_access_token_is_rejected(client, two_accounts, sign):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=sign(body), access_token=None
    )

    assert response.status_code == 401


def test_bogus_access_token_is_rejected(client, two_accounts, sign):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 1000)

    response = _post_payment(
        client,
        body,
        idempotency_key=str(uuid.uuid4()),
        jws_header=sign(body),
        access_token="not-a-real-token",
    )

    assert response.status_code == 401


def test_nonexistent_account_returns_404(client, two_accounts, sign, access_token):
    from_id, _ = two_accounts
    body = _payment_body(from_id, uuid.uuid4(), 1000)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=sign(body), access_token=access_token
    )

    assert response.status_code == 404


def test_non_positive_amount_is_rejected(client, two_accounts, sign, access_token):
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 0)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=sign(body), access_token=access_token
    )

    assert response.status_code == 422


def test_payment_flows_through_kafka_to_settled_ledger_entries(client, two_accounts, sign, access_token):
    """The full step-5/6 path: API verifies mTLS + JWS + PKCE access token,
    publishes to Kafka instead of writing entries itself, and the consumer
    (not the API) is what actually settles the transaction."""
    from_id, to_id = two_accounts
    body = _payment_body(from_id, to_id, 750)

    response = _post_payment(
        client, body, idempotency_key=str(uuid.uuid4()), jws_header=sign(body), access_token=access_token
    )
    assert response.status_code == 202
    assert response.json()["status"] == "pending"  # not settled yet — API never touches entries

    transaction_id = uuid.UUID(response.json()["transaction_id"])
    event = _drain_event_for(transaction_id, group_id=f"test-{uuid.uuid4()}")

    handle_event(event)  # what the real ledger_writer loop does per message

    with SessionLocal() as session:
        txn = session.get(Transaction, event.transaction_id)
        assert txn.status == TransactionStatus.POSTED
        entries = session.scalars(
            select(LedgerEntry).where(LedgerEntry.transaction_id == event.transaction_id)
        ).all()
        assert len(entries) == 2
