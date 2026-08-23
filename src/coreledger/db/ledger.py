"""The only place ledger entries get written. Step 5 splits this into two
halves that run in different processes:

- create_pending_transaction(): called by the API. Creates the Transaction
  row (status=PENDING) and dedupes on idempotency_key — this is what stops a
  client retry from creating two logical payments.
- settle_transaction(): called by the ledger_writer Kafka consumer. Inserts
  the actual entries and flips the transaction to POSTED — this is what
  stops a Kafka redelivery of the same message from posting entries twice.

post_transaction() is kept for callers that don't need the split (the
step 2 sample script, direct tests) — it does both steps atomically."""

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from coreledger.db.models import EntryDirection, LedgerEntry, Transaction, TransactionStatus


@dataclass
class EntryInput:
    account_id: uuid.UUID
    direction: EntryDirection
    amount_minor: int
    currency: str


def _insert_entries(session: Session, transaction_id: uuid.UUID, entries: list[EntryInput]) -> None:
    debits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.DEBIT)
    credits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.CREDIT)
    if debits != credits:
        raise ValueError(f"unbalanced transaction: debits={debits} credits={credits}")

    session.execute(
        LedgerEntry.__table__.insert(),
        [
            {
                "transaction_id": transaction_id,
                "account_id": e.account_id,
                "direction": e.direction,
                "amount_minor": e.amount_minor,
                "currency": e.currency,
            }
            for e in entries
        ],
    )


def post_transaction(
    session: Session, *, idempotency_key: str, entries: list[EntryInput]
) -> uuid.UUID | None:
    """Post a balanced transaction in one call. Returns the new transaction's
    id, or None if idempotency_key was already used. Does not commit."""
    stmt = (
        pg_insert(Transaction)
        .values(idempotency_key=idempotency_key, status=TransactionStatus.POSTED)
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Transaction.id)
    )
    row = session.execute(stmt).first()
    if row is None:
        return None

    transaction_id = row[0]
    _insert_entries(session, transaction_id, entries)
    return transaction_id


def create_pending_transaction(session: Session, *, idempotency_key: str) -> tuple[uuid.UUID, bool]:
    """Called by the API. Creates a PENDING transaction row, deduped on
    idempotency_key exactly like post_transaction's insert. Returns
    (transaction_id, is_new) — is_new is False when this idempotency_key was
    already used, so the caller knows not to publish a second Kafka event
    for it. Does not commit."""
    stmt = (
        pg_insert(Transaction)
        .values(idempotency_key=idempotency_key, status=TransactionStatus.PENDING)
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Transaction.id)
    )
    row = session.execute(stmt).first()
    if row is not None:
        return row[0], True

    existing_id = session.scalar(
        select(Transaction.id).where(Transaction.idempotency_key == idempotency_key)
    )
    return existing_id, False


def settle_transaction(
    session: Session, *, transaction_id: uuid.UUID, entries: list[EntryInput]
) -> bool:
    """Called by the ledger_writer consumer. Locks the transaction row with
    SELECT ... FOR UPDATE so a redelivered Kafka message for the same
    transaction can't post entries twice: if the transaction isn't PENDING
    anymore (already settled by a previous delivery of this same message),
    this is a no-op and returns False. Does not commit."""
    transaction = session.execute(
        select(Transaction).where(Transaction.id == transaction_id).with_for_update()
    ).scalar_one()

    if transaction.status != TransactionStatus.PENDING:
        return False

    _insert_entries(session, transaction_id, entries)
    transaction.status = TransactionStatus.POSTED
    return True
