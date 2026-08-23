import uuid
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select

from coreledger.db.ledger import EntryInput, post_transaction
from coreledger.db.models import EntryDirection, LedgerEntry, Transaction
from coreledger.db.session import SessionLocal


def attempt_post(idempotency_key: str, from_id, to_id, amount_minor: int) -> uuid.UUID | None:
    """Runs in its own thread with its own session — SQLAlchemy sessions
    aren't thread-safe, so each concurrent attempt needs an independent one,
    just like each API/consumer worker would have its own in production."""
    with SessionLocal() as session:
        entries = [
            EntryInput(account_id=from_id, direction=EntryDirection.DEBIT, amount_minor=amount_minor, currency="USD"),
            EntryInput(account_id=to_id, direction=EntryDirection.CREDIT, amount_minor=amount_minor, currency="USD"),
        ]
        result = post_transaction(session, idempotency_key=idempotency_key, entries=entries)
        session.commit()
        return result


def test_sequential_duplicate_is_a_noop(two_accounts):
    from_id, to_id = two_accounts
    key = str(uuid.uuid4())

    first = attempt_post(key, from_id, to_id, 1000)
    second = attempt_post(key, from_id, to_id, 1000)

    assert first is not None
    assert second is None  # duplicate correctly detected

    with SessionLocal() as session:
        txn = session.scalar(select(Transaction).where(Transaction.idempotency_key == key))
        assert txn is not None
        entries = session.scalars(
            select(LedgerEntry).where(LedgerEntry.transaction_id == first)
        ).all()
        assert len(entries) == 2


def test_concurrent_duplicates_post_exactly_once(two_accounts):
    """The scenario the resume bullet actually claims: N concurrent attempts
    with the same idempotency key must result in exactly one posted
    transaction, never zero and never more than one."""
    from_id, to_id = two_accounts
    key = str(uuid.uuid4())
    concurrency = 10

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(attempt_post, key, from_id, to_id, 1000) for _ in range(concurrency)
        ]
        results = [f.result() for f in futures]

    successes = [r for r in results if r is not None]
    assert len(successes) == 1, f"expected exactly 1 success, got {len(successes)}: {results}"

    with SessionLocal() as session:
        transactions = session.scalars(
            select(Transaction).where(Transaction.idempotency_key == key)
        ).all()
        assert len(transactions) == 1

        entries = session.scalars(
            select(LedgerEntry).where(LedgerEntry.transaction_id == transactions[0].id)
        ).all()
        assert len(entries) == 2
        debits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.DEBIT)
        credits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.CREDIT)
        assert debits == credits == 1000


def test_different_idempotency_keys_both_post(two_accounts):
    """Sanity check the dedup logic isn't accidentally blocking legitimate
    distinct transactions."""
    from_id, to_id = two_accounts

    first = attempt_post(str(uuid.uuid4()), from_id, to_id, 1000)
    second = attempt_post(str(uuid.uuid4()), from_id, to_id, 500)

    assert first is not None
    assert second is not None
    assert first != second


def test_unbalanced_entries_are_rejected(two_accounts):
    from_id, to_id = two_accounts

    with SessionLocal() as session:
        entries = [
            EntryInput(account_id=from_id, direction=EntryDirection.DEBIT, amount_minor=100, currency="USD"),
            EntryInput(account_id=to_id, direction=EntryDirection.CREDIT, amount_minor=999, currency="USD"),
        ]
        try:
            post_transaction(session, idempotency_key=str(uuid.uuid4()), entries=entries)
            assert False, "expected ValueError for unbalanced entries"
        except ValueError:
            session.rollback()
