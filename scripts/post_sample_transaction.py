"""Post one balanced debit/credit transaction by hand, to prove the schema and
the balance invariant work before any API or Kafka layer exists."""

import uuid

from coreledger.db.models import Account, EntryDirection, LedgerEntry, Transaction, TransactionStatus
from coreledger.db.session import SessionLocal


def get_or_create_account(session, name: str, currency: str = "USD") -> Account:
    account = session.query(Account).filter_by(name=name).one_or_none()
    if account is None:
        account = Account(name=name, currency=currency)
        session.add(account)
        session.flush()  # assigns account.id without committing
    return account


def post_balanced_transaction(
    session, *, from_account: Account, to_account: Account, amount_minor: int, currency: str
) -> Transaction:
    transaction = Transaction(
        idempotency_key=str(uuid.uuid4()),
        status=TransactionStatus.POSTED,
    )
    session.add(transaction)
    session.flush()  # assigns transaction.id

    entries = [
        LedgerEntry(
            transaction_id=transaction.id,
            account_id=from_account.id,
            direction=EntryDirection.DEBIT,
            amount_minor=amount_minor,
            currency=currency,
        ),
        LedgerEntry(
            transaction_id=transaction.id,
            account_id=to_account.id,
            direction=EntryDirection.CREDIT,
            amount_minor=amount_minor,
            currency=currency,
        ),
    ]
    session.add_all(entries)
    session.flush()

    debits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.DEBIT)
    credits = sum(e.amount_minor for e in entries if e.direction == EntryDirection.CREDIT)
    if debits != credits:
        raise ValueError(f"unbalanced transaction: debits={debits} credits={credits}")

    return transaction


def main() -> None:
    with SessionLocal() as session:
        checking = get_or_create_account(session, "Checking")
        savings = get_or_create_account(session, "Savings")

        transaction = post_balanced_transaction(
            session,
            from_account=checking,
            to_account=savings,
            amount_minor=5000,  # $50.00
            currency="USD",
        )

        session.commit()
        print(f"Posted transaction {transaction.id} (status={transaction.status.value})")
        for entry in transaction.entries:
            print(f"  {entry.direction.value:6} {entry.amount_minor:>6} {entry.currency} -> account {entry.account_id}")


if __name__ == "__main__":
    main()
