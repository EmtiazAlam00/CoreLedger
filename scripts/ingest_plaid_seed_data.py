"""One-off ingestion: pulls a fake linked account and its transaction
history from Plaid Sandbox and seeds CoreLedger's accounts/
plaid_transactions tables with realistic data. Plaid is a read-only data
source here — it never produces ledger postings or Kafka events; the
actual settlement pipeline (steps 3-6) only ever runs off client-initiated
payment requests through the API.

Safe to re-run: the access_token is cached in dev_keys/ after the first
run and reused, matching how a real app only links an account once and
reuses the resulting access_token afterward — re-linking (minting a new
Plaid Item) every run would hand back fresh account_ids each time and
defeat the ON CONFLICT dedup below, silently duplicating every account."""

import json
import os
import uuid
from pathlib import Path

import plaid
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from sqlalchemy.dialects.postgresql import insert as pg_insert

from coreledger.db.models import Account, PlaidTransaction
from coreledger.db.session import SessionLocal

load_dotenv()
PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]

# "First Platypus Bank" — one of Plaid Sandbox's fixed fake test institutions.
SANDBOX_INSTITUTION_ID = "ins_109508"

DEV_KEYS_DIR = Path(__file__).resolve().parent.parent / "dev_keys"
ACCESS_TOKEN_CACHE = DEV_KEYS_DIR / "plaid_access_token.json"


def get_client() -> plaid_api.PlaidApi:
    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
    )
    return plaid_api.PlaidApi(plaid.ApiClient(configuration))


def get_sandbox_access_token(client: plaid_api.PlaidApi) -> str:
    if ACCESS_TOKEN_CACHE.exists():
        return json.loads(ACCESS_TOKEN_CACHE.read_text())["access_token"]

    # Sandbox-only shortcut: normally a public_token comes from the Link UI
    # after a real user picks their bank and logs in. Sandbox lets you mint
    # one directly for a fake institution, skipping the UI entirely.
    sandbox_resp = client.sandbox_public_token_create(
        SandboxPublicTokenCreateRequest(
            institution_id=SANDBOX_INSTITUTION_ID,
            initial_products=[Products("transactions")],
        )
    )
    exchange_resp = client.item_public_token_exchange(
        ItemPublicTokenExchangeRequest(public_token=sandbox_resp.public_token)
    )
    access_token = exchange_resp.access_token

    DEV_KEYS_DIR.mkdir(exist_ok=True)
    ACCESS_TOKEN_CACHE.write_text(json.dumps({"access_token": access_token}))
    return access_token


def seed_accounts(session, client: plaid_api.PlaidApi, access_token: str) -> dict[str, uuid.UUID]:
    accounts_resp = client.accounts_get(AccountsGetRequest(access_token=access_token))

    account_id_by_plaid_id: dict[str, uuid.UUID] = {}
    for plaid_account in accounts_resp.accounts:
        account = (
            session.query(Account).filter_by(plaid_account_id=plaid_account.account_id).one_or_none()
        )
        if account is None:
            account = Account(
                plaid_account_id=plaid_account.account_id,
                name=plaid_account.name,
                currency=plaid_account.balances.iso_currency_code or "USD",
            )
            session.add(account)
            session.flush()  # assigns account.id
        account_id_by_plaid_id[plaid_account.account_id] = account.id
        print(f"  account: {account.name} -> {account.id}")

    session.commit()
    return account_id_by_plaid_id


def seed_transactions(
    session, client: plaid_api.PlaidApi, access_token: str, account_id_by_plaid_id: dict[str, uuid.UUID]
) -> int:
    cursor = None
    added = 0
    has_more = True

    while has_more:
        # cursor is a str-typed field that rejects an explicit None — omit
        # it entirely on the first call rather than pass cursor=None.
        request_kwargs = {"access_token": access_token}
        if cursor is not None:
            request_kwargs["cursor"] = cursor
        sync_resp = client.transactions_sync(TransactionsSyncRequest(**request_kwargs))
        for txn in sync_resp.added:
            account_id = account_id_by_plaid_id.get(txn.account_id)
            if account_id is None:
                continue
            stmt = (
                pg_insert(PlaidTransaction)
                .values(
                    plaid_transaction_id=txn.transaction_id,
                    account_id=account_id,
                    amount_minor=round(txn.amount * 100),
                    currency=txn.iso_currency_code or "USD",
                    description=txn.name,
                    occurred_on=txn.date,
                )
                .on_conflict_do_nothing(index_elements=["plaid_transaction_id"])
                .returning(PlaidTransaction.plaid_transaction_id)
            )
            if session.execute(stmt).first() is not None:
                added += 1  # only count rows actually inserted, not re-seen on a rerun
        cursor = sync_resp.next_cursor
        has_more = sync_resp.has_more

    session.commit()
    return added


def main() -> None:
    client = get_client()
    access_token = get_sandbox_access_token(client)

    with SessionLocal() as session:
        print("Seeding accounts...")
        account_id_by_plaid_id = seed_accounts(session, client, access_token)

        print("Seeding transactions...")
        added = seed_transactions(session, client, access_token, account_id_by_plaid_id)
        print(f"  seeded {added} transactions")


if __name__ == "__main__":
    main()
