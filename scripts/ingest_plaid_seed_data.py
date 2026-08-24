"""One-off ingestion: creates a Plaid Sandbox "custom user" with two
hand-authored accounts (checking + savings) and ~90 days of realistic
transaction history — payroll, rent, subscriptions, groceries, savings
transfers — then seeds CoreLedger's accounts/plaid_transactions tables from
it. Plaid is a read-only data source here — it never produces ledger
postings or Kafka events; the actual settlement pipeline (steps 3-6) only
ever runs off client-initiated payment requests through the API.

Custom user schema per https://plaid.com/docs/sandbox/user-custom/ — set
options.override_username="user_custom" and pass the JSON-stringified
config as options.override_password.

Safe to re-run: the access_token is cached in dev_keys/ after the first
run and reused, matching how a real app only links an account once and
reuses the resulting access_token afterward — re-linking (minting a new
Plaid Item) every run would hand back fresh account_ids each time and
defeat the ON CONFLICT dedup below, silently duplicating every account."""

import json
import os
import uuid
from datetime import date, timedelta
from pathlib import Path

import plaid
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.sandbox_public_token_create_request_options import (
    SandboxPublicTokenCreateRequestOptions,
)
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from sqlalchemy.dialects.postgresql import insert as pg_insert

from coreledger.db.models import Account, PlaidTransaction
from coreledger.db.session import SessionLocal

load_dotenv()
PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]

# institution_id is still required by the request even for a custom user,
# but which fake institution it names doesn't affect the account/transaction
# data below — that all comes from override_accounts instead.
SANDBOX_INSTITUTION_ID = "ins_109508"

DEV_KEYS_DIR = Path(__file__).resolve().parent.parent / "dev_keys"
ACCESS_TOKEN_CACHE = DEV_KEYS_DIR / "plaid_access_token.json"

HISTORY_DAYS = 90


def _dates_every(days_apart: int, start: date, end: date) -> list[date]:
    dates = []
    d = end
    while d >= start:
        dates.append(d)
        d -= timedelta(days=days_apart)
    return dates


def build_custom_user_config() -> dict:
    """Plaid's sign convention: positive amount = money left the account,
    negative = money came in — the opposite of this project's own
    debit/credit modeling (see PlaidTransaction's docstring)."""
    today = date.today()
    start = today - timedelta(days=HISTORY_DAYS)

    def txn(d: date, amount: float, description: str) -> dict:
        return {
            "date_transacted": d.isoformat(),
            "date_posted": d.isoformat(),
            "amount": amount,
            "currency": "USD",
            "description": description,
        }

    checking_txns = []
    for d in _dates_every(14, start, today):
        checking_txns.append(txn(d, -2500.00, "ACME CORP PAYROLL"))
    for d in _dates_every(30, start, today):
        checking_txns.append(txn(d, 1800.00, "RIVERSIDE APARTMENTS RENT"))
        checking_txns.append(txn(d, 15.99, "NETFLIX.COM"))
        checking_txns.append(txn(d, 10.99, "SPOTIFY"))
        checking_txns.append(txn(d, 500.00, "TRANSFER TO SAVINGS"))
    for d in _dates_every(7, start, today):
        checking_txns.append(txn(d, 85.00, "WHOLE FOODS MARKET"))
        checking_txns.append(txn(d, 12.50, "BLUE BOTTLE COFFEE"))

    savings_txns = []
    for d in _dates_every(30, start, today):
        savings_txns.append(txn(d, -500.00, "TRANSFER FROM CHECKING"))

    return {
        "seed": "coreledger-demo-seed-1",
        "override_accounts": [
            {
                "type": "depository",
                "subtype": "checking",
                "starting_balance": 4500.00,
                "currency": "USD",
                "name": "CoreLedger Checking",
                "transactions": checking_txns,
            },
            {
                "type": "depository",
                "subtype": "savings",
                "starting_balance": 12000.00,
                "currency": "USD",
                "name": "CoreLedger Savings",
                "transactions": savings_txns,
            },
        ],
    }


def get_client() -> plaid_api.PlaidApi:
    configuration = plaid.Configuration(
        host=plaid.Environment.Sandbox,
        api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
    )
    return plaid_api.PlaidApi(plaid.ApiClient(configuration))


def get_sandbox_access_token(client: plaid_api.PlaidApi) -> str:
    if ACCESS_TOKEN_CACHE.exists():
        return json.loads(ACCESS_TOKEN_CACHE.read_text())["access_token"]

    sandbox_resp = client.sandbox_public_token_create(
        SandboxPublicTokenCreateRequest(
            institution_id=SANDBOX_INSTITUTION_ID,
            initial_products=[Products("transactions")],
            options=SandboxPublicTokenCreateRequestOptions(
                override_username="user_custom",
                override_password=json.dumps(build_custom_user_config()),
            ),
        )
    )
    exchange_resp = client.item_public_token_exchange(
        ItemPublicTokenExchangeRequest(public_token=sandbox_resp.public_token)
    )
    access_token = exchange_resp.access_token

    DEV_KEYS_DIR.mkdir(exist_ok=True)
    ACCESS_TOKEN_CACHE.write_text(json.dumps({"access_token": access_token}))
    return access_token


# Plaid ignores override_accounts[].name (only official_name gets
# templated from subtype, e.g. "Plaid savings") — set our own display name
# by subtype instead of trusting Plaid's passthrough.
ACCOUNT_NAME_BY_SUBTYPE = {
    "checking": "CoreLedger Checking",
    "savings": "CoreLedger Savings",
}


def seed_accounts(session, client: plaid_api.PlaidApi, access_token: str) -> dict[str, uuid.UUID]:
    accounts_resp = client.accounts_get(AccountsGetRequest(access_token=access_token))

    account_id_by_plaid_id: dict[str, uuid.UUID] = {}
    for plaid_account in accounts_resp.accounts:
        account = (
            session.query(Account).filter_by(plaid_account_id=plaid_account.account_id).one_or_none()
        )
        name = ACCOUNT_NAME_BY_SUBTYPE.get(plaid_account.subtype.value, plaid_account.name)
        if account is None:
            account = Account(
                plaid_account_id=plaid_account.account_id,
                name=name,
                currency=plaid_account.balances.iso_currency_code or "USD",
            )
            session.add(account)
            session.flush()  # assigns account.id
        else:
            account.name = name  # keep in sync even on a rerun against an existing row
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
