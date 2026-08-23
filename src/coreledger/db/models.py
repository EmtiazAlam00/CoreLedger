import enum
import uuid
from datetime import date, datetime

from sqlalchemy import JSON, CheckConstraint, Enum, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from coreledger.db.base import Base


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    plaid_account_id: Mapped[str | None] = mapped_column(String, unique=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    ledger_entries: Mapped[list["LedgerEntry"]] = relationship(back_populates="account")


class PlaidTransaction(Base):
    """Raw transaction history ingested from Plaid Sandbox, kept purely as
    reference/seed data — deliberately a separate table from Transaction
    below, since these never become ledger postings or Kafka events (see
    scripts/ingest_plaid_seed_data.py). amount_minor follows Plaid's sign
    convention: positive means money left the account, negative means money
    came in — the opposite of this project's own debit/credit modeling."""

    __tablename__ = "plaid_transactions"

    plaid_transaction_id: Mapped[str] = mapped_column(String, primary_key=True)
    account_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False
    )
    amount_minor: Mapped[int] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    occurred_on: Mapped[date] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class TransactionStatus(str, enum.Enum):
    PENDING = "pending"
    POSTED = "posted"
    FAILED = "failed"


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    idempotency_key: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    status: Mapped[TransactionStatus] = mapped_column(
        Enum(TransactionStatus, name="transaction_status"),
        nullable=False,
        default=TransactionStatus.PENDING,
    )
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    entries: Mapped[list["LedgerEntry"]] = relationship(back_populates="transaction")


class EntryDirection(str, enum.Enum):
    DEBIT = "debit"
    CREDIT = "credit"


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    __table_args__ = (CheckConstraint("amount_minor > 0", name="ck_amount_minor_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=False
    )
    account_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False
    )
    direction: Mapped[EntryDirection] = mapped_column(
        Enum(EntryDirection, name="entry_direction"), nullable=False
    )
    amount_minor: Mapped[int] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    transaction: Mapped["Transaction"] = relationship(back_populates="entries")
    account: Mapped["Account"] = relationship(back_populates="ledger_entries")


class Client(Base):
    """A registered API client. client_id is the CN of its mTLS certificate
    (the two identity mechanisms are deliberately tied together — see
    api/jws_auth.py). public_jwk verifies that client's JWS-signed request
    bodies. redirect_uris is used by the OAuth PKCE consent flow below.

    Implements Authlib's ClientMixin protocol directly, hardcoded for a
    PKCE-only public client (no client_secret) supporting exactly one grant
    (authorization_code) and one response_type (code) — this server doesn't
    support anything else, so there's no stored per-client config to read."""

    __tablename__ = "clients"

    client_id: Mapped[str] = mapped_column(String, primary_key=True)
    public_jwk: Mapped[dict] = mapped_column(JSON, nullable=False)
    redirect_uris: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    def get_client_id(self) -> str:
        return self.client_id

    def get_default_redirect_uri(self) -> str | None:
        return self.redirect_uris[0] if self.redirect_uris else None

    def get_allowed_scope(self, scope: str) -> str:
        return scope or ""

    def check_redirect_uri(self, redirect_uri: str) -> bool:
        return redirect_uri in self.redirect_uris

    def check_client_secret(self, client_secret: str) -> bool:
        return False  # public client — never authenticates via a secret

    def check_endpoint_auth_method(self, method: str, endpoint: str) -> bool:
        if endpoint == "token":
            return method == "none"
        return True

    def check_response_type(self, response_type: str) -> bool:
        return response_type == "code"

    def check_grant_type(self, grant_type: str) -> bool:
        return grant_type == "authorization_code"


class AuthorizationCode(Base):
    """A short-lived code issued after user consent, exchanged once at the
    token endpoint. Implements Authlib's AuthorizationCodeMixin protocol."""

    __tablename__ = "authorization_codes"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    client_id: Mapped[str] = mapped_column(String, ForeignKey("clients.client_id"), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(String, nullable=False)
    scope: Mapped[str] = mapped_column(String, nullable=False, default="")
    code_challenge: Mapped[str | None] = mapped_column(String)
    code_challenge_method: Mapped[str | None] = mapped_column(String)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    def get_redirect_uri(self) -> str:
        return self.redirect_uri

    def get_scope(self) -> str:
        return self.scope


class Token(Base):
    """An issued access token. No refresh tokens — this server only grants
    short-lived tokens for the payment-initiation flow, re-consented each
    time via a fresh authorization code."""

    __tablename__ = "tokens"

    access_token: Mapped[str] = mapped_column(String, primary_key=True)
    client_id: Mapped[str] = mapped_column(String, ForeignKey("clients.client_id"), nullable=False)
    scope: Mapped[str] = mapped_column(String, nullable=False, default="")
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class UsedJti(Base):
    """Every JWS jti we've ever accepted. A row existing here means that
    signed request has already been processed — inserting into this table
    with ON CONFLICT DO NOTHING is the replay check itself."""

    __tablename__ = "used_jtis"

    jti: Mapped[str] = mapped_column(String, primary_key=True)
    used_at: Mapped[datetime] = mapped_column(server_default=func.now())
