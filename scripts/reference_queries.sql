-- ============================================================================
-- CoreLedger — Reference Queries
-- ============================================================================
-- Curated queries for exploring the data and, more usefully, for proving
-- the correctness properties this project is actually built around —
-- the kind of thing worth running live in an interview rather than just
-- asserting in prose. Run against Postgres from `docker compose up -d
-- postgres` (see README.md for connection details), e.g.:
--
--   docker compose exec postgres psql -U coreledger -d coreledger -f scripts/reference_queries.sql
--
-- or paste individual sections into pgAdmin's Query Tool.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. THE DOUBLE-ENTRY INVARIANT
-- ----------------------------------------------------------------------------
-- The core correctness guarantee of the ledger: every transaction's entries
-- must sum to zero (debits == credits). This is checked in application code
-- at write time (db/ledger.py::_insert_entries), never as a DB constraint —
-- this query is the independent, after-the-fact verification that the
-- invariant actually held for every transaction ever posted. This is
-- exactly the query a real auditor or reconciliation job would run.

-- Should ALWAYS return zero rows. Any row here means the invariant broke.
SELECT
    transaction_id,
    sum(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE 0 END) AS total_debits,
    sum(CASE WHEN direction = 'CREDIT' THEN amount_minor ELSE 0 END) AS total_credits
FROM ledger_entries
GROUP BY transaction_id
HAVING sum(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) != 0;


-- ----------------------------------------------------------------------------
-- 2. IDEMPOTENCY VERIFICATION
-- ----------------------------------------------------------------------------
-- idempotency_key carries a UNIQUE constraint, so true duplicates are
-- structurally impossible — this is the query that proves it rather than
-- just asserting the schema says so. The real proof of the *concurrent*
-- case (10 simultaneous identical requests, exactly one wins) is a race
-- condition and can't be demonstrated by a single SQL query — see
-- tests/test_idempotency.py::test_concurrent_duplicates_post_exactly_once.

-- Should ALWAYS return zero rows.
SELECT idempotency_key, count(*)
FROM transactions
GROUP BY idempotency_key
HAVING count(*) > 1;


-- ----------------------------------------------------------------------------
-- 3. DERIVED ACCOUNT BALANCES
-- ----------------------------------------------------------------------------
-- There is no stored `balance` column anywhere in this schema, on purpose —
-- in a double-entry system, the balance IS the entries; storing a
-- denormalized copy would just be a second source of truth that could drift
-- from the actual ledger. This computes it fresh, every time.

SELECT
    a.name,
    a.currency,
    COALESCE(sum(CASE WHEN le.direction = 'CREDIT' THEN le.amount_minor ELSE -le.amount_minor END), 0) AS balance_minor
FROM accounts a
LEFT JOIN ledger_entries le ON le.account_id = a.id
GROUP BY a.id, a.name, a.currency
ORDER BY a.name;


-- ----------------------------------------------------------------------------
-- 4. SETTLEMENT LIFECYCLE
-- ----------------------------------------------------------------------------
-- Transactions move PENDING -> POSTED asynchronously (the API only ever
-- creates PENDING rows; ledger_writer is what flips them to POSTED after
-- consuming the Kafka event). This shows where everything currently sits
-- in that lifecycle — a transaction stuck at PENDING for a while is exactly
-- what you'd alert on in production.

SELECT status, count(*), min(created_at) AS oldest, max(created_at) AS newest
FROM transactions
GROUP BY status;


-- ----------------------------------------------------------------------------
-- 5. RECENT ACTIVITY, HUMAN-READABLE
-- ----------------------------------------------------------------------------
-- Every posted payment with both its entries and readable account names,
-- most recent first.

SELECT
    t.id AS transaction_id,
    t.status,
    t.created_at,
    le.direction,
    le.amount_minor,
    a.name AS account_name
FROM transactions t
JOIN ledger_entries le ON le.transaction_id = t.id
JOIN accounts a ON a.id = le.account_id
ORDER BY t.created_at DESC, le.direction
LIMIT 50;


-- ----------------------------------------------------------------------------
-- 6. SECURITY TABLES
-- ----------------------------------------------------------------------------
-- Registered clients (public_jwk truncated — it's a full RSA public key,
-- not meant to be read at a glance).
SELECT client_id, redirect_uris, left(public_jwk::text, 60) AS jwk_preview
FROM clients;

-- Currently valid (unexpired) access tokens issued via the PKCE flow.
SELECT client_id, scope, expires_at
FROM tokens
WHERE expires_at > now()
ORDER BY expires_at DESC;

-- Every access token ever issued vs. how many are still valid — a large gap
-- here is normal (tokens are short-lived, 1 hour), just shows the churn.
SELECT count(*) AS total_issued, count(*) FILTER (WHERE expires_at > now()) AS still_valid
FROM tokens;

-- Size of the JWS replay guard — one row per signed request ever accepted.
-- This table is what test_replayed_jws_is_rejected is actually exercising.
SELECT count(*) AS jtis_seen FROM used_jtis;


-- ----------------------------------------------------------------------------
-- 7. PLAID SEED DATA
-- ----------------------------------------------------------------------------
-- Linked accounts and their ingested transaction history — reference data
-- only; nothing here ever becomes a ledger posting or a Kafka event (see
-- scripts/ingest_plaid_seed_data.py's module docstring for why).

SELECT a.name, pt.description, pt.amount_minor, pt.currency, pt.occurred_on
FROM plaid_transactions pt
JOIN accounts a ON a.id = pt.account_id
ORDER BY pt.occurred_on DESC
LIMIT 20;

-- Net cash flow per linked account. Plaid's sign convention: positive =
-- money left the account, negative = money came in — the opposite of this
-- project's own debit/credit modeling (see PlaidTransaction's docstring).
SELECT a.name, sum(pt.amount_minor) AS net_minor, count(*) AS txn_count
FROM plaid_transactions pt
JOIN accounts a ON a.id = pt.account_id
GROUP BY a.name
ORDER BY net_minor DESC;
