# CoreLedger

Event-driven payments settlement engine. A payment-initiation API publishes
to Kafka instead of writing the ledger directly; a separate consumer
(`ledger_writer`) performs the actual double-entry Postgres write. Payment
requests are secured to the FAPI 2.0 profile: mTLS client authentication,
JWS-signed request bodies, and PKCE-based consent.

## Running

There are two modes, and they should not run against the same Kafka topic
at the same time — see **Why not both at once** below.

### Dev / test loop

Bring up just the stateful services, run the app processes on the host:

```
docker compose up -d postgres kafka
uv run alembic upgrade head
uv run pytest
```

`api` and `ledger_writer` can also be run directly for manual testing:

```
uv run uvicorn coreledger.api.main:app --port 8000
uv run python -m coreledger.ledger_writer.consumer
```

### Full stack (mTLS + nginx)

```
docker compose up -d --build
```

This also builds and runs `api` and `ledger_writer` as containers, and
brings up `nginx` terminating mTLS on `https://localhost:8443`. First time
only: generate dev certs and register the demo client's JWS signing key —

```
./scripts/generate_certs.sh
uv run python scripts/register_test_client.py
```

### Why not both at once

`pytest`'s `clean_tables` fixture truncates the ledger tables before every
test, but Kafka's topic is durable and outlives that truncation. If the
containerized `ledger_writer` is running at the same time as the test
suite, it will eventually try to settle a transaction whose row was just
truncated out from under it — a genuine data-integrity violation, so it
fails loudly and exits (see `db/ledger.py::settle_transaction`) rather than
silently skip it. This is correct behavior for the consumer, but means the
two modes shouldn't share a live topic: `docker compose stop ledger_writer`
(or `docker compose down`) before running the test suite, and recreate the
`payment.events` topic if you hit this:

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic payment.events --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic payment.events --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
```

The same truncation also deletes any accounts seeded by
`scripts/ingest_plaid_seed_data.py` (and, via cascade, their
`plaid_transactions`) — test isolation intentionally wins that trade-off
rather than the fixture tip-toeing around demo data. Re-run the ingestion
script after running the test suite if you want seeded accounts back for a
demo; it's idempotent (reuses the cached `dev_keys/plaid_access_token.json`
rather than re-linking), so re-running just restores the same 14 accounts.

## Seeding realistic account data from Plaid Sandbox

```
cp .env.example .env   # fill in PLAID_CLIENT_ID / PLAID_SECRET from https://dashboard.plaid.com
uv run python scripts/ingest_plaid_seed_data.py
```

Pulls Plaid Sandbox's fixed set of fake "First Platypus Bank" test accounts
and their transaction history into `accounts`/`plaid_transactions` — seed
data only, read-only reference tables that the settlement pipeline never
touches (see the module docstring for why). If it prints `seeded 0
transactions` on the very first run, that's normal — Sandbox takes a few
seconds to generate transaction history after linking; just run it again.
