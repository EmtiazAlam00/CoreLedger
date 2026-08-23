import logging
import os

from confluent_kafka import Consumer

from coreledger.db.ledger import EntryInput, settle_transaction
from coreledger.db.models import EntryDirection
from coreledger.db.session import SessionLocal
from coreledger.events import PAYMENT_EVENTS_TOPIC, PaymentInitiatedEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ledger_writer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def handle_event(event: PaymentInitiatedEvent) -> None:
    entries = [
        EntryInput(
            account_id=event.from_account_id,
            direction=EntryDirection.DEBIT,
            amount_minor=event.amount_minor,
            currency=event.currency,
        ),
        EntryInput(
            account_id=event.to_account_id,
            direction=EntryDirection.CREDIT,
            amount_minor=event.amount_minor,
            currency=event.currency,
        ),
    ]
    with SessionLocal() as session:
        settled = settle_transaction(session, transaction_id=event.transaction_id, entries=entries)
        session.commit()

    if settled:
        logger.info("settled transaction %s", event.transaction_id)
    else:
        logger.info("transaction %s already settled, skipping redelivery", event.transaction_id)


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ledger-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after a successful write
        }
    )
    consumer.subscribe([PAYMENT_EVENTS_TOPIC])
    logger.info("ledger_writer started, subscribed to %s", PAYMENT_EVENTS_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("consumer error: %s", msg.error())
                continue

            event = PaymentInitiatedEvent.model_validate_json(msg.value())
            handle_event(event)
            consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    run()