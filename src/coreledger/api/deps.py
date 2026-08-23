from collections.abc import Generator

from confluent_kafka import Producer
from fastapi import Request
from sqlalchemy.orm import Session

from coreledger.db.session import SessionLocal


def get_db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_producer(request: Request) -> Producer:
    return request.app.state.producer
