import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://coreledger:coreledger@localhost:5432/coreledger",
)

engine = create_engine(DATABASE_URL)
SessionLocal: sessionmaker[Session] = sessionmaker(bind=engine)
