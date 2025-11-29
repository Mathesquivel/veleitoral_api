# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


def get_database_url() -> str:
    """
    Lê a DATABASE_URL do ambiente ou usa o valor padrão do Railway.
    """
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:BuRgLylYmpoIqfDgswmNPvKcFymkSffj@hopper.proxy.rlwy.net:32045/railway",
    )

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)

    # Garante o prefixo do SQLAlchemy
    if not url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return url


engine = create_engine(
    get_database_url(),
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
