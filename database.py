# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


def _build_database_url() -> str:
    """
    Usa a URL do Railway (URL_PÚBLICA_DO_BANCO_DE_DADOS) ou DATABASE_URL local.
    Converte 'postgresql://' em 'postgresql+psycopg2://'.
    """
    url = os.getenv("URL_PÚBLICA_DO_BANCO_DE_DADOS") or os.getenv("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "❌ Nenhuma URL de banco encontrada. "
            "Defina URL_PÚBLICA_DO_BANCO_DE_DADOS (Railway) ou DATABASE_URL (local)."
        )

    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return url


DATABASE_URL = _build_database_url()

engine = create_engine(
    DATABASE_URL,
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
