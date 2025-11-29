# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


def _build_database_url():
    """
    Retorna automaticamente a URL do banco (Railway ou local).
    Aceita múltiplas variáveis para garantir compatibilidade:
    - DATABASE_URL (padrão)
    - DATABASE_PUBLIC_URL (Railway criou esta)
    - POSTGRES_URL
    - POSTGRESQL_URL
    - Qualquer variável que contenha 'postgres'
    """

    preferred_keys = [
        "DATABASE_URL",
        "DATABASE_PUBLIC_URL",
        "POSTGRES_URL",
        "POSTGRESQL_URL",
        "URL_PUBLICA_DO_BANCO_DE_DADOS",
    ]

    # Busca nas chaves conhecidas
    for key in preferred_keys:
        if key in os.environ and os.environ[key].strip():
            return os.environ[key].strip()

    # Busca qualquer variável contendo 'postgres'
    for key, value in os.environ.items():
        if "postgres" in key.lower() and value.strip():
            return value.strip()

    # Se nada encontrado → erro claro
    raise RuntimeError(
        "❌ Nenhuma URL de banco encontrada.\n"
        "Crie a variável DATABASE_URL ou DATABASE_PUBLIC_URL no Railway."
    )


DATABASE_URL = _build_database_url()

# Adiciona sslmode caso não exista (necessário no Railway)
if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

# Engine SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

# Session
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Cria tabelas se não existirem."""
    Base.metadata.create_all(bind=engine)
