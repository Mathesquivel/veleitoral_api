# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


def _build_database_url():
    """
    Retorna automaticamente a URL do banco (Railway / local).
    Aceita múltiplas variáveis para evitar erros:
    - DATABASE_URL (Railway padrão)
    - POSTGRES_URL
    - POSTGRESQL_URL
    - URL_PUBLICA_DO_BANCO_DE_DADOS (sua variável antiga)
    - Qualquer variável que contenha 'postgres'
    """

    # Prioridade explícita
    env_keys = [
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRESQL_URL",
        "URL_PUBLICA_DO_BANCO_DE_DADOS",
    ]

    # Procura diretamente por essas keys
    for key in env_keys:
        if key in os.environ and os.environ[key].strip():
            return os.environ[key].strip()

    # Procura por QUALQUER variavel que contenha 'postgres'
    for key, value in os.environ.items():
        if "postgres" in key.lower() and value.strip():
            return value.strip()

    # Se nada encontrado → erro claro
    raise RuntimeError(
        "❌ Nenhuma URL de banco encontrada.\n"
        "Certifique-se de definir DATABASE_URL no Railway."
    )


DATABASE_URL = _build_database_url()

# Railway muitas vezes entrega sem sslmode — adicionamos se faltar
if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

# Cria engine SQLAlchemy
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
    """Cria tabelas automaticamente (somente se não existirem)."""
    Base.metadata.create_all(bind=engine)
