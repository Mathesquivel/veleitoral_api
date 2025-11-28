# database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# ==============================
# CONFIGURAÇÃO DA DATABASE_URL
# ==============================

# Fallback para DESENVOLVIMENTO LOCAL
# (a URL que você já usava antes, pode continuar igual)
LOCAL_DEV_DATABASE_URL = (
    "postgresql+psycopg2://"
    "postgres:BuRgLylYmpoIqfDgswmNPvKcFymkSffj"
    "@hopper.proxy.rlwy.net:32045/railway"
)

# Em produção (Railway), vamos tentar pegar das variáveis de ambiente.
# Em local, se essas variáveis não existirem, usamos o fallback acima.
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("URL_PÚBLICA_DO_BANCO_DE_DADOS")
    or LOCAL_DEV_DATABASE_URL
)

if not DATABASE_URL:
    raise RuntimeError(
        "❌ Nenhuma DATABASE_URL encontrada. "
        "Defina DATABASE_URL/URL_PÚBLICA_DO_BANCO_DE_DADOS ou ajuste LOCAL_DEV_DATABASE_URL."
    )

# ==============================
# ENGINE E SESSÃO
# ==============================

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
