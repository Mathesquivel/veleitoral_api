# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# URL FIXA DO POSTGRES NO RAILWAY
# Usa o MESMO banco tanto local quanto em produção.
DATABASE_URL = (
    "postgresql+psycopg2://"
    "postgres:BuRgLylYmpoIqfDgswmNPvKcFymkSffj"
    "@hopper.proxy.rlwy.net:32045/railway"
)

# Cria o engine de conexão
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

# Cria a sessão padrão
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
