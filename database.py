# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# SUA URL FIXA DO POSTGRES (Railway)
DATABASE_URL = (
    "postgresql+psycopg2://"
    "postgres:BuRgLylYmpoIqfDgswmNPvKcFymkSffj"
    "@hopper.proxy.rlwy.net:32045/railway"
)

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
