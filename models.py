# models.py
from datetime import datetime

from sqlalchemy import Column, Integer, String, BigInteger, DateTime
from database import Base


class VotoSecao(Base):
    __tablename__ = "votos_secao"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    ano = Column(String(4), index=True, nullable=True)
    nr_turno = Column(Integer, nullable=True)

    uf = Column(String(2), index=True, nullable=True)
    cd_municipio = Column(String(10), index=True, nullable=True)
    nm_municipio = Column(String(255), nullable=True)

    nr_zona = Column(String(10), index=True, nullable=True)
    nr_secao = Column(String(10), index=True, nullable=True)

    nr_local_votacao = Column(String(20), nullable=True)
    nm_local_votacao = Column(String(255), nullable=True)
    endereco_local = Column(String(500), nullable=True)

    cd_cargo = Column(String(10), index=True, nullable=True)
    ds_cargo = Column(String(100), index=True, nullable=True)

    nr_votavel = Column(String(20), index=True, nullable=True)
    nm_votavel = Column(String(255), nullable=True)

    nr_partido = Column(String(10), nullable=True)
    sg_partido = Column(String(20), index=True, nullable=True)

    qt_votos = Column(BigInteger, nullable=False, default=0)


class ResumoMunZona(Base):
    __tablename__ = "resumo_munzona"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    ano = Column(String(4), index=True, nullable=True)
    nr_turno = Column(Integer, nullable=True)

    uf = Column(String(2), index=True, nullable=True)
    cd_municipio = Column(String(10), index=True, nullable=True)
    nm_municipio = Column(String(255), nullable=True)

    nr_zona = Column(String(10), index=True, nullable=True)

    cd_cargo = Column(String(10), index=True, nullable=True)
    ds_cargo = Column(String(100), index=True, nullable=True)

    qt_aptos = Column(BigInteger, nullable=True, default=0)
    qt_total_secoes = Column(BigInteger, nullable=True, default=0)
    qt_comparecimento = Column(BigInteger, nullable=True, default=0)
    qt_abstencoes = Column(BigInteger, nullable=True, default=0)

    qt_votos = Column(BigInteger, nullable=True, default=0)
    qt_votos_nominais_validos = Column(BigInteger, nullable=True, default=0)
    qt_votos_brancos = Column(BigInteger, nullable=True, default=0)
    qt_total_votos_nulos = Column(BigInteger, nullable=True, default=0)
    qt_total_votos_leg_validos = Column(BigInteger, nullable=True, default=0)
    qt_votos_leg_validos = Column(BigInteger, nullable=True, default=0)


class ImportLog(Base):
    __tablename__ = "import_log"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    tipo_arquivo = Column(String(50), nullable=False)
    nome_arquivo = Column(String(255), nullable=False)
    linhas_importadas = Column(BigInteger, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
