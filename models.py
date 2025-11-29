# models.py
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime
from sqlalchemy.sql import func

from database import Base


# ============================
# TABELA DE VOTOS POR SEÇÃO
# ============================

class VotoSecao(Base):
    __tablename__ = "votos_secao"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    ano = Column(String)                     # ANO_ELEICAO
    nr_turno = Column(Integer, nullable=True)

    uf = Column(String(2))                   # SG_UF
    cd_municipio = Column(String)
    nm_municipio = Column(String)

    nr_zona = Column(String)
    nr_secao = Column(String)

    nr_local_votacao = Column(String)
    nm_local_votacao = Column(String)
    endereco_local = Column(Text)

    cd_cargo = Column(String)
    ds_cargo = Column(String)

    nr_votavel = Column(String)
    nm_votavel = Column(String)

    nr_partido = Column(String)
    sg_partido = Column(String)

    qt_votos = Column(BigInteger)


# ============================
# TABELA DE RESUMO MUNZONA
# ============================

class ResumoMunZona(Base):
    __tablename__ = "resumo_munzona"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    ano = Column(String)
    nr_turno = Column(Integer, nullable=True)

    uf = Column(String(2))
    cd_municipio = Column(String)
    nm_municipio = Column(String)

    nr_zona = Column(String)

    cd_cargo = Column(String)
    ds_cargo = Column(String)

    qt_aptos = Column(BigInteger)
    qt_total_secoes = Column(BigInteger)
    qt_comparecimento = Column(BigInteger)
    qt_abstencoes = Column(BigInteger)

    qt_votos = Column(BigInteger)
    qt_votos_nominais_validos = Column(BigInteger)
    qt_votos_brancos = Column(BigInteger)
    qt_total_votos_nulos = Column(BigInteger)
    qt_total_votos_leg_validos = Column(BigInteger)
    qt_votos_leg_validos = Column(BigInteger)


# ============================
# LOG DE IMPORTAÇÃO
# ============================

class ImportLog(Base):
    __tablename__ = "import_log"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    tipo_arquivo = Column(String)       # "secao" ou "munzona"
    nome_arquivo = Column(String)
    linhas_importadas = Column(BigInteger)
    criado_em = Column(DateTime(timezone=True), server_default=func.now())


# ============================================
# VOTACAO_CANDIDATO_MUNZONA (JÁ NO POSTGRES)
# ============================================

class VotoCandidatoMunZona(Base):
    """
    Mapeia a tabela 'votacao_candidato_munzona' que já está no Postgres.

    Layout típico TSE (versão municipal 2018/2020/2024):
    - ANO_ELEICAO          -> ano
    - SG_UF                -> uf
    - CD_MUNICIPIO         -> cd_municipio
    - NM_MUNICIPIO         -> nm_municipio
    - CD_CARGO             -> cd_cargo
    - DS_CARGO             -> ds_cargo
    - NR_VOTAVEL           -> nr_votavel
    - NM_VOTAVEL           -> nm_votavel
    - SG_PARTIDO           -> sg_partido
    - QT_VOTOS_NOMINAIS_VALIDOS -> qt_votos_nominais_validos
    - DS_SIT_TOT_TURNO     -> ds_sit_tot_turno
    """

    __tablename__ = "votacao_candidato_munzona"

    ano = Column(String, primary_key=True)
    uf = Column(String(2), primary_key=True)
    cd_municipio = Column(String, primary_key=True)
    nm_municipio = Column(String)

    cd_cargo = Column(String, primary_key=True)
    ds_cargo = Column(String)

    nr_votavel = Column(String, primary_key=True)
    nm_votavel = Column(String)

    sg_partido = Column(String)

    qt_votos_nominais_validos = Column(BigInteger)

    ds_sit_tot_turno = Column(String)
