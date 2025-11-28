# models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    BigInteger,
    Index,
    DateTime,
)
from sqlalchemy.sql import func

from database import Base


class VotoSecao(Base):
    """
    Tabela baseada no arquivo VOTACAO_SECAO_<ANO>_<UF>.
    Votos por seção/zona/local/candidato/partido.
    Perfeita para mapa e filtros detalhados.
    """
    __tablename__ = "votos_secao"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    ano = Column(String(4), index=True)            # ANO_ELEICAO
    nr_turno = Column(Integer, index=True)         # NR_TURNO

    uf = Column(String(2), index=True)             # SG_UF
    cd_municipio = Column(String(10), index=True)  # CD_MUNICIPIO
    nm_municipio = Column(String(150))             # NM_MUNICIPIO

    nr_zona = Column(String(10), index=True)       # NR_ZONA
    nr_secao = Column(String(10), index=True)      # NR_SECAO

    nr_local_votacao = Column(String(20), nullable=True)   # NR_LOCAL_VOTACAO
    nm_local_votacao = Column(String(200), nullable=True)  # NM_LOCAL_VOTACAO
    endereco_local = Column(String(500), nullable=True)    # DS_LOCAL_VOTACAO_ENDERECO (quando houver)

    cd_cargo = Column(String(10), index=True, nullable=True)   # CD_CARGO
    ds_cargo = Column(String(100), index=True)                 # DS_CARGO

    nr_votavel = Column(String(20), index=True)    # NR_VOTAVEL (número candidato/partido)
    nm_votavel = Column(String(200))               # NM_VOTAVEL

    nr_partido = Column(String(10), nullable=True) # NR_PARTIDO (quando existir)
    sg_partido = Column(String(20), index=True, nullable=True) # SG_PARTIDO

    qt_votos = Column(BigInteger)                  # QT_VOTOS

    __table_args__ = (
        Index("ix_vsec_ano_uf_mun_zona", "ano", "uf", "cd_municipio", "nr_zona"),
        Index("ix_vsec_ano_uf_mun_secao", "ano", "uf", "cd_municipio", "nr_secao"),
        Index("ix_vsec_candidato", "ano", "ds_cargo", "nr_votavel"),
        Index("ix_vsec_partido", "ano", "sg_partido"),
    )


class ResumoMunZona(Base):
    """
    Tabela baseada no arquivo DETALHE_VOTACAO_MUNZONA_<ANO>_<UF/BR>.
    Não tem votos por candidato; são totais por município+zona+cargo:
    aptos, comparecimento, abstenções, brancos, nulos etc.
    """
    __tablename__ = "resumo_munzona"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    ano = Column(String(4), index=True)             # ANO_ELEICAO
    nr_turno = Column(Integer, index=True)          # NR_TURNO

    uf = Column(String(2), index=True)              # SG_UF
    cd_municipio = Column(String(10), index=True)   # CD_MUNICIPIO
    nm_municipio = Column(String(150))              # NM_MUNICIPIO

    nr_zona = Column(String(10), index=True)        # NR_ZONA

    cd_cargo = Column(String(10), index=True)       # CD_CARGO
    ds_cargo = Column(String(100), index=True)      # DS_CARGO

    qt_aptos = Column(BigInteger, nullable=True)                    # QT_APTOS
    qt_total_secoes = Column(BigInteger, nullable=True)             # QT_TOTAL_SECOES
    qt_comparecimento = Column(BigInteger, nullable=True)           # QT_COMPARECIMENTO
    qt_abstencoes = Column(BigInteger, nullable=True)               # QT_ABSTENCOES

    qt_votos = Column(BigInteger, nullable=True)                    # QT_VOTOS
    qt_votos_nominais_validos = Column(BigInteger, nullable=True)   # QT_VOTOS_NOMINAIS_VALIDOS
    qt_votos_brancos = Column(BigInteger, nullable=True)            # QT_VOTOS_BRANCOS
    qt_total_votos_nulos = Column(BigInteger, nullable=True)        # QT_TOTAL_VOTOS_NULOS
    qt_total_votos_leg_validos = Column(BigInteger, nullable=True)  # QT_TOTAL_VOTOS_LEG_VALIDOS
    qt_votos_leg_validos = Column(BigInteger, nullable=True)        # QT_VOTOS_LEG_VALIDOS

    __table_args__ = (
        Index("ix_mz_ano_uf_mun_zona", "ano", "uf", "cd_municipio", "nr_zona"),
        Index("ix_mz_ano_cargo", "ano", "ds_cargo"),
    )


class ImportLog(Base):
    """
    Log simples das importações (secao/munzona).
    Só para controle.
    """
    __tablename__ = "import_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tipo_arquivo = Column(String(20))       # 'secao' ou 'munzona'
    nome_arquivo = Column(String(255))
    linhas_importadas = Column(BigInteger)
    criado_em = Column(DateTime, server_default=func.now())
