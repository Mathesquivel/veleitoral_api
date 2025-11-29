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
    Usada para mapa, votos por seção/zona/local.
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
    endereco_local = Column(String(500), nullable=True)    # DS_LOCAL_VOTACAO_ENDERECO

    cd_cargo = Column(String(10), index=True, nullable=True)   # CD_CARGO
    ds_cargo = Column(String(100), index=True)                 # DS_CARGO

    nr_votavel = Column(String(20), index=True)    # NR_VOTAVEL (nº candidato/partido)
    nm_votavel = Column(String(200))               # NM_VOTAVEL

    nr_partido = Column(String(10), nullable=True)          # NR_PARTIDO
    sg_partido = Column(String(20), index=True, nullable=True)  # SG_PARTIDO

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
    Totais por município+zona+cargo (aptos, comparecimento, abstenções, etc.).
    """
    __tablename__ = "resumo_munzona"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    ano = Column(String(4), index=True)
    nr_turno = Column(Integer, index=True)

    uf = Column(String(2), index=True)
    cd_municipio = Column(String(10), index=True)
    nm_municipio = Column(String(150))

    nr_zona = Column(String(10), index=True)

    cd_cargo = Column(String(10), index=True)
    ds_cargo = Column(String(100), index=True)

    qt_aptos = Column(BigInteger, nullable=True)
    qt_total_secoes = Column(BigInteger, nullable=True)
    qt_comparecimento = Column(BigInteger, nullable=True)
    qt_abstencoes = Column(BigInteger, nullable=True)

    qt_votos = Column(BigInteger, nullable=True)
    qt_votos_nominais_validos = Column(BigInteger, nullable=True)
    qt_votos_brancos = Column(BigInteger, nullable=True)
    qt_total_votos_nulos = Column(BigInteger, nullable=True)
    qt_total_votos_leg_validos = Column(BigInteger, nullable=True)
    qt_votos_leg_validos = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("ix_mz_ano_uf_mun_zona", "ano", "uf", "cd_municipio", "nr_zona"),
        Index("ix_mz_ano_cargo", "ano", "ds_cargo"),
    )


class CandidatoMeta(Base):
    """
    Tabela candidatos_meta (já existente no Postgres).
    Usada para Ranking e Pesquisa de candidatos/partidos.

    Aqui usamos os nomes FÍSICOS do banco.
    """
    __tablename__ = "candidatos_meta"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    ano = Column(String(4), index=True)
    uf = Column(String(2), index=True)

    cd_municipio = Column(String(10), index=True, nullable=True)
    nm_municipio = Column(String(150), nullable=True)

    # NOME FÍSICO IGUAL AO BANCO
    cd_cargo = Column(String(10), index=True, nullable=True)

    nr_candidato = Column(String(20), index=True, nullable=True)
    nm_candidato = Column(String(200), nullable=True)

    sg_partido = Column(String(20), index=True, nullable=True)

    total_votos = Column(BigInteger, nullable=True)


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
