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
# TABELA votacao_candidato_munzona (Postgres)
# ============================================

class VotoCandidatoMunZona(Base):
    """
    Mapeia a tabela 'votacao_candidato_munzona' do Postgres,
    usando EXATAMENTE os nomes de coluna que você listou:

    - ANO_ELEICAO
    - SG_UF
    - NM_MUNIC
    - NR_CANDI
    - NM_URNA
    - NM_CAND
    - SG_PARTIDO
    - QT_VOTOS
    - DS_SIT_TOT_TURNO
    """

    __tablename__ = "votacao_candidato_munzona"

    # chaves lógicas (podem não estar como PK no banco, mas funcionam no ORM)
    ano = Column("ANO_ELEICAO", String, primary_key=True)
    uf = Column("SG_UF", String(2), primary_key=True)
    nm_municipio = Column("NM_MUNIC", String, primary_key=True)

    nr_candidato = Column("NR_CANDI", String, primary_key=True)

    nm_urna_candidato = Column("NM_URNA", String)
    nm_candidato = Column("NM_CAND", String)

    sg_partido = Column("SG_PARTIDO", String)

    qt_votos = Column("QT_VOTOS", BigInteger)

    ds_sit_tot_turno = Column("DS_SIT_TOT_TURNO", String)

    # campos adicionais comuns na base (se existirem no seu banco):
    cd_municipio = Column("CD_MUNIC", String, nullable=True)
    cd_cargo = Column("CD_CARGO", String, nullable=True)
    ds_cargo = Column("DS_CARGO", String, nullable=True)
