# ingestor.py
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from database import engine, SessionLocal, Base
from models import VotoSecao, ResumoMunZona, ImportLog

# ==============================
# CONFIGURAÇÃO DE ARQUIVOS
# ==============================

# Volume do Railway
DEFAULT_DATA_DIR = Path("/app/dados_tse_volume")

# Para desenvolvimento local, se o volume não existir, usa ./dados_tse
if DEFAULT_DATA_DIR.exists():
    DATA_DIR = DEFAULT_DATA_DIR
else:
    DATA_DIR = Path(__file__).parent / "dados_tse"

SEP = ";"
ENCODING = "latin1"
CHUNKSIZE = 100_000


def init_db():
    """
    Cria as tabelas no banco (se não existirem).
    """
    Base.metadata.create_all(bind=engine)


def _normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().upper() for c in df.columns]
    return df


def _detectar_coluna_votos(cols) -> Optional[str]:
    candidatos = [
        "QT_VOTOS",
        "QT_VOTOS_VALIDOS",
        "QT_VOTOS_NOMINAIS",
        "QT_VOTOS_NOMINAIS_VALIDOS",
    ]
    for c in candidatos:
        if c in cols:
            return c
    return None


def _int_col(chunk: pd.DataFrame, colname: str) -> pd.Series:
    """
    Converte uma coluna do chunk para int64.
    Se a coluna não existir, retorna uma série de zeros
    com o mesmo tamanho do chunk.
    """
    if colname in chunk.columns:
        return (
            pd.to_numeric(chunk[colname].fillna(0), errors="coerce")
            .fillna(0)
            .astype("int64")
        )
    else:
        return pd.Series([0] * len(chunk), dtype="int64")


# ==============================
# INGESTÃO VOTACAO_SECAO
# ==============================

def ingest_votacao_secao(path: Path) -> int:
    """
    Ingestão do arquivo VOTACAO_SECAO_<ANO>_<UF>.
    Gera registros na tabela votos_secao.
    """
    init_db()
    linhas_total = 0

    for chunk in pd.read_csv(
        path,
        sep=SEP,
        encoding=ENCODING,
        dtype=str,
        chunksize=CHUNKSIZE,
    ):
        chunk = _normalizar_colunas(chunk)
        voto_col = _detectar_coluna_votos(chunk.columns)
        if not voto_col:
            raise ValueError(f"Não encontrei coluna de votos em {path.name}")

        df = pd.DataFrame()

        df["ano"] = chunk.get("ANO_ELEICAO")
        df["nr_turno"] = _int_col(chunk, "NR_TURNO")

        df["uf"] = chunk.get("SG_UF")
        df["cd_municipio"] = chunk.get("CD_MUNICIPIO")
        df["nm_municipio"] = chunk.get("NM_MUNICIPIO")

        df["nr_zona"] = chunk.get("NR_ZONA")
        df["nr_secao"] = chunk.get("NR_SECAO")

        df["nr_local_votacao"] = chunk.get("NR_LOCAL_VOTACAO")
        df["nm_local_votacao"] = chunk.get("NM_LOCAL_VOTACAO")

        # Endereço do local de votação (varia de nome)
        if "DS_LOCAL_VOTACAO_ENDERECO" in chunk.columns:
            endereco = chunk["DS_LOCAL_VOTACAO_ENDERECO"]
        elif "DS_ENDERECO_LOCAL_VOTACAO" in chunk.columns:
            endereco = chunk["DS_ENDERECO_LOCAL_VOTACAO"]
        else:
            endereco = pd.Series([None] * len(chunk))

        df["endereco_local"] = endereco

        df["cd_cargo"] = chunk.get("CD_CARGO")
        df["ds_cargo"] = chunk.get("DS_CARGO")

        df["nr_votavel"] = chunk.get("NR_VOTAVEL")
        df["nm_votavel"] = chunk.get("NM_VOTAVEL")

        df["nr_partido"] = chunk.get("NR_PARTIDO")
        df["sg_partido"] = chunk.get("SG_PARTIDO")

        df["qt_votos"] = _int_col(chunk, voto_col)

        # Remove registros sem votável (linhas de controle, etc.)
        df = df[df["nr_votavel"].notna()]

        if df.empty:
            continue

        df.to_sql(
            VotoSecao.__tablename__,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        linhas_total += len(df)

    # Log de importação
    with SessionLocal() as db:
        log = ImportLog(
            tipo_arquivo="secao",
            nome_arquivo=path.name,
            linhas_importadas=linhas_total,
        )
        db.add(log)
        db.commit()

    return linhas_total


# ==============================
# INGESTÃO DETALHE_VOTACAO_MUNZONA
# ==============================

def ingest_detalhe_munzona(path: Path) -> int:
    """
    Ingestão do arquivo DETALHE_VOTACAO_MUNZONA_<ANO>_<UF/BR>.
    Gera registros na tabela resumo_munzona.
    """
    init_db()
    linhas_total = 0

    for chunk in pd.read_csv(
        path,
        sep=SEP,
        encoding=ENCODING,
        dtype=str,
        chunksize=CHUNKSIZE,
    ):
        chunk = _normalizar_colunas(chunk)

        df = pd.DataFrame()

        df["ano"] = chunk.get("ANO_ELEICAO")
        df["nr_turno"] = _int_col(chunk, "NR_TURNO")

        df["uf"] = chunk.get("SG_UF")
        df["cd_municipio"] = chunk.get("CD_MUNICIPIO")
        df["nm_municipio"] = chunk.get("NM_MUNICIPIO")

        df["nr_zona"] = chunk.get("NR_ZONA")

        df["cd_cargo"] = chunk.get("CD_CARGO")
        df["ds_cargo"] = chunk.get("DS_CARGO")

        df["qt_aptos"] = _int_col(chunk, "QT_APTOS")
        df["qt_total_secoes"] = _int_col(chunk, "QT_TOTAL_SECOES")
        df["qt_comparecimento"] = _int_col(chunk, "QT_COMPARECIMENTO")
        df["qt_abstencoes"] = _int_col(chunk, "QT_ABSTENCOES")

        df["qt_votos"] = _int_col(chunk, "QT_VOTOS")
        df["qt_votos_nominais_validos"] = _int_col(
            chunk, "QT_VOTOS_NOMINAIS_VALIDOS"
        )
        df["qt_votos_brancos"] = _int_col(chunk, "QT_VOTOS_BRANCOS")
        df["qt_total_votos_nulos"] = _int_col(chunk, "QT_TOTAL_VOTOS_NULOS")
        df["qt_total_votos_leg_validos"] = _int_col(
            chunk, "QT_TOTAL_VOTOS_LEG_VALIDOS"
        )
        df["qt_votos_leg_validos"] = _int_col(chunk, "QT_VOTOS_LEG_VALIDOS")

        if df.empty:
            continue

        df.to_sql(
            ResumoMunZona.__tablename__,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        linhas_total += len(df)

    with SessionLocal() as db:
        log = ImportLog(
            tipo_arquivo="munzona",
            nome_arquivo=path.name,
            linhas_importadas=linhas_total,
        )
        db.add(log)
        db.commit()

    return linhas_total


# ==============================
# RELOAD / LIMPEZA
# ==============================

def ingest_all() -> int:
    """
    Reprocessa TODOS os CSVs em DATA_DIR.

    Heurística:
    - Se nome contém 'SECAO'  -> ingest_votacao_secao
    - Se nome contém 'MUNZONA' -> ingest_detalhe_munzona
    """
    init_db()
    total = 0

    for path in DATA_DIR.glob("*.csv"):
        name_upper = path.name.upper()
        if "SECAO" in name_upper:
            total += ingest_votacao_secao(path)
        elif "MUNZONA" in name_upper:
            total += ingest_detalhe_munzona(path)

    return total


def clear_all_data():
    """
    Limpa as tabelas de votos, sem dropar estrutura.
    Útil para /clear-volume.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"TRUNCATE TABLE {VotoSecao.__tablename__} "
                "RESTART IDENTITY CASCADE"
            )
        )
        conn.execute(
            text(
                f"TRUNCATE TABLE {ResumoMunZona.__tablename__} "
                "RESTART IDENTITY CASCADE"
            )
        )
        conn.execute(
            text(
                f"TRUNCATE TABLE {ImportLog.__tablename__} "
                "RESTART IDENTITY CASCADE"
            )
        )
