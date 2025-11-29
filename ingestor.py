# ingestor.py
from pathlib import Path
import pandas as pd

from sqlalchemy.orm import Session

from database import engine, SessionLocal
from models import VotoSecao, ResumoMunZona, ImportLog

# Diretório de dados (volume Railway)
DATA_DIR = "/app/dados_tse_volume"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

SEP = ";"
ENCODING = "latin1"


def init_db():
    """Cria tabelas se ainda não existirem (exceto candidatos_meta, que já existe)."""
    from database import Base
    Base.metadata.create_all(bind=engine)


def _insert_log(session: Session, tipo: str, nome_arquivo: str, linhas: int):
    log = ImportLog(
        tipo_arquivo=tipo,
        nome_arquivo=nome_arquivo,
        linhas_importadas=linhas,
    )
    session.add(log)
    session.commit()


def ingest_votacao_secao(csv_path: Path) -> int:
    """
    Ingere arquivo VOTACAO_SECAO_* para a tabela votos_secao.
    Usa pandas + to_sql em chunks para lidar com arquivos grandes.
    """
    csv_path = Path(csv_path)
    total_linhas = 0

    chunks = pd.read_csv(
        csv_path,
        sep=SEP,
        encoding=ENCODING,
        dtype=str,
        chunksize=200_000,
        low_memory=False,
    )

    for chunk in chunks:
        # Normaliza nomes de colunas (upper)
        chunk.columns = [c.strip().upper() for c in chunk.columns]

        # Mapeia colunas do CSV -> colunas da tabela
        df = pd.DataFrame({
            "ano": chunk.get("ANO_ELEICAO"),
            "nr_turno": chunk.get("NR_TURNO"),
            "uf": chunk.get("SG_UF"),
            "cd_municipio": chunk.get("CD_MUNICIPIO"),
            "nm_municipio": chunk.get("NM_MUNICIPIO"),
            "nr_zona": chunk.get("NR_ZONA"),
            "nr_secao": chunk.get("NR_SECAO"),
            "nr_local_votacao": chunk.get("NR_LOCAL_VOTACAO"),
            "nm_local_votacao": chunk.get("NM_LOCAL_VOTACAO"),
            "endereco_local": chunk.get("DS_LOCAL_VOTACAO_ENDERECO"),
            "cd_cargo": chunk.get("CD_CARGO"),
            "ds_cargo": chunk.get("DS_CARGO"),
            "nr_votavel": chunk.get("NR_VOTAVEL"),
            "nm_votavel": chunk.get("NM_VOTAVEL"),
            "nr_partido": chunk.get("NR_PARTIDO"),
            "sg_partido": chunk.get("SG_PARTIDO"),
            "qt_votos": chunk.get("QT_VOTOS"),
        })

        # Converte qt_votos pra numérico (NaN -> 0)
        df["qt_votos"] = pd.to_numeric(df["qt_votos"], errors="coerce").fillna(0).astype("int64")

        df.to_sql(
            VotoSecao.__tablename__,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )

        total_linhas += len(df)

    # Log
    with SessionLocal() as session:
        _insert_log(session, "secao", csv_path.name, total_linhas)

    return total_linhas


def ingest_detalhe_munzona(csv_path: Path) -> int:
    """
    Ingere arquivo DETALHE_VOTACAO_MUNZONA_* para a tabela resumo_munzona.
    """
    csv_path = Path(csv_path)
    total_linhas = 0

    chunks = pd.read_csv(
        csv_path,
        sep=SEP,
        encoding=ENCODING,
        dtype=str,
        chunksize=200_000,
        low_memory=False,
    )

    for chunk in chunks:
        chunk.columns = [c.strip().upper() for c in chunk.columns]

        def num(colname: str):
            return pd.to_numeric(chunk.get(colname), errors="coerce").fillna(0).astype("int64")

        df = pd.DataFrame({
            "ano": chunk.get("ANO_ELEICAO"),
            "nr_turno": chunk.get("NR_TURNO"),
            "uf": chunk.get("SG_UF"),
            "cd_municipio": chunk.get("CD_MUNICIPIO"),
            "nm_municipio": chunk.get("NM_MUNICIPIO"),
            "nr_zona": chunk.get("NR_ZONA"),
            "cd_cargo": chunk.get("CD_CARGO"),
            "ds_cargo": chunk.get("DS_CARGO"),
            "qt_aptos": num("QT_APTOS") if "QT_APTOS" in chunk.columns else 0,
            "qt_total_secoes": num("QT_SECOES") if "QT_SECOES" in chunk.columns else 0,
            "qt_comparecimento": num("QT_COMPARECIMENTO") if "QT_COMPARECIMENTO" in chunk.columns else 0,
            "qt_abstencoes": num("QT_ABSTENCOES") if "QT_ABSTENCOES" in chunk.columns else 0,
            "qt_votos": num("QT_VOTOS") if "QT_VOTOS" in chunk.columns else 0,
            "qt_votos_nominais_validos": num("QT_VOTOS_NOMINAIS_VALIDOS") if "QT_VOTOS_NOMINAIS_VALIDOS" in chunk.columns else 0,
            "qt_votos_brancos": num("QT_VOTOS_BRANCOS") if "QT_VOTOS_BRANCOS" in chunk.columns else 0,
            "qt_total_votos_nulos": num("QT_VOTOS_NULOS") if "QT_VOTOS_NULOS" in chunk.columns else 0,
            "qt_total_votos_leg_validos": num("QT_VOTOS_LEGENDA") if "QT_VOTOS_LEGENDA" in chunk.columns else 0,
            "qt_votos_leg_validos": num("QT_VOTOS_ANULADOS_APTOS") if "QT_VOTOS_ANULADOS_APTOS" in chunk.columns else 0,
        })

        df.to_sql(
            ResumoMunZona.__tablename__,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )

        total_linhas += len(df)

    with SessionLocal() as session:
        _insert_log(session, "munzona", csv_path.name, total_linhas)

    return total_linhas


def ingest_all() -> int:
    """
    Reingere TODOS os CSVs do diretório DATA_DIR.
    Arquivos com 'SECAO' no nome -> votos_secao
    Arquivos com 'MUNZONA' no nome -> resumo_munzona
    """
    total = 0
    root = Path(DATA_DIR)
    for csv_path in root.rglob("*.csv"):
        name_upper = csv_path.name.upper()
        if "SECAO" in name_upper:
            total += ingest_votacao_secao(csv_path)
        elif "MUNZONA" in name_upper:
            total += ingest_detalhe_munzona(csv_path)

    return total


def clear_all_data():
    """
    Limpa as tabelas de votos_secao e resumo_munzona.
    NÃO mexe em candidatos_meta.
    """
    with SessionLocal() as session:
        session.query(VotoSecao).delete()
        session.query(ResumoMunZona).delete()
        session.commit()
