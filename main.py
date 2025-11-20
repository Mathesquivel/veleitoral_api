# main.py
# API FastAPI da V-Eleitoral, lendo dados do bd SQLite alimentado pelos CSV do TSE.

from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from pydantic import BaseModel
import sqlite3
from typing import Optional, List
from pathlib import Path
import os

from ingestor import ingest_all, DB_PATH

app = FastAPI(title="API TSE - VELEITORAL")

# pasta do volume do Railway para CSVs grandes (>= 100MB)
UPLOAD_DIR = "/app/dados_tse_volume"
os.makedirs(UPLOAD_DIR, exist_ok=True)

BASE_DIR = Path(__file__).parent

# =============================
# MODELOS DE RESPOSTA
# =============================

class VotoTotal(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: str
    sg_partido: str
    total_votos: int


class VotoZona(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: str
    sg_partido: str
    nr_zona: Optional[str]
    nr_secao: Optional[str]
    votos: int


class VotoMunicipio(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nm_candidato: str
    sg_partido: str
    votos: int


class VotoCargo(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    nm_candidato: str
    sg_partido: str
    votos: int


class CandidatoInfo(BaseModel):
    nm_candidato: str
    sg_partido: str
    total_votos: int
    ano: Optional[str]
    uf: Optional[str]


class PartidoInfo(BaseModel):
    sg_partido: str
    total_votos: int
    anos: Optional[str]
    ufs: Optional[str]


class RankingPartido(BaseModel):
    sg_partido: str
    total_votos: int


class Estatisticas(BaseModel):
    total_registros: int
    anos_disponiveis: List[str]
    ufs_disponiveis: List[str]


# =============================
# FUNÇÕES DE BANCO
# =============================

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def contar_registros() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM votos")
    row = cur.fetchone()
    conn.close()
    return row["c"] if row else 0


# =============================
# EVENTO DE STARTUP
# =============================

@app.on_event("startup")
def startup_event():
    print("\n🚀 Iniciando API e carregando dados do TSE...")
    total = ingest_all(clear_table=True)
    print(f"🚀 API pronta. Registros carregados: {total}")


# =============================
# ENDPOINTS BÁSICOS
# =============================

@app.get("/")
def root():
    total = contar_registros()
    return {
        "status": "ok",
        "mensagem": "API TSE VELEITORAL rodando.",
        "registros_votos": total,
        "banco": str(DB_PATH),
    }


@app.post("/reload")
def reload_dados():
    total = ingest_all(clear_table=True)
    return {
        "status": "ok",
        "mensagem": "Recarregado com sucesso.",
        "registros_votos": total,
    }


# =============================
# ENDPOINT DE UPLOAD (STREAMING)
# =============================

TAMANHO_MIN_VOLUME_MB = 100  # apenas arquivos >= 100 MB vão para o volume
CHUNK_SIZE = 1024 * 1024     # 1MB por chunk


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Recebe um arquivo CSV e, SE TIVER PELO MENOS 100 MB,
    salva no volume (/app/dados_tse_volume).

    Implementação em streaming:
    - Lê o arquivo em blocos (chunks) de 1MB
    - Vai gravando direto no disco
    - Soma o tamanho total à medida que grava
    - Se no final tiver < 100MB, apaga o arquivo e retorna erro 400
    """

    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie apenas arquivos .csv")

    destino = Path(UPLOAD_DIR) / file.filename
    tamanho_bytes = 0

    try:
        with open(destino, "wb") as f:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                tamanho_bytes += len(chunk)
                f.write(chunk)

    except Exception as e:
        if destino.exists():
            destino.unlink()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao salvar arquivo no volume: {e}",
        )

    tamanho_mb = tamanho_bytes / (1024 * 1024)

    if tamanho_mb < TAMANHO_MIN_VOLUME_MB:
        if destino.exists():
            destino.unlink()
        raise HTTPException(
            status_code=400,
            detail=(
                f"Arquivo tem apenas {tamanho_mb:.2f} MB. "
                f"A API só armazena no volume arquivos >= {TAMANHO_MIN_VOLUME_MB} MB. "
                "Para arquivos menores, mantenha-os no repositório Git em 'dados_tse/'."
            ),
        )

    return {
        "status": "ok",
        "mensagem": (
            f"Arquivo {file.filename} (≈ {tamanho_mb:.2f} MB) salvo em {destino}. "
            "Agora você pode chamar /reload para processar junto com os demais CSV."
        ),
        "arquivo": file.filename,
        "caminho": str(destino),
        "tamanho_mb": round(tamanho_mb, 2),
    }


# =============================
# ENDPOINTS DE CONSULTA
# =============================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=50, ge=1, le=1000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY ano, uf, nm_candidato, sg_partido
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoTotal(**dict(r)) for r in rows]


@app.get("/votos/zona", response_model=List[VotoZona])
def votos_por_zona(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    zona: Optional[str] = None,
    limite: int = Query(default=100, ge=1, le=5000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido,
               nr_zona, nr_secao, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)
    if zona:
        sql += " AND nr_zona = ?"
        params.append(zona)

    sql += """
        GROUP BY ano, uf, nm_candidato, sg_partido, nr_zona, nr_secao
        ORDER BY votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoZona(**dict(r)) for r in rows]


@app.get("/votos/municipio", response_model=List[VotoMunicipio])
def votos_por_municipio(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=100, ge=1, le=5000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_municipio, nm_municipio,
               nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY ano, uf, cd_municipio, nm_municipio, nm_candidato, sg_partido
        ORDER BY votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoMunicipio(**dict(r)) for r in rows]


@app.get("/votos/cargo", response_model=List[VotoCargo])
def votos_por_cargo(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    limite: int = Query(default=100, ge=1, le=5000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_cargo, ds_cargo,
               nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)
    if cd_cargo:
        sql += " AND cd_cargo = ?"
        params.append(cd_cargo)

    sql += """
        GROUP BY ano, uf, cd_cargo, ds_cargo, nm_candidato, sg_partido
        ORDER BY votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoCargo(**dict(r)) for r in rows]


@app.get("/candidatos", response_model=List[CandidatoInfo])
def listar_candidatos(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=100, ge=1, le=5000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT nm_candidato, sg_partido, ano, uf,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY nm_candidato, sg_partido, ano, uf
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [CandidatoInfo(**dict(r)) for r in rows]


@app.get("/partidos", response_model=List[PartidoInfo])
def listar_partidos(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT sg_partido,
               SUM(votos) AS total_votos,
               GROUP_CONCAT(DISTINCT ano) AS anos,
               GROUP_CONCAT(DISTINCT uf) AS ufs
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY sg_partido
        ORDER BY total_votos DESC
    """

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [PartidoInfo(**dict(r)) for r in rows]


@app.get("/ranking/partido", response_model=List[RankingPartido])
def ranking_partidos(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=50, ge=1, le=1000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params: list = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY sg_partido
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [RankingPartido(**dict(r)) for r in rows]


@app.get("/estatisticas", response_model=Estatisticas)
def estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM votos")
    total = cur.fetchone()["c"]

    cur.execute("SELECT DISTINCT ano FROM votos WHERE ano IS NOT NULL")
    anos = sorted({r["ano"] for r in cur.fetchall() if r["ano"]})

    cur.execute("SELECT DISTINCT uf FROM votos WHERE uf IS NOT NULL")
    ufs = sorted({r["uf"] for r in cur.fetchall() if r["uf"]})

    conn.close()

    return Estatisticas(
        total_registros=total,
        anos_disponiveis=anos,
        ufs_disponiveis=ufs,
    )
