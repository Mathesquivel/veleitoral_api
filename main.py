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

# pasta onde os arquivos enviados serão salvos (volume do Railway)
UPLOAD_DIR = "/app/dados_tse"
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
    # Ao subir a API, faz ingestão automática dos CSV na pasta /app/dados_tse
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
    """
    Reprocessa todos os CSV da pasta /app/dados_tse e recria a tabela votos.
    """
    total = ingest_all(clear_table=True)
    return {
        "status": "ok",
        "mensagem": "Recarregado com sucesso.",
        "registros_votos": total,
    }


# =============================
# ENDPOINT DE UPLOAD
# =============================

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Recebe um arquivo CSV e salva no volume (/app/dados_tse).
    Use via Swagger: /docs -> POST /upload.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie apenas arquivos .csv")

    destino = Path(UPLOAD_DIR) / file.filename
    conteudo = await file.read()

    with open(destino, "wb") as f:
        f.write(conteudo)

    return {
        "status": "ok",
        "mensagem": f"Arquivo {file.filename} salvo em {destino}. Agora você pode chamar /reload para processar.",
        "arquivo": file.filename,
        "caminho": str(destino),
    }


# =============================
# ENDPOINTS DE CONSULTA
# =============================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    limite: int = Query(default=50, ge=1, le=1000),
):
    """
    Ranking de votos totais por candidato.
    Filtros opcionais: ano, uf.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    zona: Optional[str] = Query(default=None),
    limite: int = Query(default=100, ge=1, le=5000),
):
    """
    Votos por candidato / zona (/ seção se existir).
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido,
               nr_zona, nr_secao, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    limite: int = Query(default=100, ge=1, le=5000),
):
    """
    Votos por candidato agregados por município.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_municipio, nm_municipio,
               nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    cd_cargo: Optional[str] = Query(default=None),
    limite: int = Query(default=100, ge=1, le=5000),
):
    """
    Votos por candidato agregados por cargo.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_cargo, ds_cargo,
               nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    limite: int = Query(default=100, ge=1, le=5000),
):
    """
    Lista candidatos com seus votos totais.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT nm_candidato, sg_partido, ano, uf,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
):
    """
    Lista partidos com seus votos totais.
    """
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
    params = []

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
    ano: Optional[str] = Query(default=None),
    uf: Optional[str] = Query(default=None),
    limite: int = Query(default=50, ge=1, le=1000),
):
    """
    Ranking de partidos por votos totais.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params = []

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
    """
    Retorna estatísticas gerais da base.
    """
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
