# main.py — API VELEITORAL (versão otimizada e compatível com o Lovable)
# --------------------------------------------------------------

from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from pydantic import BaseModel
import sqlite3
from typing import Optional, List
from pathlib import Path
import os
import zipfile
import shutil

from ingestor import ingest_all, DB_PATH

app = FastAPI(title="API TSE - VELEITORAL")

UPLOAD_DIR = "/app/dados_tse_volume"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ======================================================
# MODELOS DE RESPOSTA
# ======================================================

class Estatisticas(BaseModel):
    total_registros: int
    total_votos: int
    total_candidatos: int
    total_partidos: int
    anos_disponiveis: List[str]
    ufs_disponiveis: List[str]


class VotoTotal(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    ds_cargo: Optional[str]
    nm_municipio: Optional[str] = None
    total_votos: int


class VotoZona(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    nr_zona: Optional[str]
    nr_secao: Optional[str]
    votos: int


class VotoMunicipio(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    votos: int


class VotoCargo(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    votos: int


# ======================================================
# FUNÇÕES DE BANCO
# ======================================================

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ======================================================
# STARTUP — CARREGAR CSV DO VOLUME
# ======================================================

@app.on_event("startup")
def startup_event():
    print("🚀 Iniciando ingestão...")
    ingest_all(clear_table=False)
    print("🚀 Ingestão concluída.")


# ======================================================
# ENDPOINTS BÁSICOS
# ======================================================

@app.get("/")
def root():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM votos")
    total = cur.fetchone()["c"]
    return {
        "status": "ok",
        "registros_votos": total,
        "banco": str(DB_PATH),
    }


# ======================================================
# ESTATÍSTICAS COMPLETAS
# ======================================================

@app.get("/estatisticas", response_model=Estatisticas)
def estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    # total de registros + soma de votos
    cur.execute("SELECT COUNT(*) AS total_registros, SUM(votos) AS total_votos FROM votos")
    row = cur.fetchone()
    total_registros = row["total_registros"]
    total_votos = row["total_votos"] or 0

    # total de candidatos
    cur.execute("SELECT COUNT(DISTINCT nm_candidato) AS c FROM votos WHERE nm_candidato IS NOT NULL")
    total_candidatos = cur.fetchone()["c"]

    # total de partidos
    cur.execute("SELECT COUNT(DISTINCT sg_partido) AS c FROM votos WHERE sg_partido IS NOT NULL")
    total_partidos = cur.fetchone()["c"]

    # lista de anos
    cur.execute("SELECT DISTINCT ano FROM votos WHERE ano IS NOT NULL")
    anos = sorted([r["ano"] for r in cur.fetchall()])

    # lista de UFs
    cur.execute("SELECT DISTINCT uf FROM votos WHERE uf IS NOT NULL")
    ufs = sorted([r["uf"] for r in cur.fetchall()])

    conn.close()

    return Estatisticas(
        total_registros=total_registros,
        total_votos=total_votos,
        total_candidatos=total_candidatos,
        total_partidos=total_partidos,
        anos_disponiveis=anos,
        ufs_disponiveis=ufs,
    )


# ======================================================
# RANKING DE CANDIDATOS
# ======================================================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = 50
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               nr_candidato,
               nm_candidato,
               sg_partido,
               ds_cargo,
               SUM(votos) AS total_votos
        FROM votos
        WHERE nm_candidato IS NOT NULL
    """
    params = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY ano, uf, nr_candidato, nm_candidato, sg_partido, ds_cargo
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    resultado = []
    for r in rows:
        d = dict(r)
        d["nm_municipio"] = None
        resultado.append(VotoTotal(**d))

    return resultado


# ======================================================
# CONSULTAS COMPLEMENTARES
# ======================================================

@app.get("/votos/zona", response_model=List[VotoZona])
def votos_por_zona(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    zona: Optional[str] = None,
    limite: int = 100
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido,
               nr_zona, nr_secao,
               SUM(votos) AS votos
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


# ======================================================
# UPLOAD CSV / ZIP
# ======================================================

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    dest = Path(UPLOAD_DIR) / file.filename
    with open(dest, "wb") as f:
        f.write(await file.read())
    return {"status": "ok", "arquivo": file.filename}


@app.post("/upload-zip")
async def upload_zip(file: UploadFile = File(...)):
    zip_path = Path(UPLOAD_DIR) / file.filename
    with open(zip_path, "wb") as f:
        f.write(await file.read())
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(UPLOAD_DIR)
    return {"status": "ok", "mensagem": "ZIP extraído"}


# ======================================================
# CLEAR VOLUME
# ======================================================

@app.post("/clear-volume")
def clear_volume():
    count = 0
    for f in Path(UPLOAD_DIR).glob("*"):
        f.unlink()
        count += 1
    return {"status": "ok", "removidos": count}
