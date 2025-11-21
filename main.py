# main.py
# API FastAPI da V-Eleitoral, lendo dados do bd SQLite alimentado pelos CSV do TSE.

from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from pydantic import BaseModel
import sqlite3
from typing import Optional, List
from pathlib import Path
import os
import zipfile
import shutil

# IMPORTANTE: CORS para permitir que o Lovable acesse a API
from fastapi.middleware.cors import CORSMiddleware

from ingestor import ingest_all, DB_PATH

app = FastAPI(title="API TSE - VELEITORAL")

# =============================
# CORS – Libera para o Lovable
# =============================
origins = ["*"]  # Se quiser deixar mais seguro depois, coloque o domínio do Lovable aqui

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# pasta do volume do Railway para CSVs
UPLOAD_DIR = "/app/dados_tse_volume"
os.makedirs(UPLOAD_DIR, exist_ok=True)

BASE_DIR = Path(__file__).parent


# =============================
# MODELOS DE RESPOSTA
# =============================

class VotoTotal(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
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


class CandidatoInfo(BaseModel):
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    total_votos: int
    ano: Optional[str]
    uf: Optional[str]


class PartidoInfo(BaseModel):
    sg_partido: Optional[str]
    total_votos: int
    anos: Optional[str]
    ufs: Optional[str]


class RankingPartido(BaseModel):
    sg_partido: Optional[str]
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
    try:
        cur.execute("SELECT COUNT(*) AS c FROM votos")
        row = cur.fetchone()
        return row["c"] if row else 0
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


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
# ENDPOINT /clear-volume
# =============================

@app.post("/clear-volume")
def clear_volume():
    dir_path = Path(UPLOAD_DIR)
    arquivos = list(dir_path.glob("*"))
    removidos = 0
    for arq in arquivos:
        try:
            arq.unlink()
            removidos += 1
        except Exception as e:
            print(f"⚠ Erro ao remover {arq}: {e}")

    return {
        "status": "ok",
        "mensagem": f"{removidos} arquivo(s) removido(s) do volume.",
    }


# =============================
# UPLOAD DE CSV E ZIP
# =============================

CHUNK_SIZE = 1024 * 1024  # 1MB


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Envie apenas CSV")

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
        raise HTTPException(500, f"Erro ao gravar CSV: {e}")

    return {
        "status": "ok",
        "arquivo": file.filename,
        "tamanho_mb": round(tamanho_bytes / (1024 * 1024), 2),
        "mensagem": "Upload concluído. Execute /reload para processar.",
    }


@app.post("/upload-zip")
async def upload_zip(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Envie um ZIP com CSVs.")

    zip_path = Path(UPLOAD_DIR) / file.filename
    tamanho_bytes = 0

    # Salvar ZIP
    try:
        with open(zip_path, "wb") as f:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                tamanho_bytes += len(chunk)
                f.write(chunk)
    except Exception as e:
        if zip_path.exists():
            zip_path.unlink()
        raise HTTPException(500, f"Erro ao salvar ZIP: {e}")

    # Extrair CSVs
    extraidos = []
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for member in z.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                nome = Path(member).name
                destino = Path(UPLOAD_DIR) / nome
                with z.open(member) as src, open(destino, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                extraidos.append(str(destino))
    except Exception as e:
        zip_path.unlink(missing_ok=True)
        raise HTTPException(500, f"Erro ao extrair ZIP: {e}")

    zip_path.unlink(missing_ok=True)

    return {
        "status": "ok",
        "total_csv": len(extraidos),
        "arquivos": extraidos,
        "mensagem": "ZIP processado. Execute /reload.",
    }


# =============================
# CONSULTAS
# =============================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(ano: Optional[str] = None, uf: Optional[str] = None, limite: int = 50):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido, SUM(votos) AS total_votos
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

    sql += " GROUP BY ano, uf, nm_candidato, sg_partido ORDER BY total_votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoTotal(**dict(r)) for r in rows]


@app.get("/votos/zona", response_model=List[VotoZona])
def votos_zona(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    zona: Optional[str] = None,
    limite: int = 100,
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nm_candidato, sg_partido, nr_zona, nr_secao, SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)
    if zona: sql += " AND nr_zona = ?"; params.append(zona)

    sql += " GROUP BY ano, uf, nm_candidato, sg_partido, nr_zona, nr_secao ORDER BY votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoZona(**dict(r)) for r in rows]


@app.get("/votos/municipio", response_model=List[VotoMunicipio])
def votos_municipio(ano: Optional[str] = None, uf: Optional[str] = None, limite: int = 100):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_municipio, nm_municipio, nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)

    sql += " GROUP BY ano, uf, cd_municipio, nm_municipio, nm_candidato, sg_partido ORDER BY votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoMunicipio(**dict(r)) for r in rows]


@app.get("/votos/cargo", response_model=List[VotoCargo])
def votos_cargo(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    limite: int = 100,
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, cd_cargo, ds_cargo, nm_candidato, sg_partido, SUM(votos) AS votos
        FROM votos WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)
    if cd_cargo: sql += " AND cd_cargo = ?"; params.append(cd_cargo)

    sql += " GROUP BY ano, uf, cd_cargo, ds_cargo, nm_candidato, sg_partido ORDER BY votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [VotoCargo(**dict(r)) for r in rows]


@app.get("/candidatos", response_model=List[CandidatoInfo])
def candidatos(ano: Optional[str] = None, uf: Optional[str] = None, limite: int = 100):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT nm_candidato, sg_partido, ano, uf, SUM(votos) AS total_votos
        FROM votos WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)

    sql += " GROUP BY nm_candidato, sg_partido, ano, uf ORDER BY total_votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [CandidatoInfo(**dict(r)) for r in rows]


@app.get("/partidos", response_model=List[PartidoInfo])
def partidos(ano: Optional[str] = None, uf: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT sg_partido, SUM(votos) AS total_votos,
               GROUP_CONCAT(DISTINCT ano) AS anos,
               GROUP_CONCAT(DISTINCT uf) AS ufs
        FROM votos WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)

    sql += " GROUP BY sg_partido ORDER BY total_votos DESC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [PartidoInfo(**dict(r)) for r in rows]


@app.get("/ranking/partido", response_model=List[RankingPartido])
def ranking_partidos(ano: Optional[str] = None, uf: Optional[str] = None, limite: int = 50):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT sg_partido, SUM(votos) AS total_votos
        FROM votos WHERE 1=1
    """
    params = []

    if ano: sql += " AND ano = ?"; params.append(ano)
    if uf: sql += " AND uf = ?"; params.append(uf)

    sql += " GROUP BY sg_partido ORDER BY total_votos DESC LIMIT ?"
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [RankingPartido(**dict(r)) for r in rows]


@app.get("/estatisticas", response_model=Estatisticas)
def estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) AS c FROM votos")
        total = cur.fetchone()["c"]
    except:
        total = 0

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
