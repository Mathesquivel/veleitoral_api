from fastapi import FastAPI, Query
from pydantic import BaseModel
import sqlite3
from typing import Optional, List
from pathlib import Path

# CORS PARA PERMITIR LOVABLE + NGROK
from fastapi.middleware.cors import CORSMiddleware

from ingestor import ingest_all, DB_PATH

app = FastAPI(title="API TSE - VELEITORAL")

# ============================================
# CORS LIBERADO PARA QUALQUER ORIGEM
# Necessário para LOVABLE e NAVEGADOR via NGROK
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent


# =============================
# MODELOS DE RESPOSTA
# =============================

class VotoTotal(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: str
    nr_candidato: Optional[str]
    sg_partido: str
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    total_votos: int


class VotoZona(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nr_turno: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    nm_candidato: str
    nr_candidato: Optional[str]
    sg_partido: str
    nr_zona: Optional[str]
    nr_secao: Optional[str]
    cd_local_votacao: Optional[str]
    nm_local_votacao: Optional[str]
    votos: int


class VotoMunicipio(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    total_votos: int


class VotoCargo(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class CandidatoInfo(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nm_candidato: str
    nr_candidato: Optional[str]
    sg_partido: str
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class PartidoInfo(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    sg_partido: str
    total_votos: int


class RankingPartido(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    sg_partido: str
    total_votos: int


class Estatisticas(BaseModel):
    total_registros: int
    total_candidatos: int
    total_partidos: int
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
# /votos/totais
# =============================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    nm_municipio: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    nr_turno: Optional[str] = Query(None),
    nr_zona: Optional[str] = Query(None),
    nr_secao: Optional[str] = Query(None),
    nr_candidato: Optional[str] = Query(None),
    limite: int = Query(50, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               nm_candidato, nr_candidato, sg_partido,
               cd_cargo, ds_cargo,
               cd_municipio, nm_municipio,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_municipio", cd_municipio)
    add("nm_municipio", nm_municipio)
    add("cd_cargo", cd_cargo)
    add("ds_cargo", ds_cargo)
    add("nr_turno", nr_turno)
    add("nr_zona", nr_zona)
    add("nr_secao", nr_secao)
    add("nr_candidato", nr_candidato)

    sql += """
        GROUP BY ano, uf,
                 nm_candidato, nr_candidato, sg_partido,
                 cd_cargo, ds_cargo,
                 cd_municipio, nm_municipio
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [VotoTotal(**dict(r)) for r in rows]


# =============================
# /votos/zona
# =============================

@app.get("/votos/zona", response_model=List[VotoZona])
def votos_por_zona(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    nr_turno: Optional[str] = Query(None),
    nr_zona: Optional[str] = Query(None),
    nr_secao: Optional[str] = Query(None),
    limite: int = Query(200, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf, nr_turno,
               cd_municipio, nm_municipio,
               cd_cargo, ds_cargo,
               nm_candidato, nr_candidato, sg_partido,
               nr_zona, nr_secao,
               cd_local_votacao, nm_local_votacao,
               SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_municipio", cd_municipio)
    add("nr_turno", nr_turno)
    add("nr_zona", nr_zona)
    add("nr_secao", nr_secao)

    sql += """
        GROUP BY ano, uf, nr_turno,
                 cd_municipio, nm_municipio,
                 cd_cargo, ds_cargo,
                 nm_candidato, nr_candidato, sg_partido,
                 nr_zona, nr_secao,
                 cd_local_votacao, nm_local_votacao
        ORDER BY votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [VotoZona(**dict(r)) for r in rows]


# =============================
# /votos/municipio
# =============================

@app.get("/votos/municipio", response_model=List[VotoMunicipio])
def votos_por_municipio(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               cd_municipio, nm_municipio,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_municipio", cd_municipio)

    sql += """
        GROUP BY ano, uf, cd_municipio, nm_municipio
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [VotoMunicipio(**dict(r)) for r in rows]


# =============================
# /votos/cargo
# =============================

@app.get("/votos/cargo", response_model=List[VotoCargo])
def votos_por_cargo(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               cd_cargo, ds_cargo,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_cargo", cd_cargo)

    sql += """
        GROUP BY ano, uf, cd_cargo, ds_cargo
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [VotoCargo(**dict(r)) for r in rows]


# =============================
# /candidatos
# =============================

@app.get("/candidatos", response_model=List[CandidatoInfo])
def listar_candidatos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    nr_candidato: Optional[str] = Query(None),
    limite: int = Query(200, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               nm_candidato, nr_candidato,
               sg_partido,
               cd_cargo, ds_cargo,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_cargo", cd_cargo)
    add("nr_candidato", nr_candidato)

    sql += """
        GROUP BY ano, uf,
                 nm_candidato, nr_candidato,
                 sg_partido, cd_cargo, ds_cargo
        ORDER BY total_votos DESC
        LIMIT ?
    """

    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [CandidatoInfo(**dict(r)) for r in rows]


# =============================
# /partidos
# =============================

@app.get("/partidos", response_model=List[PartidoInfo])
def listar_partidos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    limite: int = Query(200, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)

    sql += """
        GROUP BY ano, uf, sg_partido
        ORDER BY total_votos DESC
        LIMIT ?
    """

    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [PartidoInfo(**dict(r)) for r in rows]


# =============================
# /ranking/partido
# =============================

@app.get("/ranking/partido", response_model=List[RankingPartido])
def ranking_partidos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=20000),
):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT ano, uf,
               cd_cargo, ds_cargo,
               sg_partido,
               SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    params = []

    def add(col, val):
        nonlocal sql, params
        if val:
            sql += f" AND {col} = ?"
            params.append(val)

    add("ano", ano)
    add("uf", uf)
    add("cd_cargo", cd_cargo)

    sql += """
        GROUP BY ano, uf, cd_cargo, ds_cargo, sg_partido
        ORDER BY total_votos DESC
        LIMIT ?
    """

    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [RankingPartido(**dict(r)) for r in rows]


# =============================
# /estatisticas
# =============================

@app.get("/estatisticas", response_model=Estatisticas)
def estatisticas():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM votos")
    total_registros = cur.fetchone()["c"]

    cur.execute("""
        SELECT COUNT(DISTINCT nm_candidato || '|' || nr_candidato || '|' || ano || '|' || uf) AS c
        FROM votos
    """)
    total_candidatos = cur.fetchone()["c"]

    cur.execute("""
        SELECT COUNT(DISTINCT sg_partido || '|' || ano || '|' || uf) AS c
        FROM votos
    """)
    total_partidos = cur.fetchone()["c"]

    cur.execute("SELECT DISTINCT ano FROM votos WHERE ano IS NOT NULL ORDER BY ano")
    anos = [r["ano"] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT uf FROM votos WHERE uf IS NOT NULL ORDER BY uf")
    ufs = [r["uf"] for r in cur.fetchall()]

    conn.close()

    return Estatisticas(
        total_registros=total_registros,
        total_candidatos=total_candidatos,
        total_partidos=total_partidos,
        anos_disponiveis=anos,
        ufs_disponiveis=ufs,
    )
