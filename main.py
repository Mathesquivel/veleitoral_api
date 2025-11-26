# main.py
# API FastAPI da V-Eleitoral, lendo dados do bd SQLite alimentado pelos CSV do TSE.

from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from pathlib import Path
import sqlite3
import os
import zipfile
import shutil

from ingestor import ingest_all, DB_PATH

app = FastAPI(title="API TSE - VELEITORAL")

# pasta do volume do Railway para CSVs (TODOS os CSV ficam aqui agora)
UPLOAD_DIR = "/app/dados_tse_volume"
os.makedirs(UPLOAD_DIR, exist_ok=True)

BASE_DIR = Path(__file__).parent

# =============================
# MODELOS DE RESPOSTA
# =============================

class VotoTotal(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str] = None
    nm_municipio: Optional[str] = None
    cd_cargo: Optional[str] = None
    ds_cargo: Optional[str]
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    total_votos: int
    ds_sit_tot_turno: Optional[str] = None  # status eleitoral no turno


class VotoZona(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nr_turno: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    cd_cargo: Optional[str]
    ds_cargo: Optional[str]
    nm_candidato: Optional[str]
    nr_candidato: Optional[str]
    sg_partido: Optional[str]
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
    nr_candidato: Optional[str]
    ano: Optional[str]
    uf: Optional[str]
    total_votos: int


class PartidoInfo(BaseModel):
    sg_partido: Optional[str]
    total_votos: int
    anos: List[str]
    ufs: List[str]


class RankingPartido(BaseModel):
    sg_partido: Optional[str]
    total_votos: int


class Estatisticas(BaseModel):
    total_registros: int
    total_votos: int
    total_candidatos: int
    total_partidos: int
    anos_disponiveis: List[str]
    ufs_disponiveis: List[str]


# Modelo específico para o mapa (por escola/local de votação)
class LocalMapaCandidato(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    nr_turno: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nr_zona: Optional[str]
    cd_local_votacao: Optional[str]
    nm_local_votacao: Optional[str]
    endereco: Optional[str]  # ds_local_votacao_endereco em 'votos'
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    total_votos: int          # total de votos do candidato no LOCAL
    secoes: List[str]


# =============================
# FUNÇÕES DE BANCO
# =============================

def get_conn():
    # timeout em segundos: espera o lock liberar antes de dar erro
    conn = sqlite3.connect(DB_PATH, timeout=30)
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


def criar_indices():
    """Cria índices básicos para acelerar as consultas mais usadas."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_votos_ano_uf_partido "
            "ON votos(ano, uf, sg_partido)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_votos_candidato "
            "ON votos(nm_candidato)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_votos_cargo "
            "ON votos(cd_cargo, ds_cargo)"
        )
        conn.commit()
    except sqlite3.OperationalError:
        # Isso acontece quando a tabela 'votos' ainda não existe (ex.: nenhum CSV carregado)
        print("⚠ Não foi possível criar índices em 'votos' (tabela ainda não existe). "
              "Isso é normal se nenhum CSV foi carregado ainda.")
    finally:
        conn.close()


# =============================
# EVENTO DE STARTUP
# =============================

@app.on_event("startup")
def startup_event():
    """
    Com 65 GB de dados NÃO faz sentido reprocessar os CSV toda vez.
    A partir de agora o startup só cria índices (se possível) e
    mostra quantos registros já existem.

    Quando quiser recarregar os dados, use explicitamente o endpoint /reload.
    """
    print("\n🚀 Iniciando API VELEITORAL (sem reload automático de CSVs)...")
    criar_indices()
    total = contar_registros()
    print(f"📊 Registros já existentes em 'votos': {total}")
    print("✅ API pronta para uso.")


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
    Reprocessa todos os CSV disponíveis (no volume /app/dados_tse_volume)
    e recria as tabelas.

    ⚠ Cuidado: com 65 GB isso é uma operação pesada.
    """
    total = ingest_all(clear_table=True)
    criar_indices()
    return {
        "status": "ok",
        "mensagem": "Recarregado com sucesso.",
        "registros_votos": total,
    }


@app.post("/clear-volume")
def clear_volume():
    """
    Remove TODOS os arquivos do volume /app/dados_tse_volume.
    """
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
# ENDPOINTS DE UPLOAD
# =============================

CHUNK_SIZE = 1024 * 1024  # 1 MB


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
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


@app.post("/upload-zip")
async def upload_zip(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(
            status_code=400,
            detail="Envie um arquivo .zip contendo os CSV.",
        )

    zip_path = Path(UPLOAD_DIR) / file.filename
    tamanho_bytes = 0

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
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao salvar arquivo .zip no volume: {e}",
        )

    tamanho_mb = tamanho_bytes / (1024 * 1024)

    extraidos: List[str] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for member in z.namelist():
                if not member.lower().endswith(".csv"):
                    continue

                nome_arquivo = Path(member).name
                destino_csv = Path(UPLOAD_DIR) / nome_arquivo

                with z.open(member) as src, open(destino_csv, "wb") as dst:
                    shutil.copyfileobj(src, dst)

                extraidos.append(str(destino_csv))
    except zipfile.BadZipFile:
        zip_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=400,
            detail="Arquivo .zip inválido ou corrompido.",
        )
    except Exception as e:
        zip_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao extrair arquivos do .zip: {e}",
        )
    finally:
        zip_path.unlink(missing_ok=True)

    return {
        "status": "ok",
        "mensagem": (
            f"Arquivo {file.filename} (≈ {tamanho_mb:.2f} MB) enviado e "
            f"{len(extraidos)} arquivo(s) .csv extraído(s) para {UPLOAD_DIR}. "
            "Agora você pode chamar /reload para processar todos os CSV."
        ),
        "arquivo_zip": file.filename,
        "tamanho_zip_mb": round(tamanho_mb, 2),
        "total_csv_extraidos": len(extraidos),
        "arquivos_csv": extraidos,
    }


# =============================
# ENDPOINTS DE CONSULTA
# =============================

@app.get("/votos/totais", response_model=List[VotoTotal])
def votos_totais(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    nr_turno: Optional[str] = None,
    cd_municipio: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    nr_zona: Optional[str] = None,
    nr_secao: Optional[str] = None,
    nr_candidato: Optional[str] = None,
    sg_partido: Optional[str] = None,
    tp_voto: Optional[str] = None,
    limite: int = Query(default=50, ge=1, le=1000),
):
    """
    Totais de votos agregados com filtros opcionais.
    Retorna também o campo ds_sit_tot_turno (status eleitoral no turno).

    Usa a tabela 'candidatos_meta' (se existir) para complementar partido e status.

    ⚠ Para evitar consultas gigantes, é obrigatório informar pelo menos UM
      destes filtros: ano, uf, cd_municipio, cd_cargo, nr_candidato, sg_partido.
    """
    if not any([ano, uf, cd_municipio, cd_cargo, nr_candidato, sg_partido]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos um filtro entre: ano, uf, cd_municipio, cd_cargo, nr_candidato ou sg_partido.",
        )

    conn = get_conn()
    cur = conn.cursor()

    params: List = []

    # Versão com JOIN em candidatos_meta (se a tabela existir)
    sql_join = """
        SELECT
            v.ano,
            v.uf,
            v.cd_municipio,
            COALESCE(v.nm_municipio, 'Estadual') AS nm_municipio,
            v.cd_cargo,
            v.ds_cargo,
            v.nr_candidato,
            COALESCE(v.nm_candidato, cm.nm_candidato, 'LEGENDA') AS nm_candidato,
            COALESCE(v.sg_partido, cm.sg_partido) AS sg_partido,
            COALESCE(v.ds_sit_tot_turno, cm.ds_sit_tot_turno) AS ds_sit_tot_turno,
            SUM(v.votos) AS total_votos
        FROM votos v
        LEFT JOIN candidatos_meta cm
          ON cm.ano = v.ano
         AND cm.uf = v.uf
         AND cm.cd_cargo = v.cd_cargo
         AND cm.nr_turno = v.nr_turno
         AND cm.nr_candidato = v.nr_candidato
        WHERE 1=1
    """

    # Versão fallback (sem JOIN), caso a tabela candidatos_meta ainda não exista
    sql_sem_join = """
        SELECT
            ano,
            uf,
            cd_municipio,
            COALESCE(nm_municipio, 'Estadual') AS nm_municipio,
            cd_cargo,
            ds_cargo,
            nr_candidato,
            COALESCE(nm_candidato, 'LEGENDA') AS nm_candidato,
            sg_partido,
            ds_sit_tot_turno,
            SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """

    # Construímos o trecho de filtros (é igual para as duas consultas)
    filtros = ""
    if ano:
        filtros += " AND v.ano = ?" if "FROM votos v" in sql_join else " AND ano = ?"
        params.append(ano)
    if uf:
        filtros += " AND v.uf = ?" if "FROM votos v" in sql_join else " AND uf = ?"
        params.append(uf)
    if nr_turno:
        filtros += " AND v.nr_turno = ?" if "FROM votos v" in sql_join else " AND nr_turno = ?"
        params.append(nr_turno)
    if cd_municipio:
        filtros += " AND v.cd_municipio = ?" if "FROM votos v" in sql_join else " AND cd_municipio = ?"
        params.append(cd_municipio)
    if cd_cargo:
        filtros += " AND v.cd_cargo = ?" if "FROM votos v" in sql_join else " AND cd_cargo = ?"
        params.append(cd_cargo)
    if nr_zona:
        filtros += " AND v.nr_zona = ?" if "FROM votos v" in sql_join else " AND nr_zona = ?"
        params.append(nr_zona)
    if nr_secao:
        filtros += " AND v.nr_secao = ?" if "FROM votos v" in sql_join else " AND nr_secao = ?"
        params.append(nr_secao)
    if nr_candidato:
        filtros += " AND v.nr_candidato = ?" if "FROM votos v" in sql_join else " AND nr_candidato = ?"
        params.append(nr_candidato)
    if sg_partido:
        filtros += " AND v.sg_partido = ?" if "FROM votos v" in sql_join else " AND sg_partido = ?"
        params.append(sg_partido)
    if tp_voto:
        filtros += " AND v.tp_voto = ?" if "FROM votos v" in sql_join else " AND tp_voto = ?"
        params.append(tp_voto)

    # Completa as duas queries
    sql_join += filtros + """
        GROUP BY
            v.ano, v.uf,
            v.cd_municipio, v.nm_municipio,
            v.cd_cargo, v.ds_cargo,
            v.nr_candidato, v.nm_candidato,
            v.sg_partido, v.ds_sit_tot_turno
        ORDER BY total_votos DESC
        LIMIT ?
    """
    sql_sem_join += filtros.replace("v.", "") + """
        GROUP BY ano, uf,
                 cd_municipio, nm_municipio,
                 cd_cargo, ds_cargo,
                 nr_candidato, nm_candidato,
                 sg_partido, ds_sit_tot_turno
        ORDER BY total_votos DESC
        LIMIT ?
    """
    params_with_limit = params + [limite]

    rows = []
    try:
        # Tenta a versão com JOIN em candidatos_meta
        cur.execute(sql_join, params_with_limit)
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        # Se a tabela candidatos_meta ainda não existe, cai para a versão antiga
        if "no such table: candidatos_meta" in str(e):
            print("⚠ Tabela 'candidatos_meta' não existe. Usando consulta sem JOIN (comportamento antigo).")
            cur.execute(sql_sem_join, params_with_limit)
            rows = cur.fetchall()
        else:
            conn.close()
            raise

    conn.close()
    return [VotoTotal(**dict(r)) for r in rows]


@app.get("/votos/zona", response_model=List[VotoZona])
def votos_por_zona(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    cd_municipio: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    nr_turno: Optional[str] = None,
    nr_zona: Optional[str] = None,
    nr_secao: Optional[str] = None,
    nr_candidato: Optional[str] = None,
    sg_partido: Optional[str] = None,
    limite: int = Query(default=200, ge=1, le=5000),
):
    """
    Retorna votos agregados por zona/seção, com todos os campos de contexto.

    ⚠ Obrigatório informar pelo menos ano ou uf, para evitar consultas gigantes.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /votos/zona.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            ano,
            uf,
            nr_turno,
            cd_municipio,
            COALESCE(nm_municipio, 'Estadual') AS nm_municipio,
            cd_cargo,
            ds_cargo,
            COALESCE(nm_candidato, 'LEGENDA') AS nm_candidato,
            nr_candidato,
            sg_partido,
            nr_zona,
            nr_secao,
            cd_local_votacao,
            nm_local_votacao,
            SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: List = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)
    if cd_municipio:
        sql += " AND cd_municipio = ?"
        params.append(cd_municipio)
    if cd_cargo:
        sql += " AND cd_cargo = ?"
        params.append(cd_cargo)
    if nr_turno:
        sql += " AND nr_turno = ?"
        params.append(nr_turno)
    if nr_zona:
        sql += " AND nr_zona = ?"
        params.append(nr_zona)
    if nr_secao:
        sql += " AND nr_secao = ?"
        params.append(nr_secao)
    if nr_candidato:
        sql += " AND nr_candidato = ?"
        params.append(nr_candidato)
    if sg_partido:
        sql += " AND sg_partido = ?"
        params.append(sg_partido)

    sql += """
        GROUP BY
            ano,
            uf,
            nr_turno,
            cd_municipio,
            nm_municipio,
            cd_cargo,
            ds_cargo,
            nm_candidato,
            nr_candidato,
            sg_partido,
            nr_zona,
            nr_secao,
            cd_local_votacao,
            nm_local_votacao
        ORDER BY votos DESC
        LIMIT ?
    """
    params.append(limite)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [VotoZona(**dict(r)) for r in rows]


# =============================
# ENDPOINT DO MAPA (USANDO APENAS 'votos')
# =============================

@app.get("/mapa/locais", response_model=List[LocalMapaCandidato])
def mapa_locais_por_escola(
    ano: str = Query(...),
    uf: str = Query(...),
    cd_municipio: str = Query(...),
    nr_turno: str = Query(...),
    cd_cargo: str = Query(...),
    nr_candidato: Optional[str] = None,
    nr_zona: Optional[str] = None,
    nr_secao: Optional[str] = None,
    cd_local_votacao: Optional[str] = None,
    sg_partido: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(1000, ge=1, le=5000),
):
    """
    Cada registro representa um LOCAL DE VOTAÇÃO (escola) onde o candidato/partido teve votos.
    Usa apenas a tabela 'votos', pois ela já contém:
      - cd_local_votacao  -> código do local
      - nm_local_votacao  -> nome da escola
      - ds_local_votacao_endereco -> endereço
      - nr_secao          -> para listar as seções desse local

    O campo 'total_votos' é o total de votos do candidato naquele LOCAL (somando as seções do local).

    ⚠ Filtros obrigatórios: ano, uf, cd_municipio, nr_turno, cd_cargo.
    Recomenda-se sempre informar também nr_candidato.
    """
    offset = (page - 1) * page_size

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        WITH votos_local AS (
            SELECT
                ano,
                uf,
                nr_turno,
                cd_municipio,
                COALESCE(nm_municipio, 'Estadual') AS nm_municipio,
                cd_cargo,
                ds_cargo,
                nr_candidato,
                COALESCE(nm_candidato, 'LEGENDA') AS nm_candidato,
                sg_partido,
                nr_zona,
                cd_local_votacao,
                nm_local_votacao,
                ds_local_votacao_endereco AS endereco,
                GROUP_CONCAT(DISTINCT nr_secao) AS secoes_csv,
                SUM(votos) AS total_votos
            FROM votos
            WHERE 1=1
              AND ano = ?
              AND uf = ?
              AND cd_municipio = ?
              AND nr_turno = ?
              AND cd_cargo = ?
    """
    params: List = [ano, uf, cd_municipio, nr_turno, cd_cargo]

    # Filtros adicionais na tabela de votos
    if nr_candidato:
        sql += " AND nr_candidato = ?"
        params.append(nr_candidato)
    if nr_zona:
        sql += " AND nr_zona = ?"
        params.append(nr_zona)
    if nr_secao:
        sql += " AND nr_secao = ?"
        params.append(nr_secao)
    if cd_local_votacao:
        sql += " AND cd_local_votacao = ?"
        params.append(cd_local_votacao)
    if sg_partido:
        sql += " AND sg_partido = ?"
        params.append(sg_partido)

    sql += """
            GROUP BY
                ano,
                uf,
                nr_turno,
                cd_municipio,
                nm_municipio,
                cd_cargo,
                ds_cargo,
                nr_candidato,
                nm_candidato,
                sg_partido,
                nr_zona,
                cd_local_votacao,
                nm_local_votacao,
                endereco
        )
        SELECT
            ano,
            uf,
            nr_turno,
            cd_municipio,
            nm_municipio,
            nr_zona,
            cd_local_votacao,
            nm_local_votacao,
            endereco,
            nr_candidato,
            nm_candidato,
            sg_partido,
            total_votos,
            secoes_csv
        FROM votos_local
        ORDER BY total_votos DESC
        LIMIT ? OFFSET ?
    """
    params.extend([page_size, offset])

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    resultado: List[LocalMapaCandidato] = []
    for r in rows:
        d = dict(r)
        secoes_csv = d.pop("secoes_csv", "") or ""
        secoes_list = [s for s in secoes_csv.split(",") if s]
        resultado.append(
            LocalMapaCandidato(
                **d,
                secoes=secoes_list,
            )
        )

    return resultado


@app.get("/votos/municipio", response_model=List[VotoMunicipio])
def votos_por_municipio(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=100, ge=1, le=5000),
):
    """
    ⚠ Obrigatório informar pelo menos ano ou uf.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /votos/municipio.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            ano,
            uf,
            cd_municipio,
            nm_municipio,
            nm_candidato,
            sg_partido,
            SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: List = []

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
    """
    ⚠ Obrigatório informar pelo menos ano ou uf.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /votos/cargo.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            ano,
            uf,
            cd_cargo,
            ds_cargo,
            nm_candidato,
            sg_partido,
            SUM(votos) AS votos
        FROM votos
        WHERE 1=1
    """
    params: List = []

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
    """
    ⚠ Obrigatório informar pelo menos ano ou uf.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /candidatos.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            nm_candidato,
            sg_partido,
            nr_candidato,
            ano,
            uf,
            SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params: List = []

    if ano:
        sql += " AND ano = ?"
        params.append(ano)
    if uf:
        sql += " AND uf = ?"
        params.append(uf)

    sql += """
        GROUP BY nm_candidato, sg_partido, nr_candidato, ano, uf
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
    limite: int = Query(default=100, ge=1, le=1000),
):
    """
    Lista partidos com total de votos, e anos/UFs em que aparecem.

    ⚠ Obrigatório informar pelo menos ano ou uf.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /partidos.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            sg_partido,
            SUM(votos) AS total_votos,
            GROUP_CONCAT(DISTINCT ano) AS anos_csv,
            GROUP_CONCAT(DISTINCT uf) AS ufs_csv
        FROM votos
        WHERE 1=1
    """
    params: List = []

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

    result: List[PartidoInfo] = []
    for r in rows:
        d = dict(r)
        anos_csv = d.get("anos_csv") or ""
        ufs_csv = d.get("ufs_csv") or ""
        anos_list = [a for a in anos_csv.split(",") if a] if anos_csv else []
        ufs_list = [u for u in ufs_csv.split(",") if u] if ufs_csv else []
        result.append(
            PartidoInfo(
                sg_partido=d.get("sg_partido"),
                total_votos=int(d.get("total_votos") or 0),
                anos=anos_list,
                ufs=ufs_list,
            )
        )
    return result


@app.get("/ranking/partidos", response_model=List[RankingPartido])
def ranking_partidos(
    ano: Optional[str] = None,
    uf: Optional[str] = None,
    limite: int = Query(default=50, ge=1, le=1000),
):
    """
    ⚠ Obrigatório informar pelo menos ano ou uf.
    """
    if not any([ano, uf]):
        raise HTTPException(
            status_code=400,
            detail="Envie pelo menos ano ou uf para consultar /ranking/partidos.",
        )

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        SELECT
            sg_partido,
            SUM(votos) AS total_votos
        FROM votos
        WHERE 1=1
    """
    params: List = []

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

    # total de registros e votos
    try:
        cur.execute("SELECT COUNT(*) AS c, SUM(votos) AS t FROM votos")
        row = cur.fetchone()
        total_registros = row["c"] or 0
        total_votos = row["t"] or 0
    except sqlite3.OperationalError:
        total_registros = 0
        total_votos = 0

    # total de candidatos distintos
    try:
        cur.execute("SELECT COUNT(DISTINCT nm_candidato) AS c FROM votos")
        row = cur.fetchone()
        total_candidatos = row["c"] or 0
    except sqlite3.OperationalError:
        total_candidatos = 0

    # total de partidos distintos
    try:
        cur.execute("SELECT COUNT(DISTINCT sg_partido) AS c FROM votos")
        row = cur.fetchone()
        total_partidos = row["c"] or 0
    except sqlite3.OperationalError:
        total_partidos = 0

    # anos disponíveis
    try:
        cur.execute("SELECT DISTINCT ano FROM votos WHERE ano IS NOT NULL")
        anos = sorted({r["ano"] for r in cur.fetchall() if r["ano"]})
    except sqlite3.OperationalError:
        anos = []

    # ufs disponíveis
    try:
        cur.execute("SELECT DISTINCT uf FROM votos WHERE uf IS NOT NULL")
        ufs = sorted({r["uf"] for r in cur.fetchall() if r["uf"]})
    except sqlite3.OperationalError:
        ufs = []

    conn.close()

    return Estatisticas(
        total_registros=int(total_registros),
        total_votos=int(total_votos),
        total_candidatos=int(total_candidatos),
        total_partidos=int(total_partidos),
        anos_disponiveis=anos,
        ufs_disponiveis=ufs,
    )
