# main.py
from fastapi import (
    FastAPI,
    Query,
    UploadFile,
    File,
    HTTPException,
    Depends,
)
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from pathlib import Path
import os
import zipfile
import shutil

from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from database import get_db
from ingestor import (
    ingest_votacao_secao,
    ingest_detalhe_munzona,
    ingest_all,
    clear_all_data,
    DATA_DIR,
    init_db,
)
from models import VotoSecao, ResumoMunZona, CandidatoMeta
from schemas import (
    VotoTotalOut,
    VotoZonaOut,
    VotoMunicipioOut,
    VotoCargoOut,
    PartidoOut,
    RankingPartidosOut,
    EstatisticasOut,
    UploadResponse,
)

app = FastAPI(title="API TSE - VELEITORAL")

# =============================
# CORS
# =============================
origins = [
    "http://localhost:3000",
    "http://localhost:4173",
    "https://lovable.dev",
    "https://*.lovable.app",
    "https://veleitoral.lovable.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # se quiser, depois troca por 'origins'
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Diretório do volume
UPLOAD_DIR = DATA_DIR
os.makedirs(UPLOAD_DIR, exist_ok=True)


# =============================
# STARTUP
# =============================

@app.on_event("startup")
def on_startup():
    init_db()


# =============================
# ESTATÍSTICAS
# =============================

@app.get("/estatisticas", response_model=EstatisticasOut)
def estatisticas(db: Session = Depends(get_db)):
    total_secao = db.query(func.count(VotoSecao.id)).scalar() or 0
    total_mz = db.query(func.count(ResumoMunZona.id)).scalar() or 0

    anos_vsec = db.query(VotoSecao.ano).distinct()
    anos_mz = db.query(ResumoMunZona.ano).distinct()
    anos_cmeta = db.query(CandidatoMeta.ano).distinct()

    anos = sorted(
        {a[0] for a in anos_vsec if a[0]}
        | {a[0] for a in anos_mz if a[0]}
        | {a[0] for a in anos_cmeta if a[0]}
    )

    return EstatisticasOut(
        total_linhas_votos_secao=total_secao,
        total_linhas_resumo_munzona=total_mz,
        anos_disponiveis=anos,
    )


# =============================
# VOTOS / AGREGAÇÕES
# =============================

@app.get("/votos/totais", response_model=List[VotoTotalOut])
def votos_totais(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    nr_candidato: Optional[str] = Query(None),
    sg_partido: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Votos agregados por candidato.
    VOTOS = soma de votos_secao.qt_votos
    META = candidatos_meta
    """
    q = db.query(
        CandidatoMeta.ano,
        CandidatoMeta.uf,
        CandidatoMeta.cd_municipio,
        CandidatoMeta.nm_municipio,
        VotoSecao.ds_cargo.label("ds_cargo"),
        CandidatoMeta.nr_candidato,
        CandidatoMeta.nm_candidato,
        CandidatoMeta.sg_partido,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    ).join(
        VotoSecao,
        and_(
            VotoSecao.ano == CandidatoMeta.ano,
            VotoSecao.uf == CandidatoMeta.uf,
            VotoSecao.cd_municipio == CandidatoMeta.cd_municipio,
            VotoSecao.cd_cargo == CandidatoMeta.cd_cargo,
            VotoSecao.nr_votavel == CandidatoMeta.nr_candidato,
        ),
    )

    if ano:
        q = q.filter(CandidatoMeta.ano == ano)
    if uf:
        q = q.filter(CandidatoMeta.uf == uf)
    if cd_municipio:
        q = q.filter(CandidatoMeta.cd_municipio == cd_municipio)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)
    if nr_candidato:
        q = q.filter(CandidatoMeta.nr_candidato == nr_candidato)
    if sg_partido:
        q = q.filter(CandidatoMeta.sg_partido == sg_partido)

    q = q.group_by(
        CandidatoMeta.ano,
        CandidatoMeta.uf,
        CandidatoMeta.cd_municipio,
        CandidatoMeta.nm_municipio,
        VotoSecao.ds_cargo,
        CandidatoMeta.nr_candidato,
        CandidatoMeta.nm_candidato,
        CandidatoMeta.sg_partido,
    ).order_by(func.sum(VotoSecao.qt_votos).desc()).limit(limit)

    rows = q.all()

    return [
        VotoTotalOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            ds_cargo=r.ds_cargo,
            nr_candidato=r.nr_candidato,
            nm_candidato=r.nm_candidato,
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/votos/zona", response_model=List[VotoZonaOut])
def votos_por_zona(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    nr_zona: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Votos por ZONA, a partir de votos_secao.
    Usado para mapa e detalhamento por zona.
    """
    q = db.query(
        VotoSecao.ano.label("ano"),
        VotoSecao.uf.label("uf"),
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.ds_cargo,
        VotoSecao.nr_votavel.label("nr_candidato"),
        VotoSecao.nm_votavel.label("nm_candidato"),
        VotoSecao.sg_partido,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    )

    if ano:
        q = q.filter(VotoSecao.ano == ano)
    if uf:
        q = q.filter(VotoSecao.uf == uf)
    if cd_municipio:
        q = q.filter(VotoSecao.cd_municipio == cd_municipio)
    if nr_zona:
        q = q.filter(VotoSecao.nr_zona == nr_zona)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.ds_cargo,
        VotoSecao.nr_votavel,
        VotoSecao.nm_votavel,
        VotoSecao.sg_partido,
    ).order_by(func.sum(VotoSecao.qt_votos).desc()).limit(limit)

    rows = q.all()

    return [
        VotoZonaOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            nr_zona=r.nr_zona,
            ds_cargo=r.ds_cargo,
            nr_candidato=r.nr_candidato,
            nm_candidato=r.nm_candidato,
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/votos/municipio", response_model=List[VotoMunicipioOut])
def votos_por_municipio(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Votos agregados por MUNICÍPIO.
    """
    q = db.query(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.ds_cargo,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    )

    if ano:
        q = q.filter(VotoSecao.ano == ano)
    if uf:
        q = q.filter(VotoSecao.uf == uf)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.ds_cargo,
    ).order_by(func.sum(VotoSecao.qt_votos).desc()).limit(limit)

    rows = q.all()

    return [
        VotoMunicipioOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            ds_cargo=r.ds_cargo,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/votos/cargo", response_model=List[VotoCargoOut])
def votos_por_cargo(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Votos agregados por CARGO (usando diretamente votos_secao).
    """
    q = db.query(
        VotoSecao.ano,
        VotoSecao.ds_cargo,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    )

    if ano:
        q = q.filter(VotoSecao.ano == ano)
    if uf:
        q = q.filter(VotoSecao.uf == uf)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.ds_cargo,
    ).order_by(func.sum(VotoSecao.qt_votos).desc())

    rows = q.all()

    return [
        VotoCargoOut(
            ano=r.ano,
            ds_cargo=r.ds_cargo,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


# =============================
# CANDIDATOS / PARTIDOS
# =============================

@app.get("/candidatos", response_model=List[VotoTotalOut])
def candidatos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Lista candidatos com total de votos.
    Votos = soma de votos_secao.
    """
    q = db.query(
        CandidatoMeta.ano,
        CandidatoMeta.uf,
        CandidatoMeta.cd_municipio,
        CandidatoMeta.nm_municipio,
        VotoSecao.ds_cargo.label("ds_cargo"),
        CandidatoMeta.nr_candidato,
        CandidatoMeta.nm_candidato,
        CandidatoMeta.sg_partido,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    ).join(
        VotoSecao,
        and_(
            VotoSecao.ano == CandidatoMeta.ano,
            VotoSecao.uf == CandidatoMeta.uf,
            VotoSecao.cd_municipio == CandidatoMeta.cd_municipio,
            VotoSecao.cd_cargo == CandidatoMeta.cd_cargo,
            VotoSecao.nr_votavel == CandidatoMeta.nr_candidato,
        ),
    )

    if ano:
        q = q.filter(CandidatoMeta.ano == ano)
    if uf:
        q = q.filter(CandidatoMeta.uf == uf)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        CandidatoMeta.ano,
        CandidatoMeta.uf,
        CandidatoMeta.cd_municipio,
        CandidatoMeta.nm_municipio,
        VotoSecao.ds_cargo,
        CandidatoMeta.nr_candidato,
        CandidatoMeta.nm_candidato,
        CandidatoMeta.sg_partido,
    ).order_by(func.sum(VotoSecao.qt_votos).desc()).limit(limit)

    rows = q.all()

    return [
        VotoTotalOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            ds_cargo=r.ds_cargo,
            nr_candidato=r.nr_candidato,
            nm_candidato=r.nm_candidato,
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/partidos", response_model=List[PartidoOut])
def partidos(
    ano: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Total de votos por partido.
    """
    q = db.query(
        CandidatoMeta.sg_partido,
        CandidatoMeta.ano,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    ).join(
        VotoSecao,
        and_(
            VotoSecao.ano == CandidatoMeta.ano,
            VotoSecao.uf == CandidatoMeta.uf,
            VotoSecao.cd_municipio == CandidatoMeta.cd_municipio,
            VotoSecao.cd_cargo == CandidatoMeta.cd_cargo,
            VotoSecao.nr_votavel == CandidatoMeta.nr_candidato,
        ),
    ).filter(CandidatoMeta.sg_partido.isnot(None))

    if ano:
        q = q.filter(CandidatoMeta.ano == ano)

    q = q.group_by(
        CandidatoMeta.sg_partido,
        CandidatoMeta.ano,
    ).order_by(func.sum(VotoSecao.qt_votos).desc())

    rows = q.all()

    return [
        PartidoOut(
            sg_partido=r.sg_partido,
            ano=r.ano,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/ranking/partidos", response_model=List[RankingPartidosOut])
def ranking_partidos(
    ano: Optional[str] = Query(None),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Ranking de partidos por votos totais.
    """
    q = db.query(
        CandidatoMeta.sg_partido,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    ).join(
        VotoSecao,
        and_(
            VotoSecao.ano == CandidatoMeta.ano,
            VotoSecao.uf == CandidatoMeta.uf,
            VotoSecao.cd_municipio == CandidatoMeta.cd_municipio,
            VotoSecao.cd_cargo == CandidatoMeta.cd_cargo,
            VotoSecao.nr_votavel == CandidatoMeta.nr_candidato,
        ),
    ).filter(CandidatoMeta.sg_partido.isnot(None))

    if ano:
        q = q.filter(CandidatoMeta.ano == ano)

    q = q.group_by(
        CandidatoMeta.sg_partido,
    ).order_by(func.sum(VotoSecao.qt_votos).desc()).limit(limit)

    rows = q.all()

    return [
        RankingPartidosOut(
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


# =============================
# UPLOAD / RELOAD
# =============================

@app.post("/upload", response_model=UploadResponse)
async def upload_csv(
    tipo: str = Query(
        ...,
        regex="^(secao|munzona|generico|resumo)$",
        description="Tipo de arquivo: secao ou munzona (generico/resumo tratam como munzona)",
    ),
    file: UploadFile = File(...),
):
    """
    Faz upload de UM CSV e ingere:
      - tipo=secao  -> VOTACAO_SECAO -> votos_secao
      - tipo=munzona/generico/resumo -> DETALHE_VOTACAO_MUNZONA -> resumo_munzona
    """
    filename = file.filename

    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .csv")

    dest_path = Path(UPLOAD_DIR) / filename
    with dest_path.open("wb") as f:
        content = await file.read()
        f.write(content)

    try:
        if tipo == "secao":
            linhas = ingest_votacao_secao(dest_path)
        else:
            linhas = ingest_detalhe_munzona(dest_path)
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Erro ao processar CSV: {str(e)}")

    return UploadResponse(
        mensagem=f"Arquivo {filename} importado com sucesso",
        linhas_importadas=linhas,
    )


@app.post("/upload-zip", response_model=UploadResponse)
async def upload_zip(file: UploadFile = File(...)):
    """
    Upload de um ZIP com vários CSVs.
    - 'SECAO' no nome -> ingest_votacao_secao
    - 'MUNZONA' no nome -> ingest_detalhe_munzona
    """
    filename = file.filename

    if not filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .zip")

    zip_path = Path(UPLOAD_DIR) / filename
    with zip_path.open("wb") as f:
        content = await file.read()
        f.write(content)

    extracted_dir = Path(UPLOAD_DIR) / (filename + "_unzipped")
    if extracted_dir.exists():
        shutil.rmtree(extracted_dir)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    total_linhas = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extracted_dir)

        for path in extracted_dir.rglob("*.csv"):
            name_upper = path.name.upper()
            if "SECAO" in name_upper:
                total_linhas += ingest_votacao_secao(path)
            elif "MUNZONA" in name_upper:
                total_linhas += ingest_detalhe_munzona(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar ZIP: {str(e)}")
    finally:
        zip_path.unlink(missing_ok=True)
        shutil.rmtree(extracted_dir, ignore_errors=True)

    return UploadResponse(
        mensagem=f"ZIP {filename} importado com sucesso",
        linhas_importadas=total_linhas,
    )


@app.post("/reload", response_model=UploadResponse)
def reload_arquivos_existentes():
    """
    Reingere TODOS os CSVs presentes em /app/dados_tse_volume.
    """
    try:
        total = ingest_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no reload: {str(e)}")

    return UploadResponse(
        mensagem="Reload concluído com sucesso",
        linhas_importadas=total,
    )


@app.post("/clear-volume")
def clear_volume():
    """
    Apaga arquivos do volume e limpa votos_secao + resumo_munzona.
    NÃO mexe em candidatos_meta.
    """
    for path in Path(UPLOAD_DIR).glob("*"):
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path, ignore_errors=True)

    clear_all_data()

    return {"mensagem": "Volume e dados de votos apagados com sucesso"}
