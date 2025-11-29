# main.py
from fastapi import (
    FastAPI,
    Query,
    UploadFile,
    File,
    HTTPException,
    Depends,
)
from typing import Optional, List
from pathlib import Path
import os
import zipfile
import shutil

from sqlalchemy.orm import Session
from sqlalchemy import func, inspect

from database import get_db
from ingestor import (
    ingest_votacao_secao,
    ingest_detalhe_munzona,
    ingest_all,
    clear_all_data,
    DATA_DIR,
    init_db,
)
from models import (
    VotoSecao,
    ResumoMunZona,
    VotoCandidatoMunZona,
)
from schemas import (
    VotoTotalOut,
    VotoZonaOut,
    VotoMunicipioOut,
    VotoCargoOut,
    PartidoOut,
    RankingPartidosOut,
    EstatisticasOut,
    UploadResponse,
    LocalMapaOut,
)

app = FastAPI(title="API TSE - VELEITORAL")

UPLOAD_DIR = DATA_DIR
os.makedirs(UPLOAD_DIR, exist_ok=True)


# =============================
# STARTUP
# =============================

@app.on_event("startup")
def on_startup():
    init_db()


# =============================
# DEBUG – LISTAR COLUNAS REAIS
# =============================

@app.get("/debug/cols")
def debug_cols(db: Session = Depends(get_db)):
    """
    Endpoint para listar os nomes REAIS das colunas
    da tabela votacao_candidato_munzona no Postgres do Railway.
    """
    insp = inspect(db.bind)
    cols = insp.get_columns("votacao_candidato_munzona")
    return {"columns": [c["name"] for c in cols]}


# =============================
# ESTATÍSTICAS GERAIS
# =============================

@app.get("/estatisticas", response_model=EstatisticasOut)
def estatisticas(db: Session = Depends(get_db)):
    total_secao = db.query(func.count(VotoSecao.id)).scalar() or 0
    total_mz = db.query(func.count(ResumoMunZona.id)).scalar() or 0

    anos_vsec = db.query(VotoSecao.ano).distinct()
    anos_mz = db.query(ResumoMunZona.ano).distinct()

    anos = sorted({a[0] for a in anos_vsec if a[0]} | {a[0] for a in anos_mz if a[0]})

    return EstatisticasOut(
        total_linhas_votos_secao=total_secao,
        total_linhas_resumo_munzona=total_mz,
        anos_disponiveis=anos,
    )


# =============================
# MAPA / LOCAIS (USA SOMENTE VOTACAO_SECAO)
# =============================

@app.get("/mapa/locais", response_model=List[LocalMapaOut])
def mapa_locais(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoSecao.ano.label("ano"),
        VotoSecao.uf.label("uf"),
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.nr_secao,
        VotoSecao.nr_local_votacao,
        VotoSecao.nm_local_votacao,
        VotoSecao.endereco_local,
        VotoSecao.ds_cargo,
        func.sum(VotoSecao.qt_votos).label("total_votos"),
    )

    if ano:
        q = q.filter(VotoSecao.ano == ano)
    if uf:
        q = q.filter(VotoSecao.uf == uf)
    if cd_municipio:
        q = q.filter(VotoSecao.cd_municipio == cd_municipio)
    if cd_cargo:
        q = q.filter(VotoSecao.cd_cargo == cd_cargo)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.nr_secao,
        VotoSecao.nr_local_votacao,
        VotoSecao.nm_local_votacao,
        VotoSecao.endereco_local,
        VotoSecao.ds_cargo,
    ).order_by(
        func.sum(VotoSecao.qt_votos).desc()
    ).limit(limit)

    rows = q.all()

    return [
        LocalMapaOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            nr_zona=r.nr_zona,
            nr_secao=r.nr_secao,
            nr_local_votacao=r.nr_local_votacao,
            nm_local_votacao=r.nm_local_votacao,
            endereco_local=r.endereco_local,
            ds_cargo=r.ds_cargo,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


# =============================
# AGREGAÇÕES POR SEÇÃO (VotoSecao)
# =============================

@app.get("/votos/zona", response_model=List[VotoZonaOut])
def votos_por_zona(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    nr_zona: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoSecao.ano.label("ano"),
        VotoSecao.uf.label("uf"),
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.ds_cargo,
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
    if cd_cargo:
        q = q.filter(VotoSecao.cd_cargo == cd_cargo)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.nr_zona,
        VotoSecao.ds_cargo,
    ).order_by(
        func.sum(VotoSecao.qt_votos).desc()
    ).limit(limit)

    rows = q.all()

    return [
        VotoZonaOut(
            ano=r.ano,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            nr_zona=r.nr_zona,
            ds_cargo=r.ds_cargo,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


@app.get("/votos/municipio", response_model=List[VotoMunicipioOut])
def votos_por_municipio(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
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
    if cd_cargo:
        q = q.filter(VotoSecao.cd_cargo == cd_cargo)
    if ds_cargo:
        q = q.filter(VotoSecao.ds_cargo == ds_cargo)

    q = q.group_by(
        VotoSecao.ano,
        VotoSecao.uf,
        VotoSecao.cd_municipio,
        VotoSecao.nm_municipio,
        VotoSecao.ds_cargo,
    ).order_by(
        func.sum(VotoSecao.qt_votos).desc()
    ).limit(limit)

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
    ).order_by(
        func.sum(VotoSecao.qt_votos).desc()
    )

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
# AGREGAÇÕES USANDO VOTACAO_CANDIDATO_MUNZONA
# (PESQUISA / RANKING / PARTIDOS / CANDIDATOS)
# =============================

@app.get("/votos/totais", response_model=List[VotoTotalOut])
def votos_totais(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    sg_partido: Optional[str] = Query(None),
    ds_sit_tot_turno: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    """
    Votos agregados por candidato, usando votacao_candidato_munzona.
    Usa QT_VOTOS_NOMINAIS_VALIDOS como total de votos.

    IMPORTANTE: a tabela unificada não tem nr_candidato,
    então não filtramos por esse campo.
    """
    q = db.query(
        VotoCandidatoMunZona.ano.label("ano"),
        VotoCandidatoMunZona.uf.label("uf"),
        VotoCandidatoMunZona.cd_municipio,
        VotoCandidatoMunZona.nm_municipio,
        VotoCandidatoMunZona.cd_cargo,
        VotoCandidatoMunZona.ds_cargo,
        VotoCandidatoMunZona.nm_candidato,
        VotoCandidatoMunZona.nm_urna_candidato,
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ds_sit_tot_turno,
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).label("total_votos"),
    )

    if ano:
        q = q.filter(VotoCandidatoMunZona.ano == ano)
    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)
    if cd_municipio:
        q = q.filter(VotoCandidatoMunZona.cd_municipio == cd_municipio)
    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)
    if ds_cargo:
        q = q.filter(VotoCandidatoMunZona.ds_cargo == ds_cargo)
    if sg_partido:
        q = q.filter(VotoCandidatoMunZona.sg_partido == sg_partido)
    if ds_sit_tot_turno:
        q = q.filter(VotoCandidatoMunZona.ds_sit_tot_turno == ds_sit_tot_turno)

    q = q.group_by(
        VotoCandidatoMunZona.ano,
        VotoCandidatoMunZona.uf,
        VotoCandidatoMunZona.cd_municipio,
        VotoCandidatoMunZona.nm_municipio,
        VotoCandidatoMunZona.cd_cargo,
        VotoCandidatoMunZona.ds_cargo,
        VotoCandidatoMunZona.nm_candidato,
        VotoCandidatoMunZona.nm_urna_candidato,
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ds_sit_tot_turno,
    ).order_by(
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).desc()
    ).limit(limit)

    rows = q.all()

    return [
        VotoTotalOut(
            ano=r.ano,
            nr_turno=None,
            uf=r.uf,
            cd_municipio=r.cd_municipio,
            nm_municipio=r.nm_municipio,
            cd_cargo=r.cd_cargo,
            ds_cargo=r.ds_cargo,
            nr_candidato=None,  # tabela não tem essa coluna
            nm_candidato=r.nm_candidato,
            nm_urna_candidato=r.nm_urna_candidato,
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
            ds_sit_tot_turno=r.ds_sit_tot_turno,
        )
        for r in rows
    ]


@app.get("/candidatos", response_model=List[VotoTotalOut])
def candidatos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_municipio: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    ds_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    """
    Lista candidatos com seus totais (usa votacao_candidato_munzona).
    Mesma base de /votos/totais, mas focado em lista de candidatos.
    """
    try:
        q = db.query(
            VotoCandidatoMunZona.ano.label("ano"),
            VotoCandidatoMunZona.uf.label("uf"),
            VotoCandidatoMunZona.cd_municipio,
            VotoCandidatoMunZona.nm_municipio,
            VotoCandidatoMunZona.cd_cargo,
            VotoCandidatoMunZona.ds_cargo,
            VotoCandidatoMunZona.nm_candidato,
            VotoCandidatoMunZona.nm_urna_candidato,
            VotoCandidatoMunZona.sg_partido,
            VotoCandidatoMunZona.ds_sit_tot_turno,
            func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).label("total_votos"),
        )

        if ano:
            q = q.filter(VotoCandidatoMunZona.ano == ano)
        if uf:
            q = q.filter(VotoCandidatoMunZona.uf == uf)
        if cd_municipio:
            q = q.filter(VotoCandidatoMunZona.cd_municipio == cd_municipio)
        if cd_cargo:
            q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)
        if ds_cargo:
            q = q.filter(VotoCandidatoMunZona.ds_cargo == ds_cargo)

        q = q.group_by(
            VotoCandidatoMunZona.ano,
            VotoCandidatoMunZona.uf,
            VotoCandidatoMunZona.cd_municipio,
            VotoCandidatoMunZona.nm_municipio,
            VotoCandidatoMunZona.cd_cargo,
            VotoCandidatoMunZona.ds_cargo,
            VotoCandidatoMunZona.nm_candidato,
            VotoCandidatoMunZona.nm_urna_candidato,
            VotoCandidatoMunZona.sg_partido,
            VotoCandidatoMunZona.ds_sit_tot_turno,
        ).order_by(
            func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).desc()
        ).limit(limit)

        rows = q.all()

        return [
            VotoTotalOut(
                ano=r.ano,
                nr_turno=None,
                uf=r.uf,
                cd_municipio=r.cd_municipio,
                nm_municipio=r.nm_municipio,
                cd_cargo=r.cd_cargo,
                ds_cargo=r.ds_cargo,
                nr_candidato=None,  # sem coluna no banco
                nm_candidato=r.nm_candidato,
                nm_urna_candidato=r.nm_urna_candidato,
                sg_partido=r.sg_partido,
                total_votos=r.total_votos,
                ds_sit_tot_turno=r.ds_sit_tot_turno,
            )
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na query candidatos: {e}")


@app.get("/partidos", response_model=List[PartidoOut])
def partidos(
    ano: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ano,
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).label("total_votos"),
    ).filter(VotoCandidatoMunZona.sg_partido.isnot(None))

    if ano:
        q = q.filter(VotoCandidatoMunZona.ano == ano)
    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)
    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ano,
    ).order_by(
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).desc()
    ).limit(limit)

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
    uf: Optional[str] = Query(None),
    cd_cargo: Optional[str] = Query(None),
    limit: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoCandidatoMunZona.sg_partido,
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).label("total_votos"),
    ).filter(VotoCandidatoMunZona.sg_partido.isnot(None))

    if ano:
        q = q.filter(VotoCandidatoMunZona.ano == ano)
    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)
    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido,
    ).order_by(
        func.sum(VotoCandidatoMunZona.qt_votos_nominais_validos).desc()
    ).limit(limit)

    rows = q.all()

    return [
        RankingPartidosOut(
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


# =============================
# UPLOAD / RELOAD / CLEAR
# =============================

@app.post("/upload", response_model=UploadResponse)
async def upload_csv(
    tipo: str = Query(
        ...,
        pattern="^(secao|munzona)$",
        description="secao -> VOTACAO_SECAO, munzona -> DETALHE_VOTACAO_MUNZONA",
    ),
    file: UploadFile = File(...),
):
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
    try:
        total = ingest_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no reload: {e}")

    return UploadResponse(
        mensagem="Reload concluído com sucesso",
        linhas_importadas=total,
    )


@app.post("/clear-volume")
def clear_volume():
    for path in Path(UPLOAD_DIR).glob("*"):
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path, ignore_errors=True)

    clear_all_data()

    return {"mensagem": "Volume e dados de votos apagados com sucesso"}
