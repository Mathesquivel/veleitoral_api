# main.py — API V-ELEITORAL (versão 100% compatível com seu Postgres)

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
import os
from pathlib import Path

from database import SessionLocal, engine
from models import (
    VotoSecao,
    ResumoMunZona,
    ImportLog,
    VotoCandidatoMunZona
)
from schemas import (
    EstatisticasOut,
    LocalMapaOut,
    VotoZonaOut,
    VotoMunicipioOut,
    VotoCargoOut,
    VotoTotalOut,
    PartidoOut,
    RankingPartidosOut,
    UploadResponse
)
from ingestor import processar_csv, processar_zip


# ==============================================================================
# CONFIGURAÇÃO FASTAPI
# ==============================================================================

app = FastAPI(title="API TSE - V-Eleitoral")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Pasta do volume do Railway
UPLOAD_DIR = "/app/dados_tse_volume"
os.makedirs(UPLOAD_DIR, exist_ok=True)

BASE_DIR = Path(__file__).parent


# ==============================================================================
# DEPENDÊNCIA DE SESSÃO DO BANCO
# ==============================================================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ==============================================================================
# ENDPOINT — ESTATÍSTICAS DO BANCO
# ==============================================================================

@app.get("/estatisticas", response_model=EstatisticasOut)
def estatisticas(db: Session = Depends(get_db)):
    total_secao = db.query(VotoSecao).count()
    total_munzona = db.query(ResumoMunZona).count()

    anos = (
        db.query(VotoCandidatoMunZona.ano)
        .distinct()
        .order_by(VotoCandidatoMunZona.ano)
        .all()
    )
    anos = [a[0] for a in anos]

    return EstatisticasOut(
        total_linhas_votos_secao=total_secao,
        total_linhas_resumo_munzona=total_munzona,
        anos_disponiveis=anos,
    )


# ==============================================================================
# ENDPOINT — MAPA POR LOCAIS
# ==============================================================================

@app.get("/votos/localizacao", response_model=list[LocalMapaOut])
def votos_localizacao(
    ano: str = Query(...),
    uf: str = Query(...),
    db: Session = Depends(get_db),
):
    q = (
        db.query(
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
            func.sum(VotoSecao.qt_votos).label("total_votos"),
        )
        .filter(VotoSecao.ano == ano)
        .filter(VotoSecao.uf == uf)
        .group_by(
            VotoSecao.ano, VotoSecao.uf, VotoSecao.cd_municipio, VotoSecao.nm_municipio,
            VotoSecao.nr_zona, VotoSecao.nr_secao, VotoSecao.nr_local_votacao,
            VotoSecao.nm_local_votacao, VotoSecao.endereco_local, VotoSecao.ds_cargo
        )
        .order_by(func.sum(VotoSecao.qt_votos).desc())
    )

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
            total_votos=r.total_votos
        )
        for r in q.all()
    ]


# ==============================================================================
# ENDPOINT — CANDIDATOS (PRINCIPAL)
# ==============================================================================

@app.get("/candidatos", response_model=list[VotoTotalOut])
def candidatos(
    ano: str = Query(...),
    uf: str = Query(...),
    cd_municipio: str | None = None,
    cd_cargo: str | None = None,
    limit: int = Query(100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    """
    Consulta candidatos usando votacao_candidato_munzona
    com colunas EXACTAS solicitadas:
    NM_URNA, NM_CAND, NR_CANDI, SG_PARTIDO, SG_UF, NM_MUNIC, QT_VOTOS, DS_SIT_TOT_TURNO
    """

    try:
        q = db.query(
            VotoCandidatoMunZona.ano.label("ano"),
            VotoCandidatoMunZona.uf.label("uf"),
            VotoCandidatoMunZona.cd_municipio,
            VotoCandidatoMunZona.nm_municipio,
            VotoCandidatoMunZona.cd_cargo,
            VotoCandidatoMunZona.ds_cargo,
            VotoCandidatoMunZona.nr_candidato,
            VotoCandidatoMunZona.nm_candidato,
            VotoCandidatoMunZona.nm_urna_candidato,
            VotoCandidatoMunZona.sg_partido,
            VotoCandidatoMunZona.ds_sit_tot_turno,
            func.sum(VotoCandidatoMunZona.qt_votos).label("total_votos"),
        ).filter(
            VotoCandidatoMunZona.ano == ano,
            VotoCandidatoMunZona.uf == uf
        )

        if cd_municipio:
            q = q.filter(VotoCandidatoMunZona.cd_municipio == cd_municipio)

        if cd_cargo:
            q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

        q = q.group_by(
            VotoCandidatoMunZona.ano,
            VotoCandidatoMunZona.uf,
            VotoCandidatoMunZona.cd_municipio,
            VotoCandidatoMunZona.nm_municipio,
            VotoCandidatoMunZona.cd_cargo,
            VotoCandidatoMunZona.ds_cargo,
            VotoCandidatoMunZona.nr_candidato,
            VotoCandidatoMunZona.nm_candidato,
            VotoCandidatoMunZona.nm_urna_candidato,
            VotoCandidatoMunZona.sg_partido,
            VotoCandidatoMunZona.ds_sit_tot_turno,
        ).order_by(
            func.sum(VotoCandidatoMunZona.qt_votos).desc()
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
                nr_candidato=r.nr_candidato,
                nm_candidato=r.nm_candidato,
                nm_urna_candidato=r.nm_urna_candidato,
                sg_partido=r.sg_partido,
                total_votos=r.total_votos,
                ds_sit_tot_turno=r.ds_sit_tot_turno
            )
            for r in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na query candidatos: {e}")


# ==============================================================================
# ENDPOINT — PARTIDOS
# ==============================================================================

@app.get("/partidos", response_model=list[PartidoOut])
def partidos(
    ano: str = Query(...),
    uf: str = Query(None),
    cd_cargo: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoCandidatoMunZona.sg_partido,
        func.sum(VotoCandidatoMunZona.qt_votos).label("total_votos"),
        VotoCandidatoMunZona.ano,
    ).filter(
        VotoCandidatoMunZona.ano == ano
    )

    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)

    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ano,
    ).order_by(
        func.sum(VotoCandidatoMunZona.qt_votos).desc()
    )

    rows = q.all()

    return [
        PartidoOut(
            sg_partido=r.sg_partido,
            ano=r.ano,
            total_votos=r.total_votos,
        )
        for r in rows
    ]


# ==============================================================================
# ENDPOINT — RANKING DE PARTIDOS
# ==============================================================================

@app.get("/ranking/partidos", response_model=list[RankingPartidosOut])
def ranking_partidos(
    ano: str = Query(...),
    uf: str = None,
    cd_cargo: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(
        VotoCandidatoMunZona.sg_partido,
        func.sum(VotoCandidatoMunZona.qt_votos).label("total_votos"),
    ).filter(VotoCandidatoMunZona.ano == ano)

    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)
    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido
    ).order_by(
        func.sum(VotoCandidatoMunZona.qt_votos).desc()
    )

    return [
        RankingPartidosOut(
            sg_partido=r.sg_partido,
            total_votos=r.total_votos
        )
        for r in q.all()
    ]


# ==============================================================================
# UPLOAD CSV E ZIP
# ==============================================================================

@app.post("/upload", response_model=UploadResponse)
def upload_csv(file: UploadFile = File(...)):
    save_path = f"{UPLOAD_DIR}/{file.filename}"

    with open(save_path, "wb") as f:
        f.write(file.file.read())

    linhas = processar_csv(save_path)
    return UploadResponse(mensagem="Upload OK", linhas_importadas=linhas)


@app.post("/upload-zip", response_model=UploadResponse)
def upload_zip(file: UploadFile = File(...)):
    save_path = f"{UPLOAD_DIR}/{file.filename}"

    with open(save_path, "wb") as f:
        f.write(file.file.read())

    linhas = processar_zip(save_path)
    return UploadResponse(mensagem="ZIP processado", linhas_importadas=linhas)


# ==============================================================================
# LIMPAR VOLUME
# ==============================================================================

@app.post("/clear-volume")
def clear_volume():
    for f in os.listdir(UPLOAD_DIR):
        os.remove(f"{UPLOAD_DIR}/{f}")
    return {"mensagem": "Volume limpo"}


# ==============================================================================
# RELOAD
# ==============================================================================

@app.post("/reload")
def reload_db():
    total = 0
    for f in os.listdir(UPLOAD_DIR):
        if f.endswith(".csv"):
            total += processar_csv(f"{UPLOAD_DIR}/{f}")
        elif f.endswith(".zip"):
            total += processar_zip(f"{UPLOAD_DIR}/{f}")

    return {"mensagem": "Reload completo", "linhas_importadas": total}
