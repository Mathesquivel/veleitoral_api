# main.py — API V-ELEITORAL (versão somente leitura, focada em Postgres)

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
import os
from pathlib import Path

from database import SessionLocal
from models import (
    VotoSecao,
    ResumoMunZona,
    ImportLog,
    VotoCandidatoMunZona,
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
    UploadResponse,
)

# ==============================================================================
# CONFIGURAÇÃO FASTAPI
# ==============================================================================

app = FastAPI(title="API TSE - V-Eleitoral")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# ENDPOINT — VOTOS POR LOCALIZAÇÃO (MAPA)
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
        )
        .order_by(func.sum(VotoSecao.qt_votos).desc())
    )

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

# ==============================================================================
# ENDPOINT — CANDIDATOS (PRINCIPAL)
# ==============================================================================

@app.get("/candidatos", response_model=list[VotoTotalOut])
def candidatos(
    ano: str = Query(...),
    uf: str = Query(...),
    cd_municipio: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    limit: int = Query(100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    """
    Consulta candidatos usando votacao_candidato_munzona.

    Espera que o modelo VotoCandidatoMunZona tenha:
    - ano
    - uf
    - cd_municipio
    - nm_municipio
    - cd_cargo
    - ds_cargo
    - nr_candidato
    - nm_candidato
    - nm_urna_candidato
    - sg_partido
    - ds_sit_tot_turno
    - qt_votos
    """

    try:
        q = (
            db.query(
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
            )
            .filter(VotoCandidatoMunZona.ano == ano)
            .filter(VotoCandidatoMunZona.uf == uf)
        )

        if cd_municipio:
            q = q.filter(VotoCandidatoMunZona.cd_municipio == cd_municipio)

        if cd_cargo:
            q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

        q = (
            q.group_by(
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
            )
            .order_by(func.sum(VotoCandidatoMunZona.qt_votos).desc())
            .limit(limit)
        )

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
                ds_sit_tot_turno=r.ds_sit_tot_turno,
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
    uf: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = (
        db.query(
            VotoCandidatoMunZona.sg_partido,
            func.sum(VotoCandidatoMunZona.qt_votos).label("total_votos"),
            VotoCandidatoMunZona.ano,
        )
        .filter(VotoCandidatoMunZona.ano == ano)
    )

    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)

    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido,
        VotoCandidatoMunZona.ano,
    ).order_by(func.sum(VotoCandidatoMunZona.qt_votos).desc())

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
    uf: Optional[str] = None,
    cd_cargo: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = (
        db.query(
            VotoCandidatoMunZona.sg_partido,
            func.sum(VotoCandidatoMunZona.qt_votos).label("total_votos"),
        )
        .filter(VotoCandidatoMunZona.ano == ano)
    )

    if uf:
        q = q.filter(VotoCandidatoMunZona.uf == uf)

    if cd_cargo:
        q = q.filter(VotoCandidatoMunZona.cd_cargo == cd_cargo)

    q = q.group_by(
        VotoCandidatoMunZona.sg_partido,
    ).order_by(func.sum(VotoCandidatoMunZona.qt_votos).desc())

    rows = q.all()

    return [
        RankingPartidosOut(
            sg_partido=r.sg_partido,
            total_votos=r.total_votos,
        )
        for r in rows
    ]

# ==============================================================================
# ENDPOINTS DE UPLOAD / RELOAD (DESATIVADOS NESTA VERSÃO)
# ==============================================================================

@app.post("/upload", response_model=UploadResponse)
def upload_csv_desativado():
    """
    Mantido só para compatibilidade com o frontend.
    Dados já estão no Postgres, então não há upload aqui.
    """
    return UploadResponse(mensagem="Upload desativado nesta versão (dados já estão no Postgres).", linhas_importadas=0)

@app.post("/upload-zip", response_model=UploadResponse)
def upload_zip_desativado():
    return UploadResponse(mensagem="Upload ZIP desativado nesta versão (dados já estão no Postgres).", linhas_importadas=0)

@app.post("/clear-volume")
def clear_volume_desativado():
    return {"mensagem": "Clear volume desativado nesta versão (não é necessário em Postgres)."}

@app.post("/reload")
def reload_desativado():
    return {"mensagem": "Reload desativado nesta versão. Dados já estão carregados no Postgres."}
