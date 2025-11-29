# schemas.py
from typing import List, Optional
from pydantic import BaseModel


# ============================
# SCHEMAS BÁSICOS
# ============================

class EstatisticasOut(BaseModel):
    total_linhas_votos_secao: int
    total_linhas_resumo_munzona: int
    anos_disponiveis: List[str]


class LocalMapaOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nr_zona: Optional[str]
    nr_secao: Optional[str]
    nr_local_votacao: Optional[str]
    nm_local_votacao: Optional[str]
    endereco_local: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class VotoZonaOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nr_zona: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class VotoMunicipioOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class VotoCargoOut(BaseModel):
    ano: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int


class VotoTotalOut(BaseModel):
    # Campos chave para pesquisa/ranking
    ano: Optional[str]
    nr_turno: Optional[int] = None   # tabela votacao_candidato_munzona atual não tem, então fica None
    uf: Optional[str]
    cd_municipio: Optional[str] = None
    nm_municipio: Optional[str] = None
    cd_cargo: Optional[str] = None
    ds_cargo: Optional[str] = None

    # API continua falando em "candidato",
    # mas internamente buscamos em NR_VOTAVEL / NM_VOTAVEL
    nr_candidato: Optional[str] = None
    nm_candidato: Optional[str] = None
    nm_urna_candidato: Optional[str] = None  # não existe na tabela, então fica None
    sg_partido: Optional[str] = None

    total_votos: int
    ds_sit_tot_turno: Optional[str] = None


class PartidoOut(BaseModel):
    sg_partido: str
    ano: Optional[str]
    total_votos: int


class RankingPartidosOut(BaseModel):
    sg_partido: str
    total_votos: int


class UploadResponse(BaseModel):
    mensagem: str
    linhas_importadas: int
