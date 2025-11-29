from typing import List, Optional
from pydantic import BaseModel


class VotoTotalOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    ds_cargo: Optional[str]
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    total_votos: int


class CandidatoOut(BaseModel):
    """
    Modelo específico para /candidatos.

    Igual ao VotoTotalOut, porém incluindo:
    - ds_sit_tot_turno (status no turno: ELEITO, SUPLENTE, NÃO ELEITO, etc.)
    """
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    ds_cargo: Optional[str]
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
    ds_sit_tot_turno: Optional[str]
    total_votos: int


class VotoZonaOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    nr_zona: Optional[str]
    ds_cargo: Optional[str]
    nr_candidato: Optional[str]
    nm_candidato: Optional[str]
    sg_partido: Optional[str]
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


class PartidoOut(BaseModel):
    sg_partido: Optional[str]
    ano: Optional[str]
    total_votos: int


class RankingPartidosOut(BaseModel):
    sg_partido: Optional[str]
    total_votos: int


class EstatisticasOut(BaseModel):
    total_linhas_votos_secao: int
    total_linhas_resumo_munzona: int
    anos_disponiveis: List[str]


class UploadResponse(BaseModel):
    mensagem: str
    linhas_importadas: int
