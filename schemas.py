# schemas.py
from typing import Optional, List
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

    class Config:
        from_attributes = True


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

    class Config:
        from_attributes = True


class VotoMunicipioOut(BaseModel):
    ano: Optional[str]
    uf: Optional[str]
    cd_municipio: Optional[str]
    nm_municipio: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int

    class Config:
        from_attributes = True


class VotoCargoOut(BaseModel):
    ano: Optional[str]
    ds_cargo: Optional[str]
    total_votos: int

    class Config:
        from_attributes = True


class PartidoOut(BaseModel):
    sg_partido: str
    ano: Optional[str]
    total_votos: int

    class Config:
        from_attributes = True


class RankingPartidosOut(BaseModel):
    sg_partido: str
    total_votos: int

    class Config:
        from_attributes = True


class EstatisticasOut(BaseModel):
    total_linhas_votos_secao: int
    total_linhas_resumo_munzona: int
    anos_disponiveis: List[str]


class UploadResponse(BaseModel):
    mensagem: str
    linhas_importadas: int
