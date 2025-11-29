class VotoTotalOut(BaseModel):
    ano: Optional[str]
    nr_turno: Optional[int] = None
    uf: Optional[str]
    cd_municipio: Optional[str] = None
    nm_municipio: Optional[str] = None
    cd_cargo: Optional[str] = None
    ds_cargo: Optional[str] = None

    nr_candidato: Optional[str] = None  # continua no schema, mas hoje vem None
    nm_candidato: Optional[str] = None
    nm_urna_candidato: Optional[str] = None
    sg_partido: Optional[str] = None

    total_votos: int
    ds_sit_tot_turno: Optional[str] = None
