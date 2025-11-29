class CandidatoMeta(Base):
    """
    Tabela candidatos_meta (já existente no Postgres).
    Usada para Ranking e Pesquisa de candidatos/partidos.

    OBS: no banco a coluna é CD_CARGO,
    mas no código vamos expor como ds_cargo (string),
    mapeando a coluna física 'cd_cargo'.
    """
    __tablename__ = "candidatos_meta"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    ano = Column(String(4), index=True)
    uf = Column(String(2), index=True)

    cd_municipio = Column(String(10), index=True, nullable=True)
    nm_municipio = Column(String(150), nullable=True)

    # Aqui está o ajuste importante:
    # usamos o nome de coluna 'cd_cargo' no banco,
    # mas o atributo em Python se chama 'ds_cargo'
    ds_cargo = Column("cd_cargo", String(100), index=True, nullable=True)

    nr_candidato = Column(String(20), index=True, nullable=True)
    nm_candidato = Column(String(200), nullable=True)

    sg_partido = Column(String(20), index=True, nullable=True)

    total_votos = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("ix_cmeta_ano_uf_cargo", "ano", "uf", "ds_cargo"),
        Index("ix_cmeta_candidato", "ano", "nr_candidato"),
        Index("ix_cmeta_partido", "ano", "sg_partido"),
    )
