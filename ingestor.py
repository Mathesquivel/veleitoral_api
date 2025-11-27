import pandas as pd
import sqlite3
from pathlib import Path
import re

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path(__file__).parent

# A ingestão lê APENAS do volume do Railway
from pathlib import Path

# Banco dentro do volume da Railway (persiste junto com os CSVs)
DB_PATH = Path("/app/dados_tse_volume") / "tse_eleicoes.db"

BASE_DIR = Path(__file__).parent  # pode manter se usar para outras coisas

SEP = ";"
ENCODING = "latin1"


def detectar_colunas(df: pd.DataFrame):
    """
    Detecta colunas mínimas pra considerar que é um arquivo de votação de candidato/partido.
    Usado para arquivos como:
      - votacao_secao_XXXX_UF.csv
      - outros arquivos que trazem votos NOMINAIS ou de LEGENDA por seção/mun/zona.
    """

    # Coluna de votos
    vote_col = None
    for c in ["QT_VOTOS_NOMINAIS", "QT_VOTOS_NOMINAIS_VALIDOS", "QT_VOTOS", "QT_VOTOS_VALIDOS"]:
        if c in df.columns:
            vote_col = c
            break
    if vote_col is None:
        return None

    # Coluna de candidato (pode não existir em arquivo de partido)
    if "NM_CANDIDATO" in df.columns:
        cand_col = "NM_CANDIDATO"
    elif "NM_URNA_CANDIDATO" in df.columns:
        cand_col = "NM_URNA_CANDIDATO"
    else:
        cand_col = None

    # Coluna de partido
    if "SG_PARTIDO" in df.columns:
        party_col = "SG_PARTIDO"
    elif "NM_PARTIDO" in df.columns:
        party_col = "NM_PARTIDO"
    else:
        party_col = None

    zona_col = "NR_ZONA" if "NR_ZONA" in df.columns else None
    secao_col = "NR_SECAO" if "NR_SECAO" in df.columns else None

    return {
        "vote": vote_col,
        "cand": cand_col,
        "party": party_col,
        "zona": zona_col,
        "secao": secao_col,
    }


def extrair_ano_uf_do_arquivo(path: Path):
    """
    Extrai ano e UF do nome do arquivo, mesmo quando dividido:
      - votacao_candidato_munzona_2018_SP.csv
      - votacao_candidato_munzona_2018_SP_9.csv
      - votacao_candidato_munzona_2018_PB_PARTE2.csv
      - detalhe_votacao_secao_2022_SP.csv
      - votacao_secao_2024_AC.csv
    """
    nome = path.name.upper()
    ano = None
    uf = None

    # ano: primeiro 19xx ou 20xx
    m = re.search(r"(19|20)\d{2}", nome)
    if m:
        ano = m.group(0)

    # UF: aceita sufixo depois da UF (_SP_9.csv, _SP_PARTE2.csv, etc.)
    uf_pattern = (
        r"_(BRASIL|BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|"
        r"RS|RO|RR|SC|SP|SE|TO)(?:[_\.]|$)"
    )
    m = re.search(uf_pattern, nome)
    if m:
        uf = m.group(1)

    return ano, uf


def ler_csv_flex(path: Path) -> pd.DataFrame | None:
    """
    Lê um CSV usando a configuração padrão; se der erro de parsing,
    tenta novamente com engine='python' e on_bad_lines='skip'.
    """
    print(f"\n📄 Lendo: {path.name}")
    try:
        df = pd.read_csv(
            path,
            sep=SEP,
            encoding=ENCODING,
            dtype=str,
            low_memory=False,
        )
        return df
    except pd.errors.ParserError as e:
        print(f"   ⚠ Erro de parsing em {path.name}: {e}")
        print("   ⚠ Tentando novamente com engine='python' e ignorando linhas problemáticas (on_bad_lines='skip')...")
        try:
            df = pd.read_csv(
                path,
                sep=SEP,
                encoding=ENCODING,
                dtype=str,
                low_memory=False,
                engine="python",
                on_bad_lines="skip",
            )
            return df
        except Exception as e2:
            print(f"   ❌ Falha ao ler {path.name} mesmo com engine='python'. Arquivo será ignorado.")
            print(f"   ❌ Erro: {e2}")
            return None
    except Exception as e:
        print(f"   ❌ Erro inesperado ao ler {path.name}: {e}. Arquivo será ignorado.")
        return None


# ===============================================
# PROCESSAMENTO DOS ARQUIVOS DE VOTOS (SEÇÃO / ESCOLA)
# ===============================================

def processar_arquivo_votos(path: Path) -> pd.DataFrame | None:
    """
    Processa arquivos de votação por seção/escola (como votacao_secao_..., etc.)
    e retorna um DataFrame no formato da tabela 'votos'.

    IMPORTANTE:
    - NÃO é mais usado para arquivos VOTACAO_CANDIDATO_MUNZONA nem VOTACAO_PARTIDO_MUNZONA.
      Esses são tratados como METADADOS (candidatos_meta / partidos_meta).
    """
    df = ler_csv_flex(path)
    if df is None:
        return None

    # Limpa marcadores especiais
    df = df.replace({"#NULO": None, "#NE": None})

    cols = detectar_colunas(df)
    if cols is None:
        print("⚠ Não parece ser arquivo de votação de seção/candidato/partido. Pulando.")
        return None

    vote_col = cols["vote"]
    cand_col = cols["cand"]
    party_col = cols["party"]
    zona_col = cols["zona"]
    secao_col = cols["secao"]

    print(f"   → Coluna votos: {vote_col}")
    if cand_col:
        print(f"   → Coluna candidato: {cand_col}")
    if party_col:
        print(f"   → Coluna partido: {party_col}")
    if zona_col:
        print(f"   → Coluna zona: {zona_col}")
    if secao_col:
        print(f"   → Coluna seção: {secao_col}")

    # Converte votos para int
    df[vote_col] = pd.to_numeric(df[vote_col], errors="coerce").fillna(0).astype(int)

    # Ano e UF
    ano, uf_arquivo = extrair_ano_uf_do_arquivo(path)
    ano = ano or df.get("ANO_ELEICAO", pd.Series([None])).iloc[0]
    uf = uf_arquivo or df.get("SG_UF", pd.Series([None])).iloc[0]

    # Outras colunas opcionais
    turno = df["NR_TURNO"] if "NR_TURNO" in df.columns else None
    cd_municipio = df["CD_MUNICIPIO"] if "CD_MUNICIPIO" in df.columns else None
    nm_municipio = df["NM_MUNICIPIO"] if "NM_MUNICIPIO" in df.columns else None
    cd_cargo = df["CD_CARGO"] if "CD_CARGO" in df.columns else None
    ds_cargo = df["DS_CARGO"] if "DS_CARGO" in df.columns else None
    nr_candidato = df["NR_CANDIDATO"] if "NR_CANDIDATO" in df.columns else None

    # Status total no turno (quando existir)
    ds_sit_tot_turno = df["DS_SIT_TOT_TURNO"] if "DS_SIT_TOT_TURNO" in df.columns else None

    # 🔎 Identificação do local de votação (escola) - quando existir nesse arquivo
    cd_local = None
    nm_local = None
    ds_local_endereco = None

    for col in df.columns:
        up = col.upper()

        if cd_local is None and ("CD_LOCAL_VOT" in up or "NR_LOCAL_VOT" in up):
            cd_local = df[col]

        if nm_local is None and ("NM_LOCAL_VOT" in up or "DS_LOCAL_VOT" in up):
            nm_local = df[col]

        if ds_local_endereco is None and (
            "ENDERECO" in up and ("LOCAL_VOT" in up or "LOC_VOT" in up)
        ):
            ds_local_endereco = df[col]

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "nr_turno": turno,
        "cd_municipio": cd_municipio,
        "nm_municipio": nm_municipio,
        "cd_cargo": cd_cargo,
        "ds_cargo": ds_cargo,
        "nm_candidato": df[cand_col] if cand_col is not None else None,
        "nr_candidato": nr_candidato,
        "sg_partido": df[party_col] if party_col is not None else None,
        "nr_zona": df[zona_col] if zona_col else None,
        "nr_secao": df[secao_col] if secao_col else None,
        "cd_local_votacao": cd_local,
        "nm_local_votacao": nm_local,
        "ds_local_votacao_endereco": ds_local_endereco,
        "ds_sit_tot_turno": ds_sit_tot_turno,
        "votos": df[vote_col],
    }

    result = pd.DataFrame(base_cols)
    print(f"   → Registros processados (votos): {len(result)}")
    return result


# ========================================
# PROCESSAMENTO DOS ARQUIVOS DE DETALHE (SEÇÃO / ESCOLA / ENDEREÇO)
# ========================================

def processar_detalhe_secao(path: Path) -> pd.DataFrame | None:
    """
    Processa arquivos DETALHE_VOTACAO_SECAO_<ANO>_<UF>.csv

    Esses arquivos NÃO possuem votos por candidato, mas têm:
      - zona, seção
      - local de votação (escola)
      - endereço do local

    Aqui vamos gerar registros apenas para a tabela 'locais_secao'.
    """
    df = ler_csv_flex(path)
    if df is None:
        return None

    df = df.replace({"#NULO": None, "#NE": None})

    # Ano e UF
    ano, uf_arquivo = extrair_ano_uf_do_arquivo(path)
    ano = ano or df.get("ANO_ELEICAO", pd.Series([None])).iloc[0]
    uf = uf_arquivo or df.get("SG_UF", pd.Series([None])).iloc[0]

    cd_municipio = df["CD_MUNICIPIO"] if "CD_MUNICIPIO" in df.columns else None
    nm_municipio = df["NM_MUNICIPIO"] if "NM_MUNICIPIO" in df.columns else None
    nr_zona = df["NR_ZONA"] if "NR_ZONA" in df.columns else None
    nr_secao = df["NR_SECAO"] if "NR_SECAO" in df.columns else None

    nr_local_votacao = df["NR_LOCAL_VOTACAO"] if "NR_LOCAL_VOTACAO" in df.columns else None
    nm_local_votacao = df["NM_LOCAL_VOTACAO"] if "NM_LOCAL_VOTACAO" in df.columns else None
    ds_local_endereco = (
        df["DS_LOCAL_VOTACAO_ENDERECO"] if "DS_LOCAL_VOTACAO_ENDERECO" in df.columns else None
    )

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "cd_municipio": cd_municipio,
        "nm_municipio": nm_municipio,
        "nr_zona": nr_zona,
        "nr_secao": nr_secao,
        "nr_local_votacao": nr_local_votacao,
        "nm_local_votacao": nm_local_votacao,
        "ds_local_votacao_endereco": ds_local_endereco,
    }

    result = pd.DataFrame(base_cols)
    print(f"   → Registros processados (locais/secao): {len(result)}")
    return result


# ========================================
# PROCESSAMENTO DOS ARQUIVOS DE METADADOS DE CANDIDATOS
# ========================================

def processar_candidatos_meta(path: Path) -> pd.DataFrame | None:
    """
    Processa arquivos VOTACAO_CANDIDATO_MUNZONA_<ANO>_<UF>.csv

    Aqui NÃO vamos inserir votos na tabela 'votos'.
    Em vez disso, criamos uma tabela de metadados 'candidatos_meta' com:

      - ano, uf, cd_cargo, nr_turno
      - nr_candidato, nm_candidato (urna)
      - sg_partido, nm_partido
      - ds_sit_tot_turno (eleito, não eleito, suplente...)
      - outras situações (opcionais)

    A ideia é complementar os dados da tabela 'votos' sem duplicar contagem de votos.
    """
    df = ler_csv_flex(path)
    if df is None:
        return None

    df = df.replace({"#NULO": None, "#NE": None})

    # Ano e UF
    ano_arquivo, uf_arquivo = extrair_ano_uf_do_arquivo(path)
    ano = ano_arquivo or df.get("ANO_ELEICAO", pd.Series([None])).iloc[0]
    uf = uf_arquivo or df.get("SG_UF", pd.Series([None])).iloc[0]

    # Campos básicos
    cd_cargo = df["CD_CARGO"] if "CD_CARGO" in df.columns else None
    nr_turno = df["NR_TURNO"] if "NR_TURNO" in df.columns else None
    cd_municipio = df["CD_MUNICIPIO"] if "CD_MUNICIPIO" in df.columns else None
    nm_municipio = df["NM_MUNICIPIO"] if "NM_MUNICIPIO" in df.columns else None

    nr_candidato = df["NR_CANDIDATO"] if "NR_CANDIDATO" in df.columns else None

    # Nome do candidato: preferimos o nome de urna, se existir
    if "NM_URNA_CANDIDATO" in df.columns:
        nm_candidato = df["NM_URNA_CANDIDATO"]
    elif "NM_CANDIDATO" in df.columns:
        nm_candidato = df["NM_CANDIDATO"]
    else:
        nm_candidato = None

    sg_partido = df["SG_PARTIDO"] if "SG_PARTIDO" in df.columns else None
    nm_partido = df["NM_PARTIDO"] if "NM_PARTIDO" in df.columns else None

    ds_sit_tot_turno = df["DS_SIT_TOT_TURNO"] if "DS_SIT_TOT_TURNO" in df.columns else None

    # Situações adicionais (quando existirem)
    ds_situacao_candidatura = (
        df["DS_SITUACAO_CANDIDATURA"] if "DS_SITUACAO_CANDIDATURA" in df.columns else None
    )

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "cd_cargo": cd_cargo,
        "nr_turno": nr_turno,
        "cd_municipio": cd_municipio,
        "nm_municipio": nm_municipio,
        "nr_candidato": nr_candidato,
        "nm_candidato": nm_candidato,
        "sg_partido": sg_partido,
        "nm_partido": nm_partido,
        "ds_sit_tot_turno": ds_sit_tot_turno,
        "ds_situacao_candidatura": ds_situacao_candidatura,
    }

    result = pd.DataFrame(base_cols)

    # Remover duplicados por chave lógico-eleitoral
    subset = ["ano", "uf", "cd_cargo", "nr_turno", "nr_candidato"]
    subset = [c for c in subset if c in result.columns]
    if subset:
        before = len(result)
        result = result.drop_duplicates(subset=subset)
        print(f"   → Registros candidatos_meta (após deduplicar por {subset}): {len(result)} (antes: {before})")
    else:
        print(f"   → Registros candidatos_meta (sem chave de deduplicação definida): {len(result)}")

    return result


# ========================================
# PROCESSAMENTO DOS ARQUIVOS DE METADADOS DE PARTIDOS
# ========================================

def processar_partidos_meta(path: Path) -> pd.DataFrame | None:
    """
    Processa arquivos VOTACAO_PARTIDO_MUNZONA_<ANO>_<UF>.csv

    Aqui também não vamos inserir votos na tabela 'votos'. Vamos criar
    uma tabela de metadados 'partidos_meta', com:

      - ano, uf, cd_cargo, nr_turno
      - cd_municipio, nm_municipio
      - sg_partido, nm_partido
      - ds_sit_tot_turno
      - (opcional) algum agregado de votos de legenda
    """
    df = ler_csv_flex(path)
    if df is None:
        return None

    df = df.replace({"#NULO": None, "#NE": None})

    ano_arquivo, uf_arquivo = extrair_ano_uf_do_arquivo(path)
    ano = ano_arquivo or df.get("ANO_ELEICAO", pd.Series([None])).iloc[0]
    uf = uf_arquivo or df.get("SG_UF", pd.Series([None])).iloc[0]

    cd_cargo = df["CD_CARGO"] if "CD_CARGO" in df.columns else None
    nr_turno = df["NR_TURNO"] if "NR_TURNO" in df.columns else None
    cd_municipio = df["CD_MUNICIPIO"] if "CD_MUNICIPIO" in df.columns else None
    nm_municipio = df["NM_MUNICIPIO"] if "NM_MUNICIPIO" in df.columns else None

    sg_partido = df["SG_PARTIDO"] if "SG_PARTIDO" in df.columns else None
    nm_partido = df["NM_PARTIDO"] if "NM_PARTIDO" in df.columns else None

    ds_sit_tot_turno = df["DS_SIT_TOT_TURNO"] if "DS_SIT_TOT_TURNO" in df.columns else None

    # Exemplo de agregado de legenda (se existir)
    if "QT_TOTAL_VOTOS_LEG_VALIDOS" in df.columns:
        qt_total_votos_leg_validos = pd.to_numeric(
            df["QT_TOTAL_VOTOS_LEG_VALIDOS"], errors="coerce"
        )
    else:
        qt_total_votos_leg_validos = None

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "cd_cargo": cd_cargo,
        "nr_turno": nr_turno,
        "cd_municipio": cd_municipio,
        "nm_municipio": nm_municipio,
        "sg_partido": sg_partido,
        "nm_partido": nm_partido,
        "ds_sit_tot_turno": ds_sit_tot_turno,
        "qt_total_votos_leg_validos": qt_total_votos_leg_validos,
    }

    result = pd.DataFrame(base_cols)

    subset = ["ano", "uf", "cd_cargo", "nr_turno", "cd_municipio", "sg_partido"]
    subset = [c for c in subset if c in result.columns]
    if subset:
        before = len(result)
        result = result.drop_duplicates(subset=subset)
        print(f"   → Registros partidos_meta (após deduplicar por {subset}): {len(result)} (antes: {before})")
    else:
        print(f"   → Registros partidos_meta (sem chave de deduplicação definida): {len(result)}")

    return result


# ========================================
# ÍNDICES
# ========================================

def create_indexes(conn: sqlite3.Connection):
    """
    Cria índices na tabela 'votos' para acelerar as consultas mais comuns.
    Rodado ao final da ingestão.
    """
    print("⚙️  Criando índices na tabela 'votos'...")
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_ano_uf ON votos(ano, uf)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_cargo ON votos(ano, uf, cd_cargo)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_municipio ON votos(ano, uf, cd_municipio)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_partido ON votos(ano, uf, sg_partido)")
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_votos_mapa_locais
            ON votos(ano, uf, cd_municipio, cd_cargo, nr_turno, nr_candidato, cd_local_votacao)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_votos_zona_secao
            ON votos(ano, uf, cd_municipio, nr_zona, nr_secao)
            """
        )

        conn.commit()
        print("✅ Índices em 'votos' criados (ou já existiam).")
    except sqlite3.OperationalError as e:
        print(f"⚠ Não foi possível criar índices em 'votos': {e}")


def create_locais_indexes(conn: sqlite3.Connection):
    """
    Índices para a tabela 'locais_secao' (usada no mapa), quando existir.
    """
    print("⚙️  Criando índices na tabela 'locais_secao'...")
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_locais_ano_uf_mun_zona "
            "ON locais_secao(ano, uf, cd_municipio, nr_zona)"
        )
        conn.commit()
        print("✅ Índices em 'locais_secao' criados (ou já existiam).")
    except sqlite3.OperationalError:
        print("⚠ Tabela 'locais_secao' ainda não existe. Nenhum índice criado (ok se ainda não há DETALHE_VOTACAO_SECAO).")


def create_meta_indexes(conn: sqlite3.Connection):
    """
    Índices para tabelas de metadados (candidatos_meta, partidos_meta).
    """
    cur = conn.cursor()
    print("⚙️  Criando índices nas tabelas de metadados (candidatos_meta, partidos_meta)...")
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_candidatos_meta_chave "
            "ON candidatos_meta(ano, uf, cd_cargo, nr_turno, nr_candidato)"
        )
    except sqlite3.OperationalError:
        print("⚠ Tabela 'candidatos_meta' ainda não existe. Nenhum índice criado (ok se nenhum VOTACAO_CANDIDATO_MUNZONA foi carregado).")

    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_partidos_meta_chave "
            "ON partidos_meta(ano, uf, cd_cargo, nr_turno, cd_municipio, sg_partido)"
        )
    except sqlite3.OperationalError:
        print("⚠ Tabela 'partidos_meta' ainda não existe. Nenhum índice criado (ok se nenhum VOTACAO_PARTIDO_MUNZONA foi carregado).")

    conn.commit()
    print("✅ Índices de metadados criados (ou já existiam).")


# ========================================
# INGESTÃO GERAL
# ========================================

def ingest_all(clear_table: bool = True) -> int:
    """
    Lê todos os CSV no volume /app/dados_tse_volume e insere nas tabelas:
      - 'votos'            (arquivos de seção, ex: votacao_secao_...)
      - 'locais_secao'     (arquivos DETALHE_VOTACAO_SECAO_...)
      - 'candidatos_meta'  (arquivos VOTACAO_CANDIDATO_MUNZONA_...)
      - 'partidos_meta'    (arquivos VOTACAO_PARTIDO_MUNZONA_...)

    Se clear_table=True, derruba e recria as tabelas.
    """
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()

    if clear_table:
        print("\n🗑 Limpando tabelas 'votos', 'locais_secao', 'candidatos_meta' e 'partidos_meta' (DROP TABLE IF EXISTS)...")
        cur.execute("DROP TABLE IF EXISTS votos")
        cur.execute("DROP TABLE IF EXISTS locais_secao")
        cur.execute("DROP TABLE IF EXISTS candidatos_meta")
        cur.execute("DROP TABLE IF EXISTS partidos_meta")
        conn.commit()

    total_votos = 0
    total_locais = 0
    total_candidatos_meta = 0
    total_partidos_meta = 0

    if not DATA_DIR.exists():
        print(f"❌ Pasta de dados não encontrada: {DATA_DIR}. Pulando.")
        conn.close()
        return 0

    print(f"📁 Iniciando ingestão a partir de: {DATA_DIR}")
    arquivos = sorted(DATA_DIR.glob("*.csv"))

    if not arquivos:
        print("⚠ Nenhum arquivo CSV encontrado no volume.")
        conn.close()
        return 0

    for csv_path in arquivos:
        nome = csv_path.name
        nome_upper = nome.upper()
        print(f"\n📦 Arquivo detectado no volume: {nome} (upper={nome_upper})")

        # 1) Arquivos de DETALHE_VOTACAO_SECAO -> 'locais_secao'
        if "DETALHE_VOTACAO_SECAO" in nome_upper:
            print(f"➡ Classificado como DETALHE_VOTACAO_SECAO (locais_secao): {nome}")
            df_locais = processar_detalhe_secao(csv_path)
            if df_locais is not None and not df_locais.empty:
                df_locais.to_sql("locais_secao", conn, if_exists="append", index=False)
                total_locais += len(df_locais)
                print("   ✔ Inserido na tabela 'locais_secao'.")
            else:
                print("   ⚠ Nenhum registro gerado para 'locais_secao' a partir deste arquivo.")
            continue

        # 2) Arquivos de METADADOS DE CANDIDATOS: qualquer coisa com CANDIDATO e MUNZONA
        if "CANDIDATO" in nome_upper and "MUNZONA" in nome_upper:
            print(f"➡ Classificado como METADADOS DE CANDIDATOS (candidatos_meta): {nome}")
            df_cand = processar_candidatos_meta(csv_path)
            if df_cand is not None and not df_cand.empty:
                df_cand.to_sql("candidatos_meta", conn, if_exists="append", index=False)
                total_candidatos_meta += len(df_cand)
                print("   ✔ Inserido na tabela 'candidatos_meta'.")
            else:
                print("   ⚠ Nenhum registro gerado para 'candidatos_meta' a partir deste arquivo.")
            continue

        # 3) Arquivos de METADADOS DE PARTIDOS: qualquer coisa com PARTIDO e MUNZONA
        if "PARTIDO" in nome_upper and "MUNZONA" in nome_upper:
            print(f"➡ Classificado como METADADOS DE PARTIDOS (partidos_meta): {nome}")
            df_part = processar_partidos_meta(csv_path)
            if df_part is not None and not df_part.empty:
                df_part.to_sql("partidos_meta", conn, if_exists="append", index=False)
                total_partidos_meta += len(df_part)
                print("   ✔ Inserido na tabela 'partidos_meta'.")
            else:
                print("   ⚠ Nenhum registro gerado para 'partidos_meta' a partir deste arquivo.")
            continue

        # 4) Demais arquivos com votos (seção/candidato) -> 'votos'
        print(f"➡ Classificado como ARQUIVO DE VOTOS (votos): {nome}")
        df_votos = processar_arquivo_votos(csv_path)
        if df_votos is not None and not df_votos.empty:
            df_votos.to_sql("votos", conn, if_exists="append", index=False)
            total_votos += len(df_votos)
            print("   ✔ Inserido na tabela 'votos'.")
        else:
            print("   ⚠ Nenhum registro de votos gerado a partir deste arquivo.")

    print(f"✅ Ingestão concluída. Registros inseridos em 'votos': {total_votos}")
    print(f"✅ Ingestão concluída. Registros inseridos em 'locais_secao': {total_locais}")
    print(f"✅ Ingestão concluída. Registros inseridos em 'candidatos_meta': {total_candidatos_meta}")
    print(f"✅ Ingestão concluída. Registros inseridos em 'partidos_meta': {total_partidos_meta}")

    # Índices
    create_indexes(conn)
    create_locais_indexes(conn)
    create_meta_indexes(conn)

    conn.close()
    return total_votos
