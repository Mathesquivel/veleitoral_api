import pandas as pd
import sqlite3
from pathlib import Path
import re

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path(__file__).parent

# Pasta com CSV menores, versionados no git
DATA_DIR_REPO = BASE_DIR / "dados_tse"

# Pasta do volume (Railway) com CSV grandes (>= 100MB)
DATA_DIR_VOLUME = Path("/app/dados_tse_volume")

DB_PATH = BASE_DIR / "tse_eleicoes.db"

SEP = ";"
ENCODING = "latin1"


def detectar_colunas(df: pd.DataFrame):
    """
    Detecta colunas mínimas pra considerar que é um arquivo de votação de candidato.
    """
    # Coluna de votos
    vote_col = None
    for c in ["QT_VOTOS_NOMINAIS", "QT_VOTOS_NOMINAIS_VALIDOS", "QT_VOTOS"]:
        if c in df.columns:
            vote_col = c
            break
    if vote_col is None:
        return None

    # Coluna de candidato
    if "NM_CANDIDATO" in df.columns:
        cand_col = "NM_CANDIDATO"
    elif "NM_URNA_CANDIDATO" in df.columns:
        cand_col = "NM_URNA_CANDIDATO"
    else:
        return None

    # Coluna de partido
    if "SG_PARTIDO" in df.columns:
        party_col = "SG_PARTIDO"
    elif "NM_PARTIDO" in df.columns:
        party_col = "NM_PARTIDO"
    else:
        return None

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
    Tenta extrair ano e UF a partir do nome do arquivo.

    Funciona para nomes como:
      - votacao_candidato_munzona_2022_SP.csv
      - votacao_candidato_munzona_2022_SP_1.csv
      - votacao_candidato_munzona_2022_PB_PARTE2.csv
    """
    nome = path.name.upper()
    ano = None
    uf = None

    # ano: pega o primeiro 19xx ou 20xx que aparecer
    m = re.search(r"(19|20)\d{2}", nome)
    if m:
        ano = m.group(0)

    # UF: aceita padrões com sufixo depois da UF (ex: _SP_1.CSV, _SP_PARTE1.CSV)
    uf_pattern = (
        r"_(BRASIL|BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|"
        r"RS|RO|RR|SC|SP|SE|TO)(?:[_\.]|$)"
    )
    m = re.search(uf_pattern, nome)
    if m:
        uf = m.group(1)

    return ano, uf


def processar_arquivo(path: Path) -> pd.DataFrame | None:
    print(f"\n📄 Lendo: {path}")
    df = pd.read_csv(path, sep=SEP, encoding=ENCODING, dtype=str)
    df = df.replace({"#NULO": None, "#NE": None})

    cols = detectar_colunas(df)
    if cols is None:
        print("⚠ Não parece ser arquivo de votação de candidato. Pulando.")
        return None

    vote_col = cols["vote"]
    cand_col = cols["cand"]
    party_col = cols["party"]
    zona_col = cols["zona"]
    secao_col = cols["secao"]

    print(f"   → Coluna votos: {vote_col}")
    print(f"   → Coluna candidato: {cand_col}")
    print(f"   → Coluna partido: {party_col}")
    if zona_col:
        print(f"   → Coluna zona: {zona_col}")
    if secao_col:
        print(f"   → Coluna seção: {secao_col}")

    # Converte votos para int
    df[vote_col] = df[vote_col].astype(float).fillna(0).astype(int)

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

    # Possíveis colunas de local de votação (dependem do layout)
    cd_local = None
    nm_local = None
    for col in df.columns:
        upper = col.upper()
        if cd_local is None and ("CD_LOCAL_VOT" in upper or "NR_LOCAL_VOT" in upper):
            cd_local = df[col]
        if nm_local is None and ("NM_LOCAL_VOT" in upper or "DS_LOCAL_VOT" in upper):
            nm_local = df[col]

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "nr_turno": turno,
        "cd_municipio": cd_municipio,
        "nm_municipio": nm_municipio,
        "cd_cargo": cd_cargo,
        "ds_cargo": ds_cargo,
        "nm_candidato": df[cand_col],
        "nr_candidato": nr_candidato,
        "sg_partido": df[party_col],
        "nr_zona": df[zona_col] if zona_col else None,
        "nr_secao": df[secao_col] if secao_col else None,
        "cd_local_votacao": cd_local,
        "nm_local_votacao": nm_local,
        "votos": df[vote_col],
    }

    result = pd.DataFrame(base_cols)
    print(f"   → Registros processados: {len(result)}")
    return result


def _ingest_from_dir(dir_path: Path, conn: sqlite3.Connection) -> int:
    """
    Lê todos os CSV em um diretório e insere na tabela 'votos'.
    Retorna o total de registros inseridos.
    """
    if not dir_path.exists():
        print(f"📁 Pasta de dados não encontrada: {dir_path}. Pulando.")
        return 0

    print(f"\n📁 Iniciando ingestão a partir de: {dir_path}")
    total = 0

    for csv_path in sorted(dir_path.glob("*.csv")):
        df_proc = processar_arquivo(csv_path)
        if df_proc is not None and not df_proc.empty:
            df_proc.to_sql("votos", conn, if_exists="append", index=False)
            total += len(df_proc)
            print("   ✔ Inserido na tabela 'votos'.")

    print(f"✅ Concluído diretório {dir_path}. Registros inseridos: {total}")
    return total


def ingest_all(clear_table: bool = True) -> int:
    """
    Lê todos os CSV em:
    - DATA_DIR_REPO   (./dados_tse, arquivos menores, versionados no git)
    - DATA_DIR_VOLUME (/app/dados_tse_volume, arquivos grandes, no volume)

    e insere na tabela 'votos'.
    Se clear_table=True, apaga a tabela antes (drop + recria com novo esquema).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if clear_table:
        print("\n🗑  Limpando tabela 'votos' (DROP TABLE IF EXISTS)...")
        cur.execute("DROP TABLE IF EXISTS votos")
        conn.commit()

    total = 0

    # Ingestão dos arquivos menores (repo)
    total += _ingest_from_dir(DATA_DIR_REPO, conn)

    # Ingestão dos arquivos grandes (volume)
    total += _ingest_from_dir(DATA_DIR_VOLUME, conn)

    conn.close()
    print(f"\n✅ Ingestão concluída. Registros inseridos (total): {total}")
    return total
