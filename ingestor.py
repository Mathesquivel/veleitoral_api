import pandas as pd
import sqlite3
from pathlib import Path
import re

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path(__file__).parent

# Agora a ingestão lê APENAS do volume do Railway
DATA_DIR = Path("/app/dados_tse_volume")

DB_PATH = BASE_DIR / "tse_eleicoes.db"

SEP = ";"
ENCODING = "latin1"


def detectar_colunas(df: pd.DataFrame):
    """
    Detecta colunas mínimas pra considerar que é um arquivo de votação de candidato/partido.
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


def processar_arquivo(path: Path) -> pd.DataFrame | None:
    print(f"\n📄 Lendo: {path.name}")

    # 1) Tenta ler normalmente
    try:
        df = pd.read_csv(
            path,
            sep=SEP,
            encoding=ENCODING,
            dtype=str,
            low_memory=False,
        )
    except pd.errors.ParserError as e:
        # Erro de parsing (por exemplo: EOF inside string) → tenta modo mais tolerante
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
                on_bad_lines="skip",  # pandas >= 1.3
            )
        except Exception as e2:
            print(f"   ❌ Falha ao ler {path.name} mesmo com engine='python'. Arquivo será ignorado.")
            print(f"   ❌ Erro: {e2}")
            return None
    except Exception as e:
        print(f"   ❌ Erro inesperado ao ler {path.name}: {e}. Arquivo será ignorado.")
        return None

    # Limpa marcadores especiais
    df = df.replace({"#NULO": None, "#NE": None})

    cols = detectar_colunas(df)
    if cols is None:
        print("⚠ Não parece ser arquivo de votação de candidato/partido. Pulando.")
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
        "nm_candidato": df[cand_col] if cand_col is not None else None,
        "nr_candidato": nr_candidato,
        "sg_partido": df[party_col] if party_col is not None else None,
        "nr_zona": df[zona_col] if zona_col else None,
        "nr_secao": df[secao_col] if secao_col else None,
        "cd_local_votacao": cd_local,
        "nm_local_votacao": nm_local,
        "votos": df[vote_col],
    }

    result = pd.DataFrame(base_cols)
    print(f"   → Registros processados: {len(result)}")
    return result


def create_indexes(conn: sqlite3.Connection):
    """
    Cria índices na tabela 'votos' para acelerar as consultas mais comuns.
    Rodado ao final da ingestão.
    """
    print("⚙️  Criando índices na tabela 'votos'...")
    cur = conn.cursor()
    # Índices para filtros mais usados (ano, uf, cargo, município, partido)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_ano_uf ON votos(ano, uf)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_cargo ON votos(ano, uf, cd_cargo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_municipio ON votos(ano, uf, cd_municipio)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_votos_partido ON votos(ano, uf, sg_partido)")
    conn.commit()
    print("✅ Índices criados (ou já existiam).")


def ingest_all(clear_table: bool = True) -> int:
    """
    Lê todos os CSV no volume /app/dados_tse_volume e insere na tabela 'votos'.
    Se clear_table=True, derruba e recria a tabela 'votos'.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if clear_table:
        print("\n🗑 Limpando tabela 'votos' (DROP TABLE IF EXISTS)...")
        cur.execute("DROP TABLE IF EXISTS votos")
        conn.commit()

    total = 0

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
        df_proc = processar_arquivo(csv_path)
        if df_proc is not None and not df_proc.empty:
            df_proc.to_sql("votos", conn, if_exists="append", index=False)
            total += len(df_proc)
            print("   ✔ Inserido na tabela 'votos'.")

    print(f"✅ Ingestão concluída. Registros inseridos (total): {total}")

    # ✅ Cria índices ao final para acelerar consultas
    create_indexes(conn)

    conn.close()
    return total
