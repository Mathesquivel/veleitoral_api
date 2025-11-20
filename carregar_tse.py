import pandas as pd
import sqlite3
from pathlib import Path
import re

# ==========================================
# CONFIGURAÇÃO
# ==========================================

# Pasta onde estão TODOS os CSV do TSE
DATA_DIR = Path(__file__).parent / "dados_tse"

# Nome do banco SQLite
DB_PATH = Path(__file__).parent / "tse_eleicoes.db"

SEP = ";"
ENCODING = "latin1"


# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def detectar_colunas(df: pd.DataFrame):
    """Detecta colunas de candidato, partido e votos de forma automática."""
    # Coluna de votos
    vote_col = None
    for c in ["QT_VOTOS_NOMINAIS", "QT_VOTOS_NOMINAIS_VALIDOS", "QT_VOTOS"]:
        if c in df.columns:
            vote_col = c
            break
    if vote_col is None:
        return None  # não é um arquivo de votação de candidato

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

    # Zona / seção (podem faltar)
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
    Ex: votacao_candidato_munzona_2024_SP.csv
    """
    nome = path.name.upper()
    ano = None
    uf = None

    m = re.search(r"20\d{2}", nome)
    if m:
        ano = m.group(0)

    m = re.search(r"_(BRASIL|BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\.", nome)
    if m:
        uf = m.group(1)

    return ano, uf


# ==========================================
# PROCESSO
# ==========================================

def processar_arquivo(path: Path) -> pd.DataFrame | None:
    print(f"\n📄 Lendo: {path.name}")
    df = pd.read_csv(path, sep=SEP, encoding=ENCODING, dtype=str)

    # Limpa marcadores especiais
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

    ano, uf_arquivo = extrair_ano_uf_do_arquivo(path)
    ano = ano or df.get("ANO_ELEICAO", pd.Series([None])).iloc[0]
    uf = uf_arquivo or df.get("SG_UF", pd.Series([None])).iloc[0]

    base_cols = {
        "arquivo_origem": path.name,
        "ano": ano,
        "uf": uf,
        "nm_candidato": df[cand_col],
        "sg_partido": df[party_col],
        "votos": df[vote_col],
    }

    if zona_col:
        base_cols["nr_zona"] = df[zona_col]
    else:
        base_cols["nr_zona"] = None

    if secao_col:
        base_cols["nr_secao"] = df[secao_col]
    else:
        base_cols["nr_secao"] = None

    result = pd.DataFrame(base_cols)
    print(f"   → Registros processados: {len(result)}")
    return result


def main():
    # Conecta / cria o banco
    conn = sqlite3.connect(DB_PATH)
    print(f"\n🔌 Conectado ao banco: {DB_PATH}")

    todas_linhas = []

    # Percorre todos os CSV da pasta
    if not DATA_DIR.exists():
        print(f"❌ Pasta de dados não encontrada: {DATA_DIR}")
        return

    for csv_path in sorted(DATA_DIR.glob("*.csv")):
        df_proc = processar_arquivo(csv_path)
        if df_proc is not None and not df_proc.empty:
            # Grava direto no banco
            df_proc.to_sql("votos", conn, if_exists="append", index=False)
            todas_linhas.append(len(df_proc))
            print("   ✔ Inserido na tabela 'votos'.")

    conn.close()

    total = sum(todas_linhas)
    print("\n✅ PROCESSO FINALIZADO.")
    print(f"   Total de registros inseridos na tabela 'votos': {total}")
    print(f"   Banco gerado em: {DB_PATH}")


if __name__ == "__main__":
    main()
