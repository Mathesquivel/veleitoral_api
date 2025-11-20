import pandas as pd
from pathlib import Path

# ==========================================
# CONFIGURAÇÕES
# ==========================================

# Nome exato do arquivo CSV do TSE
NOME_ARQUIVO = "votacao_candidato_munzona_2024_SP.csv"

# Caminho completo do arquivo (mesma pasta do script)
CAMINHO_ARQUIVO = Path(__file__).parent / NOME_ARQUIVO

SEP = ';'           # Separador usado pelo TSE
ENCODING = 'latin1' # Conforme LEIAME do TSE


# ==========================================
# LEITURA DO CSV
# ==========================================

print(f"Lendo arquivo: {CAMINHO_ARQUIVO}")

df = pd.read_csv(CAMINHO_ARQUIVO, sep=SEP, encoding=ENCODING, dtype=str)

print("\nColunas encontradas no arquivo:")
print(list(df.columns))

# Detecta qual coluna de votos existe
if "QT_VOTOS_NOMINAIS" in df.columns:
    COL_VOTOS = "QT_VOTOS_NOMINAIS"
elif "QT_VOTOS" in df.columns:
    COL_VOTOS = "QT_VOTOS"
else:
    raise ValueError(
        "Não encontrei a coluna de votos (QT_VOTOS_NOMINAIS ou QT_VOTOS). "
        "Confira as colunas listadas acima."
    )

# Converte votos para número
df[COL_VOTOS] = df[COL_VOTOS].astype(int)


# ==========================================
# 1) VOTOS TOTAIS POR CANDIDATO
# ==========================================

# Colunas obrigatórias
COLS_BASE = ["NM_CANDIDATO", "SG_PARTIDO"]

for col in COLS_BASE:
    if col not in df.columns:
        raise ValueError(f"Coluna obrigatória não encontrada no CSV: {col}")

votos_totais = (
    df.groupby(COLS_BASE, as_index=False)[COL_VOTOS]
      .sum()
      .rename(columns={COL_VOTOS: "TOTAL_VOTOS"})
)

print("\nVOTOS TOTAIS POR CANDIDATO (amostra):")
print(votos_totais.head())

votos_totais.to_csv("votos_totais_por_candidato.csv", sep=';', index=False)


# ==========================================
# 2) VOTOS POR CANDIDATO / ZONA / SEÇÃO
# ==========================================

agrup = ["NM_CANDIDATO", "SG_PARTIDO"]

# Zona eleitoral
if "NR_ZONA" in df.columns:
    agrup.append("NR_ZONA")
else:
    print("\n⚠ Atenção: coluna NR_ZONA não encontrada, não será usada no agrupamento.")

# Seção eleitoral (alguns layouts não têm)
if "NR_SECAO" in df.columns:
    agrup.append("NR_SECAO")
else:
    print("⚠ Atenção: coluna NR_SECAO não encontrada, vou agrupar só até nível de ZONA.")

votos_zona_secao = (
    df.groupby(agrup, as_index=False)[COL_VOTOS]
      .sum()
      .rename(columns={COL_VOTOS: "VOTOS"})
)

print("\nVOTOS POR CANDIDATO / ZONA / SEÇÃO (amostra):")
print(votos_zona_secao.head())

votos_zona_secao.to_csv("votos_por_zona_secao.csv", sep=';', index=False)


# ==========================================
print("\n✅ Arquivos gerados nesta pasta:")
print(" - votos_totais_por_candidato.csv")
print(" - votos_por_zona_secao.csv")
