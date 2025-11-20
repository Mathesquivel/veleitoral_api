import pandas as pd
from pathlib import Path

# ================================
# CONFIGURAÇÃO DO ARQUIVO
# ================================
# 👉 deixe APENAS O NOME do arquivo aqui
NOME_ARQUIVO = "votacao_candidato_munzona_2024_SP.csv"

CAMINHO_ARQUIVO = Path(__file__).parent / NOME_ARQUIVO

SEP = ';'
ENCODING = 'latin1'


# ================================
# LEITURA DO CSV
# ================================
print(f"\n📄 Lendo arquivo: {CAMINHO_ARQUIVO}\n")

df = pd.read_csv(CAMINHO_ARQUIVO, sep=SEP, encoding=ENCODING, dtype=str)

print("🔎 Colunas encontradas:")
print(list(df.columns))


# ================================
# LIMPEZA DE DADOS
# ================================
df = df.replace({"#NULO": None, "#NE": None})


# ================================
# DETECTAR COLUNA DE VOTOS
# ================================
CAND_COL = None
PART_COL = None
VOTE_COL = None

possiveis_votos = ["QT_VOTOS_NOMINAIS", "QT_VOTOS", "QT_VOTOS_NOMINAIS_VALIDOS"]

for c in possiveis_votos:
    if c in df.columns:
        VOTE_COL = c
        break

if VOTE_COL is None:
    raise ValueError("❌ Nenhuma coluna de votos encontrada no arquivo do TSE.")

print(f"\n🟢 Coluna de votos detectada automaticamente: {VOTE_COL}")


# ================================
# DETECTAR COLUNAS DE CANDIDATO E PARTIDO
# ================================
if "NM_CANDIDATO" in df.columns:
    CAND_COL = "NM_CANDIDATO"
elif "NM_URNA_CANDIDATO" in df.columns:
    CAND_COL = "NM_URNA_CANDIDATO"

if "SG_PARTIDO" in df.columns:
    PART_COL = "SG_PARTIDO"
elif "NM_PARTIDO" in df.columns:
    PART_COL = "NM_PARTIDO"

if CAND_COL is None:
    raise ValueError("❌ Não encontrei coluna de nome de candidato.")

if PART_COL is None:
    raise ValueError("❌ Não encontrei coluna de partido.")

print(f"🟢 Coluna de candidato: {CAND_COL}")
print(f"🟢 Coluna de partido: {PART_COL}")


# Converter votos para número
df[VOTE_COL] = df[VOTE_COL].astype(float).fillna(0).astype(int)


# ================================
# GERAR VOTOS TOTAIS
# ================================
print("\n📊 Gerando votos totais por candidato...")

votos_totais = (
    df.groupby([CAND_COL, PART_COL], as_index=False)[VOTE_COL]
      .sum()
      .rename(columns={VOTE_COL: "TOTAL_VOTOS"})
)

votos_totais.to_csv("AUTO_votos_totais_por_candidato.csv", sep=';', index=False)

print("✔ Arquivo gerado: AUTO_votos_totais_por_candidato.csv")


# ================================
# GERAR VOTOS POR ZONA E SEÇÃO (AUTOMÁTICO)
# ================================

agrupamento = [CAND_COL, PART_COL]

if "NR_ZONA" in df.columns:
    agrupamento.append("NR_ZONA")
    print("🟢 Detected: NR_ZONA")
else:
    print("⚠ NR_ZONA não encontrada. Votos serão apenas gerais.")

if "NR_SECAO" in df.columns:
    agrupamento.append("NR_SECAO")
    print("🟢 Detected: NR_SECAO")
else:
    print("⚠ NR_SECAO não encontrada (layout MUNZONA), agrupando só por zona.")


print("\n📊 Gerando votos por localização (zona/seção se existir)...")

votos_local = (
    df.groupby(agrupamento, as_index=False)[VOTE_COL]
      .sum()
      .rename(columns={VOTE_COL: "VOTOS"})
)

votos_local.to_csv("AUTO_votos_localizacao.csv", sep=';', index=False)

print("✔ Arquivo gerado: AUTO_votos_localizacao.csv")


# ================================
# FINAL
# ================================
print("\n🎉 PROCESSO CONCLUÍDO!")
print("Arquivos criados:")
print(" - AUTO_votos_totais_por_candidato.csv")
print(" - AUTO_votos_localizacao.csv\n")
