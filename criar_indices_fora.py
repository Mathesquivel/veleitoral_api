import sqlite3
from pathlib import Path

DB_PATH = Path("/app/dados_tse_volume/tse_eleicoes.db")

def main():
    print("🔧 Abrindo banco:", DB_PATH)

    conn = sqlite3.connect(DB_PATH, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 60000;")

    cur = conn.cursor()

    print("\n⚙️ Criando índices principais...")

    indices = [
        "CREATE INDEX IF NOT EXISTS idx_votos_ano_uf ON votos(ano, uf);",
        "CREATE INDEX IF NOT EXISTS idx_votos_municipio ON votos(ano, uf, cd_municipio);",
        "CREATE INDEX IF NOT EXISTS idx_votos_candidato ON votos(ano, uf, cd_cargo, nr_turno, nr_candidato);",
        "CREATE INDEX IF NOT EXISTS idx_votos_local ON votos(ano, uf, cd_municipio, nr_zona, nr_secao);",
        "CREATE INDEX IF NOT EXISTS idx_votos_escola ON votos(ano, uf, cd_municipio, cd_local_votacao);"
    ]

    for sql in indices:
        try:
            print("→", sql)
            cur.execute(sql)
        except Exception as e:
            print("   ❌ Erro:", e)

    conn.commit()
    conn.close()
    print("\n✅ Índices criados com sucesso!")

if __name__ == "__main__":
    main()
