"""Exemplo 4 — Configurando memory_limit e spill para disco (temp_directory).

Conceitos:
- Por padrão o DuckDB usa até ~80% da RAM da máquina. `SET memory_limit='150MB'`
  reduz drasticamente esse teto — bem menor que os ~265MB do dataset de orders
  (33.7M linhas) já compactado em parquet, e bem menor ainda que o tamanho
  descomprimido em memória.
- `SET temp_directory='...'` diz ao DuckDB onde gravar os buffers que não
  cabem no `memory_limit`. Um `ORDER BY` sobre a tabela inteira precisa manter
  (numa primeira leitura) todas as linhas para ordenar — se isso não cabe na
  RAM configurada, o DuckDB grava blocos intermediários em disco (spill) em
  vez de falhar com erro de memória.
- Sem `temp_directory` configurado (ou com `memory_limit` alto o bastante), a
  mesma operação simplesmente roda inteira em RAM.

Rode com: `uv run examples/04_memory_limit_and_spill.py`
"""

import shutil
from pathlib import Path

import duckdb

from _common import ORDERS_GLOB, section

TMP_SPILL_DIR = Path(__file__).resolve().parent / "_tmp_spill"


if __name__ == "__main__":
    TMP_SPILL_DIR.mkdir(exist_ok=True)
    con = duckdb.connect()

    section("Configurando um teto de memória bem menor que o dataset")
    con.execute("SET memory_limit='150MB'")
    con.execute(f"SET temp_directory='{TMP_SPILL_DIR}'")
    # Sem ordem de inserção preservada, o DuckDB tem mais liberdade para
    # paralelizar/spillar sem precisar manter a ordem original das linhas.
    con.execute("SET preserve_insertion_order=false")
    print(con.sql("SELECT current_setting('memory_limit'), current_setting('temp_directory')").fetchone())

    section("ORDER BY sobre as ~33.7M linhas de orders (não cabe nos 150MB configurados)")
    query = f"""
        SELECT customer_id, product_id, quantity
        FROM read_parquet('{ORDERS_GLOB}')
        ORDER BY quantity DESC, customer_id
    """
    relation = con.sql(query)
    primeira_linha = relation.fetchone()
    print(f"query concluída sem erro de memória; primeira linha do resultado ordenado: {primeira_linha}")

    section("Arquivos de spill gravados em temp_directory durante a execução")
    spill_files = list(TMP_SPILL_DIR.iterdir())
    if spill_files:
        for f in spill_files:
            print(f"{f.name}: {f.stat().st_size / (1024 * 1024):.1f}MB")
    else:
        print("nenhum arquivo de spill encontrado (a conexão já pode ter liberado os buffers)")

    section("Comparando com memory_limit alto: a mesma query roda só em RAM")
    con.execute("SET memory_limit='4GB'")
    con.sql(query).fetchone()
    print("com memory_limit alto, o DuckDB evita tocar disco sempre que possível")

    con.close()
    shutil.rmtree(TMP_SPILL_DIR, ignore_errors=True)
