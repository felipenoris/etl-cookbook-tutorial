"""Exemplo 2 — Lendo parquet particionado (hive) e observando partition pruning.

Conceitos:
- `hive_partitioning=true` faz o DuckDB reconstruir `order_year`/`order_month`
  a partir do caminho `order_year=2025/order_month=01/...`.
- `EXPLAIN` mostra o plano físico da query — quando o filtro usa uma coluna de
  partição, o scan do parquet aparece restrito a menos arquivos.
- `glob()` do próprio DuckDB para inspecionar quais arquivos batem com o
  padrão antes de ler.

Rode com: `uv run examples/02_reading_partitioned_parquet.py`
"""

import duckdb

from _common import ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()

    section("Arquivos descobertos pelo glob")
    for row in con.sql(f"SELECT * FROM glob('{ORDERS_GLOB}')").fetchall():
        print(row[0])

    section("Leitura com hive_partitioning=true: colunas de partição aparecem no schema")
    con.sql(
        f"SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) LIMIT 3"
    ).show()

    section("EXPLAIN sem filtro: teria que varrer as 6 partições")
    plano_completo = con.sql(
        f"""
        EXPLAIN SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        """
    ).fetchall()
    print(plano_completo[0][1])

    section("EXPLAIN filtrando por order_month=1: só a partição de janeiro é lida")
    plano_filtrado = con.sql(
        f"""
        EXPLAIN SELECT COUNT(*)
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchall()
    print(plano_filtrado[0][1])

    section("Confirmando o resultado do filtro por partição")
    total_janeiro = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchone()
    print(f"pedidos em janeiro/2025: {total_janeiro[0]:,}")
