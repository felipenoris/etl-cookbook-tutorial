"""Exemplo 1 — Conectando e consultando com DuckDB.

Conceitos:
- `duckdb.connect()` sem argumento abre um banco em memória (equivalente a
  `duckdb.connect(':memory:')`).
- `con.sql(...)` devolve uma `DuckDBPyRelation` *lazy* — nada é executado até
  você pedir o resultado (`.show()`, `.fetchall()`, `.df()`, `.arrow()`).
- `con.execute(...)` segue o estilo DB-API (cursor), bom para comandos sem
  retorno tabular como `SET`/`PRAGMA`/`CREATE`.
- DuckDB lê parquet diretamente com `read_parquet(glob)`, sem precisar
  declarar uma tabela antes.

Rode com: `uv run examples/01_connecting_and_querying.py`
"""

import duckdb

from _common import ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()  # banco em memória, some ao fim do processo

    section("con.sql(): retorna uma Relation lazy")
    relation = con.sql(f"SELECT * FROM read_parquet('{ORDERS_GLOB}') LIMIT 5")
    print(type(relation))
    relation.show()

    section("Materializando a Relation como lista de tuplas")
    linhas = relation.fetchall()
    print(linhas[:2])

    section("con.execute(): estilo DB-API (cursor), útil para comandos")
    con.execute("SET threads = 4")
    resultado = con.execute("SELECT current_setting('threads')").fetchone()
    print(f"threads configuradas: {resultado[0]}")

    section("Consulta agregada direto sobre o glob de parquet")
    con.sql(
        f"""
        SELECT status, COUNT(*) AS total, AVG(quantity) AS qtd_media
        FROM read_parquet('{ORDERS_GLOB}')
        GROUP BY status
        ORDER BY total DESC
        """
    ).show()

    section("Registrando a query como view reutilizável")
    con.execute(f"CREATE VIEW orders AS SELECT * FROM read_parquet('{ORDERS_GLOB}')")
    print(con.sql("SELECT COUNT(*) AS total_orders FROM orders").fetchone())
