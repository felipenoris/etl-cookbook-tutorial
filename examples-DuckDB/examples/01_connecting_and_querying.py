"""Exemplo 1 — Conectando e consultando com DuckDB.

Para quem vem de bases transacionais (Postgres/MySQL/SQL Server), o DuckDB
inverte várias premissas — vale nomeá-las logo no primeiro exemplo:

- **Não há servidor.** O banco roda *embutido* no processo Python (como o
  SQLite), sem conexão de rede, usuário ou senha. `duckdb.connect()` sem
  argumento abre um banco em memória (equivalente a
  `duckdb.connect(':memory:')`), que desaparece no fim do processo.
- **É colunar e vetorizado (OLAP), não orientado a linhas (OLTP).** O motor
  foi desenhado para varrer/agregar milhões de linhas por segundo, não para
  milhares de pequenas transações concorrentes. Por isso ele é ideal para
  ETL analítico e ruim como banco de aplicação.
- **Consulta arquivos diretamente.** `SELECT ... FROM read_parquet(glob)`
  roda SQL direto sobre os arquivos parquet, sem `CREATE TABLE` + carga
  prévia. Num banco tradicional, dados externos exigiriam import; aqui o
  arquivo É a tabela ("schema-on-read").

Sobre a API Python:
- `con.sql(...)` devolve uma `DuckDBPyRelation` **lazy** — nada é executado
  até você pedir o resultado (`.show()`, `.fetchall()`, `.df()`, `.arrow()`).
  Isso permite compor/reaproveitar relações sem custo.
- `con.execute(...)` segue o estilo DB-API (cursor), bom para comandos sem
  retorno tabular como `SET`/`PRAGMA`/`CREATE`.
- `CREATE VIEW nome AS SELECT ... FROM read_parquet(...)` registra um nome
  amigável para o glob — as queries seguintes usam `FROM nome` como se fosse
  tabela, mas os dados continuam nos arquivos.

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
