"""Exemplo 14 — Tipos de dados no DuckDB: do BOOLEAN ao MAP, lendo e escrevendo parquet.

As dimensões de `data/raw` cobrem os principais tipos da stack; o DuckDB mapeia
cada tipo lógico do parquet/Arrow para um tipo SQL próprio:

| parquet/Arrow          | DuckDB               |
|------------------------|----------------------|
| bool                   | BOOLEAN              |
| timestamp[us]          | TIMESTAMP            |
| decimal128(12,2)       | DECIMAL(12,2)        |
| list<string>           | VARCHAR[]            |
| struct<...>            | STRUCT(...)          |
| map<string,string>     | MAP(VARCHAR, VARCHAR)|
| binary                 | BLOB                 |

Destaques para quem vem de bases transacionais:
- structs são acessados com **notação de ponto** (`address.city`), como se a
  coluna aninhada fosse uma tabela dentro da célula;
- listas têm o operador de indexação `[1]` (1-based!) e funções `list_*`;
- maps são indexados por chave (`preferences['canal']`);
- a aritmética de DECIMAL permanece DECIMAL (verificável com `typeof()`) —
  dinheiro não vira DOUBLE no meio da query;
- todos os tipos sobrevivem ao roundtrip `COPY TO parquet` -> `read_parquet`.

Rode com: `uv run examples/14_data_types.py`
"""

import shutil

import duckdb

from _common import CUSTOMERS_GLOB, PRODUCTS_GLOB, RICH_DIR, section

OUT_DIR = RICH_DIR / "duckdb_types_demo"

if __name__ == "__main__":
    con = duckdb.connect()
    con.execute(
        f"""
        CREATE VIEW customers AS
            SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true);
        CREATE VIEW products AS SELECT * FROM read_parquet('{PRODUCTS_GLOB}');
        """
    )

    section("DESCRIBE: como o DuckDB enxerga cada tipo do parquet")
    con.sql("DESCRIBE customers").show()
    con.sql("DESCRIBE products").show()

    section("BOOLEAN: filtros e agregação condicional (FILTER)")
    con.sql(
        """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE is_active) AS ativos,
               ROUND(100.0 * COUNT(*) FILTER (WHERE is_active) / COUNT(*), 1) AS pct
        FROM customers
        """
    ).show()

    section("TIMESTAMP: date_trunc, extract e aritmética de datas")
    con.sql(
        """
        SELECT date_trunc('year', signup_ts) AS ano,
               COUNT(*) AS cadastros,
               MIN(signup_ts) AS primeiro
        FROM customers GROUP BY ano ORDER BY ano
        """
    ).show()

    section("STRUCT: notação de ponto (address.city) direto no SQL")
    con.sql(
        """
        SELECT address.city AS cidade, COUNT(*) AS clientes
        FROM customers GROUP BY cidade ORDER BY clientes DESC LIMIT 5
        """
    ).show()

    section("LIST: indexação 1-based, len(), list_contains e UNNEST")
    con.sql(
        """
        SELECT customer_id, tags, len(tags) AS n, tags[1] AS primeira
        FROM customers WHERE len(tags) > 0 LIMIT 4
        """
    ).show()
    # UNNEST é o inverso de uma agregação de lista: pega UMA linha cuja coluna é
    # uma lista de N itens e a "explode" em N linhas (uma por item) — o cliente com
    # tags ['vip','online'] vira duas linhas. Depois de explodido, agrega-se por
    # cima como colunas normais; por isso o UNNEST fica numa subquery e o GROUP BY
    # do lado de fora conta as ocorrências de cada tag.
    con.sql(
        """
        SELECT tag, COUNT(*) AS clientes
        FROM (SELECT UNNEST(tags) AS tag FROM customers)
        GROUP BY tag ORDER BY clientes DESC
        """
    ).show()

    section("MAP: extração por chave e map_keys()")
    con.sql(
        """
        SELECT preferences['canal'] AS canal, COUNT(*) AS clientes
        FROM customers GROUP BY canal ORDER BY clientes DESC
        """
    ).show()

    section("DECIMAL(12,2): a aritmética NÃO degrada para DOUBLE")
    con.sql(
        """
        SELECT SUM(unit_cost) AS custo_total,
               typeof(SUM(unit_cost)) AS tipo_da_soma,
               ROUND(AVG(unit_cost), 2) AS custo_medio
        FROM products
        """
    ).show()
    con.sql(
        "SELECT unit_cost, unit_cost * 2 AS dobro, typeof(unit_cost * 2) AS tipo FROM products LIMIT 2"
    ).show()
    # margem = float64 - decimal: o DuckDB promove para DOUBLE (cuidado!);
    # para manter decimal, faça o cast do float ANTES da conta
    con.sql(
        """
        SELECT product_id,
               typeof(unit_price - unit_cost) AS tipo_ingenuo,
               typeof(CAST(unit_price AS DECIMAL(12,2)) - unit_cost) AS tipo_correto
        FROM products LIMIT 1
        """
    ).show()

    section("DECIMAL e DATE no Python: fetchone() devolve os tipos da stdlib")
    total = con.sql("SELECT SUM(unit_cost) FROM products").fetchone()[0]
    print(f"DECIMAL -> {total!r} | tipo: {type(total).__module__}.{type(total).__name__}")
    primeira_data = con.sql("SELECT MIN(signup_date) FROM customers").fetchone()[0]
    print(f"DATE    -> {primeira_data!r} | tipo: {type(primeira_data).__module__}.{type(primeira_data).__name__}")
    print("(exatidão decimal e datas de calendário atravessam o client API intactas)")

    section("BLOB (binary): hex(), octet_length()")
    con.sql(
        """
        SELECT product_id, hex(sku) AS sku_hex, octet_length(sku) AS bytes
        FROM products LIMIT 3
        """
    ).show()

    section("Roundtrip: COPY TO parquet preserva todos os tipos")
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT customer_id, is_active, signup_ts, address, tags, preferences,
                   address.city AS cidade_achatada
            FROM customers
        ) TO '{OUT_DIR / "customers_tipos.parquet"}' (FORMAT parquet)
        """
    )
    con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{OUT_DIR / 'customers_tipos.parquet'}')"
    ).show()
