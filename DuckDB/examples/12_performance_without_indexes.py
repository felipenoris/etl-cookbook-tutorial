"""Exemplo 12 — Performance sem índices: o paralelo entre índice OLTP e layout parquet.

Numa base transacional, consulta lenta = criar índice (B-tree): uma estrutura
auxiliar que evita varrer a tabela. **Arquivos parquet não têm índices** — e o
DuckDB não cria índices sobre eles. O paralelo funciona assim:

| Base transacional            | DuckDB + parquet                                |
|------------------------------|-------------------------------------------------|
| índice na coluna de filtro   | particionamento (dir) + ORDENAR o arquivo por ela |
| index scan                   | partition pruning + zonemap skipping            |
| índice composto (a, b)       | partição por `a`, ORDER BY `b` na escrita       |
| SELECT só das colunas úteis  | idem — mas aqui é de graça (formato colunar)    |
| EXPLAIN / planos             | EXPLAIN ANALYZE / parquet_metadata()            |

O mecanismo por trás: todo arquivo parquet é dividido em **row groups**
(~122k linhas por default no DuckDB), e cada row group guarda **min/max de
cada coluna** nos metadados (as "zonemaps"). Ao filtrar
`WHERE customer_id = 1234`, o DuckDB lê os metadados e PULA os row groups
cujo intervalo [min, max] não contém 1234 — sem abrir os dados.

A consequência — e a resposta para "li que envolvia ordenar colunas
selecionadas" — é que **a estatística só é seletiva se os dados estiverem
agrupados**: num arquivo em ordem de chegada, cada row group contém clientes
de 1 a 2000 (min=1, max=2000 em todos: nenhum é pulado); no MESMO dado
gravado com `ORDER BY customer_id`, cada row group cobre uma faixa estreita
(1..8, 9..15, ...) e a consulta pontual lê ~1 row group em vez de ~270.
Ordenar na escrita é o "CREATE INDEX" do mundo parquet — pago uma vez no ETL
que grava, aproveitado por toda leitura posterior.

Sobre JOINs: a intuição OLTP de "join rápido precisa de índice na FK" não se
aplica. O DuckDB usa **hash join**: constrói uma hash table em memória com o
lado menor (a dimensão) e varre o lado maior uma única vez — sem índice
nenhum. Para joins analíticos (varrer tudo), isso é mais rápido que o
nested-loop indexado do OLTP, que é imbatível apenas para buscar POUCAS
linhas.

(Tabelas internas do DuckDB têm zonemaps automáticas e aceitam
`CREATE INDEX`/PRIMARY KEY [índice ART] — útil para point lookups e
constraints, raramente para queries analíticas.)

Rode com: `uv run examples/12_performance_without_indexes.py`
"""

import shutil
import time

import duckdb

from _common import ORDERS_GLOB, PRODUCTS_GLOB, RICH_DIR, section

OUT_DIR = RICH_DIR / "duckdb_perf_demo"
ROW_GROUP_SIZE = 122_880  # o default do DuckDB, explícito para o exemplo
CLIENTE_ALVO = 1234


def cronometrar(con: duckdb.DuckDBPyConnection, sql: str, rodadas: int = 3) -> float:
    """Tempo médio de execução de uma query, em segundos."""
    inicio = time.perf_counter()
    for _ in range(rodadas):
        con.sql(sql).fetchall()
    return (time.perf_counter() - inicio) / rodadas


if __name__ == "__main__":
    con = duckdb.connect()
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    nao_ordenado = OUT_DIR / "orders_nao_ordenado.parquet"
    ordenado = OUT_DIR / "orders_ordenado_por_cliente.parquet"

    section("Preparando: o MESMO dado gravado de dois jeitos")
    # ordem de chegada (order_id): clientes espalhados por todos os row groups
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet('{ORDERS_GLOB}') ORDER BY order_id)
        TO '{nao_ordenado}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
        """
    )
    # clusterizado: ORDER BY customer_id na escrita = o "CREATE INDEX" do parquet
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet('{ORDERS_GLOB}') ORDER BY customer_id)
        TO '{ordenado}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
        """
    )
    for arquivo in (nao_ordenado, ordenado):
        print(f"{arquivo.name}: {arquivo.stat().st_size / (1024 * 1024):.0f}MB")

    section("Zonemaps via parquet_metadata(): min/max de customer_id por row group")
    for arquivo in (nao_ordenado, ordenado):
        print(f"\n{arquivo.name} (5 primeiros row groups):")
        con.sql(
            f"""
            SELECT row_group_id,
                   stats_min_value AS cliente_min,
                   stats_max_value AS cliente_max
            FROM parquet_metadata('{arquivo}')
            WHERE path_in_schema = 'customer_id' AND row_group_id < 5
            ORDER BY row_group_id
            """
        ).show()

    section(f"Quantos row groups a consulta 'customer_id = {CLIENTE_ALVO}' precisa abrir?")
    for arquivo in (nao_ordenado, ordenado):
        candidatos, total = con.sql(
            f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE CAST(stats_min_value AS BIGINT) <= {CLIENTE_ALVO}
                      AND CAST(stats_max_value AS BIGINT) >= {CLIENTE_ALVO}
                ),
                COUNT(*)
            FROM parquet_metadata('{arquivo}')
            WHERE path_in_schema = 'customer_id'
            """
        ).fetchone()
        print(f"{arquivo.name}: {candidatos} de {total} row groups")

    section("O efeito no tempo da consulta pontual (média de 3 execuções)")
    consulta = "SELECT COUNT(*), SUM(quantity) FROM read_parquet('{f}') WHERE customer_id = " + str(CLIENTE_ALVO)
    t_nao_ordenado = cronometrar(con, consulta.format(f=nao_ordenado))
    t_ordenado = cronometrar(con, consulta.format(f=ordenado))
    print(f"não ordenado: {t_nao_ordenado * 1000:6.1f}ms (abre todos os row groups)")
    print(f"ordenado:     {t_ordenado * 1000:6.1f}ms (pula quase todos via zonemap)")
    print(f"-> {t_nao_ordenado / t_ordenado:.0f}x mais rápido, mesmo dado, só mudou o layout")

    section("Colunar: ler menos colunas = ler menos bytes (de graça, sem 'covering index')")
    t_uma_coluna = cronometrar(con, f"SELECT SUM(quantity) FROM read_parquet('{ordenado}')")
    t_todas = cronometrar(
        con,
        f"""
        SELECT MAX(order_id), MAX(customer_id), MAX(product_id),
               MAX(order_date), MAX(quantity), MAX(status)
        FROM read_parquet('{ordenado}')
        """,
    )
    print(f"agregando 1 coluna:  {t_uma_coluna * 1000:6.1f}ms")
    print(f"agregando 6 colunas: {t_todas * 1000:6.1f}ms")
    print("(num formato orientado a linhas, os dois leriam o arquivo inteiro)")

    section("JOIN sem índice: hash join constrói a 'lookup table' na hora")
    plano = con.sql(
        f"""
        EXPLAIN SELECT p.category, SUM(o.quantity * p.unit_price)
        FROM read_parquet('{ordenado}') o
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        GROUP BY p.category
        """
    ).fetchall()[0][1]
    usa_hash_join = "HASH_JOIN" in plano
    t_join = cronometrar(con, f"""
        SELECT p.category, SUM(o.quantity * p.unit_price)
        FROM read_parquet('{ordenado}') o
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        GROUP BY p.category
    """, rodadas=1)
    print(f"plano usa HASH_JOIN: {usa_hash_join} | join de 33.7M x 200 linhas: {t_join:.2f}s")
    print("(o lado menor — products — vira hash table em memória; nenhum índice envolvido)")

    section("Checklist de performance para ETL com DuckDB + parquet")
    print("1. particione os diretórios pela coluna de filtro mais frequente (ex.: mês);")
    print("2. dentro de cada partição, grave com ORDER BY nas colunas de filtro")
    print("   secundárias (ex.: customer_id) — é o 'índice' do parquet;")
    print("3. selecione só as colunas necessárias (o formato colunar faz o resto);")
    print("4. row groups: o default (~122k linhas) atende; muito pequenos = overhead")
    print("   de metadados, muito grandes = zonemaps pouco seletivas;")
    print("5. meça com EXPLAIN ANALYZE e inspecione zonemaps com parquet_metadata().")
