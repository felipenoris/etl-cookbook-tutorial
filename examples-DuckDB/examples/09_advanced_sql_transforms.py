"""Exemplo 9 — Transformações avançadas: CTE recursiva, PIVOT, ASOF JOIN e listas.

Quatro transformações que costumam expulsar o processamento do SQL para o
Python — mas que têm solução SQL direta no DuckDB.

`WITH RECURSIVE contas AS (âncora UNION ALL passo)`
    CTE recursiva (SQL padrão, mas raramente vista fora de DBAs): a *âncora*
    seleciona as raízes da hierarquia (`parent_id IS NULL`); o *passo
    recursivo* junta a tabela original com o resultado acumulado, uma
    "geração" por iteração, até não produzir linhas novas. Aqui, achata um
    plano de contas calculando nível e caminho completo (`Ativo > Circulante
    > Caixa`) de cada nó — o que imperativamente seria um loop com pilha.

`PIVOT tabela ON coluna USING agg GROUP BY chave`
    Statement nativo do DuckDB (não é SQL padrão — em Postgres seria
    crosstab/CASEs manuais): transforma valores DISTINTOS de `coluna` em
    COLUNAS do resultado (long -> wide). É o `pivot_table` do pandas em SQL,
    com uma consequência importante: as colunas de saída dependem dos DADOS
    (cada status vira uma coluna), coisa que o SQL clássico não permite.
    `UNPIVOT ... ON COLUMNS(* EXCLUDE (chave))` desfaz (wide -> long);
    `COLUMNS(* EXCLUDE ...)` é outra extensão DuckDB — seleção dinâmica de
    colunas por padrão.

`ASOF JOIN ... ON chave AND data_evento >= data_vigencia`
    Join temporal ("as of" = "na data de"): para cada linha da esquerda, casa
    com a ÚLTIMA linha da direita cuja vigência já começou — "qual era o
    preço vigente na data do pedido". Em SQL comum isso exige subquery
    correlacionada com `ORDER BY ... LIMIT 1` por linha (lento e ilegível);
    o ASOF JOIN (DuckDB, kdb+, Polars) resolve com um merge ordenado. Para
    tabelas de preço/câmbio/tarifa com vigência, é O comando.

`list(...)` + `UNNEST(...)`
    Tipos aninhados: `list(DISTINCT status ORDER BY status)` agrega o grupo
    numa LISTA dentro da célula (coisa que não existe no modelo relacional
    clássico — viola a 1ª forma normal de propósito); `UNNEST` explode a
    lista de volta em linhas. Útil para semi-estruturado (JSON) e para
    transportar coleções compactas entre etapas.

Rode com: `uv run examples/09_advanced_sql_transforms.py`
"""

import duckdb

from _common import ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()

    section("WITH RECURSIVE: achatando um plano de contas")
    con.execute(
        """
        CREATE TABLE plano_contas (id INT, nome VARCHAR, parent_id INT);
        INSERT INTO plano_contas VALUES
            (1, 'Ativo', NULL),
            (2, 'Circulante', 1),
            (3, 'Caixa', 2),
            (4, 'Estoques', 2),
            (5, 'Nao Circulante', 1),
            (6, 'Imobilizado', 5),
            (7, 'Passivo', NULL),
            (8, 'Fornecedores', 7)
        """
    )
    con.sql(
        """
        WITH RECURSIVE contas AS (
            -- âncora: as raízes da hierarquia
            SELECT id, nome, parent_id, 0 AS nivel, nome AS caminho
            FROM plano_contas WHERE parent_id IS NULL
            UNION ALL
            -- passo recursivo: filhos herdam nível+1 e o caminho acumulado
            SELECT p.id, p.nome, p.parent_id, c.nivel + 1, c.caminho || ' > ' || p.nome
            FROM plano_contas p JOIN contas c ON p.parent_id = c.id
        )
        SELECT nivel, caminho FROM contas ORDER BY caminho
        """
    ).show()

    section("PIVOT: pedidos por mês (linhas) x status (colunas)")
    con.execute(
        f"""
        CREATE VIEW pedidos_mes_status AS
        SELECT order_month, status, COUNT(*) AS pedidos
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        GROUP BY ALL
        """
    )
    con.execute(
        """
        CREATE TABLE pedidos_wide AS
        PIVOT pedidos_mes_status ON status USING SUM(pedidos) GROUP BY order_month
        """
    )
    con.sql("SELECT * FROM pedidos_wide ORDER BY order_month").show()

    section("UNPIVOT: de volta ao formato longo (wide -> long)")
    con.sql(
        """
        UNPIVOT pedidos_wide
        ON COLUMNS(* EXCLUDE (order_month))
        INTO NAME status VALUE pedidos
        ORDER BY order_month, status LIMIT 8
        """
    ).show()

    section("ASOF JOIN: preço vigente na data do pedido")
    con.execute(
        """
        CREATE TABLE historico_precos (product_id BIGINT, vigente_desde DATE, preco DOUBLE);
        INSERT INTO historico_precos VALUES
            (1, DATE '2025-01-01', 100.0),
            (1, DATE '2025-01-10', 110.0),   -- reajuste no dia 10
            (1, DATE '2025-01-20', 105.0),   -- promoção no dia 20
            (2, DATE '2025-01-01', 50.0)
        """
    )
    # Para cada pedido, o ASOF JOIN pega a ÚLTIMA linha do histórico cuja
    # vigência começou até a data do pedido (>=) — sem subquery correlacionada.
    con.sql(
        f"""
        SELECT o.order_id, o.order_date, o.product_id, p.preco AS preco_vigente
        FROM (
            SELECT order_id, order_date, product_id
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1 AND product_id IN (1, 2)
            ORDER BY order_id LIMIT 6
        ) o
        ASOF JOIN historico_precos p
            ON o.product_id = p.product_id AND o.order_date >= p.vigente_desde
        ORDER BY o.product_id, o.order_date
        """
    ).show()

    section("LIST + UNNEST: agregando em lista e explodindo de volta")
    con.sql(
        f"""
        WITH por_cliente AS (
            SELECT customer_id, list(DISTINCT status ORDER BY status) AS statuses
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1 AND customer_id <= 3
            GROUP BY customer_id
        )
        SELECT customer_id, statuses, len(statuses) AS qtd_statuses
        FROM por_cliente ORDER BY customer_id
        """
    ).show()
    con.sql(
        f"""
        WITH por_cliente AS (
            SELECT customer_id, list(DISTINCT status ORDER BY status) AS statuses
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1 AND customer_id <= 2
            GROUP BY customer_id
        )
        SELECT customer_id, UNNEST(statuses) AS status
        FROM por_cliente ORDER BY customer_id, status
        """
    ).show()
