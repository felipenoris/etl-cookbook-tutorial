"""Exemplo 3 — JOINs, agregações e window functions em SQL.

JOIN de 3 tabelas (fato `orders` + dimensões `customers`/`products`) via SQL
puro, o mesmo cenário exercitado em pandas (`merge`) e pyarrow (`Table.join`).

Guia dos comandos usados (para quem vem de SQL transacional básico):

`JOIN products p USING (product_id)`
    Atalho para `JOIN products p ON o.product_id = p.product_id`, disponível
    quando a coluna de junção tem o MESMO nome nas duas tabelas. Além de mais
    curto, o `USING` deduplica a coluna no resultado (sai um `product_id` só,
    em vez de `o.product_id` e `p.product_id`). É SQL padrão, mas pouco visto
    em bases transacionais; em ETL analítico, onde os joins por chave homônima
    são a regra, aparece o tempo todo.

`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`
    Window function (função de janela). Diferente de um `GROUP BY` — que
    colapsa cada grupo em UMA linha de saída — a window function preserva
    todas as linhas e calcula um valor extra por linha, olhando para as
    "vizinhas" do mesmo grupo:
    - `PARTITION BY p.category` divide as linhas em janelas (uma por
      categoria), como um `GROUP BY` que não colapsa;
    - `ORDER BY o.quantity * p.unit_price DESC` ordena as linhas DENTRO de
      cada janela (não confundir com o `ORDER BY` final da query, que ordena
      a saída);
    - `ROW_NUMBER()` devolve 1, 2, 3... seguindo essa ordem, reiniciando a
      contagem a cada janela.
    Na prática: "numere os pedidos de cada categoria do mais caro para o mais
    barato". Filtrar por `rank <= 2` depois dá o "top 2 por grupo" — um
    problema clássico que sem window function exigiria subqueries
    correlacionadas bem mais lentas e ilegíveis.

`QUALIFY rank_na_categoria <= 2`
    Filtro que age sobre o RESULTADO da window function. O SQL padrão não
    permite window function no `WHERE` (o `WHERE` roda antes das janelas
    serem calculadas), então normalmente é preciso embrulhar a query numa
    subquery só para filtrar o rank. O `QUALIFY` — específico de poucos
    motores analíticos (DuckDB, Snowflake, BigQuery); não existe em
    Postgres/MySQL/SQL Server — elimina essa subquery: é o "WHERE das window
    functions". O exemplo mostra as duas formas produzindo o mesmo resultado.

`AVG(receita) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`
    Média móvel de 3 dias como window function. Aqui não há `PARTITION BY`
    (a janela é a tabela toda) e a novidade é o *frame* `ROWS BETWEEN ...`:
    para cada linha, a função agrega só as linhas de 2 atrás até a atual
    (na ordem de `order_date`) — ou seja, `AVG` da linha corrente + as 2
    anteriores. Nos 2 primeiros dias a janela ainda está "incompleta" (1 e 2
    linhas) e a média usa o que existe. Frames servem para qualquer agregado:
    `SUM(...) OVER (... ROWS UNBOUNDED PRECEDING)` dá acumulado, etc.

Rode com: `uv run examples/03_joins_and_aggregations.py`
"""

import duckdb

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, PRODUCTS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()
    con.execute(f"CREATE VIEW orders AS SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)")
    con.execute(f"CREATE VIEW customers AS SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)")
    con.execute(f"CREATE VIEW products AS SELECT * FROM read_parquet('{PRODUCTS_GLOB}')")

    section("JOIN de 3 tabelas + agregação: receita por categoria e região")
    con.sql(
        """
        SELECT
            c.region,
            p.category,
            SUM(o.quantity * p.unit_price) AS receita,
            COUNT(*) AS total_pedidos
        FROM orders o
        JOIN customers c USING (customer_id)
        JOIN products p USING (product_id)
        WHERE o.order_month = 1
        GROUP BY c.region, p.category
        ORDER BY receita DESC
        LIMIT 10
        """
    ).show()

    section("Window function: ranking de pedidos por receita dentro de cada categoria")
    con.sql(
        """
        SELECT * FROM (
            SELECT
                o.order_id,
                p.category,
                o.quantity * p.unit_price AS receita,
                ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY o.quantity * p.unit_price DESC) AS rank_na_categoria
            FROM orders o
            JOIN products p USING (product_id)
            WHERE o.order_month = 1
        )
        WHERE rank_na_categoria <= 2
        ORDER BY category, rank_na_categoria
        """
    ).show()

    section("QUALIFY: mesmo resultado, sem precisar de subquery")
    con.sql(
        """
        SELECT
            o.order_id,
            p.category,
            o.quantity * p.unit_price AS receita,
            ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY o.quantity * p.unit_price DESC) AS rank_na_categoria
        FROM orders o
        JOIN products p USING (product_id)
        WHERE o.order_month = 1
        QUALIFY rank_na_categoria <= 2
        ORDER BY category, rank_na_categoria
        """
    ).show()

    section("Média móvel de receita diária com window function (ROWS BETWEEN)")
    con.sql(
        """
        WITH receita_diaria AS (
            SELECT o.order_date, SUM(o.quantity * p.unit_price) AS receita
            FROM orders o
            JOIN products p USING (product_id)
            WHERE o.order_month = 1
            GROUP BY o.order_date
        )
        SELECT
            order_date,
            receita,
            AVG(receita) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS media_movel_3d
        FROM receita_diaria
        ORDER BY order_date
        LIMIT 5
        """
    ).show()
