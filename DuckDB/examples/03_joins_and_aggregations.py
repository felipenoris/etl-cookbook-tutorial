"""Exemplo 3 — JOINs, agregações e window functions em SQL.

Conceitos:
- JOIN de 3 tabelas (fato `orders` + dimensões `customers`/`products`) via SQL
  puro, o mesmo cenário exercitado em pandas (`merge`) e pyarrow (`Table.join`).
- Window functions: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` e a
  cláusula `QUALIFY` (exclusiva de poucos motores, incluindo DuckDB) para
  filtrar direto no resultado da window function sem precisar de subquery.

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
