"""Testes unitários das consultas DuckDB usadas nos exemplos.

Valida leitura particionada (hive), partition pruning no plano de execução,
consistência do join de 3 tabelas e o mecanismo de spill com memory_limit.
"""

from pathlib import Path

import duckdb
import pytest

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, PRODUCTS_GLOB


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_hive_partitioning_exposes_partition_columns(con):
    schema = con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)"
    ).fetchall()
    column_names = {row[0] for row in schema}
    assert "order_year" in column_names
    assert "order_month" in column_names


def test_partition_filter_counts_one_sixth_of_rows(con):
    total = con.sql(f"SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}')").fetchone()[0]
    january = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchone()[0]
    assert january * 6 == total


def test_explain_shows_reduced_scan_with_partition_filter(con):
    # SUM(quantity) força um scan de verdade (COUNT(*) puro é respondido só
    # com estatísticas do parquet e o plano nem mostra o READ_PARQUET).
    plan = con.sql(
        f"""
        EXPLAIN SELECT SUM(quantity)
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchall()[0][1]
    # o plano deve indicar 1 arquivo lido (dos 6 do dataset)
    assert "Scanning Files: 1/6" in plan


def test_three_table_join_preserves_fact_rows(con):
    fact_count, join_count = con.sql(
        f"""
        WITH j AS (
            SELECT o.order_id
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
            JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c USING (customer_id)
            JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
            WHERE o.order_month = 1
        )
        SELECT
            (SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) WHERE order_month = 1),
            (SELECT COUNT(*) FROM j)
        """
    ).fetchone()
    assert fact_count == join_count


def test_memory_limit_spills_to_temp_directory(tmp_path: Path, con):
    con.execute("SET memory_limit='150MB'")
    con.execute(f"SET temp_directory='{tmp_path}'")
    con.execute("SET preserve_insertion_order=false")

    first_row = con.sql(
        f"""
        SELECT customer_id, quantity FROM read_parquet('{ORDERS_GLOB}')
        ORDER BY quantity DESC, customer_id
        """
    ).fetchone()

    assert first_row is not None
    spill_files = list(tmp_path.iterdir())
    assert spill_files, "esperava arquivos de spill em temp_directory com memory_limit=150MB"


def test_arrow_roundtrip(con):
    regions = con.sql(
        f"""
        SELECT region, COUNT(*) AS total
        FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
        GROUP BY region
        """
    ).to_arrow_table()
    assert regions.num_rows == 5
    # a Table Arrow resultante pode ser consultada de volta pelo DuckDB
    total = con.sql("SELECT SUM(total) FROM regions").fetchone()[0]
    assert total == 2_000
