"""Testes de contrato do exemplo 18 (EXPLAIN ANALYZE / profiling).

Valida que o profiling em JSON expõe a árvore de operadores com timing e
cardinalidade real vs estimada, e que o scan do fato mostra o partition pruning
e o filtro dinâmico empurrado pelo join.
"""

import json

import duckdb
import pytest

from _common import CUSTOMERS_GLOB, ORDERS_GLOB

QUERY = f"""
    SELECT c.region, COUNT(*) AS pedidos, SUM(o.quantity) AS itens
    FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
    JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c
      ON o.customer_id = c.customer_id
    WHERE o.order_month = 1
    GROUP BY c.region
    ORDER BY pedidos DESC
"""


def achatar(node, out):
    tipo = node.get("operator_type", "")
    if tipo and tipo != "EXPLAIN_ANALYZE":
        out.append(node)
    for filho in node.get("children", []):
        achatar(filho, out)
    return out


@pytest.fixture
def operadores():
    con = duckdb.connect()
    con.execute("PRAGMA enable_profiling='json'")
    raiz = json.loads(con.sql("EXPLAIN ANALYZE " + QUERY).fetchall()[0][1])
    con.close()
    return achatar(raiz, [])


def test_profile_exposes_operator_tree(operadores):
    tipos = {op["operator_type"] for op in operadores}
    assert {"HASH_JOIN", "HASH_GROUP_BY", "TABLE_SCAN"} <= tipos
    # todo operador traz um timing numérico
    assert all(isinstance(op.get("operator_timing"), (int, float)) for op in operadores)


def test_group_by_estimate_is_far_from_real(operadores):
    # o GROUP BY reduz milhões para poucas regiões; o otimizador superestima muito
    gby = next(op for op in operadores if op["operator_type"] == "HASH_GROUP_BY")
    real = gby["operator_cardinality"]
    estimada = int(gby["extra_info"]["Estimated Cardinality"])
    assert real <= 10                       # poucas regiões de fato
    assert estimada > 100 * max(1, real)    # estimativa ordens de grandeza acima


def test_fact_scan_shows_pruning_and_dynamic_filter(operadores):
    scan = next(
        op
        for op in operadores
        if op["extra_info"].get("Function") == "READ_PARQUET"
        and "order" in str(op["extra_info"].get("Filename(s)", ""))
    )
    info = scan["extra_info"]
    assert info["Scanning Files"] == "1/6"          # pruning: só a partição de janeiro
    assert "customer_id" in info.get("Dynamic Filters", "")  # filtro empurrado pelo join
