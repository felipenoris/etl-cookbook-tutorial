"""Testes unitários dos datasets de `examples/_common.py` e operações-chave do Arrow.

Valida o contrato assumido pelos exemplos: schema dos datasets particionados,
partition pruning via filtro e comportamento de join/group_by.
"""

import pyarrow.compute as pc

from _common import customers_dataset, orders_dataset, products_dataset


def test_orders_dataset_discovers_six_partitions():
    orders = orders_dataset()
    assert len(orders.files) == 6
    # colunas de partição reconstruídas a partir do path (hive)
    assert "order_year" in orders.schema.names
    assert "order_month" in orders.schema.names


def test_partition_filter_reads_single_month():
    orders = orders_dataset()
    january = orders.to_table(filter=pc.field("order_month") == 1)
    full_count = orders.count_rows()
    assert january.num_rows * 6 == full_count


def test_customers_dataset_partitioned_by_region():
    customers = customers_dataset()
    table = customers.to_table()
    assert table.num_rows == 2_000
    regions = set(pc.unique(pc.cast(table["region"], "string")).to_pylist())
    assert regions == {"norte", "nordeste", "centro_oeste", "sudeste", "sul"}


def test_join_preserves_order_count():
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)
    products = products_dataset().to_table()
    joined = orders.join(products, keys="product_id", join_type="inner")
    # N:1 join contra dimensão completa: não perde nem duplica linhas
    assert joined.num_rows == orders.num_rows


def test_group_by_sum_matches_total():
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)
    by_status = orders.group_by("status").aggregate([("quantity", "sum")])
    assert pc.sum(by_status["quantity_sum"]).as_py() == pc.sum(orders["quantity"]).as_py()
