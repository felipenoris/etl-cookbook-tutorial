"""Testes unitários dos helpers de `examples/_common.py` e invariantes dos dados.

Valida o contrato que todos os exemplos assumem: colunas esperadas, dtypes
com backend Arrow (`ArrowDtype`) e integridade das chaves de join.
"""

import pandas as pd

from _common import load_customers, load_orders, load_products


def test_customers_loads_with_arrow_backend():
    customers = load_customers()
    assert set(customers.columns) == {
        "customer_id",
        "customer_name",
        "region",
        "signup_date",
        "is_active",
        "signup_ts",
        "address",
        "tags",
        "preferences",
    }
    assert isinstance(customers["customer_id"].dtype, pd.ArrowDtype)
    assert isinstance(customers["customer_name"].dtype, pd.ArrowDtype)
    assert len(customers) == 2_000
    assert customers["customer_id"].is_unique


def test_products_loads_with_arrow_backend():
    products = load_products()
    assert set(products.columns) == {
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "unit_cost",
        "sku",
    }
    assert isinstance(products["unit_price"].dtype, pd.ArrowDtype)
    assert len(products) == 200
    assert products["product_id"].is_unique
    assert (products["unit_price"] > 0).all()


def test_orders_default_loads_single_month():
    orders = load_orders()
    assert set(orders.columns) == {
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "status",
    }
    assert isinstance(orders["quantity"].dtype, pd.ArrowDtype)
    months = orders["order_date"].dt.month.unique().tolist()
    assert months == [1]


def test_orders_join_keys_reference_dimensions():
    orders = load_orders().head(50_000)
    customers = load_customers()
    products = load_products()
    assert orders["customer_id"].isin(customers["customer_id"]).all()
    assert orders["product_id"].isin(products["product_id"]).all()


def test_orders_multiple_months_concatenates():
    one = load_orders([1])
    two = load_orders([1, 2])
    assert len(two) == 2 * len(one)
